"""YouTube Data API v3 + YouTube Analytics API v2 клиент для Triple Bot.

Авторизация через refresh_token (один раз получен get_yt_token.py локально).
На Render 3 env vars: YT_CLIENT_ID, YT_CLIENT_SECRET, YT_REFRESH_TOKEN.

Analytics API требует scope yt-analytics.readonly — см. get_yt_token.py.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]


def _make_creds() -> Credentials:
    client_id = os.getenv("YT_CLIENT_ID", "").strip()
    client_secret = os.getenv("YT_CLIENT_SECRET", "").strip()
    refresh_token = os.getenv("YT_REFRESH_TOKEN", "").strip()
    if not all([client_id, client_secret, refresh_token]):
        raise RuntimeError("YT_CLIENT_ID / YT_CLIENT_SECRET / YT_REFRESH_TOKEN не заданы в env")
    return Credentials(
        token=None,
        refresh_token=refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=SCOPES,
    )


def get_yt_client():
    return build("youtube", "v3", credentials=_make_creds(), cache_discovery=False)


def get_yt_analytics_client():
    """YouTube Analytics API v2 — те же creds, scope yt-analytics.readonly.

    Требует переавторизации через get_yt_token.py если старый токен без этого scope.
    """
    return build("youtubeAnalytics", "v2", credentials=_make_creds(), cache_discovery=False)


def get_videos_analytics(video_ids: list[str], days: int = 28) -> dict[str, dict]:
    """Batch CTR + stats из YouTube Analytics API v2 за последние N дней.

    Returns: {video_id: {ctr (%), views, avg_view_duration_sec, watch_minutes}}
    Raises HttpError если scope yt-analytics.readonly не выдан (нужна переавторизация).
    """
    if not video_ids:
        return {}
    from datetime import date, timedelta
    end = date.today()
    start = end - timedelta(days=days)
    yt_a = get_yt_analytics_client()
    out: dict[str, dict] = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        resp = yt_a.reports().query(
            ids="channel==MINE",
            startDate=start.isoformat(),
            endDate=end.isoformat(),
            metrics="views,clickThroughRate,averageViewDuration,estimatedMinutesWatched",
            dimensions="video",
            filters=f"video=={','.join(batch)}",
            maxResults=200,
        ).execute()
        headers = [h["name"] for h in resp.get("columnHeaders", [])]
        for row in resp.get("rows", []):
            d = dict(zip(headers, row))
            vid = d.get("video", "")
            if not vid:
                continue
            ctr_raw = float(d.get("clickThroughRate") or 0)
            out[vid] = {
                "ctr": round(ctr_raw * 100, 2),
                "views": int(d.get("views") or 0),
                "avg_view_duration_sec": round(float(d.get("averageViewDuration") or 0), 1),
                "watch_minutes": round(float(d.get("estimatedMinutesWatched") or 0), 1),
            }
    return out


def list_channel_videos(max_results: int = 200) -> list[dict]:
    """Получает все видео канала через uploads playlist (paginated).

    Возвращает list dict с полями: video_id, title, published_at, thumbnail_url.
    Per-video stats не включены (тяжело — отдельный get_video_stats для них).

    `max_results` — soft limit. Каждая страница = 50 items + 1 quota unit.
    """
    yt = get_yt_client()
    # 1) Get my channel's uploads playlist id
    ch = yt.channels().list(part="contentDetails", mine=True).execute()
    uploads_pl = ch["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # 2) Pagination через playlistItems().list
    items = []
    page_token = None
    while True:
        resp = yt.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=uploads_pl,
            maxResults=50,
            pageToken=page_token,
        ).execute()
        for it in resp.get("items", []):
            sn = it.get("snippet", {})
            cd = it.get("contentDetails", {})
            items.append({
                "video_id": cd.get("videoId") or sn.get("resourceId", {}).get("videoId"),
                "title": sn.get("title", ""),
                "published_at": cd.get("videoPublishedAt") or sn.get("publishedAt", ""),
                "thumbnail_url": (sn.get("thumbnails") or {}).get("medium", {}).get("url", ""),
            })
            if len(items) >= max_results:
                return items
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return items


def get_videos_stats(video_ids: list[str]) -> dict[str, dict]:
    """Batch get statistics для list video_ids. YT API позволяет до 50 за раз.

    Returns: { video_id: {views, likes, comments, duration_iso} }
    """
    yt = get_yt_client()
    out = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        resp = yt.videos().list(
            part="statistics,contentDetails",
            id=",".join(batch),
        ).execute()
        for v in resp.get("items", []):
            stats = v.get("statistics", {})
            cd = v.get("contentDetails", {})
            out[v["id"]] = {
                "views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "comments": int(stats.get("commentCount", 0)),
                "duration_iso": cd.get("duration", ""),
            }
    return out


def get_video_snippet(video_id: str) -> dict:
    """Возвращает snippet существующего видео (title, description, tags, categoryId)."""
    yt = get_yt_client()
    resp = yt.videos().list(part="snippet", id=video_id).execute()
    items = resp.get("items", [])
    if not items:
        raise ValueError(f"Video not found: {video_id}")
    return items[0]["snippet"]


def update_video_title(video_id: str, new_title: str) -> dict:
    """Обновляет только title существующего видео, сохраняя остальные snippet-поля."""
    snippet = get_video_snippet(video_id)
    snippet["title"] = new_title
    yt = get_yt_client()
    body = {"id": video_id, "snippet": snippet}
    try:
        resp = yt.videos().update(part="snippet", body=body).execute()
        logger.info("YT title update OK: %s → %s", video_id, new_title[:50])
        return resp
    except HttpError as e:
        logger.error("YT title update FAIL %s: %s", video_id, e)
        raise


def update_video(
    video_id: str,
    title: str,
    description: str,
    tags: list[str],
    category_id: str = "10",  # 10 = Music
) -> dict:
    """Обновляет title/description/tags существующего видео."""
    yt = get_yt_client()
    body = {
        "id": video_id,
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags,
            "categoryId": category_id,
        },
    }
    try:
        resp = yt.videos().update(part="snippet", body=body).execute()
        logger.info("YT update OK: %s → %s", video_id, title[:50])
        return resp
    except HttpError as e:
        logger.error("YT update FAIL %s: %s", video_id, e)
        raise


def upload_video(
    video_path: Path,
    title: str,
    description: str,
    tags: list[str],
    thumbnail_path: Optional[Path] = None,
    category_id: str = "10",
    privacy: str = "public",
) -> str:
    """Загружает видео на YT + опционально кастомный thumbnail.

    Returns video_id.
    """
    if not video_path.exists():
        raise FileNotFoundError(video_path)
    yt = get_yt_client()
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags,
            "categoryId": category_id,
        },
        "status": {
            "privacyStatus": privacy,
            "selfDeclaredMadeForKids": False,
        },
    }
    media = MediaFileUpload(str(video_path), chunksize=-1, resumable=True, mimetype="video/mp4")
    req = yt.videos().insert(part="snippet,status", body=body, media_body=media)
    resp = None
    while resp is None:
        status, resp = req.next_chunk()
        if status:
            logger.info("YT upload progress: %d%%", int(status.progress() * 100))
    video_id = resp["id"]
    logger.info("YT upload OK: %s", video_id)

    if thumbnail_path and thumbnail_path.exists():
        try:
            yt.thumbnails().set(
                videoId=video_id,
                media_body=MediaFileUpload(str(thumbnail_path), mimetype="image/jpeg"),
            ).execute()
            logger.info("YT thumbnail set for %s", video_id)
        except HttpError as e:
            logger.warning("YT thumbnail failed (не критично): %s", e)

    return video_id


def get_channel_stats() -> dict:
    yt = get_yt_client()
    resp = yt.channels().list(part="snippet,statistics", mine=True).execute()
    ch = resp["items"][0]
    return {
        "title": ch["snippet"]["title"],
        "subs": int(ch["statistics"].get("subscriberCount", 0)),
        "views": int(ch["statistics"].get("viewCount", 0)),
        "videos": int(ch["statistics"].get("videoCount", 0)),
    }


def get_video(video_id: str) -> Optional[dict]:
    yt = get_yt_client()
    resp = yt.videos().list(part="snippet,statistics", id=video_id).execute()
    items = resp.get("items") or []
    return items[0] if items else None


# ── Playlists ────────────────────────────────────────────────
# Winning-паттерн type-beat каналов: один плейлист на артиста + один на сцену
# → session watch-time ×2.

_PLAYLIST_CACHE: dict[str, str] = {}  # title_lower → playlist_id


def _load_my_playlists() -> dict[str, str]:
    """Возвращает {title_lower: playlist_id} всех playlists канала.

    Кэшируется in-memory до рестарта процесса — плейлисты не меняются часто.
    """
    if _PLAYLIST_CACHE:
        return _PLAYLIST_CACHE
    yt = get_yt_client()
    page = None
    while True:
        req = yt.playlists().list(part="snippet", mine=True, maxResults=50, pageToken=page)
        resp = req.execute()
        for pl in resp.get("items", []):
            title = pl["snippet"]["title"].strip().lower()
            _PLAYLIST_CACHE[title] = pl["id"]
        page = resp.get("nextPageToken")
        if not page:
            break
    logger.info("YT playlists loaded: %d", len(_PLAYLIST_CACHE))
    return _PLAYLIST_CACHE


def find_or_create_playlist(title: str, description: str = "", privacy: str = "public") -> str:
    """Находит плейлист по title (case-insensitive) или создаёт новый. Возвращает playlistId."""
    cache = _load_my_playlists()
    key = title.strip().lower()
    if key in cache:
        return cache[key]
    yt = get_yt_client()
    body = {
        "snippet": {"title": title, "description": description},
        "status": {"privacyStatus": privacy},
    }
    resp = yt.playlists().insert(part="snippet,status", body=body).execute()
    pl_id = resp["id"]
    cache[key] = pl_id
    logger.info("YT playlist CREATED: %s → %s", title, pl_id)
    return pl_id


def post_comment(video_id: str, text: str) -> bool:
    """Постит top-level comment под видео. Pinning через API недоступен
    (убрали из публичного API в 2024) — коммент постится от имени канала.

    Даже без pinning это даёт engagement signal в первые минуты — алгоритм
    считает «у видео есть активность сразу».
    """
    try:
        yt = get_yt_client()
        body = {
            "snippet": {
                "videoId": video_id,
                "topLevelComment": {"snippet": {"textOriginal": text}},
            }
        }
        yt.commentThreads().insert(part="snippet", body=body).execute()
        logger.info("YT comment posted on %s", video_id)
        return True
    except HttpError as e:
        logger.warning("YT comment FAIL on %s: %s", video_id, e)
        return False


def add_video_to_playlist(video_id: str, playlist_title: str, playlist_desc: str = "") -> bool:
    """Добавляет видео в плейлист (создаёт если нет). Возвращает True при успехе."""
    try:
        pl_id = find_or_create_playlist(playlist_title, description=playlist_desc)
        yt = get_yt_client()
        body = {
            "snippet": {
                "playlistId": pl_id,
                "resourceId": {"kind": "youtube#video", "videoId": video_id},
            }
        }
        yt.playlistItems().insert(part="snippet", body=body).execute()
        logger.info("YT added %s to playlist '%s'", video_id, playlist_title)
        return True
    except HttpError as e:
        # 409 = duplicate (видео уже в плейлисте) — не критично
        if "duplicate" in str(e).lower():
            logger.info("YT %s already in playlist '%s'", video_id, playlist_title)
            return True
        logger.warning("YT playlist add FAIL %s → '%s': %s", video_id, playlist_title, e)
        return False
    except Exception as e:
        logger.exception("YT playlist add error")
        return False
