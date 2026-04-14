"""YouTube Data API v3 клиент для Triple Bot.

Авторизация через refresh_token (один раз получен get_yt_token.py локально).
На Render 3 env vars: YT_CLIENT_ID, YT_CLIENT_SECRET, YT_REFRESH_TOKEN.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.readonly",
]


def get_yt_client():
    client_id = os.getenv("YT_CLIENT_ID", "").strip()
    client_secret = os.getenv("YT_CLIENT_SECRET", "").strip()
    refresh_token = os.getenv("YT_REFRESH_TOKEN", "").strip()
    if not all([client_id, client_secret, refresh_token]):
        raise RuntimeError("YT_CLIENT_ID / YT_CLIENT_SECRET / YT_REFRESH_TOKEN не заданы в env")
    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=SCOPES,
    )
    return build("youtube", "v3", credentials=creds, cache_discovery=False)


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
