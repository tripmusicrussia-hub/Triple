"""Планировщик отложенной публикации битов на YT+TG.

Storage strategy (Supabase primary, JSON fallback):
- Metadata: table `scheduled_uploads` в Supabase (переживает deploy Render free tier)
- Files (mp3/mp4/jpg): bucket `scheduled-uploads` в Supabase Storage
- Local JSON `scheduled_uploads.json` — secondary backup если Supabase недоступен

Optimal slots (per data-analysis 2026-04-18):
- Fri 21:30 МСК ±5 мин — primary (40% топ-видео)
- Mon 21:00 МСК ±5 мин — secondary
Выбираем ближайший свободный.

API:
  enqueue(payload, actions)          → upload files + insert row, возвращает publish_at
  load_queue()                       → SELECT pending + download files в temp_uploads
  due_items()                        → items с publish_at <= now
  mark_published(token)              → UPDATE status + remove files
  cancel(token)                      → UPDATE status='cancelled' + remove files
"""
from __future__ import annotations

import json
import logging
import mimetypes
import os
import random
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

HERE = Path(__file__).parent
QUEUE_PATH = HERE / "scheduled_uploads.json"
TEMP_UPLOAD_DIR = HERE / "temp_uploads"

MSK_TZ = timezone(timedelta(hours=3))
SB_TABLE = "scheduled_uploads"
SB_BUCKET = "scheduled-uploads"

# Optimal slots — настраиваются через env PUBLISH_OPTIMAL_SLOTS, дефолт
# Fri 21:30 + Mon 21:00 МСК (из анализа окон type-beat каналов).
from config import PUBLISH_OPTIMAL_SLOTS as OPTIMAL_SLOTS
JITTER_MINUTES = 5

_QUEUE: list[dict] = []
_supabase = None


# ── Supabase client (lazy, graceful) ────────────────────────────────

def _get_supabase():
    """Lazy-init. Возвращает None если env не задан/supabase не доступен."""
    global _supabase
    if _supabase is not None:
        return _supabase if _supabase is not False else None
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_KEY", "").strip()
    if not url or not key:
        _supabase = False
        return None
    try:
        from supabase import create_client
        _supabase = create_client(url, key)
        logger.info("publish_scheduler: Supabase client initialized")
        print("[SCHED] _get_supabase: client initialized", flush=True)
        return _supabase
    except Exception as e:
        print(f"[SCHED] _get_supabase: init FAILED: {type(e).__name__}: {e}", flush=True)
        logger.warning("publish_scheduler: Supabase init failed (%s), JSON-only mode", e)
        _supabase = False
        return None


# ── Serialization helpers ───────────────────────────────────────────

def _serialize(obj):
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"not serializable: {type(obj).__name__}")


def _save_queue():
    """Backup JSON на диск (secondary — теряется при Render deploy, но useful local dev)."""
    try:
        QUEUE_PATH.write_text(
            json.dumps(_QUEUE, default=_serialize, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        logger.exception("save_queue failed")


def _parse_dt(s: str) -> datetime:
    return datetime.fromisoformat(s)


# ── Storage bucket ops ──────────────────────────────────────────────

def _bucket_key(token: str, kind: str) -> str:
    """Storage path: 'scheduled-uploads/<token>/audio.mp3' etc."""
    ext = {"mp3": "mp3", "video": "mp4", "thumb": "jpg"}[kind]
    return f"{token}/{kind}.{ext}"


def _upload_files_to_storage(token: str, paths: dict[str, Path]) -> bool:
    """Uploads mp3/video/thumb в Storage bucket. Возвращает True если все 3 залились."""
    sb = _get_supabase()
    if sb is None:
        return False
    ok = 0
    for kind, path in paths.items():
        if not path or not path.exists():
            logger.warning("storage upload skip: %s not found at %s", kind, path)
            continue
        try:
            with open(path, "rb") as f:
                data = f.read()
            mime, _ = mimetypes.guess_type(str(path))
            sb.storage.from_(SB_BUCKET).upload(
                _bucket_key(token, kind),
                data,
                {"content-type": mime or "application/octet-stream", "upsert": "true"},
            )
            ok += 1
        except Exception as e:
            logger.warning("storage upload fail %s/%s: %s", token, kind, e)
    logger.info("publish_scheduler: uploaded %d/3 files for %s", ok, token)
    return ok == 3


def _download_files_from_storage(token: str) -> dict[str, Path]:
    """Скачивает 3 файла в temp_uploads/<token>.{mp3,mp4,jpg}. Возвращает пути."""
    sb = _get_supabase()
    TEMP_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    out = {}
    ext_map = {"mp3": "mp3", "video": "mp4", "thumb": "jpg"}
    for kind, ext in ext_map.items():
        dst = TEMP_UPLOAD_DIR / f"{token}.{ext}"
        if dst.exists() and dst.stat().st_size > 0:
            out[kind] = dst  # уже есть локально (например скачали раньше)
            continue
        if sb is None:
            continue
        try:
            data = sb.storage.from_(SB_BUCKET).download(_bucket_key(token, kind))
            dst.write_bytes(data)
            out[kind] = dst
        except Exception as e:
            logger.warning("storage download fail %s/%s: %s", token, kind, e)
    logger.info("publish_scheduler: downloaded %d files for %s", len(out), token)
    return out


def _remove_files_from_storage(token: str):
    """Удаляет 3 файла из bucket (cleanup после publish/cancel)."""
    sb = _get_supabase()
    if sb is None:
        return
    try:
        keys = [_bucket_key(token, k) for k in ("mp3", "video", "thumb")]
        sb.storage.from_(SB_BUCKET).remove(keys)
    except Exception as e:
        logger.warning("storage remove fail %s: %s", token, e)


# ── Queue operations ────────────────────────────────────────────────

def load_queue() -> int:
    """Восстанавливает _QUEUE на startup.

    Primary: SELECT pending из Supabase, download files в temp_uploads.
    Fallback: local JSON (only useful during local dev).
    """
    global _QUEUE
    print("[SCHED] load_queue() called", flush=True)
    _QUEUE = []
    sb = _get_supabase()
    print(f"[SCHED] sb client = {sb!r}", flush=True)
    if sb is not None:
        try:
            resp = sb.table(SB_TABLE).select("*").eq("status", "pending").execute()
            rows = resp.data or []
            print(f"[SCHED] SELECT returned {len(rows)} rows", flush=True)
            for row in rows:
                # Mapping DB columns → queue item format
                item = {
                    "token": row["token"],
                    "publish_at": row["publish_at"],
                    "actions": row.get("actions") or ["yt", "tg"],
                    "meta": row.get("meta") or {},
                    "yt_post": row.get("yt_post") or {},
                    "tg_caption": row.get("tg_caption") or "",
                    "tg_style": row.get("tg_style") or "scheduled",
                    "tg_file_id": row.get("tg_file_id") or "",
                    "reserved_beat_id": row.get("reserved_beat_id"),
                    "enqueued_at": row.get("enqueued_at"),
                }
                # Download files (ephemeral temp_uploads/)
                files = _download_files_from_storage(row["token"])
                item["mp3_path"] = str(files.get("mp3", TEMP_UPLOAD_DIR / f"{row['token']}.mp3"))
                item["video_path"] = str(files.get("video", TEMP_UPLOAD_DIR / f"{row['token']}.mp4"))
                item["thumb_path"] = str(files.get("thumb", TEMP_UPLOAD_DIR / f"{row['token']}.jpg"))
                _QUEUE.append(item)
            logger.info("publish_scheduler: loaded %d items from Supabase", len(_QUEUE))
            print(f"[SCHED] returning {len(_QUEUE)} items from Supabase branch", flush=True)
            _save_queue()  # sync to local JSON for visibility
            return len(_QUEUE)
        except Exception as e:
            print(f"[SCHED] EXCEPTION: {type(e).__name__}: {e}", flush=True)
            logger.exception("publish_scheduler: Supabase load failed (%s), trying JSON fallback", e)

    # Fallback: local JSON (эта ветка работает только локально)
    if QUEUE_PATH.exists():
        try:
            _QUEUE = json.loads(QUEUE_PATH.read_text(encoding="utf-8"))
            _QUEUE = [q for q in _QUEUE if isinstance(q, dict) and q.get("token") and q.get("publish_at")]
            logger.info("publish_scheduler: loaded %d items from JSON fallback", len(_QUEUE))
        except Exception:
            logger.exception("load_queue JSON parse failed")
            _QUEUE = []
    return len(_QUEUE)


def _upcoming_slot_candidates(now: datetime, weeks_lookahead: int = 4) -> list[datetime]:
    slots = []
    for week in range(weeks_lookahead):
        for wday, hour, minute in OPTIMAL_SLOTS:
            days_ahead = (wday - now.weekday()) % 7 + week * 7
            s = now.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=days_ahead)
            if s > now:
                slots.append(s)
    return sorted(slots)


def next_optimal_slot(after: datetime | None = None) -> datetime:
    now = after or datetime.now(MSK_TZ)
    for candidate in _upcoming_slot_candidates(now):
        occupied = any(
            abs(_parse_dt(q["publish_at"]) - candidate) < timedelta(hours=6)
            for q in _QUEUE
        )
        if not occupied:
            jitter = random.randint(-JITTER_MINUTES, JITTER_MINUTES)
            return candidate + timedelta(minutes=jitter)
    return now + timedelta(days=7)


def enqueue(payload: dict, actions: list[str]) -> datetime:
    """Планирует upload. Primary path: Supabase row + Storage files. Fallback: JSON-only."""
    publish_at = next_optimal_slot()
    token = payload["token"]

    # Paths для local reference (temp_uploads/) — Storage использует _bucket_key
    item = {
        "token": token,
        "publish_at": publish_at.isoformat(),
        "actions": actions,
        "mp3_path": str(payload["mp3_path"]),
        "video_path": str(payload["video_path"]),
        "thumb_path": str(payload["thumb_path"]),
        "meta": asdict(payload["meta"]),
        "yt_post": asdict(payload["yt_post"]),
        "tg_caption": payload["tg_caption"],
        "tg_style": payload.get("tg_style", "scheduled"),
        "tg_file_id": payload["tg_file_id"],
        "reserved_beat_id": payload.get("reserved_beat_id"),
        "enqueued_at": datetime.now(MSK_TZ).isoformat(),
    }
    _QUEUE.append(item)

    # Primary: Supabase
    sb = _get_supabase()
    if sb is not None:
        # Upload files first (если row есть а файлов нет — scheduler будет пытаться вечно)
        files_ok = _upload_files_to_storage(token, {
            "mp3": Path(payload["mp3_path"]),
            "video": Path(payload["video_path"]),
            "thumb": Path(payload["thumb_path"]),
        })
        try:
            sb.table(SB_TABLE).insert({
                "token": token,
                "publish_at": item["publish_at"],
                "actions": actions,
                "meta": item["meta"],
                "yt_post": item["yt_post"],
                "tg_caption": item["tg_caption"],
                "tg_style": item["tg_style"],
                "tg_file_id": item["tg_file_id"],
                "reserved_beat_id": item["reserved_beat_id"],
                "status": "pending",
                "enqueued_at": item["enqueued_at"],
            }).execute()
            logger.info("publish_scheduler: enqueued %s → Supabase (files %s)",
                        token, "OK" if files_ok else "partial")
        except Exception:
            logger.exception("publish_scheduler: Supabase insert failed, JSON-only backup")

    # Secondary: JSON
    _save_queue()
    logger.info("publish_scheduler: enqueued %s → %s (actions=%s)",
                token, publish_at.strftime("%Y-%m-%d %H:%M МСК"), ",".join(actions))
    return publish_at


def due_items() -> list[dict]:
    now = datetime.now(MSK_TZ)
    return [q for q in _QUEUE if _parse_dt(q["publish_at"]) <= now]


def mark_published(token: str):
    """После успешной публикации: UPDATE status + remove files from Storage."""
    global _QUEUE
    before = len(_QUEUE)
    _QUEUE = [q for q in _QUEUE if q.get("token") != token]
    if len(_QUEUE) < before:
        _save_queue()
    sb = _get_supabase()
    if sb is not None:
        try:
            sb.table(SB_TABLE).update({
                "status": "published",
                "published_at": datetime.now(MSK_TZ).isoformat(),
            }).eq("token", token).execute()
        except Exception:
            logger.exception("mark_published: Supabase UPDATE failed for %s", token)
    _remove_files_from_storage(token)
    logger.info("publish_scheduler: dequeued %s (published)", token)


def cancel(token: str) -> bool:
    """Отменяет плановую публикацию. True если нашли и удалили."""
    global _QUEUE
    before = len(_QUEUE)
    _QUEUE = [q for q in _QUEUE if q.get("token") != token]
    found = len(_QUEUE) < before
    _save_queue()
    sb = _get_supabase()
    if sb is not None:
        try:
            resp = sb.table(SB_TABLE).update({"status": "cancelled"}).eq("token", token).execute()
            if resp.data:
                found = True
        except Exception:
            logger.exception("cancel: Supabase UPDATE failed for %s", token)
    if found:
        _remove_files_from_storage(token)
    return found


def queue_summary() -> list[str]:
    out = []
    for q in sorted(_QUEUE, key=lambda x: x["publish_at"]):
        dt = _parse_dt(q["publish_at"])
        meta = q.get("meta", {})
        name = meta.get("name", "?")
        artist = meta.get("artist_display", "?")
        actions = "+".join(q.get("actions", []))
        token = q.get("token", "?")
        beat_id = q.get("reserved_beat_id", "?")
        out.append(
            f"📅 {dt.strftime('%a %d %b %H:%M МСК')}  {name} — {artist}  [{actions}]\n"
            f"   id={beat_id} · token=<code>{token}</code>"
        )
    return out


def queue_size() -> int:
    return len(_QUEUE)


def is_scheduled(token: str) -> bool:
    return any(q.get("token") == token for q in _QUEUE)
