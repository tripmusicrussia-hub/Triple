"""Планировщик отложенной публикации битов на YT+TG.

Логика (per data-analysis 2026-04-18):
- **Optimal slot:** пятница 21:30 МСК ±5 мин (40% топ-видео в нише)
- Очередь сохраняется на диск → переживает restart'ы Render
- Scheduler task каждые 60с проверяет queue, публикует когда время

При планировании:
  publish_scheduler.enqueue(payload, actions=['yt','tg']) → возвращает publish_at datetime

При старте бота:
  publish_scheduler.load_queue() — восстанавливает из disk

В scheduler-loop'е:
  publish_scheduler.due_items() → list of payloads готовых к публикации
  publish_scheduler.mark_published(token) → удаляет из очереди
"""
from __future__ import annotations

import json
import logging
import random
from dataclasses import asdict, is_dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

HERE = Path(__file__).parent
QUEUE_PATH = HERE / "scheduled_uploads.json"

MSK_TZ = timezone(timedelta(hours=3))

# Optimal slots — по анализу топ-35 видео 2026-04-18:
# - Fri: 368k avg views (топ), UTC 19 (МСК 22) — пик в пятницу вечером
# - Mon: 155k avg views (второй) — старт недели
# Выбираем ближайший свободный из этих двух.
OPTIMAL_SLOTS = [
    (4, 21, 30),  # (weekday 0=Mon..6=Sun, hour, minute) — Fri 21:30 МСК (primary)
    (0, 21, 0),   # Mon 21:00 МСК (secondary)
]
JITTER_MINUTES = 5  # ±5 мин random jitter для естественности

_QUEUE: list[dict] = []


def _serialize(obj):
    """Поддержка dataclass'ов (BeatMeta, YTPost) → JSON."""
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"not serializable: {type(obj).__name__}")


def _save_queue():
    try:
        QUEUE_PATH.write_text(
            json.dumps(_QUEUE, default=_serialize, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        logger.exception("save_queue failed")


def load_queue() -> int:
    """Загружает очередь с диска. Возвращает count item'ов."""
    global _QUEUE
    if not QUEUE_PATH.exists():
        _QUEUE = []
        return 0
    try:
        _QUEUE = json.loads(QUEUE_PATH.read_text(encoding="utf-8"))
        # Валидация: каждый item должен иметь token + publish_at + paths
        _QUEUE = [q for q in _QUEUE if isinstance(q, dict) and q.get("token") and q.get("publish_at")]
        logger.info("publish_scheduler: loaded %d queued items", len(_QUEUE))
    except Exception:
        logger.exception("load_queue failed — starting empty")
        _QUEUE = []
    return len(_QUEUE)


def _upcoming_slot_candidates(now: datetime, weeks_lookahead: int = 4) -> list[datetime]:
    """Генерит все optimal-слоты на ближайшие N недель отсортированные по времени."""
    slots = []
    for week in range(weeks_lookahead):
        for wday, hour, minute in OPTIMAL_SLOTS:
            days_ahead = (wday - now.weekday()) % 7 + week * 7
            s = now.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=days_ahead)
            if s > now:
                slots.append(s)
    return sorted(slots)


def next_optimal_slot(after: datetime | None = None) -> datetime:
    """Ближайший свободный optimal-слот (Пт 21:30 / Пн 21:00 МСК) + jitter ±5 мин.

    Учитывает занятые слоты в очереди — пропускает, берёт следующий.
    При отсутствии новых битов в очереди scheduler просто ждёт — ничего не
    публикуется автоматом, нужен upload юзера.
    """
    now = after or datetime.now(MSK_TZ)
    for candidate in _upcoming_slot_candidates(now):
        # Слот свободен если в очереди нет публикации в окне ±6 часов
        occupied = any(
            abs(_parse_dt(q["publish_at"]) - candidate) < timedelta(hours=6)
            for q in _QUEUE
        )
        if not occupied:
            jitter = random.randint(-JITTER_MINUTES, JITTER_MINUTES)
            return candidate + timedelta(minutes=jitter)
    # Fallback — должно не случиться при 4 неделях lookahead
    return now + timedelta(days=7)


def _parse_dt(s: str) -> datetime:
    return datetime.fromisoformat(s)


def enqueue(payload: dict, actions: list[str]) -> datetime:
    """Добавляет upload в очередь на ближайший optimal-слот.

    payload — словарь из pending_uploads (meta, пути, yt_post, tg_caption...)
    actions — ['yt'], ['tg'], или ['yt','tg']

    Возвращает datetime публикации (MSK).
    """
    publish_at = next_optimal_slot()
    item = {
        "token": payload["token"],
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
    _save_queue()
    logger.info("publish_scheduler: enqueued %s → %s (actions=%s)",
                payload["token"], publish_at.strftime("%Y-%m-%d %H:%M МСК"),
                ",".join(actions))
    return publish_at


def due_items() -> list[dict]:
    """Возвращает список item'ов, у которых publish_at <= now."""
    now = datetime.now(MSK_TZ)
    return [q for q in _QUEUE if _parse_dt(q["publish_at"]) <= now]


def mark_published(token: str):
    """Удаляет item из очереди после успешной публикации."""
    global _QUEUE
    before = len(_QUEUE)
    _QUEUE = [q for q in _QUEUE if q.get("token") != token]
    if len(_QUEUE) < before:
        _save_queue()
        logger.info("publish_scheduler: dequeued %s", token)


def cancel(token: str) -> bool:
    """Отменяет плановую публикацию. Возвращает True если нашли и удалили."""
    global _QUEUE
    before = len(_QUEUE)
    _QUEUE = [q for q in _QUEUE if q.get("token") != token]
    if len(_QUEUE) < before:
        _save_queue()
        return True
    return False


def queue_summary() -> list[str]:
    """Короткий список с token'ами для /cancel_sched."""
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
