"""Журнал публикаций для аналитики «какой стиль подписи заходит».

Dual-write:
- Supabase `post_events` (primary) — если SUPABASE_URL/KEY заданы
- Локальный post_events.jsonl (backup) — всегда, не теряемся если Supabase лёг
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Iterator
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

STYLE_LABELS = ["short_hook", "minimal", "storytelling", "question", "emotional"]
FALLBACK_LABEL = "fallback"

_HERE = os.path.dirname(os.path.abspath(__file__))
EVENTS_PATH = os.path.join(_HERE, "post_events.jsonl")
_MSK = ZoneInfo("Europe/Moscow")
_TABLE = "post_events"

_supabase = None


def _get_supabase():
    """Lazy-init клиент. Возвращает None если env не задан или supabase не установлен."""
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
        logger.info("post_analytics: Supabase client initialized")
        return _supabase
    except Exception as e:
        logger.warning("post_analytics: Supabase init failed (%s), using jsonl only", e)
        _supabase = False
        return None


def log_event(**fields: Any) -> None:
    """Пишет событие в Supabase (если доступен) + локальный jsonl backup.
    Никогда не кидает — оба канала изолированы try/except."""
    record = {"ts": datetime.now(_MSK).isoformat(), **fields}

    try:
        with open(EVENTS_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning("post_analytics jsonl write failed: %s", e)

    client = _get_supabase()
    if client is not None:
        try:
            client.table(_TABLE).insert(record).execute()
        except Exception as e:
            logger.warning("post_analytics Supabase insert failed: %s", e)


def read_events() -> Iterator[dict]:
    if not os.path.exists(EVENTS_PATH):
        return
    with open(EVENTS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue
