"""Append-only журнал публикаций для аналитики «какой стиль подписи заходит».

Phase 1: только пишем — style_id, beat_name, tg_message_id, yt_video_id, caption.
Phase 2 (позже): collector раз в неделю обогатит события метриками из YT Analytics.
Phase 3: weekly digest + биасинг выбора стиля.
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


def log_event(**fields: Any) -> None:
    """Append JSON-line в post_events.jsonl. Никогда не кидает — только логирует warning."""
    try:
        record = {"ts": datetime.now(_MSK).isoformat(), **fields}
        with open(EVENTS_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning("post_analytics log_event failed: %s", e)


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
