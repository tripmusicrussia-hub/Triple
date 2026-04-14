"""Журнал продаж битов через Telegram Stars.

Dual-write:
- Supabase `sales` (primary) — если SUPABASE_URL/KEY заданы
- Локальный sales.jsonl (backup) — всегда

Supabase DDL (создать вручную в Supabase Studio):

    create table sales (
        id bigserial primary key,
        ts timestamptz not null,
        buyer_tg_id bigint not null,
        buyer_username text,
        buyer_name text,
        beat_id bigint,
        beat_name text,
        license_type text not null,       -- 'mp3_lease' | 'wav' | 'unlimited' | 'exclusive'
        stars_amount int not null,
        currency text default 'XTR',
        payment_charge_id text unique,    -- telegram_payment_charge_id
        provider_charge_id text,          -- provider_payment_charge_id (для Stars обычно пусто)
        status text default 'completed'   -- completed | refunded
    );
    create index on sales (buyer_tg_id);
    create index on sales (beat_id);
    create index on sales (ts desc);
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Iterator
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))
SALES_PATH = os.path.join(_HERE, "sales.jsonl")
_MSK = ZoneInfo("Europe/Moscow")
_TABLE = "sales"

_supabase = None


def _get_supabase():
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
        logger.info("sales: Supabase client initialized")
        return _supabase
    except Exception as e:
        logger.warning("sales: Supabase init failed (%s), using jsonl only", e)
        _supabase = False
        return None


def log_sale(**fields: Any) -> None:
    """Фиксирует продажу в Supabase + локальный jsonl. Не кидает."""
    record = {"ts": datetime.now(_MSK).isoformat(), **fields}

    try:
        with open(SALES_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning("sales jsonl write failed: %s", e)

    client = _get_supabase()
    if client is not None:
        try:
            client.table(_TABLE).insert(record).execute()
        except Exception as e:
            logger.warning("sales Supabase insert failed: %s", e)


def read_sales() -> Iterator[dict]:
    if not os.path.exists(SALES_PATH):
        return
    with open(SALES_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue
