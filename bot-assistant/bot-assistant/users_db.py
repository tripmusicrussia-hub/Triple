"""Persistence для пользователей бота (замена `users_data.json`).

Dual-write:
- Supabase table `bot_users` (primary) — если SUPABASE_URL / SUPABASE_KEY заданы
- Локальный users_data.json (fallback) — всегда, через save_local()

Почему понадобилось: Render free tier имеет эфемерный диск — при каждом
redeploy контейнер свежий, локальный файл пропадает. Пользователи
забывались, каждый /start шёл как "новый", sample pack раздавался заново.

Supabase DDL (создать вручную в Supabase Studio → SQL Editor):

    create table if not exists bot_users (
        tg_id bigint primary key,
        username text,
        full_name text,
        joined_at timestamptz not null default now(),
        received_sample_pack boolean not null default false,
        is_subscribed boolean not null default false,
        favorites jsonb not null default '[]'::jsonb,
        updated_at timestamptz not null default now()
    );
    create index if not exists bot_users_joined_idx on bot_users (joined_at desc);

API:
- upsert_user(tg_id, full_name, username) -> is_new : True если раньше не было
- mark_sample_pack_received(tg_id)
- has_received_sample_pack(tg_id) -> bool
- mark_subscribed(tg_id, flag=True)
- add_favorite(tg_id, beat_id) / remove_favorite / get_favorites
- count_users() / count_sample_pack_received()
- load_to_memory() -> (all_users, users_received_pack, subscribed_users,
                       user_favorites) — для in-memory кеша bot.py
- save_local(data) — snapshot JSON как fallback

in-memory кэш в bot.py остаётся для hot-path (быстрое чтение).
Все мутации: write-through в Supabase + мгновенный update кэша + local JSON snapshot.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))
USERS_LOCAL_PATH = os.path.join(_HERE, "users_data.json")
_MSK = ZoneInfo("Europe/Moscow")
_TABLE = "bot_users"

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
        logger.info("users_db: Supabase client initialized")
        return _supabase
    except Exception as e:
        logger.warning("users_db: Supabase init failed (%s), using local-only", e)
        _supabase = False
        return None


# ─── Writes ──────────────────────────────────────────────────────────────────

def upsert_user(tg_id: int, full_name: str, username: str | None) -> bool:
    """Upsert юзера. Возвращает True если раньше не было (is_new).

    На Supabase: SELECT → если нет, INSERT (is_new=True). Если есть —
    UPDATE username/full_name (но не joined_at и не received_sample_pack).
    Без Supabase: только in-memory (вызывающий сам решит persistence).
    """
    client = _get_supabase()
    if client is None:
        # In-memory-only режим: cache в bot.py сам управляет
        return False  # неизвестно, вернём False для безопасности (будет видно в логах)
    try:
        res = client.table(_TABLE).select("tg_id").eq("tg_id", tg_id).execute()
        existing = bool(res.data)
        payload = {
            "tg_id": tg_id,
            "full_name": full_name,
            "username": username or "",
            "updated_at": datetime.now(_MSK).isoformat(),
        }
        if not existing:
            payload["joined_at"] = datetime.now(_MSK).isoformat()
            client.table(_TABLE).insert(payload).execute()
            return True
        # Существующий — обновляем только имя/username
        client.table(_TABLE).update({
            "full_name": full_name,
            "username": username or "",
            "updated_at": datetime.now(_MSK).isoformat(),
        }).eq("tg_id", tg_id).execute()
        return False
    except Exception as e:
        logger.warning("users_db.upsert_user(%d) failed: %s", tg_id, e)
        return False


def mark_sample_pack_received(tg_id: int) -> None:
    """Проставить флаг что юзер получил sample_pack. Идемпотентно."""
    client = _get_supabase()
    if client is None:
        return
    try:
        client.table(_TABLE).update({
            "received_sample_pack": True,
            "updated_at": datetime.now(_MSK).isoformat(),
        }).eq("tg_id", tg_id).execute()
    except Exception as e:
        logger.warning("users_db.mark_sample_pack_received(%d) failed: %s", tg_id, e)


def has_received_sample_pack(tg_id: int) -> bool | None:
    """True/False — получал ли юзер sample pack. None — если Supabase недоступен
    (вызывающий должен fallback-нуться на in-memory флаг).
    """
    client = _get_supabase()
    if client is None:
        return None
    try:
        res = client.table(_TABLE).select("received_sample_pack").eq("tg_id", tg_id).execute()
        if res.data:
            return bool(res.data[0].get("received_sample_pack"))
        return False
    except Exception as e:
        logger.warning("users_db.has_received_sample_pack(%d) failed: %s", tg_id, e)
        return None


def set_favorites(tg_id: int, favorites: list[int]) -> None:
    """Перезаписывает список избранных бит-id для юзера."""
    client = _get_supabase()
    if client is None:
        return
    try:
        client.table(_TABLE).update({
            "favorites": favorites,
            "updated_at": datetime.now(_MSK).isoformat(),
        }).eq("tg_id", tg_id).execute()
    except Exception as e:
        logger.warning("users_db.set_favorites(%d) failed: %s", tg_id, e)


def set_subscribed(tg_id: int, flag: bool) -> None:
    client = _get_supabase()
    if client is None:
        return
    try:
        client.table(_TABLE).update({
            "is_subscribed": flag,
            "updated_at": datetime.now(_MSK).isoformat(),
        }).eq("tg_id", tg_id).execute()
    except Exception as e:
        logger.warning("users_db.set_subscribed(%d) failed: %s", tg_id, e)


# ─── Reads / bulk ────────────────────────────────────────────────────────────

def load_to_memory() -> tuple[dict[int, dict], set[int], set[int], dict[int, list[int]]]:
    """Загружает всех юзеров из Supabase → возвращает in-memory структуры
    bot.py: (all_users, users_received_pack, subscribed_users, user_favorites).

    Если Supabase недоступен — пробует прочитать локальный JSON снэпшот
    (USERS_LOCAL_PATH). Если и его нет — пустые коллекции.
    """
    client = _get_supabase()
    if client is not None:
        try:
            res = client.table(_TABLE).select("*").execute()
            all_users: dict[int, dict] = {}
            received: set[int] = set()
            subscribed: set[int] = set()
            favorites: dict[int, list[int]] = {}
            for row in (res.data or []):
                tid = int(row["tg_id"])
                all_users[tid] = {
                    "name": row.get("full_name") or "",
                    "username": row.get("username") or "",
                    "joined": row.get("joined_at") or "",
                }
                if row.get("received_sample_pack"):
                    received.add(tid)
                if row.get("is_subscribed"):
                    subscribed.add(tid)
                fav = row.get("favorites") or []
                if fav:
                    favorites[tid] = list(fav)
            logger.info("users_db: loaded %d users from Supabase", len(all_users))
            return all_users, received, subscribed, favorites
        except Exception as e:
            logger.warning("users_db.load_to_memory Supabase read failed, falling back to local: %s", e)

    # Fallback: local JSON
    if os.path.exists(USERS_LOCAL_PATH):
        try:
            with open(USERS_LOCAL_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            all_users = {int(k): v for k, v in data.get("all_users", {}).items()}
            received = set(int(x) for x in data.get("users_received_pack", []))
            subscribed = set(int(x) for x in data.get("subscribed_users", []))
            favorites = {int(k): v for k, v in data.get("user_favorites", {}).items()}
            logger.info("users_db: loaded %d users from local file (Supabase unavailable)", len(all_users))
            return all_users, received, subscribed, favorites
        except Exception as e:
            logger.error("users_db: local JSON load failed: %s", e)

    return {}, set(), set(), {}


def save_local(all_users: dict, users_received_pack: set, subscribed_users: set,
               user_favorites: dict) -> None:
    """Атомарный snapshot в local JSON — fallback, если Supabase недоступен.
    Вызывается вспомогательно для backup/debug. Supabase — primary.
    """
    tmp = USERS_LOCAL_PATH + ".tmp"
    try:
        data = {
            "all_users": {str(k): v for k, v in all_users.items()},
            "users_received_pack": list(users_received_pack),
            "subscribed_users": list(subscribed_users),
            "user_favorites": {str(k): v for k, v in user_favorites.items()},
        }
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(tmp, USERS_LOCAL_PATH)
    except Exception as e:
        logger.error("users_db.save_local failed: %s", e)
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def count_users() -> int:
    """Количество всех юзеров. Supabase HEAD count если доступен."""
    client = _get_supabase()
    if client is None:
        return 0
    try:
        res = client.table(_TABLE).select("tg_id", count="exact").execute()
        return res.count or 0
    except Exception as e:
        logger.warning("users_db.count_users failed: %s", e)
        return 0
