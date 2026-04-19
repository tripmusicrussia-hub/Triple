"""Conversational agent для админа в ЛС.

LLM-роутер классифицирует свободный текст админа в один из тулов:
  1. recent_posts     — «что постилось за неделю» → Supabase post_events
  2. catalog_search   — «найди биты Am 140+»      → beats_db.BEATS_CACHE
  3. none             — не про эти две штуки
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

logger = logging.getLogger(__name__)

MSK_TZ = ZoneInfo("Europe/Moscow")
HERE = Path(__file__).parent

ROUTER_SYSTEM = """\
Ты — роутер запросов от автора музыкального канала @iiiplfiii. Его речь разговорная, на русском.
Классифицируй запрос в один из 3 тулов и верни ТОЛЬКО JSON, без пояснений и markdown.

Доступные тулы:

1. recent_posts — «что постилось», «последние посты», «что за неделю ушло», «какие биты публиковал».
   args: {"days": <int, по умолчанию 7>}

2. catalog_search — «найди биты», «покажи биты», «есть что в Am 140», «покажи nardo wick», «биты с BPM».
   args: {
     "bpm_min": <int|null>,
     "bpm_max": <int|null>,
     "key":     <str|null>,        // "Am", "G#m", "F#" и т.п., как пишет пользователь
     "artist":  <str|null>,        // подстрока имени артиста, lowercase
     "limit":   <int, по умолчанию 5>
   }

3. none — если запрос не про эти две штуки.
   args: {"reason": "<кратко почему не подошло>"}

Формат ответа СТРОГО:
{"tool": "<имя>", "args": {...}}
"""

_ROUTER_MODELS = [
    "openai/gpt-4o-mini",
    "openai/gpt-oss-120b:free",
    "anthropic/claude-haiku-4.5",
]


async def _call_router_llm(user_text: str) -> str:
    api_key = os.getenv("OPENROUTER_KEY") or os.getenv("OPENROUTER_API_KEY", "")
    if not api_key:
        raise RuntimeError("OPENROUTER_KEY не задан")
    last_err = None
    async with httpx.AsyncClient(timeout=30) as client:
        for model in _ROUTER_MODELS:
            try:
                resp = await client.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                        "HTTP-Referer": "https://github.com/tripmusicrussia-hub/Triple",
                    },
                    json={
                        "model": model,
                        "max_tokens": 200,
                        "temperature": 0.1,
                        "messages": [
                            {"role": "system", "content": ROUTER_SYSTEM},
                            {"role": "user", "content": user_text},
                        ],
                    },
                )
                j = resp.json()
                if "error" in j:
                    last_err = j["error"].get("message", str(j["error"]))
                    continue
                content = j["choices"][0]["message"].get("content") or ""
                if content.strip():
                    return content.strip()
            except Exception as e:
                last_err = str(e)
                logger.warning("router LLM %s exception: %s", model, e)
    raise RuntimeError(f"LLM недоступен: {last_err}")


_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def _parse_route(raw: str) -> dict:
    """Вытащить JSON из ответа LLM (может быть обёрнут в ```json ... ```)."""
    m = _JSON_RE.search(raw)
    if not m:
        raise ValueError(f"нет JSON в ответе: {raw[:200]}")
    return json.loads(m.group(0))


# ─── Тулы ─────────────────────────────────────────────────────────────────────

async def tool_recent_posts(days: int = 7) -> str:
    """Список публикаций за последние N дней. Сначала Supabase, fallback — jsonl."""
    import post_analytics
    cutoff = datetime.now(MSK_TZ) - timedelta(days=days)

    events: list[dict] = []
    client = post_analytics._get_supabase()
    if client is not None:
        try:
            resp = client.table("post_events")\
                .select("*")\
                .gte("ts", cutoff.isoformat())\
                .order("ts", desc=True)\
                .limit(50)\
                .execute()
            events = list(resp.data or [])
        except Exception as e:
            logger.warning("recent_posts: Supabase query failed: %s — fallback jsonl", e)

    if not events:
        for ev in post_analytics.read_events():
            try:
                ts = datetime.fromisoformat(ev.get("ts", "").replace("Z", "+00:00"))
                if ts < cutoff:
                    continue
            except Exception:
                continue
            events.append(ev)
        events = sorted(events, key=lambda e: e.get("ts", ""), reverse=True)[:50]

    if not events:
        return f"Пусто — за последние {days} дн. публикаций не записано."

    lines = [f"📊 За последние {days} дн. — {len(events)} публикаций:\n"]
    for ev in events[:15]:
        ts_raw = ev.get("ts", "")
        try:
            dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).astimezone(MSK_TZ)
            ts_disp = dt.strftime("%d.%m %H:%M")
        except Exception:
            ts_disp = ts_raw[:16]
        beat = ev.get("beat_name") or "—"
        style = ev.get("style") or "?"
        bpm = ev.get("bpm")
        key = ev.get("key") or ""
        meta = f"{bpm} {key}".strip() if bpm else ""
        yt = " 📺" if ev.get("yt_video_id") else ""
        tg = " 💬" if ev.get("tg_message_id") else ""
        lines.append(f"• {ts_disp} — {beat} [{style}]{yt}{tg} {meta}".rstrip())
    if len(events) > 15:
        lines.append(f"\n… и ещё {len(events) - 15}")
    return "\n".join(lines)


_KEY_RE = re.compile(r"\b([A-G][#b]?m?)\b", re.IGNORECASE)


def _extract_key(beat: dict) -> str:
    """Вытаскивает ключ из beat['key'] или из title/tags."""
    k = beat.get("key") or ""
    if k:
        return k
    for src in (beat.get("name") or "", " ".join(beat.get("tags") or [])):
        m = _KEY_RE.search(src)
        if m:
            return m.group(1)
    return ""


def _key_matches(beat_key: str, query_key: str) -> bool:
    a = beat_key.lower().replace(" ", "")
    b = query_key.lower().replace(" ", "")
    return bool(a) and (b in a or a in b)


async def tool_catalog_search(
    bpm_min: int | None = None,
    bpm_max: int | None = None,
    key: str | None = None,
    artist: str | None = None,
    limit: int = 5,
) -> str:
    """Фильтрация BEATS_CACHE по параметрам."""
    import beats_db
    if not beats_db.BEATS_CACHE:
        beats_db.load_beats()

    limit = max(1, min(int(limit or 5), 20))
    artist_lc = (artist or "").strip().lower()
    key_q = (key or "").strip()

    results = []
    for b in beats_db.BEATS_CACHE:
        if b.get("content_type") != "beat":
            continue
        bpm = b.get("bpm")
        if bpm_min is not None and (bpm is None or bpm < bpm_min):
            continue
        if bpm_max is not None and (bpm is None or bpm > bpm_max):
            continue
        if key_q and not _key_matches(_extract_key(b), key_q):
            continue
        if artist_lc:
            haystack = (
                (b.get("name") or "").lower()
                + " "
                + " ".join(b.get("tags") or []).lower()
            )
            if artist_lc not in haystack:
                continue
        results.append(b)

    if not results:
        crit = []
        if bpm_min is not None or bpm_max is not None:
            crit.append(f"BPM {bpm_min or '?'}-{bpm_max or '?'}")
        if key_q:
            crit.append(f"key={key_q}")
        if artist_lc:
            crit.append(f"artist≈{artist_lc}")
        return f"🔍 Ничего не нашёл ({', '.join(crit) if crit else 'без фильтров'})."

    results = results[:limit]
    lines = [f"🔍 Найдено: {len(results)}"]
    for b in results:
        name = b.get("name", "—")
        bpm = b.get("bpm") or "?"
        bkey = _extract_key(b) or "?"
        tags = ", ".join((b.get("tags") or [])[:3])
        extra = f" · #{tags}" if tags else ""
        lines.append(f"• {name} — {bpm} BPM · {bkey}{extra}")
    return "\n".join(lines)


# ─── Основной entry-point ─────────────────────────────────────────────────────

TOOLS = {
    "recent_posts": tool_recent_posts,
    "catalog_search": tool_catalog_search,
}


async def handle(user_text: str) -> str:
    """Принимает свободный текст → LLM-роутер → dispatch → текст ответа."""
    user_text = (user_text or "").strip()
    if not user_text:
        return ""
    try:
        raw = await _call_router_llm(user_text)
        route = _parse_route(raw)
    except Exception as e:
        logger.warning("router failed: %s", e)
        return f"❌ LLM-роутер недоступен: {str(e)[:200]}"

    tool_name = route.get("tool", "none")
    args = route.get("args") or {}
    logger.info("agent route: tool=%s args=%s", tool_name, args)

    if tool_name == "none":
        reason = args.get("reason", "запрос не про текущие 3 тула")
        return (
            f"🤔 Не понял как помочь: {reason}.\n\n"
            "Сейчас умею:\n"
            "• «что постилось за неделю» — список последних публикаций\n"
            "• «что сегодня в канал» — рубрика и материал на сегодня\n"
            "• «найди биты Am 140+» — поиск по каталогу"
        )

    fn = TOOLS.get(tool_name)
    if fn is None:
        return f"⚠️ Неизвестный тул от роутера: {tool_name}"

    try:
        return await fn(**args)
    except TypeError as e:
        logger.warning("tool %s bad args %s: %s", tool_name, args, e)
        return f"⚠️ Тул {tool_name} получил неверные аргументы: {str(e)[:150]}"
    except Exception as e:
        logger.exception("tool %s failed", tool_name)
        return f"❌ Ошибка в туле {tool_name}: {str(e)[:200]}"


if __name__ == "__main__":
    import asyncio
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    async def _main():
        queries = sys.argv[1:] or [
            "что постилось за неделю",
            "что сегодня в канал",
            "найди биты Am 140+",
            "покажи nardo wick битов",
            "как дела бро",
        ]
        for q in queries:
            print("=" * 60)
            print(f"Q: {q}")
            print("-" * 60)
            print(await handle(q))

    asyncio.run(_main())
