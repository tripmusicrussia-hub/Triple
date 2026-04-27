"""YT title optimizer через LLM + yt-dlp top-10 scan.

Use case: legacy YT-видео с слабым title → юзер запускает /yt_titles <video_id>
→ бот находит топ-10 в нише через yt-dlp → передаёт в LLM как примеры паттернов
→ LLM генерит 3 альтернативы → юзер выбирает → bot.update_video.

Не зависит от sync/async — все функции синхронные кроме LLM call (он
asyncio из post_generator).
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

YT_DLP_TIMEOUT_SEC = 30  # yt-dlp может зависнуть на slow YT — таймаут защищает


def scrape_top10_for_beat(artist_display: str, year: int = 2026,
                          n: int = 10) -> list[dict]:
    """Возвращает топ-N type-beat видео по запросу `<artist> type beat <year>`.

    Каждый dict: {title, views, channel, url, duration}. Sorted by relevance
    (yt-dlp default — YT search ranking).

    Может вернуть [] если yt-dlp недоступен / network error / search блок.
    Caller должен gracefully обрабатывать пустой list.
    """
    try:
        from yt_dlp import YoutubeDL
    except ImportError:
        logger.warning("yt_strategy: yt-dlp не установлен")
        return []

    query = f"{artist_display} type beat {year}"
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": True,
        "playlistend": n,
        "socket_timeout": YT_DLP_TIMEOUT_SEC,
        "default_search": "ytsearch",
    }
    try:
        with YoutubeDL(opts) as ydl:
            result = ydl.extract_info(f"ytsearch{n}:{query}", download=False)
    except Exception as e:
        logger.warning("yt_strategy: scrape failed for %r: %s", query, e)
        return []

    entries = (result or {}).get("entries") or []
    out: list[dict] = []
    for e in entries:
        if not e:
            continue
        out.append({
            "title": e.get("title", "") or "",
            "views": int(e.get("view_count") or 0),
            "channel": e.get("channel", "") or e.get("uploader", "") or "",
            "url": e.get("url", "") or "",
            "duration": int(e.get("duration") or 0),
        })
    return out


def build_title_optimizer_prompt(
    artist_display: str,
    current_title: str,
    top10: list[dict],
    bpm: int | None = None,
    key_short: str | None = None,
    scene: str | None = None,
) -> str:
    """Строит prompt для LLM на основе current title + competitive examples.

    LLM попросят вернуть JSON с 3 alternative titles + rationale (почему
    каждый увеличит CTR). Длина каждого ≤95 символов (YT лимит 100, запас 5).
    """
    # Top-10 examples — title + views (релевантность) + channel
    examples_block = "\n".join(
        f"  {i+1}. \"{(e['title'] or '?')[:90]}\" — {e['views']:,} views ({e['channel']})"
        for i, e in enumerate(top10[:10])
    ) if top10 else "  (нет данных — генерируй чисто на основе current title)"

    meta_block = []
    if bpm:
        meta_block.append(f"BPM: {bpm}")
    if key_short:
        meta_block.append(f"Key: {key_short}")
    if scene:
        meta_block.append(f"Scene: {scene}")
    meta_str = " · ".join(meta_block) if meta_block else "(не указан)"

    return f"""Ты — YouTube title optimizer для type-beat канала.

CURRENT TITLE: "{current_title}"

TOP-10 в нише "{artist_display} type beat" (для понимания паттернов):
{examples_block}

Beat metadata: {meta_str}

Предложи 3 ALTERNATIVE TITLE которые:
1. Триггерят клик через специфичные ключи: артист, BPM, mood, year
2. Используют community pattern из топ-10 (но не копируют буквально)
3. Длина ≤95 символов каждый (YT лимит 100)
4. Префикс [FREE] обязателен
5. Шаблон-якорь: [FREE] {artist_display} Type Beat 2026 - "<NAME>" | <SCENE> | <BPM> BPM <KEY>
6. Имя бита (NAME) можно оставить как в current или предложить более звучное (UPPERCASE если ≤16 символов)

OUTPUT (строго JSON, no markdown):
{{
  "variants": [
    {{"title": "...", "rationale": "почему этот сработает"}},
    {{"title": "...", "rationale": "..."}},
    {{"title": "...", "rationale": "..."}}
  ]
}}
"""


_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


def parse_llm_titles_response(text: str) -> list[dict]:
    """Парсит LLM-output в list[{title, rationale}].

    LLM может вернуть JSON в markdown-блоке (```json...```) или с префиксом
    «Here are...». Извлекаем первый `{...}` блок и парсим.

    Возвращает [] если парсинг провалился (caller покажет error).
    """
    if not text or not text.strip():
        return []
    # Strip markdown code fences if any
    cleaned = re.sub(r"```(?:json)?", "", text).strip(" `\n")
    # Try direct parse first
    try:
        data = json.loads(cleaned)
    except Exception:
        # Find first {...} block
        m = _JSON_BLOCK_RE.search(cleaned)
        if not m:
            logger.warning("parse_llm_titles: no JSON block in: %s", text[:200])
            return []
        try:
            data = json.loads(m.group(0))
        except Exception as e:
            logger.warning("parse_llm_titles: JSON parse failed: %s | text: %s", e, text[:200])
            return []
    variants = data.get("variants") if isinstance(data, dict) else None
    if not isinstance(variants, list):
        return []
    out = []
    for v in variants:
        if not isinstance(v, dict):
            continue
        title = (v.get("title") or "").strip()
        if not title:
            continue
        # Truncate if exceeded YT limit
        if len(title) > 100:
            title = title[:97] + "..."
        out.append({
            "title": title,
            "rationale": (v.get("rationale") or "").strip()[:200],
        })
    return out[:3]  # max 3 variants
