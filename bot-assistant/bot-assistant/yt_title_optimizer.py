"""YouTube title optimizer for type beat uploads.

Two-step pipeline:
1. yt-dlp ytsearchN → get top competitor titles for "{artist} type beat"
2. LLM (OpenRouter Claude Haiku) → generate title matching winner patterns

Usage:
    from yt_title_optimizer import optimized_title
    title = optimized_title(meta)   # BeatMeta → str ≤100 chars
"""
from __future__ import annotations

import logging
import os
import re
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from beat_upload import BeatMeta

logger = logging.getLogger(__name__)

_YT_SEARCH_N = 10
_TITLE_MAX = 100


def search_competitor_titles(artist: str, scene: str, n: int = _YT_SEARCH_N) -> list[str]:
    """yt-dlp ytsearch → top N titles for '{artist} type beat'.

    Falls back to scene query if artist returns no results.
    Returns [] on any error — caller falls back to canonical title.
    """
    import yt_dlp

    def _fetch(query: str) -> list[str]:
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(f"ytsearch{n}:{query}", download=False)
                entries = (info or {}).get("entries") or []
                return [e.get("title", "") for e in entries if e.get("title")]
        except Exception as e:
            logger.warning("yt-dlp search '%s' failed: %s", query, e)
            return []

    titles = _fetch(f"{artist} type beat")
    if not titles and scene and scene.lower() != artist.lower():
        titles = _fetch(f"{scene} type beat")
    return [t for t in titles if t][:n]


def _llm_generate(meta: "BeatMeta", competitor_titles: list[str]) -> str | None:
    """Call OpenRouter Claude Haiku to generate optimized title.

    Returns None if API unavailable or response invalid.
    """
    key = os.getenv("OPENROUTER_KEY", "").strip()
    if not key:
        return None

    titles_block = "\n".join(f"• {t}" for t in competitor_titles[:10])
    prompt = (
        f"These YouTube titles for \"{meta.artist_display} type beat\" currently get the most views:\n\n"
        f"{titles_block}\n\n"
        f"Write ONE YouTube title for this beat:\n"
        f"  Beat name: {meta.name}\n"
        f"  Artist: {meta.artist_display}\n"
        f"  BPM: {meta.bpm}\n"
        f"  Key: {meta.key_short}\n"
        f"  Year: 2026\n\n"
        f"Rules:\n"
        f"- Copy the EXACT format/pattern of the top titles above\n"
        f"- Must contain '{meta.artist_display}' and 'Type Beat' and '2026'\n"
        f"- Maximum {_TITLE_MAX} characters\n"
        f"- Return ONLY the title text, no quotes, no explanation"
    )
    try:
        resp = httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}",
                     "HTTP-Referer": "https://github.com/tripmusicrussia-hub/Triple"},
            json={
                "model": "anthropic/claude-haiku-4-5",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 80,
                "temperature": 0.25,
            },
            timeout=20,
        )
        raw = resp.json()["choices"][0]["message"]["content"].strip().strip('"').strip("'")
        raw = re.sub(r"\n.*", "", raw)  # keep only first line
        if len(raw) < 15:
            return None
        if len(raw) > _TITLE_MAX:
            raw = raw[:_TITLE_MAX].rsplit(" ", 1)[0]
        return raw
    except Exception as e:
        logger.warning("LLM title generation failed: %s", e)
        return None


def optimizer_decision(ctr: float, views: int, watch_minutes: float, current_title: str) -> dict:
    """Optimizer Agent: CTR-based decision о смене title.

    CTR < 3%                → change_title
    CTR 3-6% + views < 100 → change_title (средний CTR, но мало данных)
    CTR 3-6% + views >= 100 → keep
    CTR > 6%                → keep

    Returns: {"action": "change_title"|"keep", "reason": str}
    """
    if ctr > 6:
        return {"action": "keep", "reason": f"CTR {ctr:.1f}% — отличный"}
    elif ctr >= 3:
        if views < 100:
            return {"action": "change_title",
                    "reason": f"CTR {ctr:.1f}% средний + мало просмотров ({views})"}
        return {"action": "keep", "reason": f"CTR {ctr:.1f}% — норм, {views} просмотров"}
    else:
        return {"action": "change_title", "reason": f"CTR {ctr:.1f}% — ниже порога 3%"}


def optimized_title(meta: "BeatMeta") -> tuple[str, list[str]]:
    """Main entry point: BeatMeta → (optimized_title, competitor_titles).

    competitor_titles — список топ-10 конкурентов для показа в превью.
    Falls back to canonical_yt_title if yt-dlp or LLM fails.
    """
    from beat_post_builder import canonical_yt_title, _canonical_scene

    scene = _canonical_scene(meta)
    competitors = search_competitor_titles(meta.artist_display, scene)
    logger.info("yt_title_optimizer: %d competitor titles for '%s'",
                len(competitors), meta.artist_display)

    if competitors:
        title = _llm_generate(meta, competitors)
        if title:
            logger.info("yt_title_optimizer: LLM title: %r", title)
            return title, competitors

    fallback = canonical_yt_title(meta)
    logger.info("yt_title_optimizer: fallback to canonical: %r", fallback)
    return fallback, competitors
