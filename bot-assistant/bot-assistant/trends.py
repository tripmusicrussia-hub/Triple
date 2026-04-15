"""Scene-context для LLM-генератора постов канала.

Канал = личный журнал автора-битмейкера, не новостная лента. Поэтому тренды
НЕ становятся темами постов — они инжектятся как контекст в промпт: автор
в курсе сцены, может вплести инсайт, если ложится в тему, но пишет от себя.

Источники:
  - YouTube — новые загрузки артистов-вотчлиста (через yt_api)
  - Reddit r/trap + r/FL_Studio + r/makinghiphop (public JSON, no auth)
  - RSS: HotNewHipHop, XXL, Complex, The Flow
  - Google Trends — rising queries по seed-ключам

Кэш 24ч в wiki/trends_cache.json.
"""
from __future__ import annotations

import json
import logging
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

HERE = Path(__file__).parent
CACHE_PATH = HERE / "wiki" / "trends_cache.json"
CACHE_TTL_HOURS = 24

# Артист-вотчлист — за чьими YT-каналами следим
ARTIST_WATCHLIST_US = [
    "Key Glock",
    "Kenny Muney",
    "Big Moochie Grape",
    "Nardo Wick",
    "Bossman Dlow",
    "Rob49",
    "Future",
    "GloRilla",
]

ARTIST_WATCHLIST_RU = [
    "Obladaet",
    "Скриптонит",
    "OG Buda",
    "Slava Marlow",
    "Kizaru",
    "Big Baby Tape",
    "MAYOT",
    "Платина",
]

# Reddit сабы
REDDIT_SUBS = ["trap", "FL_Studio", "makinghiphop"]

# RSS-ленты
RSS_FEEDS = {
    "HotNewHipHop": "https://www.hotnewhiphop.com/feed",
    "XXL":          "https://www.xxlmag.com/feed/",
    "Complex":      "https://www.complex.com/music/rss.xml",
    "The Flow":     "https://the-flow.ru/feed",
}

GOOGLE_TRENDS_SEEDS = [
    "memphis rap",
    "detroit rap",
    "hard trap",
    "type beat",
    "key glock",
    "nardo wick",
]

USER_AGENT = "Mozilla/5.0 (Triple-Bot iiiplfiii-scene-monitor/1.0)"


# ─── Reddit ──────────────────────────────────────────────────────────────────

def fetch_reddit_hot(sub: str, limit: int = 5) -> list[str]:
    """Топ за неделю в сабреддите — заголовки. Public JSON, без авторизации."""
    url = f"https://www.reddit.com/r/{sub}/top.json?t=week&limit={limit}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as e:
        logger.warning("reddit r/%s failed: %s", sub, e)
        return []
    titles = []
    for child in data.get("data", {}).get("children", []):
        t = child.get("data", {}).get("title", "").strip()
        if t:
            titles.append(t)
    logger.info("reddit r/%s: %d заголовков", sub, len(titles))
    return titles


# ─── RSS ─────────────────────────────────────────────────────────────────────

def fetch_rss(source_name: str, url: str, limit: int = 5) -> list[str]:
    """Свежие заголовки из RSS. Использует feedparser если есть, иначе skip."""
    try:
        import feedparser
    except ImportError:
        logger.warning("feedparser не установлен — пропускаю RSS %s", source_name)
        return []
    try:
        feed = feedparser.parse(url, agent=USER_AGENT)
    except Exception as e:
        logger.warning("RSS %s failed: %s", source_name, e)
        return []
    titles = []
    for entry in (feed.entries or [])[:limit]:
        t = getattr(entry, "title", "").strip()
        if t:
            titles.append(t)
    logger.info("RSS %s: %d заголовков", source_name, len(titles))
    return titles


# ─── YouTube watchlist ───────────────────────────────────────────────────────

def fetch_youtube_watchlist(artists: list[str], per_artist: int = 2) -> list[str]:
    """Свежие загрузки (< 14 дней) по артистам из watchlist.

    Использует search.list с каналом артиста как primary result. Ограниченный
    quota (100 units на search) — держим компактно, 1 search-call на артиста.
    """
    try:
        from yt_api import get_yt_client
    except ImportError:
        return []
    try:
        yt = get_yt_client()
    except RuntimeError as e:
        logger.warning("YT клиент не собрался: %s", e)
        return []

    published_after = (datetime.utcnow() - timedelta(days=14)).isoformat("T") + "Z"
    results = []
    for artist in artists:
        try:
            resp = yt.search().list(
                q=artist,
                part="snippet",
                type="video",
                order="date",
                maxResults=per_artist,
                publishedAfter=published_after,
                relevanceLanguage="en",
            ).execute()
            for item in resp.get("items", []):
                title = item["snippet"]["title"].strip()
                ch = item["snippet"]["channelTitle"]
                # фильтр: имя артиста (или его канала) должно встречаться
                if artist.lower() in title.lower() or artist.lower() in ch.lower():
                    results.append(f"{artist}: «{title}» ({ch})")
        except Exception as e:
            logger.warning("YT watchlist %s failed: %s", artist, e)
            continue
    logger.info("YT watchlist: %d свежих", len(results))
    return results


# ─── Google Trends ───────────────────────────────────────────────────────────

def fetch_google_trends() -> list[str]:
    try:
        from pytrends.request import TrendReq
    except ImportError:
        return []
    queries: list[str] = []
    try:
        pytrends = TrendReq(hl="en-US", tz=180, timeout=(20, 40))
    except Exception as e:
        logger.warning("TrendReq init: %s", e)
        return []

    for i in range(0, len(GOOGLE_TRENDS_SEEDS), 5):
        batch = GOOGLE_TRENDS_SEEDS[i:i + 5]
        try:
            pytrends.build_payload(batch, timeframe="now 7-d", geo="")
            related = pytrends.related_queries()
        except Exception as e:
            logger.warning("pytrends batch %s: %s", batch, e)
            continue
        for kw, data in (related or {}).items():
            if not data:
                continue
            for section_key in ("rising", "top"):
                df = data.get(section_key)
                if df is None or df.empty:
                    continue
                for q in df["query"].head(3).tolist():
                    if q:
                        queries.append(str(q))
    seen, out = set(), []
    for q in queries:
        k = q.lower().strip()
        if k and k not in seen:
            seen.add(k)
            out.append(q)
    return out


# ─── Кэш ─────────────────────────────────────────────────────────────────────

def _load_cache() -> Optional[dict]:
    if not CACHE_PATH.exists():
        return None
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    ts = data.get("fetched_at")
    if not ts:
        return None
    try:
        fetched = datetime.fromisoformat(ts)
    except ValueError:
        return None
    if datetime.now() - fetched > timedelta(hours=CACHE_TTL_HOURS):
        return None
    return data


def _save_cache(sections: dict) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(
        json.dumps(
            {
                "fetched_at": datetime.now().isoformat(timespec="seconds"),
                "sections": sections,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


# ─── Публичное API ───────────────────────────────────────────────────────────

def _fetch_all_sections() -> dict:
    """Собирает сырьё со всех источников. Каждый изолирован — падение одного
    не роняет остальные."""
    sections = {}

    us_uploads = fetch_youtube_watchlist(ARTIST_WATCHLIST_US, per_artist=2)
    ru_uploads = fetch_youtube_watchlist(ARTIST_WATCHLIST_RU, per_artist=2)
    if us_uploads:
        sections["releases_us"] = us_uploads[:10]
    if ru_uploads:
        sections["releases_ru"] = ru_uploads[:10]

    reddit_items = []
    for sub in REDDIT_SUBS:
        reddit_items.extend(fetch_reddit_hot(sub, limit=4))
    if reddit_items:
        sections["reddit"] = reddit_items[:10]

    rss_items = []
    for name, url in RSS_FEEDS.items():
        for title in fetch_rss(name, url, limit=3):
            rss_items.append(f"[{name}] {title}")
    if rss_items:
        sections["news"] = rss_items[:12]

    gt = fetch_google_trends()
    if gt:
        sections["google_trends"] = gt[:10]

    return sections


def get_scene_context(max_chars: int = 1200, force_refresh: bool = False) -> str:
    """Компактный context-блок со свежими событиями сцены. Кэш 24ч.

    Возвращает отформатированную строку для инъекции в user-message LLM.
    Пустая строка — если все источники недоступны.
    """
    if not force_refresh:
        cached = _load_cache()
        if cached and cached.get("sections"):
            return _format_context(cached["sections"], max_chars)

    sections = _fetch_all_sections()
    if not sections:
        logger.warning("scene context: все источники пусты")
        return ""
    _save_cache(sections)
    return _format_context(sections, max_chars)


def _format_context(sections: dict, max_chars: int) -> str:
    """Собирает из сырых секций компактный context-блок."""
    labels = {
        "releases_us": "🎤 Свежие US-релизы (YT, < 14 дней)",
        "releases_ru": "🎤 Свежие RU-релизы (YT, < 14 дней)",
        "reddit":      "💬 Обсуждают на reddit (r/trap, r/FL_Studio, r/makinghiphop) за неделю",
        "news":        "📰 Новости сцены (HNHH / XXL / Complex / The Flow)",
        "google_trends": "🔎 Горячие поисковые запросы недели",
    }
    parts = []
    for key, items in sections.items():
        label = labels.get(key, key)
        lines = "\n".join(f"  • {x}" for x in items[:6])
        parts.append(f"{label}:\n{lines}")
    ctx = "\n\n".join(parts)
    if len(ctx) > max_chars:
        ctx = ctx[:max_chars].rsplit("\n", 1)[0] + "\n  …"
    return ctx


# ─── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    force = "--refresh" in sys.argv
    ctx = get_scene_context(force_refresh=force)
    print(ctx if ctx else "(контекст пуст — все источники недоступны)")
