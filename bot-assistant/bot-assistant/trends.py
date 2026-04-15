"""Свежие темы из интернета для текстовых рубрик канала.

Источники:
  - YouTube Data API — search.list() по ключевикам сцены (свежие загрузки)
  - Google Trends (pytrends) — related_queries по seed-ключам

Результат кэшируется в wiki/trends_cache.json на 24 часа.
Интегрируется в post_generator.pick_text_topic() — 70% шанс взять свежий тренд
вместо темы из post_ideas.md.
"""
from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

HERE = Path(__file__).parent
CACHE_PATH = HERE / "wiki" / "trends_cache.json"
CACHE_TTL_HOURS = 24

# Seed-ключевики — сцена автора (hard trap Memphis/Detroit)
YT_QUERIES = [
    "memphis type beat 2026",
    "detroit type beat 2026",
    "hard trap type beat",
    "key glock type beat",
    "kenny muney type beat",
    "nardo wick type beat",
    "rob49 type beat",
    "bossman dlow type beat",
    "obladaet type beat",
]

GOOGLE_SEEDS = [
    "memphis rap",
    "detroit rap",
    "hard trap",
    "type beat",
    "key glock",
    "nardo wick",
]


# ─── YouTube ─────────────────────────────────────────────────────────────────

def fetch_youtube_trends(per_query: int = 5) -> list[str]:
    """Топ последних загрузок YT по каждому seed-запросу. Возвращает список заголовков."""
    try:
        from yt_api import get_yt_client
    except ImportError:
        logger.warning("yt_api недоступен — пропускаю YouTube тренды")
        return []
    try:
        yt = get_yt_client()
    except RuntimeError as e:
        logger.warning("YT клиент не собрался: %s", e)
        return []

    titles: list[str] = []
    published_after = (datetime.utcnow() - timedelta(days=14)).isoformat("T") + "Z"
    for q in YT_QUERIES:
        try:
            resp = yt.search().list(
                q=q,
                part="snippet",
                type="video",
                order="viewCount",
                maxResults=per_query,
                publishedAfter=published_after,
                relevanceLanguage="en",
            ).execute()
            for item in resp.get("items", []):
                t = item["snippet"]["title"].strip()
                if t:
                    titles.append(t)
        except Exception as e:
            logger.warning("YT search '%s' failed: %s", q, e)
            continue
    logger.info("YT trends: %d заголовков", len(titles))
    return titles


# ─── Google Trends ───────────────────────────────────────────────────────────

def fetch_google_trends() -> list[str]:
    """Related_queries (rising + top) по seed-ключам. Возвращает сырые поисковые фразы."""
    try:
        from pytrends.request import TrendReq
    except ImportError:
        logger.warning("pytrends не установлен — пропускаю Google Trends")
        return []

    queries: list[str] = []
    try:
        pytrends = TrendReq(hl="en-US", tz=180, timeout=(20, 40))
    except Exception as e:
        logger.warning("TrendReq init failed: %s", e)
        return []

    # pytrends ограничивает 5 ключей за запрос — разбиваем
    for i in range(0, len(GOOGLE_SEEDS), 5):
        batch = GOOGLE_SEEDS[i:i + 5]
        try:
            pytrends.build_payload(batch, timeframe="now 7-d", geo="")
            related = pytrends.related_queries()
        except Exception as e:
            logger.warning("pytrends batch %s failed: %s", batch, e)
            continue
        for kw, data in (related or {}).items():
            if not data:
                continue
            for section_key in ("rising", "top"):
                df = data.get(section_key)
                if df is None or df.empty:
                    continue
                for q in df["query"].head(5).tolist():
                    if q:
                        queries.append(str(q))
    # уникализируем с сохранением порядка
    seen: set[str] = set()
    out: list[str] = []
    for q in queries:
        k = q.lower().strip()
        if k and k not in seen:
            seen.add(k)
            out.append(q)
    logger.info("Google Trends: %d запросов", len(out))
    return out


# ─── Кэш ─────────────────────────────────────────────────────────────────────

def _load_cache() -> Optional[dict]:
    if not CACHE_PATH.exists():
        return None
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("trends cache corrupt: %s", e)
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


def _save_cache(topics: list[str], raw: dict) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(
        json.dumps(
            {
                "fetched_at": datetime.now().isoformat(timespec="seconds"),
                "topics": topics,
                "raw": raw,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


# ─── Публичный API ───────────────────────────────────────────────────────────

def _to_topic_lines(yt_titles: list[str], gt_queries: list[str]) -> list[str]:
    """Превращает сырые данные в пригодные topic-строки для LLM-генератора.

    Не мудрим — LLM разберётся. Даём ей контекст что это за данные.
    """
    topics: list[str] = []
    for t in yt_titles:
        topics.append(f"Свежий YouTube-заголовок из сцены: «{t}». Разбери как битмейкер — что цепляет/не цепляет в названии, о чём говорит тренд.")
    for q in gt_queries:
        topics.append(f"Горячий запрос в Google: «{q}». Выскажись как автор — почему это ищут, есть ли у тебя мысли на этот счёт.")
    return topics


def get_trending_topics(force_refresh: bool = False) -> list[str]:
    """Возвращает список topic-строк. Кэшируется 24ч. При ошибке всех источников — []."""
    if not force_refresh:
        cached = _load_cache()
        if cached and cached.get("topics"):
            return cached["topics"]

    yt_titles = fetch_youtube_trends()
    gt_queries = fetch_google_trends()
    topics = _to_topic_lines(yt_titles, gt_queries)

    if not topics:
        logger.warning("trends: оба источника пусты — темы не обновлены")
        # не перезаписываем кэш пустотой — держим старый
        return []

    _save_cache(
        topics,
        {"youtube": yt_titles, "google_trends": gt_queries},
    )
    return topics


def pick_trending_topic() -> Optional[str]:
    topics = get_trending_topics()
    if not topics:
        return None
    return random.choice(topics)


# ─── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    force = "--refresh" in sys.argv
    topics = get_trending_topics(force_refresh=force)
    print(f"Получено {len(topics)} тем\n")
    for t in topics[:20]:
        print(f"  • {t}")
