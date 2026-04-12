"""
wiki_store.py — файловая wiki для хранения знаний бота.

Словари (abbreviations, name_fixes, stopwords, categories) живут в wiki/*.json.
Если wiki/ отсутствует или файл битый — fallback на захардкоженные значения.
Бот никогда не ломается из-за wiki.

product_mappings — выученные OCR→Sigma маппинги из коррекций пользователя
и успешных автоматических матчей. Накапливаются в _pending.json, flush каждые
5 минут или при завершении process_invoice.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

WIKI_DIR = os.path.join(os.path.dirname(__file__), "wiki")

_INDEX_FILE = os.path.join(WIKI_DIR, "_index.json")
_CHANGELOG_FILE = os.path.join(WIKI_DIR, "_changelog.jsonl")
_PENDING_FILE = os.path.join(WIKI_DIR, "_pending.json")

# ── In-memory cache (loaded once, refreshed on flush) ──────────────────────
_wiki_cache: dict = {}  # topic_name → parsed data
_wiki_loaded = False

# ── Hardcoded defaults (fallback if wiki/ missing or broken) ───────────────

_DEFAULT_ABBREVIATIONS = {
    "овк": "очень важная корова",
    "овс": "очень важная свинка",
    "мп": "молочный переулок",
    "коктейль топтыжка": "молочный коктейль топтыжка",
    "сгущ.карламан": "сгущ.карламан",
    "сгущ карламан": "сгущ.карламан",
}

_DEFAULT_NAME_FIXES = {
    "карламан какао 7,5": "карламан какао 7",
    "карламан какао 7.5": "карламан какао 7",
    "вас. счас.": "васькино счастье",
    "вас счас": "васькино счастье",
    "спред октябрьский": "масло крестьянское октябрьский",
}

_DEFAULT_STOPWORDS = {
    "напиток", "напитки", "безалкогольный", "безалк", "газированный", "газ",
    "негазированный", "негаз", "газир", "среднегазированный",
    "пастер", "пастеризованный", "ультрапастер", "стерилизованный",
    "питьевой", "питьевая", "питьевое",
    "питания", "питание", "питанием",
    "подземных", "подземный", "подземные", "источников", "источник",
    "детский", "детская", "детское",
    "природный", "природная", "природное",
    "натуральный", "натуральное", "натуральная",
    "отборный", "отборное", "отборная",
    "сухарики", "сухариков", "сухарик", "сух", "ржаной", "ржаные", "ржан",
    "картофельные", "картофельный", "картоф",
    "со", "вкусом", "вкус", "вкусы",
    "для", "из", "на", "по", "от", "при", "без",
    "шт", "грамм", "миллилитр", "литр", "килограмм",
    "пэт", "пет", "стек", "стак", "стакан", "пачка", "пакет", "пак",
    "коробка", "упаковка", "упак", "рул", "бут", "бутылка", "вакуумная",
    "гр", "мл", "кг",
    "новый", "новая", "новое", "хит",
    "тат", "татар", "татарстан", "удм", "удмурт", "башк", "башкир",
    "рос", "рф",
    "пер", "кор", "вак",
    "новинка", "товар", "продукт", "продукты",
    "гофрокороб", "гофрокор", "гофро", "гофрокороба",
    "лоток", "лотки", "лотке",
    "ящик", "ящике", "ящики",
    "мешок", "мешке", "мешки",
    "картон", "картонн", "картонная",
    "термоусадка", "термо",
    "рулон", "рулоне",
    "сорт", "сорта", "сортов", "категория",
}

_DEFAULT_CATEGORIES = {
    "МОЛОЧКА": ["молоко", "сметана", "кефир", "ряженка", "йогурт", "творог", "масло слив", "сливки", "простокваша", "варенец", "коктейль молоч", "топтыжка", "овк", "очень важная", "васькино", "деревенский"],
    "БАКАЛЕЯ": ["крупа", "рис", "гречка", "макарон", "мука", "сахар", "соль", "масло раст", "уксус", "соус", "майонез", "кетчуп"],
    "КОНСЕРВЫ": ["сгущ", "тушен", "консерв", "рыбн", "горошек", "кукуруза", "фасоль"],
    "ВЫПЕЧКА": ["хлеб", "батон", "булка", "лаваш", "багет", "пита", "лепешка"],
    "ЗАМОРОЗКА": ["замороженн", "пельмен", "вареник", "котлет заморож"],
    "СЛАДОСТИ": ["конфет", "шоколад", "печенье", "вафл", "торт", "пряник", "зефир", "мармелад", "халва"],
    "МЯСО, КОЛБАСА": ["колбас", "сосиск", "сардельк", "ветчин", "окорок", "бекон", "мясо", "курин", "свинин", "говядин"],
    "СОКИ, ВОДА": ["сок", "вода", "нектар", "морс", "компот", "лимонад", "квас"],
    "СЕМЕЧКИ, ЧИПСЫ, СУХ": ["семечк", "чипс", "сухар", "снек", "орех", "попкорн"],
    "МАЙОНЕЗЫ, СОУСЫ, КЕ": ["майонез", "кетчуп", "соус", "горчиц", "хрен"],
    "МАСЛА": ["масло раст", "масло олив", "масло подсолн"],
    "КРУПЫ": ["крупа", "рис", "гречк", "перловк", "пшен", "овсян", "манн"],
    "МАКАРОНЫ": ["макарон", "спагетт", "вермишел", "лапш"],
    "МУКА": ["мука", "крахмал", "дрожж"],
    "ПРИПРАВЫ И СПЕЦИИ": ["специ", "приправ", "перец молот", "лавров", "куркум", "паприк"],
    "ФРУКТЫ, ОВОЩИ": ["яблок", "груш", "банан", "апельсин", "морковь", "картофел", "помидор", "огурец", "капуст"],
    "ЧАЙ, КОФЕ": ["чай", "кофе", "какао", "цикори"],
    "БЫТОВАЯ ХИМИЯ": ["шампун", "гель", "мыло", "порошок", "средство", "дезодорант", "зубная"],
}


# ── Loading ────────────────────────────────────────────────────────────────

def _load_topic(filename: str) -> Optional[dict]:
    """Load a single topic JSON file. Returns parsed dict or None on failure."""
    path = os.path.join(WIKI_DIR, filename)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
        logger.debug(f"wiki_store: cannot load {filename}: {e}")
        return None


def _entries_to_dict(data: Optional[dict]) -> Optional[dict]:
    """Convert topic entries list to {key: value} dict."""
    if not data or "entries" not in data:
        return None
    return {e["key"]: e["value"] for e in data["entries"] if "key" in e and "value" in e}


def _entries_to_set(data: Optional[dict]) -> Optional[set]:
    """Convert topic entries list to set of keys."""
    if not data or "entries" not in data:
        return None
    return {e["key"] for e in data["entries"] if "key" in e}


def get_abbreviations() -> dict:
    data = _load_topic("abbreviations.json")
    result = _entries_to_dict(data)
    if result is not None:
        return result
    return _DEFAULT_ABBREVIATIONS.copy()


def get_name_fixes() -> dict:
    data = _load_topic("name_fixes.json")
    result = _entries_to_dict(data)
    if result is not None:
        return result
    return _DEFAULT_NAME_FIXES.copy()


def get_stopwords() -> set:
    data = _load_topic("stopwords.json")
    result = _entries_to_set(data)
    if result is not None:
        return result
    return _DEFAULT_STOPWORDS.copy()


def get_categories() -> dict:
    """Returns {category_name: [keyword, ...]}."""
    data = _load_topic("categories.json")
    result = _entries_to_dict(data)
    if result is not None:
        return result
    return {k: list(v) for k, v in _DEFAULT_CATEGORIES.items()}


def get_product_mappings() -> dict:
    """Returns {normalized_name: {sigma_name, sigma_id, confidence, use_count}}."""
    data = _load_topic("product_mappings.json")
    if data and "entries" in data:
        return {e["key"]: e["value"] for e in data["entries"] if "key" in e and "value" in e}
    return {}


# ── Writing / Learning ─────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ensure_wiki_dir():
    os.makedirs(WIKI_DIR, exist_ok=True)


def record_correction(ocr_name: str, sigma_name: str, sigma_id: Optional[str] = None,
                       confidence: float = 0.7, source: str = "auto_match"):
    """Append a correction to _pending.json. Will be flushed later."""
    _ensure_wiki_dir()
    pending = []
    try:
        with open(_PENDING_FILE, "r", encoding="utf-8") as f:
            pending = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    pending.append({
        "ocr_name": ocr_name,
        "sigma_name": sigma_name,
        "sigma_id": sigma_id,
        "confidence": confidence,
        "source": source,
        "timestamp": _now_iso(),
    })

    with open(_PENDING_FILE, "w", encoding="utf-8") as f:
        json.dump(pending, f, ensure_ascii=False, indent=2)

    logger.debug(f"wiki: recorded correction '{ocr_name[:30]}' → '{sigma_name[:30]}'")


def flush_pending():
    """Merge _pending.json into product_mappings.json, write changelog."""
    try:
        with open(_PENDING_FILE, "r", encoding="utf-8") as f:
            pending = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return 0

    if not pending:
        return 0

    # Load existing mappings
    mappings_data = _load_topic("product_mappings.json")
    if not mappings_data:
        mappings_data = {"topic": "product_mappings", "entries": []}

    # Build lookup by key
    by_key = {e["key"]: e for e in mappings_data["entries"]}

    merged = 0
    for p in pending:
        key = p["ocr_name"].lower().strip()
        existing = by_key.get(key)
        if existing:
            # Increment use_count, update confidence
            old_count = existing["value"].get("use_count", 1)
            new_count = old_count + 1
            new_conf = min(0.7 + 0.1 * new_count, 0.95)
            # If explicit user correction, boost confidence
            if p["source"] == "user_correction":
                new_conf = max(new_conf, 0.9)
            existing["value"]["use_count"] = new_count
            existing["value"]["confidence"] = new_conf
            existing["value"]["sigma_name"] = p["sigma_name"]
            if p["sigma_id"]:
                existing["value"]["sigma_id"] = p["sigma_id"]
            _append_changelog("update", "product_mappings", key,
                              f"use_count={old_count}", f"use_count={new_count}")
        else:
            entry = {
                "key": key,
                "value": {
                    "sigma_name": p["sigma_name"],
                    "sigma_id": p["sigma_id"],
                    "confidence": p["confidence"],
                    "use_count": 1,
                    "source": p["source"],
                },
                "added": p["timestamp"],
            }
            mappings_data["entries"].append(entry)
            by_key[key] = entry
            _append_changelog("add", "product_mappings", key, None, p["sigma_name"])
        merged += 1

    # Save mappings
    _ensure_wiki_dir()
    mappings_path = os.path.join(WIKI_DIR, "product_mappings.json")
    with open(mappings_path, "w", encoding="utf-8") as f:
        json.dump(mappings_data, f, ensure_ascii=False, indent=2)

    # Update index
    _update_index_entry("product_mappings", len(mappings_data["entries"]))

    # Clear pending
    with open(_PENDING_FILE, "w", encoding="utf-8") as f:
        json.dump([], f)

    logger.info(f"wiki: flushed {merged} corrections into product_mappings ({len(mappings_data['entries'])} total)")
    return merged


def _append_changelog(action: str, topic: str, key: str,
                       old_value: Optional[str], new_value: Optional[str]):
    """Append one line to _changelog.jsonl."""
    _ensure_wiki_dir()
    entry = {
        "ts": _now_iso(),
        "action": action,
        "topic": topic,
        "key": key[:60],
        "old": old_value,
        "new": new_value,
    }
    try:
        with open(_CHANGELOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.warning(f"wiki: cannot write changelog: {e}")


def _update_index_entry(topic: str, entry_count: int):
    """Update a single topic's metadata in _index.json."""
    index = {}
    try:
        with open(_INDEX_FILE, "r", encoding="utf-8") as f:
            index = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        index = {"version": 1, "topics": {}}

    if "topics" not in index:
        index["topics"] = {}

    if topic in index["topics"]:
        index["topics"][topic]["entry_count"] = entry_count
        index["topics"][topic]["updated"] = _now_iso()
    else:
        index["topics"][topic] = {
            "path": f"{topic}.json",
            "entry_count": entry_count,
            "updated": _now_iso(),
        }

    index["last_updated"] = _now_iso()

    try:
        with open(_INDEX_FILE, "w", encoding="utf-8") as f:
            json.dump(index, f, ensure_ascii=False, indent=2)
    except OSError as e:
        logger.warning(f"wiki: cannot update index: {e}")


# ── Stats ──────────────────────────────────────────────────────────────────

def get_stats() -> dict:
    """Return wiki statistics for /wiki stats command."""
    index = {}
    try:
        with open(_INDEX_FILE, "r", encoding="utf-8") as f:
            index = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    topics = index.get("topics", {})
    pending_count = 0
    try:
        with open(_PENDING_FILE, "r", encoding="utf-8") as f:
            pending_count = len(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    changelog_count = 0
    try:
        with open(_CHANGELOG_FILE, "r", encoding="utf-8") as f:
            changelog_count = sum(1 for _ in f)
    except (FileNotFoundError, OSError):
        pass

    return {
        "topics": {name: info.get("entry_count", "?") for name, info in topics.items()},
        "pending": pending_count,
        "changelog_entries": changelog_count,
        "last_updated": index.get("last_updated", "never"),
    }


def get_unverified_mappings(limit: int = 20) -> list:
    """Return recent unverified product mappings for /wiki review."""
    data = _load_topic("product_mappings.json")
    if not data or "entries" not in data:
        return []
    unverified = [
        e for e in data["entries"]
        if e.get("value", {}).get("confidence", 0) < 1.0
    ]
    # Sort by confidence ascending (least confident first)
    unverified.sort(key=lambda e: e.get("value", {}).get("confidence", 0))
    return unverified[:limit]
