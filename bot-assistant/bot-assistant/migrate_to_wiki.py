"""
migrate_to_wiki.py — одноразовый скрипт для генерации wiki/ из хардкоженных дефолтов.
Запускать из директории bot-assistant: python migrate_to_wiki.py
"""

import json
import os
from datetime import datetime, timezone

WIKI_DIR = os.path.join(os.path.dirname(__file__), "wiki")
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_kv_entries(d: dict) -> list:
    return [
        {"key": k, "value": v, "source": "hardcoded_migration", "confidence": 1.0, "added": NOW}
        for k, v in d.items()
    ]


def make_set_entries(s: set) -> list:
    return [
        {"key": w, "source": "hardcoded_migration", "confidence": 1.0, "added": NOW}
        for w in sorted(s)
    ]


def main():
    os.makedirs(WIKI_DIR, exist_ok=True)

    # Import defaults from wiki_store
    from wiki_store import (
        _DEFAULT_ABBREVIATIONS, _DEFAULT_NAME_FIXES,
        _DEFAULT_STOPWORDS, _DEFAULT_CATEGORIES,
    )

    topics = {}

    # abbreviations.json
    data = {"topic": "abbreviations", "entries": make_kv_entries(_DEFAULT_ABBREVIATIONS)}
    with open(os.path.join(WIKI_DIR, "abbreviations.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    topics["abbreviations"] = {
        "path": "abbreviations.json",
        "description": "OCR abbreviation expansions",
        "entry_count": len(data["entries"]),
        "updated": NOW,
    }
    print(f"  abbreviations: {len(data['entries'])} entries")

    # name_fixes.json
    data = {"topic": "name_fixes", "entries": make_kv_entries(_DEFAULT_NAME_FIXES)}
    with open(os.path.join(WIKI_DIR, "name_fixes.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    topics["name_fixes"] = {
        "path": "name_fixes.json",
        "description": "Exact string replacements for invoice→Sigma name mismatches",
        "entry_count": len(data["entries"]),
        "updated": NOW,
    }
    print(f"  name_fixes: {len(data['entries'])} entries")

    # stopwords.json
    data = {"topic": "stopwords", "entries": make_set_entries(_DEFAULT_STOPWORDS)}
    with open(os.path.join(WIKI_DIR, "stopwords.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    topics["stopwords"] = {
        "path": "stopwords.json",
        "description": "Words to ignore during Sigma product search",
        "entry_count": len(data["entries"]),
        "updated": NOW,
    }
    print(f"  stopwords: {len(data['entries'])} entries")

    # categories.json
    entries = make_kv_entries(_DEFAULT_CATEGORIES)
    data = {"topic": "categories", "entries": entries}
    with open(os.path.join(WIKI_DIR, "categories.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    topics["categories"] = {
        "path": "categories.json",
        "description": "Keyword→category mappings for new product creation",
        "entry_count": len(data["entries"]),
        "updated": NOW,
    }
    print(f"  categories: {len(data['entries'])} entries")

    # product_mappings.json (empty)
    data = {"topic": "product_mappings", "entries": []}
    with open(os.path.join(WIKI_DIR, "product_mappings.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    topics["product_mappings"] = {
        "path": "product_mappings.json",
        "description": "Learned OCR→Sigma product mappings from corrections",
        "entry_count": 0,
        "updated": NOW,
    }
    print(f"  product_mappings: 0 entries (empty)")

    # _index.json
    index = {"version": 1, "last_updated": NOW, "topics": topics}
    with open(os.path.join(WIKI_DIR, "_index.json"), "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    print(f"  _index.json written")

    # _changelog.jsonl (empty)
    with open(os.path.join(WIKI_DIR, "_changelog.jsonl"), "w", encoding="utf-8") as f:
        pass
    print(f"  _changelog.jsonl created (empty)")

    # _pending.json (empty)
    with open(os.path.join(WIKI_DIR, "_pending.json"), "w", encoding="utf-8") as f:
        json.dump([], f)
    print(f"  _pending.json created (empty)")

    total = sum(t["entry_count"] for t in topics.values())
    print(f"\nDone! Wiki created at {WIKI_DIR} with {total} entries across {len(topics)} topics.")


if __name__ == "__main__":
    main()
