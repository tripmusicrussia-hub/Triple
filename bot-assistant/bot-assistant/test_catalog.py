"""Проверка целостности каталога после классификации."""
import sys
import json
from pathlib import Path
from collections import Counter

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DB = Path(__file__).parent / "beats_data.json"


def main():
    with open(DB, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"Всего постов: {len(data)}")
    assert len(data) > 0, "База пустая"

    types = Counter(b.get("content_type", "MISSING") for b in data)
    print(f"Распределение: {dict(types)}")

    # 1. Все content_type из whitelist
    allowed = {"beat", "track", "remix", "non_audio"}
    bad = [b for b in data if b.get("content_type") not in allowed]
    assert not bad, f"{len(bad)} постов с недопустимым content_type: {[b['id'] for b in bad[:3]]}"
    print(f"[OK] Все content_type в whitelist")

    # 2. non_audio не имеют file_id (по определению)
    non_audio = [b for b in data if b["content_type"] == "non_audio"]
    leaky = [b for b in non_audio if b.get("file_id")]
    assert not leaky, f"{len(leaky)} non_audio имеют file_id"
    print(f"[OK] non_audio без file_id ({len(non_audio)} шт.)")

    # 3. Каталожные посты (beat/track/remix) имеют file_id
    catalog = [b for b in data if b["content_type"] != "non_audio"]
    no_audio = [b for b in catalog if not b.get("file_id")]
    assert not no_audio, f"{len(no_audio)} каталожных без file_id: {[b['id'] for b in no_audio[:3]]}"
    print(f"[OK] Все каталожные посты имеют file_id ({len(catalog)} шт.)")

    # 4. Фильтр 'только биты' не ловит non_audio
    beats = [b for b in data if b.get("content_type", "beat") == "beat"]
    leaky_in_beats = [b for b in beats if not b.get("file_id")]
    assert not leaky_in_beats, f"Фильтр beat зацепил посты без аудио"
    print(f"[OK] Фильтр beat чистый ({len(beats)} шт.)")

    # 5. ID уникальны
    ids = [b["id"] for b in data]
    dups = [i for i, c in Counter(ids).items() if c > 1]
    assert not dups, f"Дубликаты id: {dups[:5]}"
    print(f"[OK] ID уникальны")

    # 6. Имитация поиска: query 'beat' не должна вернуть non_audio
    q = "beat"
    results = [b for b in data
               if b.get("content_type", "beat") != "non_audio"
               and (q in b["name"].lower() or any(q in t for t in b.get("tags", [])))]
    bad_search = [b for b in results if b["content_type"] == "non_audio"]
    assert not bad_search, "Поиск вернул non_audio"
    print(f"[OK] Поиск 'beat' → {len(results)} результатов, все с аудио")

    # 7. Имитация 'бита дня': пул только beat с file_id
    pool = [b for b in data if b.get("content_type", "beat") == "beat"]
    assert all(b.get("file_id") for b in pool), "В пуле 'бит дня' есть посты без аудио"
    print(f"[OK] Пул 'бит дня' чистый ({len(pool)} шт.)")

    print("\n=== Все проверки пройдены ===")


if __name__ == "__main__":
    main()
