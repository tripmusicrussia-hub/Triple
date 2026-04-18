import random
import logging
import json
import os
import re

logger = logging.getLogger(__name__)

BEATS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "beats_data.json")
BEATS_CACHE = []
BEATS_BY_ID = {}  # индекс для мгновенного поиска

# Компилируем regex один раз
BPM_REGEX = re.compile(r'(\d{2,3})\s*bpm', re.IGNORECASE)
KEY_REGEX = re.compile(r'\b([A-G][b#]?\s*(?:min|maj|m|minor|major)?)\b')


def _rebuild_index():
    global BEATS_BY_ID
    BEATS_BY_ID = {beat["id"]: beat for beat in BEATS_CACHE}


def save_beats():
    try:
        with open(BEATS_FILE, "w", encoding="utf-8") as f:
            json.dump(BEATS_CACHE, f, ensure_ascii=False, indent=2)
        logger.info("Beats saved: " + str(len(BEATS_CACHE)))
    except Exception as e:
        logger.error("Save error: " + str(e))


def load_beats():
    import traceback
    global BEATS_CACHE
    try:
        if os.path.exists(BEATS_FILE):
            size = os.path.getsize(BEATS_FILE)
            logger.info(f"load_beats: reading {BEATS_FILE} ({size} bytes)")
            with open(BEATS_FILE, "r", encoding="utf-8") as f:
                BEATS_CACHE = json.load(f)
            _rebuild_index()
            logger.info("Beats loaded: " + str(len(BEATS_CACHE)))
        else:
            logger.warning(f"load_beats: file not found at {BEATS_FILE}")
            BEATS_CACHE = []
    except Exception as e:
        logger.error(f"Load error: {e}\n{traceback.format_exc()}")
        BEATS_CACHE = []


def parse_tags_from_text(text):
    if not text:
        return []
    return [w[1:].lower() for w in text.split() if w.startswith("#")]


def parse_bpm_from_text(text):
    if not text:
        return None
    match = BPM_REGEX.search(text)
    return int(match.group(1)) if match else None


def parse_key_from_text(text):
    if not text:
        return None
    match = KEY_REGEX.search(text)
    return match.group(1).strip() if match else None


def get_all_tags():
    tags = set()
    for beat in BEATS_CACHE:
        tags.update(beat["tags"])
    return sorted(tags)


def get_beat_by_id(beat_id):
    return BEATS_BY_ID.get(beat_id)


def get_random_beat(exclude_ids=None):
    if exclude_ids is None:
        exclude_ids = []
    available = [b for b in BEATS_CACHE if b["id"] not in exclude_ids]
    if not available:
        available = BEATS_CACHE
    return random.choice(available) if available else None


def get_beats_by_tag(tag):
    return [b for b in BEATS_CACHE if tag in b["tags"]]


def get_similar_beats(current_beat, exclude_ids=None):
    """Ищет похожие биты. Scoring:
    1. Общие теги (primary signal)
    2. Fallback: BPM ±15 + тот же content_type (если тегов нет совпадений)
    """
    if exclude_ids is None:
        exclude_ids = []
    current_tags = set(current_beat.get("tags", []))
    current_bpm = current_beat.get("bpm") or 0
    current_ct = current_beat.get("content_type", "beat")
    scored = []
    bpm_fallback = []
    for beat in BEATS_CACHE:
        if beat["id"] == current_beat["id"] or beat["id"] in exclude_ids:
            continue
        if beat.get("content_type", "beat") == "non_audio":
            continue
        common = len(current_tags & set(beat.get("tags", [])))
        if common > 0:
            scored.append((common, beat))
        elif current_bpm and beat.get("bpm") and beat.get("content_type", "beat") == current_ct:
            diff = abs(current_bpm - beat["bpm"])
            if diff <= 15:
                bpm_fallback.append((diff, beat))
    scored.sort(key=lambda x: x[0], reverse=True)
    result = [b for _, b in scored[:5]]
    # Если по тегам мало — добиваем BPM-соседями
    if len(result) < 5 and bpm_fallback:
        bpm_fallback.sort(key=lambda x: x[0])  # меньше разница = ближе
        for _, b in bpm_fallback:
            if b not in result and len(result) < 5:
                result.append(b)
    return result


def get_next_similar(current_beat, exclude_ids=None):
    similar = get_similar_beats(current_beat, exclude_ids)
    return random.choice(similar) if similar else get_random_beat(exclude_ids)
