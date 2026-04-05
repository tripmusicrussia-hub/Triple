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
    global BEATS_CACHE
    try:
        if os.path.exists(BEATS_FILE):
            with open(BEATS_FILE, "r", encoding="utf-8") as f:
                BEATS_CACHE = json.load(f)
            _rebuild_index()
            logger.info("Beats loaded: " + str(len(BEATS_CACHE)))
        else:
            BEATS_CACHE = []
    except Exception as e:
        logger.error("Load error: " + str(e))
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
    if exclude_ids is None:
        exclude_ids = []
    current_tags = set(current_beat["tags"])
    scored = []
    for beat in BEATS_CACHE:
        if beat["id"] == current_beat["id"] or beat["id"] in exclude_ids:
            continue
        common = len(current_tags & set(beat["tags"]))
        if common > 0:
            scored.append((common, beat))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [b for _, b in scored[:5]]


def get_next_similar(current_beat, exclude_ids=None):
    similar = get_similar_beats(current_beat, exclude_ids)
    return random.choice(similar) if similar else get_random_beat(exclude_ids)
