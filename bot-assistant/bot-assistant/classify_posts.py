"""
Классификатор постов канала: beat / track / remix / non_audio / uncertain.

Usage:
  python classify_posts.py --sample 30     # показать 30 случайных с reasoning
  python classify_posts.py --stats         # общая статистика без изменений
  python classify_posts.py --apply         # применить к beats_data.json (backup .bak)
"""
import sys
import os
import re
import json
import random
import argparse
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DB = Path(__file__).parent / "beats_data.json"

REMIX_WORDS = ("ремикс", "remix", " rmx", "_rmx", "rework", "flip by", "flipped", "mix by", "mix_by")
TRACK_WORDS = ("релиз", " release ", "single", " ft.", " feat.", " feat ", "премьера", "трэчок", "трек", "песня")
TRACK_SELF_WORDS = ("мой трек", "мой новый трек", "наш трек", "нашего трека", "наш релиз", "мой релиз", "сделал трэчок", "мой трэчок")
BEAT_WORDS = ("type beat", "type_beat", "typebeat", "instrumental", "prod by", "prod. by", "no vocal", "no_vocal")
BPM_RE = re.compile(r"\b(\d{2,3})\s*bpm\b", re.IGNORECASE)
BPM_STANDALONE_RE = re.compile(r"(?:^|[\s_-])(\d{2,3})(?:[\s_.-]|$)")
KEY_RE = re.compile(r"(?:^|[\s_-])([A-Ga-g][#b]?(?:min|maj|m)?)(?:[\s_.-]|$)")
CYRILLIC_RE = re.compile(r"[а-яё]", re.IGNORECASE)


def classify(beat):
    name = (beat.get("name") or "").lower()
    tags = [t.lower() for t in beat.get("tags", [])]
    has_audio = bool(beat.get("file_id"))
    has_bpm_explicit = beat.get("bpm", 0) > 0 or bool(BPM_RE.search(name))
    # Standalone number 60-200 in filename with key nearby → likely BPM
    has_bpm_standalone = False
    for m in BPM_STANDALONE_RE.finditer(name):
        n = int(m.group(1))
        if 60 <= n <= 200:
            has_bpm_standalone = True
            break
    has_bpm = has_bpm_explicit or has_bpm_standalone
    # Key like "Em", "c#m", "Am", "Ebm"
    has_key = beat.get("key", "-") != "-"
    if not has_key:
        for m in KEY_RE.finditer(name):
            tok = m.group(1)
            if any(suf in tok.lower() for suf in ("min", "maj", "m")) and len(tok) <= 5:
                has_key = True
                break

    reasons = []
    score_beat = 0
    score_track = 0
    score_remix = 0

    if not has_audio:
        return "non_audio", 1.0, ["no file_id"]

    for w in REMIX_WORDS:
        if w in name or w.strip() in tags:
            score_remix += 3
            reasons.append(f"remix:{w.strip()}")

    for w in BEAT_WORDS:
        if w in name:
            score_beat += 3
            reasons.append(f"beat:{w}")

    for w in TRACK_SELF_WORDS:
        if w in name:
            score_track += 3
            reasons.append(f"track_self:{w}")

    for w in TRACK_WORDS:
        if w in name:
            score_track += 2
            reasons.append(f"track:{w.strip()}")

    if has_bpm:
        score_beat += 2
        reasons.append("has_bpm")
    if has_key:
        score_beat += 1
        reasons.append("has_key")

    # Pattern: ARTIST_KEY_BPM (lots of underscores, no spaces) → beat
    if name.count("_") >= 3 and " - " not in name:
        score_beat += 2
        reasons.append("underscore_pattern")

    # Pattern: "artist - title" with no bpm/key → likely track
    if " - " in name and not has_bpm and not has_key and score_beat == 0:
        score_track += 1
        reasons.append("artist-title_pattern")

    # Default beat name like "Beat #227" with tags but no text
    if re.match(r"beat\s*#\d+", name) and tags:
        score_beat += 2
        reasons.append("default_beat_name+tags")

    # Russian caption with audio but no beat/bpm/key signals → track from author
    if has_audio and not has_bpm and not has_key and not any(w in name for w in BEAT_WORDS):
        russian_chars = len(CYRILLIC_RE.findall(name))
        if russian_chars > 3 and russian_chars > len(name) * 0.3:
            score_track += 2
            reasons.append("russian_caption+audio")

    # Remix word in name is a decisive signal — reduce margin requirement
    remix_word_in_name = any(w in name for w in REMIX_WORDS)

    scores = {"beat": score_beat, "track": score_track, "remix": score_remix}
    max_type = max(scores, key=scores.get)
    max_score = scores[max_type]
    second = sorted(scores.values(), reverse=True)[1]

    if max_score == 0:
        return "uncertain", 0.3, reasons or ["no signals"]

    confidence = min(0.95, 0.5 + 0.1 * (max_score - second))
    # Decisive remix wins with margin 1
    min_margin = 1 if (max_type == "remix" and remix_word_in_name) else 2
    if max_score - second < min_margin:
        return "uncertain", confidence, reasons + [f"scores={scores}"]

    return max_type, confidence, reasons


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=0)
    ap.add_argument("--stats", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    with open(DB, "r", encoding="utf-8") as f:
        data = json.load(f)

    classified = []
    for b in data:
        new_type, conf, reasons = classify(b)
        classified.append((b, new_type, conf, reasons))

    if args.stats or args.sample or not args.apply:
        from collections import Counter
        old = Counter(b.get("content_type") for b, _, _, _ in classified)
        new = Counter(t for _, t, _, _ in classified)
        print(f"Было: {dict(old)}")
        print(f"Стало: {dict(new)}")
        unc = [c for c in classified if c[1] == "uncertain"]
        print(f"Uncertain: {len(unc)}")
        changed = sum(1 for b, t, _, _ in classified if b.get("content_type") != t)
        print(f"Изменится: {changed} из {len(classified)}")
        print()

    if args.sample:
        sample = random.sample(classified, min(args.sample, len(classified)))
        print(f"=== Sample {len(sample)} ===")
        for b, t, conf, reasons in sample:
            old = b.get("content_type", "?")
            mark = "→" if old != t else "="
            print(f"[{old}{mark}{t}] conf={conf:.2f} | {b.get('name')[:60]}")
            print(f"   reasons: {reasons[:5]}")

    if args.apply:
        import shutil
        bak = str(DB) + ".bak"
        shutil.copy(DB, bak)
        print(f"Backup: {bak}")
        for b, t, conf, reasons in classified:
            b["content_type"] = t
            b["classification_confidence"] = round(conf, 2)
        with open(DB, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Saved {len(data)} posts.")


if __name__ == "__main__":
    main()
