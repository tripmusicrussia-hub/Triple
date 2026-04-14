"""Парсер имени mp3-файла в структуру bit-метаданных.

Формат: "<artist> type beat <NAME> <BPM> <KEY>.mp3"

Примеры:
- "kenny muney type beat THOUGHTS 160 Am.mp3"
- "future x don toliver type beat HOOK 140 Am.mp3"
- "rob49 x bossman dlow type beat BIG FLIPPA 152 Dm.mp3"
- "nardowick type beat FRIK 153 G#m.mp3"
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path


KEY_RE = re.compile(r"^([A-G](?:#|b)?)(m?)$", re.IGNORECASE)

ARTIST_CASING = {
    "nardowick": "NardoWick",
    "nardo wick": "NardoWick",
    "keyglock": "Key Glock",
    "kennymuney": "Kenny Muney",
    "rob49": "Rob49",
    "bossmandlow": "Bossman Dlow",
    "don toliver": "Don Toliver",
    "dontoliver": "Don Toliver",
    "obladaet": "Obladaet",
    "bigmoochiegrape": "Big Moochie Grape",
    "big moochie grape": "Big Moochie Grape",
    "future": "Future",
}


@dataclass
class BeatMeta:
    artist_raw: str           # "kenny muney" / "future x don toliver"
    artist_display: str       # "Kenny Muney" / "Future x Don Toliver"
    artist_line: str          # "Kenny Muney Type Beat"
    name: str                 # "THOUGHTS"
    bpm: int                  # 160
    key: str                  # "A# minor" / "D minor"
    key_short: str            # "Am" / "Dm" / "G#m"

    @property
    def yt_title(self) -> str:
        """NAME — Artist Type Beat | Hard Trap Instrumental YEAR (формируется в post_builder)."""
        return f"{self.name} — {self.artist_display} Type Beat"


def _normalize_artist(raw: str) -> str:
    """'kenny muney' → 'Kenny Muney', 'future x don toliver' → 'Future x Don Toliver'.

    Сначала проверяем override-словарь ARTIST_CASING (для NardoWick и прочих),
    потом дефолтный capitalize.
    """
    raw_lower = raw.strip().lower()
    # Коллаб через " x " — обрабатываем каждого артиста отдельно
    if " x " in raw_lower:
        subs = [s.strip() for s in raw_lower.split(" x ")]
        parts = [ARTIST_CASING.get(s, _cap_words(s)) for s in subs]
        return " x ".join(parts)
    return ARTIST_CASING.get(raw_lower, _cap_words(raw_lower))


def _cap_words(s: str) -> str:
    return " ".join(p.capitalize() for p in s.split())


def _parse_key(raw: str) -> tuple[str, str]:
    """'Am' → ('A minor', 'Am'); 'C#m' → ('C# minor', 'C#m'); 'D' → ('D major', 'D')."""
    raw = raw.strip()
    m = KEY_RE.match(raw)
    if not m:
        raise ValueError(f"bad key: {raw!r}")
    note = m.group(1).upper()
    if len(note) == 2 and note[1] == "B":  # e.g. "Bb"
        note = note[0] + "b"
    minor = bool(m.group(2))
    key_short = note + ("m" if minor else "")
    full = f"{note} {'minor' if minor else 'major'}"
    return full, key_short


def parse_filename(filename: str) -> BeatMeta:
    """Парсит имя файла в метаданные.

    Raises ValueError если формат не совпал.
    """
    stem = Path(filename).stem.strip()

    # Ищем якорь "type beat" (case-insensitive)
    m = re.search(r"\btype\s*beat\b", stem, re.IGNORECASE)
    if not m:
        raise ValueError(
            f"имя файла должно содержать 'type beat' — получено: {filename!r}"
        )

    artist_raw = stem[: m.start()].strip(" _-")
    tail = stem[m.end():].strip(" _-")

    if not artist_raw:
        raise ValueError(f"не нашёл артиста до 'type beat' в {filename!r}")

    # Tail = "<NAME...> <BPM> <KEY>"
    tokens = tail.split()
    if len(tokens) < 3:
        raise ValueError(
            f"после 'type beat' ожидаю NAME BPM KEY, получено: {tail!r}"
        )

    key_short_raw = tokens[-1]
    bpm_raw = tokens[-2]
    name_tokens = tokens[:-2]

    try:
        bpm = int(bpm_raw)
    except ValueError:
        raise ValueError(f"не распарсил BPM из {bpm_raw!r}")
    if not (40 <= bpm <= 250):
        raise ValueError(f"BPM вне разумного диапазона: {bpm}")

    key_full, key_short = _parse_key(key_short_raw)

    name = " ".join(name_tokens).upper().strip()
    if not name:
        raise ValueError("пустое имя трека")

    artist_display = _normalize_artist(artist_raw)
    artist_line = f"{artist_display} Type Beat"

    return BeatMeta(
        artist_raw=artist_raw.lower(),
        artist_display=artist_display,
        artist_line=artist_line,
        name=name,
        bpm=bpm,
        key=key_full,
        key_short=key_short,
    )


if __name__ == "__main__":
    cases = [
        "kenny muney type beat THOUGHTS 160 Am.mp3",
        "future x don toliver type beat HOOK 140 Am.mp3",
        "rob49 x bossman dlow type beat BIG FLIPPA 152 Dm.mp3",
        "nardowick type beat FRIK 153 G#m.mp3",
        "key glock type beat MEMORY 164 Dm.mp3",
        "future type beat SHUTTLE 138 Gm.mp3",
    ]
    for c in cases:
        m = parse_filename(c)
        print(f"{c}")
        print(f"  artist: {m.artist_display!r}")
        print(f"  name:   {m.name!r}")
        print(f"  bpm:    {m.bpm}")
        print(f"  key:    {m.key!r} (short: {m.key_short!r})")
        print(f"  title:  {m.yt_title!r}")
        print()
