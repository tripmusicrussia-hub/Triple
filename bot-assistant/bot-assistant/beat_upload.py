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


def beat_record_to_meta(beat: dict) -> "BeatMeta | None":
    """Конверт записи каталога (beats_data.json) → BeatMeta для repost-публикации.

    Returns None если данных недостаточно для type-beat YT title:
    - bpm должен быть 40..250
    - key должен быть валидным (Am, Dm, C#m, ...)

    Если artist-tag отсутствует в `tags` — fallback на generic «Hard Trap»
    (canonical_yt_title подхватит как `[FREE] Hard Trap Type Beat ...`).
    Если в tags только сцена (memphis/detroit) — используем её как artist
    (Memphis Type Beat / Detroit Type Beat — валидный SEO pattern).

    Уродские имена legacy постов канала (типа `Kenny_Muney_-_Issues_ type beat
    Gm 121 bpm with vocal.mp3`) чистятся: убираем артист-префикс, BPM, key,
    `type beat`, vocal-метки, mp3-расширение, dash/underscore noise.
    """
    bpm = beat.get("bpm") or 0
    if not isinstance(bpm, int) or bpm < 40 or bpm > 250:
        return None
    raw_key = (beat.get("key") or "").strip()
    if not raw_key or raw_key == "-":
        return None
    try:
        key_full, key_short = _parse_key(raw_key)
    except ValueError:
        return None

    tags = [t for t in (beat.get("tags") or []) if t]
    # Skip technical tags типа `bpm140`, `dark`, `hard` — это не артисты.
    # Сцены (memphis/detroit/atlanta) — fallback artist если нет конкретного
    # рэпера: «Memphis Type Beat» / «Detroit Type Beat» = valid SEO.
    SKIP_TAGS = {"hard", "dark", "trap", "hardtrap", "future_2026"}
    SCENE_TAGS = {"memphis", "detroit", "atlanta", "florida", "neworleans", "nola"}
    artist_raw = next(
        (t.lower() for t in tags
         if not t.lower().startswith("bpm")
         and t.lower() not in SKIP_TAGS
         and t.lower() not in SCENE_TAGS),
        "",
    )
    if not artist_raw:
        # Нет конкретного артиста — пробуем сцену как fallback artist.
        scene_tag = next(
            (t.lower() for t in tags if t.lower() in SCENE_TAGS), "",
        )
        if scene_tag:
            # «memphis» → «Memphis» (заглавная) — будет «Memphis Type Beat».
            artist_raw = scene_tag
        else:
            # Совсем без identifying tags — generic Hard Trap.
            # canonical_yt_title fallback'ом сделает «Hard Trap Type Beat».
            artist_raw = "hard trap"
    # Decode "kennymuney" → "kenny muney" (camelCase split).
    artist_raw_decoded = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", artist_raw).lower()
    artist_display = ARTIST_CASING.get(artist_raw_decoded, _cap_words(artist_raw_decoded))

    raw_name = beat.get("name") or ""
    # Cleanup pipeline: убираем технические метки чтобы получить чистое NAME.
    # ВАЖНО: сначала конвертируем _ и - в пробелы, чтобы regex-паттерны
    # вида \btype\s*beat\b корректно матчили TYPE_BEAT / TYPE-BEAT.
    name = re.sub(r"\.mp3$", "", raw_name, flags=re.I)
    name = re.sub(r"[_\-]+", " ", name)           # ← сначала нормализуем разделители
    name = re.sub(re.escape(artist_raw_decoded), "", name, flags=re.I)
    name = re.sub(re.escape(artist_display), "", name, flags=re.I)
    name = re.sub(r"(?i)\btype\s*beat\b", "", name)
    name = re.sub(r"(?i)\b(no|with|without)\s*vocal\b", "", name)
    name = re.sub(r"(?i)\b\d+\s*bpm\b", "", name)
    # Удаляем key только если стоит standalone (Am, Dm, C#m), не внутри слова.
    name = re.sub(r"(?i)(?<![a-z])[a-g][#b]?m(?![a-z])", "", name)
    name = re.sub(r"(?i)\b(prod|by|iiiplfiii|iiiplkiii|leeptonxt|moodf1x)\b", "", name)
    # Key as two-word phrase: "F minor", "C# major", "Bb minor"
    name = re.sub(r"(?i)\b[a-g][#b]?\s+(?:minor|major)\b", "", name)
    name = re.sub(r"(?i)\bx\b", "", name)              # collab "x" separator
    name = re.sub(r"\s+", " ", name).strip(" -_")
    if not name or len(name) < 2:
        name = "INSTRUMENTAL"
    name = name[:40].upper().strip()

    artist_line = f"{artist_display} Type Beat"
    return BeatMeta(
        artist_raw=artist_raw_decoded,
        artist_display=artist_display,
        artist_line=artist_line,
        name=name,
        bpm=bpm,
        key=key_full,
        key_short=key_short,
    )


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
