"""Строит YT title/description/tags и TG caption для свежего бита.

Использует проверенную winner-формулу из yt_fixes.py.
TG-подпись генерится LLM в голосе iiiplfiii-voice (разные стили каждый раз).
"""
from __future__ import annotations

import logging
import random
import re
from dataclasses import dataclass

from beat_upload import BeatMeta

logger = logging.getLogger(__name__)

BRAND = "TRIPLE FILL"
TG_HANDLE = "@iiiplfiii"
IG_HANDLE = "@iiiplfiii"
YEAR = 2026

# Mood / описание по основному артисту (для Best for + description phrases)
ARTIST_PROFILE = {
    "kenny muney": {
        "scene": "Memphis",
        "mood": "Fast melodic",
        "adjectives": ["aggressive", "melodic", "high-energy"],
        "best_for": ["Kenny Muney type vocals", "Fast Memphis flow", "High BPM melodic rap", "Street trap / Chicago-style"],
        "related_tags": ["kennymuney", "keyglock", "bigmoochiegrape", "memphis"],
    },
    "key glock": {
        "scene": "Memphis",
        "mood": "Hard aggressive",
        "adjectives": ["dark", "high-energy", "hard"],
        "best_for": ["Key Glock type vocals", "Kenny Muney flow", "Fast Memphis rap", "Hard street trap"],
        "related_tags": ["keyglock", "kennymuney", "bigmoochiegrape", "memphis"],
    },
    "big moochie grape": {
        "scene": "Memphis",
        "mood": "Hard Memphis",
        "adjectives": ["heavy", "aggressive", "street"],
        "best_for": ["Big Moochie Grape type vocals", "Memphis street rap", "Hard trap", "Key Glock flow"],
        "related_tags": ["bigmoochiegrape", "keyglock", "kennymuney", "memphis"],
    },
    "rob49": {
        "scene": "New Orleans",
        "mood": "Hard aggressive",
        "adjectives": ["aggressive", "hard", "street"],
        "best_for": ["Rob49 type vocals", "Bossman Dlow flow", "Hard street rap", "Aggressive New Orleans trap"],
        "related_tags": ["rob49", "bossmandlow", "neworleans", "florida"],
    },
    "bossman dlow": {
        "scene": "Florida",
        "mood": "Hard street",
        "adjectives": ["hard", "aggressive", "street"],
        "best_for": ["Bossman Dlow type vocals", "Rob49 flow", "Florida hard trap", "Street rap"],
        "related_tags": ["bossmandlow", "rob49", "florida", "neworleans"],
    },
    "nardowick": {
        "scene": "Detroit",
        "mood": "Dark aggressive",
        "adjectives": ["dark", "evil", "fast", "sinister"],
        "best_for": ["NardoWick type vocals", "Obladaet flow", "Dark Detroit rap", "Fast aggressive trap"],
        "related_tags": ["nardowick", "obladaet", "detroit"],
    },
    "obladaet": {
        "scene": "Detroit",
        "mood": "Dark aggressive",
        "adjectives": ["dark", "aggressive", "fast"],
        "best_for": ["Obladaet type vocals", "NardoWick flow", "RU dark trap", "Fast aggressive rap"],
        "related_tags": ["obladaet", "nardowick", "detroit"],
    },
    "future": {
        "scene": "Atlanta",
        "mood": "Dark melodic",
        "adjectives": ["atmospheric", "melodic", "cold", "modern"],
        "best_for": ["Future type vocals", "Don Toliver flow", "Dark melodic trap", "Atmospheric rap"],
        "related_tags": ["future", "dontoliver", "melodic", "dark"],
    },
    "don toliver": {
        "scene": "Atlanta",
        "mood": "Melodic atmospheric",
        "adjectives": ["melodic", "atmospheric", "cold"],
        "best_for": ["Don Toliver type vocals", "Future flow", "Melodic trap", "Atmospheric night rap"],
        "related_tags": ["dontoliver", "future", "melodic"],
    },
}

GENERIC_PROFILE = {
    "scene": "Hard trap",
    "mood": "Hard",
    "adjectives": ["hard", "aggressive"],
    "best_for": ["Modern trap vocals", "Street rap", "Hard trap flow", "Aggressive delivery"],
    "related_tags": [],
}


def _slug(s: str) -> str:
    """Lowercase + keep alphanumerics + spaces."""
    return re.sub(r"\s+", " ", s.lower().strip())


def _get_profile(artist_raw: str) -> dict:
    """Ищет профиль по первому артисту (до ' x '). Нормализует пробелы/_/casing."""
    raw = artist_raw.split(" x ")[0].strip().lower()
    if raw in ARTIST_PROFILE:
        return ARTIST_PROFILE[raw]
    # Пользователь может написать «nardo wick» / «nardo_wick» — нормализуем
    normalized = re.sub(r"[\s_-]+", "", raw)
    for key, prof in ARTIST_PROFILE.items():
        if re.sub(r"[\s_-]+", "", key) == normalized:
            return prof
    return GENERIC_PROFILE


def _mood_label(prof: dict, bpm: int) -> str:
    """Строит краткий mood label для title: 'Fast Memphis Trap' / 'Dark Detroit Trap' etc."""
    scene = prof["scene"]
    if bpm >= 155:
        tempo = "Fast"
    elif bpm >= 140:
        tempo = "Hard"
    else:
        tempo = "Melodic"
    if scene in ("Memphis", "Detroit", "New Orleans", "Florida", "Atlanta"):
        return f"{tempo} {scene} Trap"
    return f"{tempo} Trap"


def build_yt_title(beat: BeatMeta) -> str:
    """SEO-оптимизированный тайтл по паттерну топ type-beat каналов.

    Формат: `(FREE) <Artist> Type Beat <YEAR> - "<NAME>"`

    Обоснование (анализ топ-30 видео по Kenny Muney / Key Glock / Memphis):
    - 90% (27/30) используют `(FREE)` или `[FREE]` префикс
    - 87% (26/30) разделитель `-` (не `|` и не `—`)
    - имя бита в кавычках `"NAME"` в конце (не в начале)
    - средняя длина 56 симв, макс 94 — вписываемся
    - наш прежний формат `NAME — Artist Type Beat | Fast Memphis Trap` встречался
      в 0/30 топ-видео → алгоритм YT не распознавал паттерн ниши

    Жанровые ключи (scene, mood) перенесены в description и tags — там они
    работают на SEO без раздувания тайтла.
    """
    return f'(FREE) {beat.artist_display} Type Beat {YEAR} - "{beat.name}"'


def _an(word: str) -> str:
    """'a' vs 'an' по первой букве."""
    return "an" if word and word[0].lower() in "aeiou" else "a"


def build_yt_description(beat: BeatMeta) -> str:
    prof = _get_profile(beat.artist_raw)
    scene = prof["scene"]
    mood_adj_low = prof["adjectives"][0].lower()
    second_adj_low = prof["adjectives"][1].lower() if len(prof["adjectives"]) > 1 else mood_adj_low
    # Если scene уже содержит "trap" (GENERIC = "Hard trap") — режем чтобы не повторяться
    is_generic = scene.lower().endswith("trap")
    scene_low = scene.lower()
    mood_low = prof["mood"].lower()

    if is_generic:
        artist_line = (
            f"{prof['mood']} {beat.artist_display} type beat with a hard street vibe. "
            f"Punchy 808s, {mood_adj_low} melodies and trap drums create a modern, "
            f"aggressive sound"
        )
        energy_line = f"with {_an(second_adj_low)} {second_adj_low}, street-rap energy"
    else:
        artist_line = (
            f"{prof['mood']} {beat.artist_display} type beat with {_an(scene)} {scene} vibe. "
            f"Punchy 808s, {mood_adj_low} melodies and hard trap drums create a "
            f"modern {scene} sound"
        )
        energy_line = f"with {_an(mood_adj_low)} {mood_adj_low}, {second_adj_low} {scene} energy"

    best = "\n".join(f"✦ {x}" for x in prof["best_for"])

    return (
        f'{artist_line} {energy_line}.\n\n'
        f'"{beat.name}" is a hard trap instrumental inspired by this sound. '
        f'The beat combines expressive melodic elements with punchy drums and deep bass, '
        f'creating space for confident, modern vocals.\n\n'
        f'Best for:\n{best}\n\n'
        f'🎧 Key: {beat.key}\n'
        f'⚡ BPM: {beat.bpm}\n\n'
        f'💼 License options:\n✦ MP3\n✦ WAV\n✦ Unlimited\n✦ Exclusive\n\n'
        f'📩 To purchase or lease this beat:\n'
        f'📱 Telegram: {TG_HANDLE}\n'
        f'📱 Instagram: {IG_HANDLE}\n\n'
        f'✦ Free for non-profit use only.\n'
        f'✦ Credit required: (prod. by {BRAND})'
    )


def build_yt_tags(beat: BeatMeta) -> list[str]:
    prof = _get_profile(beat.artist_raw)
    artist_slug = beat.artist_display.lower().replace(" x ", " ")
    primary = beat.artist_display.split(" x ")[0].lower()
    collab = " x ".join(beat.artist_display.split(" x ")).lower() if " x " in beat.artist_display else None

    tags = [
        f"{primary} type beat",
        f"{primary} type beat {YEAR}",
    ]
    if collab:
        tags.append(f"{collab} type beat")
    tags.extend([
        f"{prof['scene'].lower()} type beat",
        f"{prof['scene'].lower()} type beat {YEAR}",
        f"hard {prof['scene'].lower()} instrumental",
        f"{beat.bpm} bpm trap beat",
        f"{beat.key_short.lower()} trap beat",
        f"{prof['mood'].lower()} trap beat",
        "hard trap instrumental",
        f"free {primary} type beat",
    ])
    # Related artists tags
    for rel in prof.get("related_tags", []):
        if len(tags) >= 15:
            break
        t = f"{rel} type beat"
        if t not in tags:
            tags.append(t)
    return tags[:15]


def build_tg_caption(beat: BeatMeta) -> str:
    """Fallback-шаблон если LLM недоступен. Короткий, нейтральный."""
    prof = _get_profile(beat.artist_raw)
    return (
        f"{beat.name} — {beat.artist_line}\n\n"
        f"🎧 {beat.key}  ⚡ {beat.bpm} BPM\n\n"
        f"Пиши в ЛС — @iiiplfiii"
    )


# Стили для рандомизации LLM-генерации каждого нового бита.
# Порядок синхронизирован с post_analytics.STYLE_LABELS для индексации.
TG_CAPTION_STYLES = [
    ("short_hook", "короткий хук в одну строку + BPM/key + контакт. Без воды."),
    ("minimal", "минимал — почти без слов, голые факты (имя, артист-тип, BPM/key, @iiiplfiii). 3-4 строки."),
    ("storytelling", "сторителл — 3-4 строки про настроение бита и под что зайдёт. Затем BPM/key + контакт."),
    ("question", "вопрос аудитории в конце — 2-3 строки, потом BPM/key, потом вопрос («кто пишет такое?» / «кому в работу?»)."),
    ("emotional", "эмоциональный — короткие фразы, восклицание, скобки)) или многоточие… BPM/key + контакт."),
]


async def build_tg_caption_async(beat: BeatMeta) -> tuple[str, str]:
    """LLM-генерация подписи в голосе iiiplfiii-voice.
    Возвращает (text, style_label). При сбое — fallback на шаблон с label 'fallback'.
    """
    try:
        import post_generator
        prof = _get_profile(beat.artist_raw)
        style_label, style = random.choice(TG_CAPTION_STYLES)
        user_msg = (
            f"Сгенерируй подпись к АУДИО-посту в канал @iiiplfiii для свежего бита.\n\n"
            f"Метаданные:\n"
            f"- Имя бита: {beat.name}\n"
            f"- Артист (type beat для): {beat.artist_line}\n"
            f"- Сцена: {prof['scene']}\n"
            f"- Настроение/mood: {prof['mood']}\n"
            f"- BPM: {beat.bpm}\n"
            f"- Key: {beat.key}\n\n"
            f"Стиль этой подписи: {style}\n\n"
            f"Жёсткие требования:\n"
            f"- 2-6 строк, plain text. ЗАПРЕЩЕНО: хэштеги (#что_угодно), markdown (**, __, ~~), кавычки вокруг всего поста\n"
            f"- От первого лица, в голосе автора (см. system-prompt)\n"
            f"- ОБЯЗАТЕЛЬНО упомянуть имя бита «{beat.name}» и артиста «{beat.artist_display}»\n"
            f"- BPM и key — где-то в тексте (можно одной строкой типа «{beat.key} · {beat.bpm}»)\n"
            f"- Минимум эмодзи (0-3 шт), только из whitelist (🎧 🎵 🔥 🎹 ⚡)\n"
            f"- Контакт «@iiiplfiii» в конце или другая форма CTA\n"
            f"- Никаких «Пиши в ЛС за beat» — звучит как продаван. Найди живой вариант.\n\n"
            f"Верни ТОЛЬКО подпись, без преамбулы, без кавычек."
        )
        text = await post_generator._call_llm(user_msg, max_tokens=200, temperature=0.95)
        text = text.strip().strip('"\'')
        # Удаляем хэштег-простыни в конце (LLM иногда добавляет вопреки промпту)
        text = re.sub(r"\n+#[\w_]+(\s+#[\w_]+)*\s*$", "", text).rstrip()
        if beat.name.lower() not in text.lower():
            logger.warning("LLM caption не содержит имя бита, fallback. Got: %r", text[:120])
            return build_tg_caption(beat), "fallback"
        return text, style_label
    except Exception as e:
        logger.warning("build_tg_caption_async failed: %s — fallback", e)
        return build_tg_caption(beat), "fallback"


@dataclass
class YTPost:
    title: str
    description: str
    tags: list[str]


def build_yt_post(beat: BeatMeta) -> YTPost:
    return YTPost(
        title=build_yt_title(beat),
        description=build_yt_description(beat),
        tags=build_yt_tags(beat),
    )


if __name__ == "__main__":
    from beat_upload import parse_filename
    for fn in [
        "kenny muney type beat THOUGHTS 160 Am.mp3",
        "future x don toliver type beat HOOK 140 Am.mp3",
        "nardowick type beat FRIK 153 G#m.mp3",
    ]:
        b = parse_filename(fn)
        post = build_yt_post(b)
        print("=" * 70)
        print("TITLE:", post.title)
        print()
        print("DESCRIPTION:")
        print(post.description)
        print()
        print("TAGS:", post.tags)
        print()
        print("TG CAPTION:")
        print(build_tg_caption(b))
