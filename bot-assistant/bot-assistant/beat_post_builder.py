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
BOT_USERNAME = "triplekillpost_bot"  # для deep-link покупки из TG
LANDING_URL = "https://tripmusicrussia-hub.github.io/Triple/"  # YT → landing → bot
YEAR = 2026


def _buy_link(beat_id: int | None) -> str:
    """Deep-link в TG-бот на покупку конкретного битка. beat_id=None → общая ссылка."""
    if beat_id:
        return f"https://t.me/{BOT_USERNAME}?start=buy_{beat_id}"
    return f"https://t.me/{BOT_USERNAME}"

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
    # Memphis core + related
    "young dolph": {
        "scene": "Memphis",
        "mood": "Hard Memphis",
        "adjectives": ["hard", "aggressive", "street"],
        "best_for": ["Young Dolph type vocals", "Key Glock flow", "Memphis trap", "Hard street rap"],
        "related_tags": ["youngdolph", "keyglock", "kennymuney", "memphis"],
    },
    "pooh shiesty": {
        "scene": "Memphis",
        "mood": "Hard Memphis",
        "adjectives": ["hard", "aggressive", "dark"],
        "best_for": ["Pooh Shiesty type vocals", "Key Glock flow", "Memphis street trap", "Gucci Mane style"],
        "related_tags": ["poohshiesty", "keyglock", "memphis"],
    },
    "moneybagg yo": {
        "scene": "Memphis",
        "mood": "Hard melodic",
        "adjectives": ["hard", "melodic", "street"],
        "best_for": ["Moneybagg Yo type vocals", "Memphis melodic flow", "Hard trap", "Street rap"],
        "related_tags": ["moneybaggyo", "keyglock", "memphis"],
    },
    "finesse2tymes": {
        "scene": "Memphis",
        "mood": "Hard street",
        "adjectives": ["hard", "aggressive", "raw"],
        "best_for": ["Finesse2Tymes type vocals", "Memphis street trap", "Hard raw rap"],
        "related_tags": ["finesse2tymes", "moneybaggyo", "memphis"],
    },
    "three 6 mafia": {
        "scene": "Memphis",
        "mood": "Dark Memphis classic",
        "adjectives": ["dark", "sinister", "classic"],
        "best_for": ["Three 6 Mafia type vocals", "Classic Memphis horrorcore", "Dark trap", "Juicy J style"],
        "related_tags": ["three6mafia", "juicyj", "memphis"],
    },
    # Atlanta / universal dark
    "21 savage": {
        "scene": "Atlanta",
        "mood": "Dark aggressive",
        "adjectives": ["dark", "aggressive", "cold"],
        "best_for": ["21 Savage type vocals", "Metro Boomin style", "Dark Atlanta trap", "Cold aggressive rap"],
        "related_tags": ["21savage", "metroboomin", "atlanta"],
    },
    # Detroit
    "babytron": {
        "scene": "Detroit",
        "mood": "Dark comedic",
        "adjectives": ["dark", "quirky", "aggressive"],
        "best_for": ["BabyTron type vocals", "Detroit meme-rap", "ShittyBoyz style", "Dark sample trap"],
        "related_tags": ["babytron", "detroit"],
    },
    "tee grizzley": {
        "scene": "Detroit",
        "mood": "Hard Detroit",
        "adjectives": ["hard", "aggressive", "street"],
        "best_for": ["Tee Grizzley type vocals", "Detroit hard street", "Trap drums", "Gritty rap"],
        "related_tags": ["teegrizzley", "detroit"],
    },
    # Female hard
    "glorilla": {
        "scene": "Memphis",
        "mood": "Hard female",
        "adjectives": ["hard", "aggressive", "street"],
        "best_for": ["GloRilla type vocals", "Memphis female hard rap", "Sexyy Red style", "Hard female trap"],
        "related_tags": ["glorilla", "sexyyred", "memphis"],
    },
    # RU — dark/hard
    "kizaru": {
        "scene": "Hard trap",
        "mood": "Dark atmospheric",
        "adjectives": ["dark", "atmospheric", "cold"],
        "best_for": ["Kizaru type vocals", "Dark RU trap", "Atmospheric hard rap", "Night drive beats"],
        "related_tags": ["kizaru", "bigbabytape", "obladaet"],
    },
    "скриптонит": {
        "scene": "Hard trap",
        "mood": "Dark melodic",
        "adjectives": ["dark", "melodic", "cold"],
        "best_for": ["Скриптонит type vocals", "RU dark melodic rap", "Atmospheric trap", "Cold melodic flow"],
        "related_tags": ["skriptonit", "obladaet", "kizaru"],
    },
    "og buda": {
        "scene": "Hard trap",
        "mood": "Dark aggressive",
        "adjectives": ["dark", "aggressive", "street"],
        "best_for": ["OG Buda type vocals", "RU dark aggressive rap", "Hard trap", "Street flow"],
        "related_tags": ["ogbuda", "obladaet", "mayot"],
    },
    "платина": {
        "scene": "Hard trap",
        "mood": "Dark aggressive",
        "adjectives": ["dark", "aggressive", "fast"],
        "best_for": ["Платина type vocals", "RU fast aggressive trap", "Dark drums", "Hard street flow"],
        "related_tags": ["platina", "obladaet", "mayot"],
    },
    "slava marlow": {
        "scene": "Hard trap",
        "mood": "Melodic dark",
        "adjectives": ["melodic", "dark", "cold"],
        "best_for": ["Slava Marlow type vocals", "RU melodic dark trap", "Atmospheric rap", "Modern RU hip hop"],
        "related_tags": ["slavamarlow", "obladaet"],
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


def _fmt_ts(seconds: float) -> str:
    """Секунды → 'M:SS' для YT chapters/timestamps."""
    total = int(round(seconds))
    return f"{total // 60}:{total % 60:02d}"


def build_yt_description(
    beat: BeatMeta,
    beat_id: int | None = None,
    duration_sec: float | None = None,
) -> str:
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
    buy = _buy_link(beat_id)

    # Hashtag-блок в начале — YT показывает до 3 hashtag'ов над тайтлом.
    # 3 tags: artist + scene + 'typebeat' — универсально и в нише.
    primary_slug = beat.artist_display.split(" x ")[0].lower().replace(" ", "")
    scene_slug = prof["scene"].lower().replace(" ", "")
    hashtags_top = f"#{primary_slug}typebeat #{scene_slug}typebeat #typebeat"

    # Keyword wall в конец — boost SEO через keyword-density.
    # Генерим из артиста/сцены/года/коллаб-артистов.
    kw_base = [
        f"{primary_slug} type beat",
        f"{primary_slug} type beat {YEAR}",
        f"free {primary_slug} type beat",
        f"hard {primary_slug} type beat",
        f"{scene_slug} type beat",
        f"{scene_slug} type beat {YEAR}",
        f"hard {scene_slug} type beat",
        f"dark {scene_slug} type beat",
        f"{beat.bpm} bpm trap beat",
        f"{beat.key_short.lower()} trap beat",
        "free type beat",
        f"free type beat {YEAR}",
        "hard trap instrumental",
        "trap type beat",
        "free trap beat",
        "type beat",
    ]
    # Related artists
    for rel in prof.get("related_tags", []):
        kw_base.extend([f"{rel} type beat", f"free {rel} type beat"])
    # Dedupe + формируем comma-wall
    seen = set()
    kw_wall = []
    for k in kw_base:
        if k not in seen:
            seen.add(k)
            kw_wall.append(k)
    keyword_wall = ", ".join(kw_wall)

    # Timestamps — retention через chapters (winner-паттерн ниши).
    # Опциональный блок: если duration не известен — пропускаем без вреда.
    timestamps_block = ""
    if duration_sec and duration_sec > 10:
        timestamps_block = (
            f'⏱ Timestamps:\n'
            f'0:00 — "{beat.name}"\n'
            f'{_fmt_ts(duration_sec)} — Thanks for listening all the way through\n\n'
        )

    # FAQ-блок (winner-паттерн ниши) — снимает 80% DM-возражений ДО того как юзер
    # уходит читать. Вопросы пишем в реальном user-voice, не в маркетинг-voice.
    faq_block = (
        f'❓ FAQ\n\n'
        f'— What does (FREE) mean?\n'
        f'Beat is free for NON-PROFIT use only. Credit (prod. by {BRAND}) required. '
        f'For streaming / monetization / profit — you MUST purchase a lease.\n\n'
        f'— How do I get the untagged file after purchase?\n'
        f'Instantly. The bot ({buy}) sends untagged MP3 + TXT license right after '
        f'Telegram Stars / USDT payment clears.\n\n'
        f'— What\'s included in MP3 Lease (500⭐ / 7 USDT)?\n'
        f'Untagged 320kbps MP3 + TXT license: up to 100k streams, 2k paid copies, '
        f'1 music video, non-exclusive.\n\n'
        f'— Can I get WAV / Trackouts / Unlimited / Exclusive?\n'
        f'Yes. DM {TG_HANDLE} — we\'ll discuss terms and pricing.\n\n'
    )

    return (
        f'{hashtags_top}\n\n'
        f'🎧 ALL BEATS + LEASE: {LANDING_URL}\n'
        f'💰 Instant MP3 Lease (500⭐ / 7 USDT): {buy}\n'
        f'💎 WAV · Unlimited · Exclusive — DM {TG_HANDLE}\n\n'
        f'{artist_line} {energy_line}.\n\n'
        f'"{beat.name}" is a hard trap instrumental inspired by this sound. '
        f'The beat combines expressive melodic elements with punchy drums and deep bass, '
        f'creating space for confident, modern vocals.\n\n'
        f'Best for:\n{best}\n\n'
        f'🎧 Key: {beat.key}\n'
        f'⚡ BPM: {beat.bpm}\n\n'
        f'{timestamps_block}'
        f'⚠️ FREE FOR NON-PROFIT ONLY.\n'
        f'Using for profit/monetization/streaming → you MUST purchase a lease.\n'
        f'Credit required: (prod. by {BRAND})\n\n'
        f'{faq_block}'
        f'📩 Contact:\n'
        f'📱 Telegram: {TG_HANDLE}\n'
        f'📱 Instagram: {IG_HANDLE}\n\n'
        f'— tags —\n{keyword_wall}'
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


def _bot_footer(beat_id: int | None = None) -> str:
    """Стандартный футер для TG-постов: ссылка в бот + CTA покупки.

    Показывается в channel-постах чтобы подписчики могли листать каталог и
    покупать биты прямо из канала.
    """
    catalog = f"https://t.me/{BOT_USERNAME}"
    if beat_id:
        return (
            f"🎧 Весь каталог → {catalog}\n"
            f"💰 MP3 Lease 500⭐ / 7 USDT → {_buy_link(beat_id)}"
        )
    return f"🎧 Весь каталог + lease → {catalog}"


def _hashtag_nav(beat: BeatMeta) -> str:
    """Навигационная строка хэштегов для TG-постов.

    TG делает #tag кликабельным внутри канала → юзер жмёт #memphis и видит
    все memphis-биты в ленте. Это работает вместо "категорий", которых в
    каналах нет. Держим ≤4 тегов, чтобы не засорять caption.

    Формат: `#<artist> #<scene> #bpm<bucket> #typebeat`
    Пример: `#kennymuney #memphis #bpm160 #typebeat`
    """
    prof = _get_profile(beat.artist_raw)
    primary = beat.artist_display.split(" x ")[0].lower().replace(" ", "")
    scene = prof["scene"].lower().replace(" ", "")
    bpm_bucket = (beat.bpm // 10) * 10 if beat.bpm else 140
    # Dedupe на случай если артист и сцена совпадают (редко, но).
    tags = [f"#{primary}", f"#{scene}", f"#bpm{bpm_bucket}", "#typebeat"]
    seen, out = set(), []
    for t in tags:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return " ".join(out)


def build_tg_caption(beat: BeatMeta, beat_id: int | None = None) -> str:
    """Fallback-шаблон если LLM недоступен. Короткий, нейтральный."""
    return (
        f"{beat.name} — {beat.artist_line}\n\n"
        f"🎧 {beat.key}  ⚡ {beat.bpm} BPM\n\n"
        f"{_bot_footer(beat_id)}\n\n"
        f"{_hashtag_nav(beat)}"
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


async def build_tg_caption_async(beat: BeatMeta, beat_id: int | None = None) -> tuple[str, str]:
    """LLM-генерация подписи в голосе iiiplfiii-voice + bot-footer.

    LLM пишет основную часть (в voice автора), после неё deterministic
    bot-footer с ссылкой на бот для каталога и покупки. Возвращает
    (text, style_label). При сбое LLM — fallback на шаблон.
    """
    footer = _bot_footer(beat_id)
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
            f"- 2-5 строк, plain text. ЗАПРЕЩЕНО: хэштеги (#что_угодно), markdown (**, __, ~~), кавычки вокруг всего поста\n"
            f"- От первого лица, в голосе автора (см. system-prompt)\n"
            f"- ОБЯЗАТЕЛЬНО упомянуть имя бита «{beat.name}» и артиста «{beat.artist_display}»\n"
            f"- BPM и key — где-то в тексте (можно одной строкой типа «{beat.key} · {beat.bpm}»)\n"
            f"- Минимум эмодзи (0-3 шт), только из whitelist (🎧 🎵 🔥 🎹 ⚡)\n"
            f"- НЕ ПИШИ ссылки / контакты / @handles — они добавляются автоматически снизу\n"
            f"- Никаких «Пиши в ЛС за beat» — звучит как продаван.\n"
            f"{post_generator.ANTI_AI_BLOCK}\n"
            f"Верни ТОЛЬКО подпись, без преамбулы, без кавычек."
        )
        text = await post_generator._call_llm(user_msg, max_tokens=200, temperature=0.95)
        text = text.strip().strip('"\'')
        # Удаляем хэштег-простыни в конце (LLM иногда добавляет вопреки промпту)
        text = re.sub(r"\n+#[\w_]+(\s+#[\w_]+)*\s*$", "", text).rstrip()
        if beat.name.lower() not in text.lower():
            logger.warning("LLM caption не содержит имя бита, fallback. Got: %r", text[:120])
            return build_tg_caption(beat, beat_id=beat_id), "fallback"
        return f"{text}\n\n{footer}\n\n{_hashtag_nav(beat)}", style_label
    except Exception as e:
        logger.warning("build_tg_caption_async failed: %s — fallback", e)
        return build_tg_caption(beat, beat_id=beat_id), "fallback"


@dataclass
class YTPost:
    title: str
    description: str
    tags: list[str]


def build_yt_post(
    beat: BeatMeta,
    beat_id: int | None = None,
    duration_sec: float | None = None,
) -> YTPost:
    return YTPost(
        title=build_yt_title(beat),
        description=build_yt_description(beat, beat_id=beat_id, duration_sec=duration_sec),
        tags=build_yt_tags(beat),
    )


def _slugify(s: str) -> str:
    """lowercase + убрать пробелы/дефисы — для сравнения тегов."""
    return re.sub(r"[\s_\-]+", "", s.lower())


# Канонический список сцен (для классификации тегов каталога).
_KNOWN_SCENES = {
    "memphis", "detroit", "atlanta", "neworleans", "florida",
    "hardtrap", "phonk", "drill",
}


def build_pinned_hub() -> str:
    """Закреп-пост канала: навигация по каталогу через кликабельные хэштеги.

    TG рендерит #tag как ссылку «все посты канала с этим хэштегом». Это
    работает вместо «категорий», которых в каналах нет. Кайф в том, что
    юзер жмёт #memphis — и видит ровно те биты, которые ты постил под
    этим тегом.

    Генерируется из `beats_db.BEATS_CACHE`: top-сцены, top-артисты,
    bpm-бакеты, соотношение beat/track/remix. Обновлять раз в 2 недели
    (новые сцены/артисты появляются органически).
    """
    import beats_db
    from collections import Counter

    # Канонические артисты — берём ключи из ARTIST_PROFILE, слагифицируем.
    canonical_artists = {_slugify(k): k for k in ARTIST_PROFILE}

    scene_counter = Counter()
    artist_counter = Counter()
    bpm_counter = Counter()
    by_type = Counter()

    for b in beats_db.BEATS_CACHE:
        ctype = b.get("content_type", "beat")
        by_type[ctype] += 1
        # Считаем только beat'ы для hub (treck/remix не продаются)
        if ctype != "beat":
            continue
        for raw_tag in b.get("tags", []):
            tl = _slugify(raw_tag)
            if tl in _KNOWN_SCENES:
                scene_counter[tl] += 1
            elif tl in canonical_artists:
                artist_counter[tl] += 1
        bpm = b.get("bpm")
        if isinstance(bpm, int) and 80 <= bpm <= 200:
            bpm_counter[(bpm // 10) * 10] += 1

    top_scenes = [s for s, _ in scene_counter.most_common(8)]
    top_artists = [a for a, _ in artist_counter.most_common(10)]
    top_bpm = sorted((b for b, _ in bpm_counter.most_common(6)))

    scenes_line = " ".join(f"#{s}" for s in top_scenes) or "#memphis #detroit #hardtrap"
    artists_line = " ".join(f"#{a}" for a in top_artists) or "#keyglock #kennymuney #obladaet"
    bpm_line = " ".join(f"#bpm{b}" for b in top_bpm) or "#bpm130 #bpm140 #bpm150 #bpm160"

    total_beats = by_type.get("beat", 0)
    tracks = by_type.get("track", 0)
    remixes = by_type.get("remix", 0)

    counts_line_parts = [f"{total_beats} битов"]
    if tracks:
        counts_line_parts.append(f"{tracks} треков")
    if remixes:
        counts_line_parts.append(f"{remixes} ремиксов")
    counts_line = " · ".join(counts_line_parts)

    return (
        f"🗺 НАВИГАЦИЯ ПО КАНАЛУ\n\n"
        f"Каталог: {counts_line}\n"
        f"Жми хэштег → увидишь все посты по теме.\n\n"
        f"📍 Сцены\n{scenes_line}\n\n"
        f"🎤 Артисты (type beat под)\n{artists_line}\n\n"
        f"⚡ BPM\n{bpm_line}\n\n"
        f"🔍 Поиск по каталогу + lease → https://t.me/{BOT_USERNAME}\n"
        f"🎧 Все биты + landing → {LANDING_URL}\n"
        f"💎 WAV · Trackouts · Unlimited · Exclusive — DM {TG_HANDLE}\n\n"
        f"💰 MP3 Lease: 500⭐ / 7 USDT (untagged MP3 + TXT license, instant delivery)"
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
