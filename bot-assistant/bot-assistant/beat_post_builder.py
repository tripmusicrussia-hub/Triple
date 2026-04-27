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
TG_CHANNEL_URL = "https://t.me/iiiplfiii"  # канал для подписки + free sample pack
LANDING_URL = "https://tripmusicrussia-hub.github.io/Triple/"  # YT → landing → bot
YEAR = 2026


def _buy_link(beat_id: int | None, source: str | None = None) -> str:
    """Deep-link в TG-бот на покупку конкретного битка.
    `beat_id=None` → общая ссылка. `source` ставит first-touch ref в combo
    формате `ref_<src>_buy_<id>` (см. cmd_start parsing) — это позволяет
    знать откуда пришёл купивший: yt / ytshorts / tiktok / insta / landing.
    """
    if beat_id and source:
        return f"https://t.me/{BOT_USERNAME}?start=ref_{source}_buy_{beat_id}"
    if beat_id:
        return f"https://t.me/{BOT_USERNAME}?start=buy_{beat_id}"
    if source:
        return f"https://t.me/{BOT_USERNAME}?start=ref_{source}"
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
    """LEGACY title (старый формат). Используется только в старых местах
    которые ещё не мигрировали на canonical_yt_title.
    Новый код должен использовать `canonical_yt_title()`.
    """
    return f'(FREE) {beat.artist_display} Type Beat {YEAR} - "{beat.name}"'


# ── Canonical YT title (юзер-approved шаблон 2026-04-26) ───────────
# Шаблон A:
#   [FREE] {ARTIST} Type Beat 2026 - "{NAME}" | {SCENE} | {BPM} BPM {KEY}
# Examples:
#   [FREE] Kenny Muney Type Beat 2026 - "HEAT" | Memphis | 145 BPM Am
#   [FREE] Obladaet Type Beat 2026 - "DARK" | RU Hard | 152 BPM C#m
#   [FREE] Hard Trap Type Beat 2026 - "Long Beat Name" | 150 BPM (no artist, no key)
# Decisions locked в plan'е:
#   - [FREE] (квадратные брекеты) — 87% топ-каналов используют такой формат
#   - NAME в CAPS если ≤16 символов (короткий = читается как brand);
#     длинное имя сохраняется как есть (CAPS делает его кашей)
#   - KEY skip целиком если пустой — не показываем «??»
#   - SCENE определяется по ARTIST_PROFILE; RU артисты override → "RU Hard"
#   - Year 2026 (текущий, обновляется в YEAR const на стыке года)

# Артисты которых нужно метить как RU (даже если ARTIST_PROFILE.scene != "RU Hard")
_RU_ARTIST_KEYS = {
    "obladaet", "kizaru", "скриптонит", "skriptonit",
    "og buda", "ogbuda", "платина", "platina",
    "slava marlow", "slavamarlow", "big baby tape", "bigbabytape",
    "mayot",
}

# Маппинг scene из ARTIST_PROFILE → canonical short name для title.
# Без override — fallback "Hard Trap".
_SCENE_TITLE_MAP = {
    "Memphis": "Memphis",
    "Detroit": "Detroit",
    "New Orleans": "NOLA",
    "Florida": "FL Hard",
    "Atlanta": "Atlanta",
    "Hard trap": "Hard Trap",  # generic / RU fallback (RU override ниже)
}

YT_TITLE_MAX_LEN = 100  # YT API лимит


def _canonical_scene(beat: BeatMeta) -> str:
    """Возвращает scene-label для title: 'Memphis' / 'Detroit' / 'NOLA' /
    'FL Hard' / 'Atlanta' / 'RU Hard' / 'Hard Trap' (default).

    RU artists override любой scene из ARTIST_PROFILE. Если artist_raw —
    сам по себе scene-tag (после fallback в beat_record_to_meta), возвращаем
    canonical scene name (Memphis → "Memphis"). canonical_yt_title тогда
    скипнет дублирующую scene-секцию.
    """
    raw = (beat.artist_raw or "").split(" x ")[0].strip().lower()
    if raw in _RU_ARTIST_KEYS:
        return "RU Hard"
    # Scene-as-artist fallback (artist_raw из beat_record_to_meta = "memphis"/
    # "detroit"/"atlanta"/"florida"/"nola"/"hard trap")
    direct_scene_map = {
        "memphis": "Memphis",
        "detroit": "Detroit",
        "atlanta": "Atlanta",
        "florida": "FL Hard",
        "nola": "NOLA",
        "neworleans": "NOLA",
        "hard trap": "Hard Trap",
    }
    if raw in direct_scene_map:
        return direct_scene_map[raw]
    prof = _get_profile(raw)
    return _SCENE_TITLE_MAP.get(prof.get("scene", ""), "Hard Trap")


def _canonical_name(name: str) -> str:
    """Имя бита для title: CAPS если ≤16 символов, иначе как есть.

    16 — empirical порог: 'HEAT' / 'DARK NIGHT' / 'GLOCK' читаются brand'ом
    в caps. 'Some Long Beat Title' в caps выглядит как кричащий мусор.
    """
    n = (name or "").strip()
    if not n:
        return "?"
    if len(n) <= 16:
        return n.upper()
    return n


def _canonical_artist(beat: BeatMeta) -> str:
    """Display-имя артиста для title. Если artist_display пуст или unknown —
    fallback на scene-prefix `Hard Trap` (не оставляем title без artist phrase
    т.к. это убивает SEO match для type-beat search queries).
    """
    display = (beat.artist_display or "").strip()
    if display:
        return display
    return "Hard Trap"


def canonical_yt_title(beat: BeatMeta) -> str:
    """Шаблон A: [FREE] {ARTIST} Type Beat {YEAR} - "{NAME}" | {SCENE} | {BPM} BPM {KEY}

    Защита от длины: если результат >YT_TITLE_MAX_LEN — обрезаем хвост
    (сначала KEY, потом BPM, потом SCENE) пока не уложится. Брендирующая
    часть `[FREE] {ARTIST} Type Beat YEAR - "{NAME}"` не режется.

    Anti-duplicate scene: если artist == scene (например после
    `beat_record_to_meta` fallback'а — artist="Memphis" / "Hard Trap") —
    scene не показываем (избегаем `Memphis Type Beat | Memphis | ...`).
    """
    artist = _canonical_artist(beat)
    name_disp = _canonical_name(beat.name)
    scene = _canonical_scene(beat)
    bpm = beat.bpm or 0
    key_short = (beat.key_short or "").strip()

    base = f'[FREE] {artist} Type Beat {YEAR} - "{name_disp}"'
    parts: list[str] = []
    # Скип scene если он повторяет artist phrase (fallback case или generic
    # «Hard Trap» дважды).
    artist_low = artist.lower()
    scene_low = scene.lower()
    if scene and not (
        scene_low == artist_low
        or scene_low in artist_low
        or artist_low in scene_low
    ):
        parts.append(scene)
    if bpm:
        if key_short:
            parts.append(f"{bpm} BPM {key_short}")
        else:
            parts.append(f"{bpm} BPM")
    elif key_short:
        parts.append(key_short)

    full = base + ("".join(f" | {p}" for p in parts) if parts else "")
    if len(full) <= YT_TITLE_MAX_LEN:
        return full

    # Truncation: убираем по одной части с хвоста пока влезет
    while parts and len(base + "".join(f" | {p}" for p in parts)) > YT_TITLE_MAX_LEN:
        parts.pop()
    full = base + ("".join(f" | {p}" for p in parts) if parts else "")
    if len(full) <= YT_TITLE_MAX_LEN:
        return full
    # Brand part всё равно не лезет (имя слишком длинное) — обрезаем имя
    overflow = len(full) - YT_TITLE_MAX_LEN + 3  # +3 для "..."
    if name_disp and overflow > 0:
        truncated_name = name_disp[: max(1, len(name_disp) - overflow)] + "..."
        base = f'[FREE] {artist} Type Beat {YEAR} - "{truncated_name}"'
    return base[:YT_TITLE_MAX_LEN]


def canonical_yt_description_disclaimer(beat_id: int | None = None) -> str:
    """Standalone disclaimer block. Вставляется в начало или конец description.

    Юзер approved 2026-04-26: показывать non-profit clause + redirect на bot.
    """
    buy = _buy_link(beat_id, source="yt") if beat_id else f"https://t.me/{BOT_USERNAME}"
    return (
        "🆓 FREE FOR NON-PROFIT USE ONLY (SoundCloud, demos, freestyle, school).\n"
        "For monetized release (Spotify/Apple/YouTube/iTunes/TikTok) → MP3 lease 1700₽:\n"
        f"👉 {buy}\n\n"
        f'License terms: 100k streams, 2k copies, 1 music video, '
        f'credit "prod. by {BRAND}".'
    )


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
    # source="yt" → first-touch tracking: знаем что юзер пришёл из long YT.
    # Combo формат `?start=ref_yt_buy_<id>` парсится в cmd_start.
    buy = _buy_link(beat_id, source="yt")

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
        f'— What\'s included in MP3 Lease (1500⭐ / 20 USDT)?\n'
        f'Untagged 320kbps MP3 + TXT license: up to 100k streams, 2k paid copies, '
        f'1 music video, non-exclusive.\n\n'
        f'— Can I get WAV / Trackouts / Unlimited / Exclusive?\n'
        f'Yes. DM {TG_HANDLE} — we\'ll discuss terms and pricing.\n\n'
    )

    return (
        f'{hashtags_top}\n\n'
        f'🎁 FREE sample pack + 165+ beats → {TG_CHANNEL_URL}\n'
        f'💰 Instant MP3 Lease (1500⭐ / 20 USDT): {buy}\n'
        f'🎧 All beats + lease: {LANDING_URL}\n'
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
        f'📡 Telegram channel: {TG_CHANNEL_URL}\n'
        f'📱 Telegram DM: {TG_HANDLE}\n'
        f'📱 Instagram: {IG_HANDLE}\n\n'
        f'— tags —\n{keyword_wall}'
    )


def build_shorts_title(beat: BeatMeta) -> str:
    """Title для YT Short. Обязательный `#Shorts` — попадает в Shorts feed.

    Лимит title в YT — 100 симв. Держим запас под `#Shorts` (8 симв).
    """
    base = f'(FREE) {beat.artist_display} Type Beat - "{beat.name}"'
    tag = " #Shorts"
    if len(base) + len(tag) <= 95:
        return base + tag
    # Сокращаем до влезающей длины
    return base[:95 - len(tag)].rstrip() + tag


def build_shorts_description(
    beat: BeatMeta,
    beat_id: int | None = None,
    full_video_url: str | None = None,
) -> str:
    """Короткое описание YT Short с CTA на:
    - Full version (Long YT video) — главный funnel-step из Shorts
    - TG канал — подписка + free sample pack (источник нагона аудитории)
    - Buy link с ref-tracking `?start=ref_ytshorts_buy_<id>`
    """
    prof = _get_profile(beat.artist_raw)
    # source="ytshorts" → знаем что купивший пришёл из Shorts (а не из long).
    buy = _buy_link(beat_id, source="ytshorts")
    primary_slug = beat.artist_display.split(" x ")[0].lower().replace(" ", "")
    scene_slug = prof["scene"].lower().replace(" ", "")
    hashtags = f"#{primary_slug}typebeat #{scene_slug}typebeat #typebeat #Shorts"

    parts = [hashtags, ""]
    parts.append(f'{prof["mood"]} {beat.artist_display} type beat.')
    parts.append(f"⚡ BPM {beat.bpm} · 🎹 {beat.key}")
    parts.append("")
    if full_video_url:
        parts.append(f"🎧 FULL version → {full_video_url}")
    parts.append(f"🎁 FREE sample pack + 165+ beats → {TG_CHANNEL_URL}")
    parts.append(f"💰 MP3 Lease 1500⭐ / 20 USDT → {buy}")
    parts.append(f"💎 WAV / Exclusive → DM {TG_HANDLE}")
    return "\n".join(parts)


def build_tiktok_caption(beat: BeatMeta) -> str:
    """Caption для TikTok-поста. Semi-auto flow: бот шлёт mp4 + этот
    caption админу в ЛС, админ постит в TikTok app руками.

    Дизайн:
    - Первая строка — hook (показывается в первом экране)
    - Вторая — TG mention (TikTok ссылки не кликабельны, но текст видно;
      главный funnel из TikTok идёт через bio-link на @iiiplfiii)
    - Третья — tech-line (BPM + key) для producer-аудитории
    - 8-10 hashtags: discovery (#fyp #foryou) + niche + artist + scene
    - Длина ≤500 chars (TikTok max 2200, но в первом экране ~150)
    """
    prof = _get_profile(beat.artist_raw)
    primary_slug = beat.artist_display.split(" x ")[0].lower().replace(" ", "")
    scene_slug = prof["scene"].lower().replace(" ", "")
    mood_slug = prof.get("mood", "").split(" ")[0].lower().replace(",", "")

    hook = f"🔥 {beat.name} — {beat.artist_display} type beat"
    cta = f"🎁 free pack + full beats → t.me/iiiplfiii"
    tech = f"{beat.bpm} BPM · {beat.key}"

    # Порядок hashtags: discovery → niche → artist → scene → mood
    # Discovery hashtags обязательны для TikTok FYP
    hashtags = [
        "#fyp", "#foryou",
        "#typebeat", "#beatmaker", "#hardtrap",
        f"#{primary_slug}typebeat",
        f"#{scene_slug}",
        f"#bpm{(beat.bpm // 10) * 10}",
    ]
    if mood_slug and mood_slug not in ("", primary_slug, scene_slug):
        hashtags.append(f"#{mood_slug}")
    hashtags.append("#musicproducer")

    # Dedupe сохраняя порядок
    seen, tag_out = set(), []
    for t in hashtags:
        if t not in seen:
            seen.add(t)
            tag_out.append(t)

    return f"{hook}\n{cta}\n{tech}\n\n{' '.join(tag_out)}"


def build_shorts_tags(beat: BeatMeta) -> list[str]:
    """Тэги для YT Short — компактнее обычного набора. Обязательно 'shorts'."""
    prof = _get_profile(beat.artist_raw)
    primary = beat.artist_display.split(" x ")[0].lower()
    scene = prof["scene"].lower()
    tags = [
        "shorts",
        f"{primary} type beat",
        f"{primary} type beat {YEAR}",
        f"{scene} type beat",
        f"hard {scene} instrumental",
        f"{beat.bpm} bpm trap beat",
        "free type beat",
        "hard trap instrumental",
        "type beat",
    ]
    # Related artists
    for rel in prof.get("related_tags", [])[:3]:
        tags.append(f"{rel} type beat")
    # Dedupe сохраняя порядок
    seen, out = set(), []
    for t in tags:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out[:15]


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
            f"💰 MP3 Lease 1500⭐ / 20 USDT / 1700₽ → {_buy_link(beat_id)}"
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


def _tech_line(beat: BeatMeta) -> str:
    """Техническая строка с BPM/key — отдельно от художественного текста."""
    return f"⚡ {beat.bpm} BPM · 🎹 {beat.key}"


def build_tg_caption(beat: BeatMeta, beat_id: int | None = None) -> str:
    """Fallback-шаблон если LLM недоступен. Короткий, нейтральный."""
    return (
        f"{beat.name} — {beat.artist_line}\n"
        f"{_tech_line(beat)}\n\n"
        f"{_bot_footer(beat_id)}\n\n"
        f"{_hashtag_nav(beat)}"
    )


async def build_tg_caption_async(beat: BeatMeta, beat_id: int | None = None) -> tuple[str, str]:
    """LLM-генерация подписи к upload'у бита в voice автора.

    Структура результата:
        <LLM-текст: 1-3 короткие строки в тоне автора>
        <tech-line: BPM / key>
        <footer: каталог + MP3 Lease deep-link>
        <hashtag-nav: #artist #scene #bpmXXX #typebeat>

    LLM пишет ТОЛЬКО художественную часть. BPM/key и прочая техника
    добавляются deterministic'но.

    A/B test (с 2026-04-26): random 50/50 между:
    - "minimal" — LLM-creative (existing) с voice автора
    - "direct"  — deterministic short (без LLM cost) — может зайти лучше
                  если юзеры устали от over-creative captions

    Tracking через post_events.tg_style — SELECT после 2-4 недель покажет
    winner (по views на TG посте / reactions). Тогда переключим на winner.

    Возвращает (full_caption, style_label).
    """
    import random
    # A/B branch: 50% deterministic — экономит LLM call + тестирует gипотезу
    # «short metadata-style зайдёт лучше creative LLM» в type-beat нише.
    if random.random() < 0.5:
        return build_tg_caption(beat, beat_id=beat_id), "direct"
    footer = _bot_footer(beat_id)
    tech = _tech_line(beat)
    try:
        import post_generator
        prof = _get_profile(beat.artist_raw)
        user_msg = (
            f"Короткая подпись к аудио-посту для канала @iiiplfiii. Свежий бит.\n\n"
            f"Метаданные (для контекста, можешь использовать что хочешь):\n"
            f"- Имя: {beat.name}\n"
            f"- Type beat для: {beat.artist_display}\n"
            f"- Сцена: {prof['scene']}\n"
            f"- Настроение: {prof['mood']}\n\n"
            f"Правила:\n"
            f"- 1-3 короткие строки, не больше\n"
            f"- ОБЯЗАТЕЛЬНО упомянуть имя бита «{beat.name}»\n"
            f"- БЕЗ BPM/key — они добавятся отдельно\n"
            f"- БЕЗ хэштегов, БЕЗ ссылок, БЕЗ @handles — тоже добавятся отдельно\n"
            f"- БЕЗ markdown (**, __, ~~)\n"
            f"- 0-1 эмодзи (часто 0)\n"
            f"- Живой тон автора: короткие рваные фразы, первое лицо, можно lowercase\n"
            f"{post_generator.ANTI_AI_BLOCK}\n"
            f"Верни ТОЛЬКО эти 1-3 строки подписи. Без заголовков, без 'Caption:'."
        )
        text = await post_generator._call_llm(user_msg, max_tokens=150, temperature=0.95)
        text = text.strip().strip('"\'').strip()
        # Удаляем хэштег-простыни в конце (LLM иногда добавляет вопреки промпту)
        text = re.sub(r"\n+#[\w_]+(\s+#[\w_]+)*\s*$", "", text).rstrip()
        if beat.name.lower() not in text.lower():
            logger.warning("LLM caption не содержит имя бита, fallback. Got: %r", text[:120])
            return build_tg_caption(beat, beat_id=beat_id), "fallback"
        return f"{text}\n{tech}\n\n{footer}\n\n{_hashtag_nav(beat)}", "minimal"
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
    """Canonical YT post: title через `canonical_yt_title`, description с
    добавленным non-profit disclaimer в начале (юзер approved 2026-04-26).
    """
    return YTPost(
        title=canonical_yt_title(beat),
        description=(
            canonical_yt_description_disclaimer(beat_id) + "\n\n"
            + build_yt_description(beat, beat_id=beat_id, duration_sec=duration_sec)
        ),
        tags=build_yt_tags(beat),
    )


def build_product_channel_post(product: dict) -> str:
    """Текст промо-поста продукта в канал.

    Канал получает краткое описание + type + размер + CTA + хэштеги
    для навигации. Сам zip в канал не постим — только промо-текст
    с кнопкой «открыть в боте» (deep-link ниже в build_product_channel_kb).
    """
    from licensing import PRODUCT_TYPE_LABELS

    ctype = product.get("content_type", "samplepack")
    label = PRODUCT_TYPE_LABELS.get(ctype, "Pack")
    name = product.get("name", "?")
    stars = product.get("price_stars", "?")
    usdt = product.get("price_usdt", "?")
    size_mb = (product.get("file_size") or 0) / (1024 * 1024) if product.get("file_size") else 0
    description = product.get("description") or ""

    # Хэштеги: тип + typebeat универсально.
    type_tag = f"#{ctype}"  # #drumkit / #samplepack / #looppack
    extra_tags = []
    if ctype == "looppack":
        extra_tags.append("#loops")
    if ctype == "samplepack":
        extra_tags.append("#samples")
    hashtags = " ".join([type_tag] + extra_tags + ["#iiiplfiii"])

    usdt_disp = f"{usdt:g} USDT" if isinstance(usdt, (int, float)) else f"{usdt} USDT"

    parts = [
        f"📦 <b>{label}</b> — «{name}»",
        "",
    ]
    if description:
        parts.append(description)
        parts.append("")
    parts.extend([
        f"💾 {size_mb:.1f} MB",
        f"💰 {stars}⭐ / {usdt_disp}",
        "",
        hashtags,
    ])
    return "\n".join(parts)


def build_product_channel_kb(product: dict):
    """Inline-клавиатура под промо-постом продукта в канале.

    Используем url-кнопку с deep-link в бот — клик ведёт в ЛС, где
    открывается карточка продукта с реальными inline-кнопками покупки
    (callback'и из канала в ЛС не ходят надёжно; deep-link — ходит).
    """
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    pid = product["id"]
    stars = product.get("price_stars", "?")
    usdt = product.get("price_usdt", "?")
    usdt_label = f"💵 {usdt:g} USDT" if isinstance(usdt, (int, float)) else "💵 USDT"
    deep = f"https://t.me/{BOT_USERNAME}?start=prod_{pid}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⭐ {stars} — в боте", url=deep),
         InlineKeyboardButton(usdt_label, url=deep)],
        [InlineKeyboardButton("✍️ WAV / Exclusive — DM", url=f"https://t.me/{TG_HANDLE.lstrip('@')}")],
    ])


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
        # Scene/artist/bpm counters считаем только по битам (track/remix/pack
        # не имеют type-beat артиста).
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
    kits = by_type.get("drumkit", 0)
    packs = by_type.get("samplepack", 0)
    loops = by_type.get("looppack", 0)

    counts_line_parts = [f"{total_beats} битов"]
    if tracks:
        counts_line_parts.append(f"{tracks} треков")
    if remixes:
        counts_line_parts.append(f"{remixes} ремиксов")
    if kits or packs or loops:
        packs_total = kits + packs + loops
        counts_line_parts.append(f"{packs_total} паков/китов")
    counts_line = " · ".join(counts_line_parts)

    # Секция Packs — показываем только если есть хоть один продукт.
    packs_block = ""
    if kits or packs or loops:
        pack_tags = []
        if kits:
            pack_tags.append(f"#drumkit ({kits})")
        if packs:
            pack_tags.append(f"#samplepack ({packs})")
        if loops:
            pack_tags.append(f"#looppack ({loops})")
        packs_block = (
            f"📦 Паки и киты\n"
            f"{' · '.join(pack_tags)}\n"
            f"Открыть каталог → https://t.me/{BOT_USERNAME}\n\n"
        )

    return (
        f"🗺 НАВИГАЦИЯ ПО КАНАЛУ\n\n"
        f"Каталог: {counts_line}\n"
        f"Жми хэштег → увидишь все посты по теме.\n\n"
        f"📍 Сцены\n{scenes_line}\n\n"
        f"🎤 Артисты (type beat под)\n{artists_line}\n\n"
        f"⚡ BPM\n{bpm_line}\n\n"
        f"{packs_block}"
        f"🔍 Поиск по каталогу + lease → https://t.me/{BOT_USERNAME}\n"
        f"🎧 Все биты + landing → {LANDING_URL}\n"
        f"📸 Instagram → https://instagram.com/{IG_HANDLE.lstrip('@')}\n"
        f"💎 WAV · Trackouts · Unlimited · Exclusive — DM {TG_HANDLE}\n\n"
        f"💰 MP3 Lease: 1500⭐ / 20 USDT / 1700₽ (untagged MP3 + TXT license, instant delivery)"
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
