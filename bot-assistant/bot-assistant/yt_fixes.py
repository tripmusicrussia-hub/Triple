"""Готовые тексты для batch-обновления провальных type beats в winner-формате.

Запускается один раз через админ-кнопку `/yt_fix`.
После успешного апдейта можно удалить файл — данные осевшие у Google.
"""

BRAND = "TRIPLE FILL"
TG_HANDLE = "@iiiplfiii"
IG_HANDLE = "@iiiplfiii"


def _desc(name: str, artist_line: str, hook: str, best_for: list[str], key: str, bpm: int) -> str:
    best = "\n".join(f"✦ {x}" for x in best_for)
    return (
        f'{artist_line} with a {hook}\n\n'
        f'"{name}" is a hard trap instrumental inspired by this sound. '
        f'The beat combines expressive melodic elements with punchy drums and deep bass, '
        f'creating space for confident, modern vocals.\n\n'
        f'Best for:\n{best}\n\n'
        f'🎧 Key: {key}\n'
        f'⚡ BPM: {bpm}\n\n'
        f'💼 License options:\n✦ MP3\n✦ WAV\n✦ Unlimited\n✦ Exclusive\n\n'
        f'📩 To purchase or lease this beat:\n'
        f'📱 Telegram: {TG_HANDLE}\n'
        f'📱 Instagram: {IG_HANDLE}\n\n'
        f'✦ Free for non-profit use only.\n'
        f'✦ Credit required: (prod. by {BRAND})'
    )


FIXES = {
    "qR1BMDiV8n8": {
        "title": "THOUGHTS — Kenny Muney Type Beat | Fast Memphis Trap Instrumental 2026",
        "description": _desc(
            "THOUGHTS",
            "Fast Memphis-style Kenny Muney type beat with an aggressive, melodic and high-energy vibe. Bouncy 808s, bright melodies and hard trap drums create a catchy modern Memphis sound",
            "fast melodic vibe, high-energy Memphis drums.",
            ["Kenny Muney type vocals", "Fast Memphis flow", "High BPM melodic rap", "Street trap / Chicago-style"],
            "A# minor", 160,
        ),
        "tags": ["kenny muney type beat", "kenny muney type beat 2026", "memphis type beat", "memphis type beat 2026", "key glock type beat", "fast memphis trap", "hard memphis instrumental", "160 bpm trap beat", "a# minor trap beat", "melodic memphis beat", "memphis trap 2026", "big moochie grape type beat", "street trap beat", "free kenny muney type beat"],
    },
    "pqtTbiTwsGY": {
        "title": "BIG FLIPPA — Rob49 Type Beat | Hard Aggressive Trap Instrumental 2026",
        "description": _desc(
            "BIG FLIPPA",
            "Hard aggressive Rob49 type beat with a heavy street vibe. Punchy 808s, dark melodies and fast-paced drums create a powerful New Orleans trap sound",
            "hard street vibe, aggressive New Orleans energy.",
            ["Rob49 type vocals", "Bossman Dlow flow", "Hard street rap", "Aggressive New Orleans trap"],
            "D minor", 152,
        ),
        "tags": ["rob49 type beat", "rob49 type beat 2026", "bossman dlow type beat", "new orleans trap beat", "hard rob49 instrumental", "152 bpm trap beat", "d minor trap beat", "aggressive trap beat", "street trap 2026", "hard trap instrumental", "florida trap beat", "free rob49 type beat", "rob49 x bossman dlow type beat"],
    },
    "8Se8efclu2A": {
        "title": "FRIK — NardoWick Type Beat | Dark Detroit Trap Instrumental 2026",
        "description": _desc(
            "FRIK",
            "Dark aggressive NardoWick type beat with an evil, fast Detroit vibe. Hard 808s, dark melodies and fast-paced drums create a powerful high-energy sound",
            "sinister Detroit atmosphere, fast aggressive drums.",
            ["NardoWick type vocals", "Obladaet flow", "Dark Detroit rap", "Fast aggressive trap"],
            "G# minor", 153,
        ),
        "tags": ["nardowick type beat", "nardowick type beat 2026", "detroit type beat", "detroit trap 2026", "obladaet type beat", "dark detroit trap", "153 bpm trap beat", "g# minor trap beat", "evil trap instrumental", "fast dark trap", "hard detroit beat", "free nardowick type beat", "aggressive detroit trap"],
    },
    "qqIt5_T0sUo": {
        "title": "MEMORY — Key Glock Type Beat | Hard Memphis Trap Instrumental 2026",
        "description": _desc(
            "MEMORY",
            "Hard aggressive Key Glock type beat with a dark, high-energy Memphis vibe. Heavy 808s, fast tempo and hard trap drums create a powerful modern Memphis sound",
            "dark Memphis mood, fast aggressive energy.",
            ["Key Glock type vocals", "Kenny Muney flow", "Fast Memphis rap", "Hard street trap"],
            "D minor", 164,
        ),
        "tags": ["key glock type beat", "key glock type beat 2026", "kenny muney type beat", "memphis type beat 2026", "hard memphis instrumental", "164 bpm trap beat", "d minor trap beat", "fast memphis trap", "memphis trap 2026", "dark memphis beat", "big moochie grape type beat", "free key glock type beat", "hard trap instrumental memphis"],
    },
    "oTYsNdLmdFM": {
        "title": "LESSON — Future Type Beat | Melodic Dark Trap Instrumental 2026",
        "description": _desc(
            "LESSON",
            "Melodic Future type beat with an atmospheric, modern dark vibe. Deep 808s, floating melodies and modern trap bounce create a cold nocturnal sound",
            "cold atmospheric vibe, melodic trap bounce.",
            ["Future type vocals", "Don Toliver flow", "Melodic dark trap", "Atmospheric rap"],
            "D minor", 137,
        ),
        "tags": ["future type beat", "future type beat 2026", "don toliver type beat", "melodic trap beat", "dark melodic trap", "atmospheric trap instrumental", "137 bpm trap beat", "d minor trap beat", "cold trap beat", "future x don toliver type beat", "dark future instrumental", "free future type beat", "modern trap 2026"],
    },
    "ADrGRPGoTgM": {
        "title": "BODRO — Future Type Beat | Dark Melodic Trap Instrumental 2026",
        "description": _desc(
            "BODRO",
            "Dark melodic Future type beat with an atmospheric, modern vibe. Deep 808s, floating melodies and modern trap bounce create a cold nocturnal sound",
            "atmospheric night energy, floating melodies.",
            ["Future type vocals", "Don Toliver flow", "Dark melodic trap", "Atmospheric rap"],
            "E minor", 144,
        ),
        "tags": ["future type beat", "future type beat 2026", "don toliver type beat", "melodic trap beat", "dark melodic trap", "atmospheric trap instrumental", "144 bpm trap beat", "e minor trap beat", "cold trap beat", "future x don toliver type beat", "modern trap 2026", "free future type beat", "dark future instrumental"],
    },
    "8gTcLOqYOeo": {
        "title": "HOOK — Future x Don Toliver Type Beat | Dark Melodic Trap Instrumental 2026",
        "description": _desc(
            "HOOK",
            "Dark melodic Future x Don Toliver type beat with a cold, nocturnal vibe. Floating melodies, deep 808s and modern trap bounce create an emotional atmospheric sound",
            "emotional atmospheric mood, cold night energy.",
            ["Future x Don Toliver type vocals", "Dark melodic trap", "Emotional & atmospheric rap", "Night / introspective tracks"],
            "A minor", 140,
        ),
        "tags": ["future x don toliver type beat", "future type beat", "don toliver type beat", "dark melodic trap beat", "melodic trap instrumental", "future type beat 2026", "don toliver type beat 2026", "atmospheric trap beat", "a minor trap beat", "140 bpm trap beat", "cold trap beat", "night trap instrumental", "free future x don toliver type beat"],
    },
    "Mx4BN5GKg1I": {
        "title": "HOOK V2 — Future x Don Toliver Type Beat | Dark Melodic Trap Instrumental 2026",
        "description": _desc(
            "HOOK V2",
            "Dark melodic Future x Don Toliver type beat with a cold, nocturnal vibe. Floating melodies, deep 808s and modern trap bounce create an emotional atmospheric sound",
            "emotional atmospheric mood, cold night energy.",
            ["Future x Don Toliver type vocals", "Dark melodic trap", "Emotional & atmospheric rap", "Night / introspective tracks"],
            "A minor", 140,
        ),
        "tags": ["future x don toliver type beat", "future type beat", "don toliver type beat", "dark melodic trap beat", "melodic trap instrumental", "future type beat 2026", "don toliver type beat 2026", "atmospheric trap beat", "a minor trap beat", "140 bpm trap beat", "cold trap beat", "night trap instrumental", "free future x don toliver type beat"],
    },
    "_3zeyP1by-4": {
        "title": "CRIMINAL — Future Type Beat | Dark Trap Instrumental 2026",
        "description": _desc(
            "CRIMINAL",
            "Dark and cold Future type beat with a criminal night vibe. Hard 808s, heavy drums and a modern dark trap atmosphere",
            "criminal night energy, cold 808 bounce.",
            ["Future type vocals", "Dark melodic trap", "Street & melodic rap", "Night / criminal mood tracks"],
            "D minor", 144,
        ),
        "tags": ["future type beat", "future type beat dark", "dark future type beat", "future type beat 2026", "dark trap beat", "trap instrumental", "future instrumental", "melodic trap beat", "cold trap beat", "dark trap instrumental", "criminal trap beat", "street trap beat", "d minor trap beat", "144 bpm trap beat", "type beat future", "free future type beat"],
    },
    "Y6efDPKaesw": {
        "title": "SHUTTLE — Future Type Beat | Space Trap Instrumental 2026",
        "description": _desc(
            "SHUTTLE",
            "Futuristic Future type beat with a space trap atmosphere. Floating melodies, hard drums and a cold, modern sound create a levitating nocturnal vibe",
            "space atmosphere, futuristic cold mood.",
            ["Future type vocals", "Space trap / futuristic trap", "Melodic & atmospheric rap", "Cold mood tracks"],
            "G minor", 138,
        ),
        "tags": ["future type beat", "future type beat 2026", "space future type beat", "space trap beat", "futuristic trap beat", "trap instrumental", "future instrumental", "melodic trap beat", "cold trap beat", "atmospheric trap beat", "g minor trap beat", "138 bpm trap beat", "type beat future", "free future type beat"],
    },
}
