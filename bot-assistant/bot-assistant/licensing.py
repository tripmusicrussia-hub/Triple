"""Генерация текста лицензионного соглашения для продажи битов.

MP3 Lease — пока единственный авто-тип. Остальные (WAV/Unlimited/Exclusive)
обсуждаются в ЛС и лицензия выдаётся вручную.
"""
from __future__ import annotations

import os
from datetime import datetime
from zoneinfo import ZoneInfo

_MSK = ZoneInfo("Europe/Moscow")

PRICE_MP3_STARS = 1500  # buyer ~$20 (standard mid-tier type-beat pricing). Creator net ~$15 после Telegram/Apple/Google fees
PRICE_MP3_USDT = 20.0   # 20 USDT — рыночный стандарт MP3 lease 2026. Creator net ~$19.8 после CryptoBot 1%
PRICE_MP3_RUB = 1700    # ≈ $20 при курсе 85₽/$. Paritet с USDT/Stars. Через YooKassa (MIR/СБП/карты), комиссия 3.5% → net ≈ 1640₽

# Bundle: 3 бита одной транзакцией. Скидка ~12% от 3×single (5100→4500₽).
# Aim: AOV boost. Юзер выбирает 3 бита (anchor + 2) → одна оплата → 3 mp3 + 1 license.
PRICE_BUNDLE3_STARS = int(os.getenv("PRICE_BUNDLE3_STARS", "4000"))  # 4500₽ ≈ 4000⭐ при ~1.13₽/⭐ net
PRICE_BUNDLE3_USDT = float(os.getenv("PRICE_BUNDLE3_USDT", "55.0"))  # 55 USDT (~$55) скидка с 60
PRICE_BUNDLE3_RUB = int(os.getenv("PRICE_BUNDLE3_RUB", "4500"))      # 4500₽ vs 5100 single×3

# Сведение треков «под ключ» (mixing + mastering). Клиент присылает стемы WAV
# в DM @iiiplfiii после оплаты → 3-5 рабочих дней → готовый master-файл.
PRICE_MIX_STARS = 4500  # ≈ $60 (~3× MP3 lease)
PRICE_MIX_USDT = 60.0
PRICE_MIX_RUB = 5000

# Drum kit / sample pack / loop pack — цены в сравнении с рынком DIY-продюсеров.
PRICE_KIT_STARS = 1500
PRICE_KIT_USDT = 15.0
PRICE_PACK_STARS = 1000
PRICE_PACK_USDT = 10.0

PRODUCER = "TRIPLE FILL"
PRODUCER_CONTACT = "@iiiplfiii"

# Человекочитаемые лейблы + дефолтные цены по типу продукта.
# При upload админ может переопределить цену (3-я строка caption), но
# лейбл фиксирован — используется везде в UI/каталоге/license.
PRODUCT_TYPE_LABELS: dict[str, str] = {
    "drumkit":    "Drum Kit",
    "samplepack": "Sample Pack",
    "looppack":   "Loop Pack",
}

DEFAULT_PRICES: dict[str, tuple[int, float]] = {
    "drumkit":    (PRICE_KIT_STARS, PRICE_KIT_USDT),
    "samplepack": (PRICE_PACK_STARS, PRICE_PACK_USDT),
    "looppack":   (PRICE_PACK_STARS, PRICE_PACK_USDT),
}

# Алиасы в caption (1-я строка) → канонический content_type.
PRODUCT_TYPE_ALIASES: dict[str, str] = {
    "kit":        "drumkit",
    "drumkit":    "drumkit",
    "drums":      "drumkit",
    "pack":       "samplepack",
    "samplepack": "samplepack",
    "samples":    "samplepack",
    "loop":       "looppack",
    "loops":      "looppack",
    "looppack":   "looppack",
}


def mp3_lease_text(
    buyer_name: str,
    buyer_tg_id: int,
    beat_name: str,
    bpm: int | str | None,
    key: str | None,
    payment_charge_id: str,
) -> str:
    """Текст non-exclusive MP3 lease. Вкладывается как .txt рядом с битом."""
    now = datetime.now(_MSK)
    bpm_disp = str(bpm) if bpm else "—"
    key_disp = key or "—"
    license_id = f"MP3-{now.strftime('%Y%m%d')}-{buyer_tg_id}"
    return f"""MP3 LEASE AGREEMENT
================================

License ID:      {license_id}
Date:            {now.strftime("%d.%m.%Y %H:%M МСК")}
Producer:        {PRODUCER} ({PRODUCER_CONTACT})
Beat:            {beat_name}
BPM / Key:       {bpm_disp} / {key_disp}

Licensee:        {buyer_name}
Telegram ID:     {buyer_tg_id}
Payment charge:  {payment_charge_id}

USAGE RIGHTS (non-exclusive):
  • До 100 000 стримов на всех DSP (Spotify / YouTube Music / Apple / Яндекс и т.д.)
  • До 2 000 копий (физических или цифровых)
  • 1 музыкальное видео (без монетизации)
  • Использование для live-performances — без ограничений
  • Credit обязателен: "prod. by {PRODUCER}"

RESTRICTIONS:
  • Запрещена перепродажа или передача лицензии третьим лицам.
  • Запрещено регистрировать бит / инструментал отдельно на себя.
  • Бит остаётся в продаже — продюсер может лицензировать его другим артистам.
  • При превышении лимитов — апгрейд до WAV/Unlimited/Exclusive обязателен.

TERMINATION:
  Лицензия вступает в силу с момента оплаты. Действует бессрочно при соблюдении
  условий. Нарушение — расторжение и возврат всех копий.

CONTACT:
  {PRODUCER_CONTACT} (Telegram) — вопросы, апгрейд лицензии, эксклюзив.
"""


def bundled_mp3_lease_text(
    buyer_name: str,
    buyer_tg_id: int,
    beats: list[dict],
    payment_charge_id: str,
) -> str:
    """Один лицензионный документ на bundle из нескольких битов.

    `beats` — список dict-ов с полями name/bpm/key/id (каталог-формат).
    Условия идентичны single MP3 lease, но действуют на каждый бит из bundle.
    """
    now = datetime.now(_MSK)
    license_id = f"BUNDLE-{now.strftime('%Y%m%d')}-{buyer_tg_id}"

    beats_block_lines = []
    for b in beats:
        bpm_disp = str(b.get("bpm")) if b.get("bpm") else "—"
        key_disp = b.get("key") or "—"
        beats_block_lines.append(f"  • {b['name']:<40s}  {bpm_disp:>4s} BPM  {key_disp}")
    beats_block = "\n".join(beats_block_lines)

    return f"""MP3 BUNDLE LEASE AGREEMENT
================================

License ID:      {license_id}
Date:            {now.strftime("%d.%m.%Y %H:%M МСК")}
Producer:        {PRODUCER} ({PRODUCER_CONTACT})
Total beats:     {len(beats)}

BEATS IN THIS BUNDLE:
{beats_block}

Licensee:        {buyer_name}
Telegram ID:     {buyer_tg_id}
Payment charge:  {payment_charge_id}

USAGE RIGHTS (non-exclusive, на каждый бит из bundle):
  • До 100 000 стримов на всех DSP (Spotify / YouTube Music / Apple / Яндекс и т.д.)
  • До 2 000 копий (физических или цифровых)
  • 1 музыкальное видео (без монетизации)
  • Использование для live-performances — без ограничений
  • Credit обязателен: "prod. by {PRODUCER}"

RESTRICTIONS:
  • Запрещена перепродажа или передача лицензии третьим лицам.
  • Запрещено регистрировать биты / инструменталы отдельно на себя.
  • Биты остаются в продаже — продюсер может лицензировать их другим артистам.
  • При превышении лимитов — апгрейд до WAV/Unlimited/Exclusive обязателен.

TERMINATION:
  Лицензия вступает в силу с момента оплаты. Действует бессрочно при соблюдении
  условий. Нарушение — расторжение и возврат всех копий.

CONTACT:
  {PRODUCER_CONTACT} (Telegram) — вопросы, апгрейд лицензии, эксклюзив.
"""


def product_license_text(
    buyer_name: str,
    buyer_tg_id: int,
    product_type: str,
    product_name: str,
    payment_charge_id: str,
) -> str:
    """Non-exclusive лицензия на drum kit / sample pack / loop pack.

    Отличается от mp3-lease тем, что sample/loop/kit — это набор исходных
    звуков, а не готовое произведение. Артист использует их В СВОИХ
    продукциях — лицензия покрывает неограниченное использование этих
    сэмплов в производных треках (non-exclusive, без royalties продюсеру).
    Запрет — перепродавать сами сэмплы / включать их в свои паки.
    """
    now = datetime.now(_MSK)
    type_label = PRODUCT_TYPE_LABELS.get(product_type, product_type.title())
    license_id = f"{product_type.upper()}-{now.strftime('%Y%m%d')}-{buyer_tg_id}"
    return f"""{type_label.upper()} LICENSE
================================

License ID:      {license_id}
Date:            {now.strftime("%d.%m.%Y %H:%M МСК")}
Producer:        {PRODUCER} ({PRODUCER_CONTACT})
Product:         {product_name} ({type_label})

Licensee:        {buyer_name}
Telegram ID:     {buyer_tg_id}
Payment charge:  {payment_charge_id}

USAGE RIGHTS (non-exclusive):
  • Неограниченное использование сэмплов в своих музыкальных произведениях
    (треки, инструменталы, beats, remixes) — без лимита копий / стримов / шоу.
  • Коммерческое использование разрешено (продажа треков, стриминг на DSP,
    лицензирование производных битов другим артистам).
  • Credit на самого артиста не требуется, но welcomed: "samples: {PRODUCER}"

RESTRICTIONS:
  • Запрещена перепродажа отдельных сэмплов / лупов / hits как исходников.
  • Запрещено включать звуки пака в свои sample packs / kits / библиотеки
    для дальнейшей продажи.
  • Запрещена передача архива третьим лицам. Каждая копия — для одного
    покупателя (TG ID выше = обладатель лицензии).
  • При обнаружении распространения — отзыв лицензии без возврата средств.

TERMINATION:
  Лицензия бессрочная при соблюдении условий. Нарушение = расторжение.

CONTACT:
  {PRODUCER_CONTACT} (Telegram) — вопросы, другие продукты, эксклюзивы.
"""
