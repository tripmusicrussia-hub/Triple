"""Генерация текста лицензионного соглашения для продажи битов.

MP3 Lease — пока единственный авто-тип. Остальные (WAV/Unlimited/Exclusive)
обсуждаются в ЛС и лицензия выдаётся вручную.
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

_MSK = ZoneInfo("Europe/Moscow")

PRICE_MP3_STARS = 500  # buyer ~$7 (включая Apple/Google + Telegram fees), creator net ~$5
PRICE_MP3_USDT = 7.0   # 7 USDT — сопоставимо buyer-цене Stars. Creator net ~$6.9 после CryptoBot 1%

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
