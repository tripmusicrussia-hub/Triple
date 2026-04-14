"""Генерация текста лицензионного соглашения для продажи битов.

MP3 Lease — пока единственный авто-тип. Остальные (WAV/Unlimited/Exclusive)
обсуждаются в ЛС и лицензия выдаётся вручную.
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

_MSK = ZoneInfo("Europe/Moscow")

PRICE_MP3_STARS = 500  # ~$5 net после Fragment payout

PRODUCER = "TRIPLE FILL"
PRODUCER_CONTACT = "@iiiplfiii"


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
