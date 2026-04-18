"""Парсер caption для upload drum kit / sample pack / loop pack.

Админ шлёт zip в ЛС бота с caption такого формата:

    kit
    Dark Memphis Drum Kit Vol.1
    1500
    40 сэмплов: 808, kicks, hats, perc.
    Hard Memphis вайб под Key Glock.

Первая строка — тип (kit / pack / loop и их алиасы из licensing.PRODUCT_TYPE_ALIASES).
Вторая — имя продукта.
Третья — цена в Stars (опц., дефолт из licensing.DEFAULT_PRICES).
Всё дальше — описание (до конца caption).

Выбран caption-based подход (без многошагового FSM через ConversationHandler) —
ровно один zip-message даёт полностью оформленный продукт, некуда сбиться.
"""
from __future__ import annotations

from dataclasses import dataclass

import licensing

MAX_SIZE_BYTES = 50 * 1024 * 1024  # Bot API лимит send_document → 50MB

# Минимальная длина имени и описания — ловит очевидно битые caption.
MIN_NAME_LEN = 3
MIN_DESC_LEN = 10


@dataclass
class ProductMeta:
    content_type: str       # "drumkit" / "samplepack" / "looppack"
    name: str
    price_stars: int
    price_usdt: float
    description: str


class CaptionError(ValueError):
    """Бросается если caption не парсится — сообщение передаётся юзеру."""


def parse_caption(caption: str | None) -> ProductMeta:
    """Разбирает caption zip-файла в структурированный ProductMeta.

    Бросает CaptionError с человекочитаемым объяснением при любой ошибке —
    хэндлер в боте показывает это сообщение админу вместе с примером формата.
    """
    if not caption or not caption.strip():
        raise CaptionError(
            "пустая подпись. Пришли zip с caption вида:\n\n"
            "```\nkit\nDark Memphis Drum Kit\n1500\nОписание пака что внутри\n```"
        )

    # Разбиваем максимум на 4 части — всё что после 4-й строки уходит в описание.
    parts = caption.strip().split("\n", 3)
    if len(parts) < 3:
        raise CaptionError(
            f"нужно минимум 3 строки (тип / имя / цена), дали {len(parts)}. "
            "Описание — 4-я строка (опционально)."
        )

    type_raw = parts[0].strip().lower()
    name = parts[1].strip()
    price_raw = parts[2].strip()
    description = parts[3].strip() if len(parts) > 3 else ""

    # 1. Тип
    content_type = licensing.PRODUCT_TYPE_ALIASES.get(type_raw)
    if not content_type:
        allowed = ", ".join(sorted(set(licensing.PRODUCT_TYPE_ALIASES)))
        raise CaptionError(
            f"неизвестный тип «{parts[0].strip()}». Допустимо: {allowed}"
        )

    # 2. Имя
    if len(name) < MIN_NAME_LEN:
        raise CaptionError(
            f"имя продукта слишком короткое (минимум {MIN_NAME_LEN} симв), "
            f"дано: «{name}»"
        )

    # 3. Цена — число в Stars, или "default" для дефолта из DEFAULT_PRICES.
    default_stars, default_usdt = licensing.DEFAULT_PRICES[content_type]
    if price_raw.lower() in ("", "-", "default", "дефолт"):
        price_stars, price_usdt = default_stars, default_usdt
    else:
        try:
            price_stars = int(price_raw)
        except ValueError:
            raise CaptionError(
                f"цена должна быть числом в Stars (3-я строка), дано: «{price_raw}». "
                "Или напиши 'default' для дефолтной цены по типу."
            )
        if price_stars < 100 or price_stars > 50000:
            raise CaptionError(
                f"цена {price_stars}⭐ вне разумных границ (100-50000). "
                "Проверь — возможно опечатка."
            )
        # USDT пересчитываем пропорционально — держим ratio как у default.
        price_usdt = round(price_stars * (default_usdt / default_stars), 1)

    # 4. Описание
    if description and len(description) < MIN_DESC_LEN:
        # Короткое описание допустимо только если реально пустое (5 симв ок,
        # 3 символа — явно недоразумение).
        raise CaptionError(
            f"описание слишком короткое (минимум {MIN_DESC_LEN} симв либо "
            "вообще опусти 4-ю строку)."
        )

    return ProductMeta(
        content_type=content_type,
        name=name,
        price_stars=price_stars,
        price_usdt=price_usdt,
        description=description,
    )


def validate_file(file_name: str | None, file_size: int | None) -> None:
    """Проверяет что файл — zip и в пределах 50MB. Бросает CaptionError."""
    if file_size is None:
        raise CaptionError("не удалось определить размер файла.")
    if file_size > MAX_SIZE_BYTES:
        mb = file_size / (1024 * 1024)
        raise CaptionError(
            f"файл {mb:.1f}MB превышает лимит Bot API (50MB). "
            "Сожми архив или разбей на части."
        )
    if file_name:
        lower = file_name.lower()
        if not (lower.endswith(".zip") or lower.endswith(".rar") or lower.endswith(".7z")):
            raise CaptionError(
                f"формат файла должен быть zip/rar/7z, дано: {file_name}"
            )


CAPTION_EXAMPLE = (
    "📝 Формат подписи к zip:\n\n"
    "```\n"
    "kit                         ← тип (kit / pack / loop)\n"
    "Dark Memphis Drum Kit       ← имя продукта\n"
    "1500                        ← цена в Stars (или 'default')\n"
    "40 сэмплов: 808, hats...    ← описание (опц, 4-я строка)\n"
    "```\n\n"
    "Типы: kit (drumkit, 1500⭐), pack (samplepack, 1000⭐), loop (looppack, 1000⭐)."
)
