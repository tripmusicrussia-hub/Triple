"""Метаданные и валидация для upload drum kit / sample pack / loop pack.

Flow в боте — многошаговый FSM (см. bot.py: cmd_upload_product,
handle_admin_fsm_text). Этот модуль предоставляет только
контейнер-метаданных и валидатор файла — сам pipeline шагов живёт в bot.py.
"""
from __future__ import annotations

from dataclasses import dataclass

MAX_SIZE_BYTES = 50 * 1024 * 1024  # Bot API лимит send_document → 50MB


@dataclass
class ProductMeta:
    content_type: str       # "drumkit" / "samplepack" / "looppack"
    name: str
    price_stars: int
    price_usdt: float
    description: str


class CaptionError(ValueError):
    """Валидационная ошибка файла/метаданных — сообщение передаётся юзеру."""


def validate_file(file_name: str | None, file_size: int | None) -> None:
    """Проверяет что файл — архив и в пределах 50MB. Бросает CaptionError."""
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
