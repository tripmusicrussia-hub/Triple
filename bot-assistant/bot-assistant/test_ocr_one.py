# -*- coding: utf-8 -*-
"""
Тест OCR одной накладной за раз. Запуск:
    python test_ocr_one.py test_elita.jpg

Нужен чтобы отлаживать один документ без rate-limit'ов от прогона 5 подряд.
"""
import asyncio
import os
import sys
import logging

sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

_env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_env_path):
    with open(_env_path, encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if not _line or _line.startswith("#") or "=" not in _line:
                continue
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

from yandex_ocr import yandex_read_text
from sigma_api import recognize_invoice


async def main():
    if len(sys.argv) < 2:
        print("Usage: python test_ocr_one.py <image.jpg> [--ocr-only]")
        return
    path = sys.argv[1]
    ocr_only = "--ocr-only" in sys.argv
    if not os.path.exists(path):
        print(f"Файл не найден: {path}")
        return
    with open(path, "rb") as f:
        image_bytes = f.read()

    print(f"=== OCR: {path} ===")
    text = await yandex_read_text(image_bytes, model="table")
    print(text)
    print(f"\n--- {len(text.splitlines())} строк, {len(text)} символов ---\n")

    if ocr_only:
        return

    print("=== LLM → структура ===")
    result = await recognize_invoice(image_bytes)
    print(f"Поставщик : {result.get('supplier', '???')}")
    print(f"Сумма     : {result.get('total_sum', '???')}")
    print(f"Товаров   : {len(result.get('items', []))}")
    for i, item in enumerate(result.get("items", []), 1):
        print(f"  {i:2}. {item.get('name', '')[:55]}")
        print(f"      qty={item.get('qty')}  price={item.get('price')} ₽")


asyncio.run(main())
