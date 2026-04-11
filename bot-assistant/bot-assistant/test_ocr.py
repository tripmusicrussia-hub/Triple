# -*- coding: utf-8 -*-
"""
Тест OCR: отправляем фото накладной в Gemini и смотрим что распознал.
Запускать: python test_ocr.py
"""
import asyncio
import os
import sys
import json
sys.stdout.reconfigure(encoding='utf-8')

# Загружаем .env (без зависимости от python-dotenv)
_env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_env_path):
    with open(_env_path, encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if not _line or _line.startswith("#") or "=" not in _line:
                continue
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

from sigma_api import recognize_invoice

IMAGES = [
    ("photo_2026-04-06_19-11-36.jpg", "Прайс-лист с Заказом (Центропродукт)"),
    ("test_milk.jpg",                 "Расходная накладная (Молочный Переулок)"),
    ("test_elita.jpg",                "Накладная со скидкой (ИП Насруллаев/Элита)"),
    ("test_tovcheck.jpg",             "Товарный чек (крупы)"),
    ("test_mkf.jpg",                  "Накладная МКФ (семечки/конфеты)"),
]


async def test_image(path, label):
    if not os.path.exists(path):
        print(f"  [ПРОПУСК] файл не найден: {path}\n")
        return
    print(f"\n{'='*55}")
    print(f"  {label}")
    print(f"  Файл: {os.path.basename(path)}")
    print(f"{'='*55}")
    with open(path, "rb") as f:
        image_bytes = f.read()
    result = await recognize_invoice(image_bytes)
    print(f"  Поставщик : {result.get('supplier', '???')}")
    print(f"  Сумма     : {result.get('total_sum', '???')}")
    print(f"  Товаров   : {len(result.get('items', []))}\n")
    for i, item in enumerate(result.get("items", []), 1):
        print(f"  {i:2}. {item.get('name', '')[:55]}")
        print(f"      qty={item.get('qty')}  price={item.get('price')} ₽")


async def main():
    base = os.path.dirname(__file__)
    for fname, label in IMAGES:
        await test_image(os.path.join(base, fname), label)


asyncio.run(main())
