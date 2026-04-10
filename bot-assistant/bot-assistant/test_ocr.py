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

os.environ["SIGMA_LOGIN"] = "+79174854325"
os.environ["SIGMA_PASSWORD"] = "IfrbIfrb680"
os.environ["OPENROUTER_KEY"] = "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"

from sigma_api import recognize_invoice

IMAGES = [
    ("photo_2026-04-06_19-11-36.jpg", "Прайс-лист с Заказом (Центропродукт)"),
    ("test_milk.jpg",                 "Расходная накладная (Молочный Переулок)"),
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
