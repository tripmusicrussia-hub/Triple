# -*- coding: utf-8 -*-
"""
Тест матчинга товаров из накладной к товарам в Sigma.
Запускать: python test_matching.py
"""
import asyncio
import os
import sys
sys.stdout.reconfigure(encoding='utf-8')

os.environ["SIGMA_LOGIN"] = "+79174854325"
os.environ["SIGMA_PASSWORD"] = "IfrbIfrb680"

import sigma_api
from sigma_api import SigmaAPI

# Реальные названия из накладной (как приходят с OCR)
TEST_CASES = [
    ("Молоко 3.2%",           "должен найти 3.2%, НЕ 2.5%"),
    ("Молоко 2.5%",           "должен найти 2.5%"),
    ("Сметана 15%",           "должен найти 15%, НЕ 20%"),
    ("Сметана 20%",           "должен найти 20%"),
    ("Творог ОВК 9%",         "должен найти 9%, НЕ персик/манго"),
    ("Творог ОВК 5%",         "должен найти 5%, НЕ персик/манго"),
    ("Кефир ОВК 1%",          "должен найти 1%, НЕ 2.5%"),
    ("Кефир ОВК 3.2%",        "должен найти 3.2%, НЕ 2.5%"),
    ("Васькино Счастье 2.5%", "должен найти Катык Зеленодольский"),
    ("Вас. Счас. 2.5%",       "должен найти Катык Зеленодольский"),
    ("Сметана ОВК 20% 180г",  "должен найти ОВК, НЕ Карламан"),
]

# Что искать в базе Sigma для диагностики
SEARCH_TERMS = ["молоко", "сметана", "творог", "кефир", "катык", "зеленое", "карламан", "васькино"]


async def main():
    api = SigmaAPI()

    print("Подключаемся к Sigma...")
    if not await api.login():
        print("ОШИБКА: не удалось войти в Sigma")
        return
    await api.get_company_info()

    print("Загружаем кэш товаров...")
    count = await api.load_all_products()
    print(f"Загружено товаров: {count}\n")

    # Берём кэш уже ПОСЛЕ загрузки через модуль (не через старую ссылку при импорте)
    cache = sigma_api._products_cache

    # ── Диагностика: что реально есть в Sigma ─────────────────────────────────
    print("=" * 60)
    print("ТОВАРЫ В SIGMA (что есть):")
    print("=" * 60)
    for term in SEARCH_TERMS:
        hits = [p["name"] for p in cache if term in p.get("name", "").lower()]
        if hits:
            print(f"\n  [{term.upper()}] ({len(hits)} шт):")
            for h in hits[:20]:
                print(f"    {h}")
        else:
            print(f"\n  [{term.upper()}]: — ничего нет")

    # ── Матчинг ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("РЕЗУЛЬТАТЫ МАТЧИНГА:")
    print("=" * 60 + "\n")
    for name, hint in TEST_CASES:
        result = api.find_product_in_cache(name)
        status = "OK" if result else "НЕ НАЙДЕН"
        print(f"  [{name}]")
        print(f"    -> {result.get('name', '') if result else 'НЕ НАЙДЕН'}")
        print(f"    ({hint})")
        print()


asyncio.run(main())
