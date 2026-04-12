"""
sigma_api.py — интеграция с API cloud.sigma.ru
"""
import httpx
import json
import os
import re
import math
import time
import logging
from typing import Optional
from urllib.parse import quote

import wiki_store

logger = logging.getLogger(__name__)

SIGMA_LOGIN = os.getenv("SIGMA_LOGIN", "")
SIGMA_PASSWORD = os.getenv("SIGMA_PASSWORD", "")
SIGMA_AUTH_HEADER = "Basic cWFzbGFwcDpteVNlY3JldE9BdXRoU2VjcmV0"
BASE_URL = "https://api-s07.sigma.ru"

# Кэш товаров на уровне модуля (legacy — используется только диагностикой и
# как самый последний fallback в find_product). Источник истины — живые запросы
# в Sigma API через _sigma_search.
_products_cache = []
_cache_loaded = False

# Memo для повторных запросов к /products/simple в рамках одного процесса.
# Накладные часто содержат много товаров одного бренда — single-word search
# по «зеленое» используется десятки раз, и без memo это десятки сетевых вызовов.
# query → (timestamp, [products])
_search_memo: dict = {}
_SEARCH_MEMO_TTL = 300  # 5 минут — короче, чем сессия редактирования накладной

LOW_MARGIN_KEYWORDS = [
    "хлеб", "булка", "батон", "лаваш", "пита", "багет", "буханка",
    "молоко", "молоке", "молока",
]

# Словари аббревиатур и замен — загружаются из wiki/ (с fallback на хардкод).
# Ленивая инициализация: при первом обращении читаем wiki_store.
_abbreviations_cache = None
_name_fixes_cache = None

def _get_abbreviations() -> dict:
    global _abbreviations_cache
    if _abbreviations_cache is None:
        _abbreviations_cache = wiki_store.get_abbreviations()
    return _abbreviations_cache

def _get_name_fixes() -> dict:
    global _name_fixes_cache
    if _name_fixes_cache is None:
        _name_fixes_cache = wiki_store.get_name_fixes()
    return _name_fixes_cache


def _normalize_for_match(s: str) -> str:
    """Нормализация для поиска в каталоге: убирает шум, который ломает поиск,
    но сохраняет бренд + фасовку + жирность."""
    s = s.lower()
    # ё → е: каталог и накладные используют оба, стем не должен ломаться
    s = s.replace('ё', 'е')
    s = expand_abbreviations(s)
    for old, new in _get_name_fixes().items():
        s = s.replace(old, new)
    # Стрипаем кавычки — "Красный Ключ" vs Красный Ключ ломает токенизацию
    s = re.sub(r'["«»\u2018\u2019\u201c\u201d`]', ' ', s)
    # Нормализуем "гр" → "г" (180гр / 180 гр → 180г)
    s = re.sub(r'(\d+(?:[.,]\d+)?)\s*гр(?![а-яё])', r'\1г', s)
    # Стрипаем box-count после единицы: "40г/60" → "40г", "45г/24" → "45г"
    s = re.sub(r'(\d+(?:[.,]\d+)?\s*(?:г|кг|мл|л|шт))\s*/\s*\d+', r'\1', s)
    # Стрипаем "1/12", "1/6" (фасовка коробки без единицы)
    s = re.sub(r'\b\d+/\d+\b', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


# Стоп-слова — загружаются из wiki/ (с fallback на хардкод).
_stopwords_cache = None

def _get_stopwords() -> set:
    global _stopwords_cache
    if _stopwords_cache is None:
        _stopwords_cache = wiki_store.get_stopwords()
    return _stopwords_cache


_NUM_UNIT_TOKEN = re.compile(r'^\d+(?:[.,]\d+)?[а-яёa-z]*$')
_TOKEN_SPLIT = re.compile(r'[\s,./\\%\-"«»()+*;:!?]+')


def _pick_search_words(name_norm: str, n: int = 2) -> list:
    """Выбрать до n самых отличительных слов для подстрочного поиска в Sigma.
    Отличительное = длинное (>=3), не из стоп-листа, не число/число-с-единицей."""
    cands = []
    seen = set()
    for w in _TOKEN_SPLIT.split(name_norm):
        w = w.strip()
        if not w or len(w) < 3:
            continue
        if w in _get_stopwords():
            continue
        if _NUM_UNIT_TOKEN.match(w):
            continue
        if w in seen:
            continue
        seen.add(w)
        cands.append(w)
    # Сортируем по длине убыв — длинные слова обычно отличительные (бренд/вкус)
    cands.sort(key=len, reverse=True)
    return cands[:n]


def _extract_attrs(name_norm: str) -> dict:
    """Достаёт жёсткие атрибуты и отличительные токены из нормализованного имени."""
    # Проценты жирности (3.2%, 20%, 4%)
    pcts = {m.replace(',', '.') for m in re.findall(r'(\d+(?:[.,]\d+)?)\s*%', name_norm)}
    # Числа с единицами: 0.5л, 930г, 180г, 45г, 30шт
    measures = set()
    for num, unit in re.findall(r'(\d+(?:[.,]\d+)?)\s*(кг|мл|л|г|шт)(?![а-яёa-z])', name_norm):
        measures.add((num.replace(',', '.'), unit))
    # Отличительные токены для скоринга
    tokens = []
    for w in _TOKEN_SPLIT.split(name_norm):
        w = w.strip()
        if not w or len(w) < 3:
            continue
        if w in _get_stopwords():
            continue
        if _NUM_UNIT_TOKEN.match(w):
            continue
        tokens.append(w)
    return {"pcts": pcts, "measures": measures, "tokens": tokens}


def _measures_compatible(q_measures: set, p_measures: set) -> bool:
    """Каждая мера из запроса должна найтись в кандидате (с учётом альт. единиц).
    Если запрос без мер — ок. Если кандидат без мер — тоже ок (товар обобщённый)."""
    if not q_measures:
        return True
    if not p_measures:
        return True
    for num, unit in q_measures:
        if (num, unit) in p_measures:
            continue
        alt = None
        try:
            f = float(num)
            if unit == "л":
                alt = (f"{f*1000:g}", "мл")
            elif unit == "мл":
                alt = (f"{f/1000:g}", "л")
            elif unit == "кг":
                alt = (f"{f*1000:g}", "г")
            elif unit == "г" and f >= 1000:
                alt = (f"{f/1000:g}", "кг")
        except ValueError:
            pass
        if alt and alt in p_measures:
            continue
        return False
    return True


def _pcts_compatible(q_pcts: set, p_pcts: set) -> bool:
    """Если в запросе есть %, в кандидате должен быть тот же %.
    Если запрос без % — ок (товар не молочный)."""
    if not q_pcts:
        return True
    if not p_pcts:
        return False
    return q_pcts == p_pcts


def _token_stem(t: str) -> str:
    """Стем для substring-матчинга: первые 5 букв с обрезкой типичных русских
    падежных окончаний (а/я) на границе стема — фикс для генитива:
    «лайма» → «лайм», «мандарина» → «манд», «зеленое» → «зелен»."""
    if len(t) < 4:
        return t
    stem = t[:5] if len(t) >= 5 else t
    if len(stem) == 5 and stem[-1] in "аяо":
        stem = stem[:-1]
    return stem


def _pick_best_candidate(query: str, candidates: list) -> Optional[dict]:
    """Выбрать лучший товар из списка кандидатов по атрибутному фильтру + токен-скорингу.
    Шаги (повторяют ручной workflow в Sigma):
      1. Фильтр по жёстким атрибутам: % жирности + числа с единицами
      2. Фильтр по ratio совпавших значимых токенов: ≥ 50% должны найтись
      3. Скоринг: (число совпавших токенов, бонус за меру, длина совпавшего текста)
      4. Tie-reject: если лидер равен runner-up по всем метрикам — возвращаем None
    """
    if not candidates:
        return None
    q_norm = _normalize_for_match(query)
    q = _extract_attrs(q_norm)
    q_pcts = q["pcts"]
    q_measures = q["measures"]
    q_tokens = q["tokens"]

    if not q_tokens and not q_measures and not q_pcts:
        return None

    # Стемы для нечувствительности к падежам: «бекона» → «бекон», «сметаны» → «смета»
    q_stems = [_token_stem(t) for t in q_tokens]
    # Значимые стемы: те, чей исходный токен был ≥ 5 букв (бренд/тип/вкус)
    sig_stems = [s for s, t in zip(q_stems, q_tokens) if len(t) >= 5]
    # СТРОГИЙ порог: ВСЕ значимые стемы должны найтись в кандидате.
    # Бухгалтерский принцип — лучше «не нашли, создаём» чем «подобрали похожее».
    # Это отсеивает «Сметана СЕЛО ЗЕЛЕНОЕ → НЫТВЕН СМЕТАНА» (зелен не попало),
    # «СФАД ЧЁРНЫЙ → СФАД БЕЛЫЙ» (чёрны не попало) и т.п.
    min_sig_matches = len(sig_stems) if sig_stems else 0

    # Для measure_bonus: «0.5» как substring в имени кандидата
    q_measure_nums = {num for num, _ in q_measures}

    # Категориальная защита: первый значимый стем (молок/кефир/смета/чипсы/...)
    # должен встретиться в кандидате. Иначе мы подменяем категорию — для
    # бухгалтера это означает «такого товара нет, надо создавать новый», а не
    # «возьмём похожий другой категории по тем же атрибутам».
    category_stem = q_stems[0] if q_stems else None

    filtered = []
    for product in candidates:
        p_raw = product.get("name", "")
        if not p_raw:
            continue
        p_norm = _normalize_for_match(p_raw)
        p_attrs = _extract_attrs(p_norm)
        if category_stem and category_stem not in p_norm:
            continue
        if not _pcts_compatible(q_pcts, p_attrs["pcts"]):
            continue
        if not _measures_compatible(q_measures, p_attrs["measures"]):
            continue
        # Сколько значимых стемов нашлось в кандидате?
        if sig_stems:
            sig_hits = sum(1 for s in sig_stems if s in p_norm)
            if sig_hits < min_sig_matches:
                continue
        filtered.append((product, p_norm, p_attrs))

    if not filtered:
        return None

    scored = []
    for product, p_norm, p_attrs in filtered:
        matched = [s for s in q_stems if s in p_norm]
        score = len(matched)
        matched_len = sum(len(s) for s in matched)
        # Бонус за буквальное совпадение числа меры в имени кандидата
        # («0.5» в «красный ключ мохито 0.5л» → +1; отсеивает мохитос без объёма)
        measure_bonus = sum(1 for num in q_measure_nums if num in p_norm)
        scored.append({
            "product": product,
            "p_name": product.get("name", ""),
            "score": score,
            "measure_bonus": measure_bonus,
            "matched_len": matched_len,
        })
    scored.sort(key=lambda s: (-s["score"], -s["measure_bonus"], -s["matched_len"]))

    best = scored[0]
    # Требуем хотя бы 1 совпавший токен
    if q_tokens and best["score"] == 0:
        return None
    # Tie-reject: лидер полностью равен runner-up по (score, bonus, matched_len)
    if len(scored) > 1:
        second = scored[1]
        if (best["score"] == second["score"]
                and best["measure_bonus"] == second["measure_bonus"]
                and best["matched_len"] == second["matched_len"]):
            logger.info(
                f"Ambiguous match '{query[:40]}': "
                f"{best['p_name'][:35]} vs {second['p_name'][:35]}"
            )
            return None

    return best["product"]

# Категории — загружаются из wiki/ (с fallback на хардкод).
_categories_cache = None

def _get_categories() -> dict:
    global _categories_cache
    if _categories_cache is None:
        _categories_cache = wiki_store.get_categories()
    return _categories_cache


def detect_category(name: str) -> str:
    """Определить категорию товара по названию"""
    name_lower = name.lower()
    for category, keywords in _get_categories().items():
        for kw in keywords:
            if kw in name_lower:
                return category
    return "Главный экран"


def expand_abbreviations(name: str) -> str:
    """Заменяем аббревиатуры на полные названия для лучшего поиска"""
    name_lower = name.lower()
    for abbr, full in _get_abbreviations().items():
        # Заменяем аббревиатуру — ищем как отдельное слово (кириллица не поддерживает \b)
        name_lower = re.sub(r'(?<![а-яёa-z])' + abbr + r'(?![а-яёa-z])', full, name_lower)
    return name_lower


def get_markup(name: str) -> float:
    name_lower = name.lower()
    for kw in LOW_MARGIN_KEYWORDS:
        if kw in name_lower:
            return 0.10
    return 0.25


def calc_price(buy_price: float, markup: float) -> int:
    return round(buy_price * (1 + markup))


def format_items_preview(items: list) -> str:
    lines = []
    for i, item in enumerate(items, 1):
        markup = get_markup(item["name"])
        sell = calc_price(item["price"], markup)
        pct = int(markup * 100)
        lines.append(
            f"{i}. {item['name']}\n"
            f"   {item['qty']} шт/уп | Закупка: {item['price']} ₽ → Продажа: {sell} ₽ (+{pct}%)"
        )
    return "\n\n".join(lines)


def detect_weight_product(item: dict) -> dict:
    """Весовой товар — если количество указано в кг"""
    name = item["name"]
    qty = item.get("qty", 1)
    price = item.get("price", 0)
    original_qty_str = item.get("qty_str", "")

    # Вариант 1: количество в накладной указано в кг (напр. '1.53 кг', '1.297 кг')
    # Цена в накладной уже за 1 кг — просто ставим qty=вес, цена не меняется
    if original_qty_str and re.search(r'кг', str(original_qty_str).lower()):
        match = re.search(r'(\d+(?:[.,]\d+)?)', str(original_qty_str))
        if match and price > 0:
            weight = float(match.group(1).replace(",", "."))
            if weight > 0:
                return {
                    **item,
                    "qty": weight,
                    "price": price,  # цена уже за кг, не делим!
                    "weight_recalc": True,
                    "original_qty": qty,
                    "original_price": price,
                    "weight_kg": weight,
                }

    # Вариант 2: вес в названии товара (напр. "5кг"), qty=1 шт
    # Это коробочный товар — делим цену коробки на вес
    if qty == 1 and price > 0:
        match = re.search(r'(\d+(?:[.,]\d+)?)\s*кг', name.lower())
        if match:
            weight = float(match.group(1).replace(",", "."))
            if weight > 1:  # игнорируем "1кг"
                price_per_kg = round(price / weight, 2)
                return {
                    **item,
                    "qty": weight,
                    "price": price_per_kg,
                    "weight_recalc": True,
                    "original_qty": qty,
                    "original_price": price,
                    "weight_kg": weight,
                }

    return item


def process_weight_products(items: list) -> tuple:
    updated = []
    recalc_info = []
    for item in items:
        processed = detect_weight_product(item)
        updated.append(processed)
        if processed.get("weight_recalc"):
            recalc_info.append(
                f"⚖️ {processed['name'][:35]}\n"
                f"   {processed['original_qty']} шт x {processed['original_price']} р. "
                f"→ {processed['weight_kg']} кг x {processed['price']} р/кг"
            )
    return updated, recalc_info


class SigmaAPI:
    def __init__(self):
        self.token = None
        self.company_id = None
        self.storehouse_id = None
        self.storehouse_name = None
        self.user_email = None
        self.user_id = None
        self.user_fullname = None

    async def login(self) -> bool:
        logger.info(f"Sigma login attempt for: {SIGMA_LOGIN[:5]}***")
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{BASE_URL}/oauth/token",
                headers={
                    "Authorization": SIGMA_AUTH_HEADER,
                    "Accept": "application/json, text/javascript",
                    "Origin": "https://cloud.sigma.ru",
                },
                data={
                    "scope": "read write",
                    "grant_type": "password",
                    "username": SIGMA_LOGIN,
                    "password": SIGMA_PASSWORD,
                }
            )
            logger.info(f"Sigma response: {resp.status_code} {resp.text[:200]}")
            if resp.status_code != 200:
                logger.error(f"Sigma login failed: {resp.status_code} {resp.text[:200]}")
                return False
            data = resp.json()
            self.token = data.get("access_token")
            logger.info("Sigma: logged in successfully")
            return bool(self.token)

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://cloud.sigma.ru",
        }

    async def get_company_info(self) -> bool:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(f"{BASE_URL}/rest/1.1/account", headers=self._headers())
            logger.info(f"Account response: {r.status_code} {r.text[:300]}")
            if r.status_code != 200:
                return False
            data = r.json()
            self.company_id = data.get("activeCompany") or data.get("company", {}).get("id")
            self.user_email = data.get("email", "")
            self.user_id = data.get("id", "")
            self.user_fullname = f"{data.get('lastName', '')} {data.get('firstName', '')}".strip()
            logger.info(f"Sigma company_id: {self.company_id}")
            if not self.company_id:
                return False
            r2 = await client.get(
                f"{BASE_URL}/rest/v2/companies/{self.company_id}/storehouses",
                headers=self._headers()
            )
            logger.info(f"Storehouses: {r2.status_code} {r2.text[:200]}")
            if r2.status_code == 200:
                houses = r2.json()
                items = houses.get("content", houses) if isinstance(houses, dict) else houses
                if items:
                    self.storehouse_id = items[0].get("id")
                    self.storehouse_name = items[0].get("name")
                    logger.info(f"Sigma storehouse_id: {self.storehouse_id}")
            return bool(self.company_id)

    async def get_suppliers(self) -> list:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{BASE_URL}/rest/1.1/companies/null/suppliers",
                headers=self._headers(),
                params={"size": 100}
            )
            logger.info(f"Suppliers: {r.status_code} {r.text[:200]}")
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    return data
                return data.get("suppliers", data.get("content", []))
            return []

    async def find_supplier(self, name: str) -> Optional[str]:
        suppliers = await self.get_suppliers()
        name_lower = name.lower().strip().strip('"').strip("'")
        name_clean = re.sub(r'(общество с ограниченной ответственностью|ооо|ип|зао|оао)\s*["\']?', '', name_lower).strip().strip('"').strip("'")
        for s in suppliers:
            s_name = s.get("name", "").lower().strip().strip('"').strip("'")
            s_clean = re.sub(r'(ооо|ип|зао|оао)\s*["\']?', '', s_name).strip().strip('"').strip("'")
            if s_name == name_lower or s_clean == name_clean:
                return s.get("id")
        for s in suppliers:
            s_name = s.get("name", "").lower()
            s_clean = re.sub(r'(ооо|ип|зао|оао)\s*["\']?', '', s_name).strip().strip('"').strip("'")
            if name_clean in s_clean or s_clean in name_clean:
                return s.get("id")
        logger.warning(f"Supplier not found: '{name}'")
        return None

    async def _refresh_products_if_needed(self):
        """Проверяем количество товаров в Sigma — если изменилось, обновляем кэш"""
        global _products_cache, _cache_loaded
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.post(
                    f"{BASE_URL}/rest/v4/products/simple",
                    headers=self._headers(),
                    params={"page": 0},
                    json={"storehouseId": None, "isProduced": None, "waybillId": None}
                )
                if r.status_code == 200:
                    total_in_sigma = r.json().get("productsInfo", {}).get("totalElements", 0)
                    cached_count = len(_products_cache)
                    if not _cache_loaded or total_in_sigma != cached_count:
                        logger.info(f"Products changed: sigma={total_in_sigma}, cache={cached_count}. Reloading...")
                        await self.load_all_products()
                    else:
                        logger.info(f"Products cache up to date: {cached_count} items")
        except Exception as e:
            logger.error(f"Error checking products count: {e}")
            if not _cache_loaded:
                await self.load_all_products()

    async def load_all_products(self) -> int:
        """Загрузить все товары из Sigma в глобальный кэш"""
        global _products_cache, _cache_loaded
        page = 0
        total = 0
        products = []
        async with httpx.AsyncClient(timeout=60) as client:
            while True:
                r = await client.post(
                    f"{BASE_URL}/rest/v4/products/simple",
                    headers=self._headers(),
                    params={"page": page},
                    json={"storehouseId": None, "isProduced": None, "waybillId": None}
                )
                if r.status_code != 200:
                    logger.error(f"load_all_products page {page}: {r.status_code}")
                    break
                data = r.json()
                content = data.get("productsInfo", {}).get("content", [])
                if not content:
                    break
                products.extend(content)
                total += len(content)
                if data.get("productsInfo", {}).get("last", True):
                    break
                page += 1
                if page > 100:
                    break
        _products_cache = products
        _cache_loaded = True
        logger.info(f"Loaded {total} products into cache ({page+1} pages)")
        return total

    def find_product_in_cache(self, name: str, cache: list = None) -> Optional[dict]:
        """Матчинг товара по заданному списку кандидатов (или по полному кэшу)."""
        if cache is None:
            cache = _products_cache
        if not cache:
            return None
        match = _pick_best_candidate(name, cache)
        if match:
            logger.info(f"Cache hit: '{name[:30]}' → '{match.get('name', '')[:40]}'")
        return match

    async def _sigma_search(self, query: str, limit: int = 200) -> list:
        """Одиночный запрос к /rest/v4/products/simple с {name: query}.
        Возвращает список товаров (content). Memo на 5 минут — повторные
        single-word запросы (того же бренда) идут из памяти, не в сеть."""
        now = time.time()
        cached = _search_memo.get(query)
        if cached and now - cached[0] < _SEARCH_MEMO_TTL:
            return cached[1]
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(
                    f"{BASE_URL}/rest/v4/products/simple",
                    headers=self._headers(),
                    params={"page": 0, "size": limit},
                    json={"name": query, "storehouseId": None, "isProduced": None, "waybillId": None},
                )
                if r.status_code != 200:
                    logger.warning(f"Sigma search '{query}': {r.status_code}")
                    return []
                results = r.json().get("productsInfo", {}).get("content", [])
                _search_memo[query] = (now, results)
                return results
        except Exception as e:
            logger.error(f"Sigma search '{query}' failed: {e}")
            return []

    @staticmethod
    def _two_words_in_query_order(q_norm: str, words: list) -> Optional[list]:
        """Возвращает первые 2 слова из words в том порядке, в котором они
        встречаются в q_norm — для substring-search Sigma порядок имеет значение."""
        if len(words) < 2:
            return None
        positions = []
        for w in words[:2]:
            pos = q_norm.find(w)
            if pos < 0:
                return None
            positions.append((pos, w))
        positions.sort()
        return [w for _, w in positions]

    async def find_product(self, name: str) -> Optional[dict]:
        """API-first поиск товара в Sigma. Каталог в Sigma живой — пользователь
        добавляет/удаляет товары, статичный кэш всегда устаревает (мы только что
        видели: 6 МОЛОКО СЕЛО ЗЕЛЕНОЕ в UI vs 1 в нашем кэше). Поэтому источник
        истины — живые запросы в /products/simple.

        Стратегия — повторяет ручной workflow в Sigma UI:
          0. Wiki lookup — если есть выученный маппинг с confidence >= 0.8,
             используем его напрямую (skip API search).
          1. Single-word search по 1-2 самым отличительным словам (длинные не-стоп-
             слова: бренд/категория). Single-word — самый надёжный, потому что
             substring-search Sigma не страдает от перестановок порядка слов.
          2. Если 2 слова — добавляем joined-запрос в правильном порядке (как они
             идут в исходном названии) — для редких случаев, когда single-word
             даёт слишком много результатов.
          3. Aggregate dedup → _pick_best_candidate (фильтр по атрибутам +
             категориальная защита + токен-скоринг).
          4. Финальный fallback — устаревший _products_cache, если вдруг
             загружен (только для оффлайн-диагностики).
        """
        # Этап 0: wiki lookup — выученные маппинги из коррекций
        q_key = name.lower().strip()
        mappings = wiki_store.get_product_mappings()
        mapping = mappings.get(q_key)
        if mapping and mapping.get("confidence", 0) >= 0.8:
            sigma_name = mapping.get("sigma_name", "")
            sigma_id = mapping.get("sigma_id")
            if sigma_id:
                logger.info(f"Wiki hit: '{name[:30]}' → '{sigma_name[:40]}' (conf={mapping['confidence']:.2f})")
                return {"id": sigma_id, "name": sigma_name}

        q_norm = _normalize_for_match(name)
        search_words = _pick_search_words(q_norm, n=2)
        if not search_words:
            logger.info(f"find_product '{name[:40]}': no distinctive words")
            return None

        logger.info(f"find_product '{name[:40]}' → search_words={search_words}")

        candidates = []
        seen_ids = set()

        def _add(items):
            for p in items:
                pid = p.get("id") or p.get("name")
                if pid in seen_ids:
                    continue
                seen_ids.add(pid)
                candidates.append(p)

        # Этап 1: каждое слово отдельно — независимо от порядка
        for word in search_words:
            _add(await self._sigma_search(word, limit=200))

        # Этап 2: 2-словный запрос в правильном порядке (на случай редкого хвоста)
        if len(search_words) >= 2:
            ordered = self._two_words_in_query_order(q_norm, search_words)
            if ordered:
                _add(await self._sigma_search(" ".join(ordered), limit=200))

        if candidates:
            match = _pick_best_candidate(name, candidates)
            if match:
                logger.info(
                    f"API match: '{name[:30]}' → '{match.get('name', '')[:45]}' "
                    f"(из {len(candidates)} кандидатов)"
                )
                return match

        # Финальный fallback — старый кэш (если оффлайн-диагностика загрузила)
        if _cache_loaded and _products_cache:
            cached = _pick_best_candidate(name, _products_cache)
            if cached:
                logger.info(f"Cache fallback: '{name[:30]}' → '{cached.get('name', '')[:40]}'")
                return cached

        logger.info(f"Product not found: '{name[:40]}'")
        return None

    async def create_product(self, name: str, buy_price: float, sell_price: float) -> Optional[dict]:
        """Создать новый товар в Sigma если не найден в базе"""
        global _products_cache
        async with httpx.AsyncClient(timeout=15) as client:
            # Получаем список категорий
            r_menus = await client.get(
                f"{BASE_URL}/rest/v2/companies/{self.company_id}/menus",
                headers=self._headers()
            )
            menu_id = None
            if r_menus.status_code == 200:
                menus = r_menus.json()
                items = menus if isinstance(menus, list) else menus.get("content", [])
                if items:
                    menu_id = items[0].get("id")

            if not menu_id:
                logger.error("Could not get menuId for product creation")
                return None

            # Определяем категорию
            category_name = detect_category(name)
            parent_id = None

            # Ищем ID категории
            r_groups = await client.get(
                f"{BASE_URL}/rest/v2/groups",
                headers=self._headers(),
                params={"size": 1000, "path": "*", "menuId": menu_id, "page": 0}
            )
            if r_groups.status_code == 200:
                groups = r_groups.json()
                items = groups if isinstance(groups, list) else groups.get("content", [])
                for g in items:
                    if g.get("name", "").upper() == category_name.upper():
                        parent_id = g.get("id")
                        break

            payload = {
                "name": name,
                "menuId": menu_id,
                "parentIds": [parent_id],
                "variations": [{
                    "name": "",
                    "productUnitId": "be56b507-805b-4f9d-87fa-1c66bbd28795",
                    "price": {"type": "Rb", "value": sell_price}
                }]
            }
            r = await client.post(
                f"{BASE_URL}/rest/v2/companies/{self.company_id}/menus/{menu_id}/menu-products",
                headers=self._headers(),
                json=payload
            )
            logger.info(f"Create product '{name[:30]}' in '{category_name}': {r.status_code} {r.text[:200]}")
            if r.status_code in (200, 201):
                data = r.json()
                # id товара для прихода — catalogProductId из variations
                product_id = None
                variations = data.get("variations", [])
                if variations:
                    product_id = variations[0].get("catalogProductId")
                if not product_id:
                    product_id = data.get("id")
                if product_id:
                    _products_cache.append({"id": product_id, "name": name.upper()})
                    logger.info(f"Created product '{name[:30]}' with id {product_id}")
                    return {"id": product_id, "name": name}
            return None

    async def create_income(self, supplier_id: Optional[str] = None, supplier_name: Optional[str] = None) -> tuple:
        from datetime import datetime
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.") + f"{datetime.now().microsecond // 1000:03d}"
        async with httpx.AsyncClient(timeout=15) as client:
            r_num = await client.post(
                f"{BASE_URL}/waybills/generate-waybill-number",
                headers=self._headers(),
                params={"waybillType": "INCOME", "companyId": self.company_id},
                json={}
            )
            if r_num.status_code == 200:
                try:
                    number = r_num.json().get("number", "")
                except Exception:
                    number = r_num.text.strip().strip('"')
            else:
                number = ""
            logger.info(f"Waybill number: {number}")
            payload = {
                "waybillTime": now,
                "comment": "",
                "number": number,
                "isDefault": True,
                "status": "DRAFT",
                "egaisWaybillRegId": None,
                "serverTime": None,
                "storehouseDestination": {"id": self.storehouse_id, "name": self.storehouse_name},
                "storehouseSource": {"id": None, "name": None},
                "supplier": {"id": supplier_id, "name": supplier_name},
                "totalSum": 0,
                "type": "INCOME",
                "user": {"id": self.user_id, "email": self.user_email, "fullName": self.user_fullname},
            }
            r = await client.post(
                f"{BASE_URL}/waybills",
                headers=self._headers(),
                json=payload
            )
            logger.info(f"Create income: {r.status_code} {r.text[:300]}")
            if r.status_code in (200, 201):
                waybill_id = r.json().get("id")
                return waybill_id, number
            logger.error(f"Create income failed: {r.status_code} {r.text[:300]}")
            return None, None

    async def add_product_to_income(self, waybill_id: str, product_id: str, qty: float, buy_price: float, sell_price: float) -> bool:
        async with httpx.AsyncClient(timeout=15) as client:
            # Шаг 1: добавляем товар в приход
            r = await client.post(
                f"{BASE_URL}/waybills/{waybill_id}/elements/product/{product_id}",
                headers=self._headers(),
                json={"amount": qty, "buyingPrice": buy_price, "sellingPrice": sell_price}
            )
            logger.info(f"Add product {product_id}: {r.status_code} {r.text[:200]}")
            if r.status_code not in (200, 201):
                return False

            element_id = r.json().get("id")
            if not element_id:
                return False

            # Шаг 2: количество
            await client.put(
                f"{BASE_URL}/waybills/{waybill_id}/elements/{element_id}/quantity",
                headers=self._headers(),
                json={"value": qty}
            )
            logger.info(f"Set quantity {qty} for element {element_id}")

            # Шаг 3: цена закупки
            await client.put(
                f"{BASE_URL}/waybills/{waybill_id}/elements/{element_id}/buying-cost-per-unit",
                headers=self._headers(),
                json={"value": buy_price}
            )
            logger.info(f"Set buying price {buy_price} for element {element_id}")

            # Шаг 4: цена продажи
            await client.put(
                f"{BASE_URL}/waybills/{waybill_id}/elements/{element_id}/selling-cost-per-unit",
                headers=self._headers(),
                json={"value": sell_price}
            )
            logger.info(f"Set selling price {sell_price} for element {element_id}")

            return True

    async def conduct_income(self, waybill_id: str) -> bool:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                f"{BASE_URL}/waybills/{waybill_id}/conduct",
                headers=self._headers(),
                json={}
            )
            return r.status_code in (200, 201)

    async def process_invoice(self, items: list, supplier_name: str) -> dict:
        if not await self.login():
            return {"ok": False, "error": "Не удалось войти в Sigma. Проверь логин/пароль."}

        await self.get_company_info()

        # Загружаем или обновляем кэш товаров
        await self._refresh_products_if_needed()

        supplier_id = await self.find_supplier(supplier_name)
        if not supplier_id:
            logger.warning(f"Supplier not found: {supplier_name}")

        waybill_id, waybill_number = await self.create_income(supplier_id, supplier_name if supplier_id else None)
        if not waybill_id:
            return {"ok": False, "error": "Не удалось создать документ прихода в Sigma."}

        added = 0
        skipped = []
        for item in items:
            product = await self.find_product(item["name"])
            if not product:
                # Создаём новый товар
                markup = get_markup(item["name"])
                sell_price = calc_price(item["price"], markup)
                product = await self.create_product(item["name"], item["price"], sell_price)
                if not product:
                    skipped.append(item["name"])
                    continue
                logger.info(f"Created new product: {item['name']}")
            else:
                # Имплицитное обучение: записываем маппинг OCR→Sigma в wiki.
                # Если имена отличаются существенно — это ценный сигнал для будущих матчей.
                ocr_name = item["name"]
                sigma_name = product.get("name", "")
                if ocr_name and sigma_name and ocr_name.lower().strip() != sigma_name.lower().strip():
                    try:
                        wiki_store.record_correction(
                            ocr_name=ocr_name,
                            sigma_name=sigma_name,
                            sigma_id=product.get("id"),
                            confidence=0.7,
                            source="auto_match",
                        )
                    except Exception as e:
                        logger.warning(f"wiki record_correction failed: {e}")
            markup = get_markup(item["name"])
            sell_price = calc_price(item["price"], markup)
            success = await self.add_product_to_income(
                waybill_id=waybill_id,
                product_id=product["id"],
                qty=item["qty"],
                buy_price=item["price"],
                sell_price=sell_price
            )
            if success:
                added += 1
            else:
                skipped.append(item["name"])

        # Flush wiki corrections after processing the whole invoice (natural batch boundary)
        try:
            wiki_store.flush_pending()
        except Exception as e:
            logger.warning(f"wiki flush_pending failed: {e}")

        return {
            "ok": True,
            "waybill_id": waybill_id,
            "waybill_number": waybill_number,
            "added": added,
            "skipped": skipped,
            "conducted": False
        }


async def recognize_invoice(image_bytes: bytes) -> dict:
    """
    Распознавание накладной в два шага (двухступенчатая архитектура):

    Шаг 1 — Yandex Vision OCR читает изображение и возвращает текст построчно.
            Читает русский язык лучше всех, работает из РФ без гео-блока.

    Шаг 2 — Бесплатный текстовый LLM на OpenRouter работает как «профессиональный
            бухгалтер»: по тексту классифицирует тип документа, извлекает строки
            товаров в структурированный JSON, проверяет сходимость итога.

    Раньше использовался Gemini 2.0 Flash напрямую (vision), но Google AI Studio
    гео-блочит РФ → ушли на эту связку. Яндекс OCR + text-LLM также лучше,
    чем слабый vision (Nemotron / Gemma free), потому что Yandex силён на
    русской кириллице и рукописных цифрах.
    """
    import re as _re
    from yandex_ocr import yandex_read_text

    OPENROUTER_KEY = os.getenv("OPENROUTER_KEY", "")
    if not OPENROUTER_KEY:
        raise RuntimeError("OPENROUTER_KEY не задан в окружении")

    # ── Шаг 1: OCR через Yandex Vision ─────────────────────────────────────────
    ocr_text = await yandex_read_text(image_bytes, model="table")
    logger.info("Yandex OCR: %d строк, %d символов", ocr_text.count("\n") + 1, len(ocr_text))
    if len(ocr_text) < 20:
        raise Exception(f"Yandex OCR вернул слишком мало текста: {ocr_text!r}")

    # ── Шаг 2: Текстовый LLM извлекает структуру ───────────────────────────────
    prompt = f"""Ты — профессиональный бухгалтер продуктового магазина. Перед тобой текст накладной, распознанный OCR построчно (порядок строк сохранён). Твоя задача — аккуратно извлечь товарные позиции в JSON.

ШАГ 1 — ОПРЕДЕЛИ ТИП ДОКУМЕНТА И КОЛОНКУ КОЛИЧЕСТВА:

А) ТОРГ-12 / Товарная накладная (колонки идут: "В одном месте" | "Мест, штук" | "Масса брутто" | "Количество (масса нетто)" | "Цена" | "Сумма без НДС" | ... | "Сумма с НДС"):
   qty = КОЛОНКА "Количество (масса нетто)" — это 4-е число в строке после названия товара, обычно 1-50, может быть дробным (например 3.000).
   НЕ бери "В одном месте" (первая цифра) и НЕ бери "Мест, штук" (вторая цифра) — это упаковочная информация, не количество для бухучёта.
   Пример: "Наггетсы куриные | 6 | 3 | 4.50 | 3.000 | 630.00 | 1718.18 | 10% | 171.82 | 1890.00"
     ⇒ qty=3 (из "масса нетто"=3.000), price=630 (цена с НДС). Проверка: 3 × 630 = 1890 = "Сумма с НДС" ✓
   Проверка сходимости строки: qty × price должно равняться "Сумме с НДС" этой строки (±1 ₽). Если не сходится — попробуй другую колонку.

Б) УПД / Счёт-фактура / Универсальный передаточный документ (колонки: "Код товара" | "№" | "Наименование" | "Код вида" | "Единица измерения (код)" | "Единица измерения (обозначение)" | "Количество (объем)" | "Цена за единицу" | "Стоимость без налога" | "Акциз" | "Налоговая ставка" | "Сумма налога" | "Стоимость с налогом - всего" | "Страна"):
   qty = КОЛОНКА "Количество (объем)" — обычно дробное (12.000, 15.000).
   price = КОЛОНКА "Цена (тариф) за единицу измерения".
   Сумма строки с НДС = КОЛОНКА "Стоимость товаров ... с налогом - всего" (предпоследняя числовая колонка).
   Пример строки: "НФ-00000026 | 1 | Пельмени | 166 | КГ | 12,000 | 267,62 | 3211,43 | Без акциза | 5% | 160,57 | 3372,00"
     ⇒ qty=12, price=267.62, сумма с НДС=3372. Проверка: 12 × 267.62 ≈ 3211.44 ≠ 3372 (это сумма БЕЗ НДС).
     Для price-units с НДС: сумма с НДС / qty = 3372/12 = 281. Если надо price с НДС — бери 281, если без — 267.62. По правилам бери С НДС ⇒ 281.
   ВАЖНО: "Стоимость без налога" и "Стоимость с налогом" — это разные колонки. Всегда бери сумму с налогом для total_sum и для расчёта price.

В) Расходная / простая накладная:
   qty = "Количество" или "Кол-во".

Г) Товарный чек:
   qty = "Кол-во". Поставщик = продавец в шапке.

Д) Прайс-лист / каталог с колонкой "Заказ" (большая таблица товаров, где БОЛЬШИНСТВО строк в колонке "Заказ" ПУСТЫЕ, и только часть строк содержит число — рукописное или печатное):
   Это НЕ обычная накладная. Это каталог поставщика, в котором покупатель отметил то, что заказывает.
   КРИТИЧЕСКИ ВАЖНО: возвращай ТОЛЬКО те строки, где колонка "Заказ" НЕ пустая (есть число > 0). Все остальные строки — это непроданный каталог, их НЕ включай в items.
   qty = число из колонки "Заказ" (обычно 1-30, это количество УПАКОВОК/коробок).
   price = колонка "Цена за упаковку" (цена за коробку), если такая колонка есть в таблице.
          Иначе — колонка "Цена" (цена за штуку). Бери ту колонку, которая соответствует единице измерения "Заказ".
   qty_str = "{{qty}} упак" (для оптовых заказов по коробкам — это стандарт).
   Пример таблицы прайс-листа:
     "Наименование | Заказ | Цена | Цена за упаковку | Сумма"
     "Семечки Белочка XL 100 гр/30 шт |  | 62,80 | 1884,00 |"             ← Заказ пустой → ПРОПУСТИТЬ
     "Семечки Белочка жарен 70 гр/50 шт | 10 | 43,40 | 2170,00 |"         ← qty=10, price=2170 (за упак)
     "Арахис Джини Соль 80гр./25шт. | 5 | 47,40 | 1185,00 |"              ← qty=5, price=1185
     "Печенье ВВК сдобное 50 гр*12шт | 12 | 31,32 | 375,84 |"             ← qty=12, price=375.84
   В этом примере items должен содержать ТОЛЬКО 3 позиции (не 4), потому что у первой строки колонка "Заказ" пустая.
   total_sum для прайс-листа = сумма (qty × price) по отфильтрованным items. НЕ бери "Итого" внизу каталога — там может быть сумма всего каталога, а не заказа.
   ⚠️ Признаки прайс-листа (если видишь хоть один — это ветка Д, а НЕ ветка В):
     - Есть колонка "Заказ" (или "Кол-во заказа", "К заказу"), и в ней БОЛЬШЕ половины строк пустые.
     - Большой каталог товаров одного поставщика (20+ позиций однотипного товара).
     - Сверху текст типа "Прайс-лист", "Каталог", "Торговый представитель", "Рекомендуемая цена".

ШАГ 2 — ИЗВЛЕКИ ПОЛЯ (по-бухгалтерски аккуратно):

- supplier: ПРОДАВЕЦ (поля "Продавец" / "Поставщик" / "Грузоотправитель"), НЕ покупатель.
- total_sum: итоговая сумма ВСЕГО документа ("Итого", "Всего к оплате", "Сумма по накладной").
  Если "Итого" / "Всего" в OCR не виден (например фото обрезано) — посчитай сам: сумма всех (price × qty) по items.
- items[]: каждая товарная позиция:
  * name — бренд + тип товара + жирность/сорт + фасовка/упаковка, ДОСЛОВНО из накладной ("Молоко Село Зеленое 3.2% 930гр ПЭТ"). Ничего не перефразируй.
    ⚠️ ОБЯЗАТЕЛЬНО вырезай из name хвостовые метаданные поставщика, это НЕ часть названия товара и ломает поиск в каталоге:
      - даты выработки/отгрузки/годности ("02.04.2026", "07.04.2026 г", "до 15.05.26")
      - артикулы/SKU поставщика в конце строки (коды вида латинские+цифры: "БС101", "БЧ118", "KBC124", "OTX020", "05093", "19330"; обычно приклеены через пробел или слэш в самом конце)
      - рекламные суффиксы поставщика ("АКЦИЯ", "АКЦИЯ УМНЫЙ ПОКУПАТЕЛЬ", "ДАДИМ ДЕШЕВЛЕ", "ХИТ", "НОВИНКА")
      - телефоны и маркетинговые приписки ("*89173603344", "*тел. 8-...")
    Пример: "Сух.Кириешки ржан.40г/60 Бекон БС101" → name="Сух.Кириешки ржан.40г/60 Бекон" (БС101 убрано).
    Пример: "Ветчина "Вкусная" 02.04.2026" → name="Ветчина Вкусная" (дата убрана).
    Пример: "Рис КАМОЛИНО континент 25кг*89173603344" → name="Рис КАМОЛИНО континент 25кг" (телефон убран).
    Но номера-вариации, которые являются ЧАСТЬЮ названия (процент жирности "3.2%", объём "1.5л", граммовка "180г", фасовка "1/12"), НЕ трогай — они часть имени товара.
  * qty — из нужной колонки (шаг 1). Дробным может быть для весовых (кг).
  * qty_str — исходная строка с единицей ("5 шт", "1.5 кг", "3 уп").
  * uncertain — true/false. Ставь true если НЕ уверен в корректности этой строки. Критерии:
      - qty × price НЕ равно сумме строки из OCR (с допуском ±1 ₽) — значит одно из чисел прочитано неверно
      - название товара оборвано или содержит явный мусор (обрывки типа "азан...", "ваз..." без продолжения)
      - в числах есть странные символы (буквы вместо цифр, двойные запятые "12,,00", пропущенные разряды)
      - цена подозрительно низкая или высокая для типа товара (шоколадка за 50000 ₽, мясо за 0.01 ₽)
      - строка была склеена из двух (см. правило №3) и ты не уверен в разделении
    Если всё выглядит нормально — ставь false. НЕ помечай uncertain просто так на всякий случай, только при реальных признаках.
  * price — ЦЕНА ЗА ЕДИНИЦУ С НДС (итоговая для покупателя).
    ПРИОРИТЕТ источников (бери первый подходящий):
      1) Колонка "Цена с НДС" / "Цена с учетом НДС" — если есть, бери её напрямую.
      2) Иначе "Сумма с НДС" (строки) ÷ qty, округлить до 2 знаков.
      3) Иначе колонка "Цена" или "Цена за единицу" — если нет информации о НДС, бери как есть.
    ВНИМАНИЕ: если в ТОРГ-12 видны ДВЕ колонки цен ("Цена" БЕЗ НДС и "Цена с НДС" РЯДОМ), ВСЕГДА бери вторую (с НДС, она обычно больше). Пример: "29,51 | 36,00" ⇒ price=36, НЕ 29.51.
    Если указана "Цена со скидкой" — бери её.

БУХГАЛТЕРСКИЕ ПРАВИЛА (очень важно):

1. Итог должен сходиться (только для одностраничной накладной): сумма (price × qty) по всем items ≈ total_sum (±1 ₽).
   ИСКЛЮЧЕНИЕ — хвостовая страница многостраничной накладной: если номера строк в таблице идут НЕ с 1 (например с 54), значит это продолжение. В этом случае:
     - total_sum = "Всего по накладной" (итог ВСЕЙ накладной, он больше видимой суммы)
     - items = ТОЛЬКО видимые на этой странице позиции
     - is_tail_page = true (обязательно укажи это поле в JSON!)
     - НЕ проверяй сходимость, НЕ возвращай пустой items. Просто извлеки всё, что видишь.
   Для обычной одностраничной накладной поле is_tail_page не добавляй (или ставь false).
2. Извлекай ВСЕ товарные строки, которые видны в OCR — даже если часть колонок пустая. Если колонка "Сумма с НДС" не распознана, но есть qty и "Цена с НДС" — сумма строки = qty × цена с НДС. Если не распознаны и qty, и price — тогда пропусти строку. Но не выкидывай строку только потому что одна колонка пустая.
   НИКОГДА не возвращай items=[] если в OCR видно хотя бы одно чётко распознанное название товара со строкой цифр.
   ⚠️ ИСКЛЮЧЕНИЕ — ветка Д (прайс-лист с колонкой "Заказ"): там фильтрация строго по наличию значения в колонке "Заказ", правило №2 не применяется. Пустая "Заказ" = не входит в заказ = не попадает в items.
3. Строка может быть СКЛЕЕНА OCR из двух товаров. Признаки: в одной ячейке таблицы два артикула подряд ("00000012601 00000002905"), или два количества/цены подряд ("12,000 12,000 | 27,12 27,12"), или два названия товара подряд. В этом случае разбивай на ДВА items с одинаковыми qty/price (или разными, если видны разные значения).
4. Пропускай нетоварные строки: "Итого", "Всего", "НДС", "Без НДС", "К оплате", услуги ("Доставка", "Погрузка", "Транспортные").
5. Пропускай строки заголовков таблицы.
6. Имя поставщика — именно продавец, не покупатель и не грузополучатель.
7. OCR может путать похожие символы (О↔0, З↔3, рус/лат). Если число выглядит абсурдно (qty=100500, price=0.01) — пропусти строку, не угадывай.

ВЕРНИ ТОЛЬКО ВАЛИДНЫЙ JSON БЕЗ MARKDOWN И БЕЗ КОММЕНТАРИЕВ:
{{"supplier":"название","total_sum":число,"is_tail_page":false,"items":[{{"name":"название","qty":число,"qty_str":"5 шт","price":число,"uncertain":false}}]}}

=== ТЕКСТ НАКЛАДНОЙ (OCR) ===
{ocr_text}
=== КОНЕЦ ТЕКСТА ==="""

    # Fallback-цепочка: если модель rate-limited, пробуем следующую.
    # Первая — платная, но умная (~$0.001/накладная) и надёжно следует промпту.
    # Остальные — бесплатные бэкапы, если платная недоступна.
    MODELS = [
        "anthropic/claude-haiku-4.5",
        "openai/gpt-4o-mini",
        "openai/gpt-oss-120b:free",
        "minimax/minimax-m2.5:free",
        "z-ai/glm-4.5-air:free",
    ]

    raw = None
    last_error = None
    async with httpx.AsyncClient(timeout=90) as client:
        for model in MODELS:
            try:
                resp = await client.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {OPENROUTER_KEY}",
                        "Content-Type": "application/json",
                        "HTTP-Referer": "https://github.com/tripmusicrussia-hub/Triple",
                    },
                    json={
                        "model": model,
                        "max_tokens": 3000,
                        "temperature": 0,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                )
                j = resp.json()
                if "error" in j:
                    err_msg = j["error"].get("message", str(j["error"]))
                    logger.warning("LLM %s error: %s — пробую следующую", model, err_msg)
                    last_error = err_msg
                    continue
                content = j["choices"][0]["message"].get("content") or ""
                if not content.strip():
                    logger.warning("LLM %s вернул пустой content — пробую следующую", model)
                    continue
                logger.info("LLM %s: ok, %d символов ответа", model, len(content))
                if os.getenv("DEBUG_LLM_RAW"):
                    logger.info("RAW LLM response:\n%s", content[:3000])
                raw = content
                break
            except Exception as e:
                logger.warning("LLM %s exception: %s — пробую следующую", model, e)
                last_error = str(e)
                continue

    if raw is None:
        raise Exception(f"Все LLM недоступны. Последняя ошибка: {last_error}")

    # ── Очистка и парсинг JSON ─────────────────────────────────────────────────
    raw_clean = _re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', raw)
    raw_clean = raw_clean.replace("```json", "").replace("```", "").strip()
    match = _re.search(r'\{.*\}', raw_clean, _re.DOTALL)
    if match:
        raw_clean = match.group(0)
    try:
        result = json.loads(raw_clean)
    except Exception:
        raw_clean2 = ''.join(c for c in raw_clean if ord(c) >= 32 or c in '\n\r\t')
        result = json.loads(raw_clean2)

    # ── Проверка сходимости итога (total_mismatch) ─────────────────────────────
    # Ловит И ошибки поставщика в накладной, И галлюцинации LLM по total_sum.
    # Пропускаем хвостовые страницы многостраничных накладных (там computed << printed
    # по определению, потому что видим только часть позиций).
    items = result.get("items") or []
    printed = result.get("total_sum")
    is_tail = bool(result.get("is_tail_page"))
    if items and not is_tail and isinstance(printed, (int, float)) and printed > 0:
        computed = 0.0
        for it in items:
            q = it.get("qty") or 0
            p = it.get("price") or 0
            try:
                computed += float(q) * float(p)
            except (TypeError, ValueError):
                pass
        computed = round(computed, 2)
        delta = round(computed - printed, 2)
        if abs(delta) > 1.0:
            result["total_mismatch"] = {
                "computed": computed,
                "printed": round(float(printed), 2),
                "delta": delta,
            }
            logger.info(
                "Total mismatch: computed=%.2f printed=%.2f delta=%.2f",
                computed, printed, delta,
            )
    return result
