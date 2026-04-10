"""
sigma_api.py — интеграция с API cloud.sigma.ru
"""
import httpx
import json
import os
import re
import math
import logging
from typing import Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)

SIGMA_LOGIN = os.getenv("SIGMA_LOGIN", "")
SIGMA_PASSWORD = os.getenv("SIGMA_PASSWORD", "")
SIGMA_AUTH_HEADER = "Basic cWFzbGFwcDpteVNlY3JldE9BdXRoU2VjcmV0"
BASE_URL = "https://api-s07.sigma.ru"

# Кэш товаров на уровне модуля
_products_cache = []
_cache_loaded = False

LOW_MARGIN_KEYWORDS = [
    "хлеб", "булка", "батон", "лаваш", "пита", "багет", "буханка",
    "молоко", "молоке", "молока",
]

# Словарь аббревиатур — расшифровки для поиска в Sigma
ABBREVIATIONS = {
    "овк": "очень важная корова",
    "овс": "очень важная свинка",
    "мп": "молочный переулок",
    "коктейль топтыжка": "молочный коктейль топтыжка",
    "сгущ.карламан": "сгущ.карламан",
    "сгущ карламан": "сгущ.карламан",
}

# Замены для конкретных несовпадений между накладной и Sigma
PRODUCT_NAME_FIXES = {
    "карламан какао 7,5": "карламан какао 7",
    "карламан какао 7.5": "карламан какао 7",
    "вас. счас.": "васькино счастье",
    "вас счас": "васькино счастье",
}

# Словарь для определения категории по ключевым словам
CATEGORY_KEYWORDS = {
    "МОЛОЧКА": ["молоко", "сметана", "кефир", "ряженка", "йогурт", "творог", "масло слив", "сливки", "простокваша", "варенец", "коктейль молоч", "топтыжка", "овк", "очень важная", "васькино", "деревенский"],
    "БАКАЛЕЯ": ["крупа", "рис", "гречка", "макарон", "мука", "сахар", "соль", "масло раст", "уксус", "соус", "майонез", "кетчуп"],
    "КОНСЕРВЫ": ["сгущ", "тушен", "консерв", "рыбн", "горошек", "кукуруза", "фасоль"],
    "ВЫПЕЧКА": ["хлеб", "батон", "булка", "лаваш", "багет", "пита", "лепешка"],
    "ЗАМОРОЗКА": ["замороженн", "пельмен", "вареник", "котлет заморож"],
    "СЛАДОСТИ": ["конфет", "шоколад", "печенье", "вафл", "торт", "пряник", "зефир", "мармелад", "халва"],
    "МЯСО, КОЛБАСА": ["колбас", "сосиск", "сардельк", "ветчин", "окорок", "бекон", "мясо", "курин", "свинин", "говядин"],
    "СОКИ, ВОДА": ["сок", "вода", "нектар", "морс", "компот", "лимонад", "квас"],
    "СЕМЕЧКИ, ЧИПСЫ, СУХ": ["семечк", "чипс", "сухар", "снек", "орех", "попкорн"],
    "МАЙОНЕЗЫ, СОУСЫ, КЕ": ["майонез", "кетчуп", "соус", "горчиц", "хрен"],
    "МАСЛА": ["масло раст", "масло олив", "масло подсолн"],
    "КРУПЫ": ["крупа", "рис", "гречк", "перловк", "пшен", "овсян", "манн"],
    "МАКАРОНЫ": ["макарон", "спагетт", "вермишел", "лапш"],
    "МУКА": ["мука", "крахмал", "дрожж"],
    "ПРИПРАВЫ И СПЕЦИИ": ["специ", "приправ", "перец молот", "лавров", "куркум", "паприк"],
    "ФРУКТЫ, ОВОЩИ": ["яблок", "груш", "банан", "апельсин", "морковь", "картофел", "помидор", "огурец", "капуст"],
    "ЧАЙ, КОФЕ": ["чай", "кофе", "какао", "цикори"],
    "БЫТОВАЯ ХИМИЯ": ["шампун", "гель", "мыло", "порошок", "средство", "дезодорант", "зубная"],
}


def detect_category(name: str) -> str:
    """Определить категорию товара по названию"""
    name_lower = name.lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in name_lower:
                return category
    return "Главный экран"


def expand_abbreviations(name: str) -> str:
    """Заменяем аббревиатуры на полные названия для лучшего поиска"""
    name_lower = name.lower()
    for abbr, full in ABBREVIATIONS.items():
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
                    json={"storehouseId": self.storehouse_id, "isProduced": None, "waybillId": None}
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
                    json={"storehouseId": self.storehouse_id, "isProduced": None, "waybillId": None}
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
        """Нечёткий поиск товара в кэше по словам"""
        if cache is None:
            cache = _products_cache
        if not cache:
            return None

        name_lower = expand_abbreviations(name.lower())
        for old, new in PRODUCT_NAME_FIXES.items():
            name_lower = name_lower.replace(old, new)
        name_lower = re.sub(r'(\d+)гр\b', r'\1г', name_lower)
        name_lower = re.sub(r'\b\d+/\d+\b', '', name_lower)

        # Числа >= 20 из запроса — обязательны
        numbers = set(n.replace(',', '.') for n in re.findall(r'\d+(?:[.,]\d+)?', name_lower))
        numbers = {n for n in numbers if float(n) >= 20}

        # Процент жирности (3.2%, 9%, 15% и т.д.) — обязателен для точного совпадения
        # Если и в запросе, и в товаре Sigma указан %, они должны совпадать
        query_pcts = {m.replace(',', '.') for m in re.findall(r'(\d+(?:[.,]\d+)?)\s*%', name_lower)}

        # Слова для нечёткого поиска
        tokens = [w for w in re.split(r'[\s,./\\%\-]+', name_lower) if len(w) >= 3 and not re.match(r'^\d+$', w)]

        if not tokens and not numbers:
            return None

        best_match = None
        best_score = 0

        for product in cache:
            p_name = expand_abbreviations(product.get("name", "").lower())
            p_numbers = set(n.replace(',', '.') for n in re.findall(r'\d+(?:[.,]\d+)?', p_name))
            p_pcts = {m.replace(',', '.') for m in re.findall(r'(\d+(?:[.,]\d+)?)\s*%', p_name)}
            p_tokens = [w for w in re.split(r'[\s,./\\%\-]+', p_name) if len(w) >= 3 and not re.match(r'^\d+$', w)]

            # Каждое обязательное число должно быть в товаре
            num_mismatch = False
            for num in numbers:
                if num not in p_numbers:
                    num_mismatch = True
                    break
            if num_mismatch:
                continue

            # Если в запросе указан % жирности И в товаре тоже — они должны совпадать
            # Предотвращает матч Молоко 3.2% → Молоко 2.5%, Сметана 15% → Сметана 20% и т.д.
            if query_pcts and p_pcts and query_pcts != p_pcts:
                continue

            # Если в запросе есть % — не матчиться к товарам без % (снеки, чипсы со вкусом)
            if query_pcts and not p_pcts:
                continue

            # Если запрос специфичный (> 1 токена) — проверяем что товар Sigma тоже "вписывается"
            # Предотвращает матч "Сметана ОВК 20% 180г" → "Сметана КАРЛАМАН 20% 180г"
            if len(tokens) > 1 and p_tokens:
                p_reverse = sum(1 for pt in p_tokens if pt in name_lower) / len(p_tokens)
                if p_reverse < 0.35:
                    continue

            matched = sum(1 for t in tokens if t in p_name)
            score = sum(len(t) for t in tokens if t in p_name)
            if matched > 0:
                ratio = matched / len(tokens) if tokens else 1
                if ratio >= 0.35 and score > best_score:
                    best_score = score
                    best_match = product

        if best_match:
            logger.info(f"Cache hit: '{name[:30]}' → '{best_match.get('name', '')[:40]}'")
        else:
            logger.info(f"Cache miss: '{name[:30]}' | required={numbers} | pct={query_pcts} | tokens={tokens[:4]}")
        return best_match


    async def find_product(self, name: str) -> Optional[dict]:
        """Поиск товара в Sigma.
        Основной путь: API-поиск по ключевым словам (как вручную в строке поиска Sigma).
        Запасной: кэш всех товаров (если API ничего не вернул).
        """
        # ── 1. API-поиск по ключевым словам ──────────────────────────────────
        # Берём производителя + тип товара из названия → Sigma возвращает 3-10 результатов
        name_expanded = expand_abbreviations(name.lower())
        for old, new in PRODUCT_NAME_FIXES.items():
            name_expanded = name_expanded.replace(old, new)
        words = [w for w in re.split(r'[\s,./\\%\-]+', name_expanded)
                 if len(w) >= 3 and not re.match(r'^\d+$', w)]
        if words:
            search_query = ' '.join(words[:3])[:30]
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(
                    f"{BASE_URL}/rest/v4/products/simple",
                    headers=self._headers(),
                    params={"page": 0},
                    json={"name": search_query, "storehouseId": None, "isProduced": None, "waybillId": None}
                )
                logger.info(f"API search '{search_query}': {r.status_code}")
                if r.status_code == 200:
                    content = r.json().get("productsInfo", {}).get("content", [])
                    if content:
                        match = self.find_product_in_cache(name, cache=content)
                        if match:
                            logger.info(f"Found via API search: '{match.get('name', '')[:40]}'")
                            return match

        # ── 2. Кэш как запасной путь ─────────────────────────────────────────
        if _cache_loaded:
            cached = self.find_product_in_cache(name)
            if cached:
                logger.info(f"Found in cache: '{cached.get('name', '')[:40]}'")
                return cached

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

        return {
            "ok": True,
            "waybill_id": waybill_id,
            "waybill_number": waybill_number,
            "added": added,
            "skipped": skipped,
            "conducted": False
        }


async def recognize_invoice(image_bytes: bytes) -> dict:
    import base64
    import re as _re
    OPENROUTER_KEY = os.getenv("OPENROUTER_KEY", "")
    img_b64 = base64.b64encode(image_bytes).decode()

    prompt = """Ты читаешь накладную (товарная накладная, счёт-фактура, УПД) от поставщика продуктового магазина.
У разных поставщиков структура накладной отличается — адаптируйся к тому что видишь.

ЗАДАЧА:
1. Найди таблицу с товарами.
2. Для каждой строки товара определи: название, количество, цену за единицу.
3. Строки без количества или с нулевым количеством — пропускай.
4. Итоговые строки ("Итого", "Всего") — не включай в товары.
5. name — ДОСЛОВНО как написано в накладной.
6. Поставщик — название ООО/ИП из шапки документа.
7. total_sum — итоговая сумма документа.

Верни ТОЛЬКО JSON без markdown:
{"supplier":"название","total_sum":число,"items":[{"name":"дословное название","qty":число,"qty_str":"строка","price":число}]}"""

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_KEY}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com/tripmusicrussia-hub/Triple",
            },
            json={
                "model": "google/gemini-2.0-flash-001",
                "max_tokens": 2000,
                "messages": [{
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}},
                        {"type": "text", "text": prompt}
                    ]
                }]
            }
        )
        j = resp.json()
        logger.info(f"Gemini vision response: {str(j)[:300]}")
        if "error" in j:
            raise Exception(j["error"].get("message", str(j["error"])))
        raw = j["choices"][0]["message"]["content"]

    raw_clean = _re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\x80-\x9f]', '', raw)
    raw_clean = raw_clean.replace("```json", "").replace("```", "").strip()
    match = _re.search(r'\{.*\}', raw_clean, _re.DOTALL)
    if match:
        raw_clean = match.group(0)
    try:
        return json.loads(raw_clean)
    except Exception:
        raw_clean2 = ''.join(c for c in raw_clean if ord(c) >= 32 or c in '\n\r\t')
        return json.loads(raw_clean2)
