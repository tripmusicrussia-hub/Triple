"""
sigma_api.py — интеграция с API cloud.sigma.ru
"""
import httpx
import json
import os
import math
import re
import logging
from typing import Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)

SIGMA_LOGIN = os.getenv("SIGMA_LOGIN", "")
SIGMA_PASSWORD = os.getenv("SIGMA_PASSWORD", "")
SIGMA_AUTH_HEADER = "Basic cWFzbGFwcDpteVNlY3JldE9BdXRoU2VjcmV0"
BASE_URL = "https://api-s07.sigma.ru"

LOW_MARGIN_KEYWORDS = [
    "хлеб", "булка", "батон", "лаваш", "пита", "багет", "буханка",
    "молоко", "молоке", "молока",
]

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
    name = item["name"]
    qty = item.get("qty", 1)
    price = item.get("price", 0)
    match = re.search(r'(\d+(?:[.,]\d+)?)\s*кг', name.lower())
    if match and qty == 1 and price > 0:
        weight = float(match.group(1).replace(",", "."))
        if weight > 0:
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

    async def login(self) -> bool:
        """Получить токен через OAuth2"""
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
            logger.info(f"Sigma response: {resp.status_code} {resp.text[:300]}")
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
            logger.info(f"Account response: {r.status_code} {r.text[:500]}")
            if r.status_code != 200:
                return False
            data = r.json()
            # Try different paths to find company_id
            self.company_id = (
                data.get("company", {}).get("id") or
                data.get("companyId") or
                data.get("id")
            )
            logger.info(f"Sigma company_id: {self.company_id}, full data keys: {list(data.keys())}")
            if not self.company_id:
                return False
            r2 = await client.get(
                f"{BASE_URL}/rest/v2/companies/{self.company_id}/storehouses",
                headers=self._headers()
            )
            logger.info(f"Storehouses: {r2.status_code} {r2.text[:300]}")
            if r2.status_code == 200:
                houses = r2.json()
                items = houses.get("content", houses) if isinstance(houses, dict) else houses
                if items:
                    self.storehouse_id = items[0].get("id")
                    logger.info(f"Sigma storehouse_id: {self.storehouse_id}")
            return bool(self.company_id)

    async def get_suppliers(self) -> list:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{BASE_URL}/rest/v2/companies/{self.company_id}/suppliers",
                headers=self._headers(),
                params={"size": 100}
            )
            logger.info(f"Suppliers: {r.status_code} {r.text[:200]}")
            if r.status_code == 200:
                data = r.json()
                return data.get("content", data) if isinstance(data, dict) else data
            return []

    async def find_supplier(self, name: str) -> Optional[str]:
        suppliers = await self.get_suppliers()
        name_lower = name.lower()
        for s in suppliers:
            if s.get("name", "").lower() == name_lower:
                return s.get("id")
        for s in suppliers:
            if name_lower in s.get("name", "").lower() or s.get("name", "").lower() in name_lower:
                return s.get("id")
        return None

    async def find_product(self, name: str) -> Optional[dict]:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                f"{BASE_URL}/rest/v4/products/simple",
                headers=self._headers(),
                params={"page": 0},
                json={
                    "name": name[:30],
                    "storehouseId": self.storehouse_id,
                    "isProduced": None,
                    "waybillId": None
                }
            )
            logger.info(f"Find product '{name[:20]}': {r.status_code} {r.text[:300]}")
            if r.status_code == 200:
                data = r.json()
                content = data.get("content", [])
                if content:
                    return content[0]
            return None

    async def create_income(self, supplier_id: Optional[str] = None) -> Optional[str]:
        async with httpx.AsyncClient(timeout=15) as client:
            payload = {
                "storehouseId": self.storehouse_id,
                "supplierId": supplier_id,
                "type": "INCOME"
            }
            r = await client.post(
                f"{BASE_URL}/waybills",
                headers=self._headers(),
                json=payload
            )
            logger.info(f"Create income: {r.status_code} {r.text[:300]}")
            if r.status_code in (200, 201):
                return r.json().get("id")
            logger.error(f"Create income failed: {r.status_code} {r.text[:300]}")
            return None

    async def add_product_to_income(self, waybill_id: str, product_id: str, qty: float, buy_price: float, sell_price: float) -> bool:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                f"{BASE_URL}/waybills/{waybill_id}/elements/product/{product_id}",
                headers=self._headers(),
                json={"amount": qty, "buyingPrice": buy_price, "sellingPrice": sell_price}
            )
            logger.info(f"Add product {product_id}: {r.status_code} {r.text[:200]}")
            return r.status_code in (200, 201)

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

        supplier_id = await self.find_supplier(supplier_name)
        if not supplier_id:
            logger.warning(f"Supplier not found: {supplier_name}")

        waybill_id = await self.create_income(supplier_id)
        if not waybill_id:
            return {"ok": False, "error": "Не удалось создать документ прихода в Sigma."}

        added = 0
        skipped = []
        for item in items:
            product = await self.find_product(item["name"])
            if not product:
                skipped.append(item["name"])
                continue
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

        if added == 0:
            return {"ok": False, "error": "Ни один товар не добавлен. Возможно названия не совпадают с базой Sigma."}

        return {
            "ok": True,
            "waybill_id": waybill_id,
            "added": added,
            "skipped": skipped,
            "conducted": False
        }


async def ocr_yandex(image_bytes: bytes) -> str:
    import base64
    YANDEX_KEY = os.getenv("YANDEX_API_KEY", "")
    YANDEX_FOLDER = os.getenv("YANDEX_FOLDER_ID", "")
    img_b64 = base64.b64encode(image_bytes).decode()
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze",
            headers={"Authorization": f"Api-Key {YANDEX_KEY}", "Content-Type": "application/json"},
            json={
                "folderId": YANDEX_FOLDER,
                "analyzeSpecs": [{
                    "content": img_b64,
                    "features": [{"type": "TEXT_DETECTION", "textDetectionConfig": {"languageCodes": ["ru", "en"]}}]
                }]
            }
        )
        data = resp.json()
    lines = []
    try:
        pages = data["results"][0]["results"][0]["textDetection"]["pages"]
        for page in pages:
            for block in page.get("blocks", []):
                for line in block.get("lines", []):
                    words = [w["text"] for w in line.get("words", [])]
                    if words:
                        lines.append(" ".join(words))
    except Exception as e:
        logger.error(f"Yandex OCR parse error: {e}, response: {str(data)[:300]}")
    return "\n".join(lines)


async def recognize_invoice(image_bytes: bytes) -> dict:
    GROQ_KEY = os.getenv("GROQ_API_KEY", "")

    raw_text = await ocr_yandex(image_bytes)
    logger.info(f"Yandex OCR result:\n{raw_text[:500]}")

    if not raw_text.strip():
        raise Exception("Yandex OCR не смог прочитать текст с изображения")

    prompt = f"""Это текст накладной от поставщика, извлечённый OCR системой.

ТЕКСТ НАКЛАДНОЙ:
{raw_text}

ЗАДАЧА: найди все строки с товарами и их количеством/ценой.

Правила:
- Если это печатная накладная с галочками: бери только строки где рядом с товаром есть число (кол-во заказа)
- Если это рукописная накладная: бери все строки где есть название товара + число кол-во + число цена
- Пропускай: пустые строки, итоги (Итого, НДС, сумма прописью), заголовки секций без цены
- Поставщик: из строки "Поставщик", "От кого", или название компании ООО/ИП в шапке

Верни ТОЛЬКО JSON без markdown:
{{"supplier":"название","items":[{{"name":"точное название товара","qty":число,"price":число}}]}}"""

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "max_tokens": 2000,
                "messages": [{"role": "user", "content": prompt}]
            }
        )
        j = resp.json()
        if "error" in j:
            raise Exception(j["error"]["message"])
        raw = j["choices"][0]["message"]["content"]

    return json.loads(raw.replace("```json", "").replace("```", "").strip())
