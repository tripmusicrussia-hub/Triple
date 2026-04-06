"""
sigma_api.py — интеграция с API cloud.sigma.ru
"""
import httpx
import json
import os
import math
import logging
from typing import Optional

logger = logging.getLogger(__name__)

SIGMA_LOGIN = os.getenv("SIGMA_LOGIN", "")
SIGMA_PASSWORD = os.getenv("SIGMA_PASSWORD", "")
SIGMA_AUTH_HEADER = "Basic cWFzbGFwcDpteVNlY3JldE9BdXJoU2VjcmV0"
BASE_URL = "https://api-s07.sigma.ru"

# Категории с наценкой 10%
LOW_MARGIN_KEYWORDS = [
    "хлеб", "булка", "батон", "лаваш", "пита", "багет", "буханка",
    "молоко", "молоке", "молока",
]

def get_markup(name: str) -> float:
    """Определить наценку по названию товара"""
    name_lower = name.lower()
    for kw in LOW_MARGIN_KEYWORDS:
        if kw in name_lower:
            return 0.10
    return 0.25

def calc_price(buy_price: float, markup: float) -> int:
    """Цена продажи = закупка × (1 + наценка), округление вверх"""
    return math.ceil(buy_price * (1 + markup))

def format_items_preview(items: list) -> str:
    """Форматировать список товаров для проверки"""
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


class SigmaAPI:
    def __init__(self):
        self.token = None
        self.company_id = None
        self.storehouse_id = None

    async def login(self) -> bool:
        """Получить токен через OAuth2"""
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{BASE_URL}/oauth/token",
                headers={
                    "Authorization": SIGMA_AUTH_HEADER,
                    "Content-Type": "application/x-www-form-urlencoded",
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
        """Получить ID компании и склада"""
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(f"{BASE_URL}/rest/1.1/account", headers=self._headers())
            if r.status_code != 200:
                return False
            data = r.json()
            self.company_id = data.get("company", {}).get("id")

            # Get storehouses
            r2 = await client.get(f"{BASE_URL}/rest/1.1/storehouses", headers=self._headers())
            if r2.status_code == 200:
                houses = r2.json()
                if houses:
                    self.storehouse_id = houses[0].get("id")
            return bool(self.company_id)

    async def get_suppliers(self) -> list:
        """Получить список поставщиков"""
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{BASE_URL}/rest/1.1/suppliers",
                headers=self._headers(),
                params={"size": 100}
            )
            if r.status_code == 200:
                return r.json().get("content", r.json() if isinstance(r.json(), list) else [])
            return []

    async def find_supplier(self, name: str) -> Optional[str]:
        """Найти поставщика по имени, вернуть его ID"""
        suppliers = await self.get_suppliers()
        name_lower = name.lower()
        # Точное совпадение
        for s in suppliers:
            if s.get("name", "").lower() == name_lower:
                return s.get("id")
        # Частичное совпадение
        for s in suppliers:
            if name_lower in s.get("name", "").lower() or s.get("name", "").lower() in name_lower:
                return s.get("id")
        return None

    async def find_product(self, name: str) -> Optional[dict]:
        """Найти товар по названию"""
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                f"{BASE_URL}/rest/1.1/products/simple",
                headers=self._headers(),
                params={"page": 0},
                json={
                    "name": name[:30],
                    "storehouseId": self.storehouse_id,
                    "isProduced": None,
                    "waybillId": None
                }
            )
            if r.status_code == 200:
                data = r.json()
                content = data.get("productsInfo", {}).get("content", [])
                if content:
                    return content[0]
            return None

    async def create_income(self, supplier_id: Optional[str] = None) -> Optional[str]:
        """Создать документ прихода, вернуть его ID"""
        async with httpx.AsyncClient(timeout=15) as client:
            payload = {
                "storehouseId": self.storehouse_id,
                "supplierId": supplier_id,
            }
            r = await client.post(
                f"{BASE_URL}/rest/1.1/waybills",
                headers=self._headers(),
                json=payload
            )
            if r.status_code in (200, 201):
                data = r.json()
                return data.get("id")
            logger.error(f"Create income failed: {r.status_code} {r.text[:300]}")
            return None

    async def add_product_to_income(self, waybill_id: str, product_id: str, qty: float, buy_price: float, sell_price: float) -> bool:
        """Добавить товар в приход"""
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                f"{BASE_URL}/rest/1.1/waybills/{waybill_id}/elements/product/{product_id}",
                headers=self._headers(),
                json={
                    "amount": qty,
                    "buyingPrice": buy_price,
                    "sellingPrice": sell_price,
                }
            )
            return r.status_code in (200, 201)

    async def conduct_income(self, waybill_id: str) -> bool:
        """Провести документ прихода"""
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                f"{BASE_URL}/rest/1.1/waybills/{waybill_id}/conduct",
                headers=self._headers(),
                json={}
            )
            return r.status_code in (200, 201)

    async def process_invoice(self, items: list, supplier_name: str) -> dict:
        """
        Полный цикл: логин → найти поставщика → создать приход → добавить товары → провести
        items: [{"name": str, "qty": float, "price": float}]
        """
        # Login
        if not await self.login():
            return {"ok": False, "error": "Не удалось войти в Sigma. Проверь логин/пароль."}

        # Get company info
        await self.get_company_info()

        # Find supplier
        supplier_id = await self.find_supplier(supplier_name)
        if not supplier_id:
            logger.warning(f"Supplier not found: {supplier_name}")

        # Create income document
        waybill_id = await self.create_income(supplier_id)
        if not waybill_id:
            return {"ok": False, "error": "Не удалось создать документ прихода в Sigma."}

        # Add products
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

        # Conduct
        conducted = await self.conduct_income(waybill_id)

        return {
            "ok": True,
            "waybill_id": waybill_id,
            "added": added,
            "skipped": skipped,
            "conducted": conducted
        }
