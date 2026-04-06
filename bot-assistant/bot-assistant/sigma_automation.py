"""
sigma_automation.py — автоматическое заполнение приходов в cloud.sigma.ru
Использует Playwright для управления браузером
"""
import asyncio
import os
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

SIGMA_URL = "https://cloud.sigma.ru"
SIGMA_LOGIN = os.getenv("SIGMA_LOGIN", "")
SIGMA_PASSWORD = os.getenv("SIGMA_PASSWORD", "")


async def fill_income(items: list, supplier: str) -> dict:
    """
    Создаёт приход в sigma и заполняет товары.
    items: [{"name": str, "qty": float, "price": float}]
    supplier: название поставщика
    Возвращает {"ok": True, "doc_number": "..."} или {"ok": False, "error": "..."}
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return {"ok": False, "error": "Playwright не установлен. Добавь playwright в requirements.txt"}

    if not SIGMA_LOGIN or not SIGMA_PASSWORD:
        return {"ok": False, "error": "SIGMA_LOGIN или SIGMA_PASSWORD не заданы в переменных окружения"}

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        try:
            # Step 1: Login
            logger.info("Sigma: opening login page...")
            await page.goto(f"{SIGMA_URL}/login", wait_until="networkidle", timeout=30000)
            await page.fill('input[type="email"], input[name="email"], input[placeholder*="mail"]', SIGMA_LOGIN)
            await page.fill('input[type="password"]', SIGMA_PASSWORD)
            await page.click('button[type="submit"], button:has-text("Войти"), button:has-text("Вход")')
            await page.wait_for_load_state("networkidle", timeout=15000)
            logger.info("Sigma: logged in")

            # Step 2: Go to income documents
            await page.goto(f"{SIGMA_URL}/documents/INCOME", wait_until="networkidle", timeout=15000)

            # Step 3: Create new income
            await page.click('button:has-text("Создать приход"), a:has-text("Создать приход")')
            await page.wait_for_load_state("networkidle", timeout=10000)

            # Step 4: Select supplier
            logger.info(f"Sigma: selecting supplier '{supplier}'...")
            supplier_selector = 'div[class*="supplier"], select[name*="supplier"], div:has-text("Выбрать поставщика")'
            await page.click(supplier_selector)
            await asyncio.sleep(0.5)

            # Type supplier name to filter
            await page.keyboard.type(supplier[:10], delay=50)
            await asyncio.sleep(1)

            # Click matching option
            try:
                await page.click(f'li:has-text("{supplier[:15]}"), div[role="option"]:has-text("{supplier[:15]}")', timeout=5000)
            except Exception:
                # Try clicking first option in dropdown
                await page.click('li[role="option"]:first-child, div[role="option"]:first-child', timeout=3000)

            # Step 5: Click Continue
            await page.click('button:has-text("Продолжить")')
            await page.wait_for_load_state("networkidle", timeout=10000)
            logger.info("Sigma: income document created")

            # Get document number
            doc_number = ""
            try:
                doc_text = await page.text_content('text=/Приход №\\s*\\d+/')
                doc_number = doc_text.strip() if doc_text else ""
            except Exception:
                pass

            # Step 6: Add items
            logger.info(f"Sigma: adding {len(items)} items...")
            for i, item in enumerate(items):
                try:
                    # Click on item search field
                    search_field = page.locator('input[placeholder*="название товара"], input[placeholder*="Введите название"]').last
                    await search_field.click()
                    await search_field.fill("")
                    await asyncio.sleep(0.3)

                    # Type item name (first 20 chars for search)
                    search_term = item["name"][:20]
                    await search_field.type(search_term, delay=50)
                    await asyncio.sleep(1)

                    # Click first matching option
                    try:
                        option = page.locator(f'li:has-text("{item["name"][:15]}"), div[class*="option"]:has-text("{item["name"][:15]}")').first
                        await option.click(timeout=4000)
                    except Exception:
                        # Try first option
                        await page.click('li[class*="option"]:first-child, div[class*="dropdown"] li:first-child', timeout=3000)

                    await asyncio.sleep(0.5)

                    # Fill quantity - find the qty input in the last row
                    qty_inputs = page.locator('td input[type="number"], td input[class*="qty"], td input[class*="quantity"]')
                    qty_count = await qty_inputs.count()
                    if qty_count > 0:
                        await qty_inputs.last.triple_click()
                        await qty_inputs.last.fill(str(item["qty"]))
                        await qty_inputs.last.press("Tab")

                    await asyncio.sleep(0.3)

                    # Fill purchase price
                    price_inputs = page.locator('td input[class*="price"], td input[class*="cost"]')
                    price_count = await price_inputs.count()
                    if price_count > 0:
                        await price_inputs.last.triple_click()
                        await price_inputs.last.fill(str(item["price"]).replace(",", "."))
                        await price_inputs.last.press("Tab")

                    await asyncio.sleep(0.3)
                    logger.info(f"Sigma: added item {i+1}/{len(items)}: {item['name']}")

                except Exception as e:
                    logger.error(f"Sigma: error adding item {item['name']}: {e}")
                    continue

            # Step 7: Save/Conduct document
            await asyncio.sleep(1)
            try:
                # Click "Провести" (conduct)
                await page.click('button:has-text("Провести")', timeout=5000)
                await page.wait_for_load_state("networkidle", timeout=10000)
                logger.info("Sigma: document conducted!")
            except Exception:
                logger.info("Sigma: document saved as draft")

            return {"ok": True, "doc_number": doc_number, "items_added": len(items)}

        except Exception as e:
            logger.error(f"Sigma automation error: {e}")
            return {"ok": False, "error": str(e)}
        finally:
            await browser.close()


async def recognize_invoice(image_bytes: bytes) -> dict:
    """
    Распознаёт накладную через Groq Vision.
    Возвращает {"supplier": str, "items": [...]}
    """
    import httpx
    import base64

    GROQ_KEY = os.getenv("GROQ_API_KEY", "")
    img_b64 = base64.b64encode(image_bytes).decode()

    prompt = """Это накладная из продуктового магазина.
Найди ТОЛЬКО товары где в колонке "Заказ" есть галочка (v или ✓) или рукописная цифра.
Если галочка без цифры — qty=1. Если написана цифра — используй её.
Поставщик указан в шапке (ООО, ИП или название компании).

Верни ТОЛЬКО JSON без markdown:
{"supplier":"название","items":[{"name":"полное название товара","qty":число,"price":цена за единицу}]}"""

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"},
            json={
                "model": "meta-llama/llama-4-scout-17b-16e-instruct",
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
        if "error" in j:
            raise Exception(j["error"]["message"])
        raw = j["choices"][0]["message"]["content"]

    parsed = json.loads(raw.replace("```json", "").replace("```", "").strip())
    return parsed
