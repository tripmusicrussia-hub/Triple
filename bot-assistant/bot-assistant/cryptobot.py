"""Crypto Pay API client — приём USDT через @CryptoBot.

Docs: https://help.crypt.bot/crypto-pay-api
Auth: заголовок `Crypto-Pay-API-Token: <token>` (env CRYPTOBOT_TOKEN).

Используется два endpoint'а:
  • createInvoice — создать счёт, получить pay_url и invoice_id
  • getInvoices   — проверить статус (polling вместо webhook, чтобы не заводить публичный URL)
"""
from __future__ import annotations

import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://pay.crypt.bot/api"


def _token() -> str:
    t = os.getenv("CRYPTOBOT_TOKEN", "").strip()
    if not t:
        raise RuntimeError("CRYPTOBOT_TOKEN не задан в env")
    return t


async def create_invoice(
    amount: float,
    asset: str,
    description: str,
    payload: str,
    expires_in: int = 1800,
) -> dict[str, Any]:
    """Создаёт счёт. Возвращает dict с invoice_id, pay_url, status и т.д."""
    body = {
        "currency_type": "crypto",
        "asset": asset,
        "amount": str(amount),
        "description": description[:1024],
        "payload": payload[:4096],
        "expires_in": expires_in,
        "allow_comments": False,
        "allow_anonymous": True,
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{_BASE_URL}/createInvoice",
            headers={"Crypto-Pay-API-Token": _token()},
            json=body,
        )
        j = resp.json()
    if not j.get("ok"):
        raise RuntimeError(f"createInvoice failed: {j}")
    return j["result"]


async def get_invoice(invoice_id: int) -> dict[str, Any] | None:
    """Возвращает инвойс по id или None если не найден."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{_BASE_URL}/getInvoices",
            headers={"Crypto-Pay-API-Token": _token()},
            params={"invoice_ids": str(invoice_id)},
        )
        j = resp.json()
    if not j.get("ok"):
        logger.warning("getInvoices failed: %s", j)
        return None
    items = (j.get("result") or {}).get("items") or []
    return items[0] if items else None
