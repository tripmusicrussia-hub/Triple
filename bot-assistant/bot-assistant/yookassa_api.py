"""YooKassa REST API client для Triple Bot.

Direct integration (не через Telegram Payments): клиент платит на странице
YooKassa, потом webhook уведомляет нас об успехе → доставка товара.

Преимущества перед `sendInvoice` + provider_token:
- ВСЕ методы оплаты (карты MIR/Visa/MC, СБП, T-Pay, SberPay, ЮMoney)
- Можно подключать новые методы без изменений в коде
- Single source of truth — YooKassa dashboard

API docs: https://yookassa.ru/developers/api

Endpoints used:
- POST /v3/payments — create payment
- GET  /v3/payments/{id} — poll status (fallback если webhook не дошёл)

Webhook verification:
- YooKassa НЕ подписывает body криптографически (в отличие от Stripe).
- Defence-in-depth: IP-whitelist YooKassa servers (список в docs) +
  обязательный GET /v3/payments/{id} как double-check перед delivery.
  Это гарантирует что payment реально succeeded у YooKassa, даже если
  кто-то притворился webhook'ом.
"""
from __future__ import annotations

import base64
import logging
import os
import uuid
from typing import Any

import httpx

logger = logging.getLogger(__name__)

YK_API_BASE = "https://api.yookassa.ru/v3"

# IP-адреса серверов YooKassa откуда приходят webhook'и
# Источник: https://yookassa.ru/developers/using-api/webhooks#ip
YK_WEBHOOK_IPS = {
    "185.71.76.0/27",
    "185.71.77.0/27",
    "77.75.153.0/25",
    "77.75.156.11",
    "77.75.156.35",
    "77.75.154.128/25",
    "2a02:5180::/32",
}


def _auth_header() -> str:
    """Basic auth header: base64(shop_id:secret_key). Strip всегда —
    помним trailing newline bug от предыдущих инцидентов.
    """
    shop_id = (os.getenv("YOOKASSA_SHOP_ID") or "").strip()
    secret = (os.getenv("YOOKASSA_SECRET_KEY") or "").strip()
    if not shop_id or not secret:
        raise RuntimeError(
            "YooKassa credentials missing (YOOKASSA_SHOP_ID or YOOKASSA_SECRET_KEY)"
        )
    token = base64.b64encode(f"{shop_id}:{secret}".encode()).decode()
    return f"Basic {token}"


def is_configured() -> bool:
    """True если YooKassa credentials присутствуют — UI может показывать RUB-кнопки."""
    return bool(
        (os.getenv("YOOKASSA_SHOP_ID") or "").strip()
        and (os.getenv("YOOKASSA_SECRET_KEY") or "").strip()
    )


async def create_payment(
    amount_rub: int | float,
    description: str,
    metadata: dict[str, Any],
    return_url: str = "https://t.me/triplekillpost_bot",
) -> dict[str, Any]:
    """Создаёт платёж в YooKassa. Возвращает dict с полями:
        - id: payment UUID (храним для matching в webhook)
        - status: "pending"
        - confirmation.confirmation_url: URL для редиректа клиента
        - amount.value, amount.currency

    amount_rub: сумма в рублях (Decimal-совместимая). YooKassa ждёт строку
                "1700.00" с 2 знаками после точки.

    metadata: любой JSON dict, вернётся обратно в webhook callback. Туда
              кладём user_id, beat_id, type (mp3_lease/mixing/product)
              чтобы найти юзера при payment.succeeded.

    return_url: куда клиент попадёт после оплаты. По дефолту — в наш бот.

    Idempotence-Key header (UUID4) — защищает от дубликатов при retry.
    """
    amount_str = f"{float(amount_rub):.2f}"
    body = {
        "amount": {"value": amount_str, "currency": "RUB"},
        "capture": True,  # мгновенное списание, без холдирования
        "confirmation": {
            "type": "redirect",
            "return_url": return_url,
        },
        "description": description[:128],  # YooKassa limit 128 chars
        "metadata": metadata,
    }
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"{YK_API_BASE}/payments",
            headers={
                "Authorization": _auth_header(),
                "Idempotence-Key": str(uuid.uuid4()),
                "Content-Type": "application/json",
            },
            json=body,
        )
        if r.status_code >= 400:
            logger.warning("YK create_payment HTTP %d: %s", r.status_code, r.text[:300])
        r.raise_for_status()
        return r.json()


async def get_payment(payment_id: str) -> dict[str, Any]:
    """Получает текущий статус платежа. Используется в fallback polling
    если webhook не дошёл. Также используется для double-check перед
    delivery при получении webhook (защита от фальшивого webhook'а).
    """
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{YK_API_BASE}/payments/{payment_id}",
            headers={"Authorization": _auth_header()},
        )
        if r.status_code >= 400:
            logger.warning("YK get_payment %s HTTP %d: %s", payment_id, r.status_code, r.text[:300])
        r.raise_for_status()
        return r.json()


def ip_in_webhook_whitelist(ip: str) -> bool:
    """Проверяет что IP источника webhook'а — из списка YooKassa серверов.
    Это основная защита, т.к. webhook body сам не подписан.

    Использует ipaddress модуль для CIDR matching. IPv4 и IPv6.
    """
    import ipaddress
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    for spec in YK_WEBHOOK_IPS:
        try:
            if "/" in spec:
                if addr in ipaddress.ip_network(spec, strict=False):
                    return True
            else:
                if addr == ipaddress.ip_address(spec):
                    return True
        except ValueError:
            continue
    return False
