"""Тесты для yookassa_api — pure-функции (без HTTP)."""
import os

import yookassa_api


def test_ip_whitelist_v4_single():
    # 77.75.156.11 — точечный IP в whitelist
    assert yookassa_api.ip_in_webhook_whitelist("77.75.156.11") is True


def test_ip_whitelist_v4_cidr():
    # 185.71.76.0/27 — первый, середина, последний IP
    assert yookassa_api.ip_in_webhook_whitelist("185.71.76.0") is True
    assert yookassa_api.ip_in_webhook_whitelist("185.71.76.15") is True
    assert yookassa_api.ip_in_webhook_whitelist("185.71.76.31") is True
    # за пределами
    assert yookassa_api.ip_in_webhook_whitelist("185.71.76.32") is False


def test_ip_whitelist_rejects_random():
    assert yookassa_api.ip_in_webhook_whitelist("8.8.8.8") is False
    assert yookassa_api.ip_in_webhook_whitelist("1.2.3.4") is False


def test_ip_whitelist_bad_input():
    assert yookassa_api.ip_in_webhook_whitelist("not-an-ip") is False
    assert yookassa_api.ip_in_webhook_whitelist("") is False


def test_is_configured_missing(monkeypatch):
    monkeypatch.delenv("YOOKASSA_SHOP_ID", raising=False)
    monkeypatch.delenv("YOOKASSA_SECRET_KEY", raising=False)
    assert yookassa_api.is_configured() is False


def test_is_configured_present(monkeypatch):
    monkeypatch.setenv("YOOKASSA_SHOP_ID", "1335835")
    monkeypatch.setenv("YOOKASSA_SECRET_KEY", "live_test")
    assert yookassa_api.is_configured() is True


def test_is_configured_whitespace_only(monkeypatch):
    # Защита от trailing \n / пробелов (прошлый инцидент с YT_CLIENT_ID)
    monkeypatch.setenv("YOOKASSA_SHOP_ID", "  \n")
    monkeypatch.setenv("YOOKASSA_SECRET_KEY", "live_test")
    assert yookassa_api.is_configured() is False


def test_auth_header_strips_whitespace(monkeypatch):
    import base64
    monkeypatch.setenv("YOOKASSA_SHOP_ID", "1335835\n")
    monkeypatch.setenv("YOOKASSA_SECRET_KEY", " live_secret ")
    header = yookassa_api._auth_header()
    assert header.startswith("Basic ")
    decoded = base64.b64decode(header.split(" ", 1)[1]).decode()
    assert decoded == "1335835:live_secret"


def test_auth_header_raises_without_creds(monkeypatch):
    monkeypatch.delenv("YOOKASSA_SHOP_ID", raising=False)
    monkeypatch.delenv("YOOKASSA_SECRET_KEY", raising=False)
    import pytest
    with pytest.raises(RuntimeError):
        yookassa_api._auth_header()
