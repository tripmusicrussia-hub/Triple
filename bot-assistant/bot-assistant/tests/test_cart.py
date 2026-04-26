"""Tests для bundle cart — накопительная корзина для bundle-покупки.

Покрывает helpers _cart_get/_add/_remove/_clear + persistence (`bundle_carts.json`).
UI и payment flow тестируется через bundle tests + live.
"""
from __future__ import annotations

import json
import os
import tempfile
from unittest.mock import patch

import pytest


@pytest.fixture
def fresh_bot(monkeypatch, tmp_path):
    """Импортирует bot с временным BUNDLE_CART_PATH и чистым состоянием."""
    monkeypatch.setenv("BOT_TOKEN", "TEST")
    monkeypatch.setenv("ADMIN_ID", "1")
    monkeypatch.setenv("CHANNEL_ID", "@test")
    import bot as _bot
    cart_path = tmp_path / "bundle_carts.json"
    monkeypatch.setattr(_bot, "BUNDLE_CART_PATH", str(cart_path))
    _bot.bundle_cart.clear()
    return _bot


class TestCartHelpers:
    def test_get_empty(self, fresh_bot):
        assert fresh_bot._cart_get(123) == []

    def test_add_first_item(self, fresh_bot):
        ok, msg = fresh_bot._cart_add(123, 5001)
        assert ok is True
        assert "1/3" in msg
        assert fresh_bot._cart_get(123) == [5001]

    def test_add_dedup(self, fresh_bot):
        fresh_bot._cart_add(123, 5001)
        ok, msg = fresh_bot._cart_add(123, 5001)
        assert ok is False
        assert "уже" in msg.lower()

    def test_add_max_size(self, fresh_bot):
        for i in range(fresh_bot.BUNDLE_CART_MAX):
            fresh_bot._cart_add(123, 1000 + i)
        ok, msg = fresh_bot._cart_add(123, 9999)
        assert ok is False
        assert "полная" in msg.lower()

    def test_remove(self, fresh_bot):
        fresh_bot._cart_add(123, 5001)
        fresh_bot._cart_add(123, 5002)
        assert fresh_bot._cart_remove(123, 5001) is True
        assert fresh_bot._cart_get(123) == [5002]

    def test_remove_missing(self, fresh_bot):
        assert fresh_bot._cart_remove(123, 9999) is False

    def test_remove_last_drops_user(self, fresh_bot):
        # Когда корзина опустеет — user-key удаляется чтобы dict не тёк
        fresh_bot._cart_add(123, 5001)
        fresh_bot._cart_remove(123, 5001)
        assert "123" not in fresh_bot.bundle_cart

    def test_clear(self, fresh_bot):
        fresh_bot._cart_add(123, 5001)
        fresh_bot._cart_add(123, 5002)
        fresh_bot._cart_clear(123)
        assert fresh_bot._cart_get(123) == []

    def test_isolated_per_user(self, fresh_bot):
        fresh_bot._cart_add(123, 5001)
        fresh_bot._cart_add(456, 5002)
        assert fresh_bot._cart_get(123) == [5001]
        assert fresh_bot._cart_get(456) == [5002]


class TestCartPersistence:
    def test_save_writes_json(self, fresh_bot):
        fresh_bot._cart_add(123, 5001)
        fresh_bot._cart_add(123, 5002)
        with open(fresh_bot.BUNDLE_CART_PATH, encoding="utf-8") as f:
            data = json.load(f)
        assert data == {"123": [5001, 5002]}

    def test_load_restores_state(self, fresh_bot):
        # Симулируем "сохранили → рестарт → загрузили"
        with open(fresh_bot.BUNDLE_CART_PATH, "w", encoding="utf-8") as f:
            json.dump({"777": [9001, 9002, 9003]}, f)
        fresh_bot.bundle_cart.clear()
        fresh_bot._load_bundle_carts()
        assert fresh_bot._cart_get(777) == [9001, 9002, 9003]

    def test_load_missing_file_safe(self, fresh_bot, tmp_path):
        # Файла нет → load не падает, корзина пустая
        fresh_bot.BUNDLE_CART_PATH = str(tmp_path / "nonexistent.json")
        fresh_bot.bundle_cart.clear()
        fresh_bot._load_bundle_carts()
        assert fresh_bot.bundle_cart == {}


class TestCartOrder:
    """Порядок добавления критичен — первые BUNDLE_TOTAL берутся в bundle при cart_buy."""
    def test_order_preserved(self, fresh_bot):
        for bid in [5001, 5002, 5003, 5004]:
            fresh_bot._cart_add(123, bid)
        assert fresh_bot._cart_get(123) == [5001, 5002, 5003, 5004]

    def test_remove_preserves_order_of_others(self, fresh_bot):
        for bid in [5001, 5002, 5003, 5004]:
            fresh_bot._cart_add(123, bid)
        fresh_bot._cart_remove(123, 5002)
        assert fresh_bot._cart_get(123) == [5001, 5003, 5004]
