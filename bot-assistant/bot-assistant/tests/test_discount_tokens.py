"""Tests для auto-discount token system.

Покрывает:
- licensing.mp3_price_with_discount() — расчёт скидочных цен по валютам
- bot._make_discount_token / _validate / _consume / _invalidate / _cleanup helpers
"""
from __future__ import annotations

import json
import os
import time

import pytest

import licensing


class TestMp3PriceWithDiscount:
    def test_xtr_20pct(self):
        # 1500 * 0.8 = 1200
        assert licensing.mp3_price_with_discount(20, "XTR") == 1200

    def test_rub_20pct(self):
        # 1700 * 0.8 = 1360
        assert licensing.mp3_price_with_discount(20, "RUB") == 1360

    def test_usdt_20pct(self):
        # 20.0 * 0.8 = 16.0
        assert licensing.mp3_price_with_discount(20, "USDT") == 16.0

    def test_xtr_10pct(self):
        # 1500 * 0.9 = 1350
        assert licensing.mp3_price_with_discount(10, "XTR") == 1350

    def test_rub_30pct(self):
        # 1700 * 0.7 = 1190
        assert licensing.mp3_price_with_discount(30, "RUB") == 1190

    def test_xtr_returns_int(self):
        result = licensing.mp3_price_with_discount(20, "XTR")
        assert isinstance(result, int)

    def test_rub_returns_int(self):
        result = licensing.mp3_price_with_discount(20, "RUB")
        assert isinstance(result, int)

    def test_usdt_returns_float(self):
        result = licensing.mp3_price_with_discount(20, "USDT")
        assert isinstance(result, float)

    def test_invalid_pct_zero_raises(self):
        with pytest.raises(ValueError):
            licensing.mp3_price_with_discount(0, "XTR")

    def test_invalid_pct_100_raises(self):
        with pytest.raises(ValueError):
            licensing.mp3_price_with_discount(100, "XTR")

    def test_invalid_currency_raises(self):
        with pytest.raises(ValueError):
            licensing.mp3_price_with_discount(20, "EUR")

    def test_default_discount_pct_is_20(self):
        # Подтверждаем default — это влияет на бизнес (юзер ожидает -20%)
        assert licensing.DISCOUNT_PCT == 20


@pytest.fixture
def fresh_bot(monkeypatch, tmp_path):
    """Импортирует bot с чистым state + временным persist path."""
    monkeypatch.setenv("BOT_TOKEN", "TEST")
    monkeypatch.setenv("ADMIN_ID", "1")
    monkeypatch.setenv("CHANNEL_ID", "@test")
    import bot as _bot
    monkeypatch.setattr(_bot, "ACTIVE_DISCOUNTS_PATH", str(tmp_path / "active_discounts.json"))
    _bot.active_discounts.clear()
    return _bot


class TestMakeDiscountToken:
    def test_creates_token(self, fresh_bot):
        tok = fresh_bot._make_discount_token(123, 5001)
        assert tok.startswith("d123_5001_")
        assert len(tok) > len("d123_5001_")

    def test_token_stored_in_active_discounts(self, fresh_bot):
        tok = fresh_bot._make_discount_token(123, 5001)
        assert tok in fresh_bot.active_discounts
        rec = fresh_bot.active_discounts[tok]
        assert rec["user_id"] == 123
        assert rec["beat_id"] == 5001
        assert rec["pct"] == licensing.DISCOUNT_PCT
        assert rec["used"] is False
        assert rec["expires_at"] > time.time()

    def test_idempotent_returns_existing_active_token(self, fresh_bot):
        tok1 = fresh_bot._make_discount_token(123, 5001)
        tok2 = fresh_bot._make_discount_token(123, 5001)
        assert tok1 == tok2  # тот же token, не новый
        assert len(fresh_bot.active_discounts) == 1

    def test_creates_new_after_consume(self, fresh_bot):
        tok1 = fresh_bot._make_discount_token(123, 5001)
        fresh_bot._consume_discount_token(tok1)
        tok2 = fresh_bot._make_discount_token(123, 5001)
        # После use'а старый помечен used → новый создаётся
        assert tok1 != tok2

    def test_different_users_different_tokens(self, fresh_bot):
        tok_a = fresh_bot._make_discount_token(111, 5001)
        tok_b = fresh_bot._make_discount_token(222, 5001)
        assert tok_a != tok_b

    def test_different_beats_different_tokens(self, fresh_bot):
        tok_a = fresh_bot._make_discount_token(123, 5001)
        tok_b = fresh_bot._make_discount_token(123, 5002)
        assert tok_a != tok_b


class TestValidateDiscountToken:
    def test_valid_token(self, fresh_bot):
        tok = fresh_bot._make_discount_token(123, 5001)
        rec = fresh_bot._validate_discount_token(tok, 123)
        assert rec is not None
        assert rec["beat_id"] == 5001

    def test_unknown_token_returns_none(self, fresh_bot):
        assert fresh_bot._validate_discount_token("dXXXXXX", 123) is None

    def test_wrong_user_returns_none(self, fresh_bot):
        tok = fresh_bot._make_discount_token(123, 5001)
        assert fresh_bot._validate_discount_token(tok, 999) is None

    def test_used_token_returns_none(self, fresh_bot):
        tok = fresh_bot._make_discount_token(123, 5001)
        fresh_bot._consume_discount_token(tok)
        assert fresh_bot._validate_discount_token(tok, 123) is None

    def test_expired_token_returns_none(self, fresh_bot):
        tok = fresh_bot._make_discount_token(123, 5001)
        # Принудительно сделаем expired
        fresh_bot.active_discounts[tok]["expires_at"] = time.time() - 1
        assert fresh_bot._validate_discount_token(tok, 123) is None


class TestConsumeAndInvalidate:
    def test_consume_marks_used(self, fresh_bot):
        tok = fresh_bot._make_discount_token(123, 5001)
        fresh_bot._consume_discount_token(tok)
        assert fresh_bot.active_discounts[tok]["used"] is True

    def test_consume_unknown_safe(self, fresh_bot):
        # Не должен крашить
        fresh_bot._consume_discount_token("dXXXXXX")

    def test_invalidate_for_user_beat_marks_all(self, fresh_bot):
        # 2 token'а на разных битов того же юзера + 1 чужой
        tok_a = fresh_bot._make_discount_token(123, 5001)
        tok_b = fresh_bot._make_discount_token(123, 5002)
        tok_c = fresh_bot._make_discount_token(999, 5001)  # другой юзер
        fresh_bot._invalidate_discount_for_user_beat(123, 5001)
        assert fresh_bot.active_discounts[tok_a]["used"] is True
        assert fresh_bot.active_discounts[tok_b]["used"] is False  # другой бит
        assert fresh_bot.active_discounts[tok_c]["used"] is False  # другой юзер


class TestCleanupExpired:
    def test_drops_expired_unused(self, fresh_bot):
        tok = fresh_bot._make_discount_token(123, 5001)
        fresh_bot.active_discounts[tok]["expires_at"] = time.time() - 1
        n = fresh_bot._cleanup_expired_discounts()
        assert n == 1
        assert tok not in fresh_bot.active_discounts

    def test_keeps_active(self, fresh_bot):
        tok = fresh_bot._make_discount_token(123, 5001)
        n = fresh_bot._cleanup_expired_discounts()
        assert n == 0
        assert tok in fresh_bot.active_discounts

    def test_keeps_recent_used(self, fresh_bot):
        # Used <7 дней назад → keep (для аналитики)
        tok = fresh_bot._make_discount_token(123, 5001)
        fresh_bot._consume_discount_token(tok)
        # expires_at был +24h, цель cleanup = expires_at < now - 7d
        n = fresh_bot._cleanup_expired_discounts()
        assert n == 0  # used recently
        assert tok in fresh_bot.active_discounts

    def test_drops_used_older_than_7days(self, fresh_bot):
        tok = fresh_bot._make_discount_token(123, 5001)
        fresh_bot._consume_discount_token(tok)
        # Имитируем что expires_at был 8 дней назад (used ещё раньше)
        fresh_bot.active_discounts[tok]["expires_at"] = time.time() - 8 * 24 * 3600
        n = fresh_bot._cleanup_expired_discounts()
        assert n == 1
        assert tok not in fresh_bot.active_discounts


class TestPersistence:
    def test_save_writes_json(self, fresh_bot):
        tok = fresh_bot._make_discount_token(123, 5001)
        with open(fresh_bot.ACTIVE_DISCOUNTS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        assert tok in data
        assert data[tok]["user_id"] == 123

    def test_load_restores(self, fresh_bot):
        with open(fresh_bot.ACTIVE_DISCOUNTS_PATH, "w", encoding="utf-8") as f:
            json.dump({
                "dABC_5001_xxxx": {
                    "user_id": 999,
                    "beat_id": 5001,
                    "pct": 20,
                    "expires_at": time.time() + 3600,
                    "used": False,
                },
            }, f)
        fresh_bot.active_discounts.clear()
        fresh_bot._load_discounts()
        assert "dABC_5001_xxxx" in fresh_bot.active_discounts
        rec = fresh_bot._validate_discount_token("dABC_5001_xxxx", 999)
        assert rec is not None
