"""Tests для referral system (Step 3).

Покрывает:
- licensing.REFERRAL_PCT default
- bot._make_referral_discount_token (idempotent на user+friend, universal beat_id=None)
- bot._get_active_universal_discount
- bot._validate_discount_token с universal token (beat_id=None)
- Self-refer не выдаёт token (логика в cmd_start, тестируется индиректно)
"""
from __future__ import annotations

import json
import time

import pytest

import licensing


@pytest.fixture
def fresh_bot(monkeypatch, tmp_path):
    monkeypatch.setenv("BOT_TOKEN", "TEST")
    monkeypatch.setenv("ADMIN_ID", "1")
    monkeypatch.setenv("CHANNEL_ID", "@test")
    import bot as _bot
    monkeypatch.setattr(_bot, "ACTIVE_DISCOUNTS_PATH", str(tmp_path / "active_discounts.json"))
    _bot.active_discounts.clear()
    return _bot


class TestReferralPct:
    def test_default_is_10(self):
        # Бизнес-решение: -10% для referral (мягче чем -20% remarketing)
        assert licensing.REFERRAL_PCT == 10


class TestMakeReferralToken:
    def test_creates_universal_token(self, fresh_bot):
        tok = fresh_bot._make_referral_discount_token(123, 456, 10)
        rec = fresh_bot.active_discounts[tok]
        assert rec["user_id"] == 123
        assert rec["beat_id"] is None  # universal
        assert rec["pct"] == 10
        assert rec["used"] is False
        assert rec["source"] == "ref_456"

    def test_token_starts_with_r_prefix(self, fresh_bot):
        # Префикс `r` (referral) — отличает от `d` (remarketing) при дебаге
        tok = fresh_bot._make_referral_discount_token(123, 456, 10)
        assert tok.startswith("r123_456_")

    def test_idempotent_same_pair(self, fresh_bot):
        tok1 = fresh_bot._make_referral_discount_token(123, 456, 10)
        tok2 = fresh_bot._make_referral_discount_token(123, 456, 10)
        assert tok1 == tok2
        assert len(fresh_bot.active_discounts) == 1

    def test_different_friend_different_token(self, fresh_bot):
        tok_a = fresh_bot._make_referral_discount_token(123, 456, 10)
        tok_b = fresh_bot._make_referral_discount_token(123, 789, 10)
        assert tok_a != tok_b
        assert len(fresh_bot.active_discounts) == 2

    def test_ttl_is_30_days(self, fresh_bot):
        before = time.time()
        tok = fresh_bot._make_referral_discount_token(123, 456, 10)
        rec = fresh_bot.active_discounts[tok]
        # 30 дней ± 5 секунд
        expected_ttl = 30 * 24 * 3600
        actual_ttl = rec["expires_at"] - before
        assert abs(actual_ttl - expected_ttl) < 5


class TestUniversalDiscountLookup:
    def test_returns_active_universal(self, fresh_bot):
        tok = fresh_bot._make_referral_discount_token(123, 456, 10)
        found = fresh_bot._get_active_universal_discount(123)
        assert found == tok

    def test_returns_none_for_user_without_universal(self, fresh_bot):
        assert fresh_bot._get_active_universal_discount(999) is None

    def test_skips_used_universal(self, fresh_bot):
        tok = fresh_bot._make_referral_discount_token(123, 456, 10)
        fresh_bot._consume_discount_token(tok)
        assert fresh_bot._get_active_universal_discount(123) is None

    def test_skips_expired_universal(self, fresh_bot):
        tok = fresh_bot._make_referral_discount_token(123, 456, 10)
        fresh_bot.active_discounts[tok]["expires_at"] = time.time() - 1
        assert fresh_bot._get_active_universal_discount(123) is None

    def test_skips_bound_token(self, fresh_bot):
        # Bound token (beat_id != None) не считается universal
        bound_tok = fresh_bot._make_discount_token(123, 5001)
        assert fresh_bot._get_active_universal_discount(123) is None


class TestValidateUniversalToken:
    def test_validates_universal_for_correct_user(self, fresh_bot):
        tok = fresh_bot._make_referral_discount_token(123, 456, 10)
        rec = fresh_bot._validate_discount_token(tok, 123)
        assert rec is not None
        assert rec["beat_id"] is None

    def test_universal_token_rejected_for_wrong_user(self, fresh_bot):
        tok = fresh_bot._make_referral_discount_token(123, 456, 10)
        # Юзер 999 не может использовать чужой token
        assert fresh_bot._validate_discount_token(tok, 999) is None


class TestReferralPair:
    """Проверка что выдача 2 tokens (новому юзеру + другу) работает корректно."""

    def test_two_tokens_for_pair(self, fresh_bot):
        # Симулируем: новый юзер 123 пришёл по ссылке от друга 456
        new_user_tok = fresh_bot._make_referral_discount_token(123, 456, 10)
        friend_tok = fresh_bot._make_referral_discount_token(456, 123, 10)
        assert new_user_tok != friend_tok
        assert len(fresh_bot.active_discounts) == 2
        # Каждый — universal на своего юзера
        assert fresh_bot._get_active_universal_discount(123) == new_user_tok
        assert fresh_bot._get_active_universal_discount(456) == friend_tok


class TestPersistenceUniversal:
    def test_universal_token_persists(self, fresh_bot):
        tok = fresh_bot._make_referral_discount_token(123, 456, 10)
        with open(fresh_bot.ACTIVE_DISCOUNTS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        assert tok in data
        assert data[tok]["beat_id"] is None
        assert data[tok]["source"] == "ref_456"

    def test_universal_token_survives_load(self, fresh_bot):
        with open(fresh_bot.ACTIVE_DISCOUNTS_PATH, "w", encoding="utf-8") as f:
            json.dump({
                "rXYZ_5_aaa": {
                    "user_id": 123,
                    "beat_id": None,
                    "pct": 10,
                    "expires_at": time.time() + 3600,
                    "used": False,
                    "source": "ref_999",
                },
            }, f)
        fresh_bot.active_discounts.clear()
        fresh_bot._load_discounts()
        # После load — universal token доступен через lookup
        assert fresh_bot._get_active_universal_discount(123) == "rXYZ_5_aaa"


class TestAntiSelfRefer:
    """cmd_start блокирует self-refer (referral_friend_id == user_id).
    Прямую проверку cmd_start тут не делаем (нужен Telegram mock); проверяем
    что хотя бы хелпер не предотвращает legitimate cases.
    """
    def test_helper_does_not_check_self_refer(self, fresh_bot):
        # _make_referral_discount_token не должна сама блокировать self-refer
        # (это работа cmd_start). Если бы блокировала — мы бы не могли
        # тестировать pair (где user == friend_other).
        tok = fresh_bot._make_referral_discount_token(123, 123, 10)
        # Token создан, но в реальности cmd_start этот call не делает
        assert tok is not None
