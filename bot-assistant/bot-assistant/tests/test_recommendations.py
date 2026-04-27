"""Tests для Sprint 1 — Smart recommendations + post-purchase upsell.

Тестируем:
- get_similar_beats scoring (already существует в beats_db, sanity-check)
- _make_discount_token с custom pct (idempotency, rec.pct correctly stored)
- mp3_price_with_discount XTR/USDT/RUB returns правильное значение
"""
from __future__ import annotations

import pytest

import beats_db
import licensing


class TestGetSimilarBeats:
    """Sanity check для existing beats_db.get_similar_beats — Sprint 1 reuses."""

    def setup_method(self):
        # Mock BEATS_CACHE для изолированных тестов
        self._original_cache = beats_db.BEATS_CACHE.copy()
        beats_db.BEATS_CACHE.clear()
        beats_db.BEATS_CACHE.extend([
            {"id": 1, "name": "Beat A", "bpm": 140, "key_short": "Am",
             "tags": ["kennymuney"], "content_type": "beat"},
            {"id": 2, "name": "Beat B", "bpm": 142, "key_short": "Am",
             "tags": ["kennymuney", "memphis"], "content_type": "beat"},
            {"id": 3, "name": "Beat C", "bpm": 145, "key_short": "Cm",
             "tags": ["future"], "content_type": "beat"},
            {"id": 4, "name": "Beat D", "bpm": 140, "key_short": "Am",
             "tags": [], "content_type": "beat"},
            {"id": 5, "name": "Beat E", "bpm": 200, "key_short": "Em",
             "tags": [], "content_type": "beat"},
        ])

    def teardown_method(self):
        beats_db.BEATS_CACHE.clear()
        beats_db.BEATS_CACHE.extend(self._original_cache)

    def test_artist_match_priority(self):
        # Beat 1 → Beat 2 (same kennymuney tag) должен быть первым
        current = beats_db.BEATS_CACHE[0]  # id=1
        result = beats_db.get_similar_beats(current)
        assert len(result) > 0
        assert result[0]["id"] == 2  # same artist tag wins

    def test_excludes_self(self):
        current = beats_db.BEATS_CACHE[0]
        result = beats_db.get_similar_beats(current)
        assert all(b["id"] != current["id"] for b in result)

    def test_excludes_history(self):
        current = beats_db.BEATS_CACHE[0]
        result = beats_db.get_similar_beats(current, exclude_ids=[2])
        assert all(b["id"] != 2 for b in result)

    def test_bpm_fallback_when_no_tag_match(self):
        # Beat 4 (no tags, BPM 140) → должен найти Beat 1, 2 (BPM 140-142, ±15)
        # Но Beat 4 не имеет tags, fallback на BPM
        current = beats_db.BEATS_CACHE[3]  # id=4, no tags
        result = beats_db.get_similar_beats(current)
        # Должен вернуть BPM-близкие
        assert len(result) > 0
        # Beat 5 (BPM 200) слишком далеко от 140 — не должен попасть
        ids = [b["id"] for b in result]
        assert 5 not in ids


class TestMakeDiscountTokenWithCustomPct:
    """Sprint 1 добавил pct param в _make_discount_token. Проверяем что rec
    сохраняет правильный pct и idempotency работает per (user, beat, pct).
    """

    def setup_method(self):
        # Import bot module и очищаем active_discounts
        import bot
        self.bot_mod = bot
        self.bot_mod.active_discounts.clear()

    def teardown_method(self):
        self.bot_mod.active_discounts.clear()

    def test_default_pct_uses_licensing_default(self):
        token = self.bot_mod._make_discount_token(123, 456)
        rec = self.bot_mod.active_discounts[token]
        assert rec["pct"] == licensing.DISCOUNT_PCT

    def test_custom_pct_30(self):
        token = self.bot_mod._make_discount_token(123, 456, pct=30)
        rec = self.bot_mod.active_discounts[token]
        assert rec["pct"] == 30

    def test_idempotent_same_pct(self):
        # Same user+beat+pct → same token returned
        t1 = self.bot_mod._make_discount_token(123, 456, pct=30)
        t2 = self.bot_mod._make_discount_token(123, 456, pct=30)
        assert t1 == t2

    def test_different_pct_creates_different_tokens(self):
        # Same user+beat но разные pct → разные tokens (для A/B testing later)
        t1 = self.bot_mod._make_discount_token(123, 456, pct=20)
        t2 = self.bot_mod._make_discount_token(123, 456, pct=30)
        assert t1 != t2
        assert self.bot_mod.active_discounts[t1]["pct"] == 20
        assert self.bot_mod.active_discounts[t2]["pct"] == 30


class TestMp3PriceWithDiscount:
    def test_xtr_30pct(self):
        # 1500 * 0.7 = 1050
        assert licensing.mp3_price_with_discount(30, "XTR") == 1050

    def test_usdt_30pct(self):
        # 20.0 * 0.7 = 14.0
        assert licensing.mp3_price_with_discount(30, "USDT") == 14.0

    def test_rub_30pct(self):
        # 1700 * 0.7 = 1190
        assert licensing.mp3_price_with_discount(30, "RUB") == 1190

    def test_invalid_pct_raises(self):
        with pytest.raises(ValueError):
            licensing.mp3_price_with_discount(0, "XTR")
        with pytest.raises(ValueError):
            licensing.mp3_price_with_discount(100, "XTR")

    def test_invalid_currency_raises(self):
        with pytest.raises(ValueError):
            licensing.mp3_price_with_discount(20, "BTC")
