"""Tests для Sprint 5 — A/B discount % testing.

Тестируем:
- licensing.get_user_discount_pct: deterministic, returns 15/20/25
- _remarketing_touch_pct: variant base + offset (0/+5/+10)
- _remarketing_touch_count: 3 touches
"""
from __future__ import annotations

import pytest

import licensing


class TestDiscountVariants:
    def test_variants_are_15_20_25(self):
        assert licensing.DISCOUNT_VARIANTS == [15, 20, 25]

    def test_distribution_returns_one_of_three(self):
        # Любой user_id → один из 3 variants
        for uid in [123, 456, 789, 1, 999999, 1234567890]:
            pct = licensing.get_user_discount_pct(uid)
            assert pct in (15, 20, 25)

    def test_deterministic_same_user_same_variant(self):
        # Один user → один variant стабильно
        uid = 12345
        v1 = licensing.get_user_discount_pct(uid)
        v2 = licensing.get_user_discount_pct(uid)
        v3 = licensing.get_user_discount_pct(uid)
        assert v1 == v2 == v3

    def test_different_users_can_get_different_variants(self):
        # 100 random user_ids → должны быть распределены по 3 variants
        variants_seen = set()
        for uid in range(100, 200):
            v = licensing.get_user_discount_pct(uid)
            variants_seen.add(v)
        # Хотя бы 2 разных variant'а должны попасться (statistically)
        assert len(variants_seen) >= 2

    def test_user_id_none_returns_default(self):
        # None → fallback на DISCOUNT_PCT (env default)
        assert licensing.get_user_discount_pct(None) == licensing.DISCOUNT_PCT

    def test_modulo_assignment(self):
        # uid 0 → variants[0] = 15
        # uid 1 → variants[1] = 20
        # uid 2 → variants[2] = 25
        # uid 3 → variants[0] = 15 (mod 3)
        assert licensing.get_user_discount_pct(0) == 15
        assert licensing.get_user_discount_pct(1) == 20
        assert licensing.get_user_discount_pct(2) == 25
        assert licensing.get_user_discount_pct(3) == 15
        assert licensing.get_user_discount_pct(6) == 15
        assert licensing.get_user_discount_pct(7) == 20


class TestRemarketingTouchPct:
    """3-touch sequence — touch_idx 0/1/2 с base + 0/+5/+10."""

    def test_touch_count_is_3(self):
        import bot
        assert bot._remarketing_touch_count() == 3

    def test_touch_pct_offsets(self):
        # uid=0 → variant=15
        # touch 0 → 15, touch 1 → 20, touch 2 → 25
        import bot
        assert bot._remarketing_touch_pct(0, 0) == 15
        assert bot._remarketing_touch_pct(1, 0) == 20
        assert bot._remarketing_touch_pct(2, 0) == 25

    def test_touch_pct_for_variant_25(self):
        # uid=2 → variant=25 → touches 25, 30, 35
        import bot
        assert bot._remarketing_touch_pct(0, 2) == 25
        assert bot._remarketing_touch_pct(1, 2) == 30
        assert bot._remarketing_touch_pct(2, 2) == 35

    def test_touch_delays_24_72_168(self):
        import bot
        delays = bot._REMARKETING_TOUCH_DELAYS_SEC
        assert delays[0] == 24 * 3600
        assert delays[1] == 72 * 3600
        assert delays[2] == 7 * 24 * 3600
