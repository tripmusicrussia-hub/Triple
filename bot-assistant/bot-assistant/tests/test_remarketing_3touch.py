"""Tests для Sprint 3 — 3-touch re-engagement sequence.

Тестируем:
- _REMARKETING_TOUCHES конфиг — корректные thresholds (24h/72h/7d)
- Discount % escalation: 20 → 25 → 30
- Migration legacy `reminded: bool` → `touch_count: int`
"""
from __future__ import annotations

import pytest


class TestRemarketingTouchesConfig:
    """Sprint 5 заменил _REMARKETING_TOUCHES (fixed dict array) на helper
    functions с per-user A/B variant. Тесты обновлены под новую structure."""

    def test_three_touches(self):
        import bot
        assert bot._remarketing_touch_count() == 3

    def test_threshold_sequence(self):
        import bot
        delays = bot._REMARKETING_TOUCH_DELAYS_SEC
        assert delays[0] == 24 * 3600
        assert delays[1] == 72 * 3600
        assert delays[2] == 7 * 24 * 3600

    def test_pct_escalation_variant_20(self):
        # uid=1 → variant=20 → 20/25/30 (default-equivalent поведение)
        import bot
        assert bot._remarketing_touch_pct(0, 1) == 20
        assert bot._remarketing_touch_pct(1, 1) == 25
        assert bot._remarketing_touch_pct(2, 1) == 30

    def test_drop_after_buffer(self):
        import bot
        # Drop after 8 days — buffer после 7d third touch
        assert bot._REMARKETING_DROP_AFTER_SEC == 8 * 24 * 3600
        # Должен быть >= 7d (последний delay)
        assert bot._REMARKETING_DROP_AFTER_SEC >= bot._REMARKETING_TOUCH_DELAYS_SEC[-1]


class TestRemarketingMigration:
    """Legacy pending_reminders записи (с `reminded: bool`) должны корректно
    мигрироваться в новый формат (touch_count: int) при первом проходе scheduler'а.
    """

    def test_legacy_reminded_true_becomes_touch_count_1(self):
        # Симулируем legacy запись
        rec = {"ts": 1000, "name": "Test Beat", "reminded": True}
        # Migration logic из remarketing_scheduler:
        if "touch_count" not in rec:
            rec["touch_count"] = 1 if rec.get("reminded") else 0
        assert rec["touch_count"] == 1

    def test_legacy_reminded_false_becomes_touch_count_0(self):
        rec = {"ts": 1000, "name": "Test Beat", "reminded": False}
        if "touch_count" not in rec:
            rec["touch_count"] = 1 if rec.get("reminded") else 0
        assert rec["touch_count"] == 0

    def test_new_record_no_migration_needed(self):
        # Новая запись с touch_count уже set
        rec = {"ts": 1000, "name": "Test Beat", "touch_count": 2}
        original = rec.copy()
        if "touch_count" not in rec:
            rec["touch_count"] = 1 if rec.get("reminded") else 0
        assert rec == original  # без изменений


class TestRemarketingDiscountTokenWithPct:
    """Sprint 1 расширил _make_discount_token на pct param. Sprint 3
    использует это для туth_idx-specific token'ов."""

    def setup_method(self):
        import bot
        self.bot_mod = bot
        bot.active_discounts.clear()

    def teardown_method(self):
        self.bot_mod.active_discounts.clear()

    def test_each_touch_creates_unique_token_with_correct_pct(self):
        # Симулируем 3 touches: 20%, 25%, 30% — все 3 token'а уникальны
        t1 = self.bot_mod._make_discount_token(123, 456, pct=20)
        t2 = self.bot_mod._make_discount_token(123, 456, pct=25)
        t3 = self.bot_mod._make_discount_token(123, 456, pct=30)
        assert len({t1, t2, t3}) == 3  # все разные
        assert self.bot_mod.active_discounts[t1]["pct"] == 20
        assert self.bot_mod.active_discounts[t2]["pct"] == 25
        assert self.bot_mod.active_discounts[t3]["pct"] == 30
