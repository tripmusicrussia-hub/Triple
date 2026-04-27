"""Tests для Sprint 2 — Welcome sequence (multi-touch nurture).

Тестируем:
- users_db getter/setter для welcome_seq_step (Supabase mock)
- list_users_for_welcome_step с max_age_hours filter
- _WELCOME_STEP_AGE — корректные thresholds (24h/72h/7d)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

import users_db


class TestWelcomeStepCRUD:
    def test_get_returns_0_when_no_supabase(self):
        with patch.object(users_db, "_get_supabase", return_value=None):
            assert users_db.get_welcome_step(123) == 0

    def test_get_returns_step_from_supabase(self):
        client = MagicMock()
        # Mock chain: client.table().select().eq().execute()
        exec_mock = MagicMock()
        exec_mock.data = [{"welcome_seq_step": 2}]
        client.table.return_value.select.return_value.eq.return_value.execute.return_value = exec_mock
        with patch.object(users_db, "_get_supabase", return_value=client):
            assert users_db.get_welcome_step(123) == 2

    def test_get_returns_0_when_user_not_found(self):
        client = MagicMock()
        exec_mock = MagicMock()
        exec_mock.data = []
        client.table.return_value.select.return_value.eq.return_value.execute.return_value = exec_mock
        with patch.object(users_db, "_get_supabase", return_value=client):
            assert users_db.get_welcome_step(999) == 0

    def test_get_handles_null_step(self):
        client = MagicMock()
        exec_mock = MagicMock()
        exec_mock.data = [{"welcome_seq_step": None}]
        client.table.return_value.select.return_value.eq.return_value.execute.return_value = exec_mock
        with patch.object(users_db, "_get_supabase", return_value=client):
            assert users_db.get_welcome_step(123) == 0

    def test_set_calls_update_with_step(self):
        client = MagicMock()
        update_mock = MagicMock()
        client.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock()
        client.table.return_value.update = MagicMock(return_value=update_mock)
        update_mock.eq.return_value.execute.return_value = MagicMock()
        with patch.object(users_db, "_get_supabase", return_value=client):
            users_db.set_welcome_step(123, 2)
        # Verify update была вызвана с правильным payload
        call_args = client.table.return_value.update.call_args
        assert call_args is not None
        payload = call_args[0][0]
        assert payload["welcome_seq_step"] == 2
        assert "updated_at" in payload


class TestListUsersForWelcomeStep:
    def test_returns_users_filtered_by_step(self):
        client = MagicMock()
        rows = [
            {"tg_id": 1, "joined_at": "2026-04-26T10:00:00+03:00", "source": "yt"},
            {"tg_id": 2, "joined_at": "2026-04-25T10:00:00+03:00", "source": "tg"},
        ]
        exec_mock = MagicMock()
        exec_mock.data = rows
        client.table.return_value.select.return_value.eq.return_value.execute.return_value = exec_mock
        with patch.object(users_db, "_get_supabase", return_value=client):
            result = users_db.list_users_for_welcome_step(1)
        assert len(result) == 2
        assert result[0]["tg_id"] == 1

    def test_max_age_hours_filters_old(self):
        # Юзер joined_at 100 дней назад (далеко прошлое) — не должен попасть
        # при max_age_hours=24h
        old_dt = datetime.now(timezone.utc) - timedelta(days=100)
        recent_dt = datetime.now(timezone.utc) - timedelta(hours=12)
        client = MagicMock()
        rows = [
            {"tg_id": 1, "joined_at": old_dt.isoformat(), "source": "yt"},
            {"tg_id": 2, "joined_at": recent_dt.isoformat(), "source": "tg"},
        ]
        exec_mock = MagicMock()
        exec_mock.data = rows
        client.table.return_value.select.return_value.eq.return_value.execute.return_value = exec_mock
        with patch.object(users_db, "_get_supabase", return_value=client):
            # max_age 24h → пропускает только recent (joined < 24h ago)
            result = users_db.list_users_for_welcome_step(1, max_age_hours=24)
        # Recent должен пройти, old — нет
        ids = [r["tg_id"] for r in result]
        assert 2 in ids
        assert 1 not in ids

    def test_no_supabase_returns_empty(self):
        with patch.object(users_db, "_get_supabase", return_value=None):
            assert users_db.list_users_for_welcome_step(1) == []


class TestWelcomeStepThresholds:
    def test_step_age_thresholds(self):
        # Sprint 2 spec: step=1 (recs) после 24h, step=2 (digest) после 72h,
        # step=3 (discount) после 7d
        import bot
        assert bot._WELCOME_STEP_AGE[1] == 24
        assert bot._WELCOME_STEP_AGE[2] == 72
        assert bot._WELCOME_STEP_AGE[3] == 24 * 7

    def test_step_actions_mapped(self):
        # Каждый step должен иметь action handler
        import bot
        for step in (1, 2, 3):
            action = bot._WELCOME_STEP_ACTIONS.get(step)
            assert callable(action)
        # Step 4 = final, no action
        assert bot._WELCOME_STEP_ACTIONS.get(4) is None
