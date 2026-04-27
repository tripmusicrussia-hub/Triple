"""Tests для Sprint 4 — Auto-bundle suggestion после 3 viewed beats.

Тестируем:
- _track_session_view: dedupe + TTL cleanup + max 5 entries
- _get_session_unique_views: возвращает unique beat_ids в last 30 min
- _should_suggest_session_bundle: BUNDLE_TOTAL threshold + cooldown + admin skip
- _mark_session_bundle_suggested: stamp ts для cooldown
"""
from __future__ import annotations

import time

import pytest


class TestTrackSessionView:
    def setup_method(self):
        import bot
        self.bot = bot
        bot._session_views.clear()
        bot._session_bundle_suggested_at.clear()

    def teardown_method(self):
        self.bot._session_views.clear()
        self.bot._session_bundle_suggested_at.clear()

    def test_first_view_appends(self):
        self.bot._track_session_view(123, 1)
        entries = self.bot._session_views[123]
        assert len(entries) == 1
        assert entries[0][0] == 1

    def test_dedupe_same_beat_id(self):
        self.bot._track_session_view(123, 1)
        self.bot._track_session_view(123, 2)
        self.bot._track_session_view(123, 1)  # dup
        # Должно быть 2 entries: [2, 1] (1 был moved в конец)
        entries = self.bot._session_views[123]
        assert len(entries) == 2
        ids = [e[0] for e in entries]
        assert ids == [2, 1]  # 1 в конце т.к. перезаписан

    def test_max_entries_caps_at_5(self):
        for bid in range(1, 8):
            self.bot._track_session_view(123, bid)
        entries = self.bot._session_views[123]
        assert len(entries) == 5  # MAX_ENTRIES
        # Последние 5 (3, 4, 5, 6, 7)
        ids = [e[0] for e in entries]
        assert ids == [3, 4, 5, 6, 7]

    def test_expired_entries_dropped(self):
        # Inject old entry с timestamp 31 min назад
        old_ts = time.time() - 31 * 60
        self.bot._session_views[123] = [(99, old_ts)]
        # New view trigger cleanup
        self.bot._track_session_view(123, 1)
        entries = self.bot._session_views[123]
        # Old (99) должен быть очищен, остался только 1
        assert len(entries) == 1
        assert entries[0][0] == 1


class TestGetSessionUniqueViews:
    def setup_method(self):
        import bot
        self.bot = bot
        bot._session_views.clear()

    def teardown_method(self):
        self.bot._session_views.clear()

    def test_returns_unique_in_order(self):
        self.bot._track_session_view(123, 1)
        self.bot._track_session_view(123, 2)
        self.bot._track_session_view(123, 3)
        unique = self.bot._get_session_unique_views(123)
        assert unique == [1, 2, 3]

    def test_filters_expired(self):
        old_ts = time.time() - 31 * 60
        new_ts = time.time()
        self.bot._session_views[123] = [(99, old_ts), (1, new_ts)]
        unique = self.bot._get_session_unique_views(123)
        assert unique == [1]

    def test_empty_for_unknown_user(self):
        assert self.bot._get_session_unique_views(999) == []


class TestShouldSuggestSessionBundle:
    def setup_method(self):
        import bot
        self.bot = bot
        bot._session_views.clear()
        bot._session_bundle_suggested_at.clear()

    def teardown_method(self):
        self.bot._session_views.clear()
        self.bot._session_bundle_suggested_at.clear()

    def test_false_with_only_2_views(self):
        self.bot._track_session_view(123, 1)
        self.bot._track_session_view(123, 2)
        assert self.bot._should_suggest_session_bundle(123) is False

    def test_true_with_3_unique_views(self):
        self.bot._track_session_view(123, 1)
        self.bot._track_session_view(123, 2)
        self.bot._track_session_view(123, 3)
        assert self.bot._should_suggest_session_bundle(123) is True

    def test_admin_excluded(self):
        # Patch ADMIN_ID to a known value
        from unittest.mock import patch
        with patch.object(self.bot, "ADMIN_ID", 123):
            self.bot._track_session_view(123, 1)
            self.bot._track_session_view(123, 2)
            self.bot._track_session_view(123, 3)
            assert self.bot._should_suggest_session_bundle(123) is False

    def test_cooldown_blocks_repeat(self):
        # Fire suggestion → mark → next call returns False
        self.bot._track_session_view(123, 1)
        self.bot._track_session_view(123, 2)
        self.bot._track_session_view(123, 3)
        assert self.bot._should_suggest_session_bundle(123) is True
        self.bot._mark_session_bundle_suggested(123)
        # Cooldown 30 min — second call должен быть False
        assert self.bot._should_suggest_session_bundle(123) is False

    def test_cooldown_expires(self):
        self.bot._track_session_view(123, 1)
        self.bot._track_session_view(123, 2)
        self.bot._track_session_view(123, 3)
        # Mark cooldown как expired (31 min ago)
        self.bot._session_bundle_suggested_at[123] = time.time() - 31 * 60
        assert self.bot._should_suggest_session_bundle(123) is True
