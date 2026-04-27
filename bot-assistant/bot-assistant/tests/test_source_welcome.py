"""Tests для Sprint 6 — Source-specific welcome intro.

Тестируем:
- _welcome_recs_intro: разные source → разные intros
- ref_<friend_id> (numeric) → friend-specific intro
- Unknown source / None → default fallback
"""
from __future__ import annotations

import pytest


class TestWelcomeRecsIntro:
    def test_default_when_none(self):
        import bot
        intro = bot._welcome_recs_intro(None)
        assert "Прошёл день" in intro

    def test_default_when_empty(self):
        import bot
        intro = bot._welcome_recs_intro("")
        assert "Прошёл день" in intro

    def test_yt_source(self):
        import bot
        intro = bot._welcome_recs_intro("yt")
        assert "YouTube" in intro

    def test_tiktok_source(self):
        import bot
        intro = bot._welcome_recs_intro("tiktok")
        assert "TikTok" in intro

    def test_insta_source(self):
        import bot
        intro = bot._welcome_recs_intro("insta")
        assert "Insta" in intro

    def test_tg_source(self):
        import bot
        intro = bot._welcome_recs_intro("tg")
        assert "канал" in intro.lower()

    def test_landing_source(self):
        import bot
        intro = bot._welcome_recs_intro("landing")
        assert "сайт" in intro.lower()

    def test_ref_prefix_stripped(self):
        # «ref_yt» → как «yt»
        import bot
        intro1 = bot._welcome_recs_intro("ref_yt")
        intro2 = bot._welcome_recs_intro("yt")
        assert intro1 == intro2

    def test_friend_referral_numeric(self):
        # ref_123456 → friend ref intro
        import bot
        intro = bot._welcome_recs_intro("ref_123456")
        assert "Друг" in intro or "друг" in intro

    def test_unknown_source_fallback(self):
        import bot
        intro = bot._welcome_recs_intro("random_unknown_source")
        # Fallback на default
        assert intro == bot._WELCOME_RECS_INTRO_DEFAULT

    def test_case_insensitive(self):
        import bot
        intro_lower = bot._welcome_recs_intro("yt")
        intro_upper = bot._welcome_recs_intro("YT")
        assert intro_lower == intro_upper

    def test_ytshorts_distinct_from_yt(self):
        # ytshorts должен иметь отдельный intro (Shorts-specific tone)
        import bot
        intro_yt = bot._welcome_recs_intro("yt")
        intro_shorts = bot._welcome_recs_intro("ytshorts")
        assert intro_yt != intro_shorts
        assert "Shorts" in intro_shorts or "Свайп" in intro_shorts.lower() or "свайп" in intro_shorts.lower()
