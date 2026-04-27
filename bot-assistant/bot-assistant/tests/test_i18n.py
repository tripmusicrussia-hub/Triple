"""Tests для i18n MVP — detect / translate / in-memory cache."""
from __future__ import annotations

import pytest

import i18n


class TestDetectLang:
    def test_ru(self):
        assert i18n.detect_lang("ru") == "ru"
        assert i18n.detect_lang("ru-RU") == "ru"
        assert i18n.detect_lang("RU") == "ru"

    def test_uk_be_kk_as_ru(self):
        # CIS-аудитория — лучше русский чем английский
        assert i18n.detect_lang("uk") == "ru"
        assert i18n.detect_lang("be-BY") == "ru"
        assert i18n.detect_lang("kk") == "ru"

    def test_en(self):
        assert i18n.detect_lang("en") == "en"
        assert i18n.detect_lang("en-US") == "en"

    def test_other_languages_fallback_en(self):
        for code in ("de", "fr", "es", "it", "ja", "zh", "pt-BR"):
            assert i18n.detect_lang(code) == "en", f"failed for {code}"

    def test_none_or_empty_defaults_ru(self):
        # Default = 'ru' (бот для RU-аудитории, no-signal не должен дать EN
        # русскому юзеру по ошибке)
        assert i18n.detect_lang(None) == "ru"
        assert i18n.detect_lang("") == "ru"
        assert i18n.detect_lang("   ") == "ru"


class TestTranslate:
    def test_ru_returns_russian(self):
        assert "Привет" in i18n.t("welcome_first", "ru")

    def test_en_returns_english(self):
        assert "Hey!" in i18n.t("welcome_first", "en")

    def test_default_lang_ru(self):
        # default lang = ru если не указан
        assert i18n.t("welcome_first") == i18n.t("welcome_first", "ru")

    def test_format_kwargs(self):
        out = i18n.t("ref_welcome_title", "en", pct=10)
        assert "10%" in out
        assert "{pct}" not in out

    def test_format_kwargs_ru(self):
        out = i18n.t("ref_welcome_title", "ru", pct=15)
        assert "15%" in out

    def test_missing_kwarg_returns_template_safely(self):
        # Если caller забыл kwarg — НЕ крашим, возвращаем template как есть
        out = i18n.t("ref_welcome_title", "ru")
        assert "{pct}" in out  # placeholder остался — но не exception

    def test_unknown_key_returns_debug_marker(self):
        out = i18n.t("nonexistent_key_xyz", "en")
        assert "?" in out
        assert "nonexistent" in out

    def test_en_missing_falls_back_to_ru(self):
        # Симуляция: ключ есть только в RU словаре
        i18n._LANG_RU["test_only_ru"] = "только русский"
        out = i18n.t("test_only_ru", "en")
        assert out == "только русский"
        del i18n._LANG_RU["test_only_ru"]

    def test_buy_carding_translated(self):
        # critical conversion string
        ru = i18n.t("buy_carding_intro", "ru")
        en = i18n.t("buy_carding_intro", "en")
        assert "Выбери" in ru
        assert "Pick" in en
        assert ru != en

    def test_ref_friend_notify_format(self):
        out = i18n.t("ref_friend_notify", "en", name="John", pct=10)
        assert "John" in out
        assert "10%" in out


class TestUserLangCache:
    def setup_method(self):
        i18n._user_lang_cache.clear()

    def test_set_then_get_uses_cache(self):
        i18n._user_lang_cache[123] = "en"
        # get_user_lang без кода → берёт из cache
        assert i18n.get_user_lang(123) == "en"

    def test_no_cache_uses_detect_lang(self):
        i18n._user_lang_cache.clear()
        # Без Supabase (тесты не подключены) → fallback на detect
        assert i18n.get_user_lang(456, language_code="en-US") == "en"
        assert i18n.get_user_lang(457, language_code="ru") == "ru"

    def test_set_user_lang_invalid_returns_false(self):
        assert i18n.set_user_lang(123, "fr") is False
        assert i18n.set_user_lang(123, "") is False

    def test_set_user_lang_valid_caches(self):
        i18n._user_lang_cache.clear()
        # Supabase скорее всего unreachable → False, но cache update'ится
        i18n.set_user_lang(789, "en")
        assert i18n._user_lang_cache.get(789) == "en"
