"""Tests для git autopush — модуль persistence для Render free disk.

Покрывает чистые функции: is_enabled, mark_dirty, _build_remote_url.
Сам push не тестируем — он бьёт remote (нужен PAT + integration env).
"""
from __future__ import annotations

import os

import git_autopush


class TestIsEnabled:
    def test_default_disabled(self, monkeypatch):
        monkeypatch.delenv("GIT_AUTOPUSH_ENABLED", raising=False)
        assert git_autopush.is_enabled() is False

    def test_explicit_zero_disabled(self, monkeypatch):
        monkeypatch.setenv("GIT_AUTOPUSH_ENABLED", "0")
        assert git_autopush.is_enabled() is False

    def test_explicit_one_enabled(self, monkeypatch):
        monkeypatch.setenv("GIT_AUTOPUSH_ENABLED", "1")
        assert git_autopush.is_enabled() is True

    def test_arbitrary_string_disabled(self, monkeypatch):
        # Только "1" enables — защита от случайных "true"/"yes" не работает →
        # юзер должен явно ставить "1"
        monkeypatch.setenv("GIT_AUTOPUSH_ENABLED", "true")
        assert git_autopush.is_enabled() is False


class TestMarkDirty:
    def test_marks_relative_path(self):
        # Repo root резолвится по __file__ — должен включать
        # bot-assistant/bot-assistant/<file>
        git_autopush._dirty.clear()
        git_autopush.mark_dirty("beats_data.json")
        keys = list(git_autopush._dirty.keys())
        # На Windows backslash → forward slash
        assert any("beats_data.json" in k for k in keys), f"got: {keys}"
        assert all("\\" not in k for k in keys), "paths must use forward slashes"

    def test_idempotent(self):
        git_autopush._dirty.clear()
        git_autopush.mark_dirty("admin_prefs.json")
        git_autopush.mark_dirty("admin_prefs.json")
        git_autopush.mark_dirty("admin_prefs.json")
        # Один key — не дублируется
        assert len(git_autopush._dirty) == 1

    def test_multiple_files(self):
        git_autopush._dirty.clear()
        git_autopush.mark_dirty("beats_data.json")
        git_autopush.mark_dirty("admin_prefs.json")
        assert len(git_autopush._dirty) == 2

    def test_safe_on_invalid_path(self):
        # Внешний путь не должен крашить mark_dirty (просто warn + skip)
        git_autopush._dirty.clear()
        git_autopush.mark_dirty("/totally/outside/repo.txt")
        # No exception — успех. dirty может быть пустым.


class TestBuildRemoteUrl:
    def test_token_embedded(self, monkeypatch):
        monkeypatch.delenv("GIT_AUTOPUSH_REMOTE", raising=False)
        url = git_autopush._build_remote_url("MYTOKEN123")
        assert url is not None
        assert "x-access-token:MYTOKEN123@" in url
        assert "github.com" in url

    def test_explicit_remote_env(self, monkeypatch):
        monkeypatch.setenv("GIT_AUTOPUSH_REMOTE", "github.com/owner/repo.git")
        url = git_autopush._build_remote_url("XYZ")
        assert url == "https://x-access-token:XYZ@github.com/owner/repo.git"

    def test_token_not_logged_in_url_format(self, monkeypatch):
        # Format URL не должен иметь scheme/user/pass отдельно — должен быть
        # одной строкой для git push <URL> HEAD:branch
        monkeypatch.setenv("GIT_AUTOPUSH_REMOTE", "github.com/x/y.git")
        url = git_autopush._build_remote_url("T")
        assert url.startswith("https://x-access-token:T@")
