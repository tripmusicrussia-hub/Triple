"""Tests для shorts_builder — Step 1 full-screen + drawtext layout.

Не делаем end-to-end ffmpeg run (занимает 60-180 сек на free CPU, не подходит
для unit tests). Вместо этого:
- Тестируем filter chain construction (что текст правильно эскейпится,
  truncation работает, fallback на letterbox при meta=None)
- Тестируем _truncate_name + _escape_drawtext отдельно
- E2E test build_short() помечен @pytest.mark.slow и skipped по умолчанию
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

import shorts_builder


class TestTruncateName:
    def test_short_name_unchanged(self):
        assert shorts_builder._truncate_name("WAW") == "WAW"
        assert shorts_builder._truncate_name("THOUGHTS") == "THOUGHTS"

    def test_exactly_limit_unchanged(self):
        # 14 chars limit
        assert shorts_builder._truncate_name("12345678901234") == "12345678901234"

    def test_long_name_truncated_with_ellipsis(self):
        out = shorts_builder._truncate_name("AGGRESSIVE MEMPHIS DRILL", max_len=14)
        assert out.endswith("…")
        assert len(out) == 14

    def test_truncate_strips_trailing_space(self):
        # «AGGRESSIVE ME…» — "AGGRESSIVE M" + "…" — но "AGGRESSIVE " имеет
        # пробел в конце который мы strip'аем
        out = shorts_builder._truncate_name("AGGRESSIVE MORE TEXT", max_len=12)
        assert out.endswith("…")
        # Не должно быть «AGGRESSIVE …» с пробелом перед эллипсом
        assert "  " not in out


class TestEscapeDrawtext:
    def test_no_special_chars_passthrough(self):
        assert shorts_builder._escape_drawtext("WAW") == "WAW"
        assert shorts_builder._escape_drawtext("136 BPM") == "136 BPM"

    def test_colon_escaped(self):
        # «BPM: 136» → «BPM\: 136»
        out = shorts_builder._escape_drawtext("BPM: 136")
        assert out == r"BPM\: 136"

    def test_backslash_escaped(self):
        out = shorts_builder._escape_drawtext(r"path\name")
        assert out == r"path\\name"

    def test_apostrophe_replaced_with_typographic(self):
        # ' нельзя внутри single-quoted text — заменяем на ’
        out = shorts_builder._escape_drawtext("don't stop")
        assert "'" not in out
        assert "’" in out

    def test_combined_escaping(self):
        out = shorts_builder._escape_drawtext("a:b'c\\d")
        assert out == r"a\:b’c\\d"


class TestFilterChain:
    def test_legacy_letterbox_when_meta_none(self):
        chain = shorts_builder._build_filter_chain(None, None, None)
        # Legacy fallback: simple scale + pad
        assert "scale=1080:-2" in chain
        assert "pad=1080:1920" in chain
        # Без drawtext / split / boxblur
        assert "drawtext" not in chain
        assert "split" not in chain
        assert "boxblur" not in chain

    def test_full_screen_when_meta_provided(self):
        chain = shorts_builder._build_filter_chain("WAW", 136, "Cm")
        # Step 1: blurred bg + sharp center
        assert "split=2[bg_src][fg_src]" in chain
        assert "boxblur=20:5" in chain
        assert "force_original_aspect_ratio=increase" in chain
        # Step 2: drawtext overlays
        assert "drawtext" in chain
        assert "WAW" in chain
        assert "136 BPM" in chain
        assert "Cm" in chain
        # Final label для -map
        assert "[v_out]" in chain

    def test_bpm_only_when_no_key(self):
        chain = shorts_builder._build_filter_chain("WAW", 136, None)
        assert "136 BPM" in chain
        # Не должно быть «| Cm» суффикса
        assert "BPM | " not in chain

    def test_no_bpm_skip_bpm_filter(self):
        chain = shorts_builder._build_filter_chain("WAW", None, None)
        # Только name drawtext, без BPM
        assert "WAW" in chain
        assert "BPM" not in chain

    def test_long_name_truncated_in_chain(self):
        chain = shorts_builder._build_filter_chain(
            "AGGRESSIVE MEMPHIS DRILL", 145, "Am",
        )
        # Полное имя не в chain (truncated)
        assert "AGGRESSIVE MEMPHIS DRILL" not in chain
        assert "…" in chain

    def test_special_chars_escaped(self):
        # «don't» имеет ' — должен стать ’
        chain = shorts_builder._build_filter_chain("don't stop", 140, "Am")
        assert "don't" not in chain
        assert "don’t" in chain


class TestFontArg:
    def test_returns_empty_when_font_missing(self):
        with patch("shorts_builder.Path.exists", return_value=False):
            out = shorts_builder._font_arg(bold=True)
        assert out == ""

    def test_returns_fontfile_when_exists(self):
        with patch("shorts_builder.Path.exists", return_value=True):
            out = shorts_builder._font_arg(bold=True)
        assert ":fontfile=" in out
        assert "Bold" in out

    def test_regular_when_not_bold(self):
        with patch("shorts_builder.Path.exists", return_value=True):
            out = shorts_builder._font_arg(bold=False)
        assert ":fontfile=" in out
        # DejaVu-Sans.ttf (no Bold)
        assert "Bold" not in out


class TestBuildShortContract:
    """Smoke contract tests — не запускаем настоящий ffmpeg, проверяем
    raise при отсутствии input файлов."""

    def test_missing_image_raises(self, tmp_path):
        from pathlib import Path
        mp3 = tmp_path / "fake.mp3"
        mp3.touch()
        out = tmp_path / "out.mp4"
        with pytest.raises(FileNotFoundError, match="image"):
            shorts_builder.build_short(
                Path("/nonexistent/img.jpg"), mp3, out,
            )

    def test_missing_mp3_raises(self, tmp_path):
        from pathlib import Path
        img = tmp_path / "fake.jpg"
        img.touch()
        out = tmp_path / "out.mp4"
        with pytest.raises(FileNotFoundError, match="mp3"):
            shorts_builder.build_short(
                img, Path("/nonexistent/audio.mp3"), out,
            )
