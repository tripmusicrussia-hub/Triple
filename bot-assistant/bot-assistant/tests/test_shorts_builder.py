"""Tests для shorts_builder — Step 1 full-screen + PIL text overlay.

Не делаем end-to-end ffmpeg run (занимает 60-180 сек на free CPU, не подходит
для unit tests). Вместо этого:
- Тестируем filter chain construction (overlay структура для разных комбинаций)
- Тестируем _truncate_name + PIL text overlay rendering
- Smoke contract: build_short raises на missing inputs
"""
from __future__ import annotations

from pathlib import Path

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
        out = shorts_builder._truncate_name("AGGRESSIVE MORE TEXT", max_len=12)
        assert out.endswith("…")
        assert "  " not in out


class TestFilterChain:
    def test_legacy_letterbox_when_meta_none(self):
        chain = shorts_builder._build_filter_chain(None)
        assert "scale=1080:-2" in chain
        assert "pad=1080:1920" in chain
        # Без overlay структуры
        assert "split" not in chain
        assert "boxblur" not in chain

    def test_no_overlays_blurred_bg_only(self):
        # meta_name есть, но без text/eq overlay
        chain = shorts_builder._build_filter_chain(
            "WAW", text_overlay=False, eq_overlay=False,
        )
        # Step 1 base: blurred bg + sharp center
        assert "split=2[bg_src][fg_src]" in chain
        assert "boxblur=20:5" in chain
        assert "force_original_aspect_ratio=increase" in chain
        # Финал [v_out]
        assert "[v_out]" in chain
        # Никаких extra overlay'ев
        assert "[2:v]" not in chain
        assert "[3:v]" not in chain

    def test_text_overlay_adds_input_2(self):
        chain = shorts_builder._build_filter_chain(
            "WAW", text_overlay=True, eq_overlay=False,
        )
        # input #2 = text PNG
        assert "[2:v]overlay=0:0" in chain
        assert "[with_text]" in chain
        # Финал yuv420p
        assert "[v_out]" in chain

    def test_eq_only_uses_input_2(self):
        # text_overlay=False — EQ занимает input #2 (shifting)
        chain = shorts_builder._build_filter_chain(
            "WAW", text_overlay=False, eq_overlay=True,
        )
        assert "[2:v]overlay" in chain
        assert "shortest=1" in chain

    def test_text_and_eq_use_input_2_and_3(self):
        chain = shorts_builder._build_filter_chain(
            "WAW", text_overlay=True, eq_overlay=True,
        )
        assert "[2:v]overlay=0:0" in chain  # text
        assert "[3:v]overlay" in chain  # EQ
        assert "shortest=1" in chain

    def test_format_yuv420p_at_end(self):
        # format=yuv420p должен быть В САМОМ КОНЦЕ цепочки (alpha сохраняется
        # до последнего overlay'я)
        chain = shorts_builder._build_filter_chain(
            "WAW", text_overlay=True, eq_overlay=True,
        )
        # Последняя operation перед [v_out]
        assert chain.rstrip().endswith("format=yuv420p[v_out]")


class TestRenderTextOverlayPng:
    """PIL text overlay — проверяем что PNG создаётся, размер правильный, alpha сохранён."""

    def test_creates_png_with_correct_size(self, tmp_path):
        out = tmp_path / "text.png"
        shorts_builder._render_text_overlay_png(
            meta_name="WAW", meta_bpm=136, meta_key_short="Cm",
            out_path=out,
        )
        assert out.exists()
        from PIL import Image
        img = Image.open(out)
        assert img.size == (1080, 1920)
        assert img.mode == "RGBA"

    def test_handles_long_name(self, tmp_path):
        out = tmp_path / "text.png"
        # Длинное имя — не падает, truncate работает
        shorts_builder._render_text_overlay_png(
            meta_name="AGGRESSIVE MEMPHIS DRILL", meta_bpm=145, meta_key_short="Am",
            out_path=out,
        )
        assert out.exists()

    def test_handles_no_bpm(self, tmp_path):
        out = tmp_path / "text.png"
        shorts_builder._render_text_overlay_png(
            meta_name="WAW", meta_bpm=None, meta_key_short=None,
            out_path=out,
        )
        assert out.exists()

    def test_handles_bpm_no_key(self, tmp_path):
        out = tmp_path / "text.png"
        shorts_builder._render_text_overlay_png(
            meta_name="WAW", meta_bpm=140, meta_key_short=None,
            out_path=out,
        )
        assert out.exists()

    def test_unicode_name_supported(self, tmp_path):
        out = tmp_path / "text.png"
        # Cyrillic / accents — PIL handles natively (vs ffmpeg drawtext escaping)
        shorts_builder._render_text_overlay_png(
            meta_name="БИТЁНОК", meta_bpm=140, meta_key_short="Am",
            out_path=out,
        )
        assert out.exists()


class TestBuildShortContract:
    """Smoke contract tests — не запускаем настоящий ffmpeg, проверяем
    raise при отсутствии input файлов."""

    def test_missing_image_raises(self, tmp_path):
        mp3 = tmp_path / "fake.mp3"
        mp3.touch()
        out = tmp_path / "out.mp4"
        with pytest.raises(FileNotFoundError, match="image"):
            shorts_builder.build_short(
                Path("/nonexistent/img.jpg"), mp3, out,
            )

    def test_missing_mp3_raises(self, tmp_path):
        img = tmp_path / "fake.jpg"
        img.touch()
        out = tmp_path / "out.mp4"
        with pytest.raises(FileNotFoundError, match="mp3"):
            shorts_builder.build_short(
                img, Path("/nonexistent/audio.mp3"), out,
            )
