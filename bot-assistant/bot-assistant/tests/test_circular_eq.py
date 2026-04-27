"""Tests для circular_eq_renderer — Step 2.

E2E тест с реальным ffmpeg + matplotlib занимает 60-90s, не подходит для
unit. Тестируем computational helpers + smoke contract.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest

import circular_eq_renderer as eq


def _has_matplotlib() -> bool:
    try:
        import matplotlib  # noqa: F401
        return True
    except ImportError:
        return False


# matplotlib опциональный (на Render будет установлен через requirements.txt,
# локально может отсутствовать). Skip rendering-тесты если absent.
needs_matplotlib = pytest.mark.skipif(
    not _has_matplotlib(),
    reason="matplotlib not installed (required for polar rendering)",
)


class TestComputeFreqBands:
    def test_silence_returns_zeros(self):
        # 30 frames @ 30fps × sample_rate/fps samples per frame
        audio = np.zeros(30 * (eq._SAMPLE_RATE // 30), dtype=np.float32)
        bands = eq._compute_freq_bands(audio, fps=30, n_bands=32)
        assert bands.shape == (30, 32)
        assert np.all(bands == 0)

    def test_white_noise_has_all_bands_nonzero(self):
        # White noise → энергия во всех freq bins
        rng = np.random.default_rng(42)
        audio = rng.standard_normal(eq._SAMPLE_RATE).astype(np.float32) * 0.5
        bands = eq._compute_freq_bands(audio, fps=30, n_bands=32)
        assert bands.shape[1] == 32
        # Most bands должны быть > 0
        assert (bands.mean(axis=0) > 0).sum() >= 30

    def test_normalized_to_0_1(self):
        rng = np.random.default_rng(123)
        audio = rng.standard_normal(eq._SAMPLE_RATE * 2).astype(np.float32)
        bands = eq._compute_freq_bands(audio, fps=30, n_bands=32)
        assert bands.min() >= 0.0
        assert bands.max() <= 1.0

    def test_empty_audio_returns_zero_frames(self):
        audio = np.array([], dtype=np.float32)
        bands = eq._compute_freq_bands(audio, fps=30, n_bands=32)
        assert bands.shape == (0, 32)

    def test_short_audio_partial_frames(self):
        # 10 frames worth of audio — должно вернуть exactly 10 frames
        n_samples = 10 * (eq._SAMPLE_RATE // 30)
        audio = np.zeros(n_samples, dtype=np.float32)
        bands = eq._compute_freq_bands(audio, fps=30, n_bands=32)
        assert bands.shape[0] == 10


@needs_matplotlib
class TestRenderPolarFrames:
    """matplotlib backend = Agg, реально создаём PNG (быстро для small bands)."""

    def test_renders_count_matches_input(self, tmp_path):
        # 5 frames синтетических bands → 5 PNG
        bands = np.random.RandomState(0).rand(5, 32).astype(np.float32)
        n = eq._render_polar_frames(bands, tmp_path, size=200)
        assert n == 5
        pngs = sorted(tmp_path.glob("frame_*.png"))
        assert len(pngs) == 5

    def test_zero_frames_returns_zero(self, tmp_path):
        bands = np.zeros((0, 32), dtype=np.float32)
        n = eq._render_polar_frames(bands, tmp_path, size=200)
        assert n == 0

    def test_png_files_named_zero_padded(self, tmp_path):
        bands = np.zeros((3, 32), dtype=np.float32)
        eq._render_polar_frames(bands, tmp_path, size=100)
        names = sorted(p.name for p in tmp_path.glob("frame_*.png"))
        # frame_00000.png, frame_00001.png, frame_00002.png
        assert names == ["frame_00000.png", "frame_00001.png", "frame_00002.png"]


class TestRenderCircularEqOverlayContract:
    def test_missing_mp3_raises(self, tmp_path):
        out = tmp_path / "eq.webm"
        with pytest.raises(FileNotFoundError, match="mp3"):
            eq.render_circular_eq_overlay(
                Path("/nonexistent/audio.mp3"), out,
                duration_sec=30, offset_sec=0,
            )

    def test_short_audio_raises(self, tmp_path):
        # Mock _extract_pcm чтобы вернуть слишком мало samples
        mp3 = tmp_path / "fake.mp3"
        mp3.touch()
        out = tmp_path / "eq.webm"
        with patch.object(
            eq, "_extract_pcm",
            return_value=np.zeros(100, dtype=np.float32),
        ):
            with pytest.raises(RuntimeError, match="samples"):
                eq.render_circular_eq_overlay(
                    mp3, out, duration_sec=30, offset_sec=0,
                )
