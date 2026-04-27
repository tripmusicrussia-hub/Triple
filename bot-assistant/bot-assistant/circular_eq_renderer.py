"""Circular EQ visualizer для YouTube Shorts (FL Studio style).

Генерирует transparent webm с круговым equalizer'ом — 32 freq bands в
polar bar chart, brand purple gradient `#B388EB → #8B2B9C`. Накладывается
поверх blurred bg + sharp center в shorts_builder.

Подход (librosa-free для экономии 200MB на Render free):
1. `_extract_pcm` — ffmpeg → 16-bit mono PCM bytes → numpy float array
2. `_compute_freq_bands` — sliding window FFT → magnitude matrix [frames × bands]
3. `_render_polar_frames` — matplotlib per-frame, reuse Figure (memory)
4. `_assemble_webm` — ffmpeg PNG sequence → vp9 yuva420p (alpha)

Cost (Render free CPU shared):
- PCM extract: ~5s
- FFT всех frames: ~3-5s (numpy vectorized)
- matplotlib 900 frames: ~60-90s (bottleneck)
- ffmpeg assemble: ~10s
- Total: ~80-110s per Short

Memory: stream PNG saving (одна frame в RAM за раз), del PCM buffer после
FFT computation. Peak ~150MB на 30-сек 44100Hz mono audio + matplotlib.
"""
from __future__ import annotations

import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

import imageio_ffmpeg
import numpy as np

logger = logging.getLogger(__name__)

# Sample rate для PCM extraction. 22050 достаточно для EQ visualization
# (Nyquist 11025Hz покрывает все freq диапазоны полезные для bars).
# Меньше data = быстрее FFT + меньше RAM.
_SAMPLE_RATE = 22050

# Brand colors из past waveform attempt (commit 77b5ba3)
_COLOR_INNER = "#B388EB"  # бледно-фиолетовый
_COLOR_OUTER = "#8B2B9C"  # темно-фиолетовый


def _ffmpeg() -> str:
    return imageio_ffmpeg.get_ffmpeg_exe()


def _extract_pcm(mp3_path: Path, offset_sec: int, duration_sec: int,
                 sample_rate: int = _SAMPLE_RATE) -> "np.ndarray":
    """ffmpeg extract: mp3 → 16-bit signed mono PCM → numpy float array [-1.0, 1.0].

    Slice [offset, offset+duration] из mp3. Если mp3 короче — ffmpeg вернёт
    меньше samples (что normal, обработаем в caller'е).
    """
    cmd = [
        _ffmpeg(), "-y",
        "-ss", str(offset_sec),
        "-i", str(mp3_path),
        "-t", str(duration_sec),
        "-f", "s16le",  # raw signed 16-bit little-endian
        "-acodec", "pcm_s16le",
        "-ac", "1",  # mono
        "-ar", str(sample_rate),
        "-",  # stdout
    ]
    proc = subprocess.run(cmd, capture_output=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(f"PCM extract failed: {proc.stderr[-500:].decode()}")
    raw = np.frombuffer(proc.stdout, dtype=np.int16)
    # Normalize int16 [-32768, 32767] → float32 [-1.0, 1.0]
    return raw.astype(np.float32) / 32768.0


def _compute_freq_bands(audio: "np.ndarray", fps: int, n_bands: int = 32,
                        sample_rate: int = _SAMPLE_RATE) -> "np.ndarray":
    """Sliding FFT → magnitude matrix [n_frames × n_bands].

    Per-frame window = sample_rate // fps samples (для 30fps @ 22050Hz = 735).
    Each window FFT → np.abs → group в n_bands log-spaced bins (musical perception).
    Normalized к [0, 1] относительно глобального max.
    """
    window_size = sample_rate // fps
    n_frames = len(audio) // window_size
    if n_frames == 0:
        return np.zeros((0, n_bands), dtype=np.float32)

    # Hann window для smooth FFT (минимизирует spectral leakage)
    hann = np.hanning(window_size).astype(np.float32)

    # Log-spaced bin edges (musical: 20Hz - sample_rate/2 ≈ 11025Hz)
    # FFT size = window_size, freqs = np.fft.rfftfreq(window_size, 1/sample_rate)
    fft_freqs = np.fft.rfftfreq(window_size, 1.0 / sample_rate)
    # 20Hz lower bound (sub-bass), Nyquist upper
    edges = np.logspace(np.log10(max(20.0, fft_freqs[1])),
                        np.log10(fft_freqs[-1]), n_bands + 1)

    # Pre-compute bin indices: для каждого band — какие FFT bins агрегировать
    bin_groups = []
    for i in range(n_bands):
        lo, hi = edges[i], edges[i + 1]
        idx = np.where((fft_freqs >= lo) & (fft_freqs < hi))[0]
        if len(idx) == 0:
            # Edge case: band слишком узкий → ближайший bin
            idx = np.array([np.argmin(np.abs(fft_freqs - lo))])
        bin_groups.append(idx)

    # Process all frames vectorized если влезает в RAM, иначе chunk
    out = np.zeros((n_frames, n_bands), dtype=np.float32)
    for f in range(n_frames):
        chunk = audio[f * window_size : (f + 1) * window_size] * hann
        spectrum = np.abs(np.fft.rfft(chunk))
        for b, idx in enumerate(bin_groups):
            out[f, b] = spectrum[idx].mean()

    # Normalize per-frame: divide by global max → [0, 1]
    # Используем 95th percentile вместо max (resistant to outliers/spikes)
    norm = np.percentile(out, 95)
    if norm > 0:
        out = np.clip(out / norm, 0.0, 1.0)
    return out


def _render_polar_frames(bands: "np.ndarray", out_dir: Path, size: int = 800,
                         color_inner: str = _COLOR_INNER,
                         color_outer: str = _COLOR_OUTER) -> int:
    """matplotlib polar bar chart per frame → PNG sequence в out_dir.

    Reuse Figure object — clear+redraw bars per frame для memory.
    Возвращает count написанных frames.

    Бары:
    - 32 равномерно spaced 0..2π
    - height = magnitude × max_radius
    - color gradient inner→outer per bar position
    - black bg, transparent canvas (alpha)
    """
    # Lazy import — matplotlib heavy, не нужен при unit tests которые мокают
    import matplotlib
    matplotlib.use("Agg")  # non-interactive backend, faster
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap

    n_frames, n_bands = bands.shape
    if n_frames == 0:
        return 0

    # Bar angles (32 bars, one per band)
    theta = np.linspace(0.0, 2.0 * np.pi, n_bands, endpoint=False)
    width = 2.0 * np.pi / n_bands * 0.8  # leave 20% gap между bars

    # Color gradient: inner (#B388EB) → outer (#8B2B9C) per bar height
    cmap = LinearSegmentedColormap.from_list("brand", [color_inner, color_outer])
    colors = cmap(np.linspace(0.0, 1.0, n_bands))

    # Setup figure once, reuse через clear()
    dpi = 100
    fig = plt.figure(figsize=(size / dpi, size / dpi), dpi=dpi, facecolor="none")
    ax = fig.add_subplot(111, projection="polar")
    ax.set_facecolor("none")
    ax.set_yticklabels([])
    ax.set_xticklabels([])
    ax.spines["polar"].set_visible(False)
    ax.grid(False)
    ax.set_ylim(0, 1.0)

    out_dir.mkdir(parents=True, exist_ok=True)

    for f in range(n_frames):
        ax.cla()
        ax.set_facecolor("none")
        ax.set_yticklabels([])
        ax.set_xticklabels([])
        ax.spines["polar"].set_visible(False)
        ax.grid(False)
        ax.set_ylim(0, 1.0)
        # Bars per frame
        ax.bar(theta, bands[f], width=width, bottom=0.15,
               color=colors, edgecolor="none", linewidth=0)
        # Save PNG with transparent canvas
        out = out_dir / f"frame_{f:05d}.png"
        fig.savefig(out, dpi=dpi, transparent=True, bbox_inches="tight",
                    pad_inches=0)

    plt.close(fig)
    return n_frames


def _assemble_webm(frames_dir: Path, out_webm: Path, fps: int = 30) -> Path:
    """ffmpeg PNG sequence → webm vp9 yuva420p (alpha-channel preserved)."""
    cmd = [
        _ffmpeg(), "-y",
        "-framerate", str(fps),
        "-i", str(frames_dir / "frame_%05d.png"),
        "-c:v", "libvpx-vp9",
        "-pix_fmt", "yuva420p",
        "-b:v", "1M",
        "-deadline", "realtime",  # быстрее vs default `good`
        "-cpu-used", "8",  # max speed
        str(out_webm),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    if proc.returncode != 0:
        raise RuntimeError(f"webm assemble failed: {proc.stderr[-500:]}")
    return out_webm


def render_circular_eq_overlay(
    mp3_path: Path, out_webm: Path,
    duration_sec: int, offset_sec: int,
    fps: int = 30, size: int = 800,
    color_inner: str = _COLOR_INNER,
    color_outer: str = _COLOR_OUTER,
) -> Path:
    """End-to-end: mp3 + slice → circular EQ webm с alpha.

    Pipeline: PCM extract → FFT freq bands → matplotlib polar PNG sequence
    → ffmpeg webm vp9 yuva420p.

    Output webm имеет alpha-channel — overlay'ится прозрачно поверх blurred
    bg в shorts_builder final ffmpeg pass.
    """
    if not mp3_path.exists():
        raise FileNotFoundError(f"mp3 not found: {mp3_path}")

    out_webm.parent.mkdir(parents=True, exist_ok=True)

    audio = _extract_pcm(mp3_path, offset_sec, duration_sec)
    expected_samples = duration_sec * _SAMPLE_RATE
    if len(audio) < expected_samples * 0.5:
        raise RuntimeError(
            f"PCM extract returned {len(audio)} samples, expected ~{expected_samples}"
        )
    logger.info("eq: PCM extracted, %d samples (%.1fs)",
                len(audio), len(audio) / _SAMPLE_RATE)

    bands = _compute_freq_bands(audio, fps=fps, n_bands=32)
    del audio  # free PCM buffer
    logger.info("eq: FFT done, %d frames × 32 bands", bands.shape[0])

    # tempdir для PNG sequence — auto-cleanup
    tmp_dir = Path(tempfile.mkdtemp(prefix="eq_frames_"))
    try:
        n = _render_polar_frames(bands, tmp_dir, size=size,
                                 color_inner=color_inner,
                                 color_outer=color_outer)
        logger.info("eq: rendered %d polar frames → %s", n, tmp_dir)
        if n == 0:
            raise RuntimeError("no frames rendered")
        _assemble_webm(tmp_dir, out_webm, fps=fps)
        logger.info("eq: webm assembled OK: %s", out_webm)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return out_webm
