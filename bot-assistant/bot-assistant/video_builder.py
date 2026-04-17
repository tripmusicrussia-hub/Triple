"""Сборка видео для YouTube: видео-луп + mp3 → готовый mp4.

Работает через bundled ffmpeg (imageio-ffmpeg), без system-install.
По умолчанию накладывает audio-reactive waveform-визуализатор поверх лупа
в серо-фиолетовых тонах (retention-буст для type-beat видео).
Цвета совпадают с эстетикой SKILL.md: VHS / не неон.
"""
from __future__ import annotations

import logging
import subprocess
from pathlib import Path

import imageio_ffmpeg

HERE = Path(__file__).parent
LOOP_PATH = HERE / "assets" / "loop.mp4"

logger = logging.getLogger(__name__)

_FFMPEG_CACHE: str | None = None

# Палитра визуализатора — серо-фиолетовая, попадает в SKILL.md «не неон»
WAVE_COLOR_PRIMARY = "#B388EB"
WAVE_COLOR_SECONDARY = "#8B2B9C"
WAVE_HEIGHT = 90       # px, высота визуалайзера
WAVE_MARGIN_BOT = 60   # px от нижнего края


def _ffmpeg() -> str:
    global _FFMPEG_CACHE
    if _FFMPEG_CACHE:
        return _FFMPEG_CACHE
    logger.info("resolving ffmpeg binary (may download on first run)...")
    _FFMPEG_CACHE = imageio_ffmpeg.get_ffmpeg_exe()
    logger.info("ffmpeg binary: %s", _FFMPEG_CACHE)
    return _FFMPEG_CACHE


def warmup():
    """Прогревает ffmpeg-бинарник — вызвать на старте бота."""
    try:
        _ffmpeg()
    except Exception as e:
        logger.warning("ffmpeg warmup failed: %s", e)


def _probe_duration(path: Path) -> float:
    """Длительность mp3/mp4 через ffmpeg (без ffprobe — у нас только ffmpeg бинарник)."""
    cmd = [_ffmpeg(), "-i", str(path)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    stderr = proc.stderr
    for line in stderr.splitlines():
        if "Duration:" in line:
            part = line.split("Duration:", 1)[1].split(",", 1)[0].strip()
            h, m, s = part.split(":")
            return int(h) * 3600 + int(m) * 60 + float(s)
    raise RuntimeError(f"не нашёл Duration в ffmpeg output: {stderr[:500]}")


def build_video(
    mp3_path: Path,
    out_path: Path,
    loop_path: Path = LOOP_PATH,
    *,
    visualizer: bool = True,
) -> Path:
    """Собирает финальное mp4 для YT.

    - видео-луп зацикливается на длину mp3
    - mp3 → aac audio track
    - при visualizer=True — накладывает audio-reactive waveform
      поверх видео-лупа в серо-фиолетовых тонах (retention-буст)
    - при visualizer=False — stream copy (быстро, ~5 сек на 3-мин трек)

    Возвращает путь к out_path.
    """
    if not loop_path.exists():
        raise FileNotFoundError(f"loop video not found: {loop_path}")
    if not mp3_path.exists():
        raise FileNotFoundError(f"mp3 not found: {mp3_path}")

    logger.info("probing duration of %s", mp3_path)
    duration = _probe_duration(mp3_path)
    logger.info("duration: %.2fs, visualizer=%s → %s", duration, visualizer, out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Таймауты: Render free tier ~3x медленнее локальной машины.
    # Для visualizer (re-encode) закладываем 4x длительности трека, минимум 300с.
    # Для stream-copy хватает 120с.
    def _run(cmd, tout):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=tout)
        except subprocess.TimeoutExpired:
            return None

    if visualizer:
        cmd = _build_visualizer_cmd(mp3_path, out_path, loop_path, duration)
        timeout = max(300, int(duration * 4))
        proc = _run(cmd, timeout)
        if proc is None:
            logger.warning(
                "ffmpeg visualizer timeout (%ds), fallback to stream-copy (no waveform)",
                timeout,
            )
            cmd = _build_streamcopy_cmd(mp3_path, out_path, loop_path, duration)
            proc = _run(cmd, 120)
            if proc is None:
                raise RuntimeError(f"ffmpeg stream-copy fallback timeout on {mp3_path}")
    else:
        cmd = _build_streamcopy_cmd(mp3_path, out_path, loop_path, duration)
        proc = _run(cmd, 120)
        if proc is None:
            raise RuntimeError(f"ffmpeg stream-copy timeout on {mp3_path}")

    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg failed ({proc.returncode}): {proc.stderr[-1500:]}")
    logger.info("video built OK: %s (audio %.1fs)", out_path, duration)
    return out_path


def _build_streamcopy_cmd(mp3_path: Path, out_path: Path, loop_path: Path, duration: float) -> list[str]:
    """Быстрая сборка без перекодирования видео (~5 сек). Без визуалайзера."""
    return [
        _ffmpeg(), "-y",
        "-stream_loop", "-1",
        "-i", str(loop_path),
        "-i", str(mp3_path),
        "-map", "0:v:0",
        "-map", "1:a:0",
        "-c:v", "copy",
        "-c:a", "aac",
        "-b:a", "192k",
        "-shortest",
        "-t", f"{duration:.2f}",
        "-movflags", "+faststart",
        str(out_path),
    ]


def _build_visualizer_cmd(mp3_path: Path, out_path: Path, loop_path: Path, duration: float) -> list[str]:
    """Сборка с audio-reactive waveform поверх лупа.

    Filter chain:
      [1:a]showwaves — генерит визуалайзер из аудио (line-режим, мягкий градиент)
      [0:v][wave]overlay — накладывает его на лупnа по центру-снизу
    """
    filter_complex = (
        f"[1:a]showwaves="
        f"s=1280x{WAVE_HEIGHT}:"
        f"mode=line:"
        f"colors={WAVE_COLOR_PRIMARY}|{WAVE_COLOR_SECONDARY}:"
        f"rate=30:"
        f"n=30,"
        f"format=yuva420p,"
        f"colorchannelmixer=aa=0.85[wave];"
        f"[0:v][wave]overlay=(W-w)/2:H-h-{WAVE_MARGIN_BOT}:shortest=1[v]"
    )
    return [
        _ffmpeg(), "-y",
        "-stream_loop", "-1",
        "-i", str(loop_path),
        "-i", str(mp3_path),
        "-filter_complex", filter_complex,
        "-map", "[v]",
        "-map", "1:a:0",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-crf", "23",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "192k",
        "-shortest",
        "-t", f"{duration:.2f}",
        "-movflags", "+faststart",
        str(out_path),
    ]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import sys

    if len(sys.argv) < 2:
        print("usage: python video_builder.py <mp3_path> [out.mp4] [--no-viz]")
        sys.exit(1)
    mp3 = Path(sys.argv[1])
    out = Path(sys.argv[2]) if len(sys.argv) > 2 and not sys.argv[2].startswith("--") else mp3.with_suffix(".mp4")
    viz = "--no-viz" not in sys.argv
    p = build_video(mp3, out, visualizer=viz)
    print(f"OK  {p}")
