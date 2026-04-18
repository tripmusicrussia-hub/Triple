"""Сборка видео для YouTube: статичный кадр + mp3 → mp4.

**Winning pattern type-beat каналов** (анализ топ-5 видео Memphis/Kenny Muney/
Key Glock на 2026-04-18): 4 из 5 используют статичный кадр на весь трек,
без waveform, без нарезок. ffmpeg `-tune stillimage` специально для этого —
encode 3-мин трек ~5-15 сек даже на Render free tier.

Thumbnail ИЗ clip-loop'а артиста (через thumbnail_generator.generate_thumbnail_from_clip)
одновременно работает как YT-thumbnail и как фон видео. Brand-consistency +
вся картина = 1 изображение (5 MB mp4 на 3-мин трек).
"""
from __future__ import annotations

import logging
import subprocess
from pathlib import Path

import imageio_ffmpeg

HERE = Path(__file__).parent

logger = logging.getLogger(__name__)

_FFMPEG_CACHE: str | None = None


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


def probe_duration(path: Path) -> float:
    """Длительность mp3/аудио в секундах через ffmpeg stderr-parse.

    Публичный API — используется и в build_video, и в upload flow для
    YT-description timestamps.
    """
    cmd = [_ffmpeg(), "-i", str(path)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    stderr = proc.stderr
    for line in stderr.splitlines():
        if "Duration:" in line:
            part = line.split("Duration:", 1)[1].split(",", 1)[0].strip()
            h, m, s = part.split(":")
            return int(h) * 3600 + int(m) * 60 + float(s)
    raise RuntimeError(f"не нашёл Duration в ffmpeg output: {stderr[:500]}")


def build_video(image_path: Path, mp3_path: Path, out_path: Path) -> Path:
    """Собирает mp4 из статичного изображения + mp3-аудио.

    `-tune stillimage` — x264 преset для статичных видео: encode в ~10x быстрее
    обычного, финальный mp4 ~5 MB (vs 50 MB с waveform+clip-loop).

    Паттерн скопирован с winning type-beat каналов (RichBlessed 1.3M, Versa 203k,
    bxxgiemane 185k, beha2py 114k — все используют один статичный кадр на видео).
    """
    if not image_path.exists():
        raise FileNotFoundError(f"image not found: {image_path}")
    if not mp3_path.exists():
        raise FileNotFoundError(f"mp3 not found: {mp3_path}")

    duration = probe_duration(mp3_path)
    logger.info("duration: %.2fs, building stillimage video → %s", duration, out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        _ffmpeg(), "-y",
        "-loop", "1",
        "-i", str(image_path),
        "-i", str(mp3_path),
        "-c:v", "libx264",
        "-tune", "stillimage",
        "-preset", "veryfast",
        "-crf", "24",
        "-pix_fmt", "yuv420p",
        "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,"
               "pad=1280:720:(ow-iw)/2:(oh-ih)/2:color=black",
        "-r", "2",  # 2 FPS — достаточно для статика, ещё быстрее encode
        "-c:a", "aac",
        "-b:a", "192k",
        "-shortest",
        "-t", f"{duration:.2f}",
        "-movflags", "+faststart",
        str(out_path),
    ]

    # stillimage encode быстрый: 5-15 сек на 3-мин трек даже на Render.
    # 3x длительности — с запасом.
    timeout = max(120, int(duration * 3))
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"ffmpeg stillimage timeout ({timeout}s) on {mp3_path}")

    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg failed ({proc.returncode}): {proc.stderr[-1500:]}")
    logger.info("video built OK: %s (audio %.1fs)", out_path, duration)
    return out_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import sys

    if len(sys.argv) < 3:
        print("usage: python video_builder.py <image.jpg> <mp3> [out.mp4]")
        sys.exit(1)
    img = Path(sys.argv[1])
    mp3 = Path(sys.argv[2])
    out = Path(sys.argv[3]) if len(sys.argv) > 3 else mp3.with_suffix(".mp4")
    p = build_video(img, mp3, out)
    print(f"OK  {p}")
