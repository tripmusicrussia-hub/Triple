"""Сборка 9:16 видео для YouTube Shorts (и универсально — для TG Story,
Reels, TikTok если понадобится).

Подход:
* Берём тот же brand-кадр (1280×720 16:9), что уже используется как
  thumbnail для основного видео — не плодим ассеты.
* Пересобираем в 1080×1920 (9:16) с letterbox padding: кадр сохраняется,
  сверху/снизу — чёрные полосы. Простейший вариант без crop'а исходника
  и без риска потерять брендинг.
* Берём первые N секунд mp3 (по умолчанию 45 — лимит YT Shorts 60,
  оставляем буфер).
* ffmpeg `-tune stillimage` — быстрый энкод (~5-10 сек на 45-секундный
  short даже на Render free tier).
"""
from __future__ import annotations

import logging
import subprocess
from pathlib import Path

import imageio_ffmpeg

logger = logging.getLogger(__name__)

SHORTS_MAX_DURATION_SEC = 45  # YT Shorts лимит 60, держим запас

_FFMPEG_CACHE: str | None = None


def _ffmpeg() -> str:
    global _FFMPEG_CACHE
    if _FFMPEG_CACHE is None:
        _FFMPEG_CACHE = imageio_ffmpeg.get_ffmpeg_exe()
    return _FFMPEG_CACHE


def build_short(image_path: Path, mp3_path: Path, out_path: Path,
                duration_sec: int = SHORTS_MAX_DURATION_SEC) -> Path:
    """Собирает 9:16 1080×1920 mp4 из статичного кадра + первых N сек mp3.

    Filter chain:
      1. scale=1080:-2 — ресайз кадра по ширине до 1080 с сохранением
         aspect ratio (-2 = auto height, divisible by 2 для x264).
      2. pad=1080:1920 — центрируем в 9:16 холсте, чёрные полосы
         сверху/снизу.
    """
    if not image_path.exists():
        raise FileNotFoundError(f"image not found: {image_path}")
    if not mp3_path.exists():
        raise FileNotFoundError(f"mp3 not found: {mp3_path}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        _ffmpeg(),
        "-y",
        "-loop", "1",
        "-i", str(image_path),
        "-i", str(mp3_path),
        "-t", str(duration_sec),
        "-vf",
        "scale=1080:-2,pad=1080:1920:(ow-iw)/2:(oh-ih)/2:black,format=yuv420p",
        "-c:v", "libx264",
        "-tune", "stillimage",
        "-preset", "veryfast",
        "-crf", "23",
        "-r", "30",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ac", "2",
        "-shortest",
        "-movflags", "+faststart",
        str(out_path),
    ]

    timeout = max(60, duration_sec * 4)  # с запасом для медленного Render
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"shorts ffmpeg timeout {timeout}s") from e

    if proc.returncode != 0:
        raise RuntimeError(
            f"shorts ffmpeg failed ({proc.returncode}): {proc.stderr[-1500:]}"
        )

    logger.info("short built OK: %s (%ds)", out_path, duration_sec)
    return out_path
