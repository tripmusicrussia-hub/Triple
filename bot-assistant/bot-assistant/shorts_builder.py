"""Сборка 9:16 видео для YouTube Shorts (и универсально — для TG Story,
Reels, TikTok если понадобится).

Подход:
* Берём brand-кадр (1280×720 16:9) — тот же что используется как
  thumbnail основного видео — не плодим ассеты.
* Пересобираем в 1080×1920 (9:16) с letterbox padding: кадр сохраняется,
  сверху/снизу — чёрные полосы.
* Берём N секунд mp3 со смещения OFFSET (по умолчанию 30s — drop в
  hard trap нише). Если mp3 короче offset+duration — fallback на 0.
* ffmpeg `-tune stillimage` — быстрый энкод (~5-10 сек на 30-сек short
  даже на Render free tier).
"""
from __future__ import annotations

import logging
import subprocess
from pathlib import Path

import imageio_ffmpeg

from config import SHORTS_DURATION_SEC, SHORTS_OFFSET_SEC

logger = logging.getLogger(__name__)

_FFMPEG_CACHE: str | None = None


def _ffmpeg() -> str:
    global _FFMPEG_CACHE
    if _FFMPEG_CACHE is None:
        _FFMPEG_CACHE = imageio_ffmpeg.get_ffmpeg_exe()
    return _FFMPEG_CACHE


def _probe_duration_sec(mp3_path: Path) -> float:
    """Длительность mp3 через ffmpeg stderr. Не критично если упадёт —
    тогда просто не делаем offset-fallback.
    """
    try:
        proc = subprocess.run(
            [_ffmpeg(), "-i", str(mp3_path)],
            capture_output=True, text=True, timeout=10,
        )
        for line in proc.stderr.splitlines():
            if "Duration:" in line:
                hms = line.split("Duration:", 1)[1].split(",", 1)[0].strip()
                h, m, s = hms.split(":")
                return int(h) * 3600 + int(m) * 60 + float(s)
    except Exception:
        logger.warning("probe_duration failed for %s", mp3_path)
    return 0.0


def build_short(image_path: Path, mp3_path: Path, out_path: Path,
                duration_sec: int = SHORTS_DURATION_SEC,
                start_offset_sec: int = SHORTS_OFFSET_SEC) -> Path:
    """Собирает 9:16 1080×1920 mp4 из статичного кадра + N сек mp3
    начиная со смещения start_offset_sec.

    Filter chain:
      1. scale=1080:-2 — ресайз кадра по ширине до 1080 с сохранением
         aspect ratio (-2 = auto height, divisible by 2 для x264).
      2. pad=1080:1920 — центрируем в 9:16 холсте, чёрные полосы.

    Edge cases:
    - mp3 короче start_offset_sec → fallback offset=0 (берём с начала).
    - mp3 короче duration_sec → ffmpeg `-shortest` обрежет автоматом
      (получится Short короче duration_sec, что ок).
    """
    if not image_path.exists():
        raise FileNotFoundError(f"image not found: {image_path}")
    if not mp3_path.exists():
        raise FileNotFoundError(f"mp3 not found: {mp3_path}")

    # Adjust offset: если mp3 слишком короткий чтобы взять с offset —
    # берём с начала. Без этого ffmpeg выдаст пустой output / ошибку.
    actual_offset = start_offset_sec
    if start_offset_sec > 0:
        mp3_duration = _probe_duration_sec(mp3_path)
        if mp3_duration > 0 and mp3_duration < (start_offset_sec + 5):
            # Не остаётся даже 5 сек после offset — фолбек на 0.
            logger.info(
                "shorts: mp3 too short (%.1fs < offset %d+5) → fallback offset=0",
                mp3_duration, start_offset_sec,
            )
            actual_offset = 0

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # `-ss` ПЕРЕД `-i` — accurate seek без overhead'а на full decode.
    # Применяем только к audio (image — `-loop 1` бесконечная статика).
    # `-r 1` (1 fps вместо 30) — статичная картинка, ffmpeg всё равно
    # дублирует кадры, экономим ~30x на encoding для shared CPU Render.
    # `preset ultrafast` — 3-5x быстрее `veryfast`, чуть больше bitrate
    # но для Shorts CRF 28 даёт ~3-5MB файл, ОК для YT и TikTok.
    cmd = [
        _ffmpeg(),
        "-y",
        "-loop", "1",
        "-r", "1",
        "-i", str(image_path),
        "-ss", str(actual_offset),
        "-i", str(mp3_path),
        "-t", str(duration_sec),
        "-vf",
        "scale=1080:-2,pad=1080:1920:(ow-iw)/2:(oh-ih)/2:black,format=yuv420p",
        "-c:v", "libx264",
        "-tune", "stillimage",
        "-preset", "ultrafast",
        "-crf", "28",
        "-r", "30",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ac", "2",
        "-shortest",
        "-movflags", "+faststart",
        str(out_path),
    ]

    # Render free CPU shared — encoding 30-сек 1080×1920 может занять
    # 2-5 мин. timeout 600 = 10 мин с большим запасом.
    timeout = max(600, duration_sec * 20)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"shorts ffmpeg timeout {timeout}s") from e

    if proc.returncode != 0:
        raise RuntimeError(
            f"shorts ffmpeg failed ({proc.returncode}): {proc.stderr[-1500:]}"
        )

    logger.info(
        "short built OK: %s (%ds @ offset=%ds)",
        out_path, duration_sec, actual_offset,
    )
    return out_path
