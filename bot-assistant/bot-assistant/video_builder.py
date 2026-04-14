"""Сборка видео для YouTube: видео-луп + mp3 → готовый mp4.

Работает через bundled ffmpeg (imageio-ffmpeg), без system-install.
Зацикливает assets/loop.mp4 на длину аудио, накладывает mp3 как audio track.
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


def _ffmpeg() -> str:
    global _FFMPEG_CACHE
    if _FFMPEG_CACHE:
        return _FFMPEG_CACHE
    logger.info("resolving ffmpeg binary (may download on first run)...")
    _FFMPEG_CACHE = imageio_ffmpeg.get_ffmpeg_exe()
    logger.info("ffmpeg binary: %s", _FFMPEG_CACHE)
    return _FFMPEG_CACHE


def warmup():
    """Прогревает ffmpeg-бинарник — вызвать на старте бота, чтобы первый upload не ждал скачивания."""
    try:
        _ffmpeg()
    except Exception as e:
        logger.warning("ffmpeg warmup failed: %s", e)


def _probe_duration(path: Path) -> float:
    """Длительность mp3/mp4 через ffmpeg (без ffprobe — у нас только ffmpeg бинарник)."""
    cmd = [_ffmpeg(), "-i", str(path)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    # ffmpeg пишет в stderr, возвращает exit 1 когда нет -f output — это ок
    stderr = proc.stderr
    for line in stderr.splitlines():
        if "Duration:" in line:
            # "  Duration: 00:02:34.12, start: ..."
            part = line.split("Duration:", 1)[1].split(",", 1)[0].strip()
            h, m, s = part.split(":")
            return int(h) * 3600 + int(m) * 60 + float(s)
    raise RuntimeError(f"не нашёл Duration в ffmpeg output: {stderr[:500]}")


def build_video(
    mp3_path: Path,
    out_path: Path,
    loop_path: Path = LOOP_PATH,
) -> Path:
    """
    Собирает финальное видео:
    - видео-луп зацикливается на длину mp3
    - mp3 подкладывается как audio
    - codec: H.264 + AAC, 1920x1080, подходит для YT

    Возвращает путь к out_path.
    """
    if not loop_path.exists():
        raise FileNotFoundError(f"loop video not found: {loop_path}")
    if not mp3_path.exists():
        raise FileNotFoundError(f"mp3 not found: {mp3_path}")

    logger.info("probing duration of %s", mp3_path)
    duration = _probe_duration(mp3_path)
    logger.info("duration: %.2fs, building video %s", duration, out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Stream copy видео (без re-encode) — loop.mp4 уже в целевом формате 1280x720 H.264.
    # Энкодим только аудио (mp3 → aac). Билд ~5 сек вместо 2-3 мин.
    cmd = [
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
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        raise RuntimeError("ffmpeg timeout (2 min) — что-то сильно не так, stream copy должен быть секунды")
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg failed ({proc.returncode}): {proc.stderr[-1500:]}")
    logger.info("video built OK: %s (audio %.1fs)", out_path, duration)
    return out_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import sys

    if len(sys.argv) < 2:
        print("usage: python video_builder.py <mp3_path> [out.mp4]")
        sys.exit(1)
    mp3 = Path(sys.argv[1])
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else mp3.with_suffix(".mp4")
    p = build_video(mp3, out)
    print(f"OK  {p}")
