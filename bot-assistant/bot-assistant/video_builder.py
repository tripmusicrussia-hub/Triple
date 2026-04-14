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


def _ffmpeg() -> str:
    return imageio_ffmpeg.get_ffmpeg_exe()


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

    duration = _probe_duration(mp3_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        _ffmpeg(), "-y",
        "-stream_loop", "-1",        # лупим видео бесконечно
        "-i", str(loop_path),
        "-i", str(mp3_path),
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "23",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "192k",
        "-shortest",                  # режем по длине audio
        "-t", f"{duration:.2f}",
        "-vf", "scale=1920:1080:force_original_aspect_ratio=increase,crop=1920:1080",
        "-movflags", "+faststart",
        str(out_path),
    ]
    logger.info("ffmpeg cmd: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg failed ({proc.returncode}): {proc.stderr[-1500:]}")
    logger.info("video built: %s (%.1fs)", out_path, duration)
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
