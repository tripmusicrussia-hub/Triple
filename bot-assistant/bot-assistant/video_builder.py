"""Сборка видео для YouTube: статичный кадр + mp3 → mp4.

**Winning pattern type-beat каналов** (анализ топ-5 видео Memphis/Kenny Muney/
Key Glock на 2026-04-18): 4 из 5 используют статичный кадр на весь трек,
без waveform, без нарезок. ffmpeg `-tune stillimage` специально для этого —
encode 3-мин трек ~5-15 сек даже на Render free tier.

**2026-04-28 update**: добавлен text overlay (TYPE TAG + BPM/KEY + CTA)
для brand-консистентности с Shorts. Если `meta=None` → legacy без text
(backwards compat).

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


def build_video(image_path: Path, mp3_path: Path, out_path: Path,
                meta=None) -> Path:
    """Собирает mp4 из статичного изображения + mp3-аудио.

    `-tune stillimage` — x264 преset для статичных видео: encode в ~10x быстрее
    обычного, финальный mp4 ~5 MB (vs 50 MB с waveform+clip-loop).

    Если `meta` передан → накладываем text overlay (TYPE TAG + BPM/KEY + CTA),
    тот же дизайн что у Shorts. Иначе — pure brand image без overlay.

    Паттерн скопирован с winning type-beat каналов ниши — большинство winners
    использует один статичный кадр на весь трек, без waveform и нарезок.
    """
    if not image_path.exists():
        raise FileNotFoundError(f"image not found: {image_path}")
    if not mp3_path.exists():
        raise FileNotFoundError(f"mp3 not found: {mp3_path}")

    duration = probe_duration(mp3_path)
    logger.info("duration: %.2fs, building stillimage video → %s", duration, out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Pre-render text overlay PNG (1280×720) если meta предоставлен
    text_png_path: Path | None = None
    if meta is not None:
        try:
            import shorts_builder
            text_png_path = out_path.with_name(f"text_{out_path.stem}.png")
            artist_top = (getattr(meta, "artist_display", "") or "").upper().strip()
            if not artist_top:
                artist_top = "HARD TRAP"
            shorts_builder._render_text_overlay_png(
                meta_name=meta.name,
                meta_bpm=meta.bpm,
                meta_key_short=getattr(meta, "key_short", None),
                out_path=text_png_path,
                width=1280, height=720,
                top_text=artist_top,
            )
            logger.info("video: text overlay PNG generated → %s", text_png_path)
        except Exception as e:
            logger.warning("video: text overlay PNG failed (%s) — fallback без text", e)
            text_png_path = None

    # Filter chain: scale image to 1280×720 + (опционально) overlay text PNG
    if text_png_path is not None and text_png_path.exists():
        # filter_complex с 2 video inputs ([0]=image, [2]=text PNG)
        # ([1] это audio mp3 — не video)
        filter_chain = (
            "[0:v]scale=1280:720:force_original_aspect_ratio=decrease,"
            "pad=1280:720:(ow-iw)/2:(oh-ih)/2:color=black[base];"
            "[base][2:v]overlay=0:0,format=yuv420p[v_out]"
        )
        filter_arg = ["-filter_complex", filter_chain,
                      "-map", "[v_out]", "-map", "1:a"]
    else:
        # Legacy path: simple -vf без overlay
        filter_arg = [
            "-vf",
            "scale=1280:720:force_original_aspect_ratio=decrease,"
            "pad=1280:720:(ow-iw)/2:(oh-ih)/2:color=black",
        ]

    cmd = [
        _ffmpeg(), "-y",
        "-loop", "1",
        "-i", str(image_path),
        "-i", str(mp3_path),
    ]
    if text_png_path is not None and text_png_path.exists():
        cmd += ["-loop", "1", "-i", str(text_png_path)]
    cmd += [
        "-c:v", "libx264",
        "-tune", "stillimage",
        "-preset", "veryfast",
        "-crf", "24",
        "-pix_fmt", "yuv420p",
        *filter_arg,
        "-r", "2",  # 2 FPS — достаточно для статика, ещё быстрее encode
        "-threads", "2",
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

    # Cleanup intermediate text PNG (~10-30KB)
    if text_png_path is not None and text_png_path.exists():
        try:
            text_png_path.unlink()
        except Exception:
            pass

    logger.info("video built OK: %s (audio %.1fs, text=%s)",
                out_path, duration, text_png_path is not None)
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
