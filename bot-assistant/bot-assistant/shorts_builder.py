"""Сборка 9:16 видео для YouTube Shorts (и универсально — для TG Story,
Reels, TikTok если понадобится).

Дизайн (2026-04-28 update):
* **Blurred bg + sharp center** — full-screen, без чёрных полос. Тот же
  brand-кадр копируется: одна копия scale-up до 1080×1920 + boxblur =
  размытый bg, вторая копия (оригинал 16:9 → 1080×608) overlay'ится
  по центру = sharp focal point.
* **Text overlays через PIL** — beat name top-center (84px bold) + BPM/KEY
  bottom-right (44px). Рисуется в transparent PNG, накладывается через
  ffmpeg overlay filter. PIL подход вместо ffmpeg drawtext потому что
  imageio-ffmpeg static binary НЕ имеет drawtext filter (compile-time
  disabled — known issue).
* `-tune stillimage` — быстрый энкод (~60s на 30-сек short с overlay'ями
  на Render free tier).

Backwards-compat: если `meta=None` → старый letterbox approach (legacy callers).
"""
from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

import imageio_ffmpeg

from config import SHORTS_DURATION_SEC, SHORTS_OFFSET_SEC

logger = logging.getLogger(__name__)

_FFMPEG_CACHE: str | None = None

# Step 2 kill-switch: ENABLE_EQ_OVERLAY=1 включает circular EQ visualizer
# (FL Studio style polar bar chart). Default OFF — graceful degradation
# на случай OOM / timeout / matplotlib install проблем. Юзер flag'ает
# через Render env без redeploy.
ENABLE_EQ_OVERLAY = os.getenv("ENABLE_EQ_OVERLAY", "0") == "1"

# Font search paths (в порядке prioritет): system Linux DejaVu, common Mac
# и Windows локации, fallback на PIL bundled. PIL не имеет встроенных
# красивых TTF — без файла fallback на bitmap font (мелкий, ugly).
_FONT_CANDIDATES_BOLD = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",  # Debian/Ubuntu
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/Library/Fonts/Arial Bold.ttf",  # macOS
    "C:/Windows/Fonts/arialbd.ttf",  # Windows
    "DejaVuSans-Bold.ttf",  # PIL search path
]
_FONT_CANDIDATES_REGULAR = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    "/Library/Fonts/Arial.ttf",
    "C:/Windows/Fonts/arial.ttf",
    "DejaVuSans.ttf",
]

# Ограничение длины beat name для PIL text layout (84px font, 1080px viewport
# вмещает ~14 символов с padding). Длиннее — обрезаем + «…».
_NAME_MAX_LEN = 14


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


def _truncate_name(name: str, max_len: int = _NAME_MAX_LEN) -> str:
    """«AGGRESSIVE MEMPHIS DRILL» → «AGGRESSIVE ME…» — fits text layout."""
    if len(name) <= max_len:
        return name
    return name[: max_len - 1].rstrip() + "…"


def _load_pil_font(size: int, bold: bool = False):
    """Возвращает PIL ImageFont для draw.text. Перебирает candidates
    Linux/Mac/Windows; fallback на bitmap font (выглядит плохо, но не падает).
    """
    from PIL import ImageFont
    candidates = _FONT_CANDIDATES_BOLD if bold else _FONT_CANDIDATES_REGULAR
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except (OSError, IOError):
            continue
    logger.warning(
        "shorts: no TTF font found among %d candidates → fallback на bitmap",
        len(candidates),
    )
    return ImageFont.load_default()


def _render_text_overlay_png(meta_name: str, meta_bpm: int | None,
                             meta_key_short: str | None,
                             out_path: Path,
                             width: int = 1080, height: int = 1920) -> Path:
    """Рисует transparent PNG с beat name top + BPM/KEY bottom-right.

    Используется вместо ffmpeg drawtext filter, который отсутствует в
    imageio-ffmpeg static binary (compile-time disabled, known issue 2024).

    Layout:
    - Beat name (truncated): top-center, y=120, 84px bold, white + black shadow
    - BPM/KEY: bottom-right, y=h-th-80, 44px regular, white + black shadow
    """
    from PIL import Image, ImageDraw

    img = Image.new("RGBA", (width, height), (0, 0, 0, 0))  # transparent
    draw = ImageDraw.Draw(img)

    # Beat name top-center
    name_text = _truncate_name(meta_name)
    name_font = _load_pil_font(84, bold=True)
    bbox = draw.textbbox((0, 0), name_text, font=name_font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    name_x = (width - tw) // 2 - bbox[0]
    name_y = 120 - bbox[1]
    # Shadow + main
    draw.text((name_x + 3, name_y + 3), name_text, font=name_font,
              fill=(0, 0, 0, 200))
    draw.text((name_x, name_y), name_text, font=name_font,
              fill=(255, 255, 255, 255))

    # BPM/KEY bottom-right (если bpm есть)
    if meta_bpm:
        if meta_key_short:
            bpm_text = f"{meta_bpm} BPM | {meta_key_short}"
        else:
            bpm_text = f"{meta_bpm} BPM"
        bpm_font = _load_pil_font(44, bold=False)
        bbox_b = draw.textbbox((0, 0), bpm_text, font=bpm_font)
        bw, bh = bbox_b[2] - bbox_b[0], bbox_b[3] - bbox_b[1]
        bpm_x = width - bw - 60 - bbox_b[0]
        bpm_y = height - bh - 80 - bbox_b[1]
        draw.text((bpm_x + 2, bpm_y + 2), bpm_text, font=bpm_font,
                  fill=(0, 0, 0, 200))
        draw.text((bpm_x, bpm_y), bpm_text, font=bpm_font,
                  fill=(255, 255, 255, 255))

    img.save(out_path, "PNG")
    return out_path


def _build_filter_chain(meta_name: str | None,
                        text_overlay: bool = False,
                        eq_overlay: bool = False) -> str:
    """Конструирует ffmpeg filter_complex.

    Inputs ordering:
    - [0:v] = brand image (loop)
    - [1:a] = mp3
    - [2:v] = text overlay PNG (transparent), если text_overlay=True
    - [3:v] = EQ webm (transparent), если eq_overlay=True
      (или [2:v] если text_overlay=False — input shifting)

    Cases:
    - meta_name=None → legacy letterbox
    - text_overlay=False, eq_overlay=False → blurred bg + sharp, без overlay
    - text_overlay=True, eq_overlay=False → + text PNG overlay
    - text_overlay=False, eq_overlay=True → + EQ webm overlay
    - text_overlay=True, eq_overlay=True → + text + EQ
    """
    if meta_name is None:
        # Legacy fallback: simple scale + black-bar pad
        return "scale=1080:-2,pad=1080:1920:(ow-iw)/2:(oh-ih)/2:black,format=yuv420p"

    base_chain = (
        "[0:v]split=2[bg_src][fg_src];"
        "[bg_src]scale=1080:1920:force_original_aspect_ratio=increase,"
        "crop=1080:1920,boxblur=20:5[bg];"
        "[fg_src]scale=1080:-2[fg];"
        "[bg][fg]overlay=(W-w)/2:(H-h)/2"
    )

    # Build progressive chain через named labels
    chain = f"{base_chain}[base]"
    cur = "[base]"
    text_idx = 2  # input #2 = text PNG если включен
    eq_idx = 3 if text_overlay else 2  # input shifts если text disabled

    if text_overlay:
        chain += f";{cur}[{text_idx}:v]overlay=0:0[with_text]"
        cur = "[with_text]"
    if eq_overlay:
        chain += (
            f";{cur}[{eq_idx}:v]overlay=(W-w)/2:(H-h)/2:shortest=1"
            f",format=yuv420p[v_out]"
        )
    else:
        # Финал: format=yuv420p после последнего overlay (или сразу после base)
        if text_overlay:
            # cur = [with_text]
            chain += f";{cur}format=yuv420p[v_out]"
        else:
            # Нет ни text ни EQ — base сразу финал
            chain += f";{cur}format=yuv420p[v_out]"
    return chain


def build_short(image_path: Path, mp3_path: Path, out_path: Path,
                duration_sec: int = SHORTS_DURATION_SEC,
                start_offset_sec: int = SHORTS_OFFSET_SEC,
                meta=None) -> Path:
    """Собирает 9:16 1080×1920 mp4 из brand image + N сек mp3
    начиная со смещения start_offset_sec.

    Если `meta` (BeatMeta) передан → full-screen blurred bg + sharp center +
    text overlays (PIL → PNG → ffmpeg overlay).

    Если `meta=None` → legacy letterbox (для старых callers, не рекомендуется).

    Edge cases:
    - mp3 короче start_offset_sec → fallback offset=0 (берём с начала).
    - mp3 короче duration_sec → ffmpeg `-shortest` обрежет автоматом.
    - meta.name >14 chars → truncate с «…».
    - meta.key_short=None → показываем только «BPM».
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
            logger.info(
                "shorts: mp3 too short (%.1fs < offset %d+5) → fallback offset=0",
                mp3_duration, start_offset_sec,
            )
            actual_offset = 0

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Pre-render text overlay PNG если meta есть
    text_png_path: Path | None = None
    if meta is not None:
        try:
            text_png_path = out_path.with_name(f"text_{out_path.stem}.png")
            _render_text_overlay_png(
                meta_name=meta.name,
                meta_bpm=meta.bpm,
                meta_key_short=getattr(meta, "key_short", None),
                out_path=text_png_path,
            )
            logger.info("shorts: text overlay PNG generated → %s", text_png_path)
        except Exception as e:
            logger.warning(
                "shorts: text overlay PNG failed (%s) — fallback на blurred-bg only",
                e,
            )
            text_png_path = None

    # Step 2: pre-generate circular EQ webm если включен ENABLE_EQ_OVERLAY
    # Graceful degradation: при failure → fallback без EQ (text overlay остаётся).
    eq_webm_path: Path | None = None
    if ENABLE_EQ_OVERLAY and meta is not None:
        try:
            import circular_eq_renderer
            eq_webm_path = out_path.with_name(f"eq_{out_path.stem}.webm")
            circular_eq_renderer.render_circular_eq_overlay(
                mp3_path, eq_webm_path,
                duration_sec=duration_sec, offset_sec=actual_offset,
                fps=30, size=800,
            )
            logger.info("shorts: EQ overlay generated → %s", eq_webm_path)
        except Exception as e:
            logger.warning(
                "shorts: EQ overlay generation failed (%s) — fallback без EQ",
                e,
            )
            eq_webm_path = None

    # Build filter chain
    if meta is not None:
        filter_chain = _build_filter_chain(
            meta_name=meta.name,
            text_overlay=(text_png_path is not None),
            eq_overlay=(eq_webm_path is not None),
        )
        filter_arg = ["-filter_complex", filter_chain, "-map", "[v_out]", "-map", "1:a"]
    else:
        # Legacy path: simple -vf
        filter_arg = ["-vf", _build_filter_chain(None)]

    # ffmpeg cmd: inputs [0]=image, [1]=mp3, [2]=text PNG (if any), [3]=EQ webm
    # `-ss` ПЕРЕД `-i mp3` — accurate seek без overhead'а на full decode.
    cmd = [
        _ffmpeg(),
        "-y",
        "-loop", "1",
        "-r", "1",
        "-i", str(image_path),
        "-ss", str(actual_offset),
        "-i", str(mp3_path),
        "-t", str(duration_sec),
    ]
    # 3rd input = text overlay PNG (loop)
    if text_png_path is not None and text_png_path.exists():
        cmd += ["-loop", "1", "-i", str(text_png_path)]
    # 4th input = EQ webm
    if eq_webm_path is not None and eq_webm_path.exists():
        cmd += ["-i", str(eq_webm_path)]
    cmd += [
        *filter_arg,
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

    # Render free CPU shared — encoding 30-сек 1080×1920 с overlay'ями может
    # занять 2-5 мин. timeout 600 = 10 мин с большим запасом.
    timeout = max(600, duration_sec * 20)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"shorts ffmpeg timeout {timeout}s") from e

    if proc.returncode != 0:
        raise RuntimeError(
            f"shorts ffmpeg failed ({proc.returncode}): {proc.stderr[-1500:]}"
        )

    # Cleanup intermediate files (~1MB each) — не нужны после композитинга
    for tmp in (text_png_path, eq_webm_path):
        if tmp is not None and tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass

    logger.info(
        "short built OK: %s (%ds @ offset=%ds, meta=%s, text=%s, eq=%s)",
        out_path, duration_sec, actual_offset,
        meta.name if meta else "none",
        text_png_path is not None,
        eq_webm_path is not None,
    )
    return out_path
