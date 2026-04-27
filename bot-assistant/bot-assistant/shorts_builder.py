"""Сборка 9:16 видео для YouTube Shorts (и универсально — для TG Story,
Reels, TikTok если понадобится).

Дизайн (2026-04-27 update):
* **Blurred bg + sharp center** — full-screen, без чёрных полос. Тот же
  brand-кадр копируется: одна копия scale-up до 1080×1920 + boxblur =
  размытый bg, вторая копия (оригинал 16:9 → 1080×608) overlay'ится
  по центру = sharp focal point.
* **Text overlays**: beat name top-center (84px bold) + BPM/KEY
  bottom-right (44px). Artist scrolls Shorts feed → видит fit за 0.5с
  без открытия description.
* `-tune stillimage` — быстрый энкод (~60s на 30-сек short с overlay'ями
  на Render free tier; vs 50s до изменений — overhead +20% от boxblur+drawtext).

Backwards-compat: если `meta=None` → старый letterbox approach (legacy callers).
"""
from __future__ import annotations

import logging
import subprocess
from pathlib import Path

import imageio_ffmpeg

from config import SHORTS_DURATION_SEC, SHORTS_OFFSET_SEC

logger = logging.getLogger(__name__)

_FFMPEG_CACHE: str | None = None

# Font paths на Render Linux (Debian-based images). DejaVu обычно установлен.
# Если absent — fallback на ffmpeg internal font.
_DEJAVU_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVu-Sans-Bold.ttf"
_DEJAVU_REGULAR = "/usr/share/fonts/truetype/dejavu/DejaVu-Sans.ttf"

# Ограничение длины beat name для drawtext layout (84px font, 1080px viewport
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


def _escape_drawtext(s: str) -> str:
    r"""Экранирует строку для ffmpeg drawtext: `:`, `\`, `'` — special chars.

    ffmpeg parses drawtext text='...' с особыми правилами: backslash перед
    : и \ нужен. Одинарную кавычку нельзя внутри single-quoted text — заменяем
    на типографскую ’ (Unicode U+2019), не отличается визуально.
    """
    return (
        s.replace("\\", r"\\")
         .replace(":", r"\:")
         .replace("'", "’")
    )


def _truncate_name(name: str, max_len: int = _NAME_MAX_LEN) -> str:
    """«AGGRESSIVE MEMPHIS DRILL» → «AGGRESSIVE ME…» — fits drawtext layout."""
    if len(name) <= max_len:
        return name
    return name[: max_len - 1].rstrip() + "…"


def _font_arg(bold: bool = False) -> str:
    """Возвращает `:fontfile=PATH` если шрифт существует на хосте, иначе ''.

    На Render Linux DejaVu обычно есть. Если нет — ffmpeg использует internal
    monospace font (некрасиво но работает, не падает).
    """
    target = _DEJAVU_BOLD if bold else _DEJAVU_REGULAR
    if Path(target).exists():
        return f":fontfile={target}"
    return ""


def _build_filter_chain(meta_name: str | None, meta_bpm: int | None,
                        meta_key_short: str | None) -> str:
    """Конструирует ffmpeg filter_complex для blurred bg + sharp center +
    drawtext overlays.

    Если meta_name=None → fallback на старый letterbox (для legacy callers).
    """
    if meta_name is None:
        # Legacy fallback: simple scale + black-bar pad
        return "scale=1080:-2,pad=1080:1920:(ow-iw)/2:(oh-ih)/2:black,format=yuv420p"

    # Step 1 chain: blurred bg + sharp center → label [base]
    # Step 2 chain: drawtext name + drawtext bpm → label [v_out] (final)
    name_safe = _escape_drawtext(_truncate_name(meta_name))
    name_filter = (
        f"drawtext=text='{name_safe}'"
        f":fontsize=84:fontcolor=white:borderw=4:bordercolor=black@0.7"
        f":x=(w-text_w)/2:y=120"
        f"{_font_arg(bold=True)}"
    )

    text_overlays = name_filter
    if meta_bpm:
        if meta_key_short:
            bpm_text = f"{meta_bpm} BPM | {meta_key_short}"
        else:
            bpm_text = f"{meta_bpm} BPM"
        bpm_safe = _escape_drawtext(bpm_text)
        bpm_filter = (
            f"drawtext=text='{bpm_safe}'"
            f":fontsize=44:fontcolor=white:borderw=3:bordercolor=black@0.7"
            f":x=w-text_w-60:y=h-text_h-80"
            f"{_font_arg(bold=False)}"
        )
        text_overlays = f"{name_filter},{bpm_filter}"

    # Single-graph filter — все шаги в одну цепочку, финал помечен [v_out]
    return (
        "[0:v]split=2[bg_src][fg_src];"
        "[bg_src]scale=1080:1920:force_original_aspect_ratio=increase,"
        "crop=1080:1920,boxblur=20:5[bg];"
        "[fg_src]scale=1080:-2[fg];"
        f"[bg][fg]overlay=(W-w)/2:(H-h)/2,{text_overlays},format=yuv420p[v_out]"
    )


def build_short(image_path: Path, mp3_path: Path, out_path: Path,
                duration_sec: int = SHORTS_DURATION_SEC,
                start_offset_sec: int = SHORTS_OFFSET_SEC,
                meta=None) -> Path:
    """Собирает 9:16 1080×1920 mp4 из brand image + N сек mp3
    начиная со смещения start_offset_sec.

    Если `meta` (BeatMeta) передан → full-screen blurred bg + sharp center +
    text overlays (beat name + BPM/KEY).

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

    # Build filter chain (blurred bg + sharp center + drawtext OR legacy letterbox)
    if meta is not None:
        filter_chain = _build_filter_chain(
            meta_name=meta.name,
            meta_bpm=meta.bpm,
            meta_key_short=getattr(meta, "key_short", None),
        )
        filter_arg = ["-filter_complex", filter_chain, "-map", "[v_out]", "-map", "1:a"]
    else:
        # Legacy path: simple -vf
        filter_arg = ["-vf", _build_filter_chain(None, None, None)]

    # `-ss` ПЕРЕД `-i` — accurate seek без overhead'а на full decode.
    # Применяем только к audio (image — `-loop 1` бесконечная статика).
    cmd = [
        _ffmpeg(),
        "-y",
        "-loop", "1",
        "-r", "1",
        "-i", str(image_path),
        "-ss", str(actual_offset),
        "-i", str(mp3_path),
        "-t", str(duration_sec),
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

    logger.info(
        "short built OK: %s (%ds @ offset=%ds, meta=%s)",
        out_path, duration_sec, actual_offset,
        meta.name if meta else "none",
    )
    return out_path
