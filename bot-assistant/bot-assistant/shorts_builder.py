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
                             width: int = 1080, height: int = 1920,
                             cta_text: str = "FREE PACK → t.me/iiiplfiii",
                             top_text: str | None = None,
                             save: bool = True):
    """Рисует transparent PNG с TYPE top + BPM/KEY + CTA bottom.

    Используется вместо ffmpeg drawtext filter, который отсутствует в
    imageio-ffmpeg static binary (compile-time disabled, known issue 2024).

    Параметры:
    - `top_text` — что показывать сверху. Если None → fallback на meta_name
      (legacy). Для нового дизайна caller передаёт `artist_display.upper()`
      (TYPE TAG: «KENNY MUNEY» / «HARD TRAP» / «FUTURE»).
    - `width`/`height` — размеры canvas. Шрифты автомасштаб от height
      (1920 reference, минимум 24px).
    - `cta_text=""` → CTA не рисуется.
    - `save=True` сохраняет в out_path; иначе возвращает Image (для
      thumbnail composite в PIL).

    Layout (proportional к height):
    - Top text: top-center, y=120*scale, 84*scale px bold
    - BPM/KEY: bottom-right, y=h-th-280*scale, 44*scale px regular
    - CTA: bottom-center, y=h-ch-100*scale, 38*scale px bold
    """
    from PIL import Image, ImageDraw

    # Auto-scale fonts от height (1920 reference). Floor — минимальный читаемый
    # размер, чтобы на 720 не было слишком мелко.
    scale = height / 1920
    name_size = max(48, int(84 * scale))
    bpm_size = max(28, int(44 * scale))
    cta_size = max(24, int(38 * scale))
    top_y = max(40, int(120 * scale))
    bpm_offset_bottom = max(60, int(280 * scale))
    cta_offset_bottom = max(40, int(100 * scale))
    side_padding = max(30, int(60 * scale))

    img = Image.new("RGBA", (width, height), (0, 0, 0, 0))  # transparent
    draw = ImageDraw.Draw(img)

    # Top text (TYPE TAG primary, fallback на beat name для legacy)
    display_top = top_text if top_text else _truncate_name(meta_name)
    if display_top:
        name_font = _load_pil_font(name_size, bold=True)
        bbox = draw.textbbox((0, 0), display_top, font=name_font)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        name_x = (width - tw) // 2 - bbox[0]
        name_y = top_y - bbox[1]
        # Shadow + main
        draw.text((name_x + 3, name_y + 3), display_top, font=name_font,
                  fill=(0, 0, 0, 200))
        draw.text((name_x, name_y), display_top, font=name_font,
                  fill=(255, 255, 255, 255))

    # BPM/KEY bottom-right (поднято от низа — не перекрывает YT UI)
    if meta_bpm:
        if meta_key_short:
            bpm_text = f"{meta_bpm} BPM | {meta_key_short}"
        else:
            bpm_text = f"{meta_bpm} BPM"
        bpm_font = _load_pil_font(bpm_size, bold=False)
        bbox_b = draw.textbbox((0, 0), bpm_text, font=bpm_font)
        bw, bh = bbox_b[2] - bbox_b[0], bbox_b[3] - bbox_b[1]
        bpm_x = width - bw - side_padding - bbox_b[0]
        bpm_y = height - bh - bpm_offset_bottom - bbox_b[1]
        draw.text((bpm_x + 2, bpm_y + 2), bpm_text, font=bpm_font,
                  fill=(0, 0, 0, 200))
        draw.text((bpm_x, bpm_y), bpm_text, font=bpm_font,
                  fill=(255, 255, 255, 255))

    # CTA bottom-center (funnel в Telegram bot)
    if cta_text:
        cta_font = _load_pil_font(cta_size, bold=True)
        bbox_c = draw.textbbox((0, 0), cta_text, font=cta_font)
        cw, ch = bbox_c[2] - bbox_c[0], bbox_c[3] - bbox_c[1]
        cta_x = (width - cw) // 2 - bbox_c[0]
        cta_y = height - ch - cta_offset_bottom - bbox_c[1]
        draw.text((cta_x + 2, cta_y + 2), cta_text, font=cta_font,
                  fill=(0, 0, 0, 220))
        draw.text((cta_x, cta_y), cta_text, font=cta_font,
                  fill=(255, 255, 255, 255))

    if save:
        img.save(out_path, "PNG")
        return out_path
    return img  # PIL Image для in-memory composite


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

    # Zoom-fill: scale 16:9 brand image до 1920 height, crop sides до 1080.
    # Убрали blurred-bg pattern (выглядел как letterbox в vertical frame —
    # downranking signal для YT Shorts алгоритма + слабее retention).
    # Теряем ~25% по краям, но фокус (центр кадра — артефакт/цепочки) сохраняется.
    # Bonus: убрали дорогой boxblur=20:5 → encode на 30-40% быстрее.
    base_chain = (
        "[0:v]scale=1080:1920:force_original_aspect_ratio=increase,"
        "crop=1080:1920"
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

    # 2026-04-28 RAM optimization для Render free 512MB:
    # Вместо ffmpeg filter_complex с 2-3 inputs (brand image + text PNG +
    # опц. EQ webm) — pre-composite ВСЁ via PIL в один JPG. ffmpeg получает
    # single image input → simple encode без filter graph → RAM в 2-3x ниже,
    # OOM-kill устранён на free tier.
    #
    # Pipeline:
    # 1. PIL: zoom-fill brand image до 1080×1920 (was ffmpeg scale+crop)
    # 2. PIL: composite text overlay (was ffmpeg overlay filter)
    # 3. Save as JPG (small file, fast load в ffmpeg)
    # 4. ffmpeg: single image + mp3 → mp4, simple `-vf format=yuv420p`
    pre_composed_bg: Path | None = None
    if meta is not None:
        try:
            pre_composed_bg = out_path.with_name(f"bg_{out_path.stem}.jpg")
            logger.info("shorts: PIL pre-compose bg+text...")
            artist_top = (getattr(meta, "artist_display", "") or "").upper().strip()
            if not artist_top:
                artist_top = "HARD TRAP"
            _pre_compose_shorts_bg(
                image_path=image_path,
                out_path=pre_composed_bg,
                meta_name=meta.name,
                meta_bpm=meta.bpm,
                meta_key_short=getattr(meta, "key_short", None),
                top_text=artist_top,
            )
            sz = pre_composed_bg.stat().st_size if pre_composed_bg.exists() else 0
            logger.info(
                "shorts: pre-composed bg → %s (%d KB)",
                pre_composed_bg, sz // 1024,
            )
        except Exception as e:
            logger.warning(
                "shorts: pre-compose failed (%s) — fallback на raw brand image",
                e,
            )
            pre_composed_bg = None

    # Source image для ffmpeg: pre-composed (premium layout) ИЛИ raw brand
    source_image = pre_composed_bg if (pre_composed_bg and pre_composed_bg.exists()) else image_path

    # 2026-04-28 EQ redesign: вместо heavy matplotlib circular EQ webm
    # (~80-110s pre-render) → ffmpeg native `showfreqs` filter. Audio-reactive
    # horizontal bars под картой (Jay3hitta/Laruss pattern, доминирующий
    # в 40% top-tier beat-snippet Shorts).
    #
    # Layout: bars 1080×180 на y=h-300 (под карточкой 1080×1080 которая
    # центрирована y=420..1500). Между card-bottom (y=1500) и bars-top
    # (y=1620) — 120px breathing room.
    use_eq_bars = ENABLE_EQ_OVERLAY and meta is not None and pre_composed_bg and pre_composed_bg.exists()

    if pre_composed_bg and pre_composed_bg.exists() and use_eq_bars:
        # ffmpeg filter_complex: bg jpg + audio showfreqs → overlay bars
        # showfreqs=mode=bar:s=1080x180:colors=white|red:cmode=combined
        # → output transparent waveform image stream → overlay снизу
        filter_chain = (
            "[1:a]showfreqs=mode=bar:s=1080x180:fscale=lin"
            ":colors=#FFFFFF|#B388EB,format=rgba[bars];"
            "[0:v][bars]overlay=0:H-h-120:shortest=1,format=yuv420p[v_out]"
        )
        filter_arg = ["-filter_complex", filter_chain,
                      "-map", "[v_out]", "-map", "1:a"]
    elif pre_composed_bg and pre_composed_bg.exists():
        filter_arg = ["-vf", "format=yuv420p"]
    else:
        filter_arg = ["-vf", _build_filter_chain(None)]

    # ffmpeg cmd: bg image (looped) + mp3 → mp4
    cmd = [
        _ffmpeg(),
        "-y",
        "-loop", "1",
        "-r", "1",
        "-i", str(source_image),
        "-ss", str(actual_offset),
        "-i", str(mp3_path),
        "-t", str(duration_sec),
        *filter_arg,
        "-c:v", "libx264",
        "-tune", "stillimage",
        "-preset", "superfast",
        "-crf", "28",
        "-r", "30",
        "-threads", "1",  # Render free 512MB: 1 thread = minimal peak RAM
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
    logger.info(
        "shorts: starting ffmpeg subprocess (timeout=%ds, inputs=%d, filter=%s)",
        timeout, sum(1 for a in cmd if a == "-i"),
        "complex" if any(a == "-filter_complex" for a in cmd) else "simple",
    )
    import time as _time
    _t0 = _time.time()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"shorts ffmpeg timeout {timeout}s") from e
    _elapsed = _time.time() - _t0
    logger.info(
        "shorts: ffmpeg subprocess done in %.1fs (rc=%d)",
        _elapsed, proc.returncode,
    )

    if proc.returncode != 0:
        raise RuntimeError(
            f"shorts ffmpeg failed ({proc.returncode}): {proc.stderr[-1500:]}"
        )

    # Cleanup intermediate pre-composed bg jpg (~250KB)
    if pre_composed_bg is not None and pre_composed_bg.exists():
        try:
            pre_composed_bg.unlink()
        except Exception:
            pass

    logger.info(
        "short built OK: %s (%ds @ offset=%ds, meta=%s, premium_layout=%s, eq_bars=%s)",
        out_path, duration_sec, actual_offset,
        meta.name if meta else "none",
        pre_composed_bg is not None,
        use_eq_bars,
    )
    return out_path


def _blurred_mirror_bg(src_image, width: int, height: int):
    """Premium beat-snippet pattern: scaled+blurred copy того же image как
    backdrop. Заполняет весь 9:16 viewport мягким размытым тоном картинки.

    Steps:
    1. Scale source × 1.4 (zoom-in для blur padding)
    2. Center-crop to 1080×1920
    3. Heavy GaussianBlur (radius 50)
    4. Darken -25% (Brightness 0.75) — не отвлекает от центра
    """
    from PIL import Image, ImageFilter, ImageEnhance
    sw, sh = src_image.size
    # Scale image so its smaller dimension >= viewport × 1.4
    target_ratio = width / height
    src_ratio = sw / sh
    if src_ratio > target_ratio:
        new_h = int(height * 1.4)
        new_w = int(sw * (new_h / sh))
    else:
        new_w = int(width * 1.4)
        new_h = int(sh * (new_w / sw))
    scaled = src_image.resize((new_w, new_h), Image.LANCZOS)
    x0 = (new_w - width) // 2
    y0 = (new_h - height) // 2
    cropped = scaled.crop((x0, y0, x0 + width, y0 + height))
    blurred = cropped.filter(ImageFilter.GaussianBlur(radius=50))
    darkened = ImageEnhance.Brightness(blurred).enhance(0.75)
    return darkened


def _card_with_shadow(src_image, card_size: int = 1080,
                      radius: int = 24, shadow_offset: int = 12,
                      shadow_blur: int = 30):
    """Premium pattern: original image в квадратной карточке с rounded corners
    + drop shadow на transparent canvas. Возвращает RGBA того же размера что
    full viewport (1080×1920) с card centered.

    Steps:
    1. Original image scaled-cropped to card_size×card_size (square)
    2. Rounded corners (mask с radius)
    3. Drop shadow (offset + blur black @ 60% opacity)
    4. Paste on transparent 1080×1920 canvas, centered vertically
    """
    from PIL import Image, ImageFilter, ImageDraw
    # 1. Scale-crop source to square card_size×card_size (center crop)
    sw, sh = src_image.size
    if sw != sh:
        s = min(sw, sh)
        x0 = (sw - s) // 2
        y0 = (sh - s) // 2
        src_image = src_image.crop((x0, y0, x0 + s, y0 + s))
    card = src_image.resize((card_size, card_size), Image.LANCZOS)

    # 2. Rounded corners via mask
    mask = Image.new("L", (card_size, card_size), 0)
    ImageDraw.Draw(mask).rounded_rectangle(
        (0, 0, card_size, card_size), radius=radius, fill=255,
    )
    card_rgba = Image.new("RGBA", (card_size, card_size), (0, 0, 0, 0))
    card_rgba.paste(card, (0, 0), mask=mask)

    # 3. Build canvas with shadow + card
    canvas = Image.new("RGBA", (1080, 1920), (0, 0, 0, 0))
    card_x = (1080 - card_size) // 2
    card_y = (1920 - card_size) // 2

    # Shadow: black silhouette of card mask, offset + blurred
    shadow = Image.new("RGBA", (1080, 1920), (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow)
    shadow_draw.rounded_rectangle(
        (card_x + shadow_offset, card_y + shadow_offset,
         card_x + shadow_offset + card_size, card_y + shadow_offset + card_size),
        radius=radius, fill=(0, 0, 0, 150),
    )
    shadow = shadow.filter(ImageFilter.GaussianBlur(radius=shadow_blur))
    canvas = Image.alpha_composite(canvas, shadow)
    canvas.paste(card_rgba, (card_x, card_y), card_rgba)
    return canvas


def _apply_brand_logo(canvas, logo_path: Path | None,
                      pos: tuple = (40, 40), size: int = 100,
                      opacity: int = 180):
    """Если logo file существует — paste top-left с opacity. Иначе no-op.

    Universal pattern в premium type-beat Shorts (NSM, Laruss): brand
    маркировка в углу превращает Short в showcase, не разовый пост.
    """
    if logo_path is None or not logo_path.exists():
        return canvas
    try:
        from PIL import Image
        logo = Image.open(logo_path).convert("RGBA")
        logo = logo.resize((size, size), Image.LANCZOS)
        # Apply opacity
        if opacity < 255:
            alpha = logo.split()[3]
            from PIL import ImageEnhance
            alpha = ImageEnhance.Brightness(alpha).enhance(opacity / 255)
            logo.putalpha(alpha)
        canvas.paste(logo, pos, logo)
    except Exception as e:
        logger.warning("logo apply failed: %s", e)
    return canvas


def _draw_minimal_beat_name(canvas, beat_name: str, width: int = 1080,
                             top_y: int = 80):
    """Single text element — beat name только. Big bold sans-serif white,
    minimal drop-shadow. Top-tier pattern: one-text or text-free.

    Заменяет heavy 3-text overlay (TYPE TAG + BPM/KEY + CTA) старого подхода.
    Premium-tier shorts (Jay3hitta, NSM, Laruss) text-free либо только
    beat-name. BPM/Key/CTA переносим в YT description.
    """
    from PIL import ImageDraw
    if not beat_name:
        return canvas
    name = _truncate_name(beat_name)
    draw = ImageDraw.Draw(canvas)
    font = _load_pil_font(96, bold=True)
    bbox = draw.textbbox((0, 0), name, font=font)
    tw = bbox[2] - bbox[0]
    name_x = (width - tw) // 2 - bbox[0]
    name_y = top_y - bbox[1]
    # Subtle shadow
    draw.text((name_x + 2, name_y + 2), name, font=font, fill=(0, 0, 0, 160))
    draw.text((name_x, name_y), name, font=font, fill=(255, 255, 255, 255))
    return canvas


def _pre_compose_shorts_bg(image_path: Path, out_path: Path,
                           meta_name: str, meta_bpm: int | None,
                           meta_key_short: str | None,
                           top_text: str,
                           width: int = 1080, height: int = 1920) -> Path:
    """Pre-composite premium 1080×1920 frame via PIL → save as JPG.

    2026-04-28 redesign по research топ beat-snippet Shorts (Jay3hitta 14k,
    Laruss 86k, NSM 28k):

    Layout:
    - BG: blurred-mirrored copy того же image (scale×1.4 + GaussianBlur(50)
      + darken). Заменяет старый zoom-fill.
    - CENTER: 1080×1080 квадратная карточка с rounded corners (24px) +
      drop shadow. Содержит original artwork.
    - TOP-LEFT: brand logo (если assets/brand/logo.png существует), opacity 70%
    - TEXT: только beat name центром сверху (96px bold white). BPM/Key/CTA
      перенесены в YT description (top-tier обходится одним текстом или
      без него).

    Backwards-compat params (meta_bpm, meta_key_short, top_text) сохранены
    в signature — не используются в новом layout, но callers не ломаются.
    """
    from PIL import Image
    src = Image.open(image_path).convert("RGB")

    # 1. Blurred-mirrored bg
    bg = _blurred_mirror_bg(src, width, height).convert("RGBA")

    # 2. Center card 1080×1080 (RGBA с shadow)
    card_layer = _card_with_shadow(src, card_size=1080,
                                   radius=24, shadow_offset=12, shadow_blur=30)

    # 3. Composite: bg + card
    canvas = Image.alpha_composite(bg, card_layer)

    # 4. Brand logo top-left (graceful skip если файл отсутствует)
    logo_path = Path(__file__).parent / "assets" / "brand" / "logo.png"
    canvas = _apply_brand_logo(canvas, logo_path, pos=(40, 40), size=110, opacity=180)

    # 5. Beat name центром сверху (single text element)
    canvas = _draw_minimal_beat_name(canvas, meta_name, width=width, top_y=80)

    # 6. Save as JPG (no alpha needed для final)
    final = canvas.convert("RGB")
    final.save(out_path, "JPEG", quality=90)
    return out_path
