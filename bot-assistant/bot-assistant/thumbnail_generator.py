"""YouTube thumbnail generator для TRIPLE FILL.

Два режима (приоритет сверху вниз):
1. generate_thumbnail_from_clip(loop_path, out_path)
   — вырезает выразительный кадр из clip-loop артиста. НЕТ крупного текста.
   Основной путь per yt-optimization SKILL R6 (анализ топ-15 thumbnail'ов:
   0/15 используют text-overlay, 60% — реальные фото артистов).

2. generate_thumbnail(track_name, artist_line, out_path)
   — LEGACY, text-overlay с neon-green. Применяется только когда нет clip-loop
   (unknown artist + нет branded-loop'а). Нарушает brand-правило «не неон»,
   но работает как last resort.
"""
from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from random import randint

import imageio_ffmpeg
from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageStat

logger = logging.getLogger(__name__)

HERE = Path(__file__).parent
FONTS_DIR = HERE / "assets" / "fonts"
ANTON = FONTS_DIR / "Anton-Regular.ttf"
JB_MONO = FONTS_DIR / "JetBrainsMono-Bold.ttf"

W, H = 1920, 1080
BG = (10, 10, 10)
WHITE = (242, 240, 230)
GREEN = (182, 255, 26)
GREY = (90, 90, 90)

PADDING = 90


def _grain(img: Image.Image, intensity: int = 12) -> Image.Image:
    """Лёгкий шум поверх — создаёт эффект «плёнки»."""
    noise = Image.new("RGB", img.size, (0, 0, 0))
    px = noise.load()
    for y in range(0, H, 2):
        for x in range(0, W, 2):
            v = randint(0, intensity)
            px[x, y] = (v, v, v)
    return Image.blend(img, noise, 0.05)


def _neon_stroke(draw_base: Image.Image, text: str, font: ImageFont.FreeTypeFont,
                  xy: tuple[int, int], color: tuple[int, int, int]) -> Image.Image:
    """Рисует текст с неоновым свечением заданного цвета."""
    glow_layer = Image.new("RGBA", draw_base.size, (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow_layer)
    gd.text(xy, text, font=font, fill=color + (200,))
    glow = glow_layer.filter(ImageFilter.GaussianBlur(radius=14))
    base = draw_base.convert("RGBA")
    base.alpha_composite(glow)
    d = ImageDraw.Draw(base)
    d.text(xy, text, font=font, fill=color)
    return base.convert("RGB")


def _fit_font(text: str, max_width: int, font_path: Path, start_size: int) -> ImageFont.FreeTypeFont:
    """Подбирает размер шрифта чтобы текст влез в max_width."""
    size = start_size
    while size > 40:
        f = ImageFont.truetype(str(font_path), size)
        w = f.getlength(text)
        if w <= max_width:
            return f
        size -= 6
    return ImageFont.truetype(str(font_path), size)


def generate_thumbnail(
    track_name: str,
    artist_line: str,
    out_path: Path,
) -> Path:
    """
    track_name: NAME трека, например "THOUGHTS"
    artist_line: "Kenny Muney Type Beat" (уже в правильном виде)
    """
    track_name = track_name.upper()
    artist_line = artist_line.upper()

    img = Image.new("RGB", (W, H), BG)

    # Tonal gradient в центре — очень тонкий, почти незаметный
    grad = Image.new("L", (W, H), 0)
    gd = ImageDraw.Draw(grad)
    cx, cy = W // 2, int(H * 0.45)
    for r in range(900, 0, -30):
        alpha = max(0, 30 - (900 - r) // 30)
        gd.ellipse((cx - r, cy - r, cx + r, cy + r), fill=alpha)
    tint = Image.new("RGB", (W, H), (20, 35, 10))
    img = Image.composite(tint, img, grad)

    # TRIPLE FILL wordmark — правый верхний угол
    wm_font = ImageFont.truetype(str(JB_MONO), 44)
    wm_text = "TRIPLE FILL"
    wm_w = wm_font.getlength(wm_text)
    draw = ImageDraw.Draw(img)
    draw.text(
        (W - PADDING - wm_w, PADDING),
        wm_text,
        font=wm_font,
        fill=WHITE,
    )
    # Зелёная точка-акцент перед wordmark
    dot_y = PADDING + 22
    draw.ellipse(
        (W - PADDING - wm_w - 26, dot_y - 8, W - PADDING - wm_w - 10, dot_y + 8),
        fill=GREEN,
    )

    # Horizontal line divider (subtle) — под wordmark
    draw.line(
        [(W - PADDING - wm_w - 26, PADDING + 60), (W - PADDING, PADDING + 60)],
        fill=GREY,
        width=2,
    )

    # NAME трека — самый крупный, низ-слева, с неоновым свечением
    name_font = _fit_font(track_name, W - 2 * PADDING, ANTON, 360)
    name_bbox = name_font.getbbox(track_name)
    name_h = name_bbox[3] - name_bbox[1]
    name_y = H - PADDING - name_h - 140
    img = _neon_stroke(img, track_name, name_font, (PADDING, name_y), GREEN)

    # Artist line — под именем, меньше, белый
    artist_font = _fit_font(artist_line, W - 2 * PADDING, ANTON, 80)
    draw = ImageDraw.Draw(img)
    draw.text(
        (PADDING, H - PADDING - 90),
        artist_line,
        font=artist_font,
        fill=WHITE,
    )

    # Зелёная вертикальная полоса слева — brand-lockup
    draw.rectangle(
        (PADDING - 30, name_y, PADDING - 20, H - PADDING - 60),
        fill=GREEN,
    )

    # Grain overlay
    img = _grain(img)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, "JPEG", quality=92)
    return out_path


# ── NEW PRIMARY PATH: thumbnail from clip-loop frame ─────────────
# Per yt-optimization SKILL R6-R7 (2026-04-18):
# - 0/15 top-thumbnails used large text-overlay → extract frame without text
# - 60% top-thumbnails были real artist photos → clip-loop кадр попадает в паттерн
# - clip-loop уже VHS-стилизован через clip-cutter → thumbnail визуально совпадает с видео

THUMB_W, THUMB_H = 1280, 720
FRAME_SAMPLE_POSITIONS = [0.15, 0.30, 0.45, 0.60, 0.75]  # где в лупе пробовать


def _ffmpeg() -> str:
    return imageio_ffmpeg.get_ffmpeg_exe()


def _probe_duration(path: Path) -> float:
    proc = subprocess.run([_ffmpeg(), "-i", str(path)], capture_output=True, text=True)
    for line in proc.stderr.splitlines():
        if "Duration:" in line:
            part = line.split("Duration:", 1)[1].split(",", 1)[0].strip()
            h, m, s = part.split(":")
            return int(h) * 3600 + int(m) * 60 + float(s)
    return 0.0


def _extract_frame(loop_path: Path, ts_sec: float, out: Path) -> Path | None:
    """ffmpeg: один кадр из видео по timestamp'у, без re-encode аудио."""
    cmd = [
        _ffmpeg(), "-y",
        "-ss", f"{ts_sec:.2f}",
        "-i", str(loop_path),
        "-frames:v", "1",
        "-q:v", "2",
        str(out),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0 or not out.exists():
        return None
    return out


def _score_frame(img_path: Path) -> float:
    """Скоринг кадра: variance grayscale (больше → больше визуального разнообразия).

    Кадры с низким score — blur/transition/монотонный цвет → не выбираем.
    """
    try:
        img = Image.open(img_path).convert("L")
        stat = ImageStat.Stat(img)
        return stat.stddev[0]
    except Exception:
        return 0.0


def generate_thumbnail_from_clip(loop_path: Path, out_path: Path) -> Path | None:
    """Извлекает best-scoring кадр из clip-loop'а как thumbnail.

    Возвращает out_path или None если все кадры провалились.
    """
    duration = _probe_duration(loop_path)
    if duration < 5:
        logger.warning("clip-loop too short for thumbnail: %s (%.1fs)", loop_path, duration)
        return None

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = out_path.parent / f"_thumb_tmp_{out_path.stem}"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    best: tuple[float, Path] | None = None
    try:
        for i, pos in enumerate(FRAME_SAMPLE_POSITIONS):
            ts = duration * pos
            tmp = tmp_dir / f"f{i}.jpg"
            got = _extract_frame(loop_path, ts, tmp)
            if not got:
                continue
            score = _score_frame(got)
            logger.debug("  frame %d @ %.1fs → score %.1f", i, ts, score)
            if best is None or score > best[0]:
                best = (score, got)

        if not best:
            logger.warning("no valid frames from %s", loop_path)
            return None

        # Открываем best-кадр, crop/scale до 1280×720, сохраняем
        img = Image.open(best[1]).convert("RGB")
        src_w, src_h = img.size
        target_ratio = THUMB_W / THUMB_H
        src_ratio = src_w / src_h
        if abs(src_ratio - target_ratio) > 0.01:
            # Center-crop к 16:9
            if src_ratio > target_ratio:
                new_w = int(src_h * target_ratio)
                x0 = (src_w - new_w) // 2
                img = img.crop((x0, 0, x0 + new_w, src_h))
            else:
                new_h = int(src_w / target_ratio)
                y0 = (src_h - new_h) // 2
                img = img.crop((0, y0, src_w, y0 + new_h))
        if img.size != (THUMB_W, THUMB_H):
            img = img.resize((THUMB_W, THUMB_H), Image.LANCZOS)

        img.save(out_path, "JPEG", quality=92)
        logger.info("thumbnail from clip %s → %s (score %.1f)",
                    loop_path.name, out_path, best[0])
        return out_path
    finally:
        # Чистим временные кадры
        for f in tmp_dir.iterdir():
            try:
                f.unlink()
            except Exception:
                pass
        try:
            tmp_dir.rmdir()
        except Exception:
            pass


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    if len(sys.argv) > 1 and sys.argv[1] == "--from-clip":
        # Новый путь: python thumbnail_generator.py --from-clip path/to/loop.mp4 out.jpg
        loop = Path(sys.argv[2])
        out = Path(sys.argv[3])
        p = generate_thumbnail_from_clip(loop, out)
        print("OK" if p else "FAIL", "->", p)
    else:
        # Legacy сэмплы (fallback режим)
        samples = [
            ("THOUGHTS", "Kenny Muney Type Beat"),
            ("BIG FLIPPA", "Rob49 x Bossman Dlow Type Beat"),
            ("HOOK", "Future x Don Toliver Type Beat"),
            ("FRIK", "NardoWick Type Beat"),
            ("MEMORY", "Key Glock Type Beat"),
        ]
        out = HERE / "assets" / "thumb_samples"
        for name, artist in samples:
            p = out / f"{name.lower().replace(' ', '_')}.jpg"
            generate_thumbnail(name, artist, p)
            print(f"OK  {p}")
