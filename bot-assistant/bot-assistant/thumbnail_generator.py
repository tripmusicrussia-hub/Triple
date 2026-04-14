"""YouTube thumbnail generator for TRIPLE FILL.

Стиль: минимализм, чёрный фон + bone white + toxic green акцент.
- Большой Anton Bold для имени трека (низ-слева)
- Anton для артиста (под именем, меньше, зелёный)
- Monospace wordmark "TRIPLE FILL" в правом верхнем углу
- Неоновая подсветка зелёным по краю имени трека (signal "pro")
- Subtle grain overlay
"""
from __future__ import annotations

from pathlib import Path
from random import randint

from PIL import Image, ImageDraw, ImageFilter, ImageFont

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


if __name__ == "__main__":
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
