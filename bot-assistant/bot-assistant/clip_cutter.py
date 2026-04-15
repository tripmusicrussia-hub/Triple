"""Автосборка фоновых VHS-лупов из клипов артистов для YT-видео.

Flow:
  artist="kenny muney" → yt-dlp поиск "kenny muney official video"
  → 3 клипа × 60-сек фрагмент из середины (480p, без звука)
  → ffmpeg: shuffle-нарезка на куски 1.5-3 сек + VHS-стилизация
  → 60-сек loop.mp4 в assets/loops/<slug>.mp4, кэш 30 дней

Правила/фильтры см. ~/.claude/skills/clip-cutter/SKILL.md.
"""
from __future__ import annotations

import logging
import random
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import imageio_ffmpeg

logger = logging.getLogger(__name__)

HERE = Path(__file__).parent
LOOPS_DIR = HERE / "assets" / "loops"
CACHE_TTL_SEC = 30 * 24 * 3600  # 30 дней

TARGET_W, TARGET_H = 1280, 720
TARGET_FPS = 30
LOOP_DURATION_SEC = 60
SNIPPET_DURATION_SEC = 60
SNIPPETS_PER_ARTIST = 3
SUBCLIP_MIN = 1.5
SUBCLIP_MAX = 3.0

# Стилизация под-клипов: desat + darker curves + холодно-фиолетовый hue.
# noise вынесен в финальный concat — иначе убивает сжатие каждого сегмента.
VHS_FILTER = (
    "eq=saturation=0.45:contrast=1.15:brightness=-0.05,"
    "curves=preset=darker,"
    "hue=h=-10:s=0.8"
)
# Grain поверх склеенного лупа — один проход, управляемая стоимость
FINAL_GRAIN_FILTER = "noise=alls=12:allf=t"


def _ffmpeg() -> str:
    return imageio_ffmpeg.get_ffmpeg_exe()


_FFMPEG_DIR_CACHE: Path | None = None


def _ffmpeg_dir_for_ytdlp() -> str:
    """yt-dlp ищет бинарник по стандартному имени (ffmpeg/ffmpeg.exe) в директории.

    imageio-ffmpeg называет свой `ffmpeg-win-x86_64-v7.1.exe` — yt-dlp его не
    распознаёт. Создаём папку с hardlink/копией под корректным именем.
    """
    global _FFMPEG_DIR_CACHE
    if _FFMPEG_DIR_CACHE and _FFMPEG_DIR_CACHE.exists():
        return str(_FFMPEG_DIR_CACHE)

    src = Path(_ffmpeg())
    tgt_dir = Path(tempfile.gettempdir()) / "triple_ytdlp_ffmpeg"
    tgt_dir.mkdir(parents=True, exist_ok=True)
    tgt = tgt_dir / ("ffmpeg.exe" if src.suffix == ".exe" else "ffmpeg")
    if not tgt.exists():
        try:
            # Hardlink на той же volume — мгновенно, без копии 100 MB
            import os as _os
            _os.link(str(src), str(tgt))
            logger.info("ytdlp ffmpeg hardlink → %s", tgt)
        except Exception as e:
            logger.info("hardlink failed (%s), copying ffmpeg", e)
            shutil.copy2(src, tgt)
    _FFMPEG_DIR_CACHE = tgt_dir
    return str(tgt_dir)


def _slug(artist: str) -> str:
    """'Kenny Muney' → 'kenny_muney'. Для коллабов берём первого."""
    first = artist.split(" x ")[0].strip().lower()
    return re.sub(r"[^a-z0-9]+", "_", first).strip("_")


def _cache_path(artist: str) -> Path:
    return LOOPS_DIR / f"{_slug(artist)}.mp4"


def _is_cache_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    age = time.time() - path.stat().st_mtime
    return age < CACHE_TTL_SEC


def _search_videos(artist: str, limit: int = SNIPPETS_PER_ARTIST) -> list[dict]:
    """yt-dlp поиск: возвращает список {url, title, duration}.

    Фильтрует по длительности 2-6 мин (отсекает подкасты/интервью).
    """
    import yt_dlp

    query = f"ytsearch{limit * 3}:{artist} official video"
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": "in_playlist",
    }
    results: list[dict] = []
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(query, download=False)
        for entry in (info or {}).get("entries", []):
            if not entry:
                continue
            dur = entry.get("duration") or 0
            if not (120 <= dur <= 360):
                continue
            url = entry.get("url") or entry.get("webpage_url")
            if not url:
                continue
            if not url.startswith("http"):
                url = f"https://www.youtube.com/watch?v={url}"
            results.append({
                "url": url,
                "title": entry.get("title", "?"),
                "duration": dur,
            })
            if len(results) >= limit:
                break
    logger.info("clip_cutter: found %d videos for %r", len(results), artist)
    return results


def _download_snippet(video: dict, out: Path) -> Path | None:
    """Качает полный клип в 360p (без звука) и трим-ит 60-сек фрагмент из середины.

    Partial-download через yt-dlp требует system ffmpeg+ffprobe — у нас только
    bundled ffmpeg, так что полная загрузка + собственный трим проще.
    360p ~15-25 MB на 3-мин клип, три клипа ≈ 50-80 MB tmp.
    """
    import yt_dlp

    dur = video["duration"]
    start = max(10, int(dur * 0.4))

    raw = out.with_name(out.stem + "_raw.%(ext)s")
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "format": "bestvideo[height<=720][ext=mp4]/bestvideo[height<=720]/worst[height<=1080]",
        "outtmpl": str(raw),
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video["url"]])
    except Exception as e:
        logger.warning("clip_cutter: download failed %s: %s", video["url"], e)
        return None

    got = next((c for c in out.parent.glob(f"{out.stem}_raw.*")
                if c.suffix.lower() in (".mp4", ".mkv", ".webm")), None)
    if not got:
        return None

    # Трим 60 сек из середины, без звука
    cmd = [
        _ffmpeg(), "-y",
        "-ss", f"{start:.2f}",
        "-t", f"{SNIPPET_DURATION_SEC}",
        "-i", str(got),
        "-an",
        "-c:v", "copy",
        str(out),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    got.unlink(missing_ok=True)
    if proc.returncode != 0 or not out.exists():
        # copy не сработал (не keyframe на границе) — перекодируем
        cmd2 = cmd[:-2] + ["-c:v", "libx264", "-preset", "ultrafast", "-crf", "26", str(out)]
        proc2 = subprocess.run(cmd2, capture_output=True, text=True)
        if proc2.returncode != 0:
            logger.warning("clip_cutter: trim failed: %s", proc2.stderr[-400:])
            return None
    return out


def _probe_duration(path: Path) -> float:
    proc = subprocess.run([_ffmpeg(), "-i", str(path)], capture_output=True, text=True)
    for line in proc.stderr.splitlines():
        if "Duration:" in line:
            part = line.split("Duration:", 1)[1].split(",", 1)[0].strip()
            h, m, s = part.split(":")
            return int(h) * 3600 + int(m) * 60 + float(s)
    return 0.0


def _cut_subclips(snippet: Path, tmp_dir: Path, idx_start: int) -> list[Path]:
    """Режет 60-сек snippet на под-клипы 1.5-3 сек с VHS-стилизацией.

    Отбрасывает первые/последние 10% snippet (transitions).
    Возвращает список путей готовых 1280x720 кусков.
    """
    duration = _probe_duration(snippet)
    if duration < 5:
        return []
    lo = duration * 0.1
    hi = duration * 0.9
    cursor = lo
    outs: list[Path] = []
    idx = idx_start
    while cursor < hi:
        seg_len = random.uniform(SUBCLIP_MIN, SUBCLIP_MAX)
        if cursor + seg_len > hi:
            break
        out = tmp_dir / f"sub_{idx:03d}.mp4"
        vf = (
            f"scale={TARGET_W}:{TARGET_H}:force_original_aspect_ratio=increase,"
            f"crop={TARGET_W}:{TARGET_H},"
            f"{VHS_FILTER},"
            f"fps={TARGET_FPS},"
            f"format=yuv420p"
        )
        cmd = [
            _ffmpeg(), "-y",
            "-ss", f"{cursor:.2f}",
            "-t", f"{seg_len:.2f}",
            "-i", str(snippet),
            "-an",
            "-vf", vf,
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-crf", "23",
            "-pix_fmt", "yuv420p",
            str(out),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0 and out.exists() and out.stat().st_size > 1000:
            outs.append(out)
        cursor += seg_len
        idx += 1
    return outs


def _concat(subs: list[Path], out: Path, target_duration: float) -> Path:
    """Конкатит под-клипы через ffmpeg concat demuxer до целевой длительности."""
    random.shuffle(subs)
    # Достраиваем до target_duration повторами
    chosen: list[Path] = []
    total = 0.0
    while total < target_duration:
        for s in subs:
            d = _probe_duration(s)
            if d <= 0:
                continue
            chosen.append(s)
            total += d
            if total >= target_duration:
                break
        if not subs:
            break

    list_file = out.with_suffix(".txt")
    list_file.write_text(
        "\n".join(f"file '{p.as_posix()}'" for p in chosen),
        encoding="utf-8",
    )
    # Concat + grain + битрейт-cap. Grain тут, а не в subclips — иначе
    # каждый сегмент раздувается (шум несжимаем). maxrate=6M → ~45 MB на 60с.
    cmd = [
        _ffmpeg(), "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(list_file),
        "-an",
        "-vf", FINAL_GRAIN_FILTER,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "26",
        "-maxrate", "6M",
        "-bufsize", "12M",
        "-pix_fmt", "yuv420p",
        "-t", f"{target_duration:.2f}",
        "-movflags", "+faststart",
        str(out),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    list_file.unlink(missing_ok=True)
    if proc.returncode != 0:
        raise RuntimeError(f"concat failed: {proc.stderr[-800:]}")
    return out


def get_or_build_loop(artist: str, force: bool = False) -> Path | None:
    """Главный вход: вернёт путь к луп-файлу артиста или None при неудаче.

    Кэшируется в assets/loops/<slug>.mp4 на 30 дней.
    """
    if not artist or len(artist.strip()) < 3:
        logger.info("clip_cutter: artist too short, skipping")
        return None

    LOOPS_DIR.mkdir(parents=True, exist_ok=True)
    cache = _cache_path(artist)
    if not force and _is_cache_fresh(cache):
        logger.info("clip_cutter: cache hit for %r → %s", artist, cache)
        return cache

    logger.info("clip_cutter: building loop for %r (cache %s)",
                artist, "miss" if not cache.exists() else "stale")
    t0 = time.time()
    with tempfile.TemporaryDirectory(prefix="clipcut_") as td:
        tmp = Path(td)
        try:
            videos = _search_videos(artist)
        except Exception as e:
            logger.warning("clip_cutter: search failed: %s", e)
            return None
        if not videos:
            logger.warning("clip_cutter: no videos found for %r", artist)
            return None

        snippets: list[Path] = []
        for i, v in enumerate(videos):
            snip = tmp / f"snip_{i}.mp4"
            got = _download_snippet(v, snip)
            if got:
                snippets.append(got)
        if not snippets:
            logger.warning("clip_cutter: all downloads failed for %r", artist)
            return None

        all_subs: list[Path] = []
        idx = 0
        for snip in snippets:
            subs = _cut_subclips(snip, tmp, idx)
            all_subs.extend(subs)
            idx += len(subs)
        if len(all_subs) < 5:
            logger.warning("clip_cutter: too few subclips (%d) for %r",
                           len(all_subs), artist)
            return None

        try:
            _concat(all_subs, cache, LOOP_DURATION_SEC)
        except Exception as e:
            logger.warning("clip_cutter: concat failed: %s", e)
            return None

    elapsed = time.time() - t0
    logger.info("clip_cutter: loop ready %s (%.1fs, %d subclips, %d snippets)",
                cache, elapsed, len(all_subs), len(snippets))
    return cache


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    artist = sys.argv[1] if len(sys.argv) > 1 else "kenny muney"
    p = get_or_build_loop(artist, force="--force" in sys.argv)
    if p:
        print(f"OK -> {p} ({p.stat().st_size/1024/1024:.1f} MB)")
    else:
        print("FAIL (None)")
        sys.exit(1)
