from __future__ import annotations

import json
import logging
import os
import random
import re

logger = logging.getLogger(__name__)

BEATS_FILE: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "beats_data.json")
BEATS_CACHE: list[dict] = []
BEATS_BY_ID: dict[int, dict] = {}  # индекс для мгновенного поиска

# Компилируем regex один раз
BPM_REGEX = re.compile(r'(\d{2,3})\s*bpm', re.IGNORECASE)
KEY_REGEX = re.compile(r'\b([A-G][b#]?\s*(?:min|maj|m|minor|major)?)\b')


def _rebuild_index() -> None:
    global BEATS_BY_ID
    BEATS_BY_ID = {beat["id"]: beat for beat in BEATS_CACHE}


def save_beats() -> None:
    """Атомарная запись beats_data.json: tmp-файл + os.replace + .bak rotation.

    Защита от порчи файла при прерывании (Render restart, kill -9, etc):
    * Перед записью копируем текущий валидный файл в .bak — это
      known-good snapshot для auto-recovery в load_beats().
    * Пишем новые данные в .tmp → os.replace() atomic на target.
    """
    import shutil
    tmp = BEATS_FILE + ".tmp"
    bak = BEATS_FILE + ".bak"
    try:
        # Rotate: текущий валидный файл → .bak (перед его перезаписью).
        # copy (не rename), чтобы target оставался доступен читателям
        # пока мы пишем .tmp. Валидируем JSON перед копированием —
        # чтобы не затереть валидный .bak битым target'ом (может
        # такое случиться если до этого fix'а файл уже побит).
        # Best-effort — не блокируем основную запись.
        if os.path.exists(BEATS_FILE):
            try:
                with open(BEATS_FILE, "r", encoding="utf-8") as f:
                    cur = json.load(f)
                if isinstance(cur, list):
                    shutil.copy2(BEATS_FILE, bak)
            except Exception:
                pass  # target битый — НЕ трогаем .bak (он может быть валидным)

        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(BEATS_CACHE, f, ensure_ascii=False, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass  # не все FS поддерживают fsync (docker/overlayfs)
        os.replace(tmp, BEATS_FILE)  # atomic на Linux+Windows
        logger.info("Beats saved: %d", len(BEATS_CACHE))
        # Mark for git autopush (если ENABLED — background loop push'нет в next tick).
        # Render free disk эфемерный → без push'а beats_data слетит на следующем deploy.
        try:
            import git_autopush
            git_autopush.mark_dirty(BEATS_FILE)
        except Exception:
            logger.warning("git_autopush.mark_dirty failed (non-fatal)", exc_info=True)
    except Exception as e:
        logger.error("Save error: %s", e)
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _try_load_file(path: str) -> bool:
    """Пробует загрузить path в BEATS_CACHE. Возвращает True при успехе.

    Валидирует: файл существует + парсится как JSON + top-level list.
    При ошибке — НЕ трогает BEATS_CACHE, возвращает False.
    """
    global BEATS_CACHE
    if not os.path.exists(path):
        return False
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            logger.warning("load_beats: %s is not a list (got %s)", path, type(data).__name__)
            return False
        BEATS_CACHE = data
        _rebuild_index()
        logger.info("load_beats: loaded %d from %s", len(BEATS_CACHE), path)
        return True
    except Exception as e:
        logger.warning("load_beats: %s failed to parse: %s", path, e)
        return False


def _git_checkout_beats_file() -> bool:
    """Last-resort recovery: git checkout HEAD -- beats_data.json.

    Render держит полный git clone — возвращает версию из последнего
    коммита (пусть устаревшую, но точно валидную). True если команда
    отработала без ошибок; дальше load_beats пробует прочитать файл.
    """
    import subprocess
    try:
        # Путь от repo root до файла (bot-assistant/bot-assistant/beats_data.json)
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        rel_path = os.path.relpath(BEATS_FILE, repo_root)
        result = subprocess.run(
            ["git", "checkout", "HEAD", "--", rel_path],
            cwd=repo_root,
            timeout=10,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logger.warning("git checkout failed (rc=%d): %s", result.returncode, result.stderr[-300:])
            return False
        logger.info("git checkout %s: OK", rel_path)
        return True
    except Exception as e:
        logger.warning("git checkout exception: %s", e)
        return False


def load_beats() -> None:
    """Загружает beats_data.json в BEATS_CACHE с 3 уровнями recovery:

    1. Основной файл BEATS_FILE
    2. .bak (known-good snapshot от предыдущего save)
    3. git checkout HEAD (последняя commit'нутая версия)

    Все три битые → оставляем пустой кэш, индекс синхронизирован.
    Каждый уровень recovery логируется в WARNING — видно в Render logs.
    """
    global BEATS_CACHE

    # Попытка 1: основной файл
    if _try_load_file(BEATS_FILE):
        return

    # Попытка 2: .bak
    bak = BEATS_FILE + ".bak"
    if _try_load_file(bak):
        logger.warning(
            "load_beats: RECOVERED from .bak (%d beats) — main file был битый",
            len(BEATS_CACHE),
        )
        # Пересоздаём target из .bak (save_beats сам опять скопирует в .bak)
        try:
            save_beats()
        except Exception as e:
            logger.warning("load_beats: re-save after .bak recovery failed: %s", e)
        return

    # Попытка 3: git-checkout последней commit'нутой версии
    if _git_checkout_beats_file() and _try_load_file(BEATS_FILE):
        logger.warning(
            "load_beats: RECOVERED from git HEAD (%d beats) — и main и .bak битые",
            len(BEATS_CACHE),
        )
        return

    # Всё перепробовали — пустой кэш, индекс синхронизирован
    BEATS_CACHE = []
    _rebuild_index()
    logger.error("load_beats: ALL recovery attempts failed, cache empty")


def parse_tags_from_text(text: str | None) -> list[str]:
    if not text:
        return []
    return [w[1:].lower() for w in text.split() if w.startswith("#")]


def parse_bpm_from_text(text: str | None) -> int | None:
    if not text:
        return None
    match = BPM_REGEX.search(text)
    return int(match.group(1)) if match else None


def parse_key_from_text(text: str | None) -> str | None:
    if not text:
        return None
    match = KEY_REGEX.search(text)
    return match.group(1).strip() if match else None


def get_all_tags() -> list[str]:
    tags: set[str] = set()
    for beat in BEATS_CACHE:
        tags.update(beat.get("tags", []))
    return sorted(tags)


def get_beat_by_id(beat_id: int) -> dict | None:
    return BEATS_BY_ID.get(beat_id)


def get_random_beat(exclude_ids: list[int] | None = None) -> dict | None:
    if exclude_ids is None:
        exclude_ids = []
    available = [b for b in BEATS_CACHE if b["id"] not in exclude_ids]
    if not available:
        available = BEATS_CACHE
    return random.choice(available) if available else None


def get_beats_by_tag(tag: str) -> list[dict]:
    return [b for b in BEATS_CACHE if tag in b.get("tags", [])]


def get_similar_beats(current_beat: dict, exclude_ids: list[int] | None = None) -> list[dict]:
    """Ищет похожие биты. Scoring:
    1. Общие теги (primary signal)
    2. Fallback: BPM ±15 + тот же content_type (если тегов нет совпадений)
    """
    if exclude_ids is None:
        exclude_ids = []
    current_tags = set(current_beat.get("tags", []))
    current_bpm = current_beat.get("bpm") or 0
    current_ct = current_beat.get("content_type", "beat")
    scored: list[tuple[int, dict]] = []
    bpm_fallback: list[tuple[int, dict]] = []
    for beat in BEATS_CACHE:
        if beat["id"] == current_beat["id"] or beat["id"] in exclude_ids:
            continue
        if beat.get("content_type", "beat") == "non_audio":
            continue
        common = len(current_tags & set(beat.get("tags", [])))
        if common > 0:
            scored.append((common, beat))
        elif current_bpm and beat.get("bpm") and beat.get("content_type", "beat") == current_ct:
            diff = abs(current_bpm - beat["bpm"])
            if diff <= 15:
                bpm_fallback.append((diff, beat))
    scored.sort(key=lambda x: x[0], reverse=True)
    result: list[dict] = [b for _, b in scored[:5]]
    # Если по тегам мало — добиваем BPM-соседями
    if len(result) < 5 and bpm_fallback:
        bpm_fallback.sort(key=lambda x: x[0])  # меньше разница = ближе
        for _, b in bpm_fallback:
            if b not in result and len(result) < 5:
                result.append(b)
    return result


def get_next_similar(current_beat: dict, exclude_ids: list[int] | None = None) -> dict | None:
    similar = get_similar_beats(current_beat, exclude_ids)
    return random.choice(similar) if similar else get_random_beat(exclude_ids)
