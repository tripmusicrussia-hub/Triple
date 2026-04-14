"""
Генератор ежедневных постов в канал @iiiplfiii.

System-prompt для LLM читается из ~/.claude/skills/iiiplkiii-voice/SKILL.md
(единый источник tone-of-voice — общий для Claude и для бота).

Рубрики по дням недели:
  Пн — Memphis Monday (🎧 аудио)
  Вт — Мысль/процесс (💭 текст)
  Ср — Hard Lifehack (📝 текст)
  Чт — Studio Story (🔥 текст)
  Пт — Hard Friday (🎹 аудио)
  Сб — За кулисами (🎛 текст)
  Вс — Итог + вопрос (❤️ текст)
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Optional, TypedDict

import httpx

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

import beats_db

logger = logging.getLogger(__name__)

HERE = Path(__file__).parent
# Источник tone-of-voice. В приоритете — копия в репо (для прода на Render),
# fallback — локальный Claude skill (удобно для сессий/разработки).
_SKILL_REPO = HERE / "wiki" / "iiiplkiii_voice.md"
_SKILL_LOCAL = Path.home() / ".claude" / "skills" / "iiiplkiii-voice" / "SKILL.md"
SKILL_PATH = _SKILL_REPO if _SKILL_REPO.exists() else _SKILL_LOCAL
POST_IDEAS_PATH = HERE / "wiki" / "post_ideas.md"

MODELS = [
    "anthropic/claude-haiku-4.5",
    "openai/gpt-4o-mini",
    "openai/gpt-oss-120b:free",
]

AUDIO_REPOST_COOLDOWN_DAYS = 30


RubricKind = Literal["audio", "text"]


class Rubric(TypedDict):
    name: str
    kind: RubricKind
    section: str  # заголовок раздела в post_ideas.md (для text)
    user_instruction: str  # доп. указания в user-message к LLM


RUBRIC_SCHEDULE: dict[int, Rubric] = {
    0: {
        "name": "Memphis Monday",
        "kind": "audio",
        "section": "",
        "user_instruction": "Напиши подпись к биту для рубрики Memphis Monday. Memphis-вайб, хард, жёстко. 2-4 строки + 2-4 тега.",
    },
    1: {
        "name": "Quick Tip",
        "kind": "text",
        "section": "Вт — Quick Tip",
        "user_instruction": "Фишка дня — 4-6 строк. Формат: «попробуй X → эффект Y → короткое ПОЧЕМУ работает (1-2 строки: психоакустика / физика / контекст Memphis-звука)». Один приём, не мешай несколько. Используй ТОЛЬКО плагины из раздела 'Битмейкерский домен'. Без воды, но чтобы читатель понял не только ЧТО, но и ЗАЧЕМ.",
    },
    2: {
        "name": "Hard Lifehack",
        "kind": "text",
        "section": "Ср — Hard Lifehack",
        "user_instruction": "Разбор приёма: что → как → ПОЧЕМУ работает (физика звука/психоакустика). 4-6 строк. Используй ТОЛЬКО плагины/технику из раздела 'Битмейкерский домен'. В конце — «попробуй».",
    },
    3: {
        "name": "Studio Story",
        "kind": "text",
        "section": "Чт — Studio Story",
        "user_instruction": "Расскажи короткую историю из студии от первого лица. 3-5 строк, с эмоцией, с инсайтом в конце.",
    },
    4: {
        "name": "Hard Friday",
        "kind": "audio",
        "section": "",
        "user_instruction": "Напиши подпись к пятничному биту — основной релиз недели. Жёсткий, пушечный. 2-4 строки + 2-4 тега.",
    },
    5: {
        "name": "За кулисами",
        "kind": "text",
        "section": "Сб — За кулисами",
        "user_instruction": "Напиши пост «за кулисами» о процессе/плагине/шаблоне. 3-5 строк, технично но без занудства.",
    },
    6: {
        "name": "Итог + вопрос",
        "kind": "text",
        "section": "Вс — Итог + вопрос",
        "user_instruction": "Напиши рефлексию недели + открытый вопрос подписчикам в конце. 2-4 строки + вопрос.",
    },
}


# ─── System-prompt ────────────────────────────────────────────────────────────

_SYSTEM_PROMPT_CACHE: Optional[str] = None


def get_system_prompt() -> str:
    global _SYSTEM_PROMPT_CACHE
    if _SYSTEM_PROMPT_CACHE is None:
        if not SKILL_PATH.exists():
            raise FileNotFoundError(f"SKILL.md не найден: {SKILL_PATH}")
        _SYSTEM_PROMPT_CACHE = SKILL_PATH.read_text(encoding="utf-8")
    return _SYSTEM_PROMPT_CACHE


# ─── Темы для текстовых рубрик ────────────────────────────────────────────────

def _read_ideas() -> str:
    return POST_IDEAS_PATH.read_text(encoding="utf-8")


def _write_ideas(text: str) -> None:
    POST_IDEAS_PATH.write_text(text, encoding="utf-8")


def pick_text_topic(section: str) -> Optional[str]:
    """Возвращает случайную невостребованную тему из раздела. НЕ помечает — см. mark_topic_used."""
    text = _read_ideas()
    lines = text.splitlines()
    start_idx = None
    end_idx = None
    for i, line in enumerate(lines):
        if line.startswith("## ") and section in line:
            start_idx = i + 1
            continue
        if start_idx is not None and line.startswith("## "):
            end_idx = i
            break
    if start_idx is None:
        logger.warning("post_ideas: раздел '%s' не найден", section)
        return None
    if end_idx is None:
        end_idx = len(lines)

    unused = [lines[i] for i in range(start_idx, end_idx) if lines[i].startswith("- [ ] ")]
    if not unused:
        logger.warning("post_ideas: все темы в '%s' использованы", section)
        return None

    line = random.choice(unused)
    return line[len("- [ ] "):].strip()


def mark_topic_used(topic: str) -> None:
    """Помечает тему как использованную после успешной публикации."""
    text = _read_ideas()
    target_unused = f"- [ ] {topic}"
    target_used = f"- [x] {topic}"
    if target_unused not in text:
        logger.warning("mark_topic_used: тема не найдена или уже помечена: %s", topic)
        return
    _write_ideas(text.replace(target_unused, target_used, 1))


# ─── Выбор бита для аудио-рубрик ──────────────────────────────────────────────

def pick_audio_beat() -> Optional[dict]:
    """Бит, который не публиковался последние N дней. Только content_type=beat."""
    if not beats_db.BEATS_CACHE:
        beats_db.load_beats()
    cutoff = datetime.now() - timedelta(days=AUDIO_REPOST_COOLDOWN_DAYS)
    candidates = []
    for b in beats_db.BEATS_CACHE:
        if b.get("content_type") != "beat":
            continue
        if not b.get("file_id"):
            continue
        last = b.get("last_posted_at")
        if last:
            try:
                if datetime.fromisoformat(last) > cutoff:
                    continue
            except ValueError:
                pass
        candidates.append(b)
    if not candidates:
        logger.warning("pick_audio_beat: нет бит-кандидатов (все в cooldown или каталог пуст)")
        return None
    return random.choice(candidates)


def mark_beat_posted(beat_id: int) -> None:
    beat = beats_db.get_beat_by_id(beat_id)
    if not beat:
        return
    beat["last_posted_at"] = datetime.now().isoformat(timespec="seconds")
    beats_db.save_beats()


# ─── LLM ──────────────────────────────────────────────────────────────────────

async def _call_llm(user_message: str, max_tokens: int = 400, temperature: float = 0.85) -> str:
    api_key = os.getenv("OPENROUTER_KEY") or os.getenv("OPENROUTER_API_KEY", "")
    if not api_key:
        raise RuntimeError("OPENROUTER_KEY не задан в env")
    system = get_system_prompt()
    last_error = None
    async with httpx.AsyncClient(timeout=60) as client:
        for model in MODELS:
            try:
                resp = await client.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                        "HTTP-Referer": "https://github.com/tripmusicrussia-hub/Triple",
                    },
                    json={
                        "model": model,
                        "max_tokens": max_tokens,
                        "temperature": temperature,
                        "messages": [
                            {"role": "system", "content": system},
                            {"role": "user", "content": user_message},
                        ],
                    },
                )
                j = resp.json()
                if "error" in j:
                    last_error = j["error"].get("message", str(j["error"]))
                    logger.warning("LLM %s error: %s", model, last_error)
                    continue
                content = j["choices"][0]["message"].get("content") or ""
                if content.strip():
                    logger.info("LLM %s: ok, %d симв.", model, len(content))
                    return content.strip()
            except Exception as e:
                last_error = str(e)
                logger.warning("LLM %s exception: %s", model, e)
    raise RuntimeError(f"Все LLM недоступны. Последняя ошибка: {last_error}")


# ─── Генерация контента ───────────────────────────────────────────────────────

async def generate_caption(rubric: Rubric, beat: dict) -> str:
    """Подпись к аудио-посту. Держим в пределах 400-500 символов."""
    bpm = beat.get("bpm") or 0
    key = beat.get("key") or "-"
    tags = beat.get("tags") or []
    name = beat.get("name", "бит")
    meta_lines = [f"Название файла: {name}"]
    if bpm:
        meta_lines.append(f"BPM: {bpm}")
    if key and key != "-":
        meta_lines.append(f"Key: {key}")
    if tags:
        meta_lines.append(f"Теги из метаданных: {', '.join(tags[:10])}")

    user_msg = (
        f"{rubric['user_instruction']}\n\n"
        f"=== МЕТАДАННЫЕ БИТА ===\n" + "\n".join(meta_lines) + "\n"
        f"=== КОНЕЦ ===\n\n"
        "Не выдумывай BPM/key если их нет. Верни только текст подписи, без пояснений."
    )
    return await _call_llm(user_msg, max_tokens=350, temperature=0.85)


async def generate_text_post(rubric: Rubric, topic: str) -> str:
    """Текстовый пост на заданную тему."""
    user_msg = (
        f"{rubric['user_instruction']}\n\n"
        f"Тема: {topic}\n\n"
        "Верни только текст поста, без пояснений."
    )
    return await _call_llm(user_msg, max_tokens=500, temperature=0.9)


# ─── Выбор поста на сегодня ───────────────────────────────────────────────────

class PostPayload(TypedDict, total=False):
    rubric: str
    kind: RubricKind
    text: str
    beat: Optional[dict]
    topic: Optional[str]  # если текстовый пост — какую тему взяли, чтобы пометить после публикации
    weekday: int  # чтобы при regen знать какую рубрику повторить


async def generate_today_post(weekday: Optional[int] = None) -> PostPayload:
    """Полный pipeline: выбор рубрики → материал → LLM-текст. Возвращает payload для preview."""
    if weekday is None:
        weekday = datetime.now().weekday()
    rubric = RUBRIC_SCHEDULE[weekday]

    if rubric["kind"] == "audio":
        beat = pick_audio_beat()
        if beat is None:
            logger.info("Аудио-кандидатов нет, откат на текстовую рубрику вторника")
            rubric = RUBRIC_SCHEDULE[1]
        else:
            caption = await generate_caption(rubric, beat)
            return {"rubric": rubric["name"], "kind": "audio", "text": caption, "beat": beat, "weekday": weekday}

    topic = pick_text_topic(rubric["section"])
    if topic is None:
        topic = "свободная рефлексия о работе над битами на этой неделе"
    text = await generate_text_post(rubric, topic)
    return {"rubric": rubric["name"], "kind": "text", "text": text, "beat": None, "topic": topic, "weekday": weekday}


# ─── CLI для dry-run ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import asyncio
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    async def _main():
        days = sys.argv[1:] if len(sys.argv) > 1 else ["today"]
        for day in days:
            if day == "today":
                wd = datetime.now().weekday()
            elif day == "all":
                for d in range(7):
                    print(f"\n{'='*70}\n{['Пн','Вт','Ср','Чт','Пт','Сб','Вс'][d]} — {RUBRIC_SCHEDULE[d]['name']}\n{'='*70}")
                    try:
                        p = await generate_today_post(weekday=d)
                        print(f"[{p['kind']}]{' beat: '+p['beat']['name'] if p.get('beat') else ''}\n")
                        print(p["text"])
                    except Exception as e:
                        print(f"ОШИБКА: {e}")
                return
            else:
                wd = int(day)
            p = await generate_today_post(weekday=wd)
            print(f"\n{['Пн','Вт','Ср','Чт','Пт','Сб','Вс'][wd]} — {p['rubric']} [{p['kind']}]")
            if p.get("beat"):
                print(f"бит: {p['beat']['name']}")
            print()
            print(p["text"])

    asyncio.run(_main())
