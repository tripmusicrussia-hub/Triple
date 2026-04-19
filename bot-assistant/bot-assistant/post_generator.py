"""
Генератор ежедневных постов в канал @iiiplfiii.

System-prompt для LLM читается из ~/.claude/skills/iiiplfiii-voice/SKILL.md
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
_SKILL_REPO = HERE / "wiki" / "iiiplfiii_voice.md"
_SKILL_LOCAL = Path.home() / ".claude" / "skills" / "iiiplfiii-voice" / "SKILL.md"
SKILL_PATH = _SKILL_REPO if _SKILL_REPO.exists() else _SKILL_LOCAL
POST_IDEAS_PATH = HERE / "wiki" / "post_ideas.md"

# Рубрики, которые получают scene-context (свежие новости сцены) как инъекцию
# к user-message LLM — автор может вплести инсайт, если оно в тему.
# «Что слушаю сейчас» — единственная рубрика, где sceнa-контекст напрямую в тему.
RUBRICS_WITH_SCENE_CONTEXT = {"Что слушаю сейчас"}

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
        "user_instruction": "Напиши подпись к биту для рубрики Memphis Monday. Memphis-вайб, хард, жёстко, должно качать. 2-4 строки + 2-4 тега.",
    },
    1: {
        "name": "Trick of the week",
        "kind": "text",
        "section": "Вт — Trick of the week",
        "user_instruction": (
            "Одна фишка в миксе от первого лица. Формат: «раньше делал X → "
            "теперь делаю Y → слышно здесь Z». 3-5 строк. Одна конкретная "
            "вещь, не мешай несколько. Используй ТОЛЬКО плагины из раздела "
            "'Битмейкерский домен'. Без оценок что «все делают не так» — "
            "только свой личный опыт."
        ),
    },
    2: {
        "name": "Flow moments",
        "kind": "text",
        "section": "Ср — Flow moments",
        "user_instruction": (
            "Короткая история про состояние потока — когда бит написался "
            "быстро (15-20 мин). От первого лица. 3-5 строк. Начни с "
            "триггера (прикольный луп / мелодия / басок) → как всё само "
            "сложилось → результат. Без пафоса, живо. Слово «качает» "
            "используй как критерий — «чувствуешь что качает, всё остальное "
            "доделывается само»."
        ),
    },
    3: {
        "name": "Что слушаю сейчас",
        "kind": "text",
        "section": "Чт — Что слушаю сейчас",
        "user_instruction": (
            "Один артист/трек который зацепил последнее время. От первого "
            "лица. 3-5 строк. Что именно зашло — звук / флоу / продакшен / "
            "вайб. БЕЗ СРАВНЕНИЙ с другими битмейкерами или «в отличие от». "
            "Только «слушаю X потому что умеет делать кач». Если есть "
            "scene-context — можно вплести, но только как наблюдение, "
            "не как обзор."
        ),
    },
    4: {
        "name": "Hard Friday",
        "kind": "audio",
        "section": "",
        "user_instruction": "Напиши подпись к пятничному биту — основной релиз недели. Жёсткий, пушечный, качает. 2-4 строки + 2-4 тега.",
    },
    5: {
        "name": "Звук из воздуха",
        "kind": "text",
        "section": "Сб — Звук из воздуха",
        "user_instruction": (
            "Про философию «музыка везде». Короткая история как обычный "
            "звук превратился в элемент бита — шум из окна / запись своего "
            "голоса / семпл из пластинки / что-то из TG-канала знакомого "
            "лупмейкера. 3-5 строк. Конкретный пример, не абстракция."
        ),
    },
    6: {
        "name": "Low & dark",
        "kind": "text",
        "section": "Вс — Low & dark",
        "user_instruction": (
            "Про низ/атмосферу/мрачность — подпись звука автора. 3-5 строк "
            "от первого лица. Конкретный приём с 808 / басом / тёмной "
            "атмосферой на этой неделе. Можно упомянуть мониторы Beyerdynamic "
            "DT 880 как setup. Главное — конкретика, не «мой фирменный звук»."
        ),
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


# ─── Anti-AI block для всех user-промптов ─────────────────────────────────────
# LLM с одним system-prompt'ом (voice skill) всё равно даёт стерильные посты —
# следует правилам, но не копирует реальный тон. Два рычага работают в связке:
#   1. POSITIVE few-shot — LLM имитирует тон из примеров сильнее чем из правил.
#   2. NEGATIVE list — прямые запреты конкретных AI-штампов, которые иначе
#      просачиваются несмотря на voice skill.
# Блок инжектируется в каждый user_msg (generate_caption / generate_text_post /
# build_tg_caption_async). При апдейте — только здесь, в одном месте.

ANTI_AI_BLOCK = (
    "\n=== КАК ПИСАТЬ (обязательно) ===\n"
    "Примеры реальных постов автора — копируй ТОН, не слова:\n"
    '— "Готовим бомбу, почти все уже, скоро взорвется!"\n'
    '— "просто пушечный биток получился! я с него кайфую)))"\n'
    '— "Лютый детройт!"\n'
    '— "сел поработал — к вечеру уже готовый мастер лежит"\n\n'
    "Короткие рваные фразы. От первого лица. Можно начинать с маленькой буквы. "
    "Скобки-улыбки )))) и многоточие ... — норма, не пытайся \"чисто\" писать.\n"
    "Живой битмейкер черканул 2-3 строки за 30 секунд и отправил. Не эссе.\n\n"
    "=== ЗАПРЕЩЕНО (AI-штампы, отсекаются автоматом) ===\n"
    '❌ "почувствуй / ощути энергию / атмосферу / мощь / вайб"\n'
    '❌ "этот бит идеален для X", "создан для тех, кто Y", "подойдёт чтобы Z"\n'
    '❌ "фирменный звук", "мощная атмосфера", "уникальный саунд", "неповторимый"\n'
    '❌ "открой для себя", "погрузись в", "представляю вам", "встречайте"\n'
    '❌ балансные тройки "тёмный, жёсткий и атмосферный" / "808, драмы и мелодия"\n'
    '❌ обращения в начале: "друзья", "ребята", "привет всем", "добро пожаловать"\n'
    '❌ "в этом треке", "перед вами", "новый эксклюзив"\n'
    '❌ точка после односложного хука ("Лютый детройт." нет → "Лютый детройт!" да)\n'
    '❌ гладкая маркетинг-риторика вообще — автор пишет как человек, не как лендинг\n\n'
    "=== СРАВНЕНИЯ С КОЛЛЕГАМИ ПО НИШЕ — ЗАПРЕЩЕНО ===\n"
    'Автор прямо попросил НИКОГДА не сравнивать себя с другими битмейкерами.\n'
    '❌ "многие битмари дропают мусор, а я качество"\n'
    '❌ "в отличие от других продюсеров"\n'
    '❌ "большинство делает X, я делаю Y"\n'
    '❌ "не как все", "не как другие"\n'
    '❌ любой негатив про коллег по нише, даже обобщённый\n'
    "Можно: чисто про себя — «я делаю так и вот почему работает».\n\n"
    "=== КЛЮЧЕВОЕ СЛОВО ТОНА: «качает» ===\n"
    'Главная метрика оценки для автора — «качает / не качает». Это заменяет\n'
    '«классно / круто / отлично». Использовать естественно, не в каждом посте\n'
    'подряд, но как ориентир что считается хорошим.\n\n'
    "=== КОНЕЦ ПРАВИЛ ПО ТОНУ ===\n"
)


# ─── Темы для текстовых рубрик ────────────────────────────────────────────────

def _read_ideas() -> str:
    return POST_IDEAS_PATH.read_text(encoding="utf-8")


def _write_ideas(text: str) -> None:
    POST_IDEAS_PATH.write_text(text, encoding="utf-8")


def _pick_evergreen_topic(section: str) -> Optional[str]:
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


def pick_text_topic(section: str) -> Optional[str]:
    """Возвращает тему для текстового поста из evergreen-бэклога post_ideas.md.

    Свежие тренды/новости НЕ заменяют тему поста — они инжектятся как context
    в user-message LLM для рубрик из RUBRICS_WITH_SCENE_CONTEXT.
    Канал = личный журнал автора, а не RSS-бот.
    """
    return _pick_evergreen_topic(section)


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
                        # frequency_penalty штрафует повторы конкретных слов
                        # ("мощный", "атмосферный", "лютый"...), presence_penalty
                        # стимулирует лексическое разнообразие. Без них LLM
                        # на малой температуре уходит в любимые клише ниши.
                        "frequency_penalty": 0.6,
                        "presence_penalty": 0.4,
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


# ─── Automoderation ───────────────────────────────────────────────────────────

TG_LIMIT_AUDIO = 1024
TG_LIMIT_TEXT = 4000

EMOJI_WHITELIST = set("🎧🎵🎹🔥⛓️🗡️🥶⚫🌒⚡💬🔗📥❤️📝🎛🎶")

FORBIDDEN_PLUGINS = re.compile(
    r"\b(sytrus|nexus\s*2|massive\s*classic|3xosc|fl\s*slayer|harmor\s*default)\b",
    re.IGNORECASE,
)

EMOJI_RE = re.compile(
    "[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F000-\U0001F2FF]",
)


def validate_caption(text: str, kind: RubricKind) -> tuple[bool, list[str]]:
    """Проверки перед публикацией. Возвращает (ok, issues)."""
    issues: list[str] = []
    limit = TG_LIMIT_AUDIO if kind == "audio" else TG_LIMIT_TEXT
    if len(text) > limit:
        issues.append(f"длина {len(text)} > {limit}")
    if not text.strip():
        issues.append("пустой текст")

    m = FORBIDDEN_PLUGINS.search(text)
    if m:
        issues.append(f"запрещённый плагин: {m.group(0)}")

    emojis = EMOJI_RE.findall(text)
    bad = [e for e in emojis if e not in EMOJI_WHITELIST]
    if bad:
        issues.append(f"эмодзи вне whitelist: {''.join(set(bad))}")
    if len(emojis) > 3:
        issues.append(f"слишком много эмодзи ({len(emojis)}, max 3)")

    bullets = sum(1 for line in text.splitlines() if line.strip().startswith(("- ", "* ", "• ")))
    if bullets >= 3:
        issues.append(f"bullet-простыня ({bullets} пунктов)")

    return (len(issues) == 0, issues)


async def _generate_with_retry(
    generator,
    kind: RubricKind,
    max_attempts: int = 3,
) -> tuple[str, list[str]]:
    """Вызывает генератор, пока не пройдёт validate_caption или не закончатся попытки.
    Возвращает (text, issues) — пустой issues означает ok.
    """
    last_text = ""
    last_issues: list[str] = []
    for attempt in range(1, max_attempts + 1):
        text = await generator()
        ok, issues = validate_caption(text, kind)
        if ok:
            if attempt > 1:
                logger.info("automod: passed on attempt %d", attempt)
            return text, []
        logger.warning("automod attempt %d failed: %s", attempt, issues)
        last_text, last_issues = text, issues
    return last_text, last_issues


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
        f"=== КОНЕЦ ===\n"
        f"{ANTI_AI_BLOCK}\n"
        "Не выдумывай BPM/key если их нет. Верни только текст подписи, без пояснений."
    )
    return await _call_llm(user_msg, max_tokens=350, temperature=0.85)


async def generate_text_post(rubric: Rubric, topic: str) -> str:
    """Текстовый пост на заданную тему.

    Для рубрик из RUBRICS_WITH_SCENE_CONTEXT в user-message добавляется
    свежий scene-context (релизы/обсуждения сцены) как опциональный инсайт —
    автор может вплести, если ложится в тему, но не обязан.
    """
    scene_block = ""
    if rubric["name"] in RUBRICS_WITH_SCENE_CONTEXT:
        try:
            import trends
            ctx = trends.get_scene_context()
            if ctx:
                scene_block = (
                    "\n\n=== СВЕЖИЕ СОБЫТИЯ СЦЕНЫ (опционально, только если в тему) ===\n"
                    f"{ctx}\n"
                    "=== КОНЕЦ ===\n\n"
                    "Если что-то из этого органично ложится в твой пост — можешь "
                    "вплести как наблюдение ИЗ СВОЕГО УГЛА (битмейкер услышал → попробовал → выводы). "
                    "НЕ пересказывай новость. НЕ промоти других артистов. "
                    "Если не ложится — просто пиши по теме, игнорируй этот блок."
                )
        except Exception as e:
            logger.warning("scene context недоступен: %s", e)

    user_msg = (
        f"{rubric['user_instruction']}\n\n"
        f"Тема: {topic}"
        f"{scene_block}\n"
        f"{ANTI_AI_BLOCK}\n"
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
    issues: list[str]  # automod: список проблем если не прошла проверку после max_attempts


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
            caption, issues = await _generate_with_retry(
                lambda: generate_caption(rubric, beat), "audio"
            )
            return {"rubric": rubric["name"], "kind": "audio", "text": caption, "beat": beat, "weekday": weekday, "issues": issues}

    topic = pick_text_topic(rubric["section"])
    if topic is None:
        topic = "свободная рефлексия о работе над битами на этой неделе"
    text, issues = await _generate_with_retry(
        lambda: generate_text_post(rubric, topic), "text"
    )
    return {"rubric": rubric["name"], "kind": "text", "text": text, "beat": None, "topic": topic, "weekday": weekday, "issues": issues}


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
