"""LLM-caller для caption'ов битов при upload.

После удаления автопостинга (2026-04-20) этот модуль — просто
thin LLM-wrapper с voice-skill как system prompt + anti-AI guardrails.
Используется из `beat_post_builder.build_tg_caption_async`.

Публичное API:
- `_call_llm(user_message, max_tokens, temperature) -> str`
- `get_system_prompt()` — кэшированное чтение `wiki/iiiplfiii_voice.md`
- `ANTI_AI_BLOCK` — констант-блок правил тона, инжектируется в user_msg
- `MODELS` — fallback-цепочка OpenRouter моделей
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

import httpx

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

logger = logging.getLogger(__name__)

HERE = Path(__file__).parent
# Источник tone-of-voice. В приоритете — копия в репо (для прода на Render),
# fallback — локальный Claude skill (удобно для сессий/разработки).
_SKILL_REPO = HERE / "wiki" / "iiiplfiii_voice.md"
_SKILL_LOCAL = Path.home() / ".claude" / "skills" / "iiiplfiii-voice" / "SKILL.md"
SKILL_PATH = _SKILL_REPO if _SKILL_REPO.exists() else _SKILL_LOCAL

MODELS = [
    "anthropic/claude-haiku-4.5",
    "openai/gpt-4o-mini",
    "openai/gpt-oss-120b:free",
]


# ─── System-prompt (voice-skill) ──────────────────────────────────────────────

_SYSTEM_PROMPT_CACHE: Optional[str] = None


def get_system_prompt() -> str:
    """Читает `wiki/iiiplfiii_voice.md` (или локальный skill) один раз, кэширует."""
    global _SYSTEM_PROMPT_CACHE
    if _SYSTEM_PROMPT_CACHE is None:
        if not SKILL_PATH.exists():
            raise FileNotFoundError(f"SKILL.md не найден: {SKILL_PATH}")
        _SYSTEM_PROMPT_CACHE = SKILL_PATH.read_text(encoding="utf-8")
    return _SYSTEM_PROMPT_CACHE


# ─── Anti-AI block для user-промпта caption'а ─────────────────────────────────
# LLM с одним system-prompt'ом (voice skill) всё равно даёт стерильные посты —
# следует правилам, но не копирует реальный тон. Два рычага в связке:
#   1. POSITIVE few-shot — LLM имитирует тон из примеров сильнее чем из правил.
#   2. NEGATIVE list — прямые запреты конкретных AI-штампов.
# Инжектируется в user_msg `beat_post_builder.build_tg_caption_async`.

ANTI_AI_BLOCK = (
    "\n=== КАК ПИСАТЬ (обязательно) ===\n"
    "Примеры реальных постов автора — копируй ТОН, не слова:\n"
    '— "Готовим бомбу, почти все уже, скоро взорвется!"\n'
    '— "просто пушечный биток получился! я с него кайфую)))"\n'
    '— "Лютый детройт!"\n'
    '— "мощный, плотный басок, качает"\n'
    '— "вайб в мелодии есть — значит бит будет"\n'
    '— "сел поработал — к вечеру уже готовый мастер лежит"\n\n'
    "Короткие рваные фразы. От первого лица. Можно начинать с маленькой буквы. "
    "Скобки-улыбки )))) и многоточие ... — норма, не пытайся \"чисто\" писать.\n"
    "Живой битмейкер черканул 1-3 строки за 30 секунд и отправил. Не эссе.\n"
    "Максимум 1 эмодзи на пост (часто — 0). Только из whitelist voice-skill.\n\n"
    "=== ЗАПРЕЩЕНО (AI-штампы, отсекаются автоматом) ===\n"
    '❌ "почувствуй / ощути энергию / атмосферу / мощь / вайб"\n'
    '❌ "этот бит идеален для X", "создан для тех, кто Y", "подойдёт чтобы Z"\n'
    '❌ "фирменный звук", "мощная атмосфера", "уникальный саунд", "неповторимый"\n'
    '❌ "открой для себя", "погрузись в", "представляю вам", "встречайте"\n'
    '❌ балансные тройки "тёмный, жёсткий и атмосферный" / "808, драмы и мелодия"\n'
    '❌ обращения в начале: "друзья", "ребята", "привет всем", "добро пожаловать"\n'
    '❌ "в этом треке", "перед вами", "новый эксклюзив"\n'
    '❌ точка после односложного хука ("Лютый детройт." нет → "Лютый детройт!" да)\n'
    '❌ гладкая маркетинг-риторика — автор пишет как человек, не как лендинг\n\n'
    "=== СРАВНЕНИЯ С КОЛЛЕГАМИ — ЗАПРЕЩЕНО ===\n"
    'Автор прямо попросил НИКОГДА не сравнивать себя с другими битмейкерами.\n'
    '❌ "многие дропают мусор, а я качество" / "в отличие от других"\n'
    '❌ "большинство делает X, я делаю Y" / "не как все"\n'
    "Можно: чисто про себя — «я делаю так и вот почему качает».\n\n"
    "=== КЛЮЧЕВОЕ СЛОВО ТОНА: «качает» ===\n"
    'Главная метрика автора — «качает / не качает». Заменяет «классно/круто».\n'
    "=== КОНЕЦ ПРАВИЛ ПО ТОНУ ===\n"
)


# ─── LLM call ─────────────────────────────────────────────────────────────────

async def _call_llm(user_message: str, max_tokens: int = 400,
                    temperature: float = 0.85) -> str:
    """Вызывает OpenRouter с voice-skill как system prompt.

    Fallback-цепочка моделей из MODELS. frequency_penalty + presence_penalty
    штрафуют повторяющиеся клише ниши ("мощный", "атмосферный").
    """
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
