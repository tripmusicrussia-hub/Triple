"""Conversational agent для пользователей @iiiplfiii в ЛС бота.

Отвечает на вопросы про биты/канал/жанр/лицензии в голосе автора.
Единственный «живой» тул — catalog_search (переиспользуется из agent_router).
FAQ/about/tone — подшиваются в system-prompt, LLM отвечает напрямую.

Протокол: LLM либо возвращает JSON {"tool":"catalog_search","args":{...}},
либо plain text в tone-of-voice.
"""
from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

HERE = Path(__file__).parent
VOICE_MD = HERE / "wiki" / "iiiplfiii_voice.md"
FAQ_MD = HERE / "wiki" / "faq.md"

_MODELS = [
    "anthropic/claude-haiku-4.5",
    "openai/gpt-4o-mini",
    "openai/gpt-oss-120b:free",
]

_SYSTEM_CACHE: str | None = None


def _build_system() -> str:
    voice = VOICE_MD.read_text(encoding="utf-8") if VOICE_MD.exists() else ""
    faq = FAQ_MD.read_text(encoding="utf-8") if FAQ_MD.exists() else ""
    return f"""Ты — виртуальный ассистент автора канала @iiiplfiii (type beats для hard trap сцены Memphis/Detroit).
Отвечаешь пользователям в ЛС телеграм-бота. Короткие сообщения: 1-4 строки, разговорный русский, в голосе автора.

=== TONE-OF-VOICE АВТОРА ===
{voice}

=== FAQ / ФАКТЫ ===
{faq}

=== ДОСТУПНЫЙ ТУЛ ===
Если пользователь ХОЧЕТ НАЙТИ БИТЫ (упоминает BPM, ключ, артиста, «найди», «покажи», «есть что в…», «посоветуй бит»),
верни ТОЛЬКО JSON, без текста:
{{"tool":"catalog_search","args":{{"bpm_min":<int|null>,"bpm_max":<int|null>,"key":<str|null>,"artist":<str|null>,"limit":5}}}}

Извлекай параметры из текста. Если не уверен — null. «140+» → bpm_min=140. «вокруг 155» → bpm_min=150, bpm_max=160.
Артиста не переводи (nardo wick → "nardo wick"). Ключ как есть (Am, G#m).

=== ЕСЛИ ЗАПРОС НЕ ПРО ПОИСК ===
Отвечай текстом в голосе автора. НЕ возвращай JSON.
- Вопросы про лицензию/покупку/связь → используй факты из FAQ.
- Вопросы про жанр/звук/плагины → отвечай кратко, не душни.
- Приветствия/смоллток → дружелюбно, коротко, без фальши.
- Если не знаешь ответа — скажи «напиши автору в ЛС: @iiiplfiii».
- НЕ выдумывай цены, даты, лицензии которых нет в FAQ.
- Не используй хэштеги, markdown (**, __), кавычки вокруг всего ответа."""


def _get_system() -> str:
    global _SYSTEM_CACHE
    if _SYSTEM_CACHE is None:
        _SYSTEM_CACHE = _build_system()
    return _SYSTEM_CACHE


async def _call_llm(user_text: str) -> str:
    api_key = os.getenv("OPENROUTER_KEY") or os.getenv("OPENROUTER_API_KEY", "")
    if not api_key:
        raise RuntimeError("OPENROUTER_KEY не задан")
    system = _get_system()
    last_err = None
    async with httpx.AsyncClient(timeout=30) as client:
        for model in _MODELS:
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
                        "max_tokens": 350,
                        "temperature": 0.7,
                        "messages": [
                            {"role": "system", "content": system},
                            {"role": "user", "content": user_text},
                        ],
                    },
                )
                j = resp.json()
                if "error" in j:
                    last_err = j["error"].get("message", str(j["error"]))
                    continue
                content = j["choices"][0]["message"].get("content") or ""
                if content.strip():
                    return content.strip()
            except Exception as e:
                last_err = str(e)
                logger.warning("user_agent LLM %s exception: %s", model, e)
    raise RuntimeError(f"LLM недоступен: {last_err}")


_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)

# Markdown-стриппер — LLM иногда лепит **bold** / __italic__ / ### заголовки
# несмотря на запрет в промпте. Telegram рендерит их криво без parse_mode.
_MD_BOLD = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)
_MD_ITAL = re.compile(r"(?<!_)__(.+?)__(?!_)", re.DOTALL)
_MD_HEADING = re.compile(r"^#{1,6}\s*", re.MULTILINE)
_MD_INLINE_CODE = re.compile(r"`([^`\n]+)`")


def _strip_markdown(text: str) -> str:
    text = _MD_BOLD.sub(r"\1", text)
    text = _MD_ITAL.sub(r"\1", text)
    text = _MD_HEADING.sub("", text)
    text = _MD_INLINE_CODE.sub(r"\1", text)
    return text


def _maybe_tool_call(raw: str) -> dict | None:
    """Если в ответе валидный JSON с tool=catalog_search — вернуть args."""
    m = _JSON_RE.search(raw.strip())
    if not m:
        return None
    try:
        data = json.loads(m.group(0))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    if data.get("tool") != "catalog_search":
        return None
    args = data.get("args") or {}
    return args if isinstance(args, dict) else {}


async def handle(user_text: str, user_id: int | None = None) -> str:
    """Приём свободного текста от пользователя → LLM → либо tool-call, либо ответ."""
    user_text = (user_text or "").strip()
    if not user_text:
        return ""
    try:
        raw = await _call_llm(user_text)
    except Exception as e:
        logger.warning("user_agent LLM failed: %s", e)
        return "Сорри, сейчас не отвечаю — напиши автору в ЛС: @iiiplfiii"

    tool_args = _maybe_tool_call(raw)
    if tool_args is not None:
        import agent_router
        try:
            return await agent_router.tool_catalog_search(**tool_args)
        except TypeError as e:
            logger.warning("user_agent: catalog_search bad args %s: %s", tool_args, e)
        except Exception as e:
            logger.exception("user_agent: catalog_search failed")
            return "🔍 Поиск временно недоступен — попробуй чуть позже или напиши @iiiplfiii"

    # Plain text ответ — снимаем markdown (LLM иногда игнорит инструкцию в промпте)
    text = _strip_markdown(raw.strip().strip("`").strip())
    return text


if __name__ == "__main__":
    import asyncio
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    async def _main():
        queries = sys.argv[1:] or [
            "привет",
            "как купить бит?",
            "в каком жанре работаешь?",
            "найди биты Am 140+",
            "покажи nardo wick",
            "сколько стоит эксклюзив?",
            "можно бесплатно взять для клипа?",
        ]
        for q in queries:
            print("=" * 60)
            print(f"Q: {q}")
            print("-" * 60)
            print(await handle(q))

    asyncio.run(_main())
