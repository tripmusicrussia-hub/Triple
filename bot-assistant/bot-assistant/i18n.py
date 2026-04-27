"""i18n MVP — минимальная локализация для критичных conversion strings.

Архитектура: detect язык юзера через `user.language_code` (TG передаёт IETF tag),
fallback на «en» для всех не-CIS юзеров. Override через /lang ru|en.

Persist в Supabase `bot_users.lang` column (ALTER TABLE ADD COLUMN lang text).
Если column missing — graceful fallback на in-memory cache.

Coverage: только critical conversion strings (welcome, buy carding, referral).
Cart UI / catalog captions / админ-команды — остаются на русском.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# In-memory cache user_id → lang (fallback если Supabase недоступен)
_user_lang_cache: dict[int, str] = {}

# Поддерживаемые языки. RU включает украинский/белорусский/казахский — близкие
# по контексту аудитории, для них русский UI понятен (vs английский могут не).
_LANG_RU_PREFIXES = ("ru", "uk", "be", "kk")


def detect_lang(language_code: str | None) -> str:
    """TG `user.language_code` → 'ru' или 'en' (default).

    Примеры:
        'ru'         → 'ru'
        'ru-RU'      → 'ru'
        'uk'         → 'ru' (украинцы понимают русский лучше английского)
        'be-BY'      → 'ru'
        'en'         → 'en'
        'en-US'      → 'en'
        'de'         → 'en' (default для не-RU)
        None         → 'en'
    """
    if not language_code:
        return "en"
    code = language_code.lower().strip()
    for prefix in _LANG_RU_PREFIXES:
        if code.startswith(prefix):
            return "ru"
    return "en"


def get_user_lang(user_id: int, language_code: str | None = None) -> str:
    """Возвращает язык для юзера. Priority:
    1. In-memory cache (set via /lang command или после Supabase load)
    2. Supabase bot_users.lang (если есть)
    3. detect_lang(language_code) — fallback по TG settings

    Не делает Supabase round-trip каждый раз — cache hit короткий путь.
    """
    cached = _user_lang_cache.get(user_id)
    if cached:
        return cached
    # Lazy Supabase fetch (медленно, но один раз — потом cache)
    try:
        import users_db
        supa_lang = _supa_get_lang(user_id)
        if supa_lang:
            _user_lang_cache[user_id] = supa_lang
            return supa_lang
    except Exception:
        pass  # graceful — упадём на detect_lang
    lang = detect_lang(language_code)
    _user_lang_cache[user_id] = lang
    return lang


def set_user_lang(user_id: int, lang: str) -> bool:
    """Override языка через /lang команду. Returns True если успешно сохранено
    в Supabase, False если только in-memory."""
    if lang not in ("ru", "en"):
        return False
    _user_lang_cache[user_id] = lang
    return _supa_set_lang(user_id, lang)


def _supa_get_lang(user_id: int) -> str | None:
    """SELECT lang FROM bot_users WHERE tg_id = user_id. None если column
    отсутствует или юзера нет."""
    try:
        import users_db
        client = users_db._get_supabase()
        if client is None:
            return None
        res = client.table(users_db._TABLE).select("lang").eq("tg_id", user_id).limit(1).execute()
        if not res.data:
            return None
        lang = (res.data[0].get("lang") or "").strip().lower()
        return lang if lang in ("ru", "en") else None
    except Exception:
        # Column не существует / network error / etc — graceful
        return None


def _supa_set_lang(user_id: int, lang: str) -> bool:
    """UPDATE bot_users SET lang = lang WHERE tg_id = user_id."""
    try:
        import users_db
        client = users_db._get_supabase()
        if client is None:
            return False
        client.table(users_db._TABLE).update({"lang": lang}).eq("tg_id", user_id).execute()
        return True
    except Exception as e:
        if "lang" in str(e).lower():
            logger.warning("i18n: bot_users.lang column missing, persist via in-memory only")
        else:
            logger.exception("i18n: supabase update failed")
        return False


# ── Translations ────────────────────────────────────────────────
# Минимальный coverage critical conversion strings. Add больше по мере
# необходимости (когда увидим первого US юзера в bot_users.source='yt').

_LANG_RU: dict[str, str] = {
    # Welcome (cmd_start первое сообщение для нового юзера)
    "welcome_first": (
        "Привет! 👋 Я бот битмейкера @iiiplfiii.\n\n"
        "Hard trap beats в стиле Memphis/Detroit/NOLA. Type beats под "
        "Kenny Muney, Future, Rob49, BigXThaPlug и др.\n\n"
        "Кликай меню — слушай биты, цепляй то что зайдёт."
    ),
    # Carding screen после deep-link ?start=buy_<id>
    "buy_carding_intro": "Выбери способ оплаты — или напиши @iiiplfiii для WAV/Unlimited/Exclusive:",
    # Referral notify (новому юзеру который пришёл по ref_<friend_id>)
    "ref_welcome_title": "🎁 Скидка -{pct}% от друга!",
    "ref_welcome_body": (
        "Тебе и тому кто тебя пригласил — по <b>-{pct}%</b> на любой бит "
        "(действует 30 дней).\n\n"
        "Открой бит → кнопка «🎁 Купить со скидкой» появится автоматом."
    ),
    "ref_open_random_btn": "🎧 Открыть случайный бит и применить -{pct}%",
    "ref_catalog_btn": "🎹 Каталог битов",
    # Friend notify (приглашающему когда новый юзер пришёл по его ссылке)
    "ref_friend_notify": (
        "🎁 <b>{name} пришёл по твоей ссылке!</b>\n\n"
        "Тебе бонус: <b>скидка -{pct}%</b> на любой бит из каталога "
        "(действует 30 дней).\n\n"
        "Открой любой бит — кнопка «🎁 Купить со скидкой» появится автоматом."
    ),
    # /lang command response
    "lang_switched": "✅ Язык переключён на русский.",
    "lang_invalid": "⚠️ Использование: /lang ru — или — /lang en",
}

_LANG_EN: dict[str, str] = {
    "welcome_first": (
        "Hey! 👋 I'm the bot for beatmaker @iiiplfiii.\n\n"
        "Hard trap beats — Memphis/Detroit/NOLA style. Type beats for "
        "Kenny Muney, Future, Rob49, BigXThaPlug & more.\n\n"
        "Tap the menu — listen, grab what hits."
    ),
    "buy_carding_intro": "Pick payment method — or DM @iiiplfiii for WAV/Unlimited/Exclusive:",
    "ref_welcome_title": "🎁 -{pct}% off from your friend!",
    "ref_welcome_body": (
        "You and the friend who invited you both get <b>-{pct}%</b> on any beat "
        "(valid for 30 days).\n\n"
        "Open any beat → \"🎁 Buy with discount\" button shows up automatically."
    ),
    "ref_open_random_btn": "🎧 Open random beat & apply -{pct}%",
    "ref_catalog_btn": "🎹 Beats catalog",
    "ref_friend_notify": (
        "🎁 <b>{name} came in via your link!</b>\n\n"
        "Your bonus: <b>-{pct}% discount</b> on any beat from the catalog "
        "(valid for 30 days).\n\n"
        "Open any beat — \"🎁 Buy with discount\" button shows up automatically."
    ),
    "lang_switched": "✅ Language switched to English.",
    "lang_invalid": "⚠️ Usage: /lang en — or — /lang ru",
}


def t(key: str, lang: str = "ru", **kwargs: Any) -> str:
    """Возвращает translated string. Поддерживает .format(**kwargs).

    Если key missing для lang — fallback на RU (никогда на пустую строку).
    Если key missing совсем — возвращает `[?{key}]` для debug.
    """
    table = _LANG_EN if lang == "en" else _LANG_RU
    template = table.get(key) or _LANG_RU.get(key) or f"[?{key}]"
    if kwargs:
        try:
            return template.format(**kwargs)
        except (KeyError, IndexError):
            return template
    return template
