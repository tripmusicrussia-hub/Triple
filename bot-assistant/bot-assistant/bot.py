import asyncio
import html
import logging
import re
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
import random
import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

MSK_TZ = ZoneInfo("Europe/Moscow")
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, InputFile, LabeledPrice
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, PreCheckoutQueryHandler, ChatMemberHandler, filters, ContextTypes
)
from telegram.error import TelegramError
from config import (
    BOT_TOKEN, CHANNEL_ID, CHANNEL_LINK, SAMPLE_PACK_PATH, SAMPLE_PACK_FILE_ID,
    WELCOME_TEXT, CATALOG_INTRO, ADMIN_ID, CHANNEL_POST_HOUR,
    YOOKASSA_PROVIDER_TOKEN,
)
import beats_db
import beat_post_builder
import users_db
import post_generator
import licensing
import sales
import cryptobot
import uuid
import io

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USERS_FILE = os.path.join(BASE_DIR, "users_data.json")
HEARTBEAT_FILE = os.path.join(BASE_DIR, "heartbeat.txt")

subscribed_users = set()
users_received_pack = set()
all_users = {}
user_favorites = {}
user_history = {}
bulk_add_mode = {}
batch_stats = {"added": 0, "skipped": 0}
batch_report_task = None  # текущий таймер отчёта
beat_plays = {}
beat_plays_users = {}

# Single-audio playback: храним id последнего bit-аудио сообщения для каждого
# юзера. При следующем send_beat (Next/Random) удаляем предыдущий аудио →
# в чате юзера всегда **один** играющий бит, не накапливается лента.
# In-memory only — после redeploy state потеряется (юзеры начнут заново).
last_bit_audio_msg: dict[int, int] = {}

# Re-marketing: отслеживаем какие биты юзер посмотрел но не купил.
# Через 24-48 часов шлём ОДИН reminder с CTA. Структура:
# {(user_id, beat_id): { "ts": float, "name": str, "reminded": bool }}
# Persist в JSON чтобы переживать redeploy. Опт-аут через /stop_reminders.
pending_reminders: dict[str, dict] = {}
PENDING_REMINDERS_PATH = os.path.join(BASE_DIR, "pending_reminders.json")
# Юзеры которые опт-аутнулись из reminders (не шлём им)
reminders_optout: set[int] = set()
REMINDERS_OPTOUT_PATH = os.path.join(BASE_DIR, "reminders_optout.json")


def _reminder_key(user_id: int, beat_id: int) -> str:
    """JSON-keys должны быть строками — encode tuple."""
    return f"{user_id}:{beat_id}"


def _load_reminders_state() -> None:
    """Восстановить pending_reminders + opt-out на startup."""
    if os.path.exists(PENDING_REMINDERS_PATH):
        try:
            with open(PENDING_REMINDERS_PATH, encoding="utf-8") as f:
                pending_reminders.update(json.load(f))
        except Exception:
            logger.exception("reminders: load pending failed")
    if os.path.exists(REMINDERS_OPTOUT_PATH):
        try:
            with open(REMINDERS_OPTOUT_PATH, encoding="utf-8") as f:
                reminders_optout.update(json.load(f))
        except Exception:
            logger.exception("reminders: load optout failed")


def _save_reminders() -> None:
    try:
        with open(PENDING_REMINDERS_PATH, "w", encoding="utf-8") as f:
            json.dump(pending_reminders, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception("reminders: save pending failed")


def _save_optout() -> None:
    try:
        with open(REMINDERS_OPTOUT_PATH, "w", encoding="utf-8") as f:
            json.dump(sorted(reminders_optout), f)
    except Exception:
        logger.exception("reminders: save optout failed")


def track_bit_view(user_id: int, beat: dict) -> None:
    """Юзер посмотрел preview бита — записываем для re-marketing.
    Вызывается из send_beat / deep-link buy_/prod_ handlers.
    Skip если юзер опт-аутнулся."""
    if user_id in reminders_optout:
        return
    if user_id == ADMIN_ID:
        return  # сам себе reminders не шлём
    bid = beat.get("id")
    name = beat.get("name") or "?"
    if not bid:
        return
    key = _reminder_key(user_id, bid)
    pending_reminders[key] = {
        "ts": time.time(),
        "name": name,
        "reminded": False,
    }
    _save_reminders()


def mark_bit_purchased(user_id: int, beat_id: int) -> None:
    """Юзер купил бит — удаляем из pending (reminder уже не нужен) +
    убираем из корзины (если был — куплен, нет смысла держать)."""
    key = _reminder_key(user_id, beat_id)
    if key in pending_reminders:
        pending_reminders.pop(key, None)
        _save_reminders()
    _cart_remove(user_id, beat_id)

# (Автопостинг текстовых рубрик удалён 2026-04-20 — автор сам пишет посты.
# Остаётся только upload-flow битов, превью которого через pending_uploads
# в памяти — при redeploy файлы mp3/video всё равно пропадают.)

# Общий helper для user-facing ошибок. Полный exception должен быть уже
# залогирован через logger.exception() выше по стеку — юзеру показываем
# generic сообщение без технических деталей, чтобы API-URL / stack trace /
# credentials не утекли в чат.
USER_ERROR_FALLBACK_CONTACT = "@iiiplfiii"


async def _nav_reply(query, text: str, reply_markup=None, parse_mode=None):
    """Навигационный reply — редактирует текущее сообщение вместо создания нового.

    Используется в callback-handler'ах для меню / фильтров / админки, чтобы
    чат юзера не засорялся десятком одинаковых сообщений при навигации.
    Если исходное сообщение — media (audio/photo/document), edit_text упадёт
    с BadRequest → fallback на обычный reply_text.
    """
    try:
        await query.message.edit_text(
            text, reply_markup=reply_markup, parse_mode=parse_mode,
            disable_web_page_preview=True,
        )
    except Exception:
        # Media message или неизменяемое — fallback на новое сообщение.
        await query.message.reply_text(
            text, reply_markup=reply_markup, parse_mode=parse_mode,
            disable_web_page_preview=True,
        )


def _user_error_msg(short_hint: str = "") -> str:
    """Дружелюбный generic текст для юзера.

    Usage: `logger.exception("...")` → `reply_text(_user_error_msg("оплата"))`.
    Примеры hint: "оплата", "поиск", "генерация". Если hint пуст — общий текст.
    """
    if short_hint:
        return (
            f"⚠️ {short_hint.capitalize()} временно недоступна. "
            f"Попробуй ещё раз через минуту или напиши {USER_ERROR_FALLBACK_CONTACT}."
        )
    return (
        f"⚠️ Что-то сломалось. Попробуй ещё раз или напиши {USER_ERROR_FALLBACK_CONTACT}."
    )


# Превью upload-флоу (новый бит от админа) — НЕ персистим: mp3/video/thumb
# живут в temp_uploads/ на локальном диске, при redeploy они пропадут, так
# что восстанавливать бесполезно.
pending_uploads: dict[str, dict] = {}

# Превью upload-флоу для drum kit / sample pack / loop pack (zip от админа).
# Здесь file_id ссылается на TG-хранилище (переживает redeploy), поэтому
# pending_products можно безопасно персистить на диск и восстанавливать.
pending_products: dict[str, dict] = {}
PENDING_PRODUCTS_PATH = os.path.join(BASE_DIR, "pending_products.json")

# YooKassa payments ждущие webhook-подтверждения. Ключ — payment_id
# (UUID от YooKassa). Значение — dict с user_id / beat_id / type / amount.
# При webhook ищем запись здесь → доставляем товар. Persist для переживания
# redeploy'ев, fallback polling на stale записи.
pending_yk_payments: dict[str, dict] = {}
PENDING_YK_PAYMENTS_PATH = os.path.join(BASE_DIR, "pending_yk_payments.json")

# Idempotency guard: payment_id'ы которые мы уже доставили. Защита от replay'а
# (webhook retry от YooKassa, двойной приход, злоумышленник дёрнул старый UUID).
# Persistent — переживает redeploy. Without this, любое знание успешного UUID'а
# позволяет бесконечно триггерить delivery через webhook.
delivered_yk_payments: set[str] = set()
DELIVERED_YK_PAYMENTS_PATH = os.path.join(BASE_DIR, "delivered_yk_payments.json")

# In-flight guard: payment_id'ы для которых delivery сейчас идёт. Защищает от
# race между webhook (один поток) и fallback polling (другой corutine), когда
# оба прошли delivered-check одновременно и до mark'а в delivered. In-memory
# only — на crash автоматически сбросится, можно будет retry'нуть.
in_flight_yk: set[str] = set()

# Guard от множественных кликов на RUB-кнопку. Если юзер 3 раза быстро
# тапнул «💳 Оплатить» — без этой защиты создастся 3 pending payment в YK
# и 3 раза отправится «Сведение треков — 5000₽» в ЛС. Ключ — user_id,
# очищается после создания payment'а или ошибки.
_yk_creating_payment: set[int] = set()

# Mutex для on-demand Shorts builder (callback `make_shorts:<token>`).
# ffmpeg + YT upload жрёт ~300MB RAM на Render free 512MB. Если админ
# дважды кликнул на кнопку → две параллельных сборки → OOM. Set хранит
# token'ы которые сейчас строятся; повторный клик отменяется alert'ом.
_building_shorts: set[str] = set()

# Bundle FSM state: user_id → {"anchor": int, "selected": list[int], "page": int}.
# In-memory only — короткий FSM, потеря при redeploy = юзер кликает «🎁» снова.
# anchor = бит из карточки которой юзер кликнул «🎁 3 бита». selected = ровно
# 2 других бита для bundle. page — пагинация по каталогу (10 битов на страницу).
bundle_selection: dict[int, dict] = {}
BUNDLE_TOTAL = 3        # сколько битов в bundle (anchor + selected)
BUNDLE_PAGE_SIZE = 10   # битов на странице picker'а

# Bundle cart: персистентная корзина — юзер слушает биты в каталоге, кликает
# «🛒 + В корзину» по тем что нравятся, потом в /cart покупает 3-pack за 4500₽.
# Persist чтобы переживать redeploy (юзер собирал корзину 30 мин — обидно потерять).
# Format: { "<user_id>": [<beat_id>, <beat_id>, ...] } (порядок добавления)
bundle_cart: dict[str, list[int]] = {}
BUNDLE_CART_PATH = os.path.join(BASE_DIR, "bundle_carts.json")
BUNDLE_CART_MAX = 10  # защита от abuse — больше 10 в корзине не нужно


def _load_bundle_carts() -> None:
    if os.path.exists(BUNDLE_CART_PATH):
        try:
            with open(BUNDLE_CART_PATH, encoding="utf-8") as f:
                bundle_cart.update(json.load(f))
        except Exception:
            logger.exception("bundle_cart: load failed")


def _save_bundle_carts() -> None:
    try:
        with open(BUNDLE_CART_PATH, "w", encoding="utf-8") as f:
            json.dump(bundle_cart, f, ensure_ascii=False, indent=2)
        try:
            import git_autopush
            git_autopush.mark_dirty(BUNDLE_CART_PATH)
        except Exception:
            pass
    except Exception:
        logger.exception("bundle_cart: save failed")


def _cart_get(user_id: int) -> list[int]:
    """Возвращает копию корзины юзера (чтобы случайно не мутировать через ref)."""
    return list(bundle_cart.get(str(user_id), []))


def _cart_add(user_id: int, beat_id: int) -> tuple[bool, str]:
    """Добавляет бит в корзину. Возвращает (ok, message_for_user)."""
    key = str(user_id)
    cur = list(bundle_cart.get(key, []))
    if beat_id in cur:
        return False, "уже в корзине"
    if len(cur) >= BUNDLE_CART_MAX:
        return False, f"корзина полная ({BUNDLE_CART_MAX})"
    cur.append(beat_id)
    bundle_cart[key] = cur
    _save_bundle_carts()
    return True, f"добавлен ({len(cur)}/{BUNDLE_TOTAL})"


def _cart_remove(user_id: int, beat_id: int) -> bool:
    """Удаляет бит из корзины. Возвращает True если был."""
    key = str(user_id)
    cur = list(bundle_cart.get(key, []))
    if beat_id not in cur:
        return False
    cur.remove(beat_id)
    if cur:
        bundle_cart[key] = cur
    else:
        bundle_cart.pop(key, None)
    _save_bundle_carts()
    return True


def _cart_clear(user_id: int) -> None:
    if str(user_id) in bundle_cart:
        bundle_cart.pop(str(user_id), None)
        _save_bundle_carts()

# Lock на все mutation'ы pending / delivered / in_flight. Lazy-init потому что
# asyncio.Lock() требует running loop (в старых Python). Создаётся в post_init.
_yk_lock: asyncio.Lock | None = None


def _get_yk_lock() -> asyncio.Lock:
    """Lazy-init lock. Вызывается только из coroutine'ов → running loop есть."""
    global _yk_lock
    if _yk_lock is None:
        _yk_lock = asyncio.Lock()
    return _yk_lock


def _atomic_write_json(path: str, data) -> None:
    """Atomic JSON write: tmp + os.replace. Защита от partial write при
    crash/redeploy во время dump'а — иначе получим битый файл и restore
    вернёт 0 записей после рестарта (потеря pending payments).

    os.replace() атомарен на POSIX и Windows (на Windows с 3.3+).
    """
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


async def _save_yk_pending(payment_id: str, data: dict) -> None:
    """Добавить pending YooKassa payment + persist. Вызывается из async
    handler'а после create_payment."""
    async with _get_yk_lock():
        pending_yk_payments[payment_id] = data
        try:
            _atomic_write_json(PENDING_YK_PAYMENTS_PATH, pending_yk_payments)
        except Exception:
            logger.exception("yk: persist pending_yk_payments failed (non-fatal)")


def _restore_yk_pending() -> int:
    """Вызывается в post_init (sync, concurrency невозможна — loop ещё
    не принимает events). Восстанавливаем pending payments с диска."""
    if not os.path.exists(PENDING_YK_PAYMENTS_PATH):
        return 0
    try:
        with open(PENDING_YK_PAYMENTS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        pending_yk_payments.update(data)
        return len(data)
    except Exception:
        logger.exception("yk: restore pending_yk_payments failed")
        return 0


def _restore_yk_delivered() -> int:
    """Восстанавливаем delivered-set с диска. БЕЗ этого после redeploy'а
    idempotency guard пустой → первый повторный webhook переиграет delivery."""
    if not os.path.exists(DELIVERED_YK_PAYMENTS_PATH):
        return 0
    try:
        with open(DELIVERED_YK_PAYMENTS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            delivered_yk_payments.update(data)
        return len(delivered_yk_payments)
    except Exception:
        logger.exception("yk: restore delivered_yk_payments failed")
        return 0


async def _drop_yk_pending(payment_id: str) -> None:
    """Удалить после успешной доставки / отмены."""
    async with _get_yk_lock():
        pending_yk_payments.pop(payment_id, None)
        try:
            _atomic_write_json(PENDING_YK_PAYMENTS_PATH, pending_yk_payments)
        except Exception:
            logger.exception("yk: persist after drop failed (non-fatal)")


async def _mark_yk_delivered(payment_id: str) -> None:
    """Пометить payment как доставленный + persist. Idempotency guard на replay.

    Важно: сохраняем список отсортированным (стабильный diff, легче читать
    руками если понадобится дебажить). Без upper bound — delivered-set
    растёт вечно, но 1 payment = ~40 байт, 100k платежей = 4MB JSON,
    восстанавливается за миллисекунды. Cleanup можно добавить позже
    (например дропать >90 дней) если станет проблемой.
    """
    async with _get_yk_lock():
        delivered_yk_payments.add(payment_id)
        try:
            _atomic_write_json(
                DELIVERED_YK_PAYMENTS_PATH,
                sorted(delivered_yk_payments),
            )
        except Exception:
            logger.exception("yk: persist delivered failed (non-fatal)")


def _persist_pending_products() -> None:
    """Сбрасывает pending_products на диск. Вызывается после append/pop.

    meta — это dataclass ProductMeta, сериализуем через __dict__.
    """
    try:
        serializable = {}
        for token, p in pending_products.items():
            meta = p.get("meta")
            serializable[token] = {
                "meta": meta.__dict__ if hasattr(meta, "__dict__") else meta,
                "file_id": p.get("file_id"),
                "file_unique_id": p.get("file_unique_id"),
                "file_size": p.get("file_size"),
                "file_name": p.get("file_name"),
                "mime_type": p.get("mime_type"),
            }
        with open(PENDING_PRODUCTS_PATH, "w", encoding="utf-8") as f:
            import json as _json
            _json.dump(serializable, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception("persist pending_products failed (non-fatal)")


def _restore_pending_products() -> int:
    """Вызывается при старте бота — восстанавливает pending_products с диска.
    Returns: сколько записей восстановлено.
    """
    if not os.path.exists(PENDING_PRODUCTS_PATH):
        return 0
    try:
        import json as _json
        import product_upload
        with open(PENDING_PRODUCTS_PATH, "r", encoding="utf-8") as f:
            data = _json.load(f)
        for token, p in data.items():
            meta_dict = p.get("meta") or {}
            pending_products[token] = {
                "meta": product_upload.ProductMeta(**meta_dict),
                "file_id": p.get("file_id"),
                "file_unique_id": p.get("file_unique_id"),
                "file_size": p.get("file_size"),
                "file_name": p.get("file_name"),
                "mime_type": p.get("mime_type"),
            }
        return len(pending_products)
    except Exception:
        logger.exception("restore pending_products failed")
        return 0


TEMP_UPLOAD_DIR = os.path.join(BASE_DIR, "temp_uploads")
os.makedirs(TEMP_UPLOAD_DIR, exist_ok=True)

ARTIST_TAGS = [
    "keyglock", "bossmandlow", "bossman", "obladaet", "nardowick",
    "kizaru", "scryptonite", "skryptonit", "konfuz", "bigbabytape",
    "bushidozho", "dababy", "gunna", "icewearvezzo", "jayfizzle",
    "jerk", "kennymuney", "poohshiesty", "rob49", "saintjhn",
    "ytbfatt", "alblack", "florida", "future", "southside",
]


# ── Сохранение / загрузка ─────────────────────────────────────

def save_users():
    """Local JSON snapshot — backup. Primary truth = Supabase bot_users table.

    Supabase пишется write-through в точках мутации (upsert_user,
    mark_sample_pack_received, set_favorites, set_subscribed). Local
    файл — fallback если Supabase недоступен при старте следующей
    сессии Render.
    """
    users_db.save_local(all_users, users_received_pack, subscribed_users, user_favorites)


def load_users():
    """Загружает users из Supabase (primary) → в in-memory кеш bot.py.
    При недоступности Supabase — fallback на local JSON.
    """
    global all_users, users_received_pack, subscribed_users, user_favorites
    try:
        all_users, users_received_pack, subscribed_users, user_favorites = users_db.load_to_memory()
    except Exception as e:
        logger.error("Load users error: %s", e)

def write_heartbeat():
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    except Exception:
        pass

def add_to_history(user_id, beat_id):
    if user_id not in user_history:
        user_history[user_id] = []
    user_history[user_id].append(beat_id)
    if len(user_history[user_id]) > 10:
        user_history[user_id].pop(0)

def get_history(user_id):
    return user_history.get(user_id, [])

def detect_content_type(text):
    if not text:
        return "beat"
    t = text.lower()
    if any(w in t for w in ["ремикс", "remix", "rmx"]):
        return "remix"
    if any(w in t for w in ["трек", "track", "песня", "song", "релиз", "release"]):
        return "track"
    return "beat"

def parse_beat_from_text(text, msg_id, channel_username):
    tags = beats_db.parse_tags_from_text(text)
    bpm = beats_db.parse_bpm_from_text(text)
    key = beats_db.parse_key_from_text(text)
    lines = [l.strip() for l in text.strip().split("\n")
             if l.strip() and not l.strip().startswith("#") and not l.strip().startswith("@")]
    name = lines[0][:60] if lines else "Beat #" + str(msg_id)
    post_url = "https://t.me/" + channel_username.lstrip("@") + "/" + str(msg_id)
    beat_id = abs(hash(channel_username + str(msg_id))) % 10000000
    return {
        "id": beat_id, "msg_id": msg_id, "name": name,
        "tags": tags, "post_url": post_url,
        "bpm": bpm or 0, "key": key or "-", "file_id": "",
        "content_type": detect_content_type(text),
    }

def try_add_beat(beat):
    if beat["id"] in beats_db.BEATS_BY_ID:
        return False
    fuid = beat.get("file_unique_id")
    if fuid:
        for b in beats_db.BEATS_CACHE:
            if b.get("file_unique_id") == fuid:
                return False
    for b in beats_db.BEATS_CACHE:
        if b["name"].strip().lower() == beat["name"].strip().lower():
            return False
    beats_db.BEATS_CACHE.append(beat)
    beats_db.BEATS_BY_ID[beat["id"]] = beat
    return True


# ── Клавиатуры ────────────────────────────────────────────────

def kb_subscribe():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("📢 Подписаться на канал", url=CHANNEL_LINK)
    ], [
        InlineKeyboardButton("✅ Я подписался!", callback_data="check_sub")
    ]])

def kb_main_menu(user_id: int | None = None):
    beats = len([b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == "beat"])
    tracks = len([b for b in beats_db.BEATS_CACHE if b.get("content_type") == "track"])
    remixes = len([b for b in beats_db.BEATS_CACHE if b.get("content_type") == "remix"])
    cart_n = len(_cart_get(user_id)) if user_id is not None else 0
    cart_label = (
        f"🛒 Корзина · {cart_n} ({licensing.PRICE_BUNDLE3_RUB}₽ за 3)"
        if cart_n else
        "🛒 Корзина (пусто)"
    )
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🎹 Биты ({beats})", callback_data="menu_beat")],
        [InlineKeyboardButton(f"🎤 Треки ({tracks})", callback_data="menu_track"),
         InlineKeyboardButton(f"🔀 Ремиксы ({remixes})", callback_data="menu_remix")],
        [InlineKeyboardButton("📦 Kits & Packs", callback_data="menu_products")],
        [InlineKeyboardButton(f"🎛 Сведение треков ({licensing.PRICE_MIX_RUB}₽)", callback_data="menu_mixing")],
        [InlineKeyboardButton(cart_label, callback_data="cart_show")],
        [InlineKeyboardButton("ℹ️ Услуги и цены", callback_data="menu_services")],
        # Quick-filter chips — быстрый доступ к популярным сценам / mood
        [InlineKeyboardButton("🔥 Hard", callback_data="qf_hard"),
         InlineKeyboardButton("🌃 Memphis", callback_data="qf_memphis"),
         InlineKeyboardButton("🏙 Detroit", callback_data="qf_detroit"),
         InlineKeyboardButton("🇷🇺 RU", callback_data="qf_ru")],
        [InlineKeyboardButton("⚡ 130+", callback_data="qf_bpm130"),
         InlineKeyboardButton("⚡ 140+", callback_data="qf_bpm140"),
         InlineKeyboardButton("⚡ 150+", callback_data="qf_bpm150"),
         InlineKeyboardButton("⚡ 160+", callback_data="qf_bpm160")],
        [InlineKeyboardButton("🎲 Случайный", callback_data="random_beat")],
        [InlineKeyboardButton("❤️ Избранное", callback_data="my_favorites"),
         InlineKeyboardButton("🔍 Поиск", callback_data="search_prompt")],
    ])


# ── Quick-filter chips predicates ─────────────────────────────
SCENE_TAGS = {
    "memphis": {"kennymuney", "keyglock", "bigmoochiegrape", "youngdolph",
                "poohshiesty", "moneybaggyo", "finesse2tymes", "three6mafia",
                "glorilla", "memphis"},
    "detroit": {"nardowick", "babytron", "teegrizzley", "detroit"},
    "ru":      {"obladaet", "kizaru", "skriptonit", "ogbuda", "platina",
                "slavamarlow", "bigbabytape", "mayot"},
}
HARD_TAGS = {"hard", "dark", "aggressive", "evil", "mean", "street"}

# In-memory state для pagination между callback'ами
user_search_state: dict[int, dict] = {}  # user_id → {'filter': str, 'page': int, 'results_ids': list[int]}
SEARCH_PAGE_SIZE = 8


def _filter_beats(filter_name: str) -> list[dict]:
    """Возвращает отфильтрованный список битов под quick-filter."""
    audio_only = [b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") != "non_audio"]
    if filter_name == "hard":
        return [b for b in audio_only
                if any(t.lower() in HARD_TAGS for t in b.get("tags", []))
                or any(h in b["name"].lower() for h in HARD_TAGS)]
    if filter_name in SCENE_TAGS:
        scene_set = SCENE_TAGS[filter_name]
        return [b for b in audio_only
                if any(t.lower() in scene_set for t in b.get("tags", []))]
    if filter_name == "bpm130":
        return [b for b in audio_only if (b.get("bpm") or 0) >= 130]
    if filter_name == "bpm140":
        return [b for b in audio_only if (b.get("bpm") or 0) >= 140]
    if filter_name == "bpm150":
        return [b for b in audio_only if (b.get("bpm") or 0) >= 150]
    if filter_name == "bpm160":
        return [b for b in audio_only if (b.get("bpm") or 0) >= 160]
    return []


def _kb_search_results(results: list[dict], filter_name: str, page: int) -> InlineKeyboardMarkup:
    """Рендерит страницу результатов с pagination."""
    total_pages = max(1, (len(results) + SEARCH_PAGE_SIZE - 1) // SEARCH_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * SEARCH_PAGE_SIZE
    chunk = results[start:start + SEARCH_PAGE_SIZE]

    rows = []
    for b in chunk:
        bpm = b.get("bpm") or "?"
        key = b.get("key_short") or (b.get("key", "")[:3] if b.get("key") else "")
        label = f"{b['name'][:32]}  {bpm}·{key}" if key else f"{b['name'][:38]}  {bpm}"
        rows.append([InlineKeyboardButton(label, callback_data=f"play_{b['id']}")])

    # Pagination row
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"sp_{filter_name}_{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"sp_{filter_name}_{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("◀️ Меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(rows)


async def do_quick_filter(bot, chat_id: int, user_id: int, filter_name: str,
                          page: int = 0, query=None):
    """Выполняет quick-filter поиск + показ paginated результатов.

    Если передан `query` (callback) — редактирует текущее сообщение через
    _nav_reply (чтоб чат не засорялся). Если нет (вызов из /команды) —
    отправляет новое сообщение.
    """
    results = _filter_beats(filter_name)
    user_search_state[user_id] = {"filter": filter_name, "page": page}
    title = {
        "hard": "🔥 Hard",
        "memphis": "🌃 Memphis",
        "detroit": "🏙 Detroit",
        "ru": "🇷🇺 RU сцена",
        "bpm130": "⚡ 130+ BPM",
        "bpm140": "⚡ 140+ BPM",
        "bpm150": "⚡ 150+ BPM",
        "bpm160": "⚡ 160+ BPM",
    }.get(filter_name, filter_name)
    if not results:
        text = f"{title}: пусто, попробуй другой фильтр"
        if query is not None:
            await _nav_reply(query, text, reply_markup=kb_main_menu())
        else:
            await bot.send_message(chat_id, text, reply_markup=kb_main_menu())
        return
    text = f"{title} — нашёл {len(results)} треков:"
    kb = _kb_search_results(results, filter_name, page)
    if query is not None:
        await _nav_reply(query, text, reply_markup=kb)
    else:
        await bot.send_message(chat_id, text, reply_markup=kb)

def kb_beats_menu():
    beats = len([b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == "beat"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎤 По артистам", callback_data="beats_by_artist")],
        [InlineKeyboardButton("🎲 Случайный (" + str(beats) + " всего)", callback_data="randcat_beat")],
        [InlineKeyboardButton("◀️ Главное меню", callback_data="main_menu")],
    ])

def kb_artists():
    items = [b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == "beat"]
    found = set()
    for b in items:
        for tag in b.get("tags", []):
            if tag in ARTIST_TAGS:
                found.add(tag)
    rows = []
    row = []
    for tag in sorted(found):
        count = len([b for b in items if tag in b.get("tags", [])])
        row.append(InlineKeyboardButton(tag + " (" + str(count) + ")", callback_data="cattag_beat_" + tag))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu_beat")])
    return InlineKeyboardMarkup(rows)

def kb_tracks_menu():
    tracks = len([b for b in beats_db.BEATS_CACHE if b.get("content_type") == "track"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎲 Случайный трек (" + str(tracks) + " всего)", callback_data="randcat_track")],
        [InlineKeyboardButton("◀️ Главное меню", callback_data="main_menu")],
    ])

def kb_remixes_menu():
    remixes = len([b for b in beats_db.BEATS_CACHE if b.get("content_type") == "remix"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎲 Случайный ремикс (" + str(remixes) + " всего)", callback_data="randcat_remix")],
        [InlineKeyboardButton("◀️ Главное меню", callback_data="main_menu")],
    ])

def kb_after_beat(beat_id, content_type="beat", user_id: int | None = None):
    """Клавиатура под битом в DM. Если user_id передан — cart-кнопка показывает
    статус (✅ В корзине / 🛒 + В корзину) и счётчик корзины.
    """
    back_map = {"beat": "menu_beat", "track": "menu_track", "remix": "menu_remix"}
    rows = [[InlineKeyboardButton("▶️ Следующий похожий", callback_data="next_" + str(beat_id))]]
    if content_type == "beat":
        rows.append([
            InlineKeyboardButton(f"⭐ {licensing.PRICE_MP3_STARS}", callback_data="buy_mp3_" + str(beat_id)),
            InlineKeyboardButton(f"💵 {licensing.PRICE_MP3_USDT:g} USDT", callback_data="buy_usdt_" + str(beat_id)),
        ])
        # RUB-кнопка всегда видима (для KYC YooKassa нужно показать цены в ₽).
        # Если token не задан — handler graceful: alert «активируется после
        # подтверждения YooKassa». Юзер видит цену, флоу понятен.
        rows.append([
            InlineKeyboardButton(
                f"💳 {licensing.PRICE_MP3_RUB}₽ (MIR/СБП)",
                callback_data="buy_rub_" + str(beat_id),
            ),
        ])
        # Cart-кнопка: статус зависит от текущей корзины юзера.
        if user_id is not None:
            cart = _cart_get(user_id)
            if beat_id in cart:
                cart_label = f"✅ В корзине ({len(cart)}/{BUNDLE_TOTAL}) → /cart"
                cart_cb = "cart_show"
            elif len(cart) >= BUNDLE_TOTAL:
                cart_label = f"🛒 Корзина полная ({len(cart)}) → /cart"
                cart_cb = "cart_show"
            else:
                cart_label = f"🛒 + В корзину ({len(cart)}/{BUNDLE_TOTAL})"
                cart_cb = f"cart_add_{beat_id}"
            rows.append([InlineKeyboardButton(cart_label, callback_data=cart_cb)])
        rows.append([
            InlineKeyboardButton(
                f"🎁 3 бита {licensing.PRICE_BUNDLE3_RUB}₽ (выгода 600₽)",
                callback_data="bundle_start_" + str(beat_id),
            ),
        ])
        rows.append([
            InlineKeyboardButton("💎 Exclusive ($500+)", callback_data="excl_" + str(beat_id)),
        ])
    rows.append([InlineKeyboardButton("❤️ В избранное", callback_data="fav_" + str(beat_id)),
                 InlineKeyboardButton("🎲 Случайный", callback_data="random_beat")])
    rows.append([InlineKeyboardButton("◀️ Меню", callback_data=back_map.get(content_type, "main_menu"))])
    return InlineKeyboardMarkup(rows)


def kb_channel_beat_buy(beat_id: int) -> InlineKeyboardMarkup:
    """Клавиатура под публикацией бита в канале: Stars + USDT + RUB + Exclusive.

    RUB-кнопка всегда видима (нужно для KYC YooKassa). Если token не задан —
    handler показывает alert «активируется после подтверждения».
    """
    rows = [
        [InlineKeyboardButton(f"⭐ MP3 · {licensing.PRICE_MP3_STARS}", callback_data="buy_mp3_" + str(beat_id)),
         InlineKeyboardButton(f"💵 MP3 · {licensing.PRICE_MP3_USDT:g} USDT", callback_data="buy_usdt_" + str(beat_id))],
        [InlineKeyboardButton(
            f"💳 MP3 · {licensing.PRICE_MP3_RUB}₽ (MIR/СБП)",
            callback_data="buy_rub_" + str(beat_id),
        )],
        # В канале не знаем user_id заранее — кнопка просто пробует add. Handler
        # покажет alert если в корзине / уже там / переполнена.
        [InlineKeyboardButton(
            f"🛒 + В корзину (3 за {licensing.PRICE_BUNDLE3_RUB}₽)",
            callback_data="cart_add_" + str(beat_id),
        )],
        [InlineKeyboardButton(
            f"🎁 3 бита {licensing.PRICE_BUNDLE3_RUB}₽ (выгода 600₽)",
            callback_data="bundle_start_" + str(beat_id),
        )],
        [InlineKeyboardButton("💎 WAV / Unlimited / Exclusive", callback_data="excl_" + str(beat_id))],
    ]
    return InlineKeyboardMarkup(rows)


# ── Bundle picker UI ─────────────────────────────────────────

def _bundle_eligible_beats(exclude_ids: set[int]) -> list[dict]:
    """Биты-кандидаты для bundle: только beat content_type, есть file_id, не из exclude_ids.

    Сортировка по id desc — последние загруженные сверху (свежее = интереснее).
    """
    out = []
    for b in beats_db.BEATS_CACHE:
        if b.get("content_type", "beat") != "beat":
            continue
        if not b.get("file_id"):
            continue
        if int(b.get("id", 0)) in exclude_ids:
            continue
        out.append(b)
    out.sort(key=lambda x: int(x.get("id", 0)), reverse=True)
    return out


def kb_bundle_picker(user_id: int) -> InlineKeyboardMarkup:
    """Picker для выбора `BUNDLE_TOTAL - 1` дополнительных битов к anchor'у.

    Каждый бит — кнопка с ✅/⬜ префиксом. Внизу: counter + Confirm/Cancel + nav.
    """
    state = bundle_selection.get(user_id) or {}
    anchor_id = int(state.get("anchor", 0))
    selected: list[int] = list(state.get("selected", []))
    page: int = int(state.get("page", 0))
    candidates = _bundle_eligible_beats(exclude_ids={anchor_id})

    start = page * BUNDLE_PAGE_SIZE
    end = start + BUNDLE_PAGE_SIZE
    page_beats = candidates[start:end]

    rows: list[list[InlineKeyboardButton]] = []
    need_more = max(0, (BUNDLE_TOTAL - 1) - len(selected))

    for b in page_beats:
        bid = int(b["id"])
        is_picked = bid in selected
        marker = "✅" if is_picked else "⬜"
        # Имя обрезаем до 36 символов чтобы кнопка влазила.
        name = (b.get("name") or "?")[:36]
        bpm = b.get("bpm") or "?"
        label = f"{marker} {name} · {bpm}"
        action = "bundle_unpick_" if is_picked else "bundle_pick_"
        rows.append([InlineKeyboardButton(label, callback_data=action + str(bid))])

    # Пагинация
    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Назад", callback_data="bundle_page_" + str(page - 1)))
    if end < len(candidates):
        nav_row.append(InlineKeyboardButton("Ещё ▶️", callback_data="bundle_page_" + str(page + 1)))
    if nav_row:
        rows.append(nav_row)

    # Footer: counter + confirm + cancel
    counter_label = f"Выбрано {len(selected)}/{BUNDLE_TOTAL - 1}"
    if need_more == 0:
        rows.append([InlineKeyboardButton(
            f"✅ Купить 3 бита · {licensing.PRICE_BUNDLE3_RUB}₽",
            callback_data="bundle_confirm",
        )])
    else:
        rows.append([InlineKeyboardButton(counter_label, callback_data="bundle_noop")])
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="bundle_cancel")])
    return InlineKeyboardMarkup(rows)


def kb_bundle_pay() -> InlineKeyboardMarkup:
    """3 кнопки оплаты для подтверждённого bundle: Stars / USDT / RUB."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⭐ {licensing.PRICE_BUNDLE3_STARS}", callback_data="bundle_pay_stars"),
         InlineKeyboardButton(f"💵 {licensing.PRICE_BUNDLE3_USDT:g} USDT", callback_data="bundle_pay_usdt")],
        [InlineKeyboardButton(f"💳 {licensing.PRICE_BUNDLE3_RUB}₽ (MIR/СБП)", callback_data="bundle_pay_rub")],
        [InlineKeyboardButton("❌ Отмена", callback_data="bundle_cancel")],
    ])


def kb_cart(user_id: int) -> InlineKeyboardMarkup:
    """UI корзины: список битов с ❌ удалить + footer (купить/добавить ещё/очистить)."""
    cart = _cart_get(user_id)
    rows: list[list[InlineKeyboardButton]] = []
    for bid in cart:
        b = beats_db.get_beat_by_id(bid) or {"name": f"id={bid}"}
        name = (b.get("name") or "?")[:30]
        rows.append([
            InlineKeyboardButton(f"🎵 {name}", callback_data=f"buy_mp3_{bid}"),
            InlineKeyboardButton("❌", callback_data=f"cart_remove_{bid}"),
        ])
    if not cart:
        rows.append([InlineKeyboardButton(
            "🎲 Найти биты для bundle", callback_data="random_beat",
        )])
    elif len(cart) >= BUNDLE_TOTAL:
        rows.append([InlineKeyboardButton(
            f"✅ Купить {BUNDLE_TOTAL} бит(ов) за {licensing.PRICE_BUNDLE3_RUB}₽",
            callback_data="cart_buy",
        )])
        rows.append([InlineKeyboardButton(
            "➕ Добавить ещё битов", callback_data="random_beat",
        )])
    else:
        need = BUNDLE_TOTAL - len(cart)
        rows.append([InlineKeyboardButton(
            f"➕ Добавь ещё {need} → bundle 4500₽", callback_data="random_beat",
        )])
    if cart:
        rows.append([InlineKeyboardButton("🗑 Очистить корзину", callback_data="cart_clear")])
    rows.append([InlineKeyboardButton("◀️ Меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(rows)


async def _send_cart_view(bot, user_id: int, edit_message=None) -> None:
    """Шлёт юзеру актуальный вид корзины. Если edit_message задан — пытается
    edit (для callback `cart_show` из main_menu); иначе reply_text.
    """
    cart = _cart_get(user_id)
    if not cart:
        text = (
            "🛒 <b>Корзина пуста</b>\n\n"
            f"Добавь любой бит через кнопку «🛒 + В корзину» → когда наберётся "
            f"{BUNDLE_TOTAL}, купишь все за <b>{licensing.PRICE_BUNDLE3_RUB}₽</b>\n"
            f"(вместо {BUNDLE_TOTAL * licensing.PRICE_MP3_RUB}₽ по одному, "
            f"экономия {BUNDLE_TOTAL * licensing.PRICE_MP3_RUB - licensing.PRICE_BUNDLE3_RUB}₽)"
        )
    else:
        names = []
        for bid in cart:
            b = beats_db.get_beat_by_id(bid)
            if b:
                bpm = b.get("bpm") or "?"
                names.append(f"• <b>{html.escape(b.get('name') or '?')}</b> · {bpm} BPM")
            else:
                names.append(f"• id={bid} (бит пропал из каталога)")
        names_block = "\n".join(names)
        if len(cart) >= BUNDLE_TOTAL:
            total_single = len(cart) * licensing.PRICE_MP3_RUB
            tail = (
                f"\n\n💰 <b>Купить {BUNDLE_TOTAL} = {licensing.PRICE_BUNDLE3_RUB}₽</b>\n"
                f"(по одному это {total_single}₽ за {len(cart)} штук)"
            )
        else:
            need = BUNDLE_TOTAL - len(cart)
            tail = f"\n\n👇 Добавь ещё <b>{need} бит(ов)</b> → bundle {licensing.PRICE_BUNDLE3_RUB}₽"
        text = (
            f"🛒 <b>Корзина · {len(cart)}/{BUNDLE_TOTAL}</b>\n\n"
            f"{names_block}{tail}"
        )
    kb = kb_cart(user_id)
    if edit_message is not None:
        try:
            await edit_message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            return
        except TelegramError:
            pass
    await bot.send_message(user_id, text, parse_mode="HTML", reply_markup=kb)


def _bundle_anchor_and_selected(user_id: int) -> tuple[dict, list[dict]] | None:
    """Возвращает (anchor_beat, selected_beats[2]) или None если state невалидный."""
    state = bundle_selection.get(user_id)
    if not state:
        return None
    anchor_id = int(state.get("anchor", 0))
    selected_ids = [int(x) for x in state.get("selected", [])]
    if len(selected_ids) != BUNDLE_TOTAL - 1:
        return None
    anchor = beats_db.get_beat_by_id(anchor_id)
    if not anchor:
        return None
    selected_beats = []
    for bid in selected_ids:
        b = beats_db.get_beat_by_id(bid)
        if not b:
            return None
        selected_beats.append(b)
    return anchor, selected_beats


def kb_admin():
    beats = len([b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == "beat"])
    tracks = len([b for b in beats_db.BEATS_CACHE if b.get("content_type") == "track"])
    remixes = len([b for b in beats_db.BEATS_CACHE if b.get("content_type") == "remix"])
    products_n = len([
        b for b in beats_db.BEATS_CACHE
        if b.get("content_type") in ("drumkit", "samplepack", "looppack")
    ])
    # Счётчик запланированных публикаций
    try:
        import publish_scheduler
        queue_n = publish_scheduler.queue_size()
    except Exception:
        queue_n = 0
    # Quick-meta pending: биты с BPM но без key
    try:
        qm_pending = sum(
            1 for b in beats_db.BEATS_CACHE
            if b.get("content_type", "beat") == "beat"
            and (b.get("bpm") or 0) > 0
            and (not b.get("key") or b.get("key") == "-")
            and b.get("file_id")
        )
    except Exception:
        qm_pending = 0
    # Auto-repost on/off + ready pool size
    try:
        prefs = _load_admin_prefs()
        ar_on = prefs.get("auto_repost_enabled", False)
    except Exception:
        ar_on = False
    try:
        import beat_upload as _bu
        ready_count = sum(
            1 for b in beats_db.BEATS_CACHE
            if b.get("content_type", "beat") == "beat"
            and b.get("file_id")
            and _bu.beat_record_to_meta(b) is not None
        )
    except Exception:
        ready_count = 0

    queue_label = f"📅 Очередь публикаций ({queue_n})" if queue_n else "📅 Очередь публикаций (пусто)"
    products_label = f"📦 Kits & Packs ({products_n})" if products_n else "📦 Kits & Packs — залить первый"
    qm_label = f"🎹 Заполнить ключи ({qm_pending})" if qm_pending else "🎹 Все ключи заполнены ✅"
    ar_label = (
        f"🔁 Auto-repost: ✅ ON · {ready_count} ready"
        if ar_on
        else f"🔁 Auto-repost: ⛔ OFF · {ready_count} ready"
    )
    rows = [
        [InlineKeyboardButton("📊 Статистика (" + str(len(all_users)) + " польз.)", callback_data="admin_stats")],
        [InlineKeyboardButton("🎹 " + str(beats) + " / 🎤 " + str(tracks) + " / 🔀 " + str(remixes), callback_data="admin_catalog")],
        [InlineKeyboardButton(products_label, callback_data="admin_products")],
        [InlineKeyboardButton(queue_label, callback_data="admin_queue")],
        [InlineKeyboardButton(ar_label, callback_data="admin_auto_repost_toggle")],
    ]
    if qm_pending:
        rows.append([InlineKeyboardButton(qm_label, callback_data="admin_quick_meta")])
    rows.extend([
        [InlineKeyboardButton("📌 Обновить закреп-пост (навигация)", callback_data="admin_pin_hub")],
        [InlineKeyboardButton("🗑 Удалить бит из каталога", callback_data="admin_clearbeats")],
        [InlineKeyboardButton("🎬 YouTube", callback_data="admin_yt_menu")],
    ])
    return InlineKeyboardMarkup(rows)


def kb_admin_queue():
    """Список запланированных публикаций с кнопкой отмены per-item."""
    import publish_scheduler
    from datetime import datetime
    rows = []
    # Получаем raw data — нужны token'ы
    for q in sorted(publish_scheduler._QUEUE, key=lambda x: x["publish_at"]):
        token = q.get("token", "")
        meta = q.get("meta", {})
        name = meta.get("name", "?")[:20]
        artist = meta.get("artist_display", "?")[:20]
        dt = publish_scheduler._parse_dt(q["publish_at"])
        when = dt.strftime("%a %d %H:%M")
        label = f"{when} · {name} — {artist}"[:56]
        rows.append([InlineKeyboardButton(label, callback_data="noop")])
        rows.append([InlineKeyboardButton(f"   ❌ Отменить {name[:15]}", callback_data=f"qcancel_{token}")])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")])
    return InlineKeyboardMarkup(rows)


def kb_admin_yt():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика канала", callback_data="admin_yt_stats")],
        [InlineKeyboardButton("🔍 Diag OAuth env", callback_data="admin_yt_diag")],
        [InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")],
    ])


# ── Утилиты ───────────────────────────────────────────────────

async def is_subscribed(bot, user_id):
    if user_id in subscribed_users:
        return True
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        result = member.status in ("member", "administrator", "creator")
        if result:
            subscribed_users.add(user_id)
        return result
    except Exception as e:
        logger.warning("Sub check error: " + str(e))
        return False

async def send_sample_pack(bot, chat_id) -> bool:
    """Отправляет sample pack юзеру. Returns True если реально отправили файл,
    False если ни FILE_ID ни локального файла нет.

    Ранее был text-fallback `🎁 Сэмпл пак: {CHANNEL_LINK}` — убрали, потому что
    это вводило юзеров в заблуждение (обещан файл, пришла ссылка на канал).
    Сейчас при отсутствии файла — тихо skip'аем и возвращаем False, чтобы
    вызывающая сторона НЕ помечала `received_sample_pack=True` — тогда когда
    админ добавит SAMPLE_PACK_FILE_ID в env, этот юзер при следующем /start
    получит pack корректно.
    """
    try:
        if SAMPLE_PACK_FILE_ID:
            await bot.send_document(
                chat_id, document=SAMPLE_PACK_FILE_ID,
                caption="🎁 Твой FREE Sample Pack!",
            )
            return True
    except Exception as e:
        logger.warning("sample_pack: file_id send failed: %s", e)
    try:
        if SAMPLE_PACK_PATH and os.path.exists(SAMPLE_PACK_PATH):
            with open(SAMPLE_PACK_PATH, "rb") as f:
                await bot.send_document(
                    chat_id, document=f,
                    caption="🎁 Твой FREE Sample Pack!",
                )
            return True
    except Exception as e:
        logger.error("sample_pack: local file send failed: %s", e)
    logger.warning(
        "sample_pack: unavailable (no FILE_ID, no local file at %r) — "
        "skipping for user %s; will retry on next /start when env is set",
        SAMPLE_PACK_PATH, chat_id,
    )
    return False

async def show_main_menu(bot, chat_id, user_id: int | None = None):
    # Защитная перезагрузка: кэш пуст, но файл на диске есть — пробуем снова.
    # Размер не проверяем: даже корректные 2 байта "[]" — валидный JSON,
    # а битые 500 байт всё равно выявятся парсером (load_beats ловит и
    # логирует). Главное — дать шанс на retry, пока юзер смотрит в главное
    # меню, иначе каталог остаётся пустым навсегда.
    if not beats_db.BEATS_CACHE and os.path.exists(beats_db.BEATS_FILE):
        logger.warning("show_main_menu: cache пуст, перечитываю beats_data.json")
        beats_db.load_beats()
    beats = len([b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == "beat"])
    tracks = len([b for b in beats_db.BEATS_CACHE if b.get("content_type") == "track"])
    remixes = len([b for b in beats_db.BEATS_CACHE if b.get("content_type") == "remix"])
    text = "Привет! 👋 Что слушаем сегодня?\n\nВ каталоге: " + str(beats) + " битов, " + str(tracks) + " треков, " + str(remixes) + " ремиксов.\nВыбирай по настроению или жми случайный — не прогадаешь 🎲"
    # user_id default = chat_id (в DM это совпадает)
    uid = user_id if user_id is not None else chat_id
    await bot.send_message(chat_id, text, reply_markup=kb_main_menu(user_id=uid))

async def send_beat(bot, chat_id, beat, user_id):
    add_to_history(user_id, beat["id"])
    bid = beat["id"]
    beat_plays[bid] = beat_plays.get(bid, 0) + 1
    if bid not in beat_plays_users:
        beat_plays_users[bid] = set()
    beat_plays_users[bid].add(user_id)
    # Re-marketing tracking: юзер посмотрел preview бита.
    # Через 24h, если не купит — отправим reminder (см. remarketing_scheduler).
    track_bit_view(user_id, beat)

    tags_str = " ".join(["#" + t for t in beat["tags"]]) if beat["tags"] else ""
    icon = {"beat": "🎹", "track": "🎤", "remix": "🔀"}.get(beat.get("content_type", "beat"), "🎧")
    sep = "--------------------"
    caption = sep + "\n" + icon + "  " + beat["name"].upper() + "\n" + sep
    if beat.get("bpm"):
        caption += "\n⚡ " + str(beat["bpm"]) + " BPM"
        if beat.get("key") and beat["key"] != "-":
            caption += "  |  🎵 " + beat["key"]
    if tags_str:
        caption += "\n" + tags_str
    content_type = beat.get("content_type", "beat")

    # Single-audio playback: удаляем предыдущий бит-аудио этого юзера, чтобы
    # в чате не накапливалась лента из десятка треков. Best-effort: если не
    # получилось (TG лимит 48ч на delete или старый id неактуальный) — просто
    # шлём новое сообщение поверх.
    prev_msg_id = last_bit_audio_msg.get(user_id)
    if prev_msg_id:
        try:
            await bot.delete_message(chat_id, prev_msg_id)
        except Exception:
            pass

    sent = None
    if beat.get("file_id"):
        try:
            sent = await bot.send_audio(
                chat_id, audio=beat["file_id"], caption=caption,
                reply_markup=kb_after_beat(beat["id"], content_type, user_id=user_id),
            )
        except Exception as e:
            logger.warning("Audio send failed: " + str(e))

    if sent is None:
        caption += "\n\n👉 " + beat["post_url"]
        sent = await bot.send_message(
            chat_id, caption,
            reply_markup=kb_after_beat(beat["id"], content_type, user_id=user_id),
        )

    if sent is not None:
        last_bit_audio_msg[user_id] = sent.message_id


# ── /start ────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    bot = context.bot
    write_heartbeat()

    # Deep-link payload parsing. Поддерживаемые форматы:
    #   ref_<src>                     → только source (лендинг / био)
    #   buy_<id>                      → только покупка бита
    #   prod_<id>                     → только покупка продукта
    #   ref_<src>_buy_<id>            → combo source+бит (YT description)
    #   ref_<src>_prod_<id>           → combo source+продукт
    # Source (из ref_) — first-touch, записывается один раз при INSERT.
    # Target (buy_/prod_) — триггерит carding screen ниже после upsert.
    ref_source = None
    effective_arg = context.args[0] if context.args else ""
    if effective_arg.startswith("ref_"):
        rest = effective_arg[4:]  # после "ref_"
        if "_buy_" in rest:
            src_part, tail = rest.split("_buy_", 1)
            effective_arg = f"buy_{tail}"
        elif "_prod_" in rest:
            src_part, tail = rest.split("_prod_", 1)
            effective_arg = f"prod_{tail}"
        else:
            src_part = rest
            effective_arg = ""  # standalone ref_, нет target
        raw_src = src_part.lower().strip()
        _REF_WHITELIST = {"yt", "ytshorts", "insta", "tg", "tiktok", "soundcloud", "landing", "ads", "collab"}
        ref_source = raw_src if raw_src in _REF_WHITELIST else "other"

    # is_new определяется через Supabase (source of truth между Render
    # redeploy'ями). Fallback — in-memory all_users (свежий процесс).
    supabase_is_new = await asyncio.to_thread(
        users_db.upsert_user, user_id, user.full_name, user.username, ref_source
    )
    is_new = user_id not in all_users and supabase_is_new is not False
    if user_id not in all_users:
        all_users[user_id] = {
            "name": user.full_name,
            "username": user.username or "",
            "joined": datetime.now().strftime("%d.%m.%Y %H:%M"),
        }
        asyncio.create_task(asyncio.to_thread(save_users))
    if is_new:
        try:
            uname = "@" + user.username if user.username else user.full_name
            src_tag = f" · src={ref_source}" if ref_source else ""
            await bot.send_message(
                ADMIN_ID,
                "🔔 Новый: " + uname + src_tag + " | Всего: " + str(len(all_users))
            )
        except Exception:
            pass

    # Deep-link из канала на карточку продукта: /start prod_<product_id>
    # (или /start ref_<src>_prod_<product_id> — effective_arg уже нормализован выше)
    if effective_arg.startswith("prod_"):
        try:
            pid = int(effective_arg[5:])
            p = beats_db.get_beat_by_id(pid)
            if p and p.get("content_type") in licensing.PRODUCT_TYPE_LABELS:
                label = licensing.PRODUCT_TYPE_LABELS[p["content_type"]]
                size_mb = (p.get("file_size") or 0) / (1024 * 1024) if p.get("file_size") else 0
                stars = p.get("price_stars", "?")
                usdt = p.get("price_usdt", "?")
                # Escape всё что идёт в HTML caption. `name`/`description` могут
                # прийти из parse_beat_from_text (channel post text) — не trusted.
                # Если оставить без escape — юзер с правами постить в канал может
                # сломать parse_mode="HTML" и DoS'нуть /start prod_<id>.
                name_safe = html.escape(str(p.get("name") or ""))
                desc_safe = html.escape(str(p.get("description") or "")) if p.get("description") else "<i>(без описания)</i>"
                info = (
                    f"📦 <b>{html.escape(label)}</b>\n"
                    f"🎯 <b>{name_safe}</b>\n"
                    f"📎 {size_mb:.1f} MB\n\n"
                    f"{desc_safe}\n\n"
                    f"💎 WAV / Trackouts / Exclusive — DM @iiiplfiii"
                )
                usdt_label = f"💵 {usdt:g} USDT" if isinstance(usdt, (int, float)) else "💵 USDT"
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"⭐ {stars}", callback_data=f"buy_prod_{pid}"),
                     InlineKeyboardButton(usdt_label, callback_data=f"buy_prod_usdt_{pid}")],
                    [InlineKeyboardButton("📦 Все паки", callback_data="menu_products")],
                ])
                await bot.send_message(user_id, info, reply_markup=kb, parse_mode="HTML")
                return
            # Продукт не найден — graceful 404.
            await bot.send_message(
                user_id,
                f"📦 Продукт по этой ссылке пока недоступен (id={pid}).\n"
                "Вот весь каталог:",
                reply_markup=kb_main_menu(user_id=user_id),
            )
            return
        except Exception as e:
            logger.warning("deep-link prod_ failed for %r: %s", effective_arg, e)

    # Deep-link из YT-описания: /start buy_<beat_id> → сразу показать покупку этого бита
    # (или /start ref_yt_buy_<id> — combo source+target, effective_arg уже нормализован)
    if effective_arg.startswith("buy_"):
        try:
            beat_id = int(effective_arg[4:])
            beat = beats_db.get_beat_by_id(beat_id)
            if beat:
                # name/bpm/key могут прийти через parse_beat_from_text из
                # произвольного channel post text → escape перед HTML render'ом.
                name_safe = html.escape(str(beat.get("name") or "?"))
                bpm_safe = html.escape(str(beat.get("bpm") or "?"))
                key_safe = html.escape(str(beat.get("key") or "?"))
                caption = (
                    f"🎧 <b>{name_safe}</b>\n"
                    f"⚡ BPM {bpm_safe}  🎹 {key_safe}\n\n"
                    "Выбери вариант покупки — или напиши @iiiplfiii для WAV/Unlimited/Exclusive:"
                )
                await bot.send_message(
                    user_id, caption,
                    reply_markup=kb_channel_beat_buy(beat_id),
                    parse_mode="HTML",
                )
                return
            # Бит с таким id не найден — показываем вменяемое сообщение + меню
            await bot.send_message(
                user_id,
                f"🎧 Бит по этой ссылке пока не публичный (id={beat_id}).\n"
                "Вот весь каталог + поиск по BPM/сцене:",
                reply_markup=kb_main_menu(user_id=user_id),
            )
            return
        except Exception as e:
            logger.warning("deep-link buy_ failed for %r: %s", effective_arg, e)
            # fall through to normal start flow

    subscribed = await is_subscribed(bot, user_id)
    if not subscribed:
        await update.message.reply_text(WELCOME_TEXT, reply_markup=kb_subscribe())
        return

    # Sample pack: primary check через Supabase (переживает redeploy),
    # fallback — in-memory set. True/False — уже получал/не получал.
    sp_state = await asyncio.to_thread(users_db.has_received_sample_pack, user_id)
    already_received = (
        user_id in users_received_pack if sp_state is None else sp_state
    )
    if not already_received:
        # Помечаем как «получил pack» ТОЛЬКО если реально отправили файл.
        # Если FILE_ID ещё не задан в env — send_sample_pack вернёт False
        # и юзер получит pack при следующем /start (когда админ поставит FILE_ID).
        sent = await send_sample_pack(bot, user_id)
        if sent:
            users_received_pack.add(user_id)
            await asyncio.to_thread(users_db.mark_sample_pack_received, user_id)
            asyncio.create_task(asyncio.to_thread(save_users))
        try:
            uname = "@" + user.username if user.username else user.full_name
            await bot.send_message(ADMIN_ID, "🎁 " + uname + " получил пак! Всего: " + str(len(users_received_pack)))
        except Exception:
            pass

    await show_main_menu(bot, user_id)


# ── /admin ────────────────────────────────────────────────────

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    # Defensive reload — если cache пустой, но файл на диске есть
    # (аналогично show_main_menu). Покрывает case когда /admin сработал
    # до load_beats в post_init, или после cold start без прогрева.
    if not beats_db.BEATS_CACHE and os.path.exists(beats_db.BEATS_FILE):
        logger.warning("cmd_admin: cache пуст, перечитываю beats_data.json")
        beats_db.load_beats()
    if not all_users and os.path.exists(USERS_FILE):
        logger.warning("cmd_admin: all_users пуст, перечитываю users_data.json")
        load_users()
    await update.message.reply_text("🎛 Панель управления:", reply_markup=kb_admin())


# ── /diag ─────────────────────────────────────────────────────

async def cmd_diag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    import os as _os
    from collections import Counter as _Counter
    path = beats_db.BEATS_FILE
    exists = _os.path.exists(path)
    size = _os.path.getsize(path) if exists else 0
    cwd = _os.getcwd()
    before = len(beats_db.BEATS_CACHE)
    beats_db.load_beats()
    after = len(beats_db.BEATS_CACHE)
    types = dict(_Counter(b.get("content_type", "?") for b in beats_db.BEATS_CACHE))
    ch_raw = _os.getenv("CHANNEL_ID", "")
    ch_used = CHANNEL_ID
    ch_info = f"CHANNEL_ID raw: len={len(ch_raw)} repr={ch_raw!r}\nCHANNEL_ID used: {ch_used!r}"
    try:
        chat = await context.bot.get_chat(CHANNEL_ID)
        ch_info += f"\nget_chat OK: id={chat.id} type={chat.type} title={chat.title!r} username=@{chat.username}"
    except Exception as e:
        ch_info += f"\nget_chat FAIL: {e}"
    msg = (
        "🔧 Diag\n"
        f"cwd: {cwd}\n"
        f"BEATS_FILE: {path}\n"
        f"exists: {exists}, size: {size} bytes\n"
        f"BEATS_CACHE before/after reload: {before} → {after}\n"
        f"types: {types}\n\n"
        f"{ch_info}"
    )
    await update.message.reply_text(msg)


# ── /search ───────────────────────────────────────────────────

async def cmd_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = " ".join(context.args) if context.args else ""
    if query:
        await do_search(context.bot, update.effective_chat.id, query, update.effective_user.id)
    else:
        await update.message.reply_text("Напиши: /search keyglock")


# ── /queue — очередь плановых публикаций ─────────────────────

async def cmd_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    import publish_scheduler
    items = publish_scheduler.queue_summary()
    if not items:
        await update.message.reply_text("📭 Очередь пуста")
        return
    text = "📅 Очередь плановых публикаций:\n\n" + "\n\n".join(items)
    text += "\n\n<i>Отменить:</i> <code>/cancel_sched TOKEN</code>"
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_cancel_sched(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Usage: /cancel_sched <token>")
        return
    token = context.args[0]
    import publish_scheduler
    if publish_scheduler.cancel(token):
        # Файлы оставляем в pending_uploads (если ещё там) либо вручную удалять
        await update.message.reply_text(f"✅ Отменено: {token}")
    else:
        await update.message.reply_text(f"⚠️ Не нашёл в очереди: {token}")


_KNOWN_SCENES = {"memphis", "detroit", "hardtrap", "phonk", "florida", "drill", "atlanta"}
_GENERIC_TAGS = _KNOWN_SCENES | {"hard", "trap", "dark", "darktrap", "type", "beat", "typebeat", "free"}


def _fallback_caption_for_backfill(beat: dict) -> str:
    """Минималистичный caption-template для backfill'а старых постов.

    Используется в /fix_hashtags когда мы не можем достать оригинальный
    caption через Bot API. Сохраняет имя + BPM/key + deep-link в бот
    + hashtag-nav. Теряется оригинальный LLM-текст — осознанный трейдофф
    ради работающего поиска по #bpmXXX.
    """
    name = beat.get("name", "Beat")
    # Минимальная косметика: убираем расширение + подчёркивания, collapse spaces.
    # Оригинальный текст filename сохраняется максимально — не рискуем отрезать имя.
    name = re.sub(r"\.mp3$", "", name, flags=re.IGNORECASE)
    name = name.replace("_", " ")
    name = re.sub(r"\s+", " ", name).strip(" -.")
    name = name or "Beat"

    bpm = beat.get("bpm", 0)
    key = (beat.get("key") or "").strip() or "?"
    bucket = (bpm // 10) * 10

    # Хэштеги: из tags достаём артиста и сцену, BPM и typebeat всегда
    tags = [t.lower().strip() for t in (beat.get("tags") or []) if t]
    scene = next((t for t in tags if t in _KNOWN_SCENES), "")
    artist = next(
        (t.replace(" ", "") for t in tags if t not in _GENERIC_TAGS and len(t) > 2),
        "",
    )
    hashtags = []
    if artist:
        hashtags.append(f"#{artist}")
    if scene:
        hashtags.append(f"#{scene}")
    hashtags.append(f"#bpm{bucket}")
    hashtags.append("#typebeat")

    bot_link = f"https://t.me/triplekillpost_bot?start=buy_{beat['id']}"
    caption = (
        f"{name}\n"
        f"⚡ {bpm} BPM · 🎹 {key}\n\n"
        f"🎧 MP3 Lease → {bot_link}\n"
        f"💎 WAV · Unlimited · Exclusive — DM @iiiplfiii\n\n"
        f"{' '.join(hashtags)}"
    )
    return caption


async def cmd_fix_hashtags(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Backfill хэштегов в старых audio-постах канала.

    Источник — beats_data.json (msg_id + bpm + tags для каждого бита).
    Для каждого бита с content_type='beat' и валидным bpm переписывает
    caption шаблоном: name + BPM/key + deep-link + хэштеги. Оригинальный
    LLM-текст теряется — осознанный трейдофф (Option A юзера 2026-04-21).

    Биты с bpm=0 (старый код не парсил filename правильно) — пропускаются
    (Option C юзера): их backlog остаётся без тегов, но новые публикации
    уже получат _hashtag_nav автоматически через build_tg_caption_async.
    """
    if update.effective_user.id != ADMIN_ID:
        return

    beats = [
        b for b in beats_db.BEATS_CACHE
        if b.get("content_type") == "beat"
        and isinstance(b.get("bpm"), int)
        and b["bpm"] >= 80
        and b.get("msg_id")
    ]

    await update.message.reply_text(
        f"🔍 Нашёл {len(beats)} битов с валидным BPM и msg_id.\n"
        f"Будет переписан caption (оригинальный текст теряется).\n"
        f"Приблизительное время: ~{len(beats)*2}с\n\n"
        f"Начинаю..."
    )
    stats = {"updated": 0, "skipped": 0, "failed": 0, "too_long": 0}
    progress_msg = await update.message.reply_text(f"⏳ 0/{len(beats)}")

    for i, beat in enumerate(beats, 1):
        mid = beat["msg_id"]
        try:
            new_caption = _fallback_caption_for_backfill(beat)
        except Exception as e:
            logger.warning("fix_hashtags: build caption failed for beat %s: %s", beat.get("id"), e)
            stats["failed"] += 1
            continue
        if len(new_caption) > 1024:
            stats["too_long"] += 1
            continue

        try:
            await context.bot.edit_message_caption(
                chat_id=CHANNEL_ID,
                message_id=mid,
                caption=new_caption,
                parse_mode=None,  # plaintext — безопасно, не сломается на < / >
            )
            stats["updated"] += 1
        except TelegramError as e:
            err_s = str(e).lower()
            if "message is not modified" in err_s or "exactly the same" in err_s:
                stats["skipped"] += 1
            else:
                logger.warning("fix_hashtags: edit msg_id=%s failed: %s", mid, e)
                stats["failed"] += 1
        except Exception as e:
            logger.warning("fix_hashtags: edit msg_id=%s exception: %s", mid, e)
            stats["failed"] += 1

        if i % 5 == 0:
            try:
                await progress_msg.edit_text(
                    f"⏳ {i}/{len(beats)} · ✅ {stats['updated']} · ⏭ {stats['skipped']} · ❌ {stats['failed']}"
                )
            except Exception:
                pass

        await asyncio.sleep(2)  # мягкий rate-limit на edit_message_caption

    await update.message.reply_text(
        f"✅ Готово!\n\n"
        f"Обновлено: {stats['updated']}\n"
        f"Пропущено (уже тот же caption): {stats['skipped']}\n"
        f"Слишком длинный: {stats['too_long']}\n"
        f"Ошибок edit: {stats['failed']}\n\n"
        f"Поиск по #bpmXXX в канале теперь заработает для этих битов."
    )


async def cmd_cancel_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Прерывает любой шаг FSM upload_product."""
    if update.effective_user.id != ADMIN_ID:
        return
    had = bool(context.user_data.pop("product_upload", None))
    if had:
        await update.message.reply_text("❌ Загрузка продукта прервана")
    else:
        await update.message.reply_text("Нечего отменять — нет активной загрузки")


# ── /upload_product — FSM для заливки kit/pack/loop ───────────

async def _start_product_upload(bot, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Общий старт FSM: reply через cmd_upload_product ИЛИ через кнопку
    «📦 Kits & Packs» в /admin → «➕ Залить новый»."""
    context.user_data["product_upload"] = {"step": "await_type"}
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🥁 Drum Kit (1500⭐)", callback_data="prod_type_drumkit")],
        [InlineKeyboardButton("🎵 Sample Pack (1000⭐)", callback_data="prod_type_samplepack")],
        [InlineKeyboardButton("🔄 Loop Pack (1000⭐)", callback_data="prod_type_looppack")],
        [InlineKeyboardButton("❌ Отмена", callback_data="prod_abort")],
    ])
    await bot.send_message(chat_id, "📦 Новый продукт в каталог. Выбери тип:", reply_markup=kb)


async def cmd_upload_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Старт FSM загрузки продукта. Админ выбирает тип → бот ведёт по шагам:
    zip → имя → цена → описание → preview → save.

    State хранится в context.user_data["product_upload"] dict:
        {"step": "await_type|await_zip|await_name|await_price|await_desc",
         "content_type": "drumkit|samplepack|looppack",
         "file_id", "file_size", "file_name", "mime_type",
         "name", "price_stars", "price_usdt", "description"}
    """
    if update.effective_user.id != ADMIN_ID:
        return
    await _start_product_upload(context.bot, update.effective_chat.id, context)


# ── /pin_hub — навигационный закреп-пост в канал ──────────────

async def _show_pin_hub_preview(bot, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Общий preview-sender для /pin_hub И кнопки в /admin."""
    text = beat_post_builder.build_pinned_hub()
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📌 Опубликовать и закрепить", callback_data="pin_hub_go")],
        [InlineKeyboardButton("❌ Отмена", callback_data="pin_hub_cancel")],
    ])
    # Сохраняем текст в user_data — чтобы callback взял именно эту версию,
    # а не пересчитывал каталог заново (вдруг между превью и кликом что-то
    # успело измениться).
    context.user_data["pin_hub_text"] = text
    await bot.send_message(
        chat_id,
        f"👁 Превью hub-поста:\n\n{text}",
        reply_markup=kb,
        disable_web_page_preview=True,
    )


async def cmd_pin_hub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Генерирует navigation hub-пост по текущему каталогу и предлагает
    админу опубликовать + закрепить в канале. Обновлять раз в 2 недели.
    """
    if update.effective_user.id != ADMIN_ID:
        return
    await _show_pin_hub_preview(context.bot, update.effective_chat.id, context)


# ── /stats — сводка публикаций ────────────────────────────────

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Краткая сводка событий публикаций за 7 и 30 дней. Только админ."""
    if update.effective_user.id != ADMIN_ID:
        return
    from collections import Counter
    from zoneinfo import ZoneInfo
    msk = ZoneInfo("Europe/Moscow")
    now = datetime.now(msk)
    cutoff_7 = now - timedelta(days=7)
    cutoff_30 = now - timedelta(days=30)

    events: list[dict] = []
    try:
        import post_analytics
        client = post_analytics._get_supabase()
        if client is not None:
            resp = client.table("post_events").select("*")\
                .gte("ts", cutoff_30.isoformat()).order("ts", desc=True).execute()
            events = list(resp.data or [])
        if not events:
            for ev in post_analytics.read_events():
                try:
                    ts = datetime.fromisoformat(ev.get("ts", "").replace("Z", "+00:00"))
                    if ts >= cutoff_30:
                        events.append(ev)
                except Exception:
                    continue
    except Exception as e:
        await update.message.reply_text(f"⚠️ Не получилось прочитать события: {e}")
        return

    def _in_range(ev, cutoff):
        try:
            ts = datetime.fromisoformat(ev.get("ts", "").replace("Z", "+00:00"))
            return ts >= cutoff
        except Exception:
            return False

    ev7 = [e for e in events if _in_range(e, cutoff_7)]
    ev30 = [e for e in events if _in_range(e, cutoff_30)]

    if not ev30:
        await update.message.reply_text(
            "📊 Пусто за последние 30 дн.\n\nКак только начнёшь публиковать через /upload или автопостинг — здесь будет сводка."
        )
        return

    lines = [f"📊 Сводка публикаций\n"]
    lines.append(f"За 7 дн:  {len(ev7)}")
    lines.append(f"За 30 дн: {len(ev30)}")

    styles = Counter(e.get("style") or "?" for e in ev30)
    if styles:
        lines.append("\n🎨 Стили подписей (30 дн):")
        for st, n in styles.most_common(5):
            lines.append(f"  • {st}: {n}")

    yt_only = sum(1 for e in ev30 if e.get("yt_video_id") and not e.get("tg_message_id"))
    tg_only = sum(1 for e in ev30 if e.get("tg_message_id") and not e.get("yt_video_id"))
    both    = sum(1 for e in ev30 if e.get("yt_video_id") and e.get("tg_message_id"))
    lines.append("\n📡 Каналы (30 дн):")
    lines.append(f"  • YT + TG:   {both}")
    lines.append(f"  • Только YT: {yt_only}")
    lines.append(f"  • Только TG: {tg_only}")

    lines.append("\n🕑 Последние 5:")
    for ev in ev30[:5]:
        ts_raw = ev.get("ts", "")
        try:
            dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).astimezone(msk)
            ts_disp = dt.strftime("%d.%m %H:%M")
        except Exception:
            ts_disp = ts_raw[:16]
        beat = ev.get("beat_name") or "—"
        style = ev.get("style") or "?"
        marks = ""
        if ev.get("yt_video_id"): marks += " 📺"
        if ev.get("tg_message_id"): marks += " 💬"
        lines.append(f"  • {ts_disp} — {beat} [{style}]{marks}")

    await update.message.reply_text("\n".join(lines))


# ── Поиск ─────────────────────────────────────────────────────

async def do_search(bot, chat_id, query, user_id):
    q = query.lower()
    results = [b for b in beats_db.BEATS_CACHE
               if b.get("content_type", "beat") != "non_audio"
               and (q in b["name"].lower() or any(q in t for t in b.get("tags", [])))]
    if not results:
        await bot.send_message(chat_id, "Хм, по запросу \"" + query + "\" ничего не нашёл 🤔\nПопробуй другое слово!")
        return
    rows = [[InlineKeyboardButton(b["name"][:40], callback_data="play_" + str(b["id"]))] for b in results[:10]]
    rows.append([InlineKeyboardButton("◀️ Меню", callback_data="main_menu")])
    await bot.send_message(chat_id, "🔍 По запросу \"" + query + "\" нашёл " + str(len(results)) + " шт.:",
                           reply_markup=InlineKeyboardMarkup(rows))


# ══════════════════════════════════════════════════════════════
# CALLBACK HANDLER
# ══════════════════════════════════════════════════════════════

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id
    bot = context.bot
    write_heartbeat()

    await query.answer()

    # Defensive reload — если callback пришёл сразу после cold start и
    # cache ещё не прогрет, любой хэндлер использующий BEATS_CACHE
    # получит пустой ответ (было видно в /admin → pin_hub: "0 битов").
    # cmd_admin это уже делает, дублируем для callback-ветки.
    if not beats_db.BEATS_CACHE and os.path.exists(beats_db.BEATS_FILE):
        logger.warning("handle_callback: cache пуст, перечитываю beats_data.json")
        beats_db.load_beats()

    if data == "admin_yt_menu":
        if user_id != ADMIN_ID:
            return
        await _nav_reply(query, "🎬 YouTube:", reply_markup=kb_admin_yt())
        return

    if data == "admin_yt_stats":
        if user_id != ADMIN_ID:
            return
        try:
            import yt_api
            s = yt_api.get_channel_stats()
            await query.message.reply_text(
                f"📊 {s['title']}\n"
                f"Подписчиков: {s['subs']}\n"
                f"Просмотров: {s['views']:,}\n"
                f"Видео: {s['videos']}"
            )
        except Exception as e:
            logger.exception("yt stats failed")
            await query.message.reply_text(f"⚠️ Ошибка: {e}")
        return

    if data == "admin_yt_diag":
        if user_id != ADMIN_ID:
            return
        def _mask(v: str) -> str:
            if not v:
                return "(empty)"
            has_ws = any(c in v for c in (" ", "\n", "\r", "\t"))
            ws = " ⚠️whitespace" if has_ws else ""
            return f"len={len(v)} head='{v[:6]}' tail='{v[-6:]}'{ws}"
        cid = os.getenv("YT_CLIENT_ID", "")
        cs = os.getenv("YT_CLIENT_SECRET", "")
        rt = os.getenv("YT_REFRESH_TOKEN", "")
        expected_cid_tail = ".apps.googleusercontent.com"
        cid_ok = "✅" if cid.endswith(expected_cid_tail) else "❌ должен кончаться на .apps.googleusercontent.com"
        cs_ok = "✅" if cs.startswith("GOCSPX-") else "❌ должен начинаться с GOCSPX-"
        rt_ok = "✅" if rt.startswith("1//") else "❌ должен начинаться с 1//"
        await query.message.reply_text(
            f"🔍 YT env diag:\n\n"
            f"YT_CLIENT_ID: {_mask(cid)}\n  {cid_ok}\n\n"
            f"YT_CLIENT_SECRET: {_mask(cs)}\n  {cs_ok}\n\n"
            f"YT_REFRESH_TOKEN: {_mask(rt)}\n  {rt_ok}"
        )
        return

    # ── Beat upload callbacks (bu_yt, bu_tg, bu_all, bu_cancel) ─────
    if data.startswith("bu_"):
        if user_id != ADMIN_ID:
            return
        parts = data.split("_", 2)
        if len(parts) < 3:
            return
        action, token = parts[1], parts[2]
        payload = pending_uploads.get(token)
        if not payload:
            await query.message.reply_text("⚠️ Превью устарело. Загрузи mp3 ещё раз.")
            return

        if action == "cancel":
            await _delete_preview_messages(context.bot, payload)
            _cleanup_upload(token)
            try:
                await query.message.reply_text("❌ Отменено")
            except Exception:
                pass
            return

        if action == "sched":
            import publish_scheduler
            # Сохраняем token в payload чтобы потом matching при публикации
            payload["token"] = token
            publish_at = publish_scheduler.enqueue(payload, actions=["yt", "tg"])
            # Чистим превью-сообщения (audio + photo) — они больше не нужны
            await _delete_preview_messages(context.bot, payload)
            pending_uploads.pop(token, None)
            now = datetime.now(MSK_TZ)
            delta = publish_at - now
            days, rem = divmod(int(delta.total_seconds()), 86400)
            hours, _ = divmod(rem, 3600)
            human = f"{days}д {hours}ч" if days else f"{hours}ч"
            await context.bot.send_message(
                user_id,
                f"📅 Запланировано на {publish_at.strftime('%a %d %b %H:%M МСК')} (через {human})\n"
                f"В назначенное время — авто-upload YT + пост в канал @iiiplfiii.\n\n"
                f"В очереди: {publish_scheduler.queue_size()} битов"
            )
            return

        if action == "regen":
            # beat_post_builder уже импортирован на module level (строка 26).
            # Локальный `import beat_post_builder` здесь (был исторически)
            # делал Python'у считать `beat_post_builder` локальной переменной
            # на весь scope handle_callback → UnboundLocalError при
            # обращении к нему в других ветках до этой точки (например в
            # buy_rub_ / buy_mix_rub где f"{beat_post_builder.BOT_USERNAME}").
            meta = payload["meta"]
            try:
                new_caption, new_style = await beat_post_builder.build_tg_caption_async(
                    meta, beat_id=payload.get("reserved_beat_id"),
                )
            except Exception as e:
                logger.exception("regen caption failed")
                await query.answer(f"❌ LLM: {e}", show_alert=True)
                return
            payload["tg_caption"] = new_caption
            payload["tg_style"] = new_style
            chat_id = payload.get("tg_preview_chat_id")
            msg_id = payload.get("tg_preview_msg_id")
            if chat_id and msg_id:
                try:
                    await context.bot.edit_message_caption(
                        chat_id=chat_id,
                        message_id=msg_id,
                        caption=f"👁 Превью TG-поста:\n\n{new_caption}",
                    )
                    await query.answer("🔄 Подпись перезаписана")
                except Exception as e:
                    logger.exception("edit_message_caption failed")
                    await query.answer(f"❌ Edit: {e}", show_alert=True)
            else:
                await query.answer("⚠️ Превью-сообщение не найдено", show_alert=True)
            return

        from pathlib import Path
        meta = payload["meta"]
        yt_post = payload["yt_post"]
        tg_caption = payload["tg_caption"]
        video_path: Path = payload["video_path"]
        thumb_path: Path = payload["thumb_path"]

        yt_ok = None
        tg_ok = None
        yt_url = None
        yt_video_id = None
        tg_message_id = None

        if action in ("yt", "all"):
            await query.message.reply_text("⏳ Гружу на YouTube...")
            try:
                import yt_api
                loop = asyncio.get_running_loop()
                video_id = await loop.run_in_executor(
                    None,
                    lambda: yt_api.upload_video(
                        video_path, yt_post.title, yt_post.description,
                        yt_post.tags, thumb_path,
                    ),
                )
                yt_video_id = video_id
                yt_url = f"https://youtu.be/{video_id}"
                yt_ok = True
                # Добавляем в плейлисты — winning-паттерн 4/4 топ-каналов
                try:
                    await loop.run_in_executor(
                        None, lambda: _add_to_yt_playlists(video_id, meta)
                    )
                except Exception:
                    logger.exception("yt playlists add failed (non-fatal)")
                # Авто-CTA коммент — engagement signal с первой минуты
                try:
                    await loop.run_in_executor(
                        None, lambda: _post_cta_comment(video_id, payload.get("reserved_beat_id"))
                    )
                except Exception:
                    logger.exception("yt cta comment failed (non-fatal)")
                # Добавляем в каталог — используем reserved_beat_id (чтобы deep-link
                # из YT description вёл именно на этот битек).
                try:
                    new_id = payload.get("reserved_beat_id") or (
                        max([b["id"] for b in beats_db.BEATS_CACHE] + [0]) + 1
                    )
                    beats_db.BEATS_CACHE.append({
                        "id": new_id,
                        "msg_id": 0,
                        "name": yt_post.title,
                        "tags": yt_post.tags,
                        "post_url": yt_url,
                        "bpm": meta.bpm,
                        "key": meta.key,
                        "file_id": payload["tg_file_id"],
                        "content_type": "beat",
                        "file_unique_id": "",
                        "classification_confidence": 1.0,
                        "yt_video_id": video_id,
                    })
                    beats_db._rebuild_index()
                    beats_db.save_beats()
                except Exception as e:
                    logger.exception("beats_db append failed")

                # ── YT Shorts: второй upload 9:16 версии 45 сек ───────
                # Использует тот же brand-кадр, но в 1080×1920 letterbox.
                # Shorts feed = отдельный recommendation-канал внутри YT,
                # не каннибализирует основной трек.
                try:
                    import shorts_builder
                    short_path = video_path.with_name(f"short_{video_path.name}")
                    await loop.run_in_executor(
                        None,
                        lambda: shorts_builder.build_short(
                            thumb_path, mp3_path, short_path,
                        ),
                    )
                    short_title = beat_post_builder.build_shorts_title(meta)
                    short_desc = beat_post_builder.build_shorts_description(
                        meta, beat_id=payload.get("reserved_beat_id"),
                        full_video_url=yt_url,
                    )
                    short_tags = beat_post_builder.build_shorts_tags(meta)
                    short_video_id = await loop.run_in_executor(
                        None,
                        lambda: yt_api.upload_video(
                            short_path, short_title, short_desc,
                            short_tags, thumb_path,
                        ),
                    )
                    short_url = f"https://youtu.be/{short_video_id}"
                    logger.info("YT Short uploaded: %s", short_url)
                    await query.message.reply_text(
                        f"📱 YT Short опубликован: {short_url}"
                    )
                    # ── TikTok semi-auto: шлём админу mp4 + caption ──
                    # Full API upload в TikTok требует 1-4 нед approval.
                    # Пока админ постит сам: открыть в TG на телефоне,
                    # сохранить mp4, кинуть в TikTok app. 30 сек.
                    try:
                        tiktok_caption = beat_post_builder.build_tiktok_caption(meta)
                        with open(short_path, "rb") as vf:
                            await bot.send_video(
                                ADMIN_ID,
                                video=InputFile(vf, filename=short_path.name),
                                caption=(
                                    f"📱 <b>Готовый TikTok для {meta.name}</b>\n\n"
                                    f"<b>Caption (скопируй):</b>\n<code>{tiktok_caption}</code>\n\n"
                                    f"<i>Открой на телефоне → сохрани видео → загрузи в TikTok app</i>"
                                ),
                                parse_mode="HTML",
                            )
                        logger.info("TikTok mp4 sent to admin for %s", meta.name)
                    except Exception:
                        logger.exception("TikTok send to admin failed (non-fatal)")
                    # Cleanup — short file нужен был только для upload'а
                    try:
                        short_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                except Exception as e:
                    logger.exception("YT Shorts upload failed (non-fatal)")
                    await query.message.reply_text(
                        f"⚠️ Short не залился (основное видео ок): {e}"
                    )
            except Exception as e:
                logger.exception("yt upload failed")
                yt_ok = False
                await query.message.reply_text(f"❌ YT ошибка: {e}")

        if action in ("tg", "all"):
            try:
                sent = await context.bot.send_audio(
                    CHANNEL_ID,
                    audio=payload["tg_file_id"],
                    caption=tg_caption,
                )
                tg_message_id = sent.message_id
                logger.info(
                    "tg send_audio OK: target=%r landed chat_id=%s type=%s username=@%s title=%r message_id=%s",
                    CHANNEL_ID, sent.chat.id, sent.chat.type, sent.chat.username, sent.chat.title, sent.message_id,
                )
                tg_ok = True
                # Помечаем last_posted_at чтобы scheduler не взял бит сразу в daily rubric
                try:
                    for b in beats_db.BEATS_CACHE:
                        if b.get("file_id") == payload["tg_file_id"]:
                            b["last_posted_at"] = datetime.now().isoformat(timespec="seconds")
                    beats_db.save_beats()
                except Exception:
                    logger.exception("mark last_posted_at failed (non-fatal)")
            except Exception as e:
                logger.exception("tg send failed")
                tg_ok = False
                await query.message.reply_text(f"❌ TG ошибка: {e}")

        # Лог публикации для будущего анализа «какой стиль подписи заходит».
        if yt_ok or tg_ok:
            try:
                import post_analytics
                post_analytics.log_event(
                    kind="upload",
                    beat_name=meta.name,
                    artist=meta.artist_display,
                    bpm=meta.bpm,
                    key=meta.key,
                    style=payload.get("tg_style", "unknown"),
                    caption=tg_caption,
                    yt_video_id=yt_video_id,
                    tg_message_id=tg_message_id,
                    yt_title=yt_post.title,
                )
            except Exception:
                logger.exception("post_analytics log_event failed (non-fatal)")

        parts_msg = []
        if yt_ok:
            parts_msg.append(f"✅ YT: {yt_url}")
        if tg_ok:
            parts_msg.append("✅ TG: опубликовано в канал")
        if parts_msg:
            await context.bot.send_message(user_id, "\n".join(parts_msg))

        # Чистим превью-сообщения чтобы чат не засорялся
        await _delete_preview_messages(context.bot, payload)
        _cleanup_upload(token)
        return

    if data == "prod_abort":
        if user_id != ADMIN_ID:
            return
        context.user_data.pop("product_upload", None)
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await bot.send_message(user_id, "❌ Загрузка продукта отменена")
        return

    if data.startswith("prod_type_"):
        if user_id != ADMIN_ID:
            return
        ctype = data[len("prod_type_"):]
        if ctype not in licensing.PRODUCT_TYPE_LABELS:
            await query.answer("Неизвестный тип", show_alert=True)
            return
        state = context.user_data.get("product_upload") or {}
        state["content_type"] = ctype
        state["step"] = "await_zip"
        context.user_data["product_upload"] = state
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        label = licensing.PRODUCT_TYPE_LABELS[ctype]
        await bot.send_message(
            user_id,
            f"📦 {label} выбран.\n\nПришли zip-архив (≤50MB) следующим сообщением.\n\n"
            "Отмена — /cancel_product",
        )
        return

    if data.startswith("prod_cancel_"):
        if user_id != ADMIN_ID:
            return
        token = data[len("prod_cancel_"):]
        pending_products.pop(token, None)
        _persist_pending_products()
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await bot.send_message(user_id, "❌ Продукт не сохранён")
        return

    if data.startswith("prod_save_"):
        if user_id != ADMIN_ID:
            return
        token = data[len("prod_save_"):]
        pending = pending_products.pop(token, None)
        _persist_pending_products()
        if not pending:
            await query.answer("⚠️ Токен устарел — пришли zip заново", show_alert=True)
            return
        meta = pending["meta"]
        # Генерируем ID по тому же паттерну что у битов (max+1 в кэше).
        if not beats_db.BEATS_CACHE:
            beats_db.load_beats()
        new_id = max([b["id"] for b in beats_db.BEATS_CACHE] + [0]) + 1
        entry = {
            "id": new_id,
            "msg_id": 0,
            "name": meta.name,
            "tags": [meta.content_type],
            "post_url": "",
            "bpm": None,
            "key": None,
            "file_id": pending["file_id"],
            "file_unique_id": pending.get("file_unique_id", ""),
            "content_type": meta.content_type,
            "classification_confidence": 1.0,
            "description": meta.description,
            "price_stars": meta.price_stars,
            "price_usdt": meta.price_usdt,
            "file_size": pending.get("file_size"),
            "file_name": pending.get("file_name"),
            "mime_type": pending.get("mime_type"),
        }
        try:
            beats_db.BEATS_CACHE.append(entry)
            beats_db._rebuild_index()
            beats_db.save_beats()
        except Exception as e:
            logger.exception("prod_save: beats_db append failed")
            await bot.send_message(user_id, f"❌ Не сохранил: {e}")
            return
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        label = licensing.PRODUCT_TYPE_LABELS[meta.content_type]
        post_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📡 Опубликовать промо в канал",
                                   callback_data=f"prod_post_{new_id}")],
            [InlineKeyboardButton("🔕 Без публикации",
                                   callback_data="prod_post_skip")],
        ])
        await bot.send_message(
            user_id,
            f"✅ Сохранено в каталог\n"
            f"📦 {label}: <b>{meta.name}</b>\n"
            f"🆔 id={new_id} · 💰 {meta.price_stars}⭐ / {meta.price_usdt:g} USDT",
            parse_mode="HTML",
            reply_markup=post_kb,
        )
        return

    if data == "prod_post_skip":
        if user_id != ADMIN_ID:
            return
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await bot.send_message(
            user_id,
            "🔕 Опубликуешь позже — кнопка будет в /admin → «📦 Kits & Packs» → карточка продукта.",
        )
        return

    if data.startswith("prod_post_"):
        if user_id != ADMIN_ID:
            return
        try:
            pid = int(data[len("prod_post_"):])
        except ValueError:
            return
        p = beats_db.get_beat_by_id(pid)
        if not p or p.get("content_type") not in licensing.PRODUCT_TYPE_LABELS:
            await query.answer("Продукт не найден", show_alert=True)
            return
        try:
            text = beat_post_builder.build_product_channel_post(p)
            channel_kb = beat_post_builder.build_product_channel_kb(p)
            sent = await bot.send_message(
                CHANNEL_ID, text,
                reply_markup=channel_kb,
                disable_web_page_preview=True,
            )
            # Запомним msg_id публикации в записи — чтобы в будущем можно
            # было редактировать/удалять этот пост.
            try:
                p["channel_msg_id"] = sent.message_id
                beats_db.save_beats()
            except Exception:
                logger.exception("save_beats после publish продукта: non-fatal")
        except Exception as e:
            logger.exception("prod_post: publish failed")
            await bot.send_message(user_id, f"❌ Не смог запостить в канал: {e}")
            return
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await bot.send_message(user_id, "✅ Промо-пост опубликован в канале")
        return

    if data == "pin_hub_cancel":
        if user_id != ADMIN_ID:
            return
        context.user_data.pop("pin_hub_text", None)
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await bot.send_message(user_id, "❌ Hub-пост не опубликован")
        return

    if data == "pin_hub_go":
        if user_id != ADMIN_ID:
            return
        text = context.user_data.pop("pin_hub_text", None)
        if not text:
            # Если кнопка залежалась — пересчитываем
            text = beat_post_builder.build_pinned_hub()
        try:
            sent = await bot.send_message(
                CHANNEL_ID, text, disable_web_page_preview=True,
            )
        except Exception as e:
            logger.exception("pin_hub: send to CHANNEL_ID failed")
            await bot.send_message(user_id, f"❌ Не смог запостить в канал: {e}")
            return
        try:
            await bot.pin_chat_message(
                CHANNEL_ID, sent.message_id, disable_notification=True,
            )
            pinned_note = "✅ Опубликовано и закреплено"
        except Exception as e:
            logger.warning("pin_hub: pin failed (бот не admin с pin-правами?): %s", e)
            pinned_note = (
                "✅ Опубликовано, но закрепить не смог — проверь права бота "
                f"в канале (нужно Pin Messages).\nDetail: {e}"
            )
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await bot.send_message(user_id, pinned_note)
        return

    if data == "check_sub":
        subscribed_users.discard(user_id)
        if await is_subscribed(bot, user_id):
            await query.message.delete()
            sp_state = await asyncio.to_thread(users_db.has_received_sample_pack, user_id)
            already = user_id in users_received_pack if sp_state is None else sp_state
            if not already:
                sent = await send_sample_pack(bot, user_id)
                if sent:
                    users_received_pack.add(user_id)
                    await asyncio.to_thread(users_db.mark_sample_pack_received, user_id)
                    asyncio.create_task(asyncio.to_thread(save_users))
            await show_main_menu(bot, user_id)
        else:
            await query.answer("Ты ещё не подписан! Подпишись и нажми снова.", show_alert=True)
        return

    if data == "main_menu":
        beats = len([b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == "beat"])
        tracks = len([b for b in beats_db.BEATS_CACHE if b.get("content_type") == "track"])
        remixes = len([b for b in beats_db.BEATS_CACHE if b.get("content_type") == "remix"])
        text = "Привет! 👋 Что слушаем сегодня?\n\nВ каталоге: " + str(beats) + " битов, " + str(tracks) + " треков, " + str(remixes) + " ремиксов.\nВыбирай по настроению или жми случайный — не прогадаешь 🎲"
        await _nav_reply(query, text, reply_markup=kb_main_menu(user_id=user_id))
        return

    if data == "menu_beat":
        beats = len([b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == "beat"])
        await _nav_reply(query, "🎹 Целых " + str(beats) + " битов! Ищешь что-то конкретное или просто серфишь?", reply_markup=kb_beats_menu())
        return

    if data == "menu_products":
        # Раздел продуктов — показываем 3 подтипа с counter'ами.
        counts = {"drumkit": 0, "samplepack": 0, "looppack": 0}
        for b in beats_db.BEATS_CACHE:
            ct = b.get("content_type")
            if ct in counts:
                counts[ct] += 1
        total = sum(counts.values())
        if total == 0:
            await _nav_reply(
                query,
                "📦 Паков и китов пока нет — скоро будет.\n"
                "Пока лучшее — биты: тыкай 🎹 Биты в главном меню.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("◀️ Главное меню", callback_data="main_menu")],
                ]),
            )
            return
        rows = []
        if counts["drumkit"]:
            rows.append([InlineKeyboardButton(
                f"🥁 Drum Kits ({counts['drumkit']})",
                callback_data="prodcat_drumkit_0",
            )])
        if counts["samplepack"]:
            rows.append([InlineKeyboardButton(
                f"🎵 Sample Packs ({counts['samplepack']})",
                callback_data="prodcat_samplepack_0",
            )])
        if counts["looppack"]:
            rows.append([InlineKeyboardButton(
                f"🔄 Loop Packs ({counts['looppack']})",
                callback_data="prodcat_looppack_0",
            )])
        rows.append([InlineKeyboardButton("◀️ Главное меню", callback_data="main_menu")])
        await _nav_reply(
            query,
            f"📦 Паки и киты ({total}) — выбирай категорию:",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data.startswith("prodcat_"):
        # prodcat_<type>_<page> — пагинация по типу продукта.
        rest = data[len("prodcat_"):]
        try:
            ctype, page_s = rest.rsplit("_", 1)
            page = int(page_s)
        except ValueError:
            return
        if ctype not in licensing.PRODUCT_TYPE_LABELS:
            return
        items = sorted(
            [b for b in beats_db.BEATS_CACHE if b.get("content_type") == ctype],
            key=lambda x: x["id"], reverse=True,
        )
        per_page = 6
        total_pages = max(1, (len(items) + per_page - 1) // per_page)
        page = max(0, min(page, total_pages - 1))
        chunk = items[page * per_page:(page + 1) * per_page]
        rows = []
        for p in chunk:
            stars = p.get("price_stars", "?")
            rows.append([InlineKeyboardButton(
                f"{p['name']} — {stars}⭐",
                callback_data=f"prodview_{p['id']}",
            )])
        # Nav row
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"prodcat_{ctype}_{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"prodcat_{ctype}_{page+1}"))
        if nav:
            rows.append(nav)
        rows.append([InlineKeyboardButton("◀️ К категориям", callback_data="menu_products")])
        label = licensing.PRODUCT_TYPE_LABELS[ctype]
        await _nav_reply(
            query,
            f"{label}s — {len(items)} шт.\nТыкай название чтобы посмотреть.",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data.startswith("prodview_"):
        try:
            pid = int(data[len("prodview_"):])
        except ValueError:
            return
        p = beats_db.get_beat_by_id(pid)
        if not p or p.get("content_type") not in licensing.PRODUCT_TYPE_LABELS:
            await query.answer("Продукт не найден", show_alert=True)
            return
        label = licensing.PRODUCT_TYPE_LABELS[p["content_type"]]
        size_mb = (p.get("file_size") or 0) / (1024 * 1024) if p.get("file_size") else 0
        stars = p.get("price_stars", "?")
        usdt = p.get("price_usdt", "?")
        info = (
            f"📦 <b>{label}</b>\n"
            f"🎯 <b>{p['name']}</b>\n"
            f"📎 {size_mb:.1f} MB\n\n"
            f"{p.get('description') or '<i>(без описания)</i>'}\n\n"
            f"💎 WAV / Trackouts / Exclusive — DM @iiiplfiii"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"⭐ {stars}", callback_data=f"buy_prod_{pid}"),
             InlineKeyboardButton(f"💵 {usdt:g} USDT" if isinstance(usdt, (int, float)) else "💵 USDT",
                                   callback_data=f"buy_prod_usdt_{pid}")],
            [InlineKeyboardButton(f"◀️ К {label}s",
                                   callback_data=f"prodcat_{p['content_type']}_0")],
        ])
        await _nav_reply(query, info, reply_markup=kb, parse_mode="HTML")
        return

    if data == "noop":
        await query.answer()
        return

    if data == "menu_track":
        tracks = [b for b in beats_db.BEATS_CACHE if b.get("content_type") == "track"]
        if not tracks:
            await query.answer("Треков пока нет!", show_alert=True)
            return
        await _nav_reply(query, "🎤 " + str(len(tracks)) + " треков от IIIPLKIII — слушай на здоровье!", reply_markup=kb_tracks_menu())
        return

    if data == "menu_remix":
        remixes = [b for b in beats_db.BEATS_CACHE if b.get("content_type") == "remix"]
        if not remixes:
            await query.answer("Ремиксов пока нет!", show_alert=True)
            return
        await _nav_reply(query, "🔀 " + str(len(remixes)) + " ремиксов — узнаешь мелодию? 😄", reply_markup=kb_remixes_menu())
        return

    if data == "beats_by_artist":
        await _nav_reply(query, "🎤 Выбирай тайп — найду похожие биты!", reply_markup=kb_artists())
        return

    if data.startswith("cattag_"):
        parts = data.split("_", 2)
        content_type, tag = parts[1], parts[2]
        items = [b for b in beats_db.BEATS_CACHE
                 if b.get("content_type", "beat") == content_type and tag in b.get("tags", [])]
        if not items:
            await query.answer("Ничего не найдено!", show_alert=True)
            return
        history = get_history(user_id)
        available = [b for b in items if b["id"] not in history]
        beat = random.choice(available) if available else random.choice(items)
        await send_beat(bot, query.message.chat_id, beat, user_id)
        return

    if data.startswith("randcat_"):
        content_type = data[8:]
        items = [b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == content_type]
        if not items:
            await query.answer("Пока пусто!", show_alert=True)
            return
        history = get_history(user_id)
        available = [b for b in items if b["id"] not in history]
        beat = random.choice(available) if available else random.choice(items)
        await send_beat(bot, query.message.chat_id, beat, user_id)
        return

    if data.startswith("next_"):
        beat_id = int(data.split("_")[1])
        current = beats_db.get_beat_by_id(beat_id)
        if not current:
            return
        content_type = current.get("content_type", "beat")
        history = get_history(user_id)
        similar = beats_db.get_similar_beats(current, exclude_ids=history)
        similar = [b for b in similar if b.get("content_type", "beat") == content_type]
        if not similar:
            items = [b for b in beats_db.BEATS_CACHE
                     if b.get("content_type", "beat") == content_type and b["id"] not in history]
            if not items:
                items = [b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == content_type]
            next_beat = random.choice(items) if items else None
        else:
            next_beat = random.choice(similar)
        if not next_beat:
            await query.answer("Больше нет!", show_alert=True)
            return
        await send_beat(bot, query.message.chat_id, next_beat, user_id)
        return

    if data == "random_beat":
        beat = beats_db.get_random_beat(exclude_ids=get_history(user_id))
        if not beat:
            return
        await send_beat(bot, query.message.chat_id, beat, user_id)
        return

    if data.startswith("fav_"):
        beat_id = int(data.split("_")[1])
        if user_id not in user_favorites:
            user_favorites[user_id] = []
        if beat_id not in user_favorites[user_id]:
            user_favorites[user_id].append(beat_id)
            # Supabase write-through (primary truth); local JSON — backup
            asyncio.create_task(asyncio.to_thread(
                users_db.set_favorites, user_id, user_favorites[user_id]
            ))
            asyncio.create_task(asyncio.to_thread(save_users))
            await query.answer("❤️ Добавлено в избранное!")
        else:
            await query.answer("Уже в избранном!")
        return

    if data == "my_favorites":
        favs = user_favorites.get(user_id, [])
        if not favs:
            await _nav_reply(query, "Тут пока пусто 🙈\nСлушай биты и жми ❤️ — сохраню сюда!",
                             reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Меню", callback_data="main_menu")]]))
            return
        beats_list = [beats_db.get_beat_by_id(bid) for bid in favs]
        beats_list = [b for b in beats_list if b]
        rows = [[InlineKeyboardButton(b["name"][:40], callback_data="play_" + str(b["id"]))] for b in beats_list[-10:]]
        rows.append([InlineKeyboardButton("◀️ Меню", callback_data="main_menu")])
        await _nav_reply(query, "❤️ Избранное (" + str(len(beats_list)) + "):", reply_markup=InlineKeyboardMarkup(rows))
        return

    if data.startswith("play_"):
        beat = beats_db.get_beat_by_id(int(data.split("_")[1]))
        if beat:
            await send_beat(bot, query.message.chat_id, beat, user_id)
        return

    if data.startswith("buy_usdt_"):
        try:
            beat_id = int(data[len("buy_usdt_"):])
        except ValueError:
            return
        beat = beats_db.get_beat_by_id(beat_id)
        if not beat:
            await query.answer("Бит не найден", show_alert=True)
            return
        bpm = beat.get("bpm") or "?"
        key = beat.get("key") or "?"
        try:
            inv = await cryptobot.create_invoice(
                amount=licensing.PRICE_MP3_USDT,
                asset="USDT",
                description=f"MP3 Lease на «{beat['name']}» ({bpm} BPM, {key})",
                payload=f"mp3_lease:{beat_id}:{user_id}",
            )
        except Exception as e:
            logger.exception("cryptobot.create_invoice failed")
            await query.answer(_user_error_msg("оплата"), show_alert=True)
            return
        pay_url = inv.get("pay_url") or inv.get("mini_app_invoice_url") or inv.get("bot_invoice_url")
        invoice_id = int(inv.get("invoice_id"))
        pending_usdt_invoices[invoice_id] = {"user_id": user_id, "beat_id": beat_id}
        try:
            await bot.send_message(
                user_id,
                f"💵 Счёт на {licensing.PRICE_MP3_USDT:g} USDT за «{beat['name']}»\n\n"
                f"Жми кнопку ниже — откроется CryptoBot, оплати из @wallet.\n"
                f"Как пройдёт оплата — бит и лицензия автоматом придут сюда (ждать 10-30 сек).\n\n"
                f"⏱ Счёт активен 30 минут.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💳 Оплатить в CryptoBot", url=pay_url)]]),
            )
            if query.message.chat_id != user_id:
                await query.answer("💵 Счёт отправил в ЛС бота", show_alert=False)
        except TelegramError as e:
            logger.warning("send USDT invoice msg failed: %s", e)
            await query.answer("Сначала напиши /start боту в ЛС — тогда пришлю счёт.", show_alert=True)
            return
        asyncio.create_task(poll_usdt_invoice(bot, invoice_id, user_id, beat_id))
        return

    # ── Покупка продукта (drum kit / sample pack / loop pack)
    #    buy_prod_usdt_<id> — USDT через Cryptobot, buy_prod_<id> — Telegram Stars
    if data.startswith("buy_prod_usdt_"):
        try:
            pid = int(data[len("buy_prod_usdt_"):])
        except ValueError:
            return
        product = beats_db.get_beat_by_id(pid)
        if not product or product.get("content_type") not in licensing.PRODUCT_TYPE_LABELS:
            await query.answer("Продукт не найден", show_alert=True)
            return
        type_label = licensing.PRODUCT_TYPE_LABELS[product["content_type"]]
        usdt = float(product.get("price_usdt") or licensing.DEFAULT_PRICES[product["content_type"]][1])
        try:
            inv = await cryptobot.create_invoice(
                amount=usdt,
                asset="USDT",
                description=f"{type_label}: «{product['name']}»",
                payload=f"product:{pid}:{user_id}",
            )
        except Exception as e:
            logger.exception("cryptobot.create_invoice failed (product)")
            await query.answer(_user_error_msg("оплата"), show_alert=True)
            return
        pay_url = inv.get("pay_url") or inv.get("mini_app_invoice_url") or inv.get("bot_invoice_url")
        invoice_id = int(inv.get("invoice_id"))
        pending_usdt_invoices[invoice_id] = {"user_id": user_id, "beat_id": pid, "kind": "product"}
        try:
            await bot.send_message(
                user_id,
                f"💵 Счёт на {usdt:g} USDT — {type_label}: «{product['name']}»\n\n"
                f"Жми кнопку — откроется CryptoBot, оплата из @wallet.\n"
                f"Как пройдёт — zip и лицензия автоматом придут сюда (ждать 10-30 сек).\n\n"
                f"⏱ Счёт активен 30 минут.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💳 Оплатить в CryptoBot", url=pay_url)]]),
            )
            if query.message.chat_id != user_id:
                await query.answer("💵 Счёт отправил в ЛС бота", show_alert=False)
        except TelegramError as e:
            logger.warning("send USDT invoice msg failed (product): %s", e)
            await query.answer("Сначала напиши /start боту в ЛС — тогда пришлю счёт.", show_alert=True)
            return
        asyncio.create_task(poll_usdt_invoice(bot, invoice_id, user_id, pid, kind="product"))
        return

    if data.startswith("buy_prod_"):
        try:
            pid = int(data[len("buy_prod_"):])
        except ValueError:
            return
        product = beats_db.get_beat_by_id(pid)
        if not product or product.get("content_type") not in licensing.PRODUCT_TYPE_LABELS:
            await query.answer("Продукт не найден", show_alert=True)
            return
        type_label = licensing.PRODUCT_TYPE_LABELS[product["content_type"]]
        stars = int(product.get("price_stars") or licensing.DEFAULT_PRICES[product["content_type"]][0])
        title = f"{type_label} — {product['name']}"[:32]
        description = (
            f"{type_label}: «{product['name']}». Non-exclusive: безлимит в своих треках, "
            f"запрет перепродажи сэмплов. После оплаты — zip + txt-лицензия в ЛС."
        )[:255]
        try:
            await bot.send_invoice(
                chat_id=user_id,
                title=title,
                description=description,
                payload=f"product:{pid}",
                provider_token="",
                currency="XTR",
                prices=[LabeledPrice(label=type_label, amount=stars)],
            )
            if query.message.chat_id != user_id:
                await query.answer("💰 Счёт отправил в ЛС бота", show_alert=False)
        except TelegramError as e:
            logger.warning("send_invoice failed for user %s (product): %s", user_id, e)
            await query.answer(
                "Сначала напиши /start боту в ЛС — тогда пришлю счёт.",
                show_alert=True,
            )
        except Exception as e:
            logger.exception("send_invoice error (product)")
            await query.answer(_user_error_msg("оплата"), show_alert=True)
        return

    if data.startswith("excl_"):
        # Exclusive inquiry: юзер кликнул «💎 Exclusive» → просим описать проект.
        # Следующее его текстовое сообщение поймает handle_assistant (pending_excl state)
        # и форварднёт админу с beat summary.
        try:
            beat_id = int(data[len("excl_"):])
        except ValueError:
            return
        beat = beats_db.get_beat_by_id(beat_id)
        if not beat:
            await query.answer("Бит не найден", show_alert=True)
            return
        context.user_data["pending_excl"] = {
            "beat_id": beat_id,
            "beat_name": beat.get("name", "?"),
            "bpm": beat.get("bpm", "?"),
            "key": beat.get("key", "?"),
            "post_url": beat.get("post_url", ""),
            "asked_at": datetime.now(ZoneInfo("Europe/Moscow")).isoformat(),
        }
        await query.answer()
        await bot.send_message(
            user_id,
            f"💎 Exclusive rights на «{beat.get('name', '?')}».\n\n"
            "Опиши проект одним сообщением:\n"
            "• Артист (или исполнитель)\n"
            "• Стиль / жанр\n"
            "• Примерная дата релиза\n"
            "• Бюджет если есть ориентир\n\n"
            "После отправки TRIPLE FILL свяжется в ЛС в течение 24 ч.\n"
            "<i>Отменить: /cancel_excl</i>",
            parse_mode="HTML",
        )
        return

    if data == "menu_services":
        # Подробное описание всех товаров/услуг. Нужно для YooKassa KYC и для
        # юзеров чтобы понимать разницу между MP3/WAV/Exclusive/Kits/Mixing.
        text = (
            "<b>ℹ️ Услуги и цены TRIPLE FILL</b>\n\n"

            "🎧 <b>MP3 Lease — 1500⭐ / 20 USDT / 1700₽</b>\n"
            "Лицензия на использование бита в своих треках. После оплаты мгновенно "
            "приходит MP3 (untagged, без водяного знака) + TXT-файл лицензии.\n"
            "• До 100 000 стримов, до 2 000 копий\n"
            "• 1 music video (без монетизации на YT)\n"
            "• Указать credit: <i>prod. by TRIPLE FILL</i>\n"
            "• Non-exclusive (бит может быть куплен и другими)\n\n"

            "💎 <b>WAV / Unlimited / Exclusive — от $150 до $1500</b>\n"
            "WAV + стемы, снятие лимитов по стримам/копиям, или полная эксклюзивность "
            "(бит снимается с продажи). Обсуждается в ЛС @iiiplfiii.\n\n"

            "📦 <b>Drum Kits / Sample Packs / Loop Packs — 1000⭐-1500⭐</b>\n"
            "Готовые наборы ударных, сэмплов, лупов для продюсеров. После оплаты — "
            "zip-файл мгновенно в ЛС. Смотри каталог «📦 Kits &amp; Packs» в меню.\n\n"

            f"🎛 <b>Сведение треков (mixing + mastering) — "
            f"{licensing.PRICE_MIX_STARS}⭐ / {licensing.PRICE_MIX_USDT:g} USDT / {licensing.PRICE_MIX_RUB}₽</b>\n"
            "Готовый master-файл твоего трека за 3-5 рабочих дней.\n"
            "• Что ты присылаешь: стемы WAV (вокал, биты, инструменты отдельно)\n"
            "• Что получаешь: mix-mastered WAV (24-bit, -14 LUFS для стримов)\n"
            "• Оплата сначала в боте → потом стемы в DM @iiiplfiii\n"
            "Заказать: кнопка «🎛 Сведение треков» в главном меню.\n\n"

            "<b>Как купить</b>\n"
            "⭐ Stars — через Telegram native payment (международно)\n"
            "💵 USDT — через CryptoBot (TRON/TON)\n"
            "💳 1700₽ / 5000₽ — карта МИР / Visa / СБП через YooKassa\n\n"

            "Вопросы — пиши @iiiplfiii"
        )
        await _nav_reply(
            query, text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"🎛 Заказать сведение ({licensing.PRICE_MIX_RUB}₽)", callback_data="menu_mixing")],
                [InlineKeyboardButton("🎹 К битам", callback_data="menu_beat"),
                 InlineKeyboardButton("📦 Kits & Packs", callback_data="menu_products")],
                [InlineKeyboardButton("◀️ Главное меню", callback_data="main_menu")],
            ]),
        )
        return

    if data == "menu_mixing":
        # Mixing service: отдельный товар (не бит). Покупка через Stars/USDT/RUB,
        # после оплаты — клиент высылает стемы в DM @iiiplfiii.
        text = (
            "<b>🎛 Сведение треков (mix + master)</b>\n\n"
            f"<b>Цена: {licensing.PRICE_MIX_STARS}⭐ / {licensing.PRICE_MIX_USDT:g} USDT / "
            f"{licensing.PRICE_MIX_RUB}₽</b> за 1 трек «под ключ»\n\n"

            "<b>Что входит:</b>\n"
            "• Сведение (балансы, панорама, компрессия, EQ, эффекты)\n"
            "• Мастеринг (loudness −14 LUFS для стримов, пиковая −1 dBTP)\n"
            "• 1 ревизия правок если что-то нужно подправить\n\n"

            "<b>Срок:</b> 3-5 рабочих дней с момента получения стемов.\n\n"

            "<b>Что от тебя:</b>\n"
            "1. Оплатить заказ (кнопки ниже)\n"
            "2. Прислать в DM @iiiplfiii:\n"
            "   — стемы в формате WAV (24-bit, 44.1 kHz)\n"
            "   — референс-трек (на что ориентируемся)\n"
            "   — краткое ТЗ: жанр, акценты, пожелания\n\n"

            "<b>Что получаешь:</b> готовый master WAV + ревизия в цене.\n\n"
            "<i>Вопросы до оплаты — пиши @iiiplfiii</i>"
        )
        rows = [
            [InlineKeyboardButton(f"⭐ {licensing.PRICE_MIX_STARS}", callback_data="buy_mix_stars"),
             InlineKeyboardButton(f"💵 {licensing.PRICE_MIX_USDT:g} USDT", callback_data="buy_mix_usdt")],
            [InlineKeyboardButton(f"💳 {licensing.PRICE_MIX_RUB}₽ (MIR/СБП)", callback_data="buy_mix_rub")],
            [InlineKeyboardButton("◀️ Главное меню", callback_data="main_menu")],
        ]
        await _nav_reply(query, text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows))
        return

    if data == "buy_mix_stars":
        # Mixing payment через Telegram Stars
        try:
            await bot.send_invoice(
                chat_id=user_id,
                title="🎛 Сведение трека"[:32],
                description=(
                    f"Mix + master твоего трека за 3-5 рабочих дней. "
                    f"После оплаты пришли стемы WAV в DM @iiiplfiii. "
                    f"Готовый master WAV + 1 ревизия в цене."
                )[:255],
                payload="mixing_service:stars",
                provider_token="",
                currency="XTR",
                prices=[LabeledPrice(label="Mixing & Mastering", amount=licensing.PRICE_MIX_STARS)],
            )
            await query.answer()
        except TelegramError as e:
            logger.warning("send_invoice mixing stars failed for %s: %s", user_id, e)
            await query.answer("Сначала напиши /start в ЛС", show_alert=True)
        except Exception:
            logger.exception("send_invoice mixing stars error")
            await query.answer(_user_error_msg("оплата"), show_alert=True)
        return

    if data == "buy_mix_usdt":
        # Mixing payment через CryptoBot USDT
        try:
            invoice = await cryptobot.create_invoice(
                amount=licensing.PRICE_MIX_USDT,
                description=f"Mixing & Mastering — TRIPLE FILL",
                payload=f"mixing_service:{user_id}",
            )
            if invoice:
                invoice_id = invoice["invoice_id"]
                pay_url = invoice["pay_url"]
                pending_usdt_invoices[invoice_id] = {
                    "user_id": user_id, "beat_id": 0, "kind": "mixing",
                    "created_at": datetime.now(),
                }
                await bot.send_message(
                    user_id,
                    f"💵 Оплата {licensing.PRICE_MIX_USDT:g} USDT через CryptoBot\n\n"
                    f"<a href='{pay_url}'>Перейти к оплате</a>\n\n"
                    "После оплаты автоматически придёт подтверждение.",
                    parse_mode="HTML", disable_web_page_preview=False,
                )
                await query.answer()
                asyncio.create_task(poll_usdt_invoice(bot, invoice_id, user_id, 0, kind="mixing"))
            else:
                await query.answer("CryptoBot недоступен, попробуй Stars или RUB", show_alert=True)
        except Exception:
            logger.exception("mixing USDT invoice error")
            await query.answer(_user_error_msg("оплата"), show_alert=True)
        return

    if data == "buy_mix_rub":
        # Mixing payment через YooKassa REST API (все методы: карты MIR/Visa/MC,
        # СБП, T-Pay, SberPay, ЮMoney). Клиент переходит на YooKassa страницу,
        # оплачивает, webhook доставляет услугу.
        import yookassa_api
        if not yookassa_api.is_configured():
            await query.answer(
                "RUB-оплата пока не подключена. Используй ⭐ Stars или 💵 USDT.",
                show_alert=True,
            )
            return
        # Guard от multi-click — без него 3 тапа за секунду = 3 pending в YK +
        # 3 message в ЛС. Юзер-репорт: «сведение треков — 5000₽» дважды подряд.
        if user_id in _yk_creating_payment:
            await query.answer("⏳ Уже создаю платёж, подожди...", show_alert=False)
            return
        _yk_creating_payment.add(user_id)
        try:
            payment = await yookassa_api.create_payment(
                amount_rub=licensing.PRICE_MIX_RUB,
                description=f"Mixing & Mastering — TRIPLE FILL",
                metadata={
                    "type": "mixing_service",
                    "user_id": str(user_id),
                    "username": update.effective_user.username or "",
                },
                return_url=f"https://t.me/{beat_post_builder.BOT_USERNAME}",
            )
            await _save_yk_pending(payment["id"], {
                "type": "mixing_service",
                "user_id": user_id,
                "username": update.effective_user.username or "",
                "amount_rub": licensing.PRICE_MIX_RUB,
                "created_at": datetime.now(ZoneInfo("Europe/Moscow")).isoformat(),
            })
            pay_url = payment["confirmation"]["confirmation_url"]
            await bot.send_message(
                user_id,
                f"🎛 <b>Сведение треков — {licensing.PRICE_MIX_RUB}₽</b>\n\n"
                "Нажми кнопку ниже → выбери удобный способ оплаты (карта, "
                "СБП, T-Pay, SberPay, ЮMoney) → подтверди платёж.\n\n"
                "После оплаты автоматически придёт инструкция что прислать "
                "(стемы WAV, референс, ТЗ).",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"💳 Оплатить {licensing.PRICE_MIX_RUB}₽", url=pay_url)],
                ]),
            )
            await query.answer()
        except Exception:
            logger.exception("YK create_payment (mixing) failed")
            await query.answer(_user_error_msg("оплата"), show_alert=True)
        finally:
            _yk_creating_payment.discard(user_id)
        return

    # ── Cart (накопительная корзина для bundle) ────────────────
    if data.startswith("cart_add_"):
        try:
            beat_id = int(data[len("cart_add_"):])
        except ValueError:
            await query.answer()
            return
        beat = beats_db.get_beat_by_id(beat_id)
        if not beat or beat.get("content_type", "beat") != "beat":
            await query.answer("Бит не найден", show_alert=True)
            return
        ok, msg = _cart_add(user_id, beat_id)
        if not ok:
            await query.answer(msg, show_alert=False)
            return
        cart = _cart_get(user_id)
        # Показываем alert + если в корзине достигли BUNDLE_TOTAL — приглашаем купить
        if len(cart) >= BUNDLE_TOTAL:
            await query.answer(
                f"🎁 В корзине {len(cart)} бит(ов)! Открой /cart чтобы купить.",
                show_alert=True,
            )
        else:
            need = BUNDLE_TOTAL - len(cart)
            await query.answer(
                f"✅ Добавлен в корзину ({len(cart)}/{BUNDLE_TOTAL}). Ещё {need} → bundle.",
                show_alert=False,
            )
        # Обновляем карточку — кнопка должна стать «✅ В корзине»
        try:
            ct = beat.get("content_type", "beat")
            await query.message.edit_reply_markup(
                reply_markup=kb_after_beat(beat_id, ct, user_id=user_id),
            )
        except TelegramError:
            pass
        return

    if data.startswith("cart_remove_"):
        try:
            beat_id = int(data[len("cart_remove_"):])
        except ValueError:
            await query.answer()
            return
        was = _cart_remove(user_id, beat_id)
        await query.answer("🗑 Удалён" if was else "Не было в корзине", show_alert=False)
        # Перерисуем cart-view
        await _send_cart_view(bot, user_id, edit_message=query.message)
        return

    if data == "cart_clear":
        _cart_clear(user_id)
        await query.answer("🗑 Корзина очищена")
        await _send_cart_view(bot, user_id, edit_message=query.message)
        return

    if data == "cart_show":
        await _send_cart_view(bot, user_id, edit_message=query.message)
        return

    if data == "cart_buy":
        cart = _cart_get(user_id)
        if len(cart) < BUNDLE_TOTAL:
            await query.answer(
                f"Нужно {BUNDLE_TOTAL} бита в корзине, у тебя {len(cart)}.",
                show_alert=True,
            )
            return
        # Берём первые BUNDLE_TOTAL битов из корзины (порядок добавления).
        # Юзер может перед buy удалить ❌ ненужные — picker остаётся в корзине.
        chosen_ids = cart[:BUNDLE_TOTAL]
        chosen_beats = [beats_db.get_beat_by_id(bid) for bid in chosen_ids]
        chosen_beats = [b for b in chosen_beats if b]
        if len(chosen_beats) != BUNDLE_TOTAL:
            await query.answer(
                "Часть битов из корзины пропала — обнови /cart.",
                show_alert=True,
            )
            return
        # Кладём в bundle_selection как если бы юзер прошёл picker — переиспользуем
        # bundle_pay_* флоу. anchor = первый, selected = остальные.
        bundle_selection[user_id] = {
            "anchor": int(chosen_beats[0]["id"]),
            "selected": [int(b["id"]) for b in chosen_beats[1:]],
            "page": 0,
        }
        names_block = "\n".join(f"• {b['name']}" for b in chosen_beats)
        try:
            await query.message.edit_text(
                (
                    f"🎁 <b>Bundle из корзины — {licensing.PRICE_BUNDLE3_RUB}₽</b>\n\n"
                    f"<b>3 бита:</b>\n{names_block}\n\n"
                    "Выбери способ оплаты:"
                ),
                parse_mode="HTML",
                reply_markup=kb_bundle_pay(),
            )
        except TelegramError:
            pass
        return

    # ── Bundle deals (3 бита одной транзакцией) ────────────────
    if data.startswith("bundle_start_"):
        try:
            anchor_id = int(data[len("bundle_start_"):])
        except ValueError:
            await query.answer()
            return
        anchor = beats_db.get_beat_by_id(anchor_id)
        if not anchor or anchor.get("content_type", "beat") != "beat":
            await query.answer("Бит не найден", show_alert=True)
            return
        eligible_n = len(_bundle_eligible_beats(exclude_ids={anchor_id}))
        if eligible_n < BUNDLE_TOTAL - 1:
            await query.answer(
                f"В каталоге слишком мало битов для бандла (нужно ≥{BUNDLE_TOTAL}).",
                show_alert=True,
            )
            return
        bundle_selection[user_id] = {"anchor": anchor_id, "selected": [], "page": 0}
        anchor_name = anchor.get("name") or "?"
        anchor_bpm = anchor.get("bpm") or "?"
        try:
            await bot.send_message(
                user_id,
                (
                    f"🎁 <b>Bundle 3 бита — {licensing.PRICE_BUNDLE3_RUB}₽</b>\n"
                    f"(вместо 3×{licensing.PRICE_MP3_RUB} = {3 * licensing.PRICE_MP3_RUB}₽, экономия "
                    f"{3 * licensing.PRICE_MP3_RUB - licensing.PRICE_BUNDLE3_RUB}₽)\n\n"
                    f"<b>Anchor:</b> {anchor_name} ({anchor_bpm} BPM)\n\n"
                    f"Выбери ещё {BUNDLE_TOTAL - 1} бита из каталога:"
                ),
                parse_mode="HTML",
                reply_markup=kb_bundle_picker(user_id),
            )
            if query.message.chat_id != user_id:
                await query.answer("📨 Picker отправил в ЛС бота", show_alert=False)
        except TelegramError:
            await query.answer("Сначала напиши /start боту в ЛС.", show_alert=True)
            bundle_selection.pop(user_id, None)
        return

    if data.startswith("bundle_pick_") or data.startswith("bundle_unpick_"):
        is_pick = data.startswith("bundle_pick_")
        prefix_len = len("bundle_pick_") if is_pick else len("bundle_unpick_")
        try:
            bid = int(data[prefix_len:])
        except ValueError:
            await query.answer()
            return
        state = bundle_selection.get(user_id)
        if not state:
            await query.answer("Сессия устарела, нажми «🎁 3 бита» заново.", show_alert=True)
            return
        selected = list(state.get("selected", []))
        if is_pick:
            if bid in selected:
                pass
            elif len(selected) >= BUNDLE_TOTAL - 1:
                await query.answer(
                    f"Уже выбрано {BUNDLE_TOTAL - 1}, дальше — ✅ Купить.",
                    show_alert=False,
                )
                return
            elif bid == int(state.get("anchor", 0)):
                await query.answer("Этот бит уже якорь bundle.", show_alert=False)
                return
            else:
                selected.append(bid)
        else:
            selected = [x for x in selected if x != bid]
        state["selected"] = selected
        bundle_selection[user_id] = state
        try:
            await query.message.edit_reply_markup(reply_markup=kb_bundle_picker(user_id))
        except TelegramError:
            pass
        return

    if data.startswith("bundle_page_"):
        try:
            page = max(0, int(data[len("bundle_page_"):]))
        except ValueError:
            await query.answer()
            return
        state = bundle_selection.get(user_id)
        if not state:
            await query.answer("Сессия устарела, нажми «🎁 3 бита» заново.", show_alert=True)
            return
        state["page"] = page
        bundle_selection[user_id] = state
        try:
            await query.message.edit_reply_markup(reply_markup=kb_bundle_picker(user_id))
        except TelegramError:
            pass
        return

    if data == "bundle_noop":
        return  # query.answer() уже сделан в начале handle_callback

    if data == "bundle_cancel":
        bundle_selection.pop(user_id, None)
        try:
            await query.message.edit_text("❌ Bundle отменён.")
        except TelegramError:
            pass
        return

    if data == "bundle_confirm":
        resolved = _bundle_anchor_and_selected(user_id)
        if not resolved:
            await query.answer(
                f"Сначала выбери {BUNDLE_TOTAL - 1} бита.", show_alert=True,
            )
            return
        anchor, selected_beats = resolved
        names_block = (
            f"• {anchor['name']}\n" + "\n".join(f"• {b['name']}" for b in selected_beats)
        )
        try:
            await query.message.edit_text(
                (
                    f"🎁 <b>Bundle готов — {licensing.PRICE_BUNDLE3_RUB}₽</b>\n\n"
                    f"<b>3 бита:</b>\n{names_block}\n\n"
                    "Выбери способ оплаты:"
                ),
                parse_mode="HTML",
                reply_markup=kb_bundle_pay(),
            )
        except TelegramError:
            pass
        return

    if data == "bundle_pay_stars":
        resolved = _bundle_anchor_and_selected(user_id)
        if not resolved:
            await query.answer("Сессия устарела, начни заново через «🎁 3 бита».", show_alert=True)
            return
        anchor, selected_beats = resolved
        ids_csv = ",".join(str(b["id"]) for b in [anchor, *selected_beats])
        title = "Bundle 3 бита"
        description = (
            f"3 MP3 Lease beats: {anchor['name']} + " +
            ", ".join(b["name"] for b in selected_beats)
        )[:255]
        try:
            await bot.send_invoice(
                chat_id=user_id,
                title=title[:32],
                description=description,
                payload=f"bundle:{ids_csv}",
                provider_token="",
                currency="XTR",
                prices=[LabeledPrice(label="Bundle 3 бита", amount=licensing.PRICE_BUNDLE3_STARS)],
            )
            if query.message.chat_id != user_id:
                await query.answer("💰 Счёт отправил в ЛС бота", show_alert=False)
        except TelegramError as e:
            logger.warning("send bundle Stars invoice failed: %s", e)
            await query.answer("Не удалось отправить счёт. Напиши /start боту.", show_alert=True)
        return

    if data == "bundle_pay_usdt":
        resolved = _bundle_anchor_and_selected(user_id)
        if not resolved:
            await query.answer("Сессия устарела, начни заново через «🎁 3 бита».", show_alert=True)
            return
        anchor, selected_beats = resolved
        ids_csv = ",".join(str(b["id"]) for b in [anchor, *selected_beats])
        beats_for_invoice = [anchor, *selected_beats]
        try:
            inv = await cryptobot.create_invoice(
                amount=licensing.PRICE_BUNDLE3_USDT,
                asset="USDT",
                description=f"Bundle 3 бита: {anchor['name']} + 2",
                payload=f"bundle:{ids_csv}:{user_id}",
            )
        except Exception:
            logger.exception("cryptobot.create_invoice failed (bundle)")
            await query.answer(_user_error_msg("оплата"), show_alert=True)
            return
        pay_url = inv.get("pay_url") or inv.get("mini_app_invoice_url") or inv.get("bot_invoice_url")
        invoice_id = int(inv.get("invoice_id"))
        pending_usdt_invoices[invoice_id] = {
            "user_id": user_id,
            "kind": "bundle",
            "beat_ids": [int(b["id"]) for b in beats_for_invoice],
        }
        try:
            await bot.send_message(
                user_id,
                (
                    f"💵 Счёт на {licensing.PRICE_BUNDLE3_USDT:g} USDT — bundle 3 бита\n\n"
                    "Жми кнопку ниже — откроется CryptoBot, оплати из @wallet.\n"
                    "После оплаты автоматом придут 3 mp3 + общая лицензия.\n\n"
                    "⏱ Счёт активен 30 минут."
                ),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💳 Оплатить в CryptoBot", url=pay_url)]]),
            )
            if query.message.chat_id != user_id:
                await query.answer("💵 Счёт отправил в ЛС бота", show_alert=False)
        except TelegramError as e:
            logger.warning("send bundle USDT invoice msg failed: %s", e)
            await query.answer("Сначала напиши /start боту в ЛС.", show_alert=True)
            return
        asyncio.create_task(poll_usdt_invoice(bot, invoice_id, user_id, 0, kind="bundle"))
        return

    if data == "bundle_pay_rub":
        import yookassa_api
        if not yookassa_api.is_configured():
            await query.answer(
                "RUB-оплата пока не подключена. Используй ⭐ Stars или 💵 USDT.",
                show_alert=True,
            )
            return
        resolved = _bundle_anchor_and_selected(user_id)
        if not resolved:
            await query.answer("Сессия устарела, начни заново через «🎁 3 бита».", show_alert=True)
            return
        anchor, selected_beats = resolved
        ids_csv = ",".join(str(b["id"]) for b in [anchor, *selected_beats])
        if user_id in _yk_creating_payment:
            await query.answer("⏳ Уже создаю платёж, подожди...", show_alert=False)
            return
        _yk_creating_payment.add(user_id)
        try:
            payment = await yookassa_api.create_payment(
                amount_rub=licensing.PRICE_BUNDLE3_RUB,
                description=f"Bundle 3 бита: {anchor['name']} + 2",
                metadata={
                    "type": "bundle",
                    "user_id": str(user_id),
                    "username": update.effective_user.username or "",
                    "beat_ids": ids_csv,
                },
                return_url=f"https://t.me/{beat_post_builder.BOT_USERNAME}",
            )
            await _save_yk_pending(payment["id"], {
                "type": "bundle",
                "user_id": user_id,
                "username": update.effective_user.username or "",
                "beat_ids": [int(b["id"]) for b in [anchor, *selected_beats]],
                "amount_rub": licensing.PRICE_BUNDLE3_RUB,
                "created_at": datetime.now(ZoneInfo("Europe/Moscow")).isoformat(),
            })
            pay_url = payment["confirmation"]["confirmation_url"]
            await bot.send_message(
                user_id,
                (
                    f"🎁 <b>Bundle 3 бита — {licensing.PRICE_BUNDLE3_RUB}₽</b>\n\n"
                    "Нажми кнопку → выбери способ оплаты (карта, СБП, T-Pay, "
                    "SberPay, ЮMoney) → подтверди.\n\n"
                    "После оплаты автоматом придут 3 mp3 + общая лицензия в этот чат."
                ),
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"💳 Оплатить {licensing.PRICE_BUNDLE3_RUB}₽", url=pay_url)],
                ]),
            )
            if query.message.chat_id != user_id:
                await query.answer("💰 Счёт отправил в ЛС бота", show_alert=False)
        except Exception:
            logger.exception("YK create_payment (bundle) failed")
            await query.answer(_user_error_msg("оплата"), show_alert=True)
        finally:
            _yk_creating_payment.discard(user_id)
        return

    if data.startswith("buy_rub_"):
        # MP3 Lease оплата через YooKassa REST API (все методы RU).
        import yookassa_api
        if not yookassa_api.is_configured():
            await query.answer(
                "RUB-оплата пока не подключена. Используй ⭐ Stars или 💵 USDT.",
                show_alert=True,
            )
            return
        try:
            beat_id = int(data[len("buy_rub_"):])
        except ValueError:
            await query.answer()
            return
        # Multi-click guard (см. buy_mix_rub выше).
        if user_id in _yk_creating_payment:
            await query.answer("⏳ Уже создаю платёж, подожди...", show_alert=False)
            return
        _yk_creating_payment.add(user_id)
        beat = beats_db.get_beat_by_id(beat_id)
        if not beat:
            await query.answer("Бит не найден", show_alert=True)
            return
        bpm = beat.get("bpm") or "?"
        key = beat.get("key") or "?"
        try:
            payment = await yookassa_api.create_payment(
                amount_rub=licensing.PRICE_MP3_RUB,
                description=f"MP3 Lease — {beat['name']} ({bpm} BPM, {key})",
                metadata={
                    "type": "mp3_lease",
                    "user_id": str(user_id),
                    "username": update.effective_user.username or "",
                    "beat_id": str(beat_id),
                    "beat_name": beat.get("name", "?"),
                },
                return_url=f"https://t.me/{beat_post_builder.BOT_USERNAME}?start=buy_{beat_id}",
            )
            await _save_yk_pending(payment["id"], {
                "type": "mp3_lease",
                "user_id": user_id,
                "username": update.effective_user.username or "",
                "beat_id": beat_id,
                "amount_rub": licensing.PRICE_MP3_RUB,
                "created_at": datetime.now(ZoneInfo("Europe/Moscow")).isoformat(),
            })
            pay_url = payment["confirmation"]["confirmation_url"]
            await bot.send_message(
                user_id,
                f"🎧 <b>MP3 Lease на «{beat['name']}» — {licensing.PRICE_MP3_RUB}₽</b>\n\n"
                f"⚡ {bpm} BPM · 🎹 {key}\n\n"
                "Нажми кнопку → выбери способ оплаты (карта, СБП, T-Pay, "
                "SberPay, ЮMoney) → подтверди.\n\n"
                "После оплаты автоматически придёт mp3 + txt-лицензия в этот чат.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"💳 Оплатить {licensing.PRICE_MP3_RUB}₽", url=pay_url)],
                ]),
            )
            if query.message.chat_id != user_id:
                await query.answer("💰 Счёт отправил в ЛС бота", show_alert=False)
            else:
                await query.answer()
        except Exception:
            logger.exception("YK create_payment (mp3_lease) failed")
            await query.answer(_user_error_msg("оплата"), show_alert=True)
        finally:
            _yk_creating_payment.discard(user_id)
        return

    if data.startswith("buy_mp3_"):
        try:
            beat_id = int(data[len("buy_mp3_"):])
        except ValueError:
            return
        beat = beats_db.get_beat_by_id(beat_id)
        if not beat:
            await query.answer("Бит не найден", show_alert=True)
            return
        bpm = beat.get("bpm") or "?"
        key = beat.get("key") or "?"
        title = f"MP3 Lease — {beat['name']}"[:32]
        description = (
            f"MP3 Lease на «{beat['name']}» ({bpm} BPM, {key}). "
            f"Non-exclusive: до 100k стримов, до 2000 копий, 1 music video. "
            f"Credit: prod. by TRIPLE FILL. После оплаты — mp3 + txt-лицензия в ЛС."
        )[:255]
        try:
            await bot.send_invoice(
                chat_id=user_id,
                title=title,
                description=description,
                payload=f"mp3_lease:{beat_id}",
                provider_token="",
                currency="XTR",
                prices=[LabeledPrice(label="MP3 Lease", amount=licensing.PRICE_MP3_STARS)],
            )
            if query.message.chat_id != user_id:
                await query.answer("💰 Счёт отправил в ЛС бота", show_alert=False)
        except TelegramError as e:
            logger.warning("send_invoice failed for user %s: %s", user_id, e)
            await query.answer(
                "Сначала напиши /start боту в ЛС — тогда пришлю счёт.",
                show_alert=True,
            )
        except Exception as e:
            logger.exception("send_invoice error")
            await query.answer(_user_error_msg("оплата"), show_alert=True)
        return

    if data == "search_prompt":
        bulk_add_mode[str(user_id) + "_search"] = True
        await _nav_reply(query, "🔍 Напиши название бита, имя артиста или тег — найду всё что есть!")
        return

    # Quick-filter chips (qf_hard / qf_memphis / qf_detroit / qf_ru / qf_bpm140 / qf_bpm160)
    if data.startswith("qf_"):
        filter_name = data[3:]
        await do_quick_filter(bot, query.message.chat_id, user_id, filter_name, page=0, query=query)
        return

    # Search pagination: sp_<filter>_<page>
    if data.startswith("sp_"):
        try:
            _, filter_name, page_str = data.split("_", 2)
            page = int(page_str)
        except Exception:
            await query.answer("⚠️ bad pagination")
            return
        results = _filter_beats(filter_name)
        if not results:
            await query.answer("пусто", show_alert=False)
            return
        try:
            await query.message.edit_reply_markup(
                reply_markup=_kb_search_results(results, filter_name, page)
            )
            await query.answer()
        except Exception:
            logger.exception("edit pagination failed")
        return

    if data == "noop":
        await query.answer()
        return

    # ── /quick_meta callbacks ─────────────────────────────────────────
    # Batch UX для дозаполнения key у битов с librosa-BPM. Юзер слушает
    # бит → кликает тональность → бот сохраняет + шлёт next.
    if data.startswith("qm_set:"):
        if user_id != ADMIN_ID:
            await query.answer()
            return
        try:
            _, bid_s, key = data.split(":", 2)
            bid = int(bid_s)
        except Exception:
            await query.answer("⚠️ bad payload", show_alert=True)
            return
        # Save key in beats_db
        for b in beats_db.BEATS_CACHE:
            if b.get("id") == bid:
                b["key"] = key
                # confidence = 0.95 (ручная установка) — выше librosa 0.7
                b["classification_confidence"] = 0.95
                break
        try:
            beats_db.save_beats()
        except Exception:
            logger.exception("qm_set: save_beats failed")
        await query.answer(f"✅ {key}")
        await _send_quick_meta_card(bot, query.message.chat_id, user_id)
        return

    if data.startswith("qm_skip:"):
        if user_id != ADMIN_ID:
            await query.answer()
            return
        await query.answer("⏭ skip")
        await _send_quick_meta_card(bot, query.message.chat_id, user_id)
        return

    if data.startswith("qm_not_beat:"):
        # Помечаем content_type='track' чтобы запись больше не выпадала в
        # quick_meta-фильтре (он ловит только content_type='beat'/default).
        # Используется когда юзер услышал что это финальный трек, а не type beat.
        if user_id != ADMIN_ID:
            await query.answer()
            return
        try:
            bid = int(data.split(":", 1)[1])
        except Exception:
            await query.answer("⚠️ bad payload", show_alert=True)
            return
        for b in beats_db.BEATS_CACHE:
            if b.get("id") == bid:
                b["content_type"] = "track"
                break
        try:
            beats_db.save_beats()
        except Exception:
            logger.exception("qm_not_beat: save_beats failed")
        await query.answer("🚫 помечен как трек, больше не появится")
        await _send_quick_meta_card(bot, query.message.chat_id, user_id)
        return

    if data == "qm_stop":
        if user_id != ADMIN_ID:
            await query.answer()
            return
        await query.answer("🛑 Stop")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if data.startswith("qm_major:"):
        if user_id != ADMIN_ID:
            await query.answer()
            return
        try:
            bid = int(data.split(":", 1)[1])
        except Exception:
            await query.answer()
            return
        try:
            await query.edit_message_reply_markup(reply_markup=_kb_quick_meta_major(bid))
        except Exception:
            pass
        await query.answer()
        return

    if data.startswith("qm_minor:"):
        if user_id != ADMIN_ID:
            await query.answer()
            return
        try:
            bid = int(data.split(":", 1)[1])
        except Exception:
            await query.answer()
            return
        try:
            await query.edit_message_reply_markup(reply_markup=_kb_quick_meta_minor(bid))
        except Exception:
            pass
        await query.answer()
        return

    # ── Shorts on-demand callbacks ────────────────────────────────────
    # После плановой публикации long YT бот шлёт админу notification
    # с кнопками. `make_shorts:<token>` запускает heavy ffmpeg+upload
    # вне scheduler tick'а — это решает OOM. `skip_shorts:<token>`
    # просто чистит файлы из Storage если Shorts не нужен.
    if data.startswith("make_shorts:"):
        if user_id != ADMIN_ID:
            await query.answer()
            return
        token = data.split(":", 1)[1]
        # Mutex: не запускаем повторную сборку для того же token
        if token in _building_shorts:
            await query.answer("⏳ Уже собираю этот Shorts, подожди...", show_alert=True)
            return
        _building_shorts.add(token)
        await query.answer("🎬 Начинаю сборку Shorts...")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        try:
            await _build_and_upload_shorts(bot, token)
        except Exception:
            logger.exception("make_shorts: build/upload failed for %s", token)
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"❌ Сборка Shorts для {token} провалилась — см. логи Render.",
                )
            except Exception:
                pass
        finally:
            _building_shorts.discard(token)
        return

    if data.startswith("skip_shorts:"):
        if user_id != ADMIN_ID:
            await query.answer()
            return
        token = data.split(":", 1)[1]
        await query.answer("Окей, чищу файлы")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        try:
            import publish_scheduler
            await asyncio.to_thread(publish_scheduler.cleanup_published_files, token)
        except Exception:
            logger.exception("skip_shorts: cleanup failed for %s", token)
        return

    if data == "admin_panel":
        if user_id != ADMIN_ID: return
        await _nav_reply(query, "🎛 Панель управления:", reply_markup=kb_admin())
        return

    if data == "admin_quick_meta":
        if user_id != ADMIN_ID:
            await query.answer()
            return
        await query.answer("🎹 Запускаю flow...")
        # Кратко поясняем + сразу шлём первый бит-карточку через
        # _send_quick_meta_card (та же логика что в /quick_meta команде)
        try:
            await _send_quick_meta_card(bot, query.message.chat_id, user_id)
        except Exception:
            logger.exception("admin_quick_meta failed")
            await bot.send_message(query.message.chat_id, "❌ Ошибка запуска /quick_meta")
        return

    if data == "admin_auto_repost_toggle":
        if user_id != ADMIN_ID:
            await query.answer()
            return
        prefs = _load_admin_prefs()
        was_on = prefs.get("auto_repost_enabled", False)
        prefs["auto_repost_enabled"] = not was_on
        _save_admin_prefs(prefs)
        new_state = "✅ ВКЛ" if not was_on else "⛔ ВЫКЛ"
        await query.answer(f"Auto-repost: {new_state}", show_alert=True)
        # Refresh админ-панели чтобы кнопка обновила label
        try:
            await query.edit_message_reply_markup(reply_markup=kb_admin())
        except Exception:
            pass
        return

    if data == "admin_pin_hub":
        if user_id != ADMIN_ID: return
        await _show_pin_hub_preview(bot, query.message.chat_id, context)
        return

    if data == "admin_products":
        if user_id != ADMIN_ID: return
        products = [
            b for b in beats_db.BEATS_CACHE
            if b.get("content_type") in ("drumkit", "samplepack", "looppack")
        ]
        rows = []
        if products:
            # Сортируем по id desc — новые сверху.
            for p in sorted(products, key=lambda x: x["id"], reverse=True)[:20]:
                label = licensing.PRODUCT_TYPE_LABELS.get(
                    p.get("content_type", ""), "?"
                )
                price = p.get("price_stars") or "?"
                title = f"{label}: {p['name']} — {price}⭐"[:55]
                rows.append([InlineKeyboardButton(title, callback_data=f"admin_prod_{p['id']}")])
        rows.append([InlineKeyboardButton("➕ Залить новый продукт", callback_data="admin_upload_prod")])
        rows.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")])
        await _nav_reply(
            query,
            f"📦 Каталог продуктов ({len(products)}):",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data == "admin_upload_prod":
        if user_id != ADMIN_ID: return
        await _start_product_upload(bot, query.message.chat_id, context)
        return

    if data.startswith("admin_prod_"):
        if user_id != ADMIN_ID: return
        try:
            pid = int(data[len("admin_prod_"):])
        except ValueError:
            return
        p = beats_db.get_beat_by_id(pid)
        if not p or p.get("content_type") not in licensing.PRODUCT_TYPE_LABELS:
            await query.answer("Продукт не найден", show_alert=True)
            return
        label = licensing.PRODUCT_TYPE_LABELS[p["content_type"]]
        size_mb = (p.get("file_size") or 0) / (1024 * 1024) if p.get("file_size") else 0
        info = (
            f"📦 <b>{label}</b>: {p['name']}\n"
            f"🆔 id={p['id']}\n"
            f"💰 {p.get('price_stars', '?')}⭐ / {p.get('price_usdt', '?')} USDT\n"
            f"📎 {p.get('file_name','?')} ({size_mb:.1f} MB)\n\n"
            f"📝 {p.get('description') or '<i>(без описания)</i>'}"
        )
        await _nav_reply(
            query, info, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀️ К списку", callback_data="admin_products")],
            ]),
        )
        return

    if data == "admin_queue":
        if user_id != ADMIN_ID: return
        import publish_scheduler
        n = publish_scheduler.queue_size()
        # Safety net: если память пустая — перечитать из Supabase on-demand.
        # Покрывает случай когда post_init.load_queue молча вернул 0 (SDK quirk,
        # RLS, cold-start timeout). Юзер может кликом «разбудить» очередь.
        if n == 0:
            try:
                n = publish_scheduler.load_queue()
                logger.info("admin_queue: reloaded %d items from Supabase on-demand", n)
            except Exception:
                logger.exception("admin_queue: on-demand reload failed")
        if n == 0:
            await _nav_reply(
                query,
                "📭 Очередь пуста. Загружай битеки — жми «📅 В лучшее время» в превью.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")]])
            )
        else:
            await _nav_reply(
                query,
                f"📅 Запланировано: {n}\nНажми «❌ Отменить» под нужной публикацией:",
                reply_markup=kb_admin_queue(),
            )
        return

    if data.startswith("qcancel_"):
        if user_id != ADMIN_ID: return
        token = data[len("qcancel_"):]
        import publish_scheduler
        # Перед удалением чистим temp-файлы этого item'а
        items_to_remove = [q for q in publish_scheduler._QUEUE if q.get("token") == token]
        for item in items_to_remove:
            for key in ("mp3_path", "video_path", "thumb_path"):
                p = item.get(key)
                if p:
                    try:
                        os.remove(p)
                    except Exception:
                        pass
        ok = publish_scheduler.cancel(token)
        if ok:
            await query.answer("✅ Отменено", show_alert=False)
            # Refresh admin_queue view
            n = publish_scheduler.queue_size()
            if n == 0:
                try:
                    await query.message.edit_text(
                        "📭 Очередь пуста. Загружай битеки и планируй публикации.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")]])
                    )
                except Exception:
                    pass
            else:
                try:
                    await query.message.edit_reply_markup(reply_markup=kb_admin_queue())
                except Exception:
                    pass
        else:
            await query.answer("⚠️ Не нашёл в очереди (возможно уже опубликован)", show_alert=True)
        return

    if data == "admin_stats":
        if user_id != ADMIN_ID: return
        total_favs = sum(len(v) for v in user_favorites.values())
        top = sorted(beat_plays.items(), key=lambda x: x[1], reverse=True)[:5]
        top_text = ""
        for i, (bid, count) in enumerate(top):
            b = beats_db.get_beat_by_id(bid)
            name = b["name"][:25] if b else "Unknown"
            top_text += str(i+1) + ". " + name + " — " + str(count) + " plays\n"
        beats_c = len([b for b in beats_db.BEATS_CACHE if b.get("content_type","beat")=="beat"])
        tracks_c = len([b for b in beats_db.BEATS_CACHE if b.get("content_type")=="track"])
        remixes_c = len([b for b in beats_db.BEATS_CACHE if b.get("content_type")=="remix"])
        await _nav_reply(
            query,
            "📊 Статистика\n\n"
            "👥 Пользователей: " + str(len(all_users)) + "\n"
            "🎁 Получили пак: " + str(len(users_received_pack)) + "\n"
            "❤️ В избранном: " + str(total_favs) + "\n"
            "▶️ Прослушиваний: " + str(sum(beat_plays.values())) + "\n\n"
            "🎹 " + str(beats_c) + " / 🎤 " + str(tracks_c) + " / 🔀 " + str(remixes_c) + "\n\n"
            "🔥 Топ:\n" + (top_text if top_text else "Нет данных"),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📈 Полный топ", callback_data="admin_top_beats")],
                [InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")]
            ])
        )
        return

    if data == "admin_top_beats":
        if user_id != ADMIN_ID: return
        top = sorted(beat_plays.items(), key=lambda x: x[1], reverse=True)[:20]
        if not top:
            await _nav_reply(query, "Нет данных.")
            return
        text = "📈 Топ-20:\n\n"
        for i, (bid, count) in enumerate(top):
            b = beats_db.get_beat_by_id(bid)
            name = b["name"][:30] if b else "Unknown"
            uniq = len(beat_plays_users.get(bid, set()))
            favs = sum(1 for fl in user_favorites.values() if bid in fl)
            text += str(i+1) + ". " + name + "\n   ▶️ " + str(count) + "  👥 " + str(uniq) + "  ❤️ " + str(favs) + "\n\n"
        await _nav_reply(query, text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_stats")]]))
        return

    if data == "admin_catalog":
        if user_id != ADMIN_ID: return
        tags = beats_db.get_all_tags()
        await _nav_reply(
            query,
            "📂 Каталог: " + str(len(beats_db.BEATS_CACHE)) + " шт.\nТеги: " + ", ".join(tags[:20]),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")]])
        )
        return

    if data in ("admin_addbeats_track", "admin_addbeats_remix"):
        if user_id != ADMIN_ID: return
        mode_type = data.replace("admin_addbeats_", "")
        bulk_add_mode[ADMIN_ID] = mode_type
        icons = {"track": "🎤 треки", "remix": "🔀 ремиксы"}
        await _nav_reply(
            query,
            "✅ Режим включён! Всё пойдёт как: " + icons[mode_type] + "\n\nПересылай посты из канала.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⛔ Закончить", callback_data="admin_stopadd")]]))
        return

    if data == "admin_addbeats":
        if user_id != ADMIN_ID: return
        bulk_add_mode[ADMIN_ID] = "beat"
        await _nav_reply(query, "✅ Режим добавления ВКЛЮЧЁН!\nПересылай посты из канала.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⛔ Закончить", callback_data="admin_stopadd")]]))
        return

    if data == "admin_stopadd":
        if user_id != ADMIN_ID: return
        bulk_add_mode.pop(ADMIN_ID, None)
        beats_c = len([b for b in beats_db.BEATS_CACHE if b.get("content_type","beat")=="beat"])
        tracks_c = len([b for b in beats_db.BEATS_CACHE if b.get("content_type")=="track"])
        remixes_c = len([b for b in beats_db.BEATS_CACHE if b.get("content_type")=="remix"])
        await _nav_reply(query, "⛔ Добавление завершено.\n🎹 " + str(beats_c) + " / 🎤 " + str(tracks_c) + " / 🔀 " + str(remixes_c),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ В панель", callback_data="admin_panel")]]))
        return

    if data == "admin_clearbeats":
        if user_id != ADMIN_ID: return
        await _nav_reply(query, "🗑 Что удаляем?", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎹 Удалить из битов", callback_data="admin_delete_cat_beat")],
            [InlineKeyboardButton("🎤 Удалить из треков", callback_data="admin_delete_cat_track")],
            [InlineKeyboardButton("🔀 Удалить из ремиксов", callback_data="admin_delete_cat_remix")],
            [InlineKeyboardButton("💥 Очистить всё", callback_data="admin_clearbeats_yes")],
            [InlineKeyboardButton("❌ Отмена", callback_data="admin_panel")],
        ]))
        return

    if data.startswith("admin_delete_cat_"):
        if user_id != ADMIN_ID: return
        cat = data.split("_")[3]
        items = [b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == cat]
        if not items:
            await query.answer("В этой категории ничего нет!", show_alert=True)
            return
        icons = {"beat": "🎹", "track": "🎤", "remix": "🔀"}
        labels = {"beat": "биты", "track": "треки", "remix": "ремиксы"}
        rows = []
        for b in items:
            rows.append([InlineKeyboardButton(
                "🗑 " + b["name"][:35],
                callback_data="admin_del_" + str(b["id"])
            )])
        rows.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_clearbeats")])
        await _nav_reply(
            query,
            icons[cat] + " " + labels[cat].capitalize() + " (" + str(len(items)) + " шт.) — выбери что удалить:",
            reply_markup=InlineKeyboardMarkup(rows)
        )
        return

    if data.startswith("admin_del_"):
        if user_id != ADMIN_ID: return
        beat_id = int(data.split("_")[2])
        beat = beats_db.get_beat_by_id(beat_id)
        if not beat:
            await query.answer("Уже удалён!", show_alert=True)
            return
        beats_db.BEATS_CACHE.remove(beat)
        beats_db.BEATS_BY_ID.pop(beat_id, None)
        asyncio.create_task(asyncio.to_thread(beats_db.save_beats))
        await query.answer("✅ Удалено: " + beat["name"][:30], show_alert=True)
        cat = beat.get("content_type", "beat")
        items = [b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == cat]
        if not items:
            await query.message.edit_text("Категория пуста!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")]]))
            return
        icons = {"beat": "🎹", "track": "🎤", "remix": "🔀"}
        labels = {"beat": "биты", "track": "треки", "remix": "ремиксы"}
        rows = [[InlineKeyboardButton("🗑 " + b["name"][:35], callback_data="admin_del_" + str(b["id"]))] for b in items]
        rows.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_clearbeats")])
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(rows))
        return

    if data == "admin_clearbeats_yes":
        if user_id != ADMIN_ID: return
        beats_db.BEATS_CACHE.clear()
        beats_db.BEATS_BY_ID.clear()
        asyncio.create_task(asyncio.to_thread(beats_db.save_beats))
        await _nav_reply(query, "🗑 Каталог очищен!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ В панель", callback_data="admin_panel")]]))
        return

# ── Обработка текстовых сообщений ────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    user_id = update.effective_user.id
    message = update.message
    bot = context.bot
    write_heartbeat()

    if update.channel_post:
        msg = update.channel_post
        text = msg.text or msg.caption or ""
        if not text.strip():
            return
        username = CHANNEL_LINK.split("/")[-1].lstrip("@")
        beat = parse_beat_from_text(text, msg.message_id, username)
        if msg.audio:
            beat["file_id"] = msg.audio.file_id
        elif msg.voice:
            beat["file_id"] = msg.voice.file_id
        if try_add_beat(beat):
            asyncio.create_task(asyncio.to_thread(beats_db.save_beats))
            logger.info("Auto-added [" + beat["content_type"] + "]: " + beat["name"])
        return

    if not message:
        return

    text = message.text or message.caption or ""

    search_key = str(user_id) + "_search"
    if search_key in bulk_add_mode:
        del bulk_add_mode[search_key]
        await do_search(bot, message.chat_id, text, user_id)
        return

    if user_id != ADMIN_ID:
        return

    if ADMIN_ID not in bulk_add_mode or bulk_add_mode.get(ADMIN_ID) not in ("beat", "track", "remix"):
        if message.document and not text:
            fid = message.document.file_id
            await message.reply_text("FILE_ID:\n\n" + fid + "\n\nВставь в config.py:\nSAMPLE_PACK_FILE_ID = " + chr(34) + fid + chr(34))
        return

    channel_username = CHANNEL_LINK.split("/")[-1].lstrip("@")
    msg_id = None

    fwd = message.forward_origin if hasattr(message, "forward_origin") else None
    if fwd:
        if hasattr(fwd, "message_id"):
            msg_id = fwd.message_id
        if hasattr(fwd, "chat") and fwd.chat and fwd.chat.username:
            channel_username = fwd.chat.username
    if msg_id is None and hasattr(message, "forward_from_message_id") and message.forward_from_message_id:
        msg_id = message.forward_from_message_id
    if hasattr(message, "forward_from_chat") and message.forward_from_chat and message.forward_from_chat.username:
        channel_username = message.forward_from_chat.username
    if msg_id is None:
        msg_id = message.message_id

    if not text.strip():
        if message.audio:
            text = message.audio.file_name or ("Beat #" + str(msg_id))
        elif message.voice:
            text = "Voice #" + str(msg_id)
        else:
            return

    beat = parse_beat_from_text(text, msg_id, channel_username)
    if message.audio:
        beat["file_id"] = message.audio.file_id
        beat["file_unique_id"] = message.audio.file_unique_id
    elif message.voice:
        beat["file_id"] = message.voice.file_id

    mode = bulk_add_mode.get(ADMIN_ID)
    if mode in ("beat", "track", "remix"):
        beat["content_type"] = mode

    logger.info("Beat ID: " + str(beat["id"]) + " msg_id: " + str(msg_id) + " name: " + beat["name"])

    type_icons = {"beat": "🎹", "track": "🎤", "remix": "🔀"}
    icon = type_icons.get(beat["content_type"], "🎧")
    import time as time_module
    if try_add_beat(beat):
        asyncio.create_task(asyncio.to_thread(beats_db.save_beats))
        logger.info("Added " + icon + " " + beat["name"] + " total=" + str(len(beats_db.BEATS_CACHE)))
        batch_stats["added"] += 1
    else:
        logger.info("Duplicate: " + beat["name"])
        batch_stats["skipped"] += 1

    batch_stats["last_time"] = time_module.time()

    async def send_batch_report():
        await asyncio.sleep(3)
        if time_module.time() - batch_stats["last_time"] >= 2.9:
            added = batch_stats["added"]
            skipped = batch_stats["skipped"]
            if added > 0 or skipped > 0:
                beats_c = len([b for b in beats_db.BEATS_CACHE if b.get("content_type","beat")=="beat"])
                tracks_c = len([b for b in beats_db.BEATS_CACHE if b.get("content_type")=="track"])
                remixes_c = len([b for b in beats_db.BEATS_CACHE if b.get("content_type")=="remix"])
                await context.bot.send_message(
                    ADMIN_ID,
                    "📊 Итог загрузки:\n"
                    "✅ Добавлено: " + str(added) + "\n"
                    "⚠️ Дублей пропущено: " + str(skipped) + "\n\n"
                    "🎹 " + str(beats_c) + " / 🎤 " + str(tracks_c) + " / 🔀 " + str(remixes_c)
                )
                batch_stats["added"] = 0
                batch_stats["skipped"] = 0

    asyncio.create_task(send_batch_report())


# ── Приём mp3 для публикации бита ─────────────────────────────

async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Приём аудио-файла от админа: если имя в формате type-beat — запуск upload flow."""
    if not update.effective_user: return
    if update.effective_user.id != ADMIN_ID: return

    audio = update.message.audio
    if audio and audio.file_name and "type beat" in audio.file_name.lower():
        await handle_beat_upload(update, context, audio)


async def handle_admin_fsm_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """FSM-шаги на текстовых ответах админа: name → price → desc → preview.

    Возвращает True если обработал (чтобы handle_assistant не брал сообщение
    себе), False — если state не активен, пропускаем дальше по chain.
    """
    if update.effective_user.id != ADMIN_ID:
        return False
    state = context.user_data.get("product_upload") or {}
    step = state.get("step")
    if step not in ("await_name", "await_price", "await_desc"):
        return False

    text = (update.message.text or "").strip()
    if not text:
        return True  # не ловим пустые

    if step == "await_name":
        if len(text) < 3:
            await update.message.reply_text("⚠️ Имя слишком короткое (мин 3 симв). Ещё раз:")
            return True
        if len(text) > 120:
            await update.message.reply_text("⚠️ Имя слишком длинное (макс 120 симв). Ещё раз:")
            return True
        state["name"] = text
        state["step"] = "await_price"
        context.user_data["product_upload"] = state
        default_stars, default_usdt = licensing.DEFAULT_PRICES[state["content_type"]]
        await update.message.reply_text(
            f"✅ Имя: <b>{text}</b>\n\n"
            f"Теперь цена в ⭐ (число) или <code>default</code> для "
            f"{default_stars}⭐ / {default_usdt:g} USDT.\n\n"
            "Отмена — /cancel_product",
            parse_mode="HTML",
        )
        return True

    if step == "await_price":
        default_stars, default_usdt = licensing.DEFAULT_PRICES[state["content_type"]]
        if text.lower() in ("default", "дефолт", "-", "skip"):
            price_stars, price_usdt = default_stars, default_usdt
        else:
            try:
                price_stars = int(text)
            except ValueError:
                await update.message.reply_text(
                    "⚠️ Цена — число в ⭐ (или 'default'). Ещё раз:"
                )
                return True
            if price_stars < 100 or price_stars > 50000:
                await update.message.reply_text(
                    "⚠️ Цена вне разумных границ (100-50000⭐). Ещё раз:"
                )
                return True
            price_usdt = round(price_stars * (default_usdt / default_stars), 1)
        state["price_stars"] = price_stars
        state["price_usdt"] = price_usdt
        state["step"] = "await_desc"
        context.user_data["product_upload"] = state
        await update.message.reply_text(
            f"✅ Цена: <b>{price_stars}⭐ / {price_usdt:g} USDT</b>\n\n"
            "Теперь <b>описание</b> (что внутри пака, под какой вайб).\n"
            "Или <code>skip</code> — без описания.\n\n"
            "Отмена — /cancel_product",
            parse_mode="HTML",
        )
        return True

    if step == "await_desc":
        if text.lower() in ("skip", "-", "пропустить"):
            description = ""
        elif len(text) < 10:
            await update.message.reply_text(
                "⚠️ Описание слишком короткое (мин 10 симв либо 'skip'). Ещё раз:"
            )
            return True
        else:
            description = text
        state["description"] = description
        state["step"] = "await_confirm"
        context.user_data["product_upload"] = state

        import product_upload
        # Preview + save/cancel inline buttons.
        token = uuid.uuid4().hex[:10]
        pending_products[token] = {
            "meta": product_upload.ProductMeta(
                content_type=state["content_type"],
                name=state["name"],
                price_stars=state["price_stars"],
                price_usdt=state["price_usdt"],
                description=description,
            ),
            "file_id": state["file_id"],
            "file_unique_id": state.get("file_unique_id", ""),
            "file_size": state.get("file_size"),
            "file_name": state.get("file_name"),
            "mime_type": state.get("mime_type", "application/zip"),
        }
        _persist_pending_products()
        type_label = licensing.PRODUCT_TYPE_LABELS[state["content_type"]]
        size_mb = (state.get("file_size") or 0) / (1024 * 1024)
        preview = (
            f"👁 Превью продукта:\n\n"
            f"📦 Тип: <b>{type_label}</b>\n"
            f"🎯 Имя: <b>{state['name']}</b>\n"
            f"💰 Цена: <b>{state['price_stars']}⭐ / {state['price_usdt']:g} USDT</b>\n"
            f"📎 Файл: {state.get('file_name','?')} ({size_mb:.1f} MB)\n\n"
            f"📝 Описание:\n{description or '<i>(не задано)</i>'}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Сохранить в каталог", callback_data=f"prod_save_{token}")],
            [InlineKeyboardButton("❌ Отмена", callback_data=f"prod_cancel_{token}")],
        ])
        await update.message.reply_text(preview, reply_markup=kb, parse_mode="HTML")
        return True

    return False


# ── Приём zip-файла для drum kit / sample pack / loop pack ────

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """FSM-шаг: ждём zip после выбора типа через /upload_product.

    Файл остаётся на серверах Telegram — мы храним только `file_id` для
    последующей отдачи покупателю через send_document.
    """
    if not update.effective_user:
        return
    if update.effective_user.id != ADMIN_ID:
        return

    state = context.user_data.get("product_upload") or {}
    if state.get("step") != "await_zip":
        # Не в режиме /upload_product FSM — админу могут понадобиться file_id
        # (например для SAMPLE_PACK_FILE_ID в Render env). Возвращаем его.
        # Это заменяет ветку в handle_message:2937-2940 которая не вызывалась
        # потому что handle_document ловил document раньше и тихо return'ил.
        doc = update.message.document
        if doc:
            fid = doc.file_id
            size_mb = (doc.file_size or 0) / (1024 * 1024)
            await update.message.reply_text(
                f"📎 <b>{doc.file_name or '(без имени)'}</b> · {size_mb:.1f} MB\n\n"
                f"<b>FILE_ID:</b>\n<code>{fid}</code>\n\n"
                f"Для sample pack: Render env → "
                f"<code>SAMPLE_PACK_FILE_ID={fid}</code>",
                parse_mode="HTML",
            )
        return

    doc = update.message.document
    if not doc:
        return

    import product_upload
    try:
        product_upload.validate_file(doc.file_name, doc.file_size)
    except product_upload.CaptionError as e:
        await update.message.reply_text(
            f"⚠️ {e}\n\nПришли другой файл или /cancel_product чтобы прервать."
        )
        return

    state["file_id"] = doc.file_id
    state["file_unique_id"] = doc.file_unique_id
    state["file_size"] = doc.file_size
    state["file_name"] = doc.file_name
    state["mime_type"] = doc.mime_type or "application/zip"
    state["step"] = "await_name"
    context.user_data["product_upload"] = state

    size_mb = doc.file_size / (1024 * 1024)
    await update.message.reply_text(
        f"✅ Файл принят: {doc.file_name} ({size_mb:.1f} MB)\n\n"
        "Теперь пришли **имя продукта** (одно сообщение).\n\n"
        "Отмена — /cancel_product",
        parse_mode="Markdown",
    )


async def handle_beat_upload(update: Update, context: ContextTypes.DEFAULT_TYPE, audio):
    """Обработка присланного mp3: парсит имя → thumbnail + video → preview + кнопки."""
    import beat_upload
    import beat_post_builder
    import thumbnail_generator
    import video_builder
    from pathlib import Path

    try:
        meta = beat_upload.parse_filename(audio.file_name)
    except ValueError as e:
        await update.message.reply_text(
            f"❌ Имя файла не распарсил: {e}\n\n"
            "Формат: <artist> type beat <NAME> <BPM> <KEY>.mp3\n"
            "Пример: kenny muney type beat THOUGHTS 160 Am.mp3"
        )
        return

    status = await update.message.reply_text(
        f"🎧 Разобрал: {meta.name} — {meta.artist_display} Type Beat\n"
        f"⚡ BPM {meta.bpm}  🎹 {meta.key}\n\n"
        "⏳ Качаю mp3..."
    )

    token = uuid.uuid4().hex[:12]
    mp3_path = Path(TEMP_UPLOAD_DIR) / f"{token}.mp3"
    video_path = Path(TEMP_UPLOAD_DIR) / f"{token}.mp4"
    thumb_path = Path(TEMP_UPLOAD_DIR) / f"{token}.jpg"

    try:
        file = await context.bot.get_file(audio.file_id)
        await file.download_to_drive(str(mp3_path))

        # Brand-кадр канала — один JPG на весь канал (winner-паттерн ниши).
        # Качаем с GH Release при первом upload, кэшируем в assets/brand/ (не wipe'ится janitor'ом).
        loop = asyncio.get_running_loop()
        await status.edit_text(status.text + "\n🖼 Готовлю brand-кадр...")
        brand_path = await loop.run_in_executor(None, _ensure_brand_image)
        if brand_path:
            import shutil
            shutil.copy2(brand_path, thumb_path)
            logger.info("upload: using brand image as thumbnail")
        else:
            # Fallback — legacy text-overlay (для случая если GH Release недоступен)
            logger.warning("upload: brand image unavailable, using legacy text-thumbnail")
            thumbnail_generator.generate_thumbnail(meta.name, meta.artist_line, thumb_path)
        clip_loop_path = None  # больше не используется, но оставляем в payload для совместимости

        await status.edit_text(status.text + "\n🎬 Собираю видео (ffmpeg)...")
        logger.info("upload: starting ffmpeg for %s", mp3_path)
        # Статичный кадр + mp3 — winning-паттерн ниши.
        await loop.run_in_executor(
            None,
            lambda: video_builder.build_video(thumb_path, mp3_path, video_path),
        )
        logger.info("upload: ffmpeg done, building post meta")

        # Резервируем beat_id заранее, чтобы в описании YT был валидный deep-link
        # на покупку этого конкретного битка. При ошибке YT publish ID не закрепится.
        # Защита от race-condition: если cache пустой (напр. upload сразу после
        # redeploy, пока beats_db не успел прогрузиться с disk) — reload.
        if not beats_db.BEATS_CACHE:
            logger.warning("BEATS_CACHE empty before beat_id reserve — force-reloading")
            beats_db.load_beats()
        reserved_beat_id = max([b["id"] for b in beats_db.BEATS_CACHE] + [0]) + 1

        # Длительность mp3 для YT-timestamps (winner-паттерн). Если probe
        # упадёт — description соберётся без timestamps-блока, не критично.
        try:
            mp3_duration = video_builder.probe_duration(mp3_path)
        except Exception as e:
            logger.warning("probe_duration failed, YT description без timestamps: %s", e)
            mp3_duration = None

        yt_post = beat_post_builder.build_yt_post(
            meta, beat_id=reserved_beat_id, duration_sec=mp3_duration,
        )
        tg_caption, tg_style = await beat_post_builder.build_tg_caption_async(meta, beat_id=reserved_beat_id)

        pending_uploads[token] = {
            "meta": meta,
            "mp3_path": mp3_path,
            "video_path": video_path,
            "thumb_path": thumb_path,
            "clip_loop_path": clip_loop_path,
            "reserved_beat_id": reserved_beat_id,
            "yt_post": yt_post,
            "tg_caption": tg_caption,
            "tg_style": tg_style,
            "tg_file_id": audio.file_id,
        }

        await status.delete()

        # 1) Превью TG-поста — точно как увидят подписчики канала.
        tg_preview_msg = await update.message.reply_audio(
            audio=audio.file_id,
            caption=f"👁 Превью TG-поста:\n\n{tg_caption}",
        )
        pending_uploads[token]["tg_preview_chat_id"] = tg_preview_msg.chat_id
        pending_uploads[token]["tg_preview_msg_id"] = tg_preview_msg.message_id

        # 2) Превью YT-поста — thumbnail + title + tags + description + кнопки.
        # Disclaimer ставим в начале — caption у reply_photo лимитирован 1024
        # символами; при обрезке должна резаться description, а не warning.
        yt_preview_head = (
            f"👁 Превью YouTube:\n"
            f"⚠️ Ссылка buy_{reserved_beat_id} заработает после публикации\n\n"
            f"🎬 Title:\n{yt_post.title}\n\n"
            f"🏷 Tags ({len(yt_post.tags)}): {', '.join(yt_post.tags[:6])}...\n\n"
            f"📝 Description:\n"
        )
        desc_budget = max(0, 1024 - len(yt_preview_head) - 3)  # 3 — на "..."
        yt_preview = yt_preview_head + yt_post.description[:desc_budget] + "..."
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 YT + Short + TG сейчас", callback_data=f"bu_all_{token}")],
            [InlineKeyboardButton("📅 В лучшее время (авто)", callback_data=f"bu_sched_{token}")],
            [InlineKeyboardButton("🎬 YT + Short (без TG)", callback_data=f"bu_yt_{token}")],
            [InlineKeyboardButton("📡 Только в канал TG", callback_data=f"bu_tg_{token}")],
            [InlineKeyboardButton("🔄 Переписать TG-подпись", callback_data=f"bu_regen_{token}")],
            [InlineKeyboardButton("❌ Отмена", callback_data=f"bu_cancel_{token}")],
        ])
        yt_preview_msg = await update.message.reply_photo(
            photo=open(thumb_path, "rb"),
            caption=yt_preview[:1024],
            reply_markup=kb,
        )
        pending_uploads[token]["yt_preview_chat_id"] = yt_preview_msg.chat_id
        pending_uploads[token]["yt_preview_msg_id"] = yt_preview_msg.message_id
    except Exception as e:
        logger.exception("beat_upload failed")
        await status.edit_text(f"❌ Ошибка: {e}")
        _cleanup_upload(token)


async def _delete_preview_messages(bot, payload: dict):
    """Чистит превью-сообщения в чате (TG audio preview + YT photo preview)
    после финального действия (publish / schedule / cancel). Позволяет
    держать chat чистым — остаётся только финальное подтверждение.
    """
    for chat_key, msg_key in [
        ("tg_preview_chat_id", "tg_preview_msg_id"),
        ("yt_preview_chat_id", "yt_preview_msg_id"),
    ]:
        chat_id = payload.get(chat_key)
        msg_id = payload.get(msg_key)
        if chat_id and msg_id:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception:
                pass  # сообщение могло быть уже удалено / старше 48ч


def _cleanup_upload(token: str):
    """Удаляет temp файлы и запись из pending_uploads."""
    data = pending_uploads.pop(token, None)
    if not data:
        return
    # Если этот token в очереди публ. scheduler'а — НЕ удалять файлы,
    # они нужны в назначенное время. Scheduler сам почистит после publish.
    try:
        import publish_scheduler
        if publish_scheduler.is_scheduled(token):
            logger.info("cleanup: token %s scheduled, skip file delete", token)
            return
    except Exception:
        pass
    for key in ("mp3_path", "video_path", "thumb_path", "clip_loop_path"):
        p = data.get(key)
        if p:
            try:
                os.remove(p)
            except Exception:
                pass

async def handle_assistant(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message: return
    # FSM админа (product upload) — приоритет выше любого другого текстового
    # хэндлера. Если шаг активен — обрабатываем и выходим.
    if update.effective_user.id == ADMIN_ID:
        handled = await handle_admin_fsm_text(update, context)
        if handled:
            return
    user_id = update.effective_user.id
    text = update.message.text or ""
    if not text.strip() or text.startswith("/"):
        return

    # Exclusive inquiry FSM: юзер раньше кликнул «💎 Exclusive» → теперь
    # его первое текстовое сообщение считается описанием проекта. Форвардим
    # админу + очищаем state + благодарим юзера.
    pending_excl = context.user_data.get("pending_excl")
    if pending_excl:
        # ВСЁ что приходит от юзера / из профиля — escape. Иначе юзер может
        # прислать `</i><a href="phish">...</a><i>` и подменить вид сообщения
        # админу (phishing-link маскирующаяся под соцсеть артиста).
        desc = html.escape(text.strip()[:800])
        raw_uname = ("@" + update.effective_user.username
                     if update.effective_user.username
                     else (update.effective_user.full_name or ""))
        uname = html.escape(raw_uname)
        context.user_data.pop("pending_excl", None)
        # post_url — идёт в ЛС админа как plain text (не в href), но при
        # parse_mode=HTML нужно escape. `<` / `>` в URL сломают parsing.
        post_url = html.escape(pending_excl.get("post_url") or "")
        post_line = f"\nПост: {post_url}" if post_url else ""
        beat_name_safe = html.escape(str(pending_excl.get("beat_name") or ""))
        bpm_safe = html.escape(str(pending_excl.get("bpm") or ""))
        key_safe = html.escape(str(pending_excl.get("key") or ""))
        admin_msg = (
            "💎 <b>EXCLUSIVE INQUIRY</b>\n\n"
            f"От: {uname} (id={user_id})\n"
            f"Бит: <b>{beat_name_safe}</b>\n"
            f"BPM {bpm_safe} · {key_safe}"
            f"{post_line}\n\n"
            f"Проект:\n<i>{desc}</i>"
        )
        try:
            await context.bot.send_message(ADMIN_ID, admin_msg, parse_mode="HTML")
        except Exception:
            logger.exception("excl forward to admin failed")
        await update.message.reply_text(
            "✅ Принято. TRIPLE FILL свяжется в ЛС в течение 24 часов.\n\n"
            "Если что — пиши @iiiplfiii напрямую."
        )
        return

    # Кнопочный поиск (🔍 Поиск) — для всех, приоритет выше любого агента
    search_key = str(user_id) + "_search"
    if search_key in bulk_add_mode:
        del bulk_add_mode[search_key]
        await do_search(context.bot, update.message.chat_id, text, user_id)
        return

    if user_id == ADMIN_ID:
        # Admin-only: bulk-add битов/треков/ремиксов — пропускаем, handle_message обработает
        if bulk_add_mode.get(ADMIN_ID) in ("beat", "track", "remix"):
            return

        import agent_router
        agent_handle = agent_router.handle
    else:
        import user_agent
        agent_handle = user_agent.handle

    thinking = await update.message.reply_text("…")
    try:
        reply = await agent_handle(text)
    except Exception as e:
        logger.exception("agent crashed")
        reply = f"❌ Агент упал: {str(e)[:200]}"
    try:
        await thinking.edit_text(reply or "(пусто)")
    except Exception:
        await update.message.reply_text(reply or "(пусто)")


# ── Доставка бита + лицензии (общая для Stars и USDT) ────────

async def _deliver_mp3_lease(bot, user, beat: dict, *, payment_charge_id: str,
                             amount: int | float, currency: str) -> None:
    """Отправляет mp3 + txt-лицензию покупателю, логирует продажу, уведомляет админа."""
    # Юзер купил → удалить из pending re-marketing reminders.
    try:
        mark_bit_purchased(user.id, beat["id"])
    except Exception:
        pass
    buyer_name = (user.full_name or user.username or str(user.id)).strip()
    license_text = licensing.mp3_lease_text(
        buyer_name=buyer_name,
        buyer_tg_id=user.id,
        beat_name=beat["name"],
        bpm=beat.get("bpm"),
        key=beat.get("key"),
        payment_charge_id=payment_charge_id,
    )
    try:
        await bot.send_audio(
            user.id,
            audio=beat["file_id"],
            caption=f"🎹 {beat['name']}\n\nMP3 Lease — ты красавчик 🔥\nЛицензия ниже.",
        )
        license_bytes = io.BytesIO(license_text.encode("utf-8"))
        license_bytes.name = f"LICENSE_{beat['name'].replace(' ', '_')}_{user.id}.txt"
        await bot.send_document(
            user.id,
            document=InputFile(license_bytes, filename=license_bytes.name),
            caption="📄 Сохрани этот файл — это твоё подтверждение лицензии.",
        )
    except Exception as e:
        logger.exception("delivery failed")
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🚨 Оплата прошла, доставка сломалась!\n"
                f"User: {user.id} @{user.username}\nBeat: {beat['name']}\n"
                f"charge: {payment_charge_id}\nError: {e}",
            )
        except Exception:
            pass

    try:
        sales.log_sale(
            buyer_tg_id=user.id,
            buyer_username=user.username,
            buyer_name=buyer_name,
            beat_id=beat["id"],
            beat_name=beat["name"],
            license_type="mp3_lease",
            stars_amount=int(amount) if currency == "XTR" else 0,
            fiat_amount_minor=int(amount) if currency != "XTR" else 0,
            currency=currency,
            payment_charge_id=payment_charge_id,
            provider_charge_id=None,
            status="completed",
        )
    except Exception:
        logger.exception("sales.log_sale failed")

    try:
        # RUB amount приходит в копейках (minor units) — делим на 100 для показа
        # в рублях. USDT amount приходит в major units (20.00) — не делим.
        # XTR (Stars) — целое число звёзд, не делим.
        if currency == "XTR":
            amount_disp = f"{amount}⭐"
        elif currency == "RUB":
            amount_disp = f"{amount/100:g}₽"
        else:
            amount_disp = f"{amount} {currency}"
        await bot.send_message(
            ADMIN_ID,
            f"💰 Продажа MP3 Lease\n"
            f"Бит: {beat['name']}\n"
            f"Покупатель: {buyer_name} (@{user.username or '—'}, id={user.id})\n"
            f"Сумма: {amount_disp}\n"
            f"charge: {payment_charge_id}",
        )
    except Exception:
        pass


async def _deliver_bundle(bot, user, beats: list[dict], *, payment_charge_id: str,
                          amount: int | float, currency: str) -> None:
    """Delivery для bundle: N audio + одна общая лицензия. Логирует sales row
    per beat (license_type='bundle_3', meta.beat_ids=[...]).

    Failure handling: если конкретный send_audio упал — продолжаем с остальными,
    в конце шлём admin alert с failed списком.
    """
    buyer_name = (user.full_name or user.username or str(user.id)).strip()
    failed_beats: list[dict] = []
    # Очищаем все 3 бита из корзины ДО send_audio — даже если send упадёт,
    # не хотим повторно показывать «🛒 ✅ В корзине» на купленный бит.
    for b in beats:
        try:
            mark_bit_purchased(user.id, b["id"])
        except Exception:
            pass
        try:
            await bot.send_audio(
                user.id,
                audio=b["file_id"],
                caption=f"🎹 {b['name']}\n\n(bundle 3 бита — следующий ниже)",
            )
        except Exception:
            logger.exception("bundle: send_audio failed for beat %s", b.get("id"))
            failed_beats.append(b)

    license_text = licensing.bundled_mp3_lease_text(
        buyer_name=buyer_name,
        buyer_tg_id=user.id,
        beats=beats,
        payment_charge_id=payment_charge_id,
    )
    try:
        license_bytes = io.BytesIO(license_text.encode("utf-8"))
        license_bytes.name = f"LICENSE_BUNDLE_{user.id}.txt"
        await bot.send_document(
            user.id,
            document=InputFile(license_bytes, filename=license_bytes.name),
            caption="📄 Лицензия на все 3 бита — сохрани этот файл.",
        )
    except Exception:
        logger.exception("bundle: license send failed")
        failed_beats.append({"name": "license"})

    if failed_beats:
        try:
            await bot.send_message(
                ADMIN_ID,
                "🚨 Bundle delivery частично сломалась:\n"
                f"User: {user.id} @{user.username}\n"
                f"Failed: {[b.get('name') for b in failed_beats]}\n"
                f"charge: {payment_charge_id}",
            )
        except Exception:
            pass

    # sales: одна row per beat. Все ряды идут с одним payment_charge_id —
    # этого хватит чтобы группировать bundle через `GROUP BY payment_charge_id`
    # (UNIQUE constraint защитит от двойной записи на retry: первый INSERT
    # пройдёт, второй будет no-op для остальных битов из-за того же charge_id).
    # Чтобы избежать UNIQUE-конфликта — суффиксуем charge_id для остальных битов.
    for idx, b in enumerate(beats):
        sub_charge = payment_charge_id if idx == 0 else f"{payment_charge_id}#b{idx}"
        try:
            sales.log_sale(
                buyer_tg_id=user.id,
                buyer_username=user.username,
                buyer_name=buyer_name,
                beat_id=b["id"],
                beat_name=b["name"],
                license_type="bundle_3",
                stars_amount=int(amount) if currency == "XTR" else 0,
                fiat_amount_minor=int(amount) if currency != "XTR" else 0,
                currency=currency,
                payment_charge_id=sub_charge,
                provider_charge_id=None,
                status="completed",
            )
        except Exception:
            logger.exception("sales.log_sale (bundle, beat %s) failed", b.get("id"))

    try:
        if currency == "XTR":
            amount_disp = f"{amount}⭐"
        elif currency == "RUB":
            amount_disp = f"{amount/100:g}₽"
        else:
            amount_disp = f"{amount} {currency}"
        names = ", ".join(b.get("name", "?") for b in beats)
        await bot.send_message(
            ADMIN_ID,
            f"🎁 Продажа BUNDLE 3 бита\n"
            f"Биты: {names}\n"
            f"Покупатель: {buyer_name} (@{user.username or '—'}, id={user.id})\n"
            f"Сумма: {amount_disp}\n"
            f"charge: {payment_charge_id}",
        )
    except Exception:
        pass


async def _deliver_product(bot, user, product: dict, *, payment_charge_id: str,
                            amount: int | float, currency: str) -> None:
    """Отправляет zip-архив + txt-лицензию покупателю для drum kit / sample
    pack / loop pack. protect_content=True блокирует forward/save в TG —
    файл можно только скачать, не переслать.
    """
    buyer_name = (user.full_name or user.username or str(user.id)).strip()
    content_type = product.get("content_type", "samplepack")
    type_label = licensing.PRODUCT_TYPE_LABELS.get(content_type, content_type)
    license_text = licensing.product_license_text(
        buyer_name=buyer_name,
        buyer_tg_id=user.id,
        product_type=content_type,
        product_name=product["name"],
        payment_charge_id=payment_charge_id,
    )
    try:
        await bot.send_document(
            user.id,
            document=product["file_id"],
            caption=(
                f"📦 {type_label}: {product['name']}\n\n"
                f"Красавчик, забирай 🔥\n"
                f"Лицензия и условия — во втором файле."
            ),
            protect_content=True,
        )
        license_bytes = io.BytesIO(license_text.encode("utf-8"))
        license_bytes.name = f"LICENSE_{product['name'].replace(' ', '_')}_{user.id}.txt"
        await bot.send_document(
            user.id,
            document=InputFile(license_bytes, filename=license_bytes.name),
            caption="📄 Лицензионное соглашение — сохрани этот файл.",
            protect_content=True,
        )
    except Exception as e:
        logger.exception("product delivery failed")
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🚨 Оплата прошла, доставка продукта сломалась!\n"
                f"User: {user.id} @{user.username}\nProduct: {product['name']} ({type_label})\n"
                f"charge: {payment_charge_id}\nError: {e}",
            )
        except Exception:
            pass

    try:
        sales.log_sale(
            buyer_tg_id=user.id,
            buyer_username=user.username,
            buyer_name=buyer_name,
            beat_id=product["id"],
            beat_name=product["name"],
            license_type=content_type,
            stars_amount=int(amount) if currency == "XTR" else 0,
            fiat_amount_minor=int(amount) if currency != "XTR" else 0,
            currency=currency,
            payment_charge_id=payment_charge_id,
            provider_charge_id=None,
            status="completed",
        )
    except Exception:
        logger.exception("sales.log_sale failed (product)")

    try:
        # RUB в копейках → рубли. USDT в major units. XTR — звёзды.
        if currency == "XTR":
            amount_disp = f"{amount}⭐"
        elif currency == "RUB":
            amount_disp = f"{amount/100:g}₽"
        else:
            amount_disp = f"{amount} {currency}"
        await bot.send_message(
            ADMIN_ID,
            f"📦 Продажа {type_label}\n"
            f"Продукт: {product['name']}\n"
            f"Покупатель: {buyer_name} (@{user.username or '—'}, id={user.id})\n"
            f"Сумма: {amount_disp}\n"
            f"charge: {payment_charge_id}",
        )
    except Exception:
        pass


async def _deliver_mixing_service(bot, user, *, payment_charge_id: str,
                                    amount: int | float, currency: str) -> None:
    """После оплаты mixing: клиенту отправляем инструкцию что прислать,
    админу — заявку с deadline'ом. Физический файл не отдаём — услуга.
    Оплата логируется в sales как license_type='mixing_service'.
    """
    buyer_name = (user.full_name or user.username or str(user.id)).strip()
    handle = "@" + user.username if user.username else user.full_name
    try:
        await bot.send_message(
            user.id,
            "✅ <b>Оплата получена, заказ на сведение оформлен!</b>\n\n"
            "<b>Что делать дальше:</b>\n"
            "1. Напиши в ЛС @iiiplfiii «пришёл на сведение» — я свяжусь с тобой\n"
            "2. Пришли:\n"
            "   • Стемы WAV (24-bit, 44.1 kHz) — вокал, биты, инструменты отдельными файлами\n"
            "   • Референс-трек (ссылку на Spotify / YT / как звучать)\n"
            "   • Краткое ТЗ: жанр, желаемая громкость, акценты\n"
            "3. Через 3-5 рабочих дней → готовый master WAV\n"
            "4. Если нужны правки — 1 ревизия в цене\n\n"
            f"<i>Номер заказа: {payment_charge_id[:16]}...</i>",
            parse_mode="HTML",
        )
    except Exception:
        logger.exception("mixing: client instructions send failed")

    try:
        amount_disp = f"{amount}⭐" if currency == "XTR" else (
            f"{amount/100:g}₽" if currency == "RUB" else f"{amount} {currency}"
        )
        await bot.send_message(
            ADMIN_ID,
            "🎛 <b>НОВЫЙ ЗАКАЗ: Сведение треков</b>\n\n"
            f"Клиент: {handle} (id={user.id})\n"
            f"Сумма: {amount_disp}\n"
            f"Charge: <code>{payment_charge_id}</code>\n\n"
            "⏱ Дедлайн: 3-5 рабочих дней с момента получения стемов.\n"
            "📬 Жди сообщения клиента с стемами в ЛС.",
            parse_mode="HTML",
        )
    except Exception:
        logger.exception("mixing: admin notification failed")

    try:
        sales.log_sale(
            buyer_tg_id=user.id,
            buyer_username=user.username,
            buyer_name=buyer_name,
            beat_id=0,
            beat_name="Mixing Service",
            license_type="mixing_service",
            stars_amount=int(amount) if currency == "XTR" else 0,
            fiat_amount_minor=int(amount) if currency != "XTR" else 0,
            currency=currency,
            payment_charge_id=payment_charge_id,
            provider_charge_id=None,
            status="completed",
        )
    except Exception:
        logger.exception("mixing: sales.log_sale failed")


# ── YooKassa webhook delivery ─────────────────────────────────

async def _deliver_yk_payment(bot, payment_id: str) -> bool:
    """Доставляет товар по YooKassa payment_id. Вызывается из webhook'а
    и из fallback polling'а.

    Idempotency (критично):
    1. Check `delivered_yk_payments` set (persistent) — already delivered → skip.
    2. Check `in_flight_yk` set (in-memory) — concurrent delivery → skip.
    3. Claim: add to in_flight_yk под lock'ом.
    4. Double-check через GET /v3/payments/{id} что status=succeeded — защита
       от fake webhook'ов (кто-то прислал UUID непроплаченного платежа).
    5. Deliver → mark delivered → drop pending.
    6. Finally: remove from in_flight (даже если exception). Позволяет retry
       при транзиентной ошибке; replay не сработает, ибо delivered-set уже стоит.

    Returns True если доставили, False если ошибка / уже доставлено / fake.
    """
    import yookassa_api

    # ── Idempotency claim ───────────────────────────────────────
    async with _get_yk_lock():
        if payment_id in delivered_yk_payments:
            logger.info("yk: payment %s already delivered, skip replay", payment_id)
            return False
        if payment_id in in_flight_yk:
            logger.info("yk: payment %s delivery in-flight, skip duplicate", payment_id)
            return False
        in_flight_yk.add(payment_id)

    try:
        # ── Double-check статус у YooKassa (защита от fake webhook'ов) ──
        try:
            yk_info = await yookassa_api.get_payment(payment_id)
        except Exception:
            logger.exception("yk: get_payment %s failed", payment_id)
            return False
        if yk_info.get("status") != "succeeded":
            logger.warning("yk: payment %s status=%s, not succeeded, skip",
                           payment_id, yk_info.get("status"))
            return False

        # ── Найти локальную запись ──
        pending = pending_yk_payments.get(payment_id)
        if not pending:
            # Webhook пришёл до persistence (race) или после redeploy'а.
            # Берём metadata из YooKassa — её мы же и ставим при create_payment,
            # а менять её без наших creds нельзя.
            md = yk_info.get("metadata") or {}
            if not md.get("user_id"):
                logger.warning("yk: payment %s not in pending + no metadata, skip",
                               payment_id)
                return False
            pending = {
                "type": md.get("type", "unknown"),
                "user_id": int(md["user_id"]),
                "username": md.get("username", ""),
                "beat_id": int(md["beat_id"]) if md.get("beat_id") else None,
            }

        ptype = pending.get("type")
        user_id = pending.get("user_id")

        # Fake user object для существующих _deliver_* функций
        try:
            user_chat = await bot.get_chat(user_id)
            class _User:
                pass
            user = _User()
            user.id = user_id
            user.full_name = user_chat.full_name
            user.username = user_chat.username
        except Exception:
            class _U2:
                pass
            user = _U2()
            user.id = user_id
            user.full_name = pending.get("username") or str(user_id)
            user.username = pending.get("username")

        amount_value = yk_info.get("amount", {}).get("value", "0")
        try:
            amount_rub = float(amount_value)
        except ValueError:
            amount_rub = 0.0
        amount_kopecks = int(amount_rub * 100)
        charge_id = f"yookassa:{payment_id}"

        try:
            if ptype == "mp3_lease":
                beat_id = pending.get("beat_id")
                beat = beats_db.get_beat_by_id(beat_id) if beat_id else None
                if not beat:
                    await bot.send_message(
                        user_id,
                        f"⚠️ Бит пропал из каталога. Пиши @iiiplfiii — разберёмся.",
                    )
                    await bot.send_message(
                        ADMIN_ID,
                        f"🚨 YooKassa оплата прошла, но бит id={beat_id} не найден!\n"
                        f"payment={payment_id}, user={user_id}, amount={amount_rub}₽",
                    )
                    return False
                await _deliver_mp3_lease(
                    bot, user, beat,
                    payment_charge_id=charge_id,
                    amount=amount_kopecks,
                    currency="RUB",
                )
            elif ptype == "mixing_service":
                await _deliver_mixing_service(
                    bot, user,
                    payment_charge_id=charge_id,
                    amount=amount_kopecks,
                    currency="RUB",
                )
            elif ptype == "product":
                pid = pending.get("beat_id")
                product = beats_db.get_beat_by_id(pid) if pid else None
                if not product or product.get("content_type") not in licensing.PRODUCT_TYPE_LABELS:
                    await bot.send_message(
                        user_id,
                        "⚠️ Продукт пропал. Пиши @iiiplfiii — разберёмся.",
                    )
                    return False
                await _deliver_product(
                    bot, user, product,
                    payment_charge_id=charge_id,
                    amount=amount_kopecks,
                    currency="RUB",
                )
            elif ptype == "bundle":
                beat_ids = pending.get("beat_ids") or []
                beats = [beats_db.get_beat_by_id(int(bid)) for bid in beat_ids]
                beats = [b for b in beats if b]
                if len(beats) != BUNDLE_TOTAL:
                    await bot.send_message(
                        user_id,
                        "⚠️ Часть битов из бандла пропала. Пиши @iiiplfiii — разберёмся.",
                    )
                    await bot.send_message(
                        ADMIN_ID,
                        f"🚨 YooKassa bundle оплата прошла, но {BUNDLE_TOTAL - len(beats)} бит(ов) недоступн(ы).\n"
                        f"payment={payment_id}, user={user_id}, beat_ids={beat_ids}",
                    )
                    return False
                await _deliver_bundle(
                    bot, user, beats,
                    payment_charge_id=charge_id,
                    amount=amount_kopecks,
                    currency="RUB",
                )
            else:
                logger.warning("yk: unknown type=%s payment=%s", ptype, payment_id)
                return False
        except Exception:
            # Транзиентная ошибка (сеть, TG API, Supabase). Не помечаем
            # delivered — fallback polling через 5 мин повторит попытку.
            logger.exception("yk: delivery failed for %s", payment_id)
            return False

        # Успешно доставили → mark delivered (idempotency guard от replay) +
        # drop pending. Порядок важен: mark делаем ПЕРЕД drop'ом, иначе между
        # ними может прилететь ещё один webhook и пройдёт проверку.
        await _mark_yk_delivered(payment_id)
        await _drop_yk_pending(payment_id)
        logger.info("yk: delivered %s (type=%s, user=%d, amount=%s₽)",
                    payment_id, ptype, user_id, amount_rub)
        return True
    finally:
        async with _get_yk_lock():
            in_flight_yk.discard(payment_id)


async def yk_fallback_polling(bot):
    """Fallback: раз в 5 минут проверяем pending payments старше 2 мин.
    Если webhook не дошёл (network issue, Render sleep) — polling'ом
    забираем оплаченные.

    Stale >48h — дропаем без delivery (клиент уже, видимо, забил).
    """
    import yookassa_api
    while True:
        await asyncio.sleep(5 * 60)
        if not pending_yk_payments:
            continue
        try:
            now = datetime.now(ZoneInfo("Europe/Moscow"))
            stale_to_drop = []
            for pid, pending in list(pending_yk_payments.items()):
                try:
                    created = datetime.fromisoformat(pending["created_at"])
                    age_min = (now - created).total_seconds() / 60
                except Exception:
                    age_min = 0
                if age_min < 2:
                    continue  # слишком свежий, webhook ещё может прийти
                if age_min > 48 * 60:
                    stale_to_drop.append(pid)
                    continue
                try:
                    info = await yookassa_api.get_payment(pid)
                    if info.get("status") == "succeeded":
                        logger.info("yk_fallback: polling found succeeded %s (webhook miss), delivering", pid)
                        await _deliver_yk_payment(bot, pid)
                    elif info.get("status") == "canceled":
                        logger.info("yk_fallback: payment %s canceled, dropping", pid)
                        await _drop_yk_pending(pid)
                except Exception:
                    logger.warning("yk_fallback: get_payment %s failed", pid)
            for pid in stale_to_drop:
                logger.warning("yk_fallback: stale pending %s >48h, dropping", pid)
                await _drop_yk_pending(pid)
        except Exception:
            logger.exception("yk_fallback polling iteration failed")


# ── Telegram Stars payments ───────────────────────────────────

async def handle_precheckout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """PreCheckout: апрувим всё валидное. Отказ — только если payload битый
    или товар (бит/пак) пропал.

    Payload форматы:
        mp3_lease:<beat_id>          — MP3 Lease на бит
        product:<product_id>         — drum kit / sample pack / loop pack
        mixing_service:<variant>     — сведение треков (stars/usdt/rub)
    """
    pcq = update.pre_checkout_query
    payload = pcq.invoice_payload or ""
    try:
        if payload.startswith("mp3_lease:"):
            beat_id = int(payload.split(":", 1)[1])
            if not beats_db.get_beat_by_id(beat_id):
                await pcq.answer(ok=False, error_message="Бит больше недоступен")
                return
        elif payload.startswith("product:"):
            pid = int(payload.split(":", 1)[1])
            prod = beats_db.get_beat_by_id(pid)
            if not prod or prod.get("content_type") not in licensing.PRODUCT_TYPE_LABELS:
                await pcq.answer(ok=False, error_message="Продукт больше недоступен")
                return
        elif payload.startswith("mixing_service:"):
            # Mixing service — ничего дополнительно валидировать не нужно,
            # услуга всегда доступна. Stars/USDT/RUB — все варианты ok.
            pass
        elif payload.startswith("bundle:"):
            # bundle:<id1>,<id2>,<id3> — все 3 бита должны существовать.
            ids_csv = payload.split(":", 1)[1]
            beat_ids = [int(x) for x in ids_csv.split(",") if x.strip()]
            if len(beat_ids) != BUNDLE_TOTAL:
                await pcq.answer(ok=False, error_message="Bundle некорректен")
                return
            for bid in beat_ids:
                if not beats_db.get_beat_by_id(bid):
                    await pcq.answer(ok=False, error_message="Один из битов бандла недоступен")
                    return
        else:
            await pcq.answer(ok=False, error_message="Неизвестный тип покупки")
            return
    except ValueError:
        await pcq.answer(ok=False, error_message="Некорректный payload")
        return
    await pcq.answer(ok=True)


async def handle_successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """После успешной оплаты Stars: маршрутизируем доставку по payload."""
    msg = update.message
    if not msg or not msg.successful_payment:
        return
    sp = msg.successful_payment
    user = msg.from_user
    bot = context.bot
    payload = sp.invoice_payload or ""

    try:
        if payload.startswith("mp3_lease:"):
            beat_id = int(payload.split(":", 1)[1])
            beat = beats_db.get_beat_by_id(beat_id)
            if not beat:
                await msg.reply_text("⚠️ Бит пропал из каталога. Напишу автору: @iiiplfiii")
                await bot.send_message(
                    ADMIN_ID,
                    f"🚨 Stars-оплата прошла, но бит id={beat_id} не найден.\n"
                    f"User: {user.id} @{user.username}\ncharge: {sp.telegram_payment_charge_id}",
                )
                return
            await _deliver_mp3_lease(
                bot, user, beat,
                payment_charge_id=sp.telegram_payment_charge_id,
                amount=sp.total_amount,
                currency=sp.currency or "XTR",
            )
            return

        if payload.startswith("product:"):
            pid = int(payload.split(":", 1)[1])
            product = beats_db.get_beat_by_id(pid)
            if not product or product.get("content_type") not in licensing.PRODUCT_TYPE_LABELS:
                await msg.reply_text("⚠️ Продукт пропал из каталога. Напишу автору: @iiiplfiii")
                await bot.send_message(
                    ADMIN_ID,
                    f"🚨 Stars-оплата прошла, но продукт id={pid} не найден.\n"
                    f"User: {user.id} @{user.username}\ncharge: {sp.telegram_payment_charge_id}",
                )
                return
            await _deliver_product(
                bot, user, product,
                payment_charge_id=sp.telegram_payment_charge_id,
                amount=sp.total_amount,
                currency=sp.currency or "XTR",
            )
            return

        if payload.startswith("mixing_service:"):
            # Mixing service — не файл, а услуга. Юзер платит → бот шлёт
            # инструкцию как отправить стемы. Админ получает уведомление
            # с deadline reminder через 3-5 рабочих дней.
            await _deliver_mixing_service(
                bot, user,
                payment_charge_id=sp.telegram_payment_charge_id,
                amount=sp.total_amount,
                currency=sp.currency or "XTR",
            )
            return

        if payload.startswith("bundle:"):
            ids_csv = payload.split(":", 1)[1]
            try:
                beat_ids = [int(x) for x in ids_csv.split(",") if x.strip()]
            except ValueError:
                beat_ids = []
            beats = [beats_db.get_beat_by_id(bid) for bid in beat_ids]
            beats = [b for b in beats if b]
            if len(beats) != BUNDLE_TOTAL:
                await msg.reply_text("⚠️ Часть битов из бандла пропала. Напишу автору: @iiiplfiii")
                await bot.send_message(
                    ADMIN_ID,
                    f"🚨 Stars-bundle оплата прошла, но {BUNDLE_TOTAL - len(beats)} бит(ов) недоступн(ы).\n"
                    f"User: {user.id} @{user.username}\n"
                    f"Requested ids: {beat_ids}\n"
                    f"charge: {sp.telegram_payment_charge_id}",
                )
                return
            await _deliver_bundle(
                bot, user, beats,
                payment_charge_id=sp.telegram_payment_charge_id,
                amount=sp.total_amount,
                currency=sp.currency or "XTR",
            )
            return

        logger.warning("successful_payment: unknown payload %s", payload)
    except Exception:
        logger.exception("handle_successful_payment failed")


# ── Welcome funnel: детект new channel subscribers ────────────

WELCOME_TEXT_NEW_SUB = (
    "👋 Привет! Добро пожаловать в <b>@iiiplfiii</b>.\n\n"
    "Канал про hard trap beats в стиле Memphis/Detroit.\n"
    "Type beats под Kenny Muney, Key Glock, Future, Obladaet и др.\n\n"
    "🎁 Бесплатный sample pack ждёт тебя — напиши /start в @triplekillpost_bot\n"
    "🎧 Каталог 167 битов — там же в боте\n"
    "💰 MP3 Lease от 1500⭐ / 20 USDT / 1700₽\n"
    "🎛 Сведение треков — 5000₽\n\n"
    "Вопросы — пиши @iiiplfiii"
)


async def handle_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Детект новых подписчиков канала @iiiplfiii → welcome DM (если юзер
    когда-то делал /start с ботом) или silent (TG запрещает первым писать).

    Требует бот = админ канала с правом видеть members.

    Transition: left/kicked → member = новая подписка.
    Только для CHANNEL_ID, игнорируем другие чаты где бот.
    """
    cmu = update.chat_member
    if not cmu:
        return
    chat = cmu.chat
    # Фильтр: только наш канал
    if str(chat.id) != str(CHANNEL_ID) and chat.username != CHANNEL_ID.lstrip("@"):
        return
    old = cmu.old_chat_member.status if cmu.old_chat_member else None
    new = cmu.new_chat_member.status if cmu.new_chat_member else None
    # Новая подписка: раньше left/kicked → теперь member
    if old in ("left", "kicked", "restricted") and new == "member":
        new_user = cmu.new_chat_member.user
        if new_user.is_bot:
            return
        try:
            await context.bot.send_message(
                new_user.id,
                WELCOME_TEXT_NEW_SUB,
                parse_mode="HTML",
            )
            logger.info("welcome: DM sent to new subscriber %s (id=%d)",
                        new_user.username or new_user.full_name, new_user.id)
        except TelegramError as e:
            # Forbidden — юзер не /start'ал с ботом. Telegram не даёт
            # первым писать. Fallback — ничего не делаем, не спамим канал.
            logger.info("welcome: cannot DM %s (not started bot): %s",
                        new_user.id, e)
        except Exception:
            logger.exception("welcome: unexpected error for user %d", new_user.id)


# ── CryptoBot (USDT) payments ─────────────────────────────────

# Активные USDT-инвойсы: invoice_id → (user_id, beat_id, created_at)
pending_usdt_invoices: dict[int, dict] = {}


async def poll_usdt_invoice(bot, invoice_id: int, user_id: int, item_id: int,
                            kind: str = "beat", timeout_sec: int = 1800):
    """Пуллит инвойс до оплаты/expiry. Доставляет бит/продукт/mixing/bundle при paid.

    kind="beat" → _deliver_mp3_lease, kind="product" → _deliver_product,
    kind="mixing" → _deliver_mixing_service, kind="bundle" → _deliver_bundle
    (item_id игнорируется — beat_ids читаются из pending_usdt_invoices[invoice_id]).

    try/finally гарантирует очистку записи в pending_usdt_invoices даже
    при exception — защита от утечки памяти при долгом uptime.
    """
    import time
    started = time.monotonic()
    try:
        while time.monotonic() - started < timeout_sec:
            await asyncio.sleep(5)
            try:
                inv = await cryptobot.get_invoice(invoice_id)
            except Exception as e:
                logger.warning("poll_usdt_invoice getInvoices err: %s", e)
                continue
            if not inv:
                continue
            status = inv.get("status")
            if status == "paid":
                # Для mixing не нужен item_id (это услуга, не файл)
                try:
                    user = await bot.get_chat(user_id)
                except Exception:
                    class _U:
                        pass
                    user = _U()
                    user.id = user_id
                    user.full_name = str(user_id)
                    user.username = None
                charge = f"cryptobot:{invoice_id}:{inv.get('hash', '')}"
                amount = float(inv.get("amount") or 0)
                currency = inv.get("asset") or "USDT"

                if kind == "mixing":
                    await _deliver_mixing_service(
                        bot, user,
                        payment_charge_id=charge, amount=amount, currency=currency,
                    )
                    return

                if kind == "bundle":
                    pending = pending_usdt_invoices.get(invoice_id) or {}
                    beat_ids = pending.get("beat_ids") or []
                    beats = [beats_db.get_beat_by_id(int(bid)) for bid in beat_ids]
                    beats = [b for b in beats if b]
                    if len(beats) != BUNDLE_TOTAL:
                        try:
                            await bot.send_message(
                                user_id,
                                "⚠️ Часть битов из бандла пропала. Напишу автору: @iiiplfiii",
                            )
                        except Exception:
                            pass
                        return
                    await _deliver_bundle(
                        bot, user, beats,
                        payment_charge_id=charge, amount=amount, currency=currency,
                    )
                    return

                item = beats_db.get_beat_by_id(item_id)
                label = "продукт" if kind == "product" else "бит"
                if not item:
                    try:
                        await bot.send_message(
                            user_id,
                            f"⚠️ {label.capitalize()} пропал из каталога. Напишу автору: @iiiplfiii",
                        )
                    except Exception:
                        pass
                    return
                if kind == "product":
                    await _deliver_product(
                        bot, user, item,
                        payment_charge_id=charge, amount=amount, currency=currency,
                    )
                else:
                    await _deliver_mp3_lease(
                        bot, user, item,
                        payment_charge_id=charge, amount=amount, currency=currency,
                    )
                return
            if status == "expired":
                return
    finally:
        # Гарантированный cleanup: paid/expired/timeout/exception — всё удалится.
        pending_usdt_invoices.pop(invoice_id, None)


# ── Content reminders ─────────────────────────────────────────

ADMIN_PREFS_PATH = os.path.join(BASE_DIR, "admin_prefs.json")

CONTENT_REMINDER_TEMPLATES = {
    0: (  # Monday
        "🎵 Понедельник — Process Reveal Shorts",
        "Есть свежий бит? 30 сек Shorts:\n"
        "• 0-3с: hands/FL screen, hard 808 hit\n"
        "• 3-20с: speed-up процесс (drums → bass → melody)\n"
        "• 20-27с: финал бита полных 5 сек\n"
        "• 27-30с: CTA «lease в боте 1500⭐»\n\n"
        "Снял? Загрузи бит в бот — я выдам caption+title+description."
    ),
    2: (  # Wednesday
        "🛠 Среда — Gear Talk / Technique",
        "Одна техника, 30 сек, voice-over + screen. Варианты:\n"
        "• «Почему у меня 808 низкий — я не boost, я cut низ у мелодии»\n"
        "• «3 sample pack'а которые всегда юзаю в memphis»\n"
        "• «Detroit hi-hat pattern — показываю за 20 сек»\n\n"
        "Такой контент конвертится в покупателей твоего drumkit (когда запустим)."
    ),
    4: (  # Friday
        "🎤 Пятница — UGC reaction / Story beat",
        "Если кто-то из рэперов закинул трек на твой бит → сними 15 сек реакции.\n"
        "Если нет — Story beat: расскажи откуда пришёл твой любимый бит недели (15 сек, искренне, без скрипта).\n\n"
        "Пятничный контент = самый engagement-heavy, алгоритм любит «проявление»."
    ),
}


def _load_admin_prefs() -> dict:
    """Загружает admin_prefs из disk. На Render free disk эфемерный →
    после redeploy файл потерян → fallback на env defaults.

    Env-overrides (для persistence через redeploy):
    - AUTO_REPOST_DEFAULT="1" → auto_repost_enabled=True если файла нет
    - CONTENT_REMINDERS_DEFAULT="1" → content_reminders=True если файла нет
    """
    defaults = {
        "content_reminders": (os.getenv("CONTENT_REMINDERS_DEFAULT", "1") == "1"),
        "auto_repost_enabled": (os.getenv("AUTO_REPOST_DEFAULT", "0") == "1"),
        "last_reminder_date": None,
    }
    if not os.path.exists(ADMIN_PREFS_PATH):
        return defaults
    try:
        with open(ADMIN_PREFS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        # Merge с defaults — если файл существует но не имеет какого-то ключа
        merged = {**defaults, **data}
        return merged
    except Exception:
        return defaults


def _save_admin_prefs(prefs: dict) -> None:
    try:
        with open(ADMIN_PREFS_PATH, "w", encoding="utf-8") as f:
            json.dump(prefs, f, ensure_ascii=False, indent=2)
        # Mark for git autopush — admin_prefs тоже эфемерный на Render free disk.
        try:
            import git_autopush
            git_autopush.mark_dirty(ADMIN_PREFS_PATH)
        except Exception:
            logger.warning("git_autopush.mark_dirty(admin_prefs) failed (non-fatal)", exc_info=True)
    except Exception:
        logger.exception("save admin_prefs failed (non-fatal)")


async def content_reminder_scheduler(bot):
    """Шлёт ADMIN_ID напоминания о контенте Пн/Ср/Пт в 20:00 МСК.

    Dedupe через last_reminder_date — одно напоминание в сутки даже если
    bot рестартнулся. По умолчанию включено; /content off выключает.
    """
    while True:
        try:
            prefs = _load_admin_prefs()
            if prefs.get("content_reminders", True):
                now_msk = datetime.now(ZoneInfo("Europe/Moscow"))
                today_str = now_msk.date().isoformat()
                if (
                    now_msk.weekday() in CONTENT_REMINDER_TEMPLATES
                    and now_msk.hour == 20
                    and prefs.get("last_reminder_date") != today_str
                ):
                    title, body = CONTENT_REMINDER_TEMPLATES[now_msk.weekday()]
                    try:
                        await bot.send_message(
                            ADMIN_ID,
                            f"<b>{title}</b>\n\n{body}\n\n"
                            "<i>Отключить: /content off · статус: /content status</i>",
                            parse_mode="HTML",
                        )
                        prefs["last_reminder_date"] = today_str
                        _save_admin_prefs(prefs)
                        logger.info("content_reminder: sent for weekday=%d", now_msk.weekday())
                    except Exception:
                        logger.exception("content_reminder: send failed")
        except Exception:
            logger.exception("content_reminder_scheduler iteration failed")
        await asyncio.sleep(15 * 60)  # проверяем каждые 15 минут


async def cmd_content_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Вкл/выкл напоминания о контенте. Только админ."""
    if update.effective_user.id != ADMIN_ID:
        return
    prefs = _load_admin_prefs()
    arg = (context.args[0].lower() if context.args else "").strip()
    if arg == "on":
        prefs["content_reminders"] = True
        _save_admin_prefs(prefs)
        await update.message.reply_text(
            "✅ Напоминания включены.\n\n"
            "Пн — Process Reveal\n"
            "Ср — Gear Talk\n"
            "Пт — UGC / Story beat\n\n"
            "Все в 20:00 МСК."
        )
    elif arg == "off":
        prefs["content_reminders"] = False
        _save_admin_prefs(prefs)
        await update.message.reply_text("🔕 Напоминания выключены. Включить обратно: /content on")
    elif arg == "status":
        state = "ON ✅" if prefs.get("content_reminders", True) else "OFF 🔕"
        last = prefs.get("last_reminder_date") or "—"
        await update.message.reply_text(
            f"Напоминания: {state}\n"
            f"Последний пинг: {last}\n\n"
            "График: Пн (Process Reveal) · Ср (Gear Talk) · Пт (UGC/Story) — 20:00 МСК"
        )
    else:
        await update.message.reply_text(
            "Usage:\n"
            "/content on — включить\n"
            "/content off — выключить\n"
            "/content status — проверить"
        )


async def cmd_cancel_excl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отменяет активный exclusive inquiry (если юзер передумал)."""
    if context.user_data.pop("pending_excl", None):
        await update.message.reply_text("✖️ Exclusive inquiry отменён.")
    else:
        await update.message.reply_text("Нет активного запроса.")


async def cmd_feature(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/feature <track_url> <beat_id>

    Social proof: админ постит в канал «🎤 На бите X рэпер записал трек Y →
    [link]». Это +trust для новых подписчиков (видно что бит реально
    работает у артистов).

    Опционально +3-й аргумент: ссылка на артиста/название для отображения.
    """
    if update.effective_user.id != ADMIN_ID:
        return
    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: /feature <track_url> <beat_id> [<artist_name>]\n\n"
            "Пример: /feature https://soundcloud.com/user/track 9948215\n"
            "Или: /feature https://youtu.be/xxx 9948215 @rapper_name"
        )
        return
    track_url = args[0].strip()
    # URL validation: только http/https. Иначе в канал можно запостить
    # `javascript:...` / `tg://...` которое ведёт себя непредсказуемо.
    if not (track_url.startswith("http://") or track_url.startswith("https://")):
        await update.message.reply_text(
            f"⚠️ track_url должен начинаться с http:// или https://, получил: {track_url[:50]}"
        )
        return
    try:
        beat_id = int(args[1])
    except ValueError:
        await update.message.reply_text(f"⚠️ beat_id должен быть числом, получил: {args[1]}")
        return
    artist_display = " ".join(args[2:]).strip() if len(args) > 2 else ""

    beat = beats_db.get_beat_by_id(beat_id)
    if not beat:
        await update.message.reply_text(
            f"⚠️ Бит id={beat_id} не найден в каталоге. Проверь id через /stats."
        )
        return

    beat_name = beat.get("name", "?")
    bpm = beat.get("bpm", "?")
    key = beat.get("key", "?")
    # Escape всё что идёт в HTML post: beat_name/artist/track_url могут
    # содержать `<` / `&` (старые биты с парсинга из channel text, URL
    # с query-параметрами). Без escape parse_mode=HTML либо сломается,
    # либо (хуже) позволит inject'ить fake tag в publicly-visible post.
    beat_name_safe = html.escape(str(beat_name))
    bpm_safe = html.escape(str(bpm))
    key_safe = html.escape(str(key))
    artist_safe = html.escape(artist_display)
    track_url_safe = html.escape(track_url)
    artist_line = f" — {artist_safe}" if artist_display else ""

    post_text = (
        f"🎤 <b>На бите «{beat_name_safe}» записан трек{artist_line}</b>\n\n"
        f"⚡ {bpm_safe} BPM · 🎹 {key_safe}\n\n"
        f"🎧 Слушать: {track_url_safe}\n\n"
        f"<i>Хочешь такой же? Этот бит ещё доступен:</i>"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"💰 Купить «{beat_name}» — {licensing.PRICE_MP3_STARS}⭐ / {licensing.PRICE_MP3_RUB}₽",
            url=f"https://t.me/{beat_post_builder.BOT_USERNAME}?start=buy_{beat_id}"
        )],
    ])
    try:
        sent = await context.bot.send_message(
            CHANNEL_ID, post_text, parse_mode="HTML", reply_markup=kb,
            disable_web_page_preview=False,
        )
        # Лог события
        try:
            import post_analytics
            post_analytics.log_event(
                kind="feature", beat_id=beat_id, beat_name=beat_name,
                bpm=beat.get("bpm"), key=beat.get("key", ""),
                artist=artist_display,
                track_url=track_url,
                tg_message_id=sent.message_id,
                caption=post_text[:500],
            )
        except Exception:
            logger.exception("feature: post_analytics log failed (non-fatal)")
        await update.message.reply_text(
            f"✅ Feature опубликован в канал: https://t.me/{CHANNEL_ID.lstrip('@')}/{sent.message_id}"
        )
    except Exception as e:
        logger.exception("feature: channel post failed")
        await update.message.reply_text(f"❌ Не смог запостить в канал: {e}")


# ── /quick_meta ───────────────────────────────────────────────
# Batch UX для дозаполнения key у битов которые получили BPM от librosa
# но не имеют ноты. Юзер слушает 10-15 сек → кликает кнопку → next.
# Цель: расширить pool готовых для auto-repost битов с 32 до ~143.

_KEY_NOTES = ["C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B"]


def _kb_quick_meta_minor(beat_id: int) -> InlineKeyboardMarkup:
    """Picker минорных тональностей (80% hard trap = minor) + переключатель."""
    rows = []
    for i in range(0, 12, 4):
        rows.append([
            InlineKeyboardButton(f"{n}m", callback_data=f"qm_set:{beat_id}:{n}m")
            for n in _KEY_NOTES[i:i+4]
        ])
    rows.append([
        InlineKeyboardButton("→ Мажор", callback_data=f"qm_major:{beat_id}"),
        InlineKeyboardButton("⏭ Skip", callback_data=f"qm_skip:{beat_id}"),
        InlineKeyboardButton("🛑 Stop", callback_data="qm_stop"),
    ])
    rows.append([
        InlineKeyboardButton("🚫 Не бит (трек/ремикс)", callback_data=f"qm_not_beat:{beat_id}"),
    ])
    return InlineKeyboardMarkup(rows)


def _kb_quick_meta_major(beat_id: int) -> InlineKeyboardMarkup:
    """Picker мажорных тональностей."""
    rows = []
    for i in range(0, 12, 4):
        rows.append([
            InlineKeyboardButton(n, callback_data=f"qm_set:{beat_id}:{n}")
            for n in _KEY_NOTES[i:i+4]
        ])
    rows.append([
        InlineKeyboardButton("← Минор", callback_data=f"qm_minor:{beat_id}"),
        InlineKeyboardButton("⏭ Skip", callback_data=f"qm_skip:{beat_id}"),
        InlineKeyboardButton("🛑 Stop", callback_data="qm_stop"),
    ])
    rows.append([
        InlineKeyboardButton("🚫 Не бит (трек/ремикс)", callback_data=f"qm_not_beat:{beat_id}"),
    ])
    return InlineKeyboardMarkup(rows)


def _pick_next_quick_meta_beat() -> dict | None:
    """Следующий бит для /quick_meta: BPM>0, key пустой, content_type=beat,
    file_id есть. Sort by id (deterministic order)."""
    candidates = [
        b for b in beats_db.BEATS_CACHE
        if b.get("content_type", "beat") == "beat"
        and b.get("file_id")
        and (b.get("bpm") or 0) > 0
        and (not b.get("key") or b.get("key") == "-")
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda b: b.get("id", 0))
    return candidates[0]


async def _send_quick_meta_card(bot, chat_id: int, user_id: int):
    """Шлёт audio + key-picker для следующего бита. Используется и из
    /quick_meta и из callback'ов qm_set/qm_skip (после save показываем next).

    Single-audio: использует last_bit_audio_msg чтобы предыдущий аудио
    исчез при кликах next/skip.
    """
    beat = _pick_next_quick_meta_beat()
    if beat is None:
        await bot.send_message(
            chat_id,
            "🎉 Все биты с BPM получили key! Pool готовых битов для auto-repost "
            "максимально расширен. Запусти `/auto_repost status` посмотреть итог.",
        )
        return

    # Single-audio cleanup: удаляем предыдущий бит-аудио этого юзера
    prev = last_bit_audio_msg.get(user_id)
    if prev:
        try:
            await bot.delete_message(chat_id, prev)
        except Exception:
            pass

    bid = beat["id"]
    name = beat.get("name", "?")[:60]
    bpm = beat.get("bpm")
    tags = ", ".join(beat.get("tags") or [])[:60]
    caption = (
        f"🎵 <b>id={bid}</b>\n"
        f"<i>{name}</i>\n"
        f"⚡ {bpm} BPM | tags: {tags or '—'}\n\n"
        f"<b>Какая тональность?</b>"
    )
    try:
        sent = await bot.send_audio(
            chat_id,
            audio=beat["file_id"],
            caption=caption,
            parse_mode="HTML",
            reply_markup=_kb_quick_meta_minor(bid),
        )
        last_bit_audio_msg[user_id] = sent.message_id
    except Exception as e:
        logger.warning("/quick_meta send_audio failed for %d: %s", bid, e)
        await bot.send_message(chat_id, f"⚠️ id={bid} — file_id не работает, skip")
        # Skip and try next
        beat["key"] = "—broken—"  # marker, beat_record_to_meta вернёт None
        beats_db.save_beats()
        await _send_quick_meta_card(bot, chat_id, user_id)


async def cmd_quick_meta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/quick_meta` — batch UX для заполнения key у битов с librosa-BPM.

    Flow: бот шлёт первый бит без key + клавиатуру с тональностями →
    админ слушает 10-20 сек → кликает → бот сохраняет, шлёт следующий.
    Управление: ⏭ Skip (пропустить), 🛑 Stop (выйти), → Мажор (переключить
    клавиатуру на мажорные).
    """
    if update.effective_user.id != ADMIN_ID:
        return
    # Подсчитать сколько осталось
    pending = [
        b for b in beats_db.BEATS_CACHE
        if b.get("content_type", "beat") == "beat"
        and (b.get("bpm") or 0) > 0
        and (not b.get("key") or b.get("key") == "-")
        and b.get("file_id")
    ]
    if not pending:
        await update.message.reply_text(
            "🎉 Все биты с BPM уже имеют key. Auto-repost pool максимальный."
        )
        return
    await update.message.reply_text(
        f"🎹 Заполняем key для {len(pending)} битов.\n"
        f"Слушай 10-15 сек, кликай тональность. Skip — если не уверен, "
        f"Stop — выйти."
    )
    await _send_quick_meta_card(
        context.bot, update.effective_chat.id, update.effective_user.id,
    )


async def cmd_repost_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/repost_now <beat_id>` — single-shot repost legacy-бита из канала
    через нашу новую систему: long YT + новый TG post + удаление старого TG.

    Для битов которые когда-то висели на YT (канал был удалён) и сейчас
    остались только в TG канале без актуального YT video. Строит на лету
    thumbnail + video + caption, грузит на YT, постит в TG, удаляет
    старый TG post, обновляет каталог.

    Use case: тестовый прогон одного бита перед auto_repost_scheduler.
    """
    if update.effective_user.id != ADMIN_ID:
        return
    args = context.args or []
    if not args:
        await update.message.reply_text(
            "Usage: /repost_now <beat_id>\n\n"
            "Бит должен иметь BPM, key, артист-tag, file_id."
        )
        return
    try:
        beat_id = int(args[0])
    except ValueError:
        await update.message.reply_text(f"⚠️ beat_id должен быть числом: {args[0]}")
        return
    beat = beats_db.get_beat_by_id(beat_id)
    if not beat:
        await update.message.reply_text(f"❌ Бит id={beat_id} не найден в каталоге.")
        return
    if beat.get("content_type", "beat") != "beat":
        await update.message.reply_text(
            f"❌ id={beat_id} это не бит (content_type={beat.get('content_type')}). "
            "Repost только для битов."
        )
        return
    import beat_upload
    meta = beat_upload.beat_record_to_meta(beat)
    if meta is None:
        await update.message.reply_text(
            f"❌ Бит id={beat_id} не подходит для repost — нужны BPM, key, "
            f"артист-tag.\n"
            f"Получено: bpm={beat.get('bpm')} key={beat.get('key')!r} "
            f"tags={beat.get('tags')}"
        )
        return

    await _do_repost(context.bot, update.effective_chat.id, beat, meta)


async def _do_repost(bot, reply_chat_id: int, beat: dict, meta) -> str | None:
    """Полный repost flow для одного бита. Вызывается из /repost_now (manual)
    и из auto_repost_scheduler (auto).

    Returns repost_token (для последующего вызова Shorts builder) или None
    при ошибке.

    Шаги (всё синхронно, ~60 секунд):
    1. Download mp3 by file_id
    2. Build thumbnail (brand image) + video (ffmpeg)
    3. Build YT post + TG caption (LLM)
    4. YT upload long → vid
    5. Pinned auto-CTA comment + playlists
    6. New TG channel post → new_msg_id
    7. Delete old TG post by beat['msg_id']
    8. Update beats_db: post_url, msg_id, last_reposted_at
    9. Notification админу + кнопка 🎬 Сделать Shorts
    """
    import beat_post_builder
    import video_builder
    import yt_api
    from pathlib import Path as _P

    beat_id = beat["id"]
    old_msg_id = beat.get("msg_id")
    file_id = beat.get("file_id")
    if not file_id:
        await bot.send_message(reply_chat_id, f"❌ id={beat_id} нет file_id, не могу скачать mp3")
        return

    status = await bot.send_message(
        reply_chat_id,
        f"🔁 Repost {meta.name} ({meta.artist_display}) запущен...\n"
        f"⏱ ~60 сек: download → ffmpeg → YT upload → TG post → cleanup"
    )

    token = uuid.uuid4().hex[:12]
    mp3_path = _P(TEMP_UPLOAD_DIR) / f"{token}.mp3"
    video_path = _P(TEMP_UPLOAD_DIR) / f"{token}.mp4"
    thumb_path = _P(TEMP_UPLOAD_DIR) / f"{token}.jpg"

    loop = asyncio.get_running_loop()
    try:
        # 1. Download mp3
        f = await bot.get_file(file_id)
        await f.download_to_drive(str(mp3_path))

        # 2. Thumbnail (brand)
        brand_path = await loop.run_in_executor(None, _ensure_brand_image)
        if brand_path:
            import shutil
            shutil.copy2(brand_path, thumb_path)
        else:
            import thumbnail_generator
            thumbnail_generator.generate_thumbnail(meta.name, meta.artist_line, thumb_path)

        # 3. Video
        await loop.run_in_executor(
            None,
            lambda: video_builder.build_video(thumb_path, mp3_path, video_path),
        )

        # 4. mp3 duration для YT timestamps
        try:
            mp3_duration = video_builder.probe_duration(mp3_path)
        except Exception:
            mp3_duration = None

        yt_post = beat_post_builder.build_yt_post(
            meta, beat_id=beat_id, duration_sec=mp3_duration,
        )
        tg_caption, _tg_style = await beat_post_builder.build_tg_caption_async(
            meta, beat_id=beat_id,
        )

        # 5. YT upload
        vid = await loop.run_in_executor(
            None,
            lambda: yt_api.upload_video(
                video_path, yt_post.title, yt_post.description, yt_post.tags,
                thumb_path,
            ),
        )
        # 6. Auto-CTA + playlists (best-effort)
        try:
            await loop.run_in_executor(
                None, lambda: _post_cta_comment(vid, beat_id, source="yt"),
            )
        except Exception:
            logger.warning("repost: cta comment failed (non-fatal)")
        try:
            await loop.run_in_executor(
                None, lambda: _add_to_yt_playlists(vid, meta),
            )
        except Exception:
            logger.warning("repost: playlists failed (non-fatal)")

        # 7. Post в TG канал
        sent = await bot.send_audio(CHANNEL_ID, audio=file_id, caption=tg_caption)
        new_msg_id = sent.message_id

        # 8. Delete old TG post.
        # Bot API ограничен: бот может delete только свои сообщения и
        # только в пределах 48ч после публикации. Legacy-биты публиковались
        # давно → Bot API всегда возвращает 400 'Message can't be deleted'.
        # Fallback: Telethon (user-account API), который не имеет 48h лимита.
        if old_msg_id:
            deleted = False
            try:
                await bot.delete_message(CHANNEL_ID, old_msg_id)
                deleted = True
            except Exception as e:
                logger.info(
                    "repost: bot.delete failed (expected for legacy >48h): %s", e,
                )
            if not deleted:
                try:
                    await _telethon_delete(CHANNEL_ID, old_msg_id)
                    deleted = True
                except Exception as e:
                    logger.warning("repost: telethon delete %d failed: %s",
                                   old_msg_id, e)
            if not deleted:
                logger.warning(
                    "repost: НЕ удалил старый пост msg_id=%d — удали вручную",
                    old_msg_id,
                )

        # 9. Update beats_db
        try:
            for b in beats_db.BEATS_CACHE:
                if b.get("id") == beat_id:
                    b["msg_id"] = new_msg_id
                    b["post_url"] = f"https://t.me/{CHANNEL_ID.lstrip('@')}/{new_msg_id}"
                    b["last_reposted_at"] = datetime.now().isoformat(timespec="seconds")
                    b["last_posted_at"] = b["last_reposted_at"]
                    # Обновляем чистое имя в каталоге чтобы будущие посты были аккуратнее
                    b["name"] = meta.name
                    break
            beats_db.save_beats()
        except Exception:
            logger.exception("repost: beats_db update failed (non-fatal)")

        # 10. Final notification + Shorts button
        try:
            await status.delete()
        except Exception:
            pass
        # Создаём fake "scheduled_uploads" запись чтобы make_shorts callback мог
        # её найти. Делаем через publish_scheduler.save_yt_video_id с ad-hoc token.
        # Альтернатива: можно сразу автоматом запускать Shorts через 30s, но
        # для MVP оставим кнопку (юзер сам решает делать ли Shorts).
        # Для repost flow чтобы make_shorts работал — нужен row в БД.
        # Создаю минимальную запись.
        repost_token = uuid.uuid4().hex[:12]
        try:
            import publish_scheduler
            from dataclasses import asdict
            sb = publish_scheduler._get_supabase()
            if sb is not None:
                sb.table(publish_scheduler.SB_TABLE).insert({
                    "token": repost_token,
                    "publish_at": datetime.now(ZoneInfo("Europe/Moscow")).isoformat(),
                    "actions": ["yt", "tg"],
                    "meta": asdict(meta),
                    "yt_post": {**asdict(yt_post), "yt_video_id": vid},
                    "tg_caption": tg_caption,
                    "tg_style": "repost",
                    "tg_file_id": file_id,
                    "reserved_beat_id": beat_id,
                    "status": "published",
                    "enqueued_at": datetime.now(ZoneInfo("Europe/Moscow")).isoformat(),
                    "published_at": datetime.now(ZoneInfo("Europe/Moscow")).isoformat(),
                }).execute()
                # Upload файлы в Storage чтобы on-demand Shorts builder мог их скачать
                publish_scheduler._upload_files_to_storage(repost_token, {
                    "mp3": mp3_path, "video": video_path, "thumb": thumb_path,
                })
        except Exception:
            logger.exception("repost: failed to create scheduled_uploads row (Shorts unavailable)")
            repost_token = ""

        msg_text = (
            f"✅ Repost готов: {meta.name}\n"
            f"🎬 https://youtu.be/{vid}\n"
            f"📢 https://t.me/{CHANNEL_ID.lstrip('@')}/{new_msg_id}\n"
            f"{'🗑 Старый пост удалён' if old_msg_id else '(старого msg_id не было)'}"
        )
        keyboard = None
        if repost_token:
            msg_text += "\n\n🎬 <b>Сделать Shorts (YT + TikTok)?</b>"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🎬 Сделать Shorts (YT + TikTok)",
                                      callback_data=f"make_shorts:{repost_token}")],
                [InlineKeyboardButton("❌ Без Shorts",
                                      callback_data=f"skip_shorts:{repost_token}")],
            ])
        await bot.send_message(
            ADMIN_ID, msg_text,
            reply_markup=keyboard,
            parse_mode="HTML" if keyboard else None,
        )
        return repost_token or None
    except Exception as e:
        logger.exception("repost: flow failed for beat_id=%d", beat_id)
        try:
            await bot.send_message(
                reply_chat_id,
                f"❌ Repost провалился: {type(e).__name__}: {e}\nСм. логи Render."
            )
        except Exception:
            pass
        return None
    finally:
        # Cleanup local temp-файлы
        for p in (mp3_path, video_path, thumb_path):
            try:
                _P(p).unlink(missing_ok=True)
            except Exception:
                pass


def _pick_next_repost_candidate() -> dict | None:
    """Выбирает следующий бит для auto-repost по LRU policy.

    Filter: content_type=beat, file_id есть, beat_record_to_meta returns
    not None (есть BPM+key+артист), не репостился последние 30 дней.

    Sort key: (last_reposted_at OR last_posted_at OR null) ASC — это
    least-recently-posted идёт первым. Бит который **никогда** не
    репостился (last_reposted_at=None) приоритетнее всех — оба None
    sortируются в начало.

    Returns None если нет подходящих кандидатов.
    """
    import beat_upload as _bu
    from datetime import timedelta as _td
    cutoff = (datetime.now() - _td(days=30)).isoformat()
    candidates = []
    for b in beats_db.BEATS_CACHE:
        if b.get("content_type", "beat") != "beat":
            continue
        if not b.get("file_id"):
            continue
        meta = _bu.beat_record_to_meta(b)
        if meta is None:
            continue
        last_repost = b.get("last_reposted_at") or ""
        # Skip recently reposted (< 30 days ago)
        if last_repost and last_repost > cutoff:
            continue
        candidates.append((last_repost or b.get("last_posted_at") or "", b))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])  # LRU first
    return candidates[0][1]


async def remarketing_scheduler(bot):
    """Раз в час проходит pending_reminders. Если запись >24h и юзер ещё не
    куплен этот бит — шлёт ОДИН reminder с CTA + ref_remind tracking.
    Если >7 дней — silent drop (не агрессивно).

    Anti-spam защита:
    - Один reminder per (user, beat)
    - Skip юзеров в reminders_optout (через /stop_reminders)
    - Skip ADMIN_ID
    - Если bot.send_message упал с Forbidden (юзер blocked бот) — auto-optout
    """
    REMINDER_AFTER_SEC = 24 * 3600       # 24h after view
    DROP_AFTER_SEC = 7 * 24 * 3600        # 7d → drop без действия
    while True:
        try:
            await asyncio.sleep(60 * 60)  # каждый час
            now = time.time()
            to_drop = []
            to_remind = []
            for key, rec in list(pending_reminders.items()):
                age = now - rec.get("ts", now)
                if age > DROP_AFTER_SEC:
                    to_drop.append(key)
                    continue
                if rec.get("reminded"):
                    continue
                if age >= REMINDER_AFTER_SEC:
                    to_remind.append((key, rec))

            for key, rec in to_remind:
                try:
                    user_id_s, beat_id_s = key.split(":", 1)
                    user_id = int(user_id_s)
                    beat_id = int(beat_id_s)
                except Exception:
                    to_drop.append(key)
                    continue
                if user_id in reminders_optout:
                    to_drop.append(key)
                    continue
                if user_id == ADMIN_ID:
                    to_drop.append(key)
                    continue

                beat = beats_db.get_beat_by_id(beat_id)
                if not beat:
                    to_drop.append(key)
                    continue

                # Сообщение с deep-link `?start=ref_remind_buy_<id>` —
                # ref tracking покажет в /today сколько конверсий из reminders.
                buy_url = (
                    f"https://t.me/{beat_post_builder.BOT_USERNAME}"
                    f"?start=ref_remind_buy_{beat_id}"
                )
                bpm = beat.get("bpm") or "?"
                key_short = beat.get("key") or ""
                rec_name = rec.get("name") or beat.get("name") or "бит"
                text = (
                    f"🎧 Помнишь <b>«{html.escape(str(rec_name))}»</b>?\n"
                    f"⚡ {bpm} BPM · 🎹 {html.escape(str(key_short))}\n\n"
                    f"Бит ещё на месте — <b>1500⭐ / 20 USDT / 1700₽</b>\n"
                    f"Купи: {buy_url}\n\n"
                    f"<i>Не интересно? /stop_reminders — больше не напишу.</i>"
                )
                try:
                    await bot.send_message(user_id, text, parse_mode="HTML",
                                           disable_web_page_preview=True)
                    rec["reminded"] = True
                    logger.info("remarketing: sent reminder user=%d beat=%d",
                                user_id, beat_id)
                except Exception as e:
                    err_s = str(e).lower()
                    if "forbidden" in err_s or "blocked" in err_s or "deactivated" in err_s:
                        # Юзер blocked бот — auto-optout
                        reminders_optout.add(user_id)
                        _save_optout()
                        to_drop.append(key)
                        logger.info("remarketing: auto-optout user=%d (blocked)", user_id)
                    else:
                        logger.warning("remarketing: send failed user=%d: %s",
                                       user_id, e)

            for key in to_drop:
                pending_reminders.pop(key, None)
            if to_remind or to_drop:
                _save_reminders()
        except Exception:
            logger.exception("remarketing_scheduler iteration failed")


async def cmd_cart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/cart` — показ персональной корзины битов для bundle-покупки."""
    user_id = update.effective_user.id
    await _send_cart_view(context.bot, user_id, edit_message=None)


async def cmd_stop_reminders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/stop_reminders` — юзер отказывается от reminder-сообщений
    о просмотренных битах. Anti-spam, mandatory для TG ToS-friendly bot.
    """
    user_id = update.effective_user.id
    if user_id in reminders_optout:
        await update.message.reply_text(
            "Уже отписан. Если передумаешь — /start_reminders включит снова."
        )
        return
    reminders_optout.add(user_id)
    _save_optout()
    # Удалить все pending для этого юзера
    to_drop = [k for k in pending_reminders if k.startswith(f"{user_id}:")]
    for k in to_drop:
        pending_reminders.pop(k, None)
    if to_drop:
        _save_reminders()
    await update.message.reply_text(
        "🤐 Окей, больше не напомню про просмотренные биты.\n\n"
        "Включить обратно: /start_reminders"
    )


async def cmd_start_reminders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Откатывает /stop_reminders."""
    user_id = update.effective_user.id
    if user_id not in reminders_optout:
        await update.message.reply_text("Reminders уже включены.")
        return
    reminders_optout.discard(user_id)
    _save_optout()
    await update.message.reply_text(
        "✅ Reminders включены. Если посмотрел бит и не купил — через 24ч "
        "напомню (один раз)."
    )


async def auto_repost_scheduler(bot):
    """Раз в сутки в `AUTO_REPOST_TIME_MSK` выбирает next бит и запускает
    repost flow. После long publish (если AUTO_SHORTS=1) — автоматически
    через 30 сек запускает Shorts builder.

    Управление через `/auto_repost on|off|status`. Состояние хранится в
    admin_prefs.json (`auto_repost_enabled` ключ) — переключение мгновенно
    без redeploy. По умолчанию **ВЫКЛЮЧЕНО** — включай когда готов.
    """
    from config import AUTO_REPOST_TIME_MSK, AUTO_SHORTS
    msk = ZoneInfo("Europe/Moscow")
    while True:
        try:
            now = datetime.now(msk)
            target_h, target_m = map(int, AUTO_REPOST_TIME_MSK.split(":"))
            target = now.replace(hour=target_h, minute=target_m,
                                 second=0, microsecond=0)
            if target <= now:
                target = target + timedelta(days=1)
            wait_sec = max(60, (target - now).total_seconds())
            logger.info(
                "auto_repost: next run at %s МСК (wait %ds)",
                target.strftime("%Y-%m-%d %H:%M"), int(wait_sec),
            )
            await asyncio.sleep(wait_sec)
        except Exception:
            logger.exception("auto_repost: sleep iteration failed")
            await asyncio.sleep(60 * 60)
            continue

        # Check enable flag (file-based, мгновенное переключение через /auto_repost)
        try:
            prefs = _load_admin_prefs()
            if not prefs.get("auto_repost_enabled", False):
                logger.info("auto_repost: disabled in admin_prefs, skip iteration")
                continue
        except Exception:
            logger.exception("auto_repost: load_prefs failed")
            continue

        # Pick next bit
        try:
            beat = _pick_next_repost_candidate()
        except Exception:
            logger.exception("auto_repost: pick_candidate failed")
            continue
        if beat is None:
            logger.warning(
                "auto_repost: no candidates available (need bpm+key+артист в tags); "
                "запусти /quick_meta чтобы дополнить metadata legacy битов"
            )
            try:
                await bot.send_message(
                    ADMIN_ID,
                    "⚠️ auto_repost: нет битов готовых к repost.\n\n"
                    "Все ready-биты репостились < 30 дней назад, или в каталоге "
                    "только legacy-биты без BPM/key. Дополни metadata через "
                    "/quick_meta или подожди пока 30-day cooldown пройдёт.",
                )
            except Exception:
                pass
            continue

        import beat_upload as _bu
        meta = _bu.beat_record_to_meta(beat)
        if meta is None:
            logger.warning("auto_repost: candidate %d no longer valid, skip", beat.get("id"))
            continue

        logger.info("auto_repost: starting %s (id=%d)", meta.name, beat.get("id"))
        repost_token = await _do_repost(bot, ADMIN_ID, beat, meta)
        if not repost_token:
            logger.warning("auto_repost: _do_repost returned None for %d", beat.get("id"))
            continue

        # Auto-Shorts через 30 сек (RAM Render free должен освободиться от
        # long upload), если флаг включён. Запускаем как отдельную task'у
        # чтобы не блокировать scheduler loop.
        if AUTO_SHORTS:
            async def _auto_shorts_after_delay(t: str):
                try:
                    await asyncio.sleep(30)
                    if t in _building_shorts:
                        logger.info("auto_shorts: already in flight for %s, skip", t)
                        return
                    _building_shorts.add(t)
                    try:
                        await _build_and_upload_shorts(bot, t)
                    finally:
                        _building_shorts.discard(t)
                except Exception:
                    logger.exception("auto_shorts: failed for %s", t)
            asyncio.create_task(_auto_shorts_after_delay(repost_token))
            logger.info("auto_repost: Shorts scheduled in 30s for %s", repost_token)


async def cmd_auto_repost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/auto_repost on|off|status` — управление авто-репостом legacy битов.

    Состояние в admin_prefs.json — изменение мгновенное без redeploy.
    Auto-loop работает раз в сутки в AUTO_REPOST_TIME_MSK (env, default 21:00).
    """
    if update.effective_user.id != ADMIN_ID:
        return
    args = (context.args or [])
    cmd = args[0].lower() if args else "status"
    prefs = _load_admin_prefs()

    if cmd == "on":
        prefs["auto_repost_enabled"] = True
        _save_admin_prefs(prefs)
        from config import AUTO_REPOST_TIME_MSK
        # Сразу показать сколько кандидатов в очереди
        try:
            count_ready = 0
            import beat_upload as _bu
            for b in beats_db.BEATS_CACHE:
                if b.get("content_type", "beat") == "beat" and b.get("file_id"):
                    if _bu.beat_record_to_meta(b) is not None:
                        count_ready += 1
        except Exception:
            count_ready = 0
        await update.message.reply_text(
            f"✅ Auto-repost ВКЛЮЧЁН\n\n"
            f"⏰ Время: {AUTO_REPOST_TIME_MSK} МСК ежедневно\n"
            f"📚 Готовых битов: {count_ready}\n"
            f"🎬 Auto-Shorts: ON (через 30 сек после long)\n\n"
            f"Управление: /auto_repost off · /auto_repost status"
        )
    elif cmd == "off":
        prefs["auto_repost_enabled"] = False
        _save_admin_prefs(prefs)
        await update.message.reply_text("⛔ Auto-repost ВЫКЛЮЧЕН")
    else:  # status
        from config import AUTO_REPOST_TIME_MSK, AUTO_SHORTS
        enabled = prefs.get("auto_repost_enabled", False)
        # Подсчитать кандидатов
        count_ready = 0
        count_total_beats = 0
        try:
            import beat_upload as _bu
            for b in beats_db.BEATS_CACHE:
                if b.get("content_type", "beat") == "beat":
                    count_total_beats += 1
                    if b.get("file_id") and _bu.beat_record_to_meta(b) is not None:
                        count_ready += 1
        except Exception:
            pass
        await update.message.reply_text(
            f"📋 Auto-repost статус\n\n"
            f"{'✅ ВКЛ' if enabled else '⛔ ВЫКЛ'}\n"
            f"⏰ {AUTO_REPOST_TIME_MSK} МСК ежедневно\n"
            f"📚 Готовых: {count_ready} из {count_total_beats}\n"
            f"🎬 Auto-Shorts: {'ON' if AUTO_SHORTS else 'OFF'}\n\n"
            f"Команды: /auto_repost on · /auto_repost off"
        )


YT_SNAPSHOT_PATH = os.path.join(BASE_DIR, "yt_daily_snapshot.json")


def _load_yt_snapshot() -> dict:
    """JSON: { 'YYYY-MM-DD': {'subs': N, 'views': N, 'videos': N} } —
    нужно чтобы в /today показывать delta (что сегодня изменилось)."""
    if not os.path.exists(YT_SNAPSHOT_PATH):
        return {}
    try:
        with open(YT_SNAPSHOT_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_yt_snapshot(snap: dict) -> None:
    try:
        with open(YT_SNAPSHOT_PATH, "w", encoding="utf-8") as f:
            json.dump(snap, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.warning("yt_snapshot save failed")


async def _fetch_yt_today_block(today_iso: str, recent_video_ids: list[str]) -> str:
    """Возвращает форматированный block для /today с YT-метриками.

    1. Channel stats (subs/views/videos) + delta vs вчера через snapshot
    2. Top recent uploads с views (для битов залитых сегодня)

    Падение YT API → возвращаем "" (silent skip — не ломаем /today).
    """
    try:
        import yt_api
    except Exception:
        return ""
    loop = asyncio.get_running_loop()
    try:
        stats = await loop.run_in_executor(None, yt_api.get_channel_stats)
    except Exception:
        logger.warning("yt analytics: get_channel_stats failed")
        return ""
    snap = _load_yt_snapshot()
    # Yesterday key — last entry not equal to today (избегаем сравнения с самим собой)
    prev_keys = sorted(k for k in snap.keys() if k != today_iso)
    prev = snap.get(prev_keys[-1]) if prev_keys else None

    def _delta(curr: int, key: str) -> str:
        if not prev or key not in prev:
            return ""
        d = curr - prev[key]
        if d > 0:
            return f" (+{d})"
        if d < 0:
            return f" ({d})"
        return ""

    lines = ["", f"📺 YT-канал «{stats['title']}»:"]
    lines.append(f"   👥 Subscribers: <b>{stats['subs']:,}</b>" + _delta(stats['subs'], 'subs'))
    lines.append(f"   👁 Total views: <b>{stats['views']:,}</b>" + _delta(stats['views'], 'views'))
    lines.append(f"   🎬 Videos: <b>{stats['videos']}</b>" + _delta(stats['videos'], 'videos'))

    # Save today's snapshot (overwrite — каждый /today обновляет current day)
    snap[today_iso] = {"subs": stats["subs"], "views": stats["views"],
                       "videos": stats["videos"]}
    # Cleanup старых snapshots (>30 дней — не нужны для daily delta)
    if len(snap) > 30:
        for old_key in sorted(snap.keys())[:-30]:
            snap.pop(old_key, None)
    _save_yt_snapshot(snap)

    # Per-video stats для сегодняшних upload'ов
    if recent_video_ids:
        video_lines = []
        for vid in recent_video_ids[:5]:
            try:
                v = await loop.run_in_executor(None, lambda: yt_api.get_video(vid))
            except Exception:
                continue
            if not v:
                continue
            stats_v = v.get("statistics", {})
            title = (v.get("snippet", {}).get("title") or "")[:40]
            views = stats_v.get("viewCount", "0")
            likes = stats_v.get("likeCount", "0")
            comments = stats_v.get("commentCount", "0")
            video_lines.append(
                f"   • {title} · 👁 {views} 👍 {likes} 💬 {comments}"
            )
        if video_lines:
            lines.append("")
            lines.append(f"🔥 Сегодняшние upload'ы:")
            lines.extend(video_lines)

    return "\n".join(lines)


async def cmd_yt_audit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/yt_audit` — анализ всех видео канала: топ-5 / анти-топ-5 + рекомендации.

    Скан channel uploads playlist через yt_api.list_channel_videos
    (paginated), batch-stats через get_videos_stats, sort by views.

    Что показывает:
    - Top-5 by views — что работает, удваиваем подход (тэги/тайтл/жанр)
    - Anti-top-5 by views — что НЕ работает, candidates для refresh/delete
    - Total channel stats
    - Average views, median (определить «pulse» канала)
    """
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text("📊 Сканирую канал... (~10-30 сек)")
    loop = asyncio.get_running_loop()
    try:
        import yt_api
        videos = await loop.run_in_executor(None, lambda: yt_api.list_channel_videos(max_results=200))
        if not videos:
            await update.message.reply_text("⚠️ Видео не найдены на канале")
            return
        ids = [v["video_id"] for v in videos if v.get("video_id")]
        stats = await loop.run_in_executor(None, lambda: yt_api.get_videos_stats(ids))
        # Merge stats в videos list
        for v in videos:
            v["stats"] = stats.get(v.get("video_id"), {"views": 0, "likes": 0, "comments": 0})
    except Exception as e:
        logger.exception("yt_audit failed")
        await update.message.reply_text(f"❌ Ошибка YT API: {e}")
        return

    total = len(videos)
    by_views = sorted(videos, key=lambda v: -v["stats"]["views"])
    total_views = sum(v["stats"]["views"] for v in videos)
    total_likes = sum(v["stats"]["likes"] for v in videos)
    total_comments = sum(v["stats"]["comments"] for v in videos)
    avg_views = total_views / max(1, total)
    median_views = sorted([v["stats"]["views"] for v in videos])[total // 2] if total else 0
    zero_views = sum(1 for v in videos if v["stats"]["views"] == 0)

    lines = [
        f"📊 <b>YT Audit</b> ({total} видео)\n",
        f"👁 Total views: <b>{total_views:,}</b>",
        f"👍 Total likes:  <b>{total_likes:,}</b>",
        f"💬 Total comments: <b>{total_comments:,}</b>",
        f"📈 Avg views: <b>{avg_views:.0f}</b>",
        f"🎯 Median views: <b>{median_views}</b>",
        f"⚠️ Видео с 0 views: <b>{zero_views}</b>",
        "",
        "🔥 <b>TOP-5 by views</b>:",
    ]
    for v in by_views[:5]:
        title = html.escape(v["title"][:55])
        s = v["stats"]
        lines.append(
            f"   • {title}\n"
            f"     👁 {s['views']:,} 👍 {s['likes']} 💬 {s['comments']} → "
            f"<a href='https://youtu.be/{v['video_id']}'>open</a>"
        )
    lines.append("")
    lines.append("💀 <b>ANTI-TOP-5 (реliase candidates)</b>:")
    for v in by_views[-5:]:
        title = html.escape(v["title"][:55])
        s = v["stats"]
        lines.append(
            f"   • {title}\n"
            f"     👁 {s['views']} 👍 {s['likes']} → "
            f"<a href='https://youtu.be/{v['video_id']}'>open</a>"
        )
    lines.append("")
    lines.append("💡 <b>Рекомендации</b>:")
    if zero_views >= 3:
        lines.append(f"   • {zero_views} видео с 0 views → /yt_refresh_old &lt;id&gt; для SEO refresh")
    if avg_views < 50 and total > 5:
        lines.append("   • Avg views &lt;50: видео не залетают. Проверь thumbnails — самый сильный driver CTR.")
    if total_comments == 0 and total > 5:
        lines.append("   • Нет комментариев — комментируй сам в первые 5 мин после publish (boosts engagement signal)")
    if avg_views > 100:
        lines.append(f"   • Avg views ${avg_views:.0f} — здоровая база. Удваивай тип контента из TOP-5.")

    await update.message.reply_text(
        "\n".join(lines), parse_mode="HTML",
        disable_web_page_preview=True,
    )


async def cmd_yt_refresh_old(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/yt_refresh_old <video_id>` — обновить description+tags старого видео:
    подтянуть актуальные цены (1500/20/1700) + актуальные ссылки на TG канал
    + ref-tracking (?start=ref_yt_buy_<beat_id>).

    Используется для legacy-видео которые публиковались до Этапа B (когда
    цены были 500/7 и не было ссылки на TG канал).
    """
    if update.effective_user.id != ADMIN_ID:
        return
    args = context.args or []
    if not args:
        await update.message.reply_text(
            "Usage: /yt_refresh_old <video_id>\n\n"
            "Подтянет актуальные цены + TG channel link в description."
        )
        return
    video_id = args[0].strip()
    await update.message.reply_text(f"🔄 Обновляю {video_id}...")
    loop = asyncio.get_running_loop()
    try:
        import yt_api
        v = await loop.run_in_executor(None, lambda: yt_api.get_video(video_id))
        if not v:
            await update.message.reply_text(f"❌ Видео {video_id} не найдено")
            return
        sn = v["snippet"]
        old_title = sn.get("title", "")
        old_desc = sn.get("description", "")
        old_tags = sn.get("tags", [])

        # Регенерим description через простой replace актуальных констант.
        # Не пытаемся LLM-переписать — сохраняем оригинальный flow.
        import beat_post_builder as _bpb
        new_desc = old_desc
        # Replace старых цен
        new_desc = new_desc.replace("500⭐ / 7 USDT", "1500⭐ / 20 USDT / 1700₽")
        new_desc = new_desc.replace("(500 / 7 USDT)", "(1500⭐ / 20 USDT)")
        new_desc = new_desc.replace("500 / 7 USDT", "1500⭐ / 20 USDT / 1700₽")
        # Add TG channel link sверху если ещё не было
        if "t.me/iiiplfiii" not in new_desc:
            tg_line = f"🎁 FREE sample pack + 165+ beats → {_bpb.TG_CHANNEL_URL}\n"
            # Вставить после первой строки (обычно hashtag-line)
            lines_split = new_desc.split("\n", 1)
            if len(lines_split) == 2:
                new_desc = lines_split[0] + "\n" + tg_line + lines_split[1]
            else:
                new_desc = tg_line + new_desc

        await loop.run_in_executor(
            None, lambda: yt_api.update_video(video_id, old_title, new_desc, old_tags),
        )
        await update.message.reply_text(
            f"✅ {video_id} обновлён\n"
            f"https://youtu.be/{video_id}\n\n"
            f"Изменения: цены 500/7 → 1500/20/1700 + TG channel link"
        )
    except Exception as e:
        logger.exception("yt_refresh_old failed for %s", video_id)
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сводка дня для админа: новые юзеры (с разбивкой по source), продажи,
    залитые биты + YT analytics (subscribers/views delta vs вчера + topvideos).
    Выборка из Supabase за ts >= сегодня 00:00 МСК."""
    if update.effective_user.id != ADMIN_ID:
        return

    import httpx
    from collections import Counter

    sb_url = os.getenv("SUPABASE_URL", "").strip()
    sb_key = os.getenv("SUPABASE_KEY", "").strip()
    if not sb_url or not sb_key:
        await update.message.reply_text("❌ Supabase env не задан")
        return
    headers = {"apikey": sb_key, "Authorization": f"Bearer {sb_key}"}

    msk = ZoneInfo("Europe/Moscow")
    today = datetime.now(msk).date()
    since = datetime.combine(today, datetime.min.time()).replace(tzinfo=msk).isoformat()

    users, sales_rows, events = [], [], []
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r1 = await client.get(
                f"{sb_url}/rest/v1/bot_users",
                params={"select": "tg_id,source,joined_at", "joined_at": f"gte.{since}"},
                headers=headers,
            )
            if r1.status_code == 200:
                users = r1.json()
            r2 = await client.get(
                f"{sb_url}/rest/v1/sales",
                params={
                    "select": "beat_name,stars_amount,fiat_amount_minor,currency,license_type",
                    "ts": f"gte.{since}",
                    "status": "eq.completed",
                },
                headers=headers,
            )
            if r2.status_code == 200:
                sales_rows = r2.json()
            r3 = await client.get(
                f"{sb_url}/rest/v1/post_events",
                params={
                    "select": "beat_name,bpm,kind",
                    "kind": "in.(upload,scheduled_upload)",
                    "ts": f"gte.{since}",
                },
                headers=headers,
            )
            if r3.status_code == 200:
                events = r3.json()
    except Exception as e:
        logger.exception("cmd_today: Supabase fetch failed")
        await update.message.reply_text(f"❌ Ошибка fetch: {e}")
        return

    # Breakdown
    source_c = Counter((u.get("source") or "direct") for u in users)
    # USD-эквивалент по currency: Stars/75 ≈ USD (1500⭐ ≈ $20), RUB/8500 ≈ USD
    # (копейки → рубли → USD при курсе 85), USDT/USD 1:1
    usd_est = 0.0
    for s in sales_rows:
        cur = s.get("currency") or ""
        if cur == "XTR":
            usd_est += (s.get("stars_amount") or 0) / 75.0
        elif cur == "RUB":
            usd_est += (s.get("fiat_amount_minor") or 0) / 8500.0
        else:  # USDT или неизвестная — ориентируемся на $20 default
            usd_est += 20.0

    lines = [f"📊 Сегодня · {today.strftime('%d %b %Y')} (МСК):", ""]
    lines.append(f"👥 Новых юзеров: <b>{len(users)}</b>")
    if users:
        src_parts = [f"{k}={v}" for k, v in source_c.most_common()]
        lines.append(f"   {' · '.join(src_parts)}")
    lines.append("")
    lines.append(f"💰 Продажи: <b>{len(sales_rows)}</b>" + (f" (~${usd_est:.0f})" if sales_rows else ""))
    for s in sales_rows[:5]:
        nm = (s.get("beat_name") or "?")[:35]
        cur = s.get("currency") or "?"
        if cur == "XTR":
            price_s = f"{s.get('stars_amount', 0) or 0}⭐"
        elif cur == "RUB":
            price_s = f"{(s.get('fiat_amount_minor') or 0) / 100:g}₽"
        else:
            amt = s.get("stars_amount") or s.get("fiat_amount_minor", 0) or 0
            price_s = f"{amt} {cur}"
        lt = s.get("license_type") or "?"
        lines.append(f"   • {nm} · {price_s} · {lt}")
    lines.append("")
    lines.append(f"🎵 Битов залили: <b>{len(events)}</b>")
    for e in events[:5]:
        nm = (e.get("beat_name") or "?")[:35]
        bpm = e.get("bpm", "?")
        lines.append(f"   • {nm} · {bpm} BPM")

    # YT analytics block — channel stats with delta vs yesterday +
    # per-video views для сегодняшних upload'ов. Silent skip если YT API
    # недоступен (не ломаем /today).
    today_iso = today.strftime("%Y-%m-%d")
    # Recent uploads из Supabase events — используем post_events.yt_video_id
    recent_vids = []
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r4 = await client.get(
                f"{sb_url}/rest/v1/post_events",
                params={
                    "select": "yt_video_id",
                    "kind": "in.(upload,scheduled_upload)",
                    "ts": f"gte.{since}",
                },
                headers=headers,
            )
            if r4.status_code == 200:
                recent_vids = [
                    e["yt_video_id"] for e in r4.json()
                    if e.get("yt_video_id")
                ]
    except Exception:
        pass
    yt_block = await _fetch_yt_today_block(today_iso, recent_vids)
    if yt_block:
        lines.append(yt_block)

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


# ── Запуск ────────────────────────────────────────────────────

async def heartbeat_scheduler():
    while True:
        write_heartbeat()
        await asyncio.sleep(30)


async def post_init(application):
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
        logger.info("post_init: webhook cleared, pending updates dropped")
    except Exception as e:
        logger.warning("post_init: delete_webhook failed: %s", e)
    beats_db.load_beats()
    if not beats_db.BEATS_CACHE and os.path.exists(beats_db.BEATS_FILE):
        # Файл есть, кэш пуст → значит парсинг упал. Ждём 2 сек (вдруг
        # файл ещё пишется после рестарта) и пробуем ещё раз. Без размер-
        # условия: даже битые 500 байт заслуживают повторной попытки —
        # если всё ещё битые, load_beats() молча оставит пусто.
        logger.warning("post_init: cache empty but file exists — retrying load_beats after 2s")
        await asyncio.sleep(2)
        beats_db.load_beats()
    load_users()
    logger.info("Bot started: " + str(len(beats_db.BEATS_CACHE)) + " beats, " + str(len(all_users)) + " users")
    # Восстанавливаем очередь плановых публикаций.
    # КРИТИЧНО: load_queue() это СИНХРОННЫЙ call → SDK Supabase + storage
    # downloads. Без wait_for'а может заблокировать весь post_init
    # (Render network quirks) → scheduled_publish_loop никогда не стартует.
    # Timeout 30с + to_thread + graceful fallback: даже если load_queue
    # помрёт, scheduler запустится и safety-net reload в его цикле
    # заполнит _QUEUE на следующей минуте.
    try:
        import publish_scheduler
        n = await asyncio.wait_for(
            asyncio.to_thread(publish_scheduler.load_queue),
            timeout=30.0,
        )
        logger.info("publish_scheduler: restored %d queued items on startup", n)
    except asyncio.TimeoutError:
        logger.warning("publish_scheduler: load_queue timed out (30s) — scheduler will self-heal")
    except Exception:
        logger.exception("publish_scheduler restore failed (non-fatal)")
    # Восстанавливаем pending_products — недосохранённые продукты на preview-шаге.
    try:
        restored = _restore_pending_products()
        if restored:
            logger.info("pending_products: restored %d items on startup", restored)
    except Exception:
        logger.exception("pending_products restore failed (non-fatal)")
    # Восстанавливаем YooKassa pending payments — те что ждут webhook.
    # Без этого после redeploy webhook приходит на payment_id которого нет
    # в памяти → delivery fallback на metadata (рабочий, но лишняя ветка).
    try:
        n_yk = _restore_yk_pending()
        if n_yk:
            logger.info("yk: restored %d pending payments on startup", n_yk)
    except Exception:
        logger.exception("yk: restore pending_yk_payments failed (non-fatal)")
    # Восстанавливаем delivered-set — ОСНОВНОЙ idempotency guard.
    # Без этого после redeploy webhook retry заставит нас доставить товар
    # второй раз (sales duplicate + user получит mp3 дважды). Critical.
    try:
        n_delivered = _restore_yk_delivered()
        if n_delivered:
            logger.info("yk: restored %d delivered payment_ids on startup", n_delivered)
    except Exception:
        logger.exception("yk: restore delivered_yk_payments failed (non-fatal)")
    # Передаём loop + bot в HTTPServer thread — webhook'у нужно schedule'ить
    # _deliver_yk_payment обратно на main loop через run_coroutine_threadsafe.
    global _WEBHOOK_BOT, _WEBHOOK_LOOP
    _WEBHOOK_BOT = application.bot
    _WEBHOOK_LOOP = asyncio.get_running_loop()
    # Re-marketing state — load from disk перед запуском loop'а
    try:
        _load_reminders_state()
        logger.info(
            "remarketing: loaded %d pending, %d opted-out",
            len(pending_reminders), len(reminders_optout),
        )
    except Exception:
        logger.exception("remarketing load failed")
    # Bundle carts — load from disk (persist через git autopush)
    try:
        _load_bundle_carts()
        logger.info("bundle_cart: loaded %d users with non-empty carts", len(bundle_cart))
    except Exception:
        logger.exception("bundle_cart load failed")
    asyncio.create_task(scheduled_publish_loop(application.bot))
    asyncio.create_task(heartbeat_scheduler())
    asyncio.create_task(content_reminder_scheduler(application.bot))
    asyncio.create_task(yk_fallback_polling(application.bot))
    asyncio.create_task(auto_repost_scheduler(application.bot))
    asyncio.create_task(remarketing_scheduler(application.bot))
    # Git autopush для beats_data.json + admin_prefs.json — Render free disk
    # эфемерный, без push'а файлы слетают на каждый redeploy. Loop debounced 60с.
    try:
        import git_autopush
        asyncio.create_task(git_autopush.autopush_loop())
    except Exception:
        logger.exception("git_autopush loop start failed (non-fatal)")
    asyncio.create_task(asyncio.to_thread(_warmup_ffmpeg))
    write_heartbeat()


async def scheduled_publish_loop(bot):
    """Каждые 60с проверяет очередь publish_scheduler и публикует due item'ы.

    Self-healing: если `_QUEUE` пустой — пробуем reload из Supabase.
    Закрывает случай когда post_init.load_queue упал / timeout'нулся
    или бот рестартился — scheduler сам восстановит очередь через минуту.

    Tick-log каждые 5 минут показывает что loop жив и что у него в
    памяти — без этого log'а мы слепы к состоянию scheduler'а.
    """
    import publish_scheduler
    iteration = 0
    while True:
        iteration += 1
        try:
            # Self-heal: пустой _QUEUE → reload из Supabase
            if not publish_scheduler._QUEUE:
                try:
                    n = await asyncio.wait_for(
                        asyncio.to_thread(publish_scheduler.load_queue),
                        timeout=30.0,
                    )
                    if n > 0:
                        logger.info("scheduled_publish_loop: reloaded %d items (was empty)", n)
                except asyncio.TimeoutError:
                    logger.warning("scheduled_publish_loop: reload timed out, skipping iteration")
                except Exception:
                    logger.exception("scheduled_publish_loop: auto-reload failed")

            due = publish_scheduler.due_items()
            qsize = publish_scheduler.queue_size()
            # Tick-log каждые 5 минут: видимость в logs не спамя
            if iteration == 1 or iteration % 5 == 0:
                logger.info("scheduled_publish_loop: tick #%d, _QUEUE=%d due=%d",
                            iteration, qsize, len(due))

            for item in due:
                try:
                    await _execute_scheduled_publish(bot, item)
                    # keep_files=True — оставляем mp3/mp4/thumb в Supabase Storage
                    # на случай если админ нажмёт «🎬 Сделать Shorts». Cleanup
                    # делается в callback `make_shorts` / `skip_shorts`.
                    publish_scheduler.mark_published(item["token"], keep_files=True)
                    # Чистим только LOCAL temp-файлы (RAM-disk Render). В Supabase
                    # Storage остаются 3 файла привязанные к token, доступные
                    # для on-demand Shorts builder.
                    for key in ("mp3_path", "video_path", "thumb_path"):
                        p = item.get(key)
                        if p:
                            try:
                                os.remove(p)
                            except Exception:
                                pass
                except Exception:
                    logger.exception("scheduled publish failed for token=%s", item.get("token"))
                    # Не удаляем из очереди — повторим через минуту
        except Exception:
            logger.exception("scheduled_publish_loop iteration failed")
        await asyncio.sleep(60)


async def _execute_scheduled_publish(bot, item: dict):
    """Публикует один scheduled item на YT + TG (или одно из)."""
    import yt_api, post_analytics
    from pathlib import Path as _P
    actions = item.get("actions", ["yt", "tg"])
    meta_d = item["meta"]
    yt_post_d = item["yt_post"]
    tg_caption = item["tg_caption"]
    video_path = _P(item["video_path"])
    thumb_path = _P(item["thumb_path"])
    reserved_beat_id = item.get("reserved_beat_id")
    tg_file_id = item["tg_file_id"]

    yt_video_id = None
    yt_ok = None
    tg_ok = None
    tg_message_id = None

    if "yt" in actions and video_path.exists():
        try:
            loop = asyncio.get_running_loop()
            vid = await loop.run_in_executor(
                None,
                lambda: yt_api.upload_video(
                    video_path, yt_post_d["title"], yt_post_d["description"],
                    yt_post_d["tags"], thumb_path,
                ),
            )
            yt_video_id = vid
            yt_ok = True
            # Сохраняем yt_video_id в Supabase сразу — нужно для on-demand
            # Shorts builder (он читает yt_post.yt_video_id из БД когда
            # юзер кликает «🎬 Сделать Shorts»). Без этого save make_shorts
            # упадёт с «long YT video не найден».
            try:
                import publish_scheduler
                await asyncio.to_thread(
                    publish_scheduler.save_yt_video_id, item["token"], vid,
                )
            except Exception:
                logger.exception("scheduled: save_yt_video_id failed (non-fatal)")
            # Playlists (artist + scene) + auto-CTA comment
            try:
                from beat_upload import BeatMeta
                meta_obj = BeatMeta(**meta_d)
                await loop.run_in_executor(None, lambda: _add_to_yt_playlists(vid, meta_obj))
            except Exception:
                logger.exception("scheduled: playlists add failed (non-fatal)")
            try:
                await loop.run_in_executor(
                    None, lambda: _post_cta_comment(vid, reserved_beat_id)
                )
            except Exception:
                logger.exception("scheduled: CTA comment failed (non-fatal)")
            # Добавляем в каталог с reserved_beat_id
            try:
                new_id = reserved_beat_id or (max([b["id"] for b in beats_db.BEATS_CACHE] + [0]) + 1)
                beats_db.BEATS_CACHE.append({
                    "id": new_id, "msg_id": 0, "name": yt_post_d["title"],
                    "tags": yt_post_d["tags"], "post_url": f"https://youtu.be/{vid}",
                    "bpm": meta_d.get("bpm"), "key": meta_d.get("key"),
                    "file_id": tg_file_id, "content_type": "beat",
                    "file_unique_id": "", "classification_confidence": 1.0,
                    "yt_video_id": vid,
                })
                beats_db._rebuild_index()
                beats_db.save_beats()
            except Exception:
                logger.exception("scheduled: beats_db append failed (non-fatal)")

            # ── Shorts: НЕ строится автоматом ───────────────────────
            # Раньше тут билдился 9:16 mp4 + uploaded YT Shorts + TikTok send.
            # Это был причиной OOM на Render free tier (512MB RAM): scheduler
            # параллельно держал mp3+mp4+thumb downloads + ffmpeg Shorts encode +
            # YT upload одновременно → kill процесса.
            # Теперь Shorts билдится **по запросу** — после long publish бот
            # шлёт админу notification с кнопками 🎬 Shorts / ❌ Без. RAM
            # свободна когда админ тапает кнопку (нет других heavy операций).
            # См. callback `make_shorts:<token>` в handle_callback.
        except Exception:
            logger.exception("scheduled: YT upload failed")
            yt_ok = False

    if "tg" in actions:
        try:
            sent = await bot.send_audio(CHANNEL_ID, audio=tg_file_id, caption=tg_caption)
            tg_message_id = sent.message_id
            tg_ok = True
            try:
                for b in beats_db.BEATS_CACHE:
                    if b.get("file_id") == tg_file_id:
                        b["last_posted_at"] = datetime.now().isoformat(timespec="seconds")
                beats_db.save_beats()
            except Exception:
                logger.exception("scheduled: mark last_posted_at failed (non-fatal)")
        except Exception:
            logger.exception("scheduled: TG send failed")
            tg_ok = False

    # Лог публикации
    if yt_ok or tg_ok:
        try:
            post_analytics.log_event(
                kind="scheduled_upload",
                beat_name=meta_d.get("name", "?"),
                artist=meta_d.get("artist_display", "?"),
                bpm=meta_d.get("bpm"),
                key=meta_d.get("key", ""),
                style=item.get("tg_style", "scheduled"),
                caption=tg_caption,
                yt_video_id=yt_video_id,
                tg_message_id=tg_message_id,
                yt_title=yt_post_d["title"],
            )
        except Exception:
            logger.exception("scheduled: post_analytics failed (non-fatal)")

    # Уведомление админу + предложение собрать Shorts on-demand.
    # Кнопка `make_shorts:<token>` запускает heavy ffmpeg+upload вне
    # scheduler tick'а — RAM свободна, OOM не случается.
    # Кнопка `skip_shorts:<token>` чистит файлы из Storage (не нужны).
    try:
        parts = []
        if "yt" in actions:
            parts.append(f"YT: {'✅ https://youtu.be/' + yt_video_id if yt_ok else '❌'}")
        if "tg" in actions:
            parts.append(f"TG: {'✅ msg_id=' + str(tg_message_id) if tg_ok else '❌'}")
        msg_text = (
            f"📅 Плановая публикация отработала — {meta_d.get('name','?')} — {meta_d.get('artist_display','?')}\n"
            + "\n".join(parts)
        )
        keyboard = None
        if yt_ok and yt_video_id:
            msg_text += "\n\n🎬 <b>Сделать Shorts (YT + TikTok)?</b>"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "🎬 Сделать Shorts (YT + TikTok)",
                    callback_data=f"make_shorts:{item['token']}",
                )],
                [InlineKeyboardButton(
                    "❌ Без Shorts",
                    callback_data=f"skip_shorts:{item['token']}",
                )],
            ])
        await bot.send_message(
            ADMIN_ID, msg_text,
            reply_markup=keyboard,
            parse_mode="HTML" if keyboard else None,
        )
    except Exception:
        logger.exception("scheduled: admin notify failed")


async def _build_and_upload_shorts(bot, token: str) -> None:
    """On-demand Shorts builder. Вызывается из callback `make_shorts:<token>`
    после клика админа. Шаги:

    1. SELECT scheduled_uploads → достать meta + yt_video_id (long).
    2. Скачать mp3+thumb из Supabase Storage (если файлы ещё там).
    3. ffmpeg build_short → 9:16 1080×1920 mp4, offset 30s, длина 30s.
    4. YT Shorts upload через yt_api.upload_video.
    5. Send mp4 + TikTok-caption админу в ЛС (semi-auto TikTok).
    6. Cleanup: local short_*.mp4 + Supabase Storage files (token больше
       не нужен, long+Shorts опубликованы).

    Heavy операция ~30-60 сек. Mutex `_building_shorts` снаружи защищает
    от concurrent call'ов с тем же token.
    """
    import shorts_builder
    import yt_api
    import publish_scheduler
    from beat_upload import BeatMeta as _BM
    from pathlib import Path as _P

    item = await asyncio.to_thread(publish_scheduler.fetch_item, token)
    if not item:
        await bot.send_message(
            ADMIN_ID,
            f"❌ Shorts: запись `{token}` не найдена в Supabase. Файлы возможно "
            f"уже почищены (TTL).",
        )
        return

    meta_d = item.get("meta") or {}
    yt_post_d = item.get("yt_post") or {}
    yt_long_id = yt_post_d.get("yt_video_id") or ""
    if not yt_long_id:
        await bot.send_message(
            ADMIN_ID,
            f"❌ Shorts: long YT video для `{token}` не найден — Shorts без "
            f"ссылки на full неэффективен. Отменяю.",
        )
        return

    reserved_beat_id = item.get("reserved_beat_id")
    beat_name = meta_d.get("name", "?")
    await bot.send_message(
        ADMIN_ID,
        f"🎬 Собираю Shorts для «{beat_name}»...\n⏱ ~30-60 сек на ffmpeg + upload.",
    )

    # 1) Файлы из Storage
    files = await asyncio.to_thread(publish_scheduler.download_files, token)
    mp3_path = files.get("mp3")
    thumb_path = files.get("thumb")
    if not mp3_path or not thumb_path or not mp3_path.exists() or not thumb_path.exists():
        await bot.send_message(
            ADMIN_ID,
            f"❌ Shorts: mp3 или thumb для `{token}` недоступны в Storage. "
            f"Файлы устарели (TTL) — собрать Shorts уже нельзя.",
        )
        return

    # 2) Build short via ffmpeg (executor — не блокируем event loop)
    short_path = _P(str(mp3_path)).with_name(f"short_{token}.mp4")
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,
            lambda: shorts_builder.build_short(thumb_path, mp3_path, short_path),
        )
    except Exception:
        logger.exception("shorts: build_short failed for %s", token)
        await bot.send_message(ADMIN_ID, f"❌ ffmpeg сборка Shorts провалилась для {token}")
        return

    # 3) YT Shorts upload
    try:
        meta_obj = _BM(**meta_d)
    except Exception:
        logger.exception("shorts: BeatMeta hydrate failed for %s", token)
        try:
            short_path.unlink(missing_ok=True)
        except Exception:
            pass
        return

    short_title = beat_post_builder.build_shorts_title(meta_obj)
    short_desc = beat_post_builder.build_shorts_description(
        meta_obj, beat_id=reserved_beat_id,
        full_video_url=f"https://youtu.be/{yt_long_id}",
    )
    short_tags = beat_post_builder.build_shorts_tags(meta_obj)
    short_yt_id: str | None = None
    try:
        short_yt_id = await loop.run_in_executor(
            None,
            lambda: yt_api.upload_video(
                short_path, short_title, short_desc, short_tags, thumb_path,
            ),
        )
        logger.info("shorts: YT Short uploaded https://youtu.be/%s", short_yt_id)
        # Auto-CTA comment под Shorts (как у Long video) —
        # дополнительный funnel-point с TG channel link.
        try:
            await loop.run_in_executor(
                None,
                lambda: _post_cta_comment(
                    short_yt_id, reserved_beat_id, source="ytshorts",
                ),
            )
        except Exception:
            logger.warning("shorts: cta comment post failed (non-fatal)")
    except Exception:
        logger.exception("shorts: YT upload failed for %s", token)

    # 4) TikTok semi-auto: send mp4 + caption админу в ЛС
    if short_path.exists():
        try:
            tiktok_caption = beat_post_builder.build_tiktok_caption(meta_obj)
            with open(short_path, "rb") as vf:
                await bot.send_video(
                    ADMIN_ID,
                    video=InputFile(vf, filename=short_path.name),
                    caption=(
                        f"📱 <b>Готовый TikTok для {beat_name}</b>\n\n"
                        f"<b>Caption (скопируй):</b>\n<code>{html.escape(tiktok_caption)}</code>\n\n"
                        f"<i>Открой на телефоне → сохрани видео → загрузи в TikTok app</i>"
                    ),
                    parse_mode="HTML",
                )
        except Exception:
            logger.exception("shorts: TikTok send to admin failed")

    # 5) Cleanup
    try:
        short_path.unlink(missing_ok=True)
    except Exception:
        pass
    try:
        await asyncio.to_thread(publish_scheduler.cleanup_published_files, token)
    except Exception:
        logger.exception("shorts: cleanup_published_files failed for %s", token)

    # 6) Final notification
    summary = []
    if short_yt_id:
        summary.append(f"✅ YT Shorts: https://youtu.be/{short_yt_id}")
    else:
        summary.append("❌ YT Shorts upload не удался")
    summary.append("📱 TikTok mp4 + caption отправлены тебе в ЛС выше")
    try:
        await bot.send_message(ADMIN_ID, "\n".join(summary))
    except Exception:
        pass


BRAND_IMAGE_URL = (
    "https://github.com/tripmusicrussia-hub/Triple/releases/download/"
    "clip-loops-v1/iiiplfiii_brand.jpg"
)


def _post_cta_comment(
    video_id: str,
    reserved_beat_id: int | None,
    source: str = "yt",
):
    """Постит auto-CTA коммент под YT видео с CTA на TG канал + бот.

    Pinning недоступен через API (убрали в 2024) — админ пиннит вручную
    в YouTube Studio один раз. Даже непиннутый коммент от owner'а
    даёт engagement-signal YT алгоритму в первые минуты.

    `source` — для tracking refs (`yt` для long, `ytshorts` для Shorts).
    Combo deep-link: `?start=ref_<source>_buy_<id>` → first-touch source
    в bot_users.source.
    """
    import yt_api, beat_post_builder
    buy_link = beat_post_builder._buy_link(reserved_beat_id, source=source)
    import licensing
    text = (
        f"🎁 FREE sample pack + 165+ beats → {beat_post_builder.TG_CHANNEL_URL}\n"
        f"💰 Instant MP3 Lease ({licensing.PRICE_MP3_STARS}⭐ / {licensing.PRICE_MP3_USDT:g} USDT / {licensing.PRICE_MP3_RUB}₽) → {buy_link}\n"
        f"🎧 All beats + lease → {beat_post_builder.LANDING_URL}\n"
        f"💎 WAV / Unlimited / Exclusive — DM @iiiplfiii"
    )
    yt_api.post_comment(video_id, text)


def _add_to_yt_playlists(video_id: str, meta):
    """Добавляет YT-видео в artist + scene плейлисты после успешного upload'а.

    Формат названий (повторяем winning-паттерн ниши):
    - '<Artist> Type Beats'     — per-artist (обязательно)
    - 'Hard <Scene> Type Beats' — per-scene (если известна)
    Для коллабов добавляем отдельный плейлист коллаба.
    """
    import yt_api, beat_post_builder
    # Per-artist — основной
    artist_primary = meta.artist_display.split(" x ")[0].strip()
    yt_api.add_video_to_playlist(
        video_id,
        f"{artist_primary} Type Beats",
        playlist_desc=f"Free {artist_primary} type beats by TRIPLE FILL. MP3 Lease: @iiiplfiii",
    )
    # Per-collab если есть (Future x Don Toliver Type Beats)
    if " x " in meta.artist_display:
        yt_api.add_video_to_playlist(
            video_id,
            f"{meta.artist_display} Type Beats",
            playlist_desc=f"Collab beats by TRIPLE FILL. Lease: @iiiplfiii",
        )
    # Per-scene — Memphis / Detroit / Atlanta / Florida и т.д.
    prof = beat_post_builder._get_profile(meta.artist_raw)
    scene = prof.get("scene", "")
    if scene and scene.lower() not in ("hard trap", ""):
        yt_api.add_video_to_playlist(
            video_id,
            f"Hard {scene} Type Beats",
            playlist_desc=f"Hard {scene} type beats for upcoming rappers. @iiiplfiii",
        )


def _ensure_brand_image():
    """Возвращает путь к brand-кадру канала, качая при первом вызове.

    Кэширует в assets/brand/ — директория НЕ wipe'ится janitor'ом.
    Если GH Release недоступен → None (бот падает на legacy text-thumbnail).
    """
    from pathlib import Path as _P
    import httpx as _httpx
    brand_dir = _P(__file__).parent / "assets" / "brand"
    brand_dir.mkdir(parents=True, exist_ok=True)
    brand_path = brand_dir / "iiiplfiii_brand.jpg"
    if brand_path.exists() and brand_path.stat().st_size > 10000:
        return brand_path
    try:
        with _httpx.Client(timeout=60, follow_redirects=True) as c:
            r = c.get(BRAND_IMAGE_URL)
            if r.status_code != 200 or len(r.content) < 10000:
                logger.warning("brand image fetch: status=%d len=%d", r.status_code, len(r.content))
                return None
            brand_path.write_bytes(r.content)
            logger.info("brand image cached: %s (%d KB)", brand_path, len(r.content) // 1024)
            return brand_path
    except Exception as e:
        logger.warning("brand image fetch failed: %s", e)
        return None


def _warmup_ffmpeg():
    try:
        import video_builder
        video_builder.warmup()
    except Exception as e:
        logger.warning("ffmpeg warmup failed: %s", e)


# ── Telethon (user-account API) для delete_message без 48h лимита ────
# Bot API → 48h delete. Telethon с StringSession от phone-аккаунта
# админа → unlimited delete своих сообщений.
# Setup: см. setup_telethon.py + env vars TELETHON_API_ID/HASH/SESSION_STRING.
# Если env пустые → silent skip (бот работает как раньше, только без auto-delete).

_telethon_client = None
_telethon_lock: "asyncio.Lock | None" = None


async def _get_telethon():
    """Lazy-init Telethon client. None если env credentials отсутствуют."""
    global _telethon_client, _telethon_lock
    if _telethon_lock is None:
        _telethon_lock = asyncio.Lock()
    async with _telethon_lock:
        if _telethon_client is not None:
            return _telethon_client if _telethon_client is not False else None
        api_id = (os.getenv("TELETHON_API_ID") or "").strip()
        api_hash = (os.getenv("TELETHON_API_HASH") or "").strip()
        session_str = (os.getenv("TELETHON_SESSION_STRING") or "").strip()
        if not api_id or not api_hash or not session_str:
            _telethon_client = False
            return None
        try:
            from telethon import TelegramClient
            from telethon.sessions import StringSession
            client = TelegramClient(StringSession(session_str), int(api_id), api_hash)
            await client.connect()
            if not await client.is_user_authorized():
                logger.error("telethon: not authorized — re-run setup_telethon.py")
                _telethon_client = False
                return None
            me = await client.get_me()
            logger.info("telethon: authorized as %s (@%s)",
                        me.first_name, me.username)
            _telethon_client = client
            return client
        except Exception:
            logger.exception("telethon init failed")
            _telethon_client = False
            return None


async def _telethon_delete(channel_id: str, msg_id: int) -> None:
    """Удаляет сообщение в канале через user-account (без 48h лимита).

    `channel_id` — `@iiiplfiii` или `-100...` форма.
    Raises на ошибку — caller catch'ит.
    """
    client = await _get_telethon()
    if client is None:
        raise RuntimeError("Telethon not configured (env TELETHON_* missing)")
    # Telethon принимает username в формате '@channel' или int chat_id
    target = channel_id
    if isinstance(target, str) and target.startswith("@"):
        target = target[1:]  # без '@'
    await client.delete_messages(target, [msg_id])
    logger.info("telethon: deleted msg %d from %s", msg_id, channel_id)


# Webhook'у нужен async event loop чтобы dispatch'ить delivery — HTTPServer
# работает в отдельном thread'е, asyncio.run_coroutine_threadsafe требует
# явную ссылку на loop. Заполняются в post_init (когда loop уже запущен).
_WEBHOOK_BOT = None
_WEBHOOK_LOOP = None


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')

    def do_POST(self):
        """YooKassa webhook receiver. Body: {"event": "payment.succeeded",
        "object": {"id": "<uuid>", ...}}.

        ── Security model ──────────────────────────────────────────
        YooKassa НЕ подписывает webhook body криптографически.
        Используем 3 слоя:

        1. **Secret path-token** (основная защита). URL имеет вид
           `/yk_webhook/<TOKEN>` где TOKEN — 32+ байт random hex в env
           YOOKASSA_WEBHOOK_TOKEN. Только YooKassa dashboard знает этот URL.
           Без верного токена → 404 (не 401/403 чтобы не лить информацию).

           IP-whitelist мы НЕ используем как hard gate: `X-Forwarded-For`
           на Render trivially подделывается клиентом (Render append'ит
           real client IP, но leftmost остаётся под контролем атакующего).

        2. **Double-check через YooKassa API**: `_deliver_yk_payment`
           делает GET /v3/payments/{id} с нашими creds → подтверждает
           status=succeeded прежде чем доставить. Fake webhook с
           несуществующим UUID не пройдёт.

        3. **Idempotency guard** (`delivered_yk_payments` set): даже
           если webhook повторяется или кто-то знает валидный UUID
           старого платежа — delivery сработает ровно 1 раз.

        ACK на 200 сразу (YooKassa retry'ит при не-200 в течение 3х суток) →
        delivery async в main loop через run_coroutine_threadsafe.
        """
        # Проверяем path: /yk_webhook/<TOKEN>. TOKEN берём из env —
        # не кэшируем в module var, чтобы можно было ротировать через
        # Render redeploy без code change.
        expected_token = (os.getenv("YOOKASSA_WEBHOOK_TOKEN") or "").strip()
        if not expected_token:
            # Fail-closed: если админ забыл задать env — webhook выключен.
            # Это безопаснее чем open endpoint. Fallback polling заберёт
            # оплаты с ~5 мин задержкой.
            logger.error("yk_webhook: YOOKASSA_WEBHOOK_TOKEN not set, rejecting")
            self.send_response(503)
            self.end_headers()
            return

        expected_path = f"/yk_webhook/{expected_token}"
        # Constant-time compare — защита от timing side-channel.
        # hmac.compare_digest ожидает bytes/str одинаковой длины
        # (иначе утечёт info о длине). Делаем только после проверки
        # префикса, чтобы не сравнивать произвольные пути.
        import hmac
        if not self.path.startswith("/yk_webhook/"):
            self.send_response(404)
            self.end_headers()
            return
        if not hmac.compare_digest(self.path, expected_path):
            # Intentionally 404 (не 401/403) — не подтверждаем существование endpoint'а.
            logger.warning("yk_webhook: bad path token from %s", self.client_address[0])
            self.send_response(404)
            self.end_headers()
            return

        # IP-whitelist: оставляем как **soft signal** для логов.
        # НЕ отклоняем запросы — path-token уже гарантирует authenticity.
        # XFF парсим rightmost (ближайший к нам proxy), но на Render
        # XFF может быть спуфнут клиентом → не верим ему как source of truth.
        fwd = self.headers.get("X-Forwarded-For", "") or ""
        xff_parts = [p.strip() for p in fwd.split(",") if p.strip()]
        client_ip_logged = xff_parts[-1] if xff_parts else self.client_address[0]

        try:
            length = int(self.headers.get("Content-Length", "0"))
            # Защита от огромных body'ев (DoS через память).
            if length > 64 * 1024:
                logger.warning("yk_webhook: body too large %d, reject", length)
                self.send_response(413)
                self.end_headers()
                return
            raw = self.rfile.read(length) if length > 0 else b""
            payload = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            logger.exception("yk_webhook: bad body")
            self.send_response(400)
            self.end_headers()
            return

        event = payload.get("event", "")
        obj = payload.get("object") or {}
        payment_id = obj.get("id") or ""

        # ACK сначала — не тормозим YooKassa пока мы доставляем.
        self.send_response(200)
        self.end_headers()
        try:
            self.wfile.write(b'OK')
        except Exception:
            pass

        # Логгируем payment_id (не секрет сам по себе — уже требуется token
        # чтобы webhook пройти, так что знание UUID без токена бесполезно).
        logger.info("yk_webhook: event=%s payment=%s ip=%s", event, payment_id, client_ip_logged)

        if not payment_id:
            return

        if _WEBHOOK_BOT is None or _WEBHOOK_LOOP is None:
            logger.error("yk_webhook: bot/loop not initialized — skip %s", payment_id)
            return

        if event == "payment.succeeded":
            try:
                asyncio.run_coroutine_threadsafe(
                    _deliver_yk_payment(_WEBHOOK_BOT, payment_id),
                    _WEBHOOK_LOOP,
                )
            except Exception:
                logger.exception("yk_webhook: schedule delivery failed for %s", payment_id)
        elif event == "payment.canceled":
            try:
                # _drop_yk_pending теперь async → schedule на main loop.
                asyncio.run_coroutine_threadsafe(
                    _drop_yk_pending(payment_id),
                    _WEBHOOK_LOOP,
                )
                logger.info("yk_webhook: payment %s canceled, drop scheduled", payment_id)
            except Exception:
                logger.exception("yk_webhook: schedule drop canceled %s failed", payment_id)

    def log_message(self, format, *args):
        pass

def run_health_server():
    port = int(os.environ.get('PORT', 8080))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    server.serve_forever()


async def run_bot():
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("diag", cmd_diag))
    app.add_handler(CommandHandler("search", cmd_search))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("queue", cmd_queue))
    app.add_handler(CommandHandler("cancel_sched", cmd_cancel_sched))
    app.add_handler(CommandHandler("fix_hashtags", cmd_fix_hashtags))
    app.add_handler(CommandHandler("content", cmd_content_schedule))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("cancel_excl", cmd_cancel_excl))
    app.add_handler(CommandHandler("pin_hub", cmd_pin_hub))
    app.add_handler(CommandHandler("upload_product", cmd_upload_product))
    app.add_handler(CommandHandler("cancel_product", cmd_cancel_product))
    app.add_handler(CommandHandler("feature", cmd_feature))
    app.add_handler(CommandHandler("repost_now", cmd_repost_now))
    app.add_handler(CommandHandler("auto_repost", cmd_auto_repost))
    app.add_handler(CommandHandler("quick_meta", cmd_quick_meta))
    app.add_handler(CommandHandler("stop_reminders", cmd_stop_reminders))
    app.add_handler(CommandHandler("start_reminders", cmd_start_reminders))
    app.add_handler(CommandHandler("yt_audit", cmd_yt_audit))
    app.add_handler(CommandHandler("yt_refresh_old", cmd_yt_refresh_old))
    app.add_handler(CommandHandler("cart", cmd_cart))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(PreCheckoutQueryHandler(handle_precheckout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, handle_successful_payment))
    app.add_handler(MessageHandler(filters.VOICE | filters.AUDIO, handle_voice))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    # ChatMemberHandler.CHAT_MEMBER — получаем updates о members в чатах
    # где бот admin. Наш канал @iiiplfiii — основной use-case.
    app.add_handler(ChatMemberHandler(handle_chat_member_update, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_assistant))
    app.add_handler(MessageHandler(filters.ALL, handle_message))
    logger.info("Starting bot...")
    await app.initialize()
    # КРИТИЧНО: post_init hook НЕ вызывается автоматически когда мы
    # используем manual app.initialize()+start()+updater.start_polling()
    # вместо app.run_polling(). Без этого вызова scheduled_publish_loop,
    # heartbeat_scheduler, content_reminder_scheduler, load_queue —
    # никогда не запускались. Это был корневой баг почему scheduled
    # publications не работали никогда.
    await post_init(app)
    await app.start()
    # allowed_updates включает "chat_member" — без этого ChatMemberHandler
    # не получает события о новых подписчиках канала. По умолчанию TG
    # не шлёт chat_member updates (security/traffic).
    await app.updater.start_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )
    await asyncio.Event().wait()


def main():
    t = threading.Thread(target=run_health_server, daemon=True)
    t.start()
    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
