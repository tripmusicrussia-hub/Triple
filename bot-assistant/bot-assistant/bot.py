import asyncio
import logging
import threading
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
    CallbackQueryHandler, PreCheckoutQueryHandler, filters, ContextTypes
)
from telegram.error import TelegramError
from config import (
    BOT_TOKEN, CHANNEL_ID, CHANNEL_LINK, SAMPLE_PACK_PATH, SAMPLE_PACK_FILE_ID,
    WELCOME_TEXT, CATALOG_INTRO, ADMIN_ID, CHANNEL_POST_HOUR,
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

# Превью автопостов в канал: token → payload (rubric, kind, text, beat).
# Персистится на диск (Render free tier часто перезапускается — без persist
# после рестарта юзер жал "Опубликовать" и получал "Превью устарело" вместо
# публикации; это был скрытый баг). Файл ниже обновляется при append/pop.
pending_posts: dict[str, dict] = {}
PENDING_POSTS_PATH = os.path.join(BASE_DIR, "pending_posts.json")


def _persist_pending_posts() -> None:
    """Сбрасывает pending_posts на диск. Payload уже JSON-serializable
    (kind/rubric/text/topic/issues/weekday — примитивы, beat — dict).
    """
    try:
        import json as _json
        with open(PENDING_POSTS_PATH, "w", encoding="utf-8") as f:
            _json.dump(pending_posts, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception("persist pending_posts failed (non-fatal)")


def _restore_pending_posts() -> int:
    """Восстанавливает pending_posts с диска в post_init."""
    if not os.path.exists(PENDING_POSTS_PATH):
        return 0
    try:
        import json as _json
        with open(PENDING_POSTS_PATH, "r", encoding="utf-8") as f:
            data = _json.load(f)
        if isinstance(data, dict):
            pending_posts.update(data)
            return len(data)
    except Exception:
        logger.exception("restore pending_posts failed")
    return 0


# CHANNEL_POST_HOUR теперь в config.py (env-configurable с дефолтом 16 МСК)

# Общий helper для user-facing ошибок. Полный exception должен быть уже
# залогирован через logger.exception() выше по стеку — юзеру показываем
# generic сообщение без технических деталей, чтобы API-URL / stack trace /
# credentials не утекли в чат.
USER_ERROR_FALLBACK_CONTACT = "@iiiplfiii"


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

def kb_main_menu():
    beats = len([b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == "beat"])
    tracks = len([b for b in beats_db.BEATS_CACHE if b.get("content_type") == "track"])
    remixes = len([b for b in beats_db.BEATS_CACHE if b.get("content_type") == "remix"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🎹 Биты ({beats})", callback_data="menu_beat")],
        [InlineKeyboardButton(f"🎤 Треки ({tracks})", callback_data="menu_track"),
         InlineKeyboardButton(f"🔀 Ремиксы ({remixes})", callback_data="menu_remix")],
        [InlineKeyboardButton("📦 Kits & Packs", callback_data="menu_products")],
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


async def do_quick_filter(bot, chat_id: int, user_id: int, filter_name: str, page: int = 0):
    """Выполняет quick-filter поиск + показ paginated результатов."""
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
        await bot.send_message(chat_id, f"{title}: пусто, попробуй другой фильтр",
                               reply_markup=kb_main_menu())
        return
    await bot.send_message(
        chat_id,
        f"{title} — нашёл {len(results)} треков:",
        reply_markup=_kb_search_results(results, filter_name, page),
    )

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

def kb_after_beat(beat_id, content_type="beat"):
    back_map = {"beat": "menu_beat", "track": "menu_track", "remix": "menu_remix"}
    rows = [[InlineKeyboardButton("▶️ Следующий похожий", callback_data="next_" + str(beat_id))]]
    if content_type == "beat":
        rows.append([
            InlineKeyboardButton(f"⭐ {licensing.PRICE_MP3_STARS}", callback_data="buy_mp3_" + str(beat_id)),
            InlineKeyboardButton(f"💵 {licensing.PRICE_MP3_USDT:g} USDT", callback_data="buy_usdt_" + str(beat_id)),
        ])
    rows.append([InlineKeyboardButton("❤️ В избранное", callback_data="fav_" + str(beat_id)),
                 InlineKeyboardButton("🎲 Случайный", callback_data="random_beat")])
    rows.append([InlineKeyboardButton("◀️ Меню", callback_data=back_map.get(content_type, "main_menu"))])
    return InlineKeyboardMarkup(rows)


def kb_channel_beat_buy(beat_id: int) -> InlineKeyboardMarkup:
    """Клавиатура под публикацией бита в канале: Stars + USDT + ссылка на ЛС для WAV/Exclusive."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⭐ MP3 · {licensing.PRICE_MP3_STARS}", callback_data="buy_mp3_" + str(beat_id)),
         InlineKeyboardButton(f"💵 MP3 · {licensing.PRICE_MP3_USDT:g} USDT", callback_data="buy_usdt_" + str(beat_id))],
        [InlineKeyboardButton("✍️ WAV / Unlimited / Exclusive", url="https://t.me/iiiplfiii")],
    ])


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
    queue_label = f"📅 Очередь публикаций ({queue_n})" if queue_n else "📅 Очередь публикаций (пусто)"
    products_label = f"📦 Kits & Packs ({products_n})" if products_n else "📦 Kits & Packs — залить первый"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика (" + str(len(all_users)) + " польз.)", callback_data="admin_stats")],
        [InlineKeyboardButton("🎹 " + str(beats) + " / 🎤 " + str(tracks) + " / 🔀 " + str(remixes), callback_data="admin_catalog")],
        [InlineKeyboardButton(products_label, callback_data="admin_products")],
        [InlineKeyboardButton(queue_label, callback_data="admin_queue")],
        [InlineKeyboardButton("📌 Обновить закреп-пост (навигация)", callback_data="admin_pin_hub")],
        [InlineKeyboardButton("🗑 Удалить бит из каталога", callback_data="admin_clearbeats")],
        [InlineKeyboardButton("📡 Автопост в канал", callback_data="admin_channelpost")],
        [InlineKeyboardButton("🎬 YouTube", callback_data="admin_yt_menu")],
    ])


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


def kb_admin_channel():
    dry = "🧪 DRY " if os.getenv("POST_DRY_RUN") == "1" else ""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⚡ {dry}Сгенерить на сегодня", callback_data="admin_postnow_today")],
        [InlineKeyboardButton("Пн Memphis Monday", callback_data="admin_postnow_0"),
         InlineKeyboardButton("Пт Hard Friday", callback_data="admin_postnow_4")],
        [InlineKeyboardButton("Вт Quick Tip", callback_data="admin_postnow_1"),
         InlineKeyboardButton("Ср Hard Lifehack", callback_data="admin_postnow_2")],
        [InlineKeyboardButton("Чт Studio Story", callback_data="admin_postnow_3"),
         InlineKeyboardButton("Сб За кулисами", callback_data="admin_postnow_5")],
        [InlineKeyboardButton("Вс Итог + вопрос", callback_data="admin_postnow_6")],
        [InlineKeyboardButton("➕ Добавить тему в бэклог", callback_data="admin_idea_menu")],
        [InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")],
    ])


def kb_admin_idea_day():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Вт Quick Tip", callback_data="admin_idea_1"),
         InlineKeyboardButton("Ср Hard Lifehack", callback_data="admin_idea_2")],
        [InlineKeyboardButton("Чт Studio Story", callback_data="admin_idea_3"),
         InlineKeyboardButton("Сб За кулисами", callback_data="admin_idea_5")],
        [InlineKeyboardButton("Вс Итог + вопрос", callback_data="admin_idea_6")],
        [InlineKeyboardButton("◀️ Назад", callback_data="admin_channelpost")],
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

async def send_sample_pack(bot, chat_id):
    try:
        if SAMPLE_PACK_FILE_ID:
            await bot.send_document(chat_id, document=SAMPLE_PACK_FILE_ID, caption="🎁 Твой FREE Sample Pack!")
            return
    except Exception as e:
        logger.warning("file_id failed: " + str(e))
    try:
        if os.path.exists(SAMPLE_PACK_PATH):
            with open(SAMPLE_PACK_PATH, "rb") as f:
                await bot.send_document(chat_id, document=f, caption="🎁 Твой FREE Sample Pack!")
        else:
            await bot.send_message(chat_id, "🎁 Сэмпл пак: " + CHANNEL_LINK)
    except Exception as e:
        logger.error("Sample pack error: " + str(e))

async def show_main_menu(bot, chat_id):
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
    await bot.send_message(chat_id, text, reply_markup=kb_main_menu())

async def send_beat(bot, chat_id, beat, user_id):
    add_to_history(user_id, beat["id"])
    bid = beat["id"]
    beat_plays[bid] = beat_plays.get(bid, 0) + 1
    if bid not in beat_plays_users:
        beat_plays_users[bid] = set()
    beat_plays_users[bid].add(user_id)

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

    if beat.get("file_id"):
        try:
            await bot.send_audio(chat_id, audio=beat["file_id"], caption=caption,
                                 reply_markup=kb_after_beat(beat["id"], content_type))
            return
        except Exception as e:
            logger.warning("Audio send failed: " + str(e))

    caption += "\n\n👉 " + beat["post_url"]
    await bot.send_message(chat_id, caption, reply_markup=kb_after_beat(beat["id"], content_type))


# ── /start ────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    bot = context.bot
    write_heartbeat()

    # is_new определяется через Supabase (source of truth между Render
    # redeploy'ями). Fallback — in-memory all_users (свежий процесс).
    supabase_is_new = await asyncio.to_thread(
        users_db.upsert_user, user_id, user.full_name, user.username
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
            await bot.send_message(ADMIN_ID, "🔔 Новый: " + uname + " | Всего: " + str(len(all_users)))
        except Exception:
            pass

    # Deep-link из канала на карточку продукта: /start prod_<product_id>
    if context.args and context.args[0].startswith("prod_"):
        try:
            pid = int(context.args[0][5:])
            p = beats_db.get_beat_by_id(pid)
            if p and p.get("content_type") in licensing.PRODUCT_TYPE_LABELS:
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
                reply_markup=kb_main_menu(),
            )
            return
        except Exception as e:
            logger.warning("deep-link prod_ failed for %r: %s", context.args[0], e)

    # Deep-link из YT-описания: /start buy_<beat_id> → сразу показать покупку этого бита
    if context.args and context.args[0].startswith("buy_"):
        try:
            beat_id = int(context.args[0][4:])
            beat = beats_db.get_beat_by_id(beat_id)
            if beat:
                caption = (
                    f"🎧 <b>{beat.get('name','?')}</b>\n"
                    f"⚡ BPM {beat.get('bpm','?')}  🎹 {beat.get('key','?')}\n\n"
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
                reply_markup=kb_main_menu(),
            )
            return
        except Exception as e:
            logger.warning("deep-link buy_ failed for %r: %s", context.args[0], e)
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
        await send_sample_pack(bot, user_id)
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


# ── /postnow и /idea — автопостинг в канал ────────────────────

RUBRIC_ALIASES = {
    "пн": 0, "mon": 0, "0": 0,
    "вт": 1, "tue": 1, "1": 1,
    "ср": 2, "wed": 2, "2": 2,
    "чт": 3, "thu": 3, "3": 3,
    "пт": 4, "fri": 4, "4": 4,
    "сб": 5, "sat": 5, "5": 5,
    "вс": 6, "sun": 6, "6": 6,
}


async def cmd_postnow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сгенерировать preview автопоста сейчас. /postnow [день]. Только админ."""
    if update.effective_user.id != ADMIN_ID:
        return
    weekday = None
    if context.args:
        arg = context.args[0].lower()
        if arg in RUBRIC_ALIASES:
            weekday = RUBRIC_ALIASES[arg]
        else:
            await update.message.reply_text("Рубрика: пн/вт/ср/чт/пт/сб/вс или 0-6")
            return
    await update.message.reply_text("⏳ Генерирую превью...")
    await preview_daily_post(context.bot, ADMIN_ID, weekday=weekday)


async def _append_idea(update: Update, wd: int, topic: str) -> None:
    if wd not in (1, 2, 3, 5, 6):
        await update.message.reply_text("Только текстовые рубрики: вт/ср/чт/сб/вс")
        return
    section = post_generator.RUBRIC_SCHEDULE[wd]["section"]
    ideas_path = post_generator.POST_IDEAS_PATH
    try:
        text = ideas_path.read_text(encoding="utf-8")
        lines = text.splitlines()
        insert_idx = None
        for i, line in enumerate(lines):
            if line.startswith("## ") and section in line:
                j = i + 1
                while j < len(lines) and not lines[j].startswith("## "):
                    j += 1
                while j > i + 1 and lines[j - 1].strip() == "":
                    j -= 1
                insert_idx = j
                break
        if insert_idx is None:
            await update.message.reply_text(f"⚠️ Раздел не найден: {section}")
            return
        lines.insert(insert_idx, f"- [ ] {topic}")
        ideas_path.write_text("\n".join(lines) + ("\n" if text.endswith("\n") else ""), encoding="utf-8")
        await update.message.reply_text(f"✅ Добавлено в «{section}»:\n- [ ] {topic}")
    except Exception as e:
        await update.message.reply_text(f"⚠️ Ошибка: {e}")


async def cmd_idea(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавить тему в post_ideas.md. Формат: /idea <день> <тема>. Только админ."""
    if update.effective_user.id != ADMIN_ID:
        return
    if len(context.args) < 2:
        await update.message.reply_text(
            "Формат: /idea <день> <тема>\n"
            "Дни: вт, ср, чт, сб, вс"
        )
        return
    day = context.args[0].lower()
    topic = " ".join(context.args[1:]).strip()
    wd = RUBRIC_ALIASES.get(day)
    if wd is None:
        await update.message.reply_text("Неизвестный день. Дни: вт/ср/чт/сб/вс")
        return
    await _append_idea(update, wd, topic)


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


# ── Автопостинг в канал @iiiplfiii ────────────────────────────
WEEKDAY_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


def _post_preview_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Опубликовать", callback_data="pub_" + token),
         InlineKeyboardButton("🔄 Перегенерировать", callback_data="regen_" + token)],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_" + token)],
    ])


async def send_preview(bot, admin_id: int, payload: dict) -> None:
    """Показывает preview админу в ЛС с кнопками."""
    token = uuid.uuid4().hex[:12]
    pending_posts[token] = payload
    _persist_pending_posts()
    wd = datetime.now().weekday()
    header = f"📅 {WEEKDAY_RU[wd]} — {payload['rubric']} [{payload['kind']}]"
    issues = payload.get("issues") or []
    if issues:
        header = "⚠️ automod не прошёл: " + "; ".join(issues) + "\n" + header
    kb = _post_preview_keyboard(token)
    try:
        if payload["kind"] == "audio" and payload.get("beat"):
            beat = payload["beat"]
            caption = header + "\n\n" + payload["text"]
            if len(caption) > 1024:
                caption = caption[:1020] + "..."
            await bot.send_audio(admin_id, audio=beat["file_id"], caption=caption, reply_markup=kb)
        else:
            await bot.send_message(admin_id, header + "\n\n" + payload["text"], reply_markup=kb)
    except Exception as e:
        logger.error("send_preview error: %s", e)
        await bot.send_message(admin_id, f"⚠️ Ошибка preview: {e}")


async def preview_daily_post(bot, admin_id: int, weekday: int | None = None) -> None:
    try:
        payload = await post_generator.generate_today_post(weekday=weekday)
    except Exception as e:
        logger.error("generate_today_post failed: %s", e)
        await bot.send_message(admin_id, f"⚠️ Генерация упала: {e}")
        return
    await send_preview(bot, admin_id, payload)


async def daily_channel_scheduler(bot, admin_id: int):
    """Каждый день в 16:00 МСК готовит preview автопоста и шлёт админу в ЛС."""
    while True:
        try:
            now = datetime.now(MSK_TZ)
            target = now.replace(hour=CHANNEL_POST_HOUR, minute=0, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            wait_s = (target - now).total_seconds()
            logger.info("daily_channel_scheduler: sleep until %s MSK (%.0fs)", target.isoformat(), wait_s)
            await asyncio.sleep(wait_s)
            if admin_id:
                await preview_daily_post(bot, admin_id)
        except Exception as e:
            logger.error("daily_channel_scheduler: %s", e)
            await asyncio.sleep(60)


async def publish_to_channel(bot, payload: dict) -> bool:
    """Публикует payload в CHANNEL_ID. Возвращает True при успехе.
    Если POST_DRY_RUN=1 — шлёт в ЛС админа с пометкой и НЕ трогает cooldown/темы."""
    dry_run = os.getenv("POST_DRY_RUN") == "1"
    target = ADMIN_ID if dry_run else CHANNEL_ID
    if not target:
        logger.error("Не задан target (CHANNEL_ID или ADMIN_ID)")
        return False
    prefix = "🧪 [DRY RUN — было бы в канале]\n\n" if dry_run else ""
    try:
        if payload["kind"] == "audio" and payload.get("beat"):
            beat = payload["beat"]
            caption = prefix + payload["text"]
            if len(caption) > 1024:
                caption = caption[:1020] + "..."
            buy_kb = kb_channel_beat_buy(beat["id"])
            await bot.send_audio(target, audio=beat["file_id"], caption=caption, reply_markup=buy_kb)
            if not dry_run:
                post_generator.mark_beat_posted(beat["id"])
        else:
            await bot.send_message(target, prefix + payload["text"])
            if not dry_run:
                topic = payload.get("topic")
                if topic:
                    post_generator.mark_topic_used(topic)
        return True
    except Exception as e:
        logger.error("publish_to_channel error: %s", e)
        return False


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

    if data == "admin_channelpost":
        if user_id != ADMIN_ID:
            return
        dry_hint = "\n🧪 DRY_RUN включён — публикация пойдёт тебе в ЛС" if os.getenv("POST_DRY_RUN") == "1" else ""
        await query.message.reply_text("📡 Автопост в канал:" + dry_hint, reply_markup=kb_admin_channel())
        return

    if data == "admin_yt_menu":
        if user_id != ADMIN_ID:
            return
        await query.message.reply_text("🎬 YouTube:", reply_markup=kb_admin_yt())
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
            import beat_post_builder
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
                    # Cleanup — short file нужен только для upload'а
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

    if data.startswith("admin_postnow_"):
        if user_id != ADMIN_ID:
            return
        arg = data[len("admin_postnow_"):]
        wd = None if arg == "today" else int(arg)
        await query.message.reply_text("⏳ Генерирую превью...")
        await preview_daily_post(bot, ADMIN_ID, weekday=wd)
        return

    if data == "admin_idea_menu":
        if user_id != ADMIN_ID:
            return
        await query.message.reply_text(
            "➕ Выбери рубрику для новой темы:", reply_markup=kb_admin_idea_day()
        )
        return

    if data.startswith("admin_idea_") and data != "admin_idea_menu":
        if user_id != ADMIN_ID:
            return
        wd = int(data[len("admin_idea_"):])
        bulk_add_mode[str(ADMIN_ID) + "_idea"] = wd
        section = post_generator.RUBRIC_SCHEDULE[wd]["section"]
        await query.message.reply_text(
            f"✍️ Пришли следующим сообщением текст темы для «{section}».\n"
            "Пример: «Pro-Q 3 dynamic EQ на 200 Hz у 808»\n\n"
            "Отменить — /cancel"
        )
        return

    if data.startswith(("pub_", "regen_", "cancel_")):
        if user_id != ADMIN_ID:
            await query.answer("Только админ", show_alert=True)
            return
        action, token = data.split("_", 1)
        payload = pending_posts.get(token)
        if not payload:
            await query.answer("Превью уже обработано или устарело", show_alert=True)
            try:
                await query.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            return

        if action == "pub":
            ok = await publish_to_channel(bot, payload)
            pending_posts.pop(token, None)
            _persist_pending_posts()
            try:
                await query.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            if ok:
                await bot.send_message(user_id, f"✅ Опубликовано в канал ({payload['rubric']})")
            else:
                await bot.send_message(user_id, "⚠️ Не удалось опубликовать — смотри логи")
            return

        if action == "regen":
            pending_posts.pop(token, None)
            _persist_pending_posts()
            try:
                await query.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            await bot.send_message(user_id, "🔄 Перегенерирую...")
            await preview_daily_post(bot, user_id, weekday=payload.get("weekday"))
            return

        if action == "cancel":
            pending_posts.pop(token, None)
            _persist_pending_posts()
            try:
                await query.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            await bot.send_message(user_id, "❌ Отменено")
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
                await send_sample_pack(bot, user_id)
                users_received_pack.add(user_id)
                await asyncio.to_thread(users_db.mark_sample_pack_received, user_id)
                asyncio.create_task(asyncio.to_thread(save_users))
            await show_main_menu(bot, user_id)
        else:
            await query.answer("Ты ещё не подписан! Подпишись и нажми снова.", show_alert=True)
        return

    if data == "main_menu":
        await show_main_menu(bot, query.message.chat_id)
        return

    if data == "menu_beat":
        beats = len([b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == "beat"])
        await query.message.reply_text("🎹 Целых " + str(beats) + " битов! Ищешь что-то конкретное или просто серфишь?", reply_markup=kb_beats_menu())
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
            await query.message.reply_text(
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
        await query.message.reply_text(
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
        await query.message.reply_text(
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
        await query.message.reply_text(info, reply_markup=kb, parse_mode="HTML")
        return

    if data == "noop":
        await query.answer()
        return

    if data == "menu_track":
        tracks = [b for b in beats_db.BEATS_CACHE if b.get("content_type") == "track"]
        if not tracks:
            await query.answer("Треков пока нет!", show_alert=True)
            return
        await query.message.reply_text("🎤 " + str(len(tracks)) + " треков от IIIPLKIII — слушай на здоровье!", reply_markup=kb_tracks_menu())
        return

    if data == "menu_remix":
        remixes = [b for b in beats_db.BEATS_CACHE if b.get("content_type") == "remix"]
        if not remixes:
            await query.answer("Ремиксов пока нет!", show_alert=True)
            return
        await query.message.reply_text("🔀 " + str(len(remixes)) + " ремиксов — узнаешь мелодию? 😄", reply_markup=kb_remixes_menu())
        return

    if data == "beats_by_artist":
        await query.message.reply_text("🎤 Под кого делаем? Выбирай артиста — найду похожие биты!", reply_markup=kb_artists())
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
            await query.message.reply_text("Тут пока пусто 🙈\nСлушай биты и жми ❤️ — сохраню сюда!")
            return
        beats_list = [beats_db.get_beat_by_id(bid) for bid in favs]
        beats_list = [b for b in beats_list if b]
        rows = [[InlineKeyboardButton(b["name"][:40], callback_data="play_" + str(b["id"]))] for b in beats_list[-10:]]
        rows.append([InlineKeyboardButton("◀️ Меню", callback_data="main_menu")])
        await query.message.reply_text("❤️ Избранное (" + str(len(beats_list)) + "):", reply_markup=InlineKeyboardMarkup(rows))
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
        await query.message.reply_text("🔍 Напиши название бита, имя артиста или тег — найду всё что есть!")
        return

    # Quick-filter chips (qf_hard / qf_memphis / qf_detroit / qf_ru / qf_bpm140 / qf_bpm160)
    if data.startswith("qf_"):
        filter_name = data[3:]
        await do_quick_filter(bot, query.message.chat_id, user_id, filter_name, page=0)
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

    if data == "admin_panel":
        if user_id != ADMIN_ID: return
        await query.message.reply_text("🎛 Панель управления:", reply_markup=kb_admin())
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
        await query.message.reply_text(
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
        await query.message.reply_text(
            info, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀️ К списку", callback_data="admin_products")],
            ]),
        )
        return

    if data == "admin_queue":
        if user_id != ADMIN_ID: return
        import publish_scheduler
        n = publish_scheduler.queue_size()
        if n == 0:
            await query.message.reply_text(
                "📭 Очередь пуста. Загружай битеки — жми «📅 В лучшее время» в превью.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")]])
            )
        else:
            await query.message.reply_text(
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
        await query.message.reply_text(
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
            await query.message.reply_text("Нет данных.")
            return
        text = "📈 Топ-20:\n\n"
        for i, (bid, count) in enumerate(top):
            b = beats_db.get_beat_by_id(bid)
            name = b["name"][:30] if b else "Unknown"
            uniq = len(beat_plays_users.get(bid, set()))
            favs = sum(1 for fl in user_favorites.values() if bid in fl)
            text += str(i+1) + ". " + name + "\n   ▶️ " + str(count) + "  👥 " + str(uniq) + "  ❤️ " + str(favs) + "\n\n"
        await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_stats")]]))
        return

    if data == "admin_catalog":
        if user_id != ADMIN_ID: return
        tags = beats_db.get_all_tags()
        await query.message.reply_text(
            "📂 Каталог: " + str(len(beats_db.BEATS_CACHE)) + " шт.\nТеги: " + ", ".join(tags[:20]),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")]])
        )
        return

    if data in ("admin_addbeats_track", "admin_addbeats_remix"):
        if user_id != ADMIN_ID: return
        mode_type = data.replace("admin_addbeats_", "")
        bulk_add_mode[ADMIN_ID] = mode_type
        icons = {"track": "🎤 треки", "remix": "🔀 ремиксы"}
        await query.message.reply_text(
            "✅ Режим включён! Всё пойдёт как: " + icons[mode_type] + "\n\nПересылай посты из канала.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⛔ Закончить", callback_data="admin_stopadd")]]))
        return

    if data == "admin_addbeats":
        if user_id != ADMIN_ID: return
        bulk_add_mode[ADMIN_ID] = "beat"
        await query.message.reply_text("✅ Режим добавления ВКЛЮЧЁН!\nПересылай посты из канала.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⛔ Закончить", callback_data="admin_stopadd")]]))
        return

    if data == "admin_stopadd":
        if user_id != ADMIN_ID: return
        bulk_add_mode.pop(ADMIN_ID, None)
        beats_c = len([b for b in beats_db.BEATS_CACHE if b.get("content_type","beat")=="beat"])
        tracks_c = len([b for b in beats_db.BEATS_CACHE if b.get("content_type")=="track"])
        remixes_c = len([b for b in beats_db.BEATS_CACHE if b.get("content_type")=="remix"])
        await query.message.reply_text("⛔ Добавление завершено.\n🎹 " + str(beats_c) + " / 🎤 " + str(tracks_c) + " / 🔀 " + str(remixes_c),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ В панель", callback_data="admin_panel")]]))
        return

    if data == "admin_clearbeats":
        if user_id != ADMIN_ID: return
        await query.message.reply_text("🗑 Что удаляем?", reply_markup=InlineKeyboardMarkup([
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
        await query.message.reply_text(
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
        await query.message.reply_text("🗑 Каталог очищен!",
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
        # Не в режиме загрузки — игнорим document тихо (админ мог прислать
        # PDF/скрин/что-то ещё по другому поводу).
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

        # Ввод новой темы в post_ideas.md (из inline-меню)
        idea_key = str(ADMIN_ID) + "_idea"
        if idea_key in bulk_add_mode:
            if text.strip() == "/cancel":
                del bulk_add_mode[idea_key]
                await update.message.reply_text("❌ Отменено")
                return
            wd = bulk_add_mode.pop(idea_key)
            await _append_idea(update, wd, text.strip())
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
            currency=currency,
            payment_charge_id=payment_charge_id,
            provider_charge_id=None,
            status="completed",
        )
    except Exception:
        logger.exception("sales.log_sale failed")

    try:
        amount_disp = f"{amount}⭐" if currency == "XTR" else f"{amount} {currency}"
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
            currency=currency,
            payment_charge_id=payment_charge_id,
            provider_charge_id=None,
            status="completed",
        )
    except Exception:
        logger.exception("sales.log_sale failed (product)")

    try:
        amount_disp = f"{amount}⭐" if currency == "XTR" else f"{amount} {currency}"
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


# ── Telegram Stars payments ───────────────────────────────────

async def handle_precheckout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """PreCheckout: апрувим всё валидное. Отказ — только если payload битый
    или товар (бит/пак) пропал.

    Payload форматы:
        mp3_lease:<beat_id>          — MP3 Lease на бит
        product:<product_id>         — drum kit / sample pack / loop pack
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

        logger.warning("successful_payment: unknown payload %s", payload)
    except Exception:
        logger.exception("handle_successful_payment failed")


# ── CryptoBot (USDT) payments ─────────────────────────────────

# Активные USDT-инвойсы: invoice_id → (user_id, beat_id, created_at)
pending_usdt_invoices: dict[int, dict] = {}


async def poll_usdt_invoice(bot, invoice_id: int, user_id: int, item_id: int,
                            kind: str = "beat", timeout_sec: int = 1800):
    """Пуллит инвойс до оплаты/expiry. Доставляет бит или продукт при paid.

    kind="beat" → _deliver_mp3_lease, kind="product" → _deliver_product.

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
    # Восстанавливаем очередь плановых публикаций с диска
    try:
        import publish_scheduler
        n = publish_scheduler.load_queue()
        logger.info("publish_scheduler: restored %d queued items on startup", n)
    except Exception:
        logger.exception("publish_scheduler restore failed (non-fatal)")
    # Восстанавливаем pending_products — недосохранённые продукты на preview-шаге.
    try:
        restored = _restore_pending_products()
        if restored:
            logger.info("pending_products: restored %d items on startup", restored)
    except Exception:
        logger.exception("pending_products restore failed (non-fatal)")
    # Восстанавливаем pending_posts — превью автопостов, на которые админ
    # ещё не нажал "Опубликовать". Без restore после redeploy клик на кнопку
    # приводил к "Превью устарело" вместо публикации.
    try:
        restored = _restore_pending_posts()
        if restored:
            logger.info("pending_posts: restored %d items on startup", restored)
    except Exception:
        logger.exception("pending_posts restore failed (non-fatal)")
    asyncio.create_task(daily_channel_scheduler(application.bot, ADMIN_ID))
    asyncio.create_task(scheduled_publish_loop(application.bot))
    asyncio.create_task(heartbeat_scheduler())
    asyncio.create_task(asyncio.to_thread(_warmup_ffmpeg))
    write_heartbeat()


async def scheduled_publish_loop(bot):
    """Каждые 60с проверяет очередь publish_scheduler и публикует due item'ы."""
    import publish_scheduler
    while True:
        try:
            for item in publish_scheduler.due_items():
                try:
                    await _execute_scheduled_publish(bot, item)
                    publish_scheduler.mark_published(item["token"])
                    # Чистим temp-файлы
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

    # Уведомление админу
    try:
        parts = []
        if "yt" in actions:
            parts.append(f"YT: {'✅ https://youtu.be/' + yt_video_id if yt_ok else '❌'}")
        if "tg" in actions:
            parts.append(f"TG: {'✅ msg_id=' + str(tg_message_id) if tg_ok else '❌'}")
        await bot.send_message(
            ADMIN_ID,
            f"📅 Плановая публикация отработала — {meta_d.get('name','?')} — {meta_d.get('artist_display','?')}\n"
            + "\n".join(parts)
        )
    except Exception:
        logger.exception("scheduled: admin notify failed")


BRAND_IMAGE_URL = (
    "https://github.com/tripmusicrussia-hub/Triple/releases/download/"
    "clip-loops-v1/iiiplfiii_brand.jpg"
)


def _post_cta_comment(video_id: str, reserved_beat_id: int | None):
    """Постит auto-CTA коммент под YT видео с landing-ссылкой + deep-link.

    Pinning недоступен через API (убрали в 2024) — админ пиннит вручную
    в YouTube Studio один раз. Даже непиннутый коммент от owner'а
    даёт engagement-signal YT алгоритму в первые минуты.
    """
    import yt_api, beat_post_builder
    buy_link = beat_post_builder._buy_link(reserved_beat_id)
    import licensing
    text = (
        f"🎧 All beats + lease → {beat_post_builder.LANDING_URL}\n"
        f"💰 Instant MP3 Lease ({licensing.PRICE_MP3_STARS}⭐ / {licensing.PRICE_MP3_USDT:g} USDT) → {buy_link}\n"
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


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')
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
    app.add_handler(CommandHandler("postnow", cmd_postnow))
    app.add_handler(CommandHandler("idea", cmd_idea))
    app.add_handler(CommandHandler("search", cmd_search))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("queue", cmd_queue))
    app.add_handler(CommandHandler("cancel_sched", cmd_cancel_sched))
    app.add_handler(CommandHandler("pin_hub", cmd_pin_hub))
    app.add_handler(CommandHandler("upload_product", cmd_upload_product))
    app.add_handler(CommandHandler("cancel_product", cmd_cancel_product))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(PreCheckoutQueryHandler(handle_precheckout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, handle_successful_payment))
    app.add_handler(MessageHandler(filters.VOICE | filters.AUDIO, handle_voice))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_assistant))
    app.add_handler(MessageHandler(filters.ALL, handle_message))
    logger.info("Starting bot...")
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    await asyncio.Event().wait()


def main():
    t = threading.Thread(target=run_health_server, daemon=True)
    t.start()
    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
