import asyncio
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import random
import os
import json
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes
)
from telegram.error import TelegramError
from config import BOT_TOKEN, CHANNEL_ID, CHANNEL_LINK, SAMPLE_PACK_PATH, SAMPLE_PACK_FILE_ID, WELCOME_TEXT, CATALOG_INTRO, ADMIN_ID
import beats_db
import assistant_ai
import sigma_api

# Глобальный экземпляр SigmaAPI — кэш товаров сохраняется между накладными
_sigma_instance = sigma_api.SigmaAPI()

conversation_history = {}

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
giveaway = {"active": False, "prize_file": None, "prize_name": "", "end_time": None, "participants": {}}

ARTIST_TAGS = [
    "keyglock", "bossmandlow", "bossman", "obladaet", "nardowick",
    "kizaru", "scryptonite", "skryptonit", "konfuz", "bigbabytape",
    "bushidozho", "dababy", "gunna", "icewearvezzo", "jayfizzle",
    "jerk", "kennymuney", "poohshiesty", "rob49", "saintjhn",
    "ytbfatt", "alblack", "florida", "future", "southside",
]


# ── Сохранение / загрузка ─────────────────────────────────────

def save_users():
    try:
        data = {
            "all_users": {str(k): v for k, v in all_users.items()},
            "users_received_pack": list(users_received_pack),
            "subscribed_users": list(subscribed_users),
            "user_favorites": {str(k): v for k, v in user_favorites.items()},
        }
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Save users error: " + str(e))

def load_users():
    global all_users, users_received_pack, subscribed_users, user_favorites
    try:
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            all_users = {int(k): v for k, v in data.get("all_users", {}).items()}
            users_received_pack = set(int(x) for x in data.get("users_received_pack", []))
            subscribed_users = set(int(x) for x in data.get("subscribed_users", []))
            user_favorites = {int(k): v for k, v in data.get("user_favorites", {}).items()}
            logger.info("Users loaded: " + str(len(all_users)))
    except Exception as e:
        logger.error("Load users error: " + str(e))

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
        [InlineKeyboardButton("🎹 Биты (" + str(beats) + ")", callback_data="menu_beat")],
        [InlineKeyboardButton("🎤 Треки (" + str(tracks) + ")", callback_data="menu_track")],
        [InlineKeyboardButton("🔀 Ремиксы (" + str(remixes) + ")", callback_data="menu_remix")],
        [InlineKeyboardButton("❤️ Избранное", callback_data="my_favorites"),
         InlineKeyboardButton("🔍 Поиск", callback_data="search_prompt")],
        [InlineKeyboardButton("🎲 Случайный бит", callback_data="random_beat")],
    ])

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
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Следующий похожий", callback_data="next_" + str(beat_id))],
        [InlineKeyboardButton("❤️ В избранное", callback_data="fav_" + str(beat_id)),
         InlineKeyboardButton("🎲 Случайный", callback_data="random_beat")],
        [InlineKeyboardButton("◀️ Меню", callback_data=back_map.get(content_type, "main_menu"))],
    ])

def kb_giveaway():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎁 Участвовать!", callback_data="join_giveaway")],
        [InlineKeyboardButton("👥 Сколько участников?", callback_data="giveaway_stats")],
    ])

def kb_repost():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Перейти в канал для репоста", url=CHANNEL_LINK)],
        [InlineKeyboardButton("✅ Я сделал репост!", callback_data="confirm_repost")],
    ])

def kb_admin():
    beats = len([b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == "beat"])
    tracks = len([b for b in beats_db.BEATS_CACHE if b.get("content_type") == "track"])
    remixes = len([b for b in beats_db.BEATS_CACHE if b.get("content_type") == "remix"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика (" + str(len(all_users)) + " польз.)", callback_data="admin_stats")],
        [InlineKeyboardButton("🎹 " + str(beats) + " / 🎤 " + str(tracks) + " / 🔀 " + str(remixes), callback_data="admin_catalog")],
        [InlineKeyboardButton("🎹 Добавить биты", callback_data="admin_addbeats_beat"),
         InlineKeyboardButton("🎤 Добавить треки", callback_data="admin_addbeats_track")],
        [InlineKeyboardButton("🔀 Добавить ремиксы", callback_data="admin_addbeats_remix"),
         InlineKeyboardButton("🗑 Очистить", callback_data="admin_clearbeats")],
        [InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton("🎵 Бит дня сейчас", callback_data="admin_beatofday")],
        [InlineKeyboardButton("🎁 Розыгрыш: " + ("🟢 Активен" if giveaway["active"] else "🔴 Нет"), callback_data="admin_giveaway")],
    ])

def kb_admin_giveaway():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎁 Запустить (/giveaway)", callback_data="admin_giveaway_hint")],
        [InlineKeyboardButton("🏁 Завершить", callback_data="admin_giveaway_end")],
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_giveaway_stats")],
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

    is_new = user_id not in all_users
    if is_new:
        all_users[user_id] = {
            "name": user.full_name,
            "username": user.username or "",
            "joined": datetime.now().strftime("%d.%m.%Y %H:%M")
        }
        asyncio.create_task(asyncio.to_thread(save_users))
        try:
            uname = "@" + user.username if user.username else user.full_name
            await bot.send_message(ADMIN_ID, "🔔 Новый: " + uname + " | Всего: " + str(len(all_users)))
        except Exception:
            pass

    subscribed = await is_subscribed(bot, user_id)
    if not subscribed:
        await update.message.reply_text(WELCOME_TEXT, reply_markup=kb_subscribe())
        return

    if user_id not in users_received_pack:
        await send_sample_pack(bot, user_id)
        users_received_pack.add(user_id)
        asyncio.create_task(asyncio.to_thread(save_users))
        try:
            uname = "@" + user.username if user.username else user.full_name
            await bot.send_message(ADMIN_ID, "🎁 " + uname + " получил пак! Всего: " + str(len(users_received_pack)))
        except Exception:
            pass

    await show_main_menu(bot, user_id)
    if giveaway["active"]:
        await bot.send_message(user_id, "🎁 Идёт розыгрыш! Приз: " + giveaway["prize_name"], reply_markup=kb_giveaway())


# ── /admin ────────────────────────────────────────────────────

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text("🎛 Панель управления:", reply_markup=kb_admin())


# ── /search ───────────────────────────────────────────────────

async def cmd_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = " ".join(context.args) if context.args else ""
    if query:
        await do_search(context.bot, update.effective_chat.id, query, update.effective_user.id)
    else:
        await update.message.reply_text("Напиши: /search keyglock")


# ── /giveaway ─────────────────────────────────────────────────

async def cmd_giveaway(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if giveaway["active"]:
        await update.message.reply_text("Розыгрыш уже идёт!")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Формат: /giveaway 24 Название")
        return
    try:
        hours = int(context.args[0])
        prize_name = " ".join(context.args[1:])
    except ValueError:
        await update.message.reply_text("Ошибка! Формат: /giveaway 24 Название")
        return
    end_time = datetime.now() + timedelta(hours=hours)
    giveaway.update({"active": True, "prize_name": prize_name, "end_time": end_time, "participants": {}, "prize_file": None})
    await update.message.reply_text("Розыгрыш создан!\nПриз: " + prize_name + "\nКонец: " + end_time.strftime("%d.%m.%Y в %H:%M") + "\n\nОтправь мне файл с призом.")
    asyncio.create_task(auto_end_giveaway(context.bot, hours * 3600))


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


# ── Бит дня ───────────────────────────────────────────────────

async def send_beat_of_day(bot):
    pool = [b for b in beats_db.BEATS_CACHE if b.get("content_type", "beat") == "beat"]
    if not pool:
        return 0
    beat = random.choice(pool)
    sent = 0
    for uid in list(all_users.keys()):
        try:
            await bot.send_message(uid, "🎵 Бит дня от IIIPLKIII специально для тебя! Врубай 🔥")
            await send_beat(bot, uid, beat, uid)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    return sent

async def daily_beat_scheduler(bot):
    while True:
        now = datetime.now()
        target = now.replace(hour=18, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        await send_beat_of_day(bot)


# ── Розыгрыш ──────────────────────────────────────────────────

async def auto_end_giveaway(bot, delay):
    await asyncio.sleep(delay)
    if giveaway["active"]:
        await finish_giveaway(bot)

async def finish_giveaway(bot):
    if not giveaway["active"]:
        return
    giveaway["active"] = False
    valid = {uid: p for uid, p in giveaway["participants"].items() if p["reposted"]}
    if not valid:
        await bot.send_message(ADMIN_ID, "Розыгрыш завершён, участников не было.")
        return
    winner_id, winner_data = random.choice(list(valid.items()))
    try:
        await bot.send_message(winner_id, "🏆 ПОЗДРАВЛЯЕМ! Ты выиграл \"" + giveaway["prize_name"] + "\"!\nДержи — заслужил! 🔥")
        if giveaway["prize_file"]:
            await bot.send_document(winner_id, document=giveaway["prize_file"])
    except Exception:
        pass
    for uid in giveaway["participants"]:
        if uid != winner_id:
            try:
                await bot.send_message(uid, "Розыгрыш завершён!\nПобедитель: " + winner_data["name"] + " 🎉\n\nНе расстраивайся — следующий будет твоим! Следи за каналом 👀")
            except Exception:
                pass
    await bot.send_message(ADMIN_ID, "Победитель: " + winner_data["name"] + " (" + winner_data.get("username", "-") + ")\nID: " + str(winner_id))


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

    if data == "check_sub":
        subscribed_users.discard(user_id)
        if await is_subscribed(bot, user_id):
            await query.message.delete()
            if user_id not in users_received_pack:
                await send_sample_pack(bot, user_id)
                users_received_pack.add(user_id)
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

    if data == "search_prompt":
        bulk_add_mode[str(user_id) + "_search"] = True
        await query.message.reply_text("🔍 Напиши название бита, имя артиста или тег — найду всё что есть!")
        return

    if data == "join_giveaway":
        if not giveaway["active"]:
            await query.answer("Розыгрыш завершён!", show_alert=True)
            return
        if not await is_subscribed(bot, user_id):
            await query.answer("Сначала подпишись!", show_alert=True)
            return
        if user_id in giveaway["participants"]:
            if giveaway["participants"][user_id]["reposted"]:
                await query.answer("Ты уже участвуешь!", show_alert=True)
            else:
                await query.message.edit_text("✅ Шаг 1 выполнен!\n\nОсталось сделать репост поста в канале — и ты в игре! 🎯", reply_markup=kb_repost())
            return
        name = query.from_user.full_name
        username = "@" + query.from_user.username if query.from_user.username else "no username"
        giveaway["participants"][user_id] = {"name": name, "username": username, "reposted": False}
        await query.message.edit_text("✅ Шаг 1 выполнен!\n\nОсталось сделать репост поста в канале — и ты в игре! 🎯", reply_markup=kb_repost())
        return

    if data == "confirm_repost":
        if user_id not in giveaway["participants"]:
            await query.answer("Сначала нажми Участвовать!", show_alert=True)
            return
        giveaway["participants"][user_id]["reposted"] = True
        end_str = giveaway["end_time"].strftime("%d.%m.%Y в %H:%M") if giveaway.get("end_time") else "-"
        await query.message.edit_text(
            "🎉 Ты участвуешь!\n\nПриз: " + giveaway["prize_name"] +
            "\nИтоги: " + end_str +
            "\nУчастников: " + str(len(giveaway["participants"])) + "\n\nУдачи! 🍀"
        )
        return

    if data == "giveaway_stats":
        total = len(giveaway["participants"])
        reposted = sum(1 for p in giveaway["participants"].values() if p["reposted"])
        await query.answer("Всего: " + str(total) + "\nС репостом: " + str(reposted), show_alert=True)
        return

    if data == "admin_panel":
        if user_id != ADMIN_ID: return
        await query.message.reply_text("🎛 Панель управления:", reply_markup=kb_admin())
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

    if data in ("admin_addbeats_beat", "admin_addbeats_track", "admin_addbeats_remix"):
        if user_id != ADMIN_ID: return
        mode_type = data.replace("admin_addbeats_", "")
        bulk_add_mode[ADMIN_ID] = mode_type
        icons = {"beat": "🎹 биты", "track": "🎤 треки", "remix": "🔀 ремиксы"}
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

    if data == "admin_broadcast":
        if user_id != ADMIN_ID: return
        bulk_add_mode[str(ADMIN_ID) + "_broadcast"] = True
        await query.message.reply_text("📢 Напиши текст — отправлю всем " + str(len(all_users)) + " пользователям:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_panel")]]))
        return

    if data == "admin_beatofday":
        if user_id != ADMIN_ID: return
        sent = await send_beat_of_day(bot)
        await query.message.reply_text("✅ Бит дня отправлен " + str(sent) + " пользователям!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ В панель", callback_data="admin_panel")]]))
        return

    if data == "admin_giveaway":
        if user_id != ADMIN_ID: return
        await query.message.reply_text("🎁 Розыгрыш:", reply_markup=kb_admin_giveaway())
        return

    if data == "admin_giveaway_hint":
        if user_id != ADMIN_ID: return
        await query.message.reply_text("Напиши: /giveaway 24 Название приза")
        return

    if data == "admin_giveaway_end":
        if user_id != ADMIN_ID: return
        if not giveaway["active"]:
            await query.message.reply_text("Розыгрыш не активен.")
            return
        await finish_giveaway(bot)
        await query.message.reply_text("✅ Розыгрыш завершён!")
        return

    if data == "admin_giveaway_stats":
        if user_id != ADMIN_ID: return
        total = len(giveaway["participants"])
        reposted = sum(1 for p in giveaway["participants"].values() if p["reposted"])
        end = giveaway["end_time"].strftime("%d.%m.%Y %H:%M") if giveaway.get("end_time") else "-"
        await query.message.reply_text(
            "Розыгрыш: " + ("🟢 Активен" if giveaway["active"] else "🔴 Нет") +
            "\nПриз: " + giveaway.get("prize_name", "-") +
            "\nКонец: " + end +
            "\nУчастников: " + str(total) + " / репост: " + str(reposted),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_giveaway")]]))
        return

    # ── Накладная: ещё листы ──────────────────────────────────
    if data == "invoice_more":
        if user_id != ADMIN_ID: return
        accumulated = invoice_pages.get(user_id)
        if not accumulated:
            await query.message.edit_text(
                "Эта накладная уже закрыта (авто-завершение по тишине). "
                "Пришли фото — начну новую."
            )
            return
        _schedule_invoice_autofinish(user_id, query.message.chat_id, context.bot)
        total = len(accumulated.get("items", []))
        await query.message.edit_text(
            f"Ок, жду следующий лист 📸\nУже накоплено товаров: {total}"
        )
        return

    if data == "invoice_done":
        if user_id != ADMIN_ID: return
        _cancel_invoice_autofinish(user_id)
        if user_id not in invoice_pages or not invoice_pages[user_id].get("items"):
            await query.answer("Нет данных!", show_alert=True)
            return
        await _finalize_invoice_for_review(
            user_id, query.message.chat_id, context.bot, auto=False
        )
        # Убираем кнопки с предыдущего статусного сообщения, чтобы их не жали повторно
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if data.startswith("inv_edit_"):
        if user_id != ADMIN_ID: return
        idx = int(data.split("_")[2])
        if user_id not in pending_invoices: return
        item = pending_invoices[user_id]["items"][idx]
        await query.message.reply_text(
            f"Что исправить?\n{item['name'][:50]}\nКол-во: {item['qty']} шт | Цена: {item['price']} р.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Кол-во", callback_data=f"inv_qty_{idx}"),
                 InlineKeyboardButton("Цену", callback_data=f"inv_price_{idx}")],
                [InlineKeyboardButton("Название", callback_data=f"inv_name_{idx}")],
            ])
        )
        return

    if data.startswith("inv_qty_"):
        if user_id != ADMIN_ID: return
        idx = int(data.split("_")[2])
        if user_id not in pending_invoices: return
        item = pending_invoices[user_id]["items"][idx]
        editing_qty[user_id] = ("qty", idx)
        await query.message.edit_text(
            f"Введи новое количество для:\n{item['name'][:50]}\nСейчас: {item['qty']} шт"
        )
        return

    if data.startswith("inv_price_"):
        if user_id != ADMIN_ID: return
        idx = int(data.split("_")[2])
        if user_id not in pending_invoices: return
        item = pending_invoices[user_id]["items"][idx]
        editing_qty[user_id] = ("price", idx)
        await query.message.edit_text(
            f"Введи новую цену для:\n{item['name'][:50]}\nСейчас: {item['price']} р."
        )
        return

    if data.startswith("inv_name_"):
        if user_id != ADMIN_ID: return
        idx = int(data.split("_")[2])
        if user_id not in pending_invoices: return
        item = pending_invoices[user_id]["items"][idx]
        editing_qty[user_id] = ("name", idx)
        await query.message.edit_text(
            f"Введи новое название для:\n{item['name'][:80]}\n\n"
            f"Бот пересчитает матч в Sigma и запомнит коррекцию."
        )
        return

    if data == "invoice_upload":
        if user_id != ADMIN_ID: return
        if user_id not in pending_invoices:
            await query.answer("Нет данных!", show_alert=True)
            return
        invoice = pending_invoices[user_id]
        await query.message.edit_text("⏳ Загружаю в Sigma...")
        try:
            api = _sigma_instance
            result = await api.process_invoice(
                items=invoice["items"],
                supplier_name=invoice["supplier"]
            )
            if result["ok"]:
                del pending_invoices[user_id]
                num = result.get("waybill_number") or ""
                num_line = f" №{num}" if num else ""
                skipped_text = ""
                if result.get("skipped"):
                    skipped_text = f"\n\nНе найдено в базе Sigma:\n" + "\n".join(f"- {s}" for s in result["skipped"])
                await query.message.edit_text(
                    f"✅ Черновик{num_line} создан в Sigma!\n"
                    f"Добавлено товаров: {result.get('added', 0)}"
                    f"{skipped_text}\n\n"
                    f"Зайди в Sigma → Документы → Приходы → проверь и нажми Провести"
                )
            else:
                await query.message.edit_text(f"❌ Ошибка: {result['error']}")
        except Exception as e:
            logger.error(f"Invoice upload error: {e}")
            await query.message.edit_text(f"❌ Ошибка: {str(e)[:200]}")
        return

    if data == "invoice_cancel":
        if user_id != ADMIN_ID: return
        _cancel_invoice_autofinish(user_id)
        pending_invoices.pop(user_id, None)
        invoice_pages.pop(user_id, None)
        editing_qty.pop(user_id, None)
        await query.message.edit_text("Накладная отменена.")
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

    broadcast_key = str(ADMIN_ID) + "_broadcast"
    if broadcast_key in bulk_add_mode:
        del bulk_add_mode[broadcast_key]
        if not text:
            return
        sent = 0
        for uid in list(all_users.keys()):
            try:
                await bot.send_message(uid, text)
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                pass
        await message.reply_text("✅ Рассылка улетела! Отправлено: " + str(sent) + " чел.")
        return

    if ADMIN_ID not in bulk_add_mode or bulk_add_mode.get(ADMIN_ID) not in ("beat", "track", "remix"):
        if message.document and not text:
            fid = message.document.file_id
            await message.reply_text("FILE_ID:\n\n" + fid + "\n\nВставь в config.py:\nSAMPLE_PACK_FILE_ID = " + chr(34) + fid + chr(34))
            if giveaway["active"] and giveaway["prize_file"] is None:
                giveaway["prize_file"] = fid
                await message.reply_text("Файл сохранён как приз!")
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


# ── Sigma Invoice Handler ─────────────────────────────────────

pending_invoices = {}
invoice_pages = {}       # накапливаем страницы до нажатия "это всё"
editing_qty = {}         # {user_id: item_index} — ждём новое кол-во от пользователя
invoice_autofinish_tasks = {}  # {user_id: asyncio.Task} — автозавершение по тишине
INVOICE_AUTOFINISH_SECONDS = 20  # сколько ждать новой страницы перед автозакрытием

def kb_invoice_more_pages():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📄 Да, ещё лист", callback_data="invoice_more")],
        [InlineKeyboardButton("✅ Нет, это всё", callback_data="invoice_done")],
    ])

def kb_invoice_edit(items):
    """Кнопки для редактирования каждого товара + загрузка"""
    rows = []
    for i, item in enumerate(items):
        name_short = item["name"][:22]
        rows.append([InlineKeyboardButton(
            f"✏️ {i+1}. {name_short} | {item['qty']}шт x {item['price']}р",
            callback_data=f"inv_edit_{i}"
        )])
    rows.append([InlineKeyboardButton("🚀 Загрузить в Sigma", callback_data="invoice_upload")])
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="invoice_cancel")])
    return InlineKeyboardMarkup(rows)

def format_invoice_final(supplier, items):
    lines = ["📋 Накладная — проверь и исправь если надо\n"]
    lines.append(f"🏭 {supplier}")
    lines.append(f"📦 Товаров: {len(items)}\n")
    for i, item in enumerate(items, 1):
        markup = sigma_api.get_markup(item["name"])
        sell = sigma_api.calc_price(item["price"], markup)
        pct = int(markup * 100)
        lines.append(
            f"{i}. {item['name'][:40]}\n"
            f"   {item['qty']} шт x {item['price']} р. → продажа {sell} р. (+{pct}%)"
        )
    lines.append("\nНажми на товар чтобы исправить кол-во или цену")
    return "\n".join(lines)

async def _finalize_invoice_for_review(user_id: int, chat_id: int, bot, auto: bool):
    """Перевод накладной из накопления в режим финального review.
    Вызывается и руками ('Нет, это всё'), и автоматически по таймауту."""
    accumulated = invoice_pages.pop(user_id, None)
    if not accumulated or not accumulated.get("items"):
        return False

    items, recalc_info = sigma_api.process_weight_products(accumulated["items"])
    pending_invoices[user_id] = {
        "supplier": accumulated["supplier"],
        "items": items,
    }
    invoice = pending_invoices[user_id]
    text = format_invoice_final(invoice["supplier"], invoice["items"])
    if auto:
        n_pages = accumulated.get("page_count", 1)
        pages_word = "лист" if n_pages == 1 else "листа" if 2 <= n_pages <= 4 else "листов"
        text = (
            f"⏱ Жду новые страницы {INVOICE_AUTOFINISH_SECONDS} сек — тишина, "
            f"считаю что накладная кончилась ({n_pages} {pages_word}).\n\n"
            + text
        )
    if recalc_info:
        text += "\n\n⚖️ Пересчитано как весовой товар:\n" + "\n".join(recalc_info)
    await bot.send_message(chat_id, text, reply_markup=kb_invoice_edit(invoice["items"]))
    return True


async def _autofinish_invoice_after_delay(user_id: int, chat_id: int, bot):
    try:
        await asyncio.sleep(INVOICE_AUTOFINISH_SECONDS)
    except asyncio.CancelledError:
        return
    invoice_autofinish_tasks.pop(user_id, None)
    # Гонка: если пользователь уже нажал кнопку вручную — invoice_pages уже пуст, no-op.
    if user_id in pending_invoices:
        return
    try:
        await _finalize_invoice_for_review(user_id, chat_id, bot, auto=True)
    except Exception as e:
        logger.error(f"autofinish invoice failed: {e}")


def _schedule_invoice_autofinish(user_id: int, chat_id: int, bot):
    old = invoice_autofinish_tasks.pop(user_id, None)
    if old and not old.done():
        old.cancel()
    invoice_autofinish_tasks[user_id] = asyncio.create_task(
        _autofinish_invoice_after_delay(user_id, chat_id, bot)
    )


def _cancel_invoice_autofinish(user_id: int):
    task = invoice_autofinish_tasks.pop(user_id, None)
    if task and not task.done():
        task.cancel()


async def handle_invoice_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle photo of invoice - supports multi-page"""
    if not update.effective_user: return
    user_id = update.effective_user.id
    if user_id != ADMIN_ID: return
    if not update.message.photo: return

    msg = await update.message.reply_text("Читаю лист накладной...")
    try:
        photo = update.message.photo[-1]
        file = await context.bot.get_file(photo.file_id)
        img_bytes = bytes(await file.download_as_bytearray())

        result = await sigma_api.recognize_invoice(img_bytes)
        supplier = result.get("supplier", "Не определён")
        items = result.get("items", [])

        if not items:
            await msg.edit_text("Не нашёл товаров на этом листе. Попробуй другое фото.")
            return

        # Накапливаем страницы
        if user_id not in invoice_pages:
            invoice_pages[user_id] = {"supplier": supplier, "items": [], "page_count": 0}
        invoice_pages[user_id]["items"].extend(items)
        invoice_pages[user_id]["page_count"] = invoice_pages[user_id].get("page_count", 0) + 1
        if not invoice_pages[user_id]["supplier"] or invoice_pages[user_id]["supplier"] == "Не определён":
            invoice_pages[user_id]["supplier"] = supplier

        page_num = invoice_pages[user_id]["page_count"]
        total_so_far = len(invoice_pages[user_id]["items"])
        lines = [f"📋 Лист {page_num} распознан! Товаров на листе: {len(items)}\n"]
        uncertain_indices = []
        computed_total = 0.0
        for i, item in enumerate(items, 1):
            mark = "⚠️ " if item.get("uncertain") else ""
            try:
                qty_f = float(item.get("qty") or 0)
            except (TypeError, ValueError):
                qty_f = 0.0
            try:
                price_f = float(item.get("price") or 0)
            except (TypeError, ValueError):
                price_f = 0.0
            line_sum = qty_f * price_f
            computed_total += line_sum
            qty_disp = int(qty_f) if qty_f.is_integer() else qty_f
            price_disp = f"{price_f:.2f}".rstrip("0").rstrip(".")
            sum_disp = f"{line_sum:,.2f}".rstrip("0").rstrip(".").replace(",", " ")
            lines.append(f"{mark}{i}. {item['name']}")
            lines.append(f"   {qty_disp} × {price_disp} ₽ = {sum_disp} ₽")
            if item.get("uncertain"):
                uncertain_indices.append(i)

        # Итог по листу + сверка с напечатанным на накладной
        printed = result.get("total_sum")
        computed_disp = f"{round(computed_total, 2):,.2f}".rstrip("0").rstrip(".").replace(",", " ")
        lines.append(f"\n💰 Итого по листу: {computed_disp} ₽")
        if isinstance(printed, (int, float)) and printed > 0:
            printed_disp = f"{float(printed):,.2f}".rstrip("0").rstrip(".").replace(",", " ")
            delta = round(computed_total - float(printed), 2)
            if abs(delta) <= 1.0:
                lines.append(f"   В накладной:    {printed_disp} ₽ ✓")
            else:
                sign = "+" if delta > 0 else "−"
                lines.append(f"   В накладной:    {printed_disp} ₽  ⚠️ расхождение {sign}{abs(delta):.2f} ₽")

        if total_so_far > len(items):
            lines.append(f"\nВсего накоплено: {total_so_far} товаров")
        # Предупреждение о неуверенных позициях
        if uncertain_indices:
            idx_str = ", ".join(str(i) for i in uncertain_indices)
            lines.append(
                f"\n⚠️ Проверь глазами позиции: {idx_str}\n"
                f"   (в этих строках я не уверен — мог неправильно "
                f"прочитать цену, количество или название)"
            )
        lines.append(
            f"\nЕщё листы есть? Если тишина {INVOICE_AUTOFINISH_SECONDS} сек — "
            f"считаю накладную завершённой автоматически."
        )
        await msg.edit_text("\n".join(lines), reply_markup=kb_invoice_more_pages())

        # Запускаем/обновляем таймер авто-завершения
        _schedule_invoice_autofinish(user_id, update.effective_chat.id, context.bot)

    except Exception as e:
        logger.error(f"Invoice error: {e}")
        err_text = str(e)
        if "слишком мало текста" in err_text:
            await msg.edit_text(
                "📷 Фото нечёткое — OCR почти ничего не разобрал.\n"
                "Попробуй переснять при хорошем освещении, держи камеру ровно, "
                "чтобы текст был крупным и в фокусе."
            )
        elif "rate quota limit exceed" in err_text:
            await msg.edit_text("⏳ Yandex OCR занят. Подожди 2 секунды и пришли фото снова.")
        else:
            await msg.edit_text(f"Ошибка распознавания: {err_text[:200]}")

async def handle_invoice_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handle 'ok' confirmation to upload to sigma"""
    if not update.effective_user: return False
    user_id = update.effective_user.id
    if user_id != ADMIN_ID: return False

    text = (update.message.text or "").strip().lower()
    if text not in ["ок", "ok", "окей", "да", "загрузить", "загружай"]: return False
    if user_id not in pending_invoices: return False

    invoice = pending_invoices[user_id]
    msg = await update.message.reply_text("⏳ Загружаю в Sigma...")

    try:
        api = _sigma_instance
        result = await api.process_invoice(
            items=invoice["items"],
            supplier_name=invoice["supplier"]
        )
        if result["ok"]:
            del pending_invoices[user_id]
            num = result.get("waybill_number") or ""
            num_line = f" №{num}" if num else ""
            skipped_text = ""
            if result.get("skipped"):
                skipped_text = f"\n⚠️ Не найдено в базе Sigma: {', '.join(result['skipped'])}"
            await msg.edit_text(
                f"✅ Готово! Приход{num_line} создан в Sigma\n"
                f"Добавлено товаров: {result.get('added', 0)}"
                f"{skipped_text}"
            )
        else:
            await msg.edit_text(f"❌ Ошибка: {result['error']}")
    except Exception as e:
        logger.error(f"Invoice confirm error: {e}")
        await msg.edit_text(f"❌ Ошибка: {str(e)[:200]}")
    return True


# ── AI Assistant ──────────────────────────────────────────────

async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user: return
    if update.effective_user.id != ADMIN_ID: return
    voice = update.message.voice or update.message.audio
    if not voice: return
    thinking = await update.message.reply_text("Слушаю...")
    try:
        file = await context.bot.get_file(voice.file_id)
        file_bytes = bytes(await file.download_as_bytearray())
        text = await assistant_ai.transcribe_voice(file_bytes, "voice.ogg")
        if not text.strip():
            await thinking.edit_text("Не расслышал, попробуй ещё раз")
            return
        await thinking.edit_text("Слышу: " + text)
        result = await process_ai(update.effective_user.id, text)
        await thinking.edit_text(result)
    except Exception as e:
        logger.error("Voice error: " + str(e))
        await thinking.edit_text("Ошибка голосового. Попробуй текстом.")

async def handle_assistant(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message: return
    user_id = update.effective_user.id
    if user_id != ADMIN_ID: return
    if ADMIN_ID in bulk_add_mode and bulk_add_mode.get(ADMIN_ID) in ("beat","track","remix"): return
    if str(ADMIN_ID) + "_broadcast" in bulk_add_mode: return
    text = update.message.text or ""
    if not text.strip() or text.startswith("/"): return

    # Редактирование количества, цены или названия товара в накладной
    if user_id in editing_qty and user_id in pending_invoices:
        field, idx = editing_qty[user_id]
        invoice = pending_invoices[user_id]
        if field == "name":
            new_name = text.strip()
            if not new_name:
                await update.message.reply_text("Название не может быть пустым")
                return
            editing_qty.pop(user_id, None)
            old_name = invoice["items"][idx]["name"]
            invoice["items"][idx]["name"] = new_name
            # Перепоиск в Sigma и запись корректировки в wiki
            try:
                api = _sigma_instance
                if not api.token:
                    await api.login()
                product = await api.find_product(new_name)
                if product:
                    sigma_name = product.get("name", "")
                    sigma_id = product.get("id")
                    # Записываем две коррекции: old_name → sigma_name (пользователь подтвердил)
                    # и new_name → sigma_name (чтобы в следующий раз отрабатывало имплицитно)
                    try:
                        sigma_api.wiki_store.record_correction(
                            ocr_name=old_name, sigma_name=sigma_name,
                            sigma_id=sigma_id, confidence=0.9, source="user_correction",
                        )
                        sigma_api.wiki_store.record_correction(
                            ocr_name=new_name, sigma_name=sigma_name,
                            sigma_id=sigma_id, confidence=0.9, source="user_correction",
                        )
                        sigma_api.wiki_store.flush_pending()
                    except Exception as e:
                        logger.warning(f"wiki correction failed: {e}")
                    await update.message.reply_text(
                        f"✅ Найдено в Sigma: {sigma_name[:60]}\nКоррекция сохранена в wiki."
                    )
                else:
                    await update.message.reply_text(
                        f"⚠️ В Sigma не найдено: '{new_name[:50]}'\n"
                        f"При загрузке накладной будет создан новый товар."
                    )
            except Exception as e:
                await update.message.reply_text(f"Ошибка поиска в Sigma: {str(e)[:100]}")
            text_summary = format_invoice_final(invoice["supplier"], invoice["items"])
            await update.message.reply_text(text_summary, reply_markup=kb_invoice_edit(invoice["items"]))
            return
        try:
            new_val = float(text.strip().replace(",", "."))
            editing_qty.pop(user_id, None)
            invoice["items"][idx][field] = new_val
            item_name = invoice["items"][idx]["name"][:35]
            label = "кол-во" if field == "qty" else "цена"
            await update.message.reply_text(f"✅ {item_name}\n{label} обновлено: {new_val}")
            text_summary = format_invoice_final(invoice["supplier"], invoice["items"])
            await update.message.reply_text(text_summary, reply_markup=kb_invoice_edit(invoice["items"]))
        except ValueError:
            await update.message.reply_text("Введи число, например: 5 или 37.50")
        return

    if await handle_invoice_confirm(update, context): return
    msg = update.message
    is_forwarded = (msg.audio or msg.forward_origin or
        getattr(msg, 'forward_from', None) or
        getattr(msg, 'forward_from_chat', None) or
        getattr(msg, 'forward_date', None) or
        getattr(msg, 'forward_sender_name', None))
    if is_forwarded: return
    thinking = await update.message.reply_text("...")
    try:
        result = await process_ai(user_id, text)
        await thinking.edit_text(result)
    except Exception as e:
        await thinking.edit_text("Ошибка: " + str(e))

async def process_ai(user_id, text):
    if user_id not in conversation_history:
        conversation_history[user_id] = []
    history = conversation_history[user_id]
    parsed = await assistant_ai.process_message(text, history)
    history.append({"role": "user", "content": text})
    history.append({"role": "assistant", "content": parsed.get("response", "")})
    if len(history) > 20:
        conversation_history[user_id] = history[-20:]
    return parsed.get("response", "Готово")

async def cmd_wiki(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /wiki stats  — сколько записей в каждом топике
    /wiki review — список невалидированных product_mappings
    /wiki push   — commit + push wiki/ в git (персистентность на Render)
    """
    if not update.effective_user: return
    if update.effective_user.id != ADMIN_ID: return
    args = context.args or []
    subcmd = args[0].lower() if args else "stats"
    ws = sigma_api.wiki_store

    if subcmd == "stats":
        stats = ws.get_stats()
        lines = ["📚 Wiki статистика\n"]
        for name, count in stats["topics"].items():
            lines.append(f"• {name}: {count}")
        lines.append(f"\nPending (не сброшено): {stats['pending']}")
        lines.append(f"Changelog записей: {stats['changelog_entries']}")
        lines.append(f"Последнее обновление: {stats['last_updated']}")
        await update.message.reply_text("\n".join(lines))
        return

    if subcmd == "review":
        items = ws.get_unverified_mappings(limit=20)
        if not items:
            await update.message.reply_text("Нет невалидированных маппингов.")
            return
        lines = ["⚠️ Невалидированные product_mappings:\n"]
        for e in items:
            v = e.get("value", {})
            lines.append(
                f"• {e['key'][:35]} → {v.get('sigma_name', '')[:35]}\n"
                f"  conf={v.get('confidence', 0):.2f} use={v.get('use_count', 0)}"
            )
        await update.message.reply_text("\n".join(lines)[:4000])
        return

    if subcmd == "push":
        import subprocess
        try:
            subprocess.run(["git", "add", "wiki/"], cwd=os.path.dirname(os.path.abspath(__file__)), check=True)
            result = subprocess.run(
                ["git", "commit", "-m", "wiki: update from bot corrections"],
                cwd=os.path.dirname(os.path.abspath(__file__)),
                capture_output=True, text=True,
            )
            if result.returncode != 0 and "nothing to commit" in (result.stdout + result.stderr):
                await update.message.reply_text("Нечего коммитить — wiki актуальна.")
                return
            subprocess.run(["git", "push"], cwd=os.path.dirname(os.path.abspath(__file__)), check=True)
            await update.message.reply_text("✅ Wiki запушена в git.")
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка push: {str(e)[:300]}")
        return

    await update.message.reply_text(
        "Подкоманды: /wiki stats | /wiki review | /wiki push"
    )


async def cmd_my(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user: return
    if update.effective_user.id != ADMIN_ID: return
    await update.message.reply_text(assistant_ai.get_summary())


# ── Запуск ────────────────────────────────────────────────────

async def heartbeat_scheduler():
    while True:
        write_heartbeat()
        await asyncio.sleep(30)

async def load_sigma_suppliers():
    """Загружаем поставщиков из Sigma и передаём в assistant_ai"""
    try:
        api = _sigma_instance
        if await api.login():
            suppliers = await api.get_suppliers()
            names = [s.get("name", "") for s in suppliers if s.get("name")]
            assistant_ai.set_suppliers(names)
            logger.info(f"Sigma suppliers loaded: {len(names)}")
    except Exception as e:
        logger.warning(f"Could not load Sigma suppliers: {e}")

async def sigma_suppliers_scheduler():
    """Обновляем список поставщиков раз в час"""
    while True:
        await asyncio.sleep(3600)
        await load_sigma_suppliers()

async def post_init(application):
    beats_db.load_beats()
    load_users()
    logger.info("Bot started: " + str(len(beats_db.BEATS_CACHE)) + " beats, " + str(len(all_users)) + " users")
    asyncio.create_task(daily_beat_scheduler(application.bot))
    asyncio.create_task(heartbeat_scheduler())
    asyncio.create_task(sigma_suppliers_scheduler())
    asyncio.create_task(load_sigma_suppliers())
    asyncio.create_task(load_sigma_products())
    write_heartbeat()


async def load_sigma_products():
    """Загрузить все товары из Sigma в глобальный кэш при старте"""
    try:
        api = _sigma_instance
        if not await api.login():
            logger.warning("Could not load Sigma products: login failed")
            return
        await api.get_company_info()
        count = await api.load_all_products()
        logger.info(f"Sigma products loaded at startup: {count}")
    except Exception as e:
        logger.error(f"Error loading Sigma products: {e}")


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


async def reminder_scheduler(bot, admin_id):
    while True:
        try:
            data = assistant_ai.load_data()
            now = datetime.now().strftime("%Y-%m-%dT%H:%M")
            changed = False
            for r in data.get("reminders", []):
                if not r.get("done") and r.get("datetime", "") <= now:
                    try:
                        await bot.send_message(admin_id, f"⏰ Напоминание: {r['text']}")
                        r["done"] = True
                        changed = True
                    except Exception:
                        pass
            if changed:
                assistant_ai.save_data(data)
        except Exception as e:
            logger.error("Reminder error: " + str(e))
        await asyncio.sleep(60)

async def wiki_autopush_scheduler():
    """Раз в сутки в 04:00 флашит pending и пушит wiki/ в git.
    Один редеплой Render в тихий час вместо постоянных пушей в течение дня."""
    import subprocess
    cwd = os.path.dirname(os.path.abspath(__file__))
    while True:
        try:
            now = datetime.now()
            target = now.replace(hour=4, minute=0, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            await asyncio.sleep((target - now).total_seconds())

            # 1) Сброс накопленных коррекций в product_mappings.json
            try:
                sigma_api.wiki_store.flush_pending()
            except Exception as e:
                logger.error("wiki autopush flush error: " + str(e))

            # 2) git add wiki/ + commit + push
            subprocess.run(["git", "add", "wiki/"], cwd=cwd, check=True)
            result = subprocess.run(
                ["git", "commit", "-m", "wiki: daily autopush"],
                cwd=cwd, capture_output=True, text=True,
            )
            if result.returncode != 0 and "nothing to commit" in (result.stdout + result.stderr):
                logger.info("wiki autopush: nothing to commit")
                continue
            subprocess.run(["git", "push"], cwd=cwd, check=True)
            logger.info("wiki autopush: pushed to git")
        except Exception as e:
            logger.error("wiki autopush error: " + str(e))
            await asyncio.sleep(3600)


async def daily_report_scheduler(bot, admin_id):
    while True:
        try:
            now = datetime.now()
            target = now.replace(hour=12, minute=0, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            wait_seconds = (target - now).total_seconds()
            await asyncio.sleep(wait_seconds)
            data = assistant_ai.load_data()
            report = assistant_ai.get_daily_report(data)
            await bot.send_message(admin_id, report)
        except Exception as e:
            logger.error("Daily report error: " + str(e))
            await asyncio.sleep(3600)


async def run_bot():
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("search", cmd_search))
    app.add_handler(CommandHandler("giveaway", cmd_giveaway))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(CommandHandler("my", cmd_my))
    app.add_handler(CommandHandler("wiki", cmd_wiki))
    app.add_handler(MessageHandler(filters.PHOTO, handle_invoice_photo))
    app.add_handler(MessageHandler(filters.VOICE | filters.AUDIO, handle_voice))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_assistant))
    app.add_handler(MessageHandler(filters.ALL, handle_message))
    logger.info("Starting bot...")
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    asyncio.create_task(reminder_scheduler(app.bot, ADMIN_ID))
    asyncio.create_task(daily_report_scheduler(app.bot, ADMIN_ID))
    asyncio.create_task(wiki_autopush_scheduler())
    await asyncio.Event().wait()


def main():
    t = threading.Thread(target=run_health_server, daemon=True)
    t.start()
    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
