"""
Скрипт для импорта всех постов из канала в базу битов.
Запускать один раз: python import_channel.py
"""
import asyncio
import os
import sys
from telegram import Bot
from telegram.error import TelegramError

# Настройки
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL = "@iiiplkiii"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import beats_db

def detect_content_type(text):
    if not text:
        return "beat"
    t = text.lower()
    if any(w in t for w in ["ремикс", "remix", "rmx"]):
        return "remix"
    if any(w in t for w in ["трек", "track", "песня", "song", "релиз", "release"]):
        return "track"
    return "beat"

def parse_beat(text, msg_id, audio=None, voice=None):
    tags = beats_db.parse_tags_from_text(text) if text else []
    bpm = beats_db.parse_bpm_from_text(text) if text else 0
    key = beats_db.parse_key_from_text(text) if text else "-"
    lines = [l.strip() for l in (text or "").strip().split("\n")
             if l.strip() and not l.strip().startswith("#") and not l.strip().startswith("@")]
    name = lines[0][:60] if lines else f"Beat #{msg_id}"
    channel_username = CHANNEL.lstrip("@")
    post_url = f"https://t.me/{channel_username}/{msg_id}"
    beat_id = abs(hash(channel_username + str(msg_id))) % 10000000
    beat = {
        "id": beat_id,
        "msg_id": msg_id,
        "name": name,
        "tags": tags,
        "post_url": post_url,
        "bpm": bpm or 0,
        "key": key or "-",
        "file_id": "",
        "content_type": detect_content_type(text),
    }
    if audio:
        beat["file_id"] = audio.file_id
        beat["file_unique_id"] = audio.file_unique_id
    elif voice:
        beat["file_id"] = voice.file_id
    return beat

async def import_all():
    if not BOT_TOKEN:
        print("Ошибка: BOT_TOKEN не задан")
        return

    beats_db.load_beats()
    print(f"Текущая база: {len(beats_db.BEATS_CACHE)} постов")

    bot = Bot(token=BOT_TOKEN)
    added = 0
    skipped = 0
    errors = 0

    print(f"Читаю канал {CHANNEL}...")

    try:
        # Получаем последнее сообщение чтобы узнать max ID
        chat = await bot.get_chat(CHANNEL)
        print(f"Канал найден: {chat.title}")
    except TelegramError as e:
        print(f"Ошибка доступа к каналу: {e}")
        return

    # Перебираем сообщения с конца к началу
    # Начинаем с большого числа и идём вниз пока не закончатся
    msg_id = 1
    max_id = 500  # пробуем до 500, если больше - увеличь

    for msg_id in range(1, max_id + 1):
        try:
            msg = await bot.forward_message(
                chat_id=chat.id,
                from_chat_id=CHANNEL,
                message_id=msg_id,
                disable_notification=True
            )
            # Удаляем пересланное сообщение
            try:
                await bot.delete_message(chat_id=chat.id, message_id=msg.message_id)
            except Exception:
                pass

            text = msg.text or msg.caption or ""
            if not text.strip() and not msg.audio and not msg.voice:
                continue

            if not text.strip():
                if msg.audio:
                    text = msg.audio.file_name or f"Beat #{msg_id}"
                elif msg.voice:
                    text = f"Voice #{msg_id}"

            beat = parse_beat(text, msg_id, msg.audio, msg.voice)

            # Проверка дублей
            if beat["id"] in beats_db.BEATS_BY_ID:
                skipped += 1
                continue

            beats_db.BEATS_CACHE.append(beat)
            beats_db.BEATS_BY_ID[beat["id"]] = beat
            added += 1

            if added % 10 == 0:
                beats_db.save_beats()
                print(f"  Добавлено: {added}, пропущено: {skipped}")

            await asyncio.sleep(0.1)

        except TelegramError as e:
            if "Message to forward not found" in str(e):
                continue  # пост удалён или не существует
            elif "Too Many Requests" in str(e):
                print("  Лимит запросов, жду 5 секунд...")
                await asyncio.sleep(5)
            else:
                errors += 1

    beats_db.save_beats()
    print(f"\nГотово!")
    print(f"✅ Добавлено: {added}")
    print(f"⚠️ Пропущено дублей: {skipped}")
    print(f"❌ Ошибок: {errors}")
    print(f"📊 Всего в базе: {len(beats_db.BEATS_CACHE)}")

if __name__ == "__main__":
    asyncio.run(import_all())
