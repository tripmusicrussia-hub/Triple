"""
Импорт постов из канала в базу.
Пересылает в личку администратора (не в канал), читает и удаляет.
"""
import asyncio
import os
import sys
import re
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

from telegram import Bot
from telegram.error import TelegramError

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL = "@iiiplfiii"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import beats_db

try:
    from config import ADMIN_ID
except Exception:
    ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

def detect_content_type(text):
    if not text:
        return "beat"
    t = text.lower()
    if any(w in t for w in ["ремикс", "remix", "rmx"]):
        return "remix"
    if any(w in t for w in ["трек", "track", "песня", "song", "релиз", "release"]):
        return "track"
    return "beat"

def try_add(beat):
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

async def import_all():
    if not BOT_TOKEN or not ADMIN_ID:
        print("Ошибка: BOT_TOKEN или ADMIN_ID не задан")
        return

    beats_db.load_beats()
    print(f"База: {len(beats_db.BEATS_CACHE)} постов")
    print(f"Читаю {CHANNEL}, пересылаю в личку администратора...")

    bot = Bot(token=BOT_TOKEN)
    channel_username = CHANNEL.lstrip("@")
    added = skipped = 0

    for msg_id in range(1, 601):
        try:
            fwd = await bot.forward_message(
                chat_id=ADMIN_ID,
                from_chat_id=CHANNEL,
                message_id=msg_id,
                disable_notification=True
            )

            text = fwd.text or fwd.caption or ""

            if not text.strip() and not fwd.audio and not fwd.voice:
                try:
                    await bot.delete_message(chat_id=ADMIN_ID, message_id=fwd.message_id)
                except Exception:
                    pass
                await asyncio.sleep(0.15)
                continue

            if not text.strip():
                text = fwd.audio.file_name if fwd.audio else f"Beat #{msg_id}"

            lines = [l.strip() for l in text.strip().split("\n")
                     if l.strip() and not l.strip().startswith("#") and not l.strip().startswith("@")]
            name = lines[0][:60] if lines else f"Beat #{msg_id}"
            beat_id = abs(hash(channel_username + str(msg_id))) % 10000000

            beat = {
                "id": beat_id,
                "msg_id": msg_id,
                "name": name,
                "tags": beats_db.parse_tags_from_text(text),
                "post_url": f"https://t.me/{channel_username}/{msg_id}",
                "bpm": beats_db.parse_bpm_from_text(text) or 0,
                "key": beats_db.parse_key_from_text(text) or "-",
                "file_id": "",
                "content_type": detect_content_type(text),
            }
            if fwd.audio:
                beat["file_id"] = fwd.audio.file_id
                beat["file_unique_id"] = fwd.audio.file_unique_id
            elif fwd.voice:
                beat["file_id"] = fwd.voice.file_id

            try:
                await bot.delete_message(chat_id=ADMIN_ID, message_id=fwd.message_id)
            except Exception:
                pass

            if try_add(beat):
                added += 1
                if added % 20 == 0:
                    beats_db.save_beats()
                    print(f"  Добавлено: {added}")
            else:
                skipped += 1

            await asyncio.sleep(0.15)

        except TelegramError as e:
            err = str(e)
            if "not found" in err.lower() or "invalid" in err.lower() or "MESSAGE_ID_INVALID" in err:
                await asyncio.sleep(0.1)
                continue
            elif "Too Many Requests" in err:
                m = re.search(r'retry after (\d+)', err)
                wait = int(m.group(1)) if m else 5
                print(f"  Лимит, жду {wait}с...")
                await asyncio.sleep(wait)
            else:
                await asyncio.sleep(0.1)
                continue

    beats_db.save_beats()
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print(f"\nГотово. Добавлено: {added}, пропущено дублей: {skipped}")
    print(f"Всего в базе: {len(beats_db.BEATS_CACHE)}")

if __name__ == "__main__":
    asyncio.run(import_all())
