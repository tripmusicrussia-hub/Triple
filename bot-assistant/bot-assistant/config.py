import os

# Telegram
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
CHANNEL_ID = os.getenv("CHANNEL_ID", "")
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "")
SAMPLE_PACK_PATH = os.getenv("SAMPLE_PACK_PATH", "")
SAMPLE_PACK_FILE_ID = os.getenv("SAMPLE_PACK_FILE_ID", "")
WELCOME_TEXT = os.getenv("WELCOME_TEXT", "Привет!")
CATALOG_INTRO = os.getenv("CATALOG_INTRO", "Каталог:")
