import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

# Telegram
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "").strip()
SAMPLE_PACK_PATH = os.getenv("SAMPLE_PACK_PATH", "")
SAMPLE_PACK_FILE_ID = os.getenv("SAMPLE_PACK_FILE_ID", "")
WELCOME_TEXT = os.getenv("WELCOME_TEXT", "Привет!")
CATALOG_INTRO = os.getenv("CATALOG_INTRO", "Каталог:")


# ── Настройки, которые раньше были хардкодом ──────────────────
# Меняются через env без релиза — например через Render dashboard.
# Дефолты соответствуют текущему поведению бота.

# Час автопостинга в канал по МСК
CHANNEL_POST_HOUR = int(os.getenv("CHANNEL_POST_HOUR", "16"))

# Лимит размера zip-архива продукта (drum kit / sample pack / loop pack).
# 50 MB — ограничение Bot API для send_document. Можно уменьшить, но
# не увеличить выше 50 без перехода на Local Bot API Server.
PRODUCT_MAX_SIZE_BYTES = int(os.getenv("PRODUCT_MAX_SIZE_BYTES", str(50 * 1024 * 1024)))

# Длительность YT Shorts-версии видео (секунды).
# Research топ-Shorts (2026-04): для нового канала 90 подписчиков —
# completion rate важнее чем full-track showcase. 30 сек оптимум:
# юзер слышит хук → если интересно, идёт по ссылке на Long video.
# Long YT — это где воронка завершается в покупку через бот.
SHORTS_DURATION_SEC = int(os.getenv("SHORTS_DURATION_SEC", "30"))

# Offset (секунды от начала mp3) с которого начинается Shorts-нарезка.
# В hard trap 140-180 BPM drop обычно после 16-го такта = ~25-30 сек.
# Берём с 30 → попадаем в drop = самая яркая часть бита, hook'ает
# зрителя в первые 1-2 сек просмотра (критично для алгоритма Shorts).
# Fallback если mp3 короче offset+duration: shorts_builder возьмёт с 0.
SHORTS_OFFSET_SEC = int(os.getenv("SHORTS_OFFSET_SEC", "30"))

# Оптимальные слоты для автопубликации битов в формате
# "wday:hour:minute,wday:hour:minute,...". По умолчанию — Fri 21:30 + Mon 21:00
# МСК (из исследования лучших окон для type-beat каналов).
def _parse_slots(raw: str) -> list[tuple[int, int, int]]:
    slots = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            w, h, m = (int(x) for x in part.split(":"))
            slots.append((w, h, m))
        except Exception:
            continue
    return slots


PUBLISH_OPTIMAL_SLOTS = _parse_slots(
    os.getenv("PUBLISH_OPTIMAL_SLOTS", "4:21:30,0:21:0")
) or [(4, 21, 30), (0, 21, 0)]

# YooKassa provider_token для Telegram sendInvoice (RUB оплата MIR/СБП/карты).
# Получается в BotFather → /mybots → Payments → YooKassa → Connect.
# Если пустой — RUB-кнопка автоматически скрывается в preview битов.
YOOKASSA_PROVIDER_TOKEN = os.getenv("YOOKASSA_PROVIDER_TOKEN", "").strip()
