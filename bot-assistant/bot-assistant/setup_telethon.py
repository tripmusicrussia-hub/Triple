"""One-time локальный script для получения Telethon StringSession.

Запускается ОДИН РАЗ на машине администратора:
    python setup_telethon.py

Что делает:
1. Спрашивает API_ID и API_HASH (получи на https://my.telegram.org/apps)
2. Спрашивает номер телефона
3. Telegram присылает SMS-код, ты его вводишь
4. Если включена 2FA — спросит cloud password
5. Печатает StringSession (длинная base64 строка)

Дальше:
- Скопируй session string
- Поставь в Render env переменную `TELETHON_SESSION_STRING`
- Также добавь `TELETHON_API_ID` и `TELETHON_API_HASH`
- Render автодеплой подхватит → бот сможет удалять старые TG посты
  через user-account API (без 48h лимита Bot API)

Зачем нужно:
- Bot API позволяет боту удалять только свои сообщения и только в
  пределах 48 часов с момента публикации
- Telethon работает от имени **user account** — может удалять любые
  свои сообщения (без временных лимитов)
- Используется в auto-repost flow для удаления legacy-постов

⚠️ Безопасность:
- StringSession даёт **полный доступ** к твоему TG аккаунту через API
- Никогда не коммить session string в git
- Только в Render env (защищён 2FA, который ты уже включил)
- Если когда-нибудь скомпрометирован — Telegram → Settings → Devices → Logout
"""
from __future__ import annotations

import sys

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except ImportError:
    print("Telethon не установлен. Запусти: pip install Telethon==1.42.0")
    sys.exit(1)


def main():
    print("=" * 60)
    print("Telethon StringSession setup для Triple Bot")
    print("=" * 60)
    print()
    print("Шаг 1: получи API credentials на https://my.telegram.org/apps")
    print("(один раз, бесплатно — login твоим phone, потом 'API development tools')")
    print()
    api_id = input("Введи api_id: ").strip()
    api_hash = input("Введи api_hash: ").strip()
    if not api_id.isdigit() or len(api_hash) < 30:
        print("⚠️ api_id должен быть числом, api_hash — длинной hex строкой. Перепроверь.")
        sys.exit(1)
    api_id = int(api_id)

    print()
    print("Шаг 2: phone login (на номер @iiiplfiii аккаунта)")
    print("Telegram пришлёт SMS-код. Если есть 2FA cloud password — спросит и его.")
    print()

    with TelegramClient(StringSession(), api_id, api_hash) as client:
        # Auto-prompts: phone, code, password
        session_str = client.session.save()
        me = client.get_me()
        print()
        print("=" * 60)
        print(f"✅ Залогинен как: {me.first_name} (@{me.username})")
        print("=" * 60)
        print()
        print("СКОПИРУЙ ВСЁ НИЖЕ И ПОСТАВЬ В RENDER ENV:")
        print()
        print(f"TELETHON_API_ID={api_id}")
        print(f"TELETHON_API_HASH={api_hash}")
        print(f"TELETHON_SESSION_STRING={session_str}")
        print()
        print("=" * 60)
        print("После добавления — Render автодеплой 3-4 мин.")
        print("Дальше бот сам будет удалять старые TG посты в auto_repost.")


if __name__ == "__main__":
    main()
