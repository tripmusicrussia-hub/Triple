# Triple Bot — музыкальный AI-ассистент IIIPLFIII

Telegram-бот для направления IIIPLFIII (hard trap Memphis/Detroit type beats).
Персональный ассистент: каталог битов, публикация в канал @iiiplfiii + YouTube,
продажа MP3 lease (Stars / USDT / RUB), drumkits/packs, аналитика.

## Правила работы

**Обязательно перед каждым пушем:**
1. Написать тест под конкретное изменение (если применимо)
2. Прогнать `pytest -q` локально
3. Syntax check через `ast.parse` на изменённых файлах
4. Только потом `git push`

Никогда не пушить непроверенный код — бот в проде на Render.

## Архитектура

**Стек:**
- `python-telegram-bot 21.5` — бот
- Supabase (PostgREST REST API) — `bot_users`, `sales`, `post_events`, `scheduled_uploads`
- OpenRouter LLM (`openai/gpt-oss-120b:free` + haiku fallback) — генерация TG-captions
- YouTube Data API v3 — загрузка видео + Shorts (OAuth refresh_token)
- CryptoBot API — USDT платежи
- Telegram Stars — native payments через XTR currency
- YooKassa — RUB auto-pay через Telegram Payments 2.0 (MIR/Visa/СБП)
- `imageio-ffmpeg` — сборка mp4 из thumbnail + mp3
- Render — деплой (автодеплой с GitHub при пуше в `main`)

**Основные flow:**
1. **Upload бита**: админ шлёт mp3 с именем `<artist> type beat <NAME> <BPM> <KEY>.mp3` →
   парсинг → brand-frame + thumbnail + video + 9:16 Shorts → LLM-caption →
   preview в ЛС → кнопки «🚀 YT + Shorts + TG» / «📅 В лучшее время» / и т.д.
2. **Публикация из очереди**: `publish_scheduler` + `scheduled_publish_loop`
   (каждые 60с) → due items публикуются в оптимальные слоты (Fri 21:30 + Mon 21:00 МСК).
3. **Покупка бита**: юзер в канале/боте → preview → `⭐ 1500` / `💵 20 USDT` /
   `💳 1700₽` / `💎 Exclusive`. Оплата → MP3 + TXT-лицензия мгновенно в ЛС.
4. **Продукты** (drumkits/packs): `/upload_product` FSM → preview → промо в канал.
5. **Exclusive inquiry**: клик `💎` → бот спрашивает про проект → форвард админу.
6. **Content reminders**: Пн/Ср/Пт 20:00 МСК бот пинает админа с темой дня
   (Process Reveal / Gear Talk / UGC-Story) для Shorts.
7. **Referral tracking**: `?start=ref_<src>` → first-touch source в `bot_users.source`.
   Combo формат: `?start=ref_yt_buy_<id>` (YT description) — source + сразу покупка.

## Ключевые файлы

| Файл | Назначение |
|------|-----------|
| `bot.py` | Handlers, callbacks, scheduler'ы, админ-команды, FSM |
| `beats_db.py` | Каталог — atomic save + .bak rotation + git recovery |
| `users_db.py` | bot_users Supabase + in-memory fallback, source tracking |
| `sales.py` | Лог продаж (Supabase + jsonl), `fiat_amount_minor` для RUB/USDT |
| `beat_post_builder.py` | YT post + TG caption (single minimal style + hashtag_nav) |
| `beat_upload.py` | parse_filename → BeatMeta |
| `post_generator.py` | Минимальный LLM-caller + voice skill loader + ANTI_AI_BLOCK |
| `licensing.py` | Цены (MP3 1500⭐/20USDT/1700₽), license text templates |
| `product_upload.py` | FSM для drumkits/packs + ProductMeta |
| `publish_scheduler.py` | Очередь плановых публикаций в Supabase scheduled_uploads |
| `cryptobot.py` | USDT payments через CryptoPay API |
| `post_analytics.py` | Dual-write post_events (upload / scheduled_upload) |
| `thumbnail_generator.py` | PIL для YT обложек |
| `video_builder.py` | ffmpeg для mp4 + 9:16 Shorts |
| `youtube_uploader.py` | YT Data API OAuth + upload |
| `config.py` | Env vars |

## Команды

**Юзер:**
- `/start` (+deep-link `ref_<src>`, `buy_<id>`, `prod_<id>`, combo `ref_<src>_buy_<id>`)
- `/cancel_excl` — отменить активный exclusive inquiry

**Админ:**
- `/admin` — меню: Статистика, Каталог, Kits&Packs, 📅 Очередь, Закреп, Удалить бит, 🎬 YouTube
- `/today` — сводка дня: новые юзеры (по source), продажи (USD-эквивалент), заливы
- `/content on|off|status` — напоминания Пн/Ср/Пт 20:00 про Shorts-съёмку
- `/fix_hashtags` — backfill #bpmXXX #typebeat в старых постах канала (однократно)
- `/pin_hub` — обновить закреплённый hub-пост с актуальной навигацией
- `/queue` + `/cancel_sched <token>` — управление очередью публикаций
- `/upload_product` + `/cancel_product` — FSM для kits/packs/loops
- `/stats`, `/diag`, `/search` — диагностика и поиск

## Env vars (Render)

- `BOT_TOKEN`, `ADMIN_ID` — Telegram
- `CHANNEL_ID` (=`@iiiplfiii`), `CHANNEL_LINK`
- `OPENROUTER_API_KEY` — LLM для caption'ов
- `SUPABASE_URL`, `SUPABASE_KEY` — persistence юзеров/продаж/событий/очереди
- `YT_CLIENT_ID`, `YT_CLIENT_SECRET`, `YT_REFRESH_TOKEN` — YouTube OAuth
- `CRYPTOBOT_TOKEN` — USDT payments
- `YOOKASSA_PROVIDER_TOKEN` — RUB auto-pay (графически скрывается если не задан)
- `RENDER_API_KEY` — самодиагностика через API (опционально)

## Supabase schema

**`bot_users`** — аудитория:
- `tg_id bigint pk`, `username text`, `full_name text`, `joined_at timestamptz`,
  `updated_at timestamptz`, `received_sample_pack bool`, `is_subscribed bool`,
  `favorites jsonb`, `source text` (yt/insta/tg/landing/ads/collab/other)

**`sales`** — продажи:
- `id bigserial`, `ts timestamptz`, `buyer_tg_id bigint`, `buyer_username text`,
  `beat_id bigint`, `beat_name text`, `license_type text`, `stars_amount int`,
  `fiat_amount_minor bigint` (копейки RUB / центы USDT), `currency text`
  (XTR/USDT/RUB), `payment_charge_id text`, `status text`

**`post_events`** — лог публикаций:
- `id bigserial`, `ts timestamptz`, `kind text` (upload/scheduled_upload),
  `beat_name text`, `artist text`, `bpm int`, `key text`, `style text`
  (minimal/fallback), `caption text`, `yt_video_id text`, `tg_message_id bigint`,
  `yt_title text`

**`scheduled_uploads`** — очередь публикаций:
- `token text pk`, `reserved_beat_id bigint`, `publish_at timestamptz`,
  `status text` (pending/published/cancelled), `meta jsonb`, `yt_post jsonb`,
  `tg_caption text`, `tg_style text`, `tg_file_id text`, `enqueued_at timestamptz`,
  `published_at timestamptz`, `error_log text`

**Storage bucket** `scheduled-uploads/` — 3 файла на токен: mp3, mp4, jpg.

## Tone-of-voice автора

Единый источник — `~/.claude/skills/iiiplfiii-voice/SKILL.md` (копия на
`bot-assistant/SKILL.md` для Render). Читается Claude в сессии и ботом
как system-prompt для LLM. Правила:
- 1-3 короткие строки, первое лицо, короткие рваные фразы
- 0-1 эмодзи, без markdown, без хэштегов в тексте (они добавляются deterministic'но)
- Триггер-слово тона: **«качает»**
- Запрещено: сравнения с другими битмейкерами, AI-клише, маркетинг-гипербола

## Sub-agents (.claude/agents/)

- `tb-reviewer` — code review перед коммитом (10-пунктный чеклист)
- `tb-deployer` — Render monitor (логи, deploy status, restart)
- `tb-release-validator` — pre-YT-upload SEO check (R1-R8 + yt-dlp топ-10)
- `tb-channel-watcher` — парсит t.me/s/iiiplfiii (публичные посты канала)
- `tb-supabase` — SELECT к таблицам (sales, bot_users, scheduled_uploads, post_events)
- `tb-content` — генерит контент-пакеты (captions, titles, Shorts hooks) под бит

## Деплой

```bash
git add <конкретные файлы>
git commit -m "feat/fix/chore(scope): ..."
git push origin main          # Render автодеплой 3-4 мин
```

После push — `tb-deployer` можно вызвать для проверки статуса deploy'а
и фильтрации логов.
