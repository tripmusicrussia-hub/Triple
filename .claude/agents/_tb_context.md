# Triple Bot — shared context для суб-агентов

Этот файл — общая «база знаний проекта», на которую ссылаются специализированные
агенты (tb-reviewer / tb-deployer / tb-release-validator / etc). Не агент сам
по себе — а **reference**, который каждый агент цитирует в своём prompt'е.

## Проект

**Triple Bot** — Telegram-бот битмейкера IIIPLFIII. Репо: `c:\Triple Bot\`
(на публичном GitHub `tripmusicrussia-hub/Triple`). Хостинг: Render free tier
(`srv-d79fei5m5p6s73a29rg0`).

### Функции
1. Каталог битов (315 записей — 167 beat / 49 track / 11 remix / 88 non_audio)
2. Автопостинг в TG-канал `@iiiplfiii` по расписанию 16:00 МСК (2 аудио + 5 текстов/неделю)
3. YT content machine — upload mp3 → thumbnail + video + shorts → YT + TG
4. Продажа MP3 Lease (500⭐ TG Stars / 7 USDT CryptoBot)
5. Продажа drum kits / sample packs / loop packs (zip через `send_document`
   с `protect_content=True`)
6. User-facing каталог «📦 Kits & Packs» + деп-link `?start=prod_<id>`
7. Conversational agent для admin и user (OpenRouter LLM)

### Стек
- python-telegram-bot 21.5
- OpenRouter: claude-haiku-4.5 → gpt-4o-mini → gpt-oss-120b:free (fallback)
- YouTube Data API v3 (`yt_api.py` с refresh_token OAuth)
- Supabase (primary для `sales`, `bot_users`, `post_events`)
- CryptoBot API (USDT)
- `imageio-ffmpeg` (bundled ffmpeg для Render)
- Pillow (thumbnails)

## Критичные файлы

- `bot-assistant/bot-assistant/bot.py` — главный (~3500 строк, god-module)
- `bot-assistant/bot-assistant/beats_db.py` — persistence битов + 3-layer recovery
- `bot-assistant/bot-assistant/users_db.py` — Supabase bot_users dual-write
- `bot-assistant/bot-assistant/post_generator.py` — LLM-генератор постов,
  `RUBRIC_SCHEDULE`, `ANTI_AI_BLOCK`, `_call_llm`
- `bot-assistant/bot-assistant/beat_post_builder.py` — YT/TG/Shorts title/desc/tags
- `bot-assistant/bot-assistant/shorts_builder.py` — 9:16 video via ffmpeg
- `bot-assistant/bot-assistant/video_builder.py` — статичный кадр + mp3 → mp4
- `bot-assistant/bot-assistant/product_upload.py` — валидация zip продукта
- `bot-assistant/bot-assistant/licensing.py` — цены, labels, license-тексты
- `bot-assistant/bot-assistant/yt_api.py` — YouTube upload
- `bot-assistant/bot-assistant/sales.py` — журнал продаж
- `bot-assistant/bot-assistant/publish_scheduler.py` — очередь плановых публикаций
- `bot-assistant/bot-assistant/wiki/iiiplfiii_voice.md` — voice-skill для LLM
- `bot-assistant/bot-assistant/wiki/post_ideas.md` — бэклог тем для рубрик
- `bot-assistant/bot-assistant/tests/` — 70 pytest-тестов
- `.github/workflows/keepalive.yml` — 24/7 keepalive Render

## Правила проекта (строго соблюдать)

### Публичный репо — ОСТОРОЖНО
- **Не упоминать имена конкурентов в коде** (docstrings / commits / comments).
  Писать обобщённо: «winner-паттерн ниши», «DIY-продюсеры». Evidence с handles
  остаётся в memory/wiki/skills (приватно).

### Контент канала
- **Не сравнивать себя с другими битмейкерами** в постах (даже обобщённо).
  Никаких «многие делают X, а я Y», «в отличие от других». Фокус только на себе.
- **Ключевое слово тона: «качает»** — главная метрика качества автора.
- Стиль: короткие рваные фразы, первое лицо, )))-)) и многоточие норм, мрачно/дарк.

### Admin UX
- **Multi-step admin операции → FSM**, а не structured text в одном сообщении.
  Бот спрашивает по шагам, юзер не держит формат в голове.

### Payment flow
- **Идемпотентно через charge_id** — sales.jsonl + Supabase имеют `payment_charge_id` unique
- **Snapshot в pre_checkout** гарантирует доставку даже если бит удалён

### Persistence
- **Render free tier disk эфемерный** — runtime state в Supabase (bot_users),
  beats_data.json + .bak + git-checkout как 3-layer recovery, pending_posts/products
  персистятся на диск (в .gitignore)
- **Atomic writes** везде (save_beats / save_users / save_products):
  `.tmp` + `os.replace()` + fsync
- **Defensive reload** в cmd_admin / show_main_menu / handle_callback — если
  cache пустой но файл есть → reload

### Архитектура решений
- **beats_data.json в git** — ephemeral Render disk → git pull восстанавливает
- **users в Supabase** (а не git) — privacy
- **pending_posts/products на диске + .gitignore** — restore после redeploy
  (кроме pending_uploads битов — там локальные mp3/video файлы тоже пропадают)

## YT-оптимизация (из skill yt-optimization)

### Title — R1
`(FREE) <Artist> Type Beat <YEAR> - "<NAME>"` (40-60 симв, 0 emoji)

### Shorts — R2
То же + ` #Shorts` в конце (≤95 симв для запаса)

### Tags — 12-15 максимум
Паттерн: `<artist> type beat`, `<artist> type beat <year>`, `<scene> type beat`,
`hard <scene> instrumental`, `<bpm> bpm trap beat`, related artists.

### Description
Первые 100 симв критичны (YT показывает в snippet). Должны содержать:
artist, scene, genre (`trap`), year.

### Thumbnail — R6
- Без text-overlay (0/15 winners используют)
- Dark desaturated palette, VHS/Memphis style
- 1280×720 JPEG

### Video — R8
Статичный кадр на весь трек (4/5 winners). `-tune stillimage` encode x10 быстрее.

## Render service

- **Service ID:** `srv-d79fei5m5p6s73a29rg0`
- **Owner ID:** `tea-d79eaidactks73d5ltsg`
- **URL:** `https://triple-dnke.onrender.com/`
- **API:** `RENDER_API_KEY` в `bot-assistant/bot-assistant/.env`
- **Deploy endpoint:** `POST /v1/services/{SID}/deploys` (auto на git push)
- **Restart:** `POST /v1/services/{SID}/restart` (kill + fresh container)
- **Logs:** `GET /v1/logs?ownerId={OID}&resource={SID}&limit=500`
  — free tier отдаёт 500 последних; 99% — polling шум (фильтровать httpx/getUpdates)

## Типичные gotchas (уже наступали)

1. **Conflict-loop после deploy** — 2 инстанса дерутся за `getUpdates`. Фикс:
   `POST /restart` или `clearCache: 'clear'` deploy.
2. **Stale Python process** — deploy прошёл, но процесс на старом коде
   (редко, но было). Фикс: restart.
3. **PAT без `workflow` scope** — не пушит `.github/workflows/*.yml`.
   Решение: обновить PAT с scope `workflow` или push через VS Code OAuth.
4. **Render disk эфемерный** — `users_data.json` теряется каждый redeploy
   (→ Supabase). `beats_data.json` в git (→ git pull восстанавливает).
5. **kb_admin_channel / kb_admin_idea_day** — hardcoded названия рубрик.
   При переименовании RUBRIC_SCHEDULE — не забыть обновить их.
6. **import cohesion** — некоторые модули импортируются локально
   (`beat_post_builder`, `publish_scheduler`). Перед добавлением нового callback
   который их использует — проверить что import есть в данной ветке.
7. **pending_* dicts** — require persist/restore pair в post_init для переживания
   redeploy (pending_posts, pending_products сделано; pending_uploads — нет
   намеренно, т.к. mp3/thumb файлы всё равно пропадают).

## Memory (дополнительный context)

В `C:\Users\1\.claude\projects\c--Triple-Bot\memory\MEMORY.md` — индекс
правил и решений по проекту. Каждый агент перед работой может прочитать.
