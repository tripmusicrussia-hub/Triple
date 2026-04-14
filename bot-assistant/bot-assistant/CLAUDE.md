# Triple Bot — музыкальный AI-ассистент IIIPLFIII

Telegram-бот для направления IIIPLFIII (hard trap Memphis/Detroit type beats).
Персональный ассистент: каталог битов, автопостинг в канал @iiiplfiii, загрузка на YouTube, LLM-подписи в голосе автора.

## Правила работы

**Обязательно перед каждым пушем:**
1. Написать тест под конкретное изменение
2. Прогнать тест локально
3. Только потом `git push`

Никогда не пушить непроверенный код — бот в проде на Render.

## Архитектура

**Стек:**
- `python-telegram-bot 21.5` — бот
- `supabase 2.9.1` — аналитика `post_events` (primary) + локальный jsonl backup
- OpenRouter LLM (`openai/gpt-oss-120b:free`) — генерация подписей для TG-постов в tone-of-voice IIIPLKIII
- YouTube Data API — загрузка видео на канал
- `imageio-ffmpeg` — сборка mp4 из thumbnail + mp3 для YouTube
- Render — деплой (автодеплой с GitHub при пуше в `main`)

**Основные flow:**
1. **Upload бита**: админ присылает mp3 с именем `<artist> type beat <NAME> <BPM> <KEY>.mp3` →
   бот парсит → генерит thumbnail/video → LLM-подпись → preview в ЛС → кнопки публикации YT+TG.
2. **Авто-постинг канала**: scheduler в 16:00 МСК → `post_generator.pick_post_for_today()` (рубрика по дню недели) →
   preview админу → кнопки "Опубликовать"/"Перегенерить"/"Отмена".
3. **Каталог битов**: пользователи через `/start` → меню → поиск по BPM/ключу/стилю.
4. **Розыгрыши**: через `/giveaway` админ заводит приз → пользователи участвуют через репост.

## Ключевые файлы

| Файл | Назначение |
|------|-----------|
| `bot.py` | Главный обработчик, callback'и, scheduler'ы, админка |
| `beats_db.py` | Каталог битов — парсинг, cache, fuzzy поиск |
| `beat_post_builder.py` | Сборка TG-подписей: 5 стилей через LLM + fallback |
| `beat_upload.py` | Парсинг имени mp3, подготовка метаданных |
| `post_generator.py` | LLM-генератор постов для канала (рубрики по дням) |
| `post_analytics.py` | Dual-write событий в Supabase + jsonl |
| `thumbnail_generator.py` | PIL-сборка обложки для YouTube |
| `video_builder.py` | ffmpeg-сборка mp4 из обложки + mp3 |
| `youtube_uploader.py` | Загрузка на YouTube через OAuth |
| `config.py` | Env vars |

## Env vars (Render)

- `BOT_TOKEN`, `ADMIN_ID` — Telegram
- `CHANNEL_ID` (=`@iiiplfiii`), `CHANNEL_LINK`
- `OPENROUTER_API_KEY` — LLM для подписей
- `SUPABASE_URL`, `SUPABASE_KEY` — аналитика
- `YT_CLIENT_ID`, `YT_CLIENT_SECRET`, `YT_REFRESH_TOKEN` — YouTube OAuth

## Tone-of-voice автора

Единый источник — `C:\Users\1\.claude\skills\iiiplkiii-voice\SKILL.md` (лежит в репо
как `SKILL.md` на корне `bot-assistant/` для Render; читается и Claude в сессии, и ботом
как system-prompt LLM).

## Деплой

```bash
git add <конкретные файлы>
git commit -m "..."
git push origin main          # Render подхватит автодеплоем
```

## Супабейз-схема

Таблица `post_events`:
- `id bigserial`, `ts timestamptz`, `kind text`, `beat_name text`, `artist text`,
  `bpm int`, `key text`, `style text` (short_hook / minimal / storytelling / question / emotional / fallback),
  `caption text`, `yt_video_id text`, `tg_message_id bigint`, `yt_title text`
