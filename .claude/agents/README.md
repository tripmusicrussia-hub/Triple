# Triple Bot — Claude Code sub-agents

Специализированные агенты для автоматизации рутины в проекте Triple Bot.
Помогают главному Claude быстрее решать задачи без потери контекста
основного разговора с юзером.

## Как использовать

Агенты активируются Claude Code **при старте сессии** (чтение
`.claude/agents/*.md`). Если добавил новый — **перезагрузи Claude Code**
(закрой/открой VS Code extension или CLI-сессию).

Главный Claude дёргает их через `Agent(subagent_type="<name>", ...)`.
Пример пользовательских запросов, которые запускают агентов:

| Запрос юзера | Какой агент |
|---|---|
| «проверь diff перед коммитом», «ревью этих изменений» | `tb-reviewer` |
| «как там бот?», «что в логах?», «почему не отвечает?» | `tb-deployer` |
| «проверь бит X перед upload», «готов ли релиз?» | `tb-release-validator` |
| «что в канале?», «был ли пост в такое-то время?», «давно не постил» | `tb-channel-watcher` |
| «что в Supabase», «где моя запись», «сколько продаж / юзеров» | `tb-supabase` |

## Список агентов

### `_tb_context.md`
**Не агент**, а shared knowledge base для всех агентов. Каждый агент
ссылается на него в своём system prompt. Содержит: архитектуру, критичные
файлы, правила проекта, типичные gotchas, Render credentials (без ключей).

### `tb-reviewer.md`
Code-review агент. Прогоняет diff по чек-листу из 10 пунктов:
UI↔code consistency, import cohesion, persistence rules, callback handlers,
payment flow, правила публичного репо (no competitor names), контент
(no comparison, «качает»), error handling, тесты, syntax.

**Когда вызывать:** перед `git commit`, когда diff нетривиален (>50 строк,
новые callbacks, persistence changes, external APIs).

**Возвращает:** список находок с severity (CRITICAL/HIGH/MEDIUM/LOW/NIT)
+ конкретика где проблема и как исправить.

### `tb-deployer.md`
Render monitor. Знает API, фильтрует polling-шум, детектит Conflict-loop,
stale process, crash-loop, catalog wipe.

**Когда вызывать:** после `git push` (проверить подхватил ли Render),
или по запросу юзера «как бот».

**Возвращает:** deploy status + live ping + отфильтрованные логи +
рекомендация (restart / ничего / нужна помощь).

### `tb-release-validator.md`
Pre-YT-upload SEO check. Валидирует title/tags/description/thumbnail
против R1-R8 из `yt-optimization` skill + evidence-based сравнение
с текущим топ-10 через yt-dlp.

**Когда вызывать:** перед публикацией нового YT-видео, или для аудита
уже залитых.

**Возвращает:** структурированный отчёт по каждому чек-пункту +
evidence из топа ниши + verdict (готов / правки / critical).

## План расширения (следующие сессии)

- `tb-content` — еженедельная инжекция новых тем в `post_ideas.md`
- `tb-competitor-watcher` — раз в 2 недели анализ топа YT/TG, подсветка сдвигов
- `tb-catalog-curator` — аудит `beats_data.json` на дубликаты / битые BPM / сироты
- `tb-analytics` (когда накопятся данные) — unit economics, конверсии
- `tb-welfare` — daily healthcheck всей инфры
