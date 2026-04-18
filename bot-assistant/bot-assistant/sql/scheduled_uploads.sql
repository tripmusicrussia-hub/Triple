-- Queue для отложенных публикаций Triple Bot.
-- Запустить один раз в Supabase Dashboard → SQL Editor → New query → RUN.
-- Параллельно создать Storage bucket 'scheduled-uploads' (Dashboard → Storage → New bucket,
-- public=false, file size limit 50 MB).

create table if not exists scheduled_uploads (
    id            bigserial primary key,
    token         text unique not null,
    publish_at    timestamptz not null,
    actions       text[] not null default '{yt,tg}',
    meta          jsonb not null,          -- {name, artist_display, artist_raw, bpm, key, key_short, ...BeatMeta}
    yt_post       jsonb not null,          -- {title, description, tags[]}
    tg_caption    text not null,
    tg_style      text,
    tg_file_id    text not null,           -- Telegram file_id для re-send audio
    reserved_beat_id int,
    status        text not null default 'pending',  -- pending | published | failed | cancelled
    enqueued_at   timestamptz default now(),
    published_at  timestamptz,
    error_log     text
);

create index if not exists sched_pending_by_time
    on scheduled_uploads (publish_at) where status = 'pending';

create index if not exists sched_token
    on scheduled_uploads (token);

-- Необязательный: автоматически удалять очень старые published/cancelled records (older than 90d)
-- Если хочешь auto-cleanup — включай pg_cron в Supabase и добавь:
--   select cron.schedule('sched_gc', '0 3 * * *',
--     'delete from scheduled_uploads where status != ''pending'' and enqueued_at < now() - interval ''90 days''');
