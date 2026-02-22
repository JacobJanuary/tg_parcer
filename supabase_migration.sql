-- Run this in the Supabase SQL Editor to sync the schema with the latest Python code
-- IMPORTANT: Make sure you are running this on the correct project database, and that
-- the SQL Editor role is set to `postgres` (the default admin role).

ALTER TABLE events
    ALTER COLUMN title TYPE JSONB USING to_jsonb(title),
    ALTER COLUMN summary TYPE JSONB USING to_jsonb(summary),
    ADD COLUMN IF NOT EXISTS description JSONB,
    ADD COLUMN IF NOT EXISTS source_chat_id BIGINT REFERENCES chats(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS source_chat_title TEXT,
    ADD COLUMN IF NOT EXISTS message_id BIGINT,
    ADD COLUMN IF NOT EXISTS sender TEXT,
    ADD COLUMN IF NOT EXISTS filter_score INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS original_text TEXT,
    ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'listener',
    ADD COLUMN IF NOT EXISTS fingerprint TEXT UNIQUE,
    ADD COLUMN IF NOT EXISTS detected_at TIMESTAMPTZ DEFAULT now();
