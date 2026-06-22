ALTER TABLE notifications ADD COLUMN IF NOT EXISTS chat_id TEXT;

UPDATE notifications SET chat_id = 'legacy' WHERE chat_id IS NULL;
ALTER TABLE notifications ALTER COLUMN chat_id SET NOT NULL;

DROP INDEX IF EXISTS idx_notifications_main_call;
ALTER TABLE notifications DROP CONSTRAINT IF EXISTS notifications_pkey;
ALTER TABLE notifications ADD CONSTRAINT notifications_pkey PRIMARY KEY (event_id, chat_id);

CREATE UNIQUE INDEX idx_notifications_main_call
ON notifications(call_id, chat_id) WHERE channel = 'main';
