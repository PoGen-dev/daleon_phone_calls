CREATE TABLE IF NOT EXISTS calls (
    id TEXT PRIMARY KEY,
    entry_id TEXT,
    call_id TEXT,
    recording_id TEXT,
    recording_url TEXT,
    direction TEXT,
    from_number TEXT,
    to_number TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    disconnect_reason TEXT,
    raw JSONB NOT NULL DEFAULT '{}'::jsonb,
    audio_bucket TEXT,
    audio_object_name TEXT,
    audio_filename TEXT,
    status TEXT NOT NULL DEFAULT 'discovered',
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_calls_started_at ON calls(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_calls_recording_id ON calls(recording_id);
CREATE INDEX IF NOT EXISTS idx_calls_status ON calls(status);

CREATE TABLE IF NOT EXISTS transcriptions (
    call_id TEXT PRIMARY KEY REFERENCES calls(id) ON DELETE CASCADE,
    transcript TEXT NOT NULL,
    model TEXT NOT NULL,
    language TEXT,
    duration_seconds NUMERIC,
    raw JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS quality_scores (
    call_id TEXT PRIMARY KEY REFERENCES calls(id) ON DELETE CASCADE,
    score INTEGER NOT NULL CHECK (score BETWEEN 0 AND 100),
    risk_level TEXT NOT NULL CHECK (risk_level IN ('critical', 'warning', 'normal')),
    risk_reason TEXT NOT NULL,
    summary TEXT NOT NULL,
    errors JSONB NOT NULL DEFAULT '[]'::jsonb,
    recommendation TEXT NOT NULL,
    criteria JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw JSONB NOT NULL DEFAULT '{}'::jsonb,
    model TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS notifications (
    event_id TEXT PRIMARY KEY,
    call_id TEXT,
    channel TEXT NOT NULL CHECK (channel IN ('main', 'error')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_notifications_main_call
ON notifications(call_id) WHERE channel = 'main';

CREATE TABLE IF NOT EXISTS worker_state (
    name TEXT PRIMARY KEY,
    value JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS calls_set_updated_at ON calls;
CREATE TRIGGER calls_set_updated_at BEFORE UPDATE ON calls
FOR EACH ROW EXECUTE FUNCTION set_updated_at();
