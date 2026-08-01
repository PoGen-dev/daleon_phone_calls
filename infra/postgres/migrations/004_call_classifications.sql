CREATE TABLE IF NOT EXISTS call_classifications (
    call_id TEXT PRIMARY KEY REFERENCES calls(id) ON DELETE CASCADE,
    call_type TEXT NOT NULL CHECK (
        call_type IN ('appointment', 'sales', 'delivery', 'consultation', 'completed_deal', 'critical')
    ),
    reason TEXT NOT NULL,
    critical_errors JSONB NOT NULL DEFAULT '[]'::jsonb,
    confidence TEXT NOT NULL CHECK (confidence IN ('high', 'medium', 'low')),
    raw JSONB NOT NULL DEFAULT '{}'::jsonb,
    model TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_call_classifications_type
ON call_classifications(call_type, created_at DESC);