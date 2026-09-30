CREATE INDEX IF NOT EXISTS idx_calls_direction_started_at
ON calls(direction, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_calls_mango_account_started_at
ON calls((COALESCE(NULLIF(raw->>'mango_account', ''), 'primary')), started_at DESC);

CREATE INDEX IF NOT EXISTS idx_calls_manager_started_at
ON calls((COALESCE(NULLIF(raw#>>'{mango_employee_summary,name}', ''), '—')), started_at DESC);

CREATE INDEX IF NOT EXISTS idx_quality_scores_risk_level
ON quality_scores(risk_level, call_id);

CREATE INDEX IF NOT EXISTS idx_quality_scores_score
ON quality_scores(score, call_id);
