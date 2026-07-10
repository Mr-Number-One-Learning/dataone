CREATE TABLE IF NOT EXISTS _pipeline_runs (
    run_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_name          TEXT NOT NULL,
    start_time        TIMESTAMPTZ NOT NULL DEFAULT now(),
    end_time          TIMESTAMPTZ,
    status            TEXT CHECK (status IN ('running', 'success', 'failed')),
    rows_processed    BIGINT,
    rows_quarantined  BIGINT,
    date_range_start  DATE,
    date_range_end    DATE,
    error_message     TEXT
);
