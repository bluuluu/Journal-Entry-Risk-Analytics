CREATE SCHEMA IF NOT EXISTS audit_analytics;
SET search_path TO audit_analytics;

CREATE TABLE IF NOT EXISTS gl_entries (
    entry_id        BIGINT,
    entity          TEXT,
    je_number       TEXT,
    line_num        INT,
    account         TEXT,
    offset_account  TEXT,
    description     TEXT,
    amount          NUMERIC(18,2),
    currency        TEXT,
    debit_credit    TEXT,
    posting_date    DATE,
    posting_timestamp TIMESTAMP WITH TIME ZONE,
    time_zone       TEXT,
    created_by      TEXT,
    source          TEXT,
    approval_status TEXT
);

-- Optional: user/entity calendar to parameterize business hours and weekends.
CREATE TABLE IF NOT EXISTS entity_calendar (
    entity          TEXT PRIMARY KEY,
    tz              TEXT NOT NULL,
    business_start  INT NOT NULL DEFAULT 8,
    business_end    INT NOT NULL DEFAULT 18,
    weekend_start   INT NOT NULL DEFAULT 6,
    weekend_end     INT NOT NULL DEFAULT 0
);

INSERT INTO entity_calendar (entity, tz, business_start, business_end, weekend_start, weekend_end)
VALUES
    ('US', 'America/New_York', 8, 18, 6, 0),
    ('UK', 'Europe/London', 8, 18, 6, 0),
    ('APAC', 'Asia/Singapore', 9, 18, 6, 0)
ON CONFLICT (entity) DO NOTHING;
CREATE INDEX IF NOT EXISTS idx_gl_entries_posting_date ON gl_entries (posting_date);
CREATE INDEX IF NOT EXISTS idx_gl_entries_account ON gl_entries (account);
CREATE INDEX IF NOT EXISTS idx_gl_entries_created_by ON gl_entries (created_by);
CREATE INDEX IF NOT EXISTS idx_gl_entries_posting_ts ON gl_entries (posting_timestamp);
