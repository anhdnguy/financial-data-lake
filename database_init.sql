-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Universe registry
CREATE TABLE IF NOT EXISTS universe (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Symbol registry
CREATE TABLE IF NOT EXISTS membership (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Universe membership junction
CREATE TABLE IF NOT EXISTS universe_membership (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    membership_id UUID NOT NULL REFERENCES membership(id),
    universe_id UUID NOT NULL REFERENCES universe(id),
    enter_date DATE NOT NULL,
    exit_date DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    delisted_reason TEXT
);

-- Volatility rolling
create table if not exists volatility_rolling (
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	symbol_id UUID NOT NULL REFERENCES membership(id),
	created_at TIMESTAMP DEFAULT NOW(),
	updated_at TIMESTAMP DEFAULT NOW(),
	rolling_sd FLOAT not null,
	rolling_avg_volume FLOAT not null
);

create type failure_mode as ENUM ('HTTP_ERROR', 'VALIDATION');

-- DLQ
create table if not exists failed_ingestion (
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	symbol_id UUID REFERENCES membership(id),
	raw_symbol VARCHAR(10),
	created_at TIMESTAMP DEFAULT NOW(),
	failure failure_mode,
	failure_error TEXT,
	retry_after timestamp,
	attempts INT default 0,
	review_required bool default False
);

-- Pipeline Run Log
CREATE TABLE IF NOT EXISTS pipeline_run (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dag_id VARCHAR(100) NOT NULL,
    run_date DATE NOT NULL,
    status VARCHAR(20) CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL')),
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    records_processed INT DEFAULT 0,
    records_failed INT DEFAULT 0,
    notes TEXT
);

-- Schwab refresh token (durable source of truth).
-- Holds the long-lived (7-day) refresh token; the short-lived (~30-min) access
-- token lives only in Redis. issued_at anchors the 7-day hard wall, so the
-- token provider can compute the TRUE remaining life when it seeds Redis or
-- decides whether a proactive re-auth is due. One active token per provider
-- (UNIQUE) → human re-auth is an idempotent ON CONFLICT (provider) upsert.
-- issued_at is TIMESTAMPTZ on purpose: the wall math (issued_at + 7 days) is
-- correctness-critical and a tz-naive value would shift the deadline by hours.
CREATE TABLE IF NOT EXISTS schwab_token (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    provider VARCHAR(50) NOT NULL UNIQUE DEFAULT 'schwab',
    refresh_token TEXT NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE UNIQUE INDEX idx_universe_membership_active
ON universe_membership(membership_id, universe_id)
WHERE exit_date IS NULL;

CREATE UNIQUE INDEX idx_membership_symbol 
ON membership(symbol);

CREATE INDEX idx_universe_membership_dates
ON universe_membership(enter_date, exit_date);

CREATE INDEX idx_pipeline_run_dag_date
ON pipeline_run(dag_id, run_date);

-- One rolling volatility record per symbol; required for the volatility upsert's
-- ON CONFLICT (symbol_id) target in update_rolling_volatility.
CREATE UNIQUE INDEX IF NOT EXISTS idx_volatility_rolling_symbol
ON volatility_rolling(symbol_id);

-- One open DLQ record per symbol; required for the ON CONFLICT (symbol_id) upsert in
-- _log_ingestion_failure / run_statistical_validation. Makes failure logging idempotent
-- (a re-trigger updates the existing row instead of inserting a duplicate) and lets the
-- attempts counter actually accumulate across runs so the attempts < 3 retry cap works.
CREATE UNIQUE INDEX IF NOT EXISTS idx_failed_ingestion_symbol
ON failed_ingestion(symbol_id);

-- Updated_at trigger function
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to membership
CREATE TRIGGER on_membership_updated
BEFORE UPDATE ON membership
FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

-- Apply trigger to universe_membership
CREATE TRIGGER on_universe_membership_updated
BEFORE UPDATE ON universe_membership
FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

--- Apply trigger to volatility_rolling
CREATE TRIGGER on_volatility_rolling_updated
BEFORE UPDATE ON volatility_rolling
FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

-- Apply trigger to schwab_token (refreshes updated_at on each re-auth upsert)
CREATE TRIGGER on_schwab_token_updated
BEFORE UPDATE ON schwab_token
FOR EACH ROW EXECUTE PROCEDURE set_updated_at();