CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id TEXT NOT NULL,
    email TEXT NOT NULL,
    full_name TEXT NOT NULL,
    country CHAR(2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    logical_date DATE NOT NULL,
    run_id TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (logical_date, customer_id)
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id TEXT NOT NULL,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    updated_at TIMESTAMPTZ NOT NULL,
    logical_date DATE NOT NULL,
    run_id TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (logical_date, product_id)
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    order_total NUMERIC(12, 2) NOT NULL CHECK (order_total >= 0),
    status TEXT NOT NULL CHECK (status IN ('placed', 'shipped', 'delivered', 'cancelled')),
    ordered_at TIMESTAMPTZ NOT NULL,
    logical_date DATE NOT NULL,
    run_id TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (logical_date, order_id)
);

CREATE TABLE IF NOT EXISTS raw.payments (
    payment_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    method TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('captured', 'refunded', 'failed')),
    paid_at TIMESTAMPTZ NOT NULL,
    logical_date DATE NOT NULL,
    run_id TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (logical_date, payment_id)
);

CREATE TABLE IF NOT EXISTS raw.rejected_records (
    dataset TEXT NOT NULL,
    logical_date DATE NOT NULL,
    row_number INTEGER NOT NULL,
    business_key TEXT,
    reasons TEXT[] NOT NULL,
    messages TEXT[] NOT NULL,
    raw_record JSONB NOT NULL,
    run_id TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset, logical_date, row_number)
);

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id TEXT PRIMARY KEY,
    logical_date DATE NOT NULL,
    trigger TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    error TEXT
);

CREATE TABLE IF NOT EXISTS audit.task_runs (
    run_id TEXT NOT NULL,
    task_name TEXT NOT NULL,
    try_number INTEGER NOT NULL,
    logical_date DATE NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    error_type TEXT,
    error TEXT,
    PRIMARY KEY (run_id, task_name, try_number)
);

CREATE TABLE IF NOT EXISTS audit.ingestion_batches (
    dataset TEXT NOT NULL,
    logical_date DATE NOT NULL,
    run_id TEXT NOT NULL,
    try_number INTEGER NOT NULL,
    source_path TEXT NOT NULL,
    records_read INTEGER NOT NULL,
    records_accepted INTEGER NOT NULL,
    records_rejected INTEGER NOT NULL,
    reject_reasons JSONB NOT NULL,
    unexpected_columns TEXT[] NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset, logical_date)
);

CREATE TABLE IF NOT EXISTS audit.quality_results (
    logical_date DATE NOT NULL,
    stage TEXT NOT NULL,
    check_name TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('blocking', 'warning')),
    outcome TEXT NOT NULL CHECK (outcome IN ('pass', 'warn', 'fail', 'skipped')),
    observed NUMERIC,
    threshold NUMERIC,
    details TEXT NOT NULL,
    run_id TEXT NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (logical_date, stage, check_name)
);

CREATE INDEX IF NOT EXISTS orders_customer_idx ON raw.orders (customer_id);
CREATE INDEX IF NOT EXISTS payments_order_idx ON raw.payments (order_id);
CREATE INDEX IF NOT EXISTS task_runs_date_idx ON audit.task_runs (logical_date);
