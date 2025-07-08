-- /db-init/timescaledb/01-schema.sql

-- This script runs on the 'timescaledb' service.

-- Create the regular PostgreSQL table first.
CREATE TABLE IF NOT EXISTS forecasts (
    date TIMESTAMPTZ NOT NULL,
    category_id VARCHAR(100) NOT NULL,
    forecast_value DOUBLE PRECISION NOT NULL,
    upper_bound DOUBLE PRECISION,
    lower_bound DOUBLE PRECISION,
    unit VARCHAR(50) NOT NULL,
    PRIMARY KEY (date, category_id) -- Ensures each data point is unique for a given time and category.
);

-- Convert the 'forecasts' table into a TimescaleDB hypertable, partitioned by the 'date' column.
-- This is the core command that enables TimescaleDB's performance benefits.
SELECT create_hypertable('forecasts', 'date', if_not_exists => TRUE);