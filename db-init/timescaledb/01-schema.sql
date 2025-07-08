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

-- Insert some sample forecast data for the 'electronics' and 'food_and_beverage' categories for July 2025.
-- This makes the /forecasts endpoint immediately testable.
-- The ON CONFLICT clause makes this script idempotent (safe to run multiple times).
INSERT INTO forecasts (date, category_id, forecast_value, upper_bound, lower_bound, unit) VALUES
('2025-07-01', 'electronics', 150.75, 165.2, 135.0, 'sales_units'),
('2025-07-02', 'electronics', 148.32, 162.85, 133.45, 'sales_units'),
('2025-07-03', 'electronics', 152.10, 167.0, 137.0, 'sales_units'),
('2025-07-04', 'electronics', 155.60, 170.5, 140.1, 'sales_units'),
('2025-07-05', 'electronics', 160.25, 175.0, 145.5, 'sales_units'),
('2025-07-01', 'food_and_beverage', 550.50, 600.0, 500.0, 'sales_units'),
('2025-07-02', 'food_and_beverage', 545.80, 595.0, 495.0, 'sales_units')
ON CONFLICT (date, category_id) DO NOTHING;