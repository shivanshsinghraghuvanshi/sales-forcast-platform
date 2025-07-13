-- This script runs automatically when the Docker container starts for the first time.

-- Create the table for storing relational category metadata
CREATE TABLE categories (
    category_id TEXT PRIMARY KEY,
    category_name TEXT NOT NULL
);

-- Create the table for storing relational product metadata
-- NOTE: This schema is already correct for the new products.csv,
-- as it includes the category_id column with a foreign key.
CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    description TEXT,
    category_id TEXT REFERENCES categories(category_id)
);

-- Create the table for storing time-series sales data
CREATE TABLE hourly_sales_by_category (
    time TIMESTAMPTZ NOT NULL,
    category_id TEXT NOT NULL,
    total_sales NUMERIC,
    total_quantity INTEGER
);

-- Turn the sales table into a TimescaleDB hypertable for performance
SELECT create_hypertable('hourly_sales_by_category', 'time');

-- Create the table for storing external promotions data
-- *** CHANGE: Added target_type and target_id to link promotions to products or categories ***
CREATE TABLE promotions (
    promotion_id VARCHAR(50) PRIMARY KEY,
    promotion_name VARCHAR(255) NOT NULL,
    discount_percentage NUMERIC(5, 2) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    target_type VARCHAR(20), -- Can be 'product' or 'category'
    target_id VARCHAR(50)    -- The ID of the product or category being targeted
);

-- Create the table to track the status of our ETL jobs
CREATE TABLE etl_job_status (
    id SERIAL PRIMARY KEY,
    file_name TEXT NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL, -- e.g., 'PENDING', 'SUCCESS', 'FAILED'
    last_updated TIMESTAMPTZ DEFAULT NOW()
);


-- Create the table for model versioning and metadata
CREATE TABLE model_versions (
    id SERIAL PRIMARY KEY,
    category_id TEXT NOT NULL,
    version TEXT NOT NULL,
    model_path TEXT NOT NULL,
    training_date_utc TIMESTAMPTZ NOT NULL,
    is_latest BOOLEAN DEFAULT FALSE,
    metadata JSONB,
    -- NEW: Add a field for storing backtesting accuracy metrics
    backtesting_metrics JSONB,
    UNIQUE (category_id, version)
);

-- NEW: Create a table to store live performance metrics for drift detection
CREATE TABLE forecast_performance (
    id SERIAL PRIMARY KEY,
    model_version_id INTEGER REFERENCES model_versions(id),
    evaluation_period_start TIMESTAMPTZ NOT NULL,
    evaluation_period_end TIMESTAMPTZ NOT NULL,
    metric_name TEXT NOT NULL, -- e.g., 'MAPE', 'RMSE'
    metric_value NUMERIC NOT NULL,
    UNIQUE(model_version_id, evaluation_period_end, metric_name)
);

-- Insert some initial data for demonstration
INSERT INTO categories (category_id, category_name) VALUES
('CAT_01', 'Electronics'),
('CAT_02', 'Home & Kitchen'),
('CAT_03', 'Apparel'),
('CAT_04', 'Books'),
('CAT_05', 'Sports');