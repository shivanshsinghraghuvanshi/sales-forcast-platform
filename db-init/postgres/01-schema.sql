-- /db-init/postgres/01-schema.sql

-- This script runs on the main 'postgres' service.

-- Create the 'categories' table to store forecastable product categories.
CREATE TABLE IF NOT EXISTS categories (
    id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Insert sample data to match the OpenAPI spec, making the API usable immediately.
-- The ON CONFLICT clause makes this script idempotent (safe to run multiple times).
INSERT INTO categories (id, name) VALUES
('electronics', 'Electronics & Gadgets'),
('food_and_beverage', 'Food & Beverage'),
('clothing', 'Clothing & Apparel'),
('home_and_garden', 'Home & Garden'),
('all_products', 'All Products Combined')
ON CONFLICT (id) DO NOTHING;

-- Create the 'jobs' table to track the status of async forecast generation tasks.
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    progress INTEGER NOT NULL DEFAULT 0,
    message TEXT,
    category_id TEXT NOT NULL,
    forecast_horizon VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    result_url TEXT,
    webhook_url TEXT,
    error_details JSONB
);