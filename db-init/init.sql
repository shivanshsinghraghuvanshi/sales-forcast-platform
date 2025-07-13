-- This script runs automatically when the Docker container starts for the first time.

-- Create the table for storing relational category metadata
CREATE TABLE categories (
    category_id TEXT PRIMARY KEY,
    category_name TEXT NOT NULL
);

-- Create the table for storing relational product metadata
create table products (
   product_id   text primary key,
   product_name text not null,
   description  text,
   category_id  text
      references categories ( category_id )
);

-- Create the table for storing time-series sales data
create table hourly_sales_by_category (
   time           timestamptz not null,
   category_id    text not null,
   total_sales    numeric,
   total_quantity integer,
    -- *** FIX: Add a UNIQUE constraint on the combination of time and category_id ***
    -- This is required for the ON CONFLICT clause in the Go processor to work correctly.
   unique ( time,
            category_id )
);

-- Turn the sales table into a TimescaleDB hypertable for performance
select create_hypertable(
   'hourly_sales_by_category',
   'time'
);


-- Create the table to track the status of our ETL jobs
create table etl_job_status (
   id           serial primary key,
   file_name    text not null unique,
   status       varchar(20) not null, -- e.g., 'PENDING', 'SUCCESS', 'FAILED'
   last_updated timestamptz default now()
);

-- Create the table for storing external promotions data
create table promotions (
   promotion_id        text primary key,
   promotion_name      text,
   start_date          date,
   end_date            date,
   discount_percentage integer,
   target_type         varchar(20), -- Can be 'product' or 'category'
   target_id           varchar(50)    -- The ID of the product or category being targeted
);

-- Create the table for model versioning and metadata
create table model_versions (
   id                  serial primary key,
   category_id         text not null,
   version             text not null,
   model_path          text not null,
   training_date_utc   timestamptz not null,
   is_latest           boolean default false,
   metadata            jsonb,
   backtesting_metrics jsonb,
   unique ( category_id,
            version )
);

-- Create a table to store live performance metrics for drift detection
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