-- This script runs automatically when the Docker container starts for the first time.

-- Create the table for storing relational category metadata
CREATE TABLE categories (
    category_id TEXT PRIMARY KEY,
    category_name TEXT NOT NULL
);

-- Create the table for storing relational product metadata
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
    total_quantity INTEGER,
    UNIQUE(time, category_id)
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
   target_type         varchar(20),
   target_id           varchar(50)
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
create table forecast_performance (
   id                      serial primary key,
   model_version_id        integer
      references model_versions ( id ),
   evaluation_period_start timestamptz not null,
   evaluation_period_end   timestamptz not null,
   metric_name             text not null, -- e.g., 'MAPE', 'RMSE'
   metric_value            numeric not null,
   unique ( model_version_id,
            evaluation_period_end,
            metric_name )
);

-- NEW: Create the table for serving the LATEST pre-calculated forecasts (the cache)
create table live_forecasts (
   id               serial primary key,
   model_version_id integer
      references model_versions ( id ),
   category_id      text not null,
   forecast_date    date not null,
   predicted_sales  numeric,
   lower_bound      numeric,
   upper_bound      numeric,
   granularity      varchar(10) not null,
    -- A unique constraint to ensure only one prediction per category/date/granularity exists
   unique ( category_id,
            forecast_date,
            granularity )
);
-- Add an index for fast lookups by category and granularity
create index idx_live_forecasts_category_granularity on
   live_forecasts (
      category_id,
      granularity
   );


-- NEW: Create the table for storing historical forecasts for analysis
create table historical_forecasts (
   id               serial primary key,
   model_version_id integer
      references model_versions ( id ),
   category_id      text not null,
   forecast_date    date not null,
   predicted_sales  numeric,
   lower_bound      numeric,
   upper_bound      numeric,
   granularity      varchar(10) not null,
   archived_at      timestamptz default now()
);
-- Add an index for faster analysis queries
create index idx_historical_forecasts_category_date on
   historical_forecasts (
      category_id,
      forecast_date
   );


-- Insert some initial data for demonstration
insert into categories (
   category_id,
   category_name
) values ( 'CAT_01',
           'Electronics' ),( 'CAT_02',
                             'Home & Kitchen' ),( 'CAT_03',
                                                  'Apparel' ),( 'CAT_04',
                                                                'Books' ),( 'CAT_05',
                                                                            'Sports' );