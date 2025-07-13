import pandas as pd
import os
from datetime import datetime
from .db_utils import get_db_engine
from sqlalchemy import text

RAW_SALES_PATH = "./../local_s3_bucket/sales/"


def process_sales_data():
    """Processes and loads the daily sales data file."""
    today_str = datetime.now().strftime('%Y-%m-%d')
    file_name = f"sales_{today_str}.csv"
    file_path = os.path.join(RAW_SALES_PATH, file_name)
    engine = get_db_engine()

    # First, check if the file has already been processed successfully.
    try:
        with engine.connect() as conn:
            status_query = text("SELECT status FROM etl_job_status WHERE file_name = :file_name")
            result = conn.execute(status_query, {"file_name": file_name}).scalar_one_or_none()

        if result == 'SUCCESS':
            print(f"File '{file_name}' has already been processed successfully. Skipping.")
            return
    except Exception as e:
        print(f"Error checking ETL status for {file_name}. Error: {e}")
        # Exit if we can't even check the status, as we might process a file twice.
        return

    if not os.path.exists(file_path):
        print(f"Error: Sales file not found at {file_path}.")
        return

    try:
        # 2. Extract & Transform
        print(f"Processing sales file: {file_name}")
        sales_df = pd.read_csv(file_path)

        # Data validation and cleansing
        sales_df.dropna(subset=['transaction_id', 'product_id', 'timestamp', 'quantity', 'price_per_unit'],
                        inplace=True)
        sales_df['timestamp'] = pd.to_datetime(sales_df['timestamp'])
        sales_df['quantity'] = pd.to_numeric(sales_df['quantity'], errors='coerce')
        sales_df['price_per_unit'] = pd.to_numeric(sales_df['price_per_unit'], errors='coerce')
        sales_df.dropna(subset=['quantity', 'price_per_unit'], inplace=True)
        sales_df = sales_df[(sales_df['quantity'] > 0) & (sales_df['price_per_unit'] > 0)]

        sales_df['total_sales_value'] = sales_df['quantity'] * sales_df['price_per_unit']

        # Data aggregation
        hourly_aggregated_sales = sales_df.groupby([
            pd.Grouper(key='timestamp', freq='h'),
            'category_id'
        ]).agg(
            total_sales=('total_sales_value', 'sum'),
            total_quantity=('quantity', 'sum')
        ).reset_index()
        hourly_aggregated_sales.rename(columns={'timestamp': 'time'}, inplace=True)

        # **THE FIX**: Use engine.connect() to get a connection, then conn.begin()
        # to start a transaction. This pattern is more compatible with pandas.
        with engine.connect() as conn:
            with conn.begin():  # This starts a transaction and handles commit/rollback
                # 3. Load to Database
                # pandas receives a "plain" connection and uses the active transaction.
                hourly_aggregated_sales.to_sql(
                    'hourly_sales_by_category',
                    conn,
                    if_exists='append',
                    index=False
                )

                # 4. Update ETL status table to SUCCESS within the same transaction
                update_query = text("""
                    INSERT INTO etl_job_status (file_name, status) VALUES (:file_name, 'SUCCESS')
                    ON CONFLICT (file_name) DO UPDATE SET status = 'SUCCESS', last_updated = NOW();
                """)
                conn.execute(update_query, {"file_name": file_name})

        print(f"Successfully processed and loaded '{file_name}'.")

    except Exception as e:
        # 5. The `conn.begin()` block automatically rolls back on failure.
        # Now, update the status to FAILED in a new, separate transaction.
        print(f"Failed to process file {file_name}. Error: {e}")
        with engine.begin() as conn:
            update_query = text("""
                INSERT INTO etl_job_status (file_name, status) VALUES (:file_name, 'FAILED')
                ON CONFLICT (file_name) DO UPDATE SET status = 'FAILED', last_updated = NOW();
            """)
            conn.execute(update_query, {"file_name": file_name})


if __name__ == "__main__":
    process_sales_data()