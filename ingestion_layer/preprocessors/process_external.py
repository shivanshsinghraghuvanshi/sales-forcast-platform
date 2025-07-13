import pandas as pd
import os
from .db_utils import get_db_engine

RAW_EXTERNAL_PATH = "./../ingestion_layer/local_s3_bucket/external/"


def process_external_data():
    """Processes and loads the external promotions data file."""
    engine = get_db_engine()
    file_name = "promotions.csv"
    file_path = os.path.join(RAW_EXTERNAL_PATH, file_name)

    # 1. Check ETL status
    status_query = f"SELECT status FROM etl_job_status WHERE file_name = '{file_name}'"
    with engine.connect() as conn:
        result = conn.execute(status_query).scalar_one_or_none()

    if result == 'SUCCESS':
        print(f"File '{file_name}' has already been processed successfully. Skipping.")
        return

    if not os.path.exists(file_path):
        print(f"Error: External data file not found at {file_path}.")
        return

    # 2. Extract, Transform, Load
    try:
        print(f"Processing external file: {file_name}")
        df = pd.read_csv(file_path)

        # Ensure data types are correct for the database
        df['start_date'] = pd.to_datetime(df['start_date'])
        df['end_date'] = pd.to_datetime(df['end_date'])

        # Load to database, replacing the table
        df.to_sql('promotions', engine, if_exists='replace', index=False)

        # 3. Update ETL status
        update_query = f"""
        INSERT INTO etl_job_status (file_name, status) VALUES ('{file_name}', 'SUCCESS')
        ON CONFLICT (file_name) DO UPDATE SET status = 'SUCCESS', last_updated = NOW();
        """
        with engine.connect() as conn:
            conn.execute(update_query)
        print(f"Successfully processed and loaded '{file_name}'.")

    except Exception as e:
        print(f"Failed to process file {file_name}. Error: {e}")
        update_query = f"""
        INSERT INTO etl_job_status (file_name, status) VALUES ('{file_name}', 'FAILED')
        ON CONFLICT (file_name) DO UPDATE SET status = 'FAILED', last_updated = NOW();
        """
        with engine.connect() as conn:
            conn.execute(update_query)


if __name__ == "__main__":
    process_external_data()
