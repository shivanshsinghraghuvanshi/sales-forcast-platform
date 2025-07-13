import pandas as pd
import os
from .db_utils import get_db_engine

RAW_METADATA_PATH = "./../ingestion_layer/local_s3_bucket/metadata/"


def process_file(engine, file_name, table_name, **kwargs):
    """A generic function to process and load a single metadata file."""
    file_path = os.path.join(RAW_METADATA_PATH, file_name)

    # 1. Check ETL status
    status_query = f"SELECT status FROM etl_job_status WHERE file_name = '{file_name}'"
    with engine.connect() as conn:
        result = conn.execute(status_query).scalar_one_or_none()

    if result == 'SUCCESS':
        print(f"File '{file_name}' has already been processed successfully. Skipping.")
        return

    if not os.path.exists(file_path):
        print(f"Error: Metadata file not found at {file_path}.")
        return

    # 2. Extract, Transform, Load
    try:
        print(f"Processing metadata file: {file_name}")
        df = pd.read_csv(file_path)

        # Load to database, replacing the entire table to ensure data is fresh
        df.to_sql(table_name, engine, if_exists='replace', index=False, **kwargs)

        # 3. Update ETL status
        update_query = f"""
        INSERT INTO etl_job_status (file_name, status) VALUES ('{file_name}', 'SUCCESS')
        ON CONFLICT (file_name) DO UPDATE SET status = 'SUCCESS', last_updated = NOW();
        """
        with engine.connect() as conn:
            conn.execute(update_query)
        print(f"Successfully processed and loaded '{file_name}' to table '{table_name}'.")

    except Exception as e:
        print(f"Failed to process file {file_name}. Error: {e}")
        update_query = f"""
        INSERT INTO etl_job_status (file_name, status) VALUES ('{file_name}', 'FAILED')
        ON CONFLICT (file_name) DO UPDATE SET status = 'FAILED', last_updated = NOW();
        """
        with engine.connect() as conn:
            conn.execute(update_query)


def process_metadata():
    """Processes and loads all metadata files."""
    engine = get_db_engine()

    # Process categories first due to foreign key relationship
    process_file(engine, "categories.csv", "categories", dtype={'category_id': 'TEXT', 'category_name': 'TEXT'})

    # Process products
    process_file(engine, "products.csv", "products",
                 dtype={'product_id': 'TEXT', 'product_name': 'TEXT', 'description': 'TEXT', 'category_id': 'TEXT'})


if __name__ == "__main__":
    process_metadata()
