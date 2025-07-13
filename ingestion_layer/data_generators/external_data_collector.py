import pandas as pd
from datetime import datetime, timedelta
import os

# --- Configuration ---
LOCAL_S3_PATH = "./../local_s3_bucket/external/"

def create_external_data_files(**kwargs):
    """Simulates fetching external data and saves it to a file. For Airflow."""
    print("Generating external promotions data file...")

    # --- Promotions Data ---
    today = datetime.now()
    promotions_data = {
        "promotion_id": [f"PROMO_{i:03d}" for i in range(1, 5)],
        "promotion_name": ["Summer Sale", "Back to School", "Holiday Special", "Flash Sale"],
        "start_date": [(today + timedelta(days=i*10)).strftime('%Y-%m-%d') for i in range(4)],
        "end_date": [(today + timedelta(days=i*10 + 5)).strftime('%Y-%m-%d') for i in range(4)],
        "discount_percentage": [15, 20, 25, 10]
    }
    promotions_df = pd.DataFrame(promotions_data)

    os.makedirs(LOCAL_S3_PATH, exist_ok=True)

    file_path = os.path.join(LOCAL_S3_PATH, "promotions.csv")
    promotions_df.to_csv(file_path, index=False)

    print(f"Successfully created external data file at: {file_path}")

if __name__ == "__main__":
    create_external_data_files()
