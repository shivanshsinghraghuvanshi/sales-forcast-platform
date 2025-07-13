import json
import time
import pandas as pd
from faker import Faker
from kafka import KafkaProducer
from datetime import datetime, timedelta
import random
import os

# --- Configuration ---
KAFKA_TOPIC = "sales_transactions"
KAFKA_SERVER = "localhost:9092"
LOCAL_S3_PATH = "./../local_s3_bucket/sales/"
METADATA_PATH = "./../local_s3_bucket/metadata/products.csv"

# --- Load Product-to-Category Mapping ---
try:
    # This mapping is the single source of truth for product-category relationships
    product_df = pd.read_csv(METADATA_PATH)
    PRODUCT_CATEGORY_MAP = pd.Series(product_df.category_id.values, index=product_df.product_id).to_dict()
    print("Successfully loaded product-to-category map.")
except FileNotFoundError:
    print(f"Error: Metadata file not found at {METADATA_PATH}.")
    print("Please run the metadata_collector.py script first.")
    PRODUCT_CATEGORY_MAP = {} # Prevent crash if file doesn't exist

# Initialize Faker
fake = Faker()

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka Producer initialized successfully.")
except Exception as e:
    print(f"Error initializing Kafka Producer: {e}. Real-time streaming will be disabled.")
    producer = None


def generate_sales_transaction(date_for_transaction: datetime):
    """
    Generates a single fake sales transaction ensuring the product_id
    and category_id are consistent for a specific date.
    """
    if not PRODUCT_CATEGORY_MAP:
        return None # Cannot generate data without the map

    # 1. Pick a random product
    product_id = random.choice(list(PRODUCT_CATEGORY_MAP.keys()))
    # 2. Look up its correct category
    category_id = PRODUCT_CATEGORY_MAP[product_id]

    # Create a random time within the given day
    transaction_time = date_for_transaction.replace(
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=random.randint(0, 999999)
    )

    return {
        "transaction_id": fake.uuid4(),
        "product_id": product_id,
        "category_id": category_id, # This is now consistent
        "quantity": random.randint(1, 5),
        "price_per_unit": round(random.uniform(10.5, 200.5), 2),
        "timestamp": transaction_time.isoformat()
    }


def create_sales_batch_files(days=100, transactions_per_day=1000, **kwargs):
    """Generates batches of sales data for a specified number of days and saves them as daily CSV files."""
    if not PRODUCT_CATEGORY_MAP:
        print("Could not generate sales file because product map is missing.")
        return

    print(f"Generating sales batch files for {days} days...")
    # Generate data for the past `days` days, ending yesterday.
    start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days)

    for i in range(days):
        current_date = start_date + timedelta(days=i)
        print(f"  - Generating data for {current_date.strftime('%Y-%m-%d')}...")

        transactions = [generate_sales_transaction(current_date) for _ in range(transactions_per_day)]
        # Filter out any None values that might occur if the map fails to load
        transactions = [t for t in transactions if t is not None]

        if not transactions:
            print(f"    - No transactions generated for {current_date.strftime('%Y-%m-%d')}. Skipping file creation.")
            continue

        df = pd.DataFrame(transactions)

        os.makedirs(LOCAL_S3_PATH, exist_ok=True)
        file_date_str = current_date.strftime('%Y-%m-%d')
        file_path = os.path.join(LOCAL_S3_PATH, f"{file_date_str}.csv")
        df.to_csv(file_path, index=False)
        print(f"    - Successfully created sales batch file at: {file_path}")
    print("Finished generating all sales files.")


if __name__ == "__main__":
    # You can specify the number of days and transactions per day here
    create_sales_batch_files(days=100, transactions_per_day=1000)