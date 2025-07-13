import json
import time
import pandas as pd
from faker import Faker
from kafka import KafkaProducer
from datetime import datetime
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


def generate_sales_transaction():
    """
    Generates a single fake sales transaction ensuring the product_id
    and category_id are consistent.
    """
    if not PRODUCT_CATEGORY_MAP:
        return None # Cannot generate data without the map

    # 1. Pick a random product
    product_id = random.choice(list(PRODUCT_CATEGORY_MAP.keys()))
    # 2. Look up its correct category
    category_id = PRODUCT_CATEGORY_MAP[product_id]

    return {
        "transaction_id": fake.uuid4(),
        "product_id": product_id,
        "category_id": category_id, # This is now consistent
        "quantity": random.randint(1, 5),
        "price_per_unit": round(random.uniform(10.5, 200.5), 2),
        "timestamp": datetime.now().isoformat()
    }


def create_daily_sales_batch_file(**kwargs):
    """Generates a batch of sales data and saves it as a CSV."""
    if not PRODUCT_CATEGORY_MAP:
        print("Could not generate sales file because product map is missing.")
        return

    print("Generating daily sales batch file with consistent product-category data...")
    transactions = [generate_sales_transaction() for _ in range(1000)]
    # Filter out any None values that might occur if the map fails to load
    transactions = [t for t in transactions if t is not None]

    df = pd.DataFrame(transactions)

    os.makedirs(LOCAL_S3_PATH, exist_ok=True)
    file_date = datetime.now().strftime('%Y-%m-%d')
    file_path = os.path.join(LOCAL_S3_PATH, f"sales_{file_date}.csv")
    df.to_csv(file_path, index=False)
    print(f"Successfully created daily sales batch file at: {file_path}")


if __name__ == "__main__":
    create_daily_sales_batch_file()