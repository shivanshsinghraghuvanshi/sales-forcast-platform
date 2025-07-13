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

# Initialize Faker for generating realistic data
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
    """Generates a single fake sales transaction."""
    return {
        "transaction_id": fake.uuid4(),
        "product_id": f"PROD_{random.randint(1, 100):03d}",
        "category_id": f"CAT_{random.randint(1, 5):02d}",
        "quantity": random.randint(1, 5),
        "price_per_unit": round(random.uniform(10.5, 200.5), 2),
        "timestamp": datetime.now().isoformat()
    }


def stream_real_time_sales(duration_seconds=60):
    """Streams sales data to Kafka for a given duration."""
    if not producer:
        print("Cannot stream sales data: Kafka producer is not available.")
        return

    print(f"Starting real-time sales stream to Kafka topic '{KAFKA_TOPIC}' for {duration_seconds} seconds...")
    end_time = time.time() + duration_seconds
    while time.time() < end_time:
        transaction = generate_sales_transaction()
        producer.send(KAFKA_TOPIC, transaction)
        print(f"Sent: {transaction['transaction_id']}")
        time.sleep(random.uniform(0.5, 2.0))
    producer.flush()
    print("Finished real-time sales stream.")


def create_daily_sales_batch_file(**kwargs):
    """Generates a batch of sales data and saves it as a CSV. For Airflow."""
    print("Generating daily sales batch file...")
    transactions = [generate_sales_transaction() for _ in range(1000)]
    df = pd.DataFrame(transactions)

    os.makedirs(LOCAL_S3_PATH, exist_ok=True)
    file_date = datetime.now().strftime('%Y-%m-%d')
    file_path = os.path.join(LOCAL_S3_PATH, f"sales_{file_date}.csv")
    df.to_csv(file_path, index=False)
    print(f"Successfully created daily sales batch file at: {file_path}")


if __name__ == "__main__":
    # To test real-time streaming:
    # stream_real_time_sales(duration_seconds=30)

    # To test batch file creation:
    create_daily_sales_batch_file()
