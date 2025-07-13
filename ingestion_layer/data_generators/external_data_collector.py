import pandas as pd
from datetime import datetime, timedelta
import os
import random

# --- Configuration ---
LOCAL_S3_PATH = "./../local_s3_bucket/external/"
METADATA_PRODUCTS_PATH = "./../local_s3_bucket/metadata/products.csv"
METADATA_CATEGORIES_PATH = "./../local_s3_bucket/metadata/categories.csv"

def create_external_data_files(**kwargs):
    """
    Simulates fetching external data, linking promotions to specific
    products or categories.
    """
    print("Generating external promotions data file with targets...")

    # --- Load product and category IDs to use as targets ---
    try:
        product_ids = pd.read_csv(METADATA_PRODUCTS_PATH)['product_id'].tolist()
        category_ids = pd.read_csv(METADATA_CATEGORIES_PATH)['category_id'].tolist()
        print("Successfully loaded product and category IDs for promotion targeting.")
    except FileNotFoundError:
        print("Error: Metadata files not found. Cannot generate targeted promotions.")
        return

    # --- Promotions Data ---
    today = datetime.now()
    promotions_data = {
        "promotion_id": [],
        "promotion_name": [],
        "start_date": [],
        "end_date": [],
        "discount_percentage": [],
        "target_type": [], # New column: 'product' or 'category'
        "target_id": []    # New column: The ID of the product/category
    }

    promo_names = ["Summer Sale", "Back to School", "Holiday Special", "Flash Sale", "Weekend Deal", "Clearance"]
    for i, name in enumerate(promo_names):
        promotions_data["promotion_id"].append(f"PROMO_{i+1:03d}")
        promotions_data["promotion_name"].append(name)
        promotions_data["start_date"].append((today + timedelta(days=i*10)).strftime('%Y-%m-%d'))
        promotions_data["end_date"].append((today + timedelta(days=i*10 + 5)).strftime('%Y-%m-%d'))
        promotions_data["discount_percentage"].append(random.choice([10, 15, 20, 25, 30]))

        # Randomly decide if the promo targets a product or a category
        if random.random() > 0.5:
            promotions_data["target_type"].append("product")
            promotions_data["target_id"].append(random.choice(product_ids))
        else:
            promotions_data["target_type"].append("category")
            promotions_data["target_id"].append(random.choice(category_ids))

    promotions_df = pd.DataFrame(promotions_data)

    os.makedirs(LOCAL_S3_PATH, exist_ok=True)
    file_path = os.path.join(LOCAL_S3_PATH, "promotions.csv")
    promotions_df.to_csv(file_path, index=False)

    print(f"Successfully created external data file at: {file_path}")
    print("-> 'promotions.csv' now contains 'target_type' and 'target_id' columns.")

if __name__ == "__main__":
    create_external_data_files()