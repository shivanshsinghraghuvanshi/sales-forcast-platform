import pandas as pd
import os
import random

# --- Configuration ---
LOCAL_S3_PATH = "./../local_s3_bucket/metadata/"

def create_metadata_files(**kwargs):
    """
    Generates and saves metadata files.
    - Establishes a permanent mapping of product_id to a category_id.
    """
    print("Generating metadata files with product-category relationships...")

    # --- Category Metadata (The reference data) ---
    categories_data = {
        "category_id": [f"CAT_{i:02d}" for i in range(1, 6)],
        "category_name": ["Electronics", "Home & Kitchen", "Apparel", "Books", "Sports"]
    }
    categories_df = pd.DataFrame(categories_data)
    category_ids = categories_df["category_id"].tolist()

    # --- Product Metadata (Assign a category to each product) ---
    product_ids = [f"PROD_{i:03d}" for i in range(1, 101)]
    products_data = {
        "product_id": product_ids,
        "product_name": [f"Product Name {i}" for i in range(1, 101)],
        "description": [f"Description for product {i}." for i in range(1, 101)],
        # Assign a random but permanent category to each product
        "category_id": [random.choice(category_ids) for _ in product_ids]
    }
    products_df = pd.DataFrame(products_data)

    # --- Save to CSV ---
    os.makedirs(LOCAL_S3_PATH, exist_ok=True)

    products_file_path = os.path.join(LOCAL_S3_PATH, "products.csv")
    categories_file_path = os.path.join(LOCAL_S3_PATH, "categories.csv")

    products_df.to_csv(products_file_path, index=False)
    categories_df.to_csv(categories_file_path, index=False)

    print(f"Successfully created metadata files at: {products_file_path} and {categories_file_path}")
    print("-> 'products.csv' now contains a 'category_id' for each product.")

if __name__ == "__main__":
    create_metadata_files()