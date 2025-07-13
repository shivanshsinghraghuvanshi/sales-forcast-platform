import pandas as pd
import os

# --- Configuration ---
LOCAL_S3_PATH = "local_s3_bucket/metadata/"

def create_metadata_files(**kwargs):
    """Generates and saves metadata files for products and categories. For Airflow."""
    print("Generating metadata files...")

    # --- Product Metadata ---
    products_data = {
        "product_id": [f"PROD_{i:03d}" for i in range(1, 101)],
        "product_name": [f"Product Name {i}" for i in range(1, 101)],
        "description": [f"Description for product {i}." for i in range(1, 101)]
    }
    products_df = pd.DataFrame(products_data)

    # --- Category Metadata ---
    categories_data = {
        "category_id": [f"CAT_{i:02d}" for i in range(1, 6)],
        "category_name": ["Electronics", "Home & Kitchen", "Apparel", "Books", "Sports"]
    }
    categories_df = pd.DataFrame(categories_data)

    os.makedirs(LOCAL_S3_PATH, exist_ok=True)

    products_file_path = os.path.join(LOCAL_S3_PATH, "products.csv")
    categories_file_path = os.path.join(LOCAL_S3_PATH, "categories.csv")

    products_df.to_csv(products_file_path, index=False)
    categories_df.to_csv(categories_file_path, index=False)

    print(f"Successfully created metadata files at: {products_file_path} and {categories_file_path}")

if __name__ == "__main__":
    create_metadata_files()
