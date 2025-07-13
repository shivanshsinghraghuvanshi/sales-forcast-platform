import pandas as pd
import os
import joblib
import json
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from sqlalchemy import create_engine, text
from datetime import datetime

# --- Configuration ---
MODEL_REGISTRY_PATH = "model_registry/"
DB_CONNECTION_STRING = "postgresql://admin:password@localhost:5432/sales_db"


def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    return create_engine(DB_CONNECTION_STRING)


def fetch_training_data(engine):
    """Fetches hourly aggregated sales data from the database."""
    print("Fetching training data from database...")
    query = "SELECT time, category_id, total_sales FROM hourly_sales_by_category"
    df = pd.read_sql(query, engine)

    df.rename(columns={'time': 'ds', 'total_sales': 'y'}, inplace=True)
    df['ds'] = pd.to_datetime(df['ds'])

    print(f"Successfully fetched {len(df)} records.")
    return df


def run_backtesting(model, training_df):
    """
    Performs cross-validation to get model accuracy metrics.
    """
    print("Running backtesting (cross-validation)...")
    # Use 10% of the data as the forecast horizon for testing
    horizon_days = max(30, int(len(training_df) * 0.1))  # Ensure at least 30 days horizon
    horizon = f'{horizon_days} days'

    df_cv = cross_validation(model, initial=f'{len(training_df) - horizon_days} days',
                             period=f'{horizon_days // 2} days', horizon=horizon, parallel="processes")
    df_p = performance_metrics(df_cv)

    print("Backtesting complete. Performance metrics:")
    print(df_p.head())

    # Return the performance metrics as a dictionary
    return df_p.to_dict('records')


def save_model_and_metadata_to_db(engine, model, category, training_df, backtesting_metrics):
    """
    Saves a model file and records its metadata and accuracy in the database.
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    category_path = os.path.join(MODEL_REGISTRY_PATH, category)
    version_path = os.path.join(category_path, timestamp)
    model_filepath = os.path.join(version_path, "model.joblib")

    metadata = {
        "training_data_points": len(training_df),
        "data_start_date": str(training_df['ds'].min()),
        "data_end_date": str(training_df['ds'].max()),
    }

    try:
        os.makedirs(version_path, exist_ok=True)
        joblib.dump(model, model_filepath)
        print(f"Model file saved to: {model_filepath}")

        with engine.begin() as connection:
            unmark_latest_sql = text("UPDATE model_versions SET is_latest = FALSE WHERE category_id = :category_id")
            connection.execute(unmark_latest_sql, {"category_id": category})

            insert_sql = text("""
                              INSERT INTO model_versions
                              (category_id, version, model_path, training_date_utc, is_latest, metadata,
                               backtesting_metrics)
                              VALUES (:category_id, :version, :model_path, :training_date, TRUE, :metadata, :metrics)
                              """)
            connection.execute(
                insert_sql,
                {
                    "category_id": category,
                    "version": timestamp,
                    "model_path": model_filepath,
                    "training_date": datetime.utcnow(),
                    "metadata": json.dumps(metadata),
                    "metrics": json.dumps(backtesting_metrics)
                }
            )
            print(f"Successfully inserted new model version '{timestamp}' into database.")

    except Exception as e:
        print(f"An error occurred during model saving. Rolling back. Error: {e}")
        if os.path.exists(model_filepath):
            os.remove(model_filepath)
        raise


def train_and_save_models():
    """Main function to orchestrate the model training and evaluation process."""
    engine = get_db_engine()
    all_data = fetch_training_data(engine)

    if all_data.empty:
        print("No training data found. Aborting.")
        return {"status": "failed", "reason": "No data in database"}

    trained_models = []
    categories = all_data['category_id'].unique()

    for category in categories:
        print(f"\n--- Processing category: {category} ---")
        category_df = all_data[all_data['category_id'] == category].copy()

        if len(category_df) < 100:  # Need enough data for meaningful backtesting
            print(f"Skipping category '{category}' due to insufficient data for backtesting.")
            continue

        try:
            model = Prophet()
            model.fit(category_df[['ds', 'y']])

            backtesting_metrics = run_backtesting(model, category_df)

            save_model_and_metadata_to_db(engine, model, category, category_df, backtesting_metrics)

            trained_models.append(category)
        except Exception as e:
            print(f"Failed to complete training for category '{category}'. Error: {e}")

    return {"status": "success", "trained_categories": trained_models}


if __name__ == "__main__":
    train_and_save_models()
