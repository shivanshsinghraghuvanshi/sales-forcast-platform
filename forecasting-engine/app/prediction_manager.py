import os
import joblib
from sqlalchemy import text
from .db_utils import get_db_engine

def get_latest_model_path(category_id: str):
    """
    Queries the database to find the file path of the latest model for a given category.
    """
    print(f"Querying database for the latest model for category: {category_id}")
    engine = get_db_engine()

    query = text("""
        SELECT model_path FROM model_versions
        WHERE category_id = :category_id AND is_latest = TRUE
        LIMIT 1
    """)

    with engine.connect() as connection:
        result = connection.execute(query, {"category_id": category_id}).scalar_one_or_none()

    if not result:
        print(f"No 'latest' model found for category '{category_id}' in the database.")
        return None

    print(f"Found model path: {result}")
    return result

def load_model(model_path: str):
    """Loads a model from a given file path."""
    if not os.path.exists(model_path):
        print(f"Model file does not exist at path: {model_path}")
        return None

    try:
        model = joblib.load(model_path)
        return model
    except Exception as e:
        print(f"Failed to load model from {model_path}. Error: {e}")
        return None

def generate_forecast(category_id: str, days: int = 30):
    """
    Generates a sales forecast for a specific category.
    """
    # 1. Find the latest model from the database
    model_path = get_latest_model_path(category_id)
    if not model_path:
        return {"error": f"No model available for category '{category_id}'."}

    # 2. Load the model file
    model = load_model(model_path)
    if not model:
        return {"error": f"Failed to load model for category '{category_id}'."}

    # 3. Use the model to make a forecast
    try:
        future_df = model.make_future_dataframe(periods=days)
        forecast_df = model.predict(future_df)

        # 4. Format the output for the API response
        results = forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(days)
        results.rename(columns={
            'ds': 'date',
            'yhat': 'predicted_sales',
            'yhat_lower': 'lower_bound',
            'yhat_upper': 'upper_bound'
        }, inplace=True)

        # Convert dates to string for JSON serialization
        results['date'] = results['date'].dt.strftime('%Y-%m-%d')

        return {"category_id": category_id, "forecast": results.to_dict('records')}

    except Exception as e:
        return {"error": f"An error occurred during prediction. Error: {e}"}
