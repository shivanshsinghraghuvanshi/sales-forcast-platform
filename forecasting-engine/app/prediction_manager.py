# forecasting-engine/app/prediction_manager.py
import os
import joblib
import logging
from sqlalchemy import text
from .db_utils import get_db_engine
from .custom_exceptions import ModelNotFoundError, ModelLoadError

# Set up a logger for this module
logger = logging.getLogger(__name__)

def get_latest_model_path(category_id: str) -> str:
    """
    Queries the database for the latest model path for a category.
    Raises ModelNotFoundError if no model is found.
    """
    logger.info(f"Querying database for the latest model for category: {category_id}")
    engine = get_db_engine()
    query = text("SELECT model_path FROM model_versions WHERE category_id = :category_id AND is_latest = TRUE LIMIT 1")

    with engine.connect() as connection:
        result = connection.execute(query, {"category_id": category_id}).scalar_one_or_none()

    if not result:
        raise ModelNotFoundError(f"No 'latest' model found for category '{category_id}' in the database.")

    logger.info(f"Found model path: {result}")
    return result

def load_model(model_path: str):
    """
    Loads a model from a file path.
    Raises ModelLoadError if the file is missing or fails to load.
    """
    if not os.path.exists(model_path):
        raise ModelLoadError(f"Model file does not exist at path: {model_path}")

    try:
        model = joblib.load(model_path)
        return model
    except Exception as e:
        raise ModelLoadError(f"Failed to load model from {model_path}. Error: {e}")

def generate_forecast(category_id: str, days: int = 30) -> dict:
    """
    Generates a sales forecast for a specific category.
    Raises exceptions on failure.
    """
    # 1. Find and load the model (will raise exceptions on failure)
    model_path = get_latest_model_path(category_id)
    model = load_model(model_path)

    # 2. Use the model to make a forecast
    try:
        future_df = model.make_future_dataframe(periods=days)
        forecast_df = model.predict(future_df)

        # 3. Format the output (using method chaining instead of inplace=True)
        results = (
            forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
            .tail(days)
            .rename(columns={
                'ds': 'date',
                'yhat': 'predicted_sales',
                'yhat_lower': 'lower_bound',
                'yhat_upper': 'upper_bound'
            })
        )
        results['date'] = results['date'].dt.strftime('%Y-%m-%d')

        return {"category_id": category_id, "forecast": results.to_dict('records')}

    except Exception as e:
        logger.error(f"An error occurred during prediction for category {category_id}: {e}")
        raise  # Re-raise the exception to be handled by the API layer