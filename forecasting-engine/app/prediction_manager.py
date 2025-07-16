# forecasting-engine/app/prediction_manager.py
import os
import joblib
import logging
import pandas as pd
from sqlalchemy import text
from .db_utils import get_db_engine
from .custom_exceptions import ModelNotFoundError, ModelLoadError
# *** NEW: Import the function to get promotion data ***
from .training_manager import fetch_holidays_for_category

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

def generate_forecast(category_id: str, forecast_horizon: str, period: int) -> dict:
    """
    Generates a sales forecast for a specific category.
    Raises exceptions on failure.
    """
    engine = get_db_engine()
    
    # 1. Find and load the model
    model_path = get_latest_model_path(category_id)
    model = load_model(model_path)

    # 2. Use the model to make a forecast
    try:
        # Create the future dataframe
        if forecast_horizon == "daily":
            future_df = model.make_future_dataframe(periods=int(period), freq='D')
        elif forecast_horizon == "monthly":
            future_df = model.make_future_dataframe(periods=int(period), freq='ME')
        elif forecast_horizon == "yearly":
            future_df = model.make_future_dataframe(periods=int(period), freq='Y')
        else:
            raise ValueError(f"Invalid forecast_horizon: {forecast_horizon}.")
            
        # *** NEW: Fetch future promotions and add them to the future dataframe ***
        # The model was trained to recognize these holiday names.
        future_holidays = fetch_holidays_for_category(engine, category_id)
        if future_holidays is not None:
            # Prophet automatically uses holiday information present in the future dataframe.
            future_df = pd.merge(future_df, future_holidays, on='ds', how='left')

        # Generate the forecast
        forecast_df = model.predict(future_df)

        # 3. Format the output
        results = (
            forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
            .tail(period)
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
        raise
