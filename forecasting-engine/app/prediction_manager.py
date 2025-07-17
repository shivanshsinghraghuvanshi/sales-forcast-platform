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


def generate_delta_forecast(category_id: str, count: int, granularity: str) -> dict:
    """
    Generates a forecast for a given number of future periods, starting from the last cached date.
    """
    engine = get_db_engine()
    model_path = get_latest_model_path(category_id)
    model = load_model(model_path)

    try:
        # 1. Find the last date available in the cache for this granularity
        last_date_query = text("""
            SELECT MAX(forecast_date) FROM live_forecasts
            WHERE category_id = :category_id AND granularity = :granularity
        """)
        with engine.connect() as connection:
            last_cached_date = connection.execute(
                last_date_query, 
                {"category_id": category_id, "granularity": granularity}
            ).scalar_one_or_none()

        # If cache is empty, start forecasting from today
        if last_cached_date is None:
            last_cached_date = pd.Timestamp.now().date()

        # 2. *** FIX: Manually construct the future date range ***
        # This ensures the dates are always correct, regardless of the model's history.
        freq_map = {'daily': 'D', 'monthly': 'M', 'yearly': 'Y'}
        if granularity not in freq_map:
            raise ValueError(f"Invalid granularity '{granularity}'.")
        
        # Calculate the start date for our new forecast range
        start_date = pd.to_datetime(last_cached_date) + pd.DateOffset(days=1)
        
        # Create the date range manually
        future_dates = pd.date_range(start=start_date, periods=count, freq=freq_map[granularity])
        future_df = pd.DataFrame({'ds': future_dates})
        
        # 3. Incorporate holidays/promotions
        future_holidays = fetch_holidays_for_category(engine, category_id)
        if future_holidays is not None:
            future_df = pd.merge(future_df, future_holidays, on='ds', how='left')

        # If after the merge, the dataframe is empty, we can't predict.
        if future_df.empty:
            raise ValueError("Dataframe has no rows after date range creation and holiday merge. Cannot predict.")

        forecast_df = model.predict(future_df)

        # 4. Format and return the results
        results = (
            forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
            .rename(columns={'ds': 'forecast_date', 'yhat': 'predicted_sales', 'yhat_lower': 'lower_bound', 'yhat_upper': 'upper_bound'})
        )
        results['category_id'] = category_id
        with engine.connect() as conn:
            version_query = text("SELECT id FROM model_versions WHERE model_path = :path")
            results['model_version_id'] = conn.execute(version_query, {"path": model_path}).scalar_one()
        results['granularity'] = granularity

        return results.to_dict('records')

    except Exception as e:
        logger.error(f"An error occurred during delta prediction for category {category_id}: {e}")
        raise