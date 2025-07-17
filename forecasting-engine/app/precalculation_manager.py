import pandas as pd
import logging
from sqlalchemy import text
# Assuming these modules exist in your project structure
from .db_utils import get_db_engine
from .prediction_manager import get_latest_model_path, load_model
from .training_manager import fetch_holidays_for_category

# Set up a logger for this module
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_standard_forecasts(model, engine, category_id):
    """
    Generates a standard set of daily, monthly, and yearly forecasts for a given model.
    """
    all_forecasts = []
    
    # Fetch future holidays once to be used by all granularities
    future_holidays = fetch_holidays_for_category(engine, category_id)

    # 1. Daily for 90 days
    future_d = model.make_future_dataframe(periods=90, freq='D')
    if future_holidays is not None:
        future_d = pd.merge(future_d, future_holidays, on='ds', how='left')
    fcst_d = model.predict(future_d).tail(90)
    fcst_d['granularity'] = 'daily'
    all_forecasts.append(fcst_d)

    # 2. Monthly for 24 months
    future_m = model.make_future_dataframe(periods=24, freq='M')
    if future_holidays is not None:
        future_m = pd.merge(future_m, future_holidays, on='ds', how='left')
    fcst_m = model.predict(future_m).tail(24)
    fcst_m['granularity'] = 'monthly'
    all_forecasts.append(fcst_m)
    
    # 3. Yearly for 5 years
    future_y = model.make_future_dataframe(periods=5, freq='Y')
    if future_holidays is not None:
        future_y = pd.merge(future_y, future_holidays, on='ds', how='left')
    fcst_y = model.predict(future_y).tail(5)
    fcst_y['granularity'] = 'yearly'
    all_forecasts.append(fcst_y)

    # Combine and format the results into a single DataFrame
    combined_df = pd.concat(all_forecasts)
    combined_df = combined_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'granularity']]
    combined_df.rename(columns={
        'ds': 'forecast_date',
        'yhat': 'predicted_sales',
        'yhat_lower': 'lower_bound',
        'yhat_upper': 'upper_bound'
    }, inplace=True)
    
    combined_df['category_id'] = category_id
    
    return combined_df

def run_precalculation_job():
    """
    The main job to refresh the forecast cache. It archives old forecasts
    and generates new ones.
    """
    logger.info("--- Starting pre-calculated forecast refresh job ---")
    engine = get_db_engine()
    
    # 1. Archive old forecasts and clear the live cache in a single transaction
    try:
        with engine.begin() as connection:
            logger.info("Archiving old forecasts from live_forecasts to historical_forecasts...")
            archive_sql = text("""
                INSERT INTO historical_forecasts 
                (model_version_id, category_id, forecast_date, predicted_sales, lower_bound, upper_bound, granularity)
                SELECT model_version_id, category_id, forecast_date, predicted_sales, lower_bound, upper_bound, granularity
                FROM live_forecasts;
            """)
            connection.execute(archive_sql)
            
            logger.info("Clearing the live_forecasts cache table...")
            connection.execute(text("DELETE FROM live_forecasts;"))
        logger.info("Archiving and clearing complete.")
    except Exception as e:
        logger.error(f"Failed during archiving and clearing step: {e}")
        return {"status": "failed", "reason": "Archiving failed"}

    # 2. Get all categories that have a latest model
    with engine.connect() as connection:
        categories_query = text("SELECT DISTINCT category_id FROM model_versions WHERE is_latest = TRUE")
        categories = connection.execute(categories_query).scalars().all()

    # 3. Generate and insert new forecasts into the live cache for each category
    for category_id in categories:
        logger.info(f"Generating new forecast cache for category: {category_id}")
        try:
            model_path = get_latest_model_path(category_id)
            model = load_model(model_path)
            
            # Get the model version ID to link the forecasts
            version_query = text("SELECT id FROM model_versions WHERE model_path = :path")
            with engine.connect() as conn:
                model_version_id = conn.execute(version_query, {"path": model_path}).scalar_one()

            # Generate the standard set of daily, monthly, and yearly forecasts
            forecasts_df = generate_standard_forecasts(model, engine, category_id)
            forecasts_df['model_version_id'] = model_version_id

            # Insert the new forecasts into the now-empty live_forecasts table
            with engine.connect() as connection:
                logger.info(f"Inserting {len(forecasts_df)} new cached forecasts into live_forecasts for category: {category_id}")
                forecasts_df.to_sql('live_forecasts', connection, if_exists='append', index=False)
            
            logger.info(f"Successfully updated cache for category: {category_id}")

        except Exception as e:
            logger.error(f"Failed to update cache for category {category_id}: {e}")

    logger.info("--- Pre-calculated forecast refresh job finished ---")
    return {"status": "success"}

if __name__ == "__main__":
    run_precalculation_job()
