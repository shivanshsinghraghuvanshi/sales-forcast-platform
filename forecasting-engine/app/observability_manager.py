from sqlalchemy import create_engine, text
from .db_utils import get_db_engine

def get_model_versions_for_category(category_id: str):
    """
    Retrieves all model version history for a specific category from the database.
    """
    engine = get_db_engine()
    query = text("""
        SELECT version, model_path, training_date_utc, is_latest, backtesting_metrics
        FROM model_versions
        WHERE category_id = :category_id
        ORDER BY training_date_utc DESC
    """)
    with engine.connect() as connection:
        results = connection.execute(query, {"category_id": category_id}).mappings().all()
    return results if results else []

def get_performance_for_model_version(version_id: int):
    """
    Retrieves all live performance metrics for a specific model version.
    """
    engine = get_db_engine()
    query = text("""
        SELECT evaluation_period_start, evaluation_period_end, metric_name, metric_value
        FROM forecast_performance
        WHERE model_version_id = :version_id
        ORDER BY evaluation_period_end DESC
    """)
    with engine.connect() as connection:
        results = connection.execute(query, {"version_id": version_id}).mappings().all()
    return results if results else []
