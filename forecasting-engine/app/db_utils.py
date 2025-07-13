# forecasting-engine/app/db_utils.py
from sqlalchemy import create_engine
from .settings import settings # Import the settings instance

def get_db_engine():
    """Creates and returns a SQLAlchemy engine from application settings."""
    # The connection string is now managed centrally
    return create_engine(settings.db_connection_string)