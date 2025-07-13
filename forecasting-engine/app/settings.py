# forecasting-engine/app/settings.py
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """Manages application configuration using environment variables."""
    # Default value is used if the environment variable is not set
    db_connection_string: str = "postgresql://admin:password@localhost:5432/sales_db"

    # This tells Pydantic to look for a .env file
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')

# Create a single, importable instance of the settings
settings = Settings()