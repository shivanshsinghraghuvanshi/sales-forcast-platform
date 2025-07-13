import os
from sqlalchemy import create_engine


def get_db_engine():
    """Creates and returns a SQLAlchemy engine for the database."""
    db_user = os.getenv("DB_USER", "admin")
    db_pass = os.getenv("DB_PASSWORD", "password")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "sales_db")

    connection_string = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)
    return engine