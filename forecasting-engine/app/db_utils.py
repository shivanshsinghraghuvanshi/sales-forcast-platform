from sqlalchemy import create_engine

# --- Configuration ---
DB_CONNECTION_STRING = "postgresql://admin:password@localhost:5432/sales_db"

def get_db_engine():
    """Creates and returns a SQLAlchemy engine for the database."""
    return create_engine(DB_CONNECTION_STRING)
