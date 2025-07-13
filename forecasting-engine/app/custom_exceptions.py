# forecasting-engine/app/custom_exceptions.py
class ModelNotFoundError(Exception):
    """Raised when a model for a given category is not found in the database."""
    pass

class ModelLoadError(Exception):
    """Raised when a model file exists but fails to load."""
    pass