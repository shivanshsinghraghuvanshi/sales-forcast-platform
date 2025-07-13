# forecasting-engine/app/schemas.py
from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import datetime

# --- Prediction Schemas ---
class ForecastPoint(BaseModel):
    date: str
    predicted_sales: float
    lower_bound: float
    upper_bound: float

class ForecastResponse(BaseModel):
    category_id: str
    forecast: List[ForecastPoint]

# --- Observability Schemas ---
class ModelVersion(BaseModel):
    id: int
    category_id: str
    version: str
    model_path: str
    training_date_utc: datetime
    is_latest: bool
    metadata: Dict[str, Any] | None = None
    backtesting_metrics: Dict[str, Any] | None = None

class ModelPerformance(BaseModel):
    id: int
    model_version_id: int
    evaluation_period_start: datetime
    evaluation_period_end: datetime
    metric_name: str
    metric_value: float