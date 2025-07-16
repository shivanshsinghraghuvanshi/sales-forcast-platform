# forecasting-engine/app/main.py
from typing import List

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import JSONResponse

# Import our new schemas and exceptions
from .schemas import ForecastResponse, ModelVersion, ModelPerformance
from .custom_exceptions import ModelNotFoundError, ModelLoadError

from .prediction_manager import generate_forecast
from .training_manager import train_and_save_models
from .observability_manager import get_model_versions_for_category, get_performance_for_model_version

app = FastAPI(
    title="Sales Forecasting API & MLOps",
    description="An API to get sales forecasts and manage the model lifecycle.",
    version="2.0.0"
)

# --- Exception Handlers ---
# This makes our error handling clean and centralized.
@app.exception_handler(ModelNotFoundError)
async def model_not_found_exception_handler(request: Request, exc: ModelNotFoundError):
    return JSONResponse(
        status_code=404,
        content={"detail": str(exc)},
    )

@app.exception_handler(ModelLoadError)
async def model_load_exception_handler(request: Request, exc: ModelLoadError):
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
    )


# --- Prediction Endpoint ---
@app.get(
    "/forecasts/{category_id}",
    response_model=ForecastResponse,
    tags=["Forecasting"],
    summary="Generate sales forecasts with specified horizon and period",
    description="Generates sales forecasts for a category based on the specified forecast horizon (daily, monthly, or yearly) and the number of periods to forecast.",
)
def get_forecast(
    category_id: str,
    forecast_horizon: str,
    period: int,
) -> ForecastResponse:
    if forecast_horizon not in ["daily", "monthly", "yearly"]:
        raise HTTPException(status_code=400, detail="Invalid forecast_horizon. Choose 'daily', 'monthly', or 'yearly'.")
    if period <= 0:
        raise HTTPException(status_code=400, detail="Period must be a positive integer.")

    return generate_forecast(category_id, forecast_horizon, period)


# --- MLOps & Observability Endpoints ---
@app.post("/training/run", status_code=202, tags=["MLOps"])
def trigger_model_training(background_tasks: BackgroundTasks):
    """
    Triggers a background job to retrain all models for all categories.
    """
    background_tasks.add_task(train_and_save_models)
    return {"message": "Model training job started in the background."}

@app.get("/observability/versions/{category_id}", response_model=List[ModelVersion], tags=["Observability"])
def get_model_version_history(category_id: str):
    """
    Retrieves the complete version history for a specific category's model.
    """
    versions = get_model_versions_for_category(category_id)
    if not versions:
        raise HTTPException(status_code=404, detail=f"No model versions found for category '{category_id}'.")
    return versions

@app.get("/observability/performance/{version_id}", response_model=List[ModelPerformance], tags=["Observability"])
def get_model_performance_history(version_id: int):
    """
    Retrieves the live performance history for a specific model version ID.
    """
    performance_data = get_performance_for_model_version(version_id)
    if not performance_data:
        raise HTTPException(status_code=404, detail=f"No performance data found for model version ID '{version_id}'.")
    return performance_data