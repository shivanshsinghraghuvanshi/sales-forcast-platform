from fastapi import FastAPI, HTTPException, BackgroundTasks
from typing import List, Dict, Any

# Assuming these modules exist in your project structure
from .prediction_manager import generate_forecast
from .training_manager import train_and_save_models
from .observability_manager import get_model_versions_for_category, get_performance_for_model_version

app = FastAPI(
    title="Sales Forecasting API & MLOps",
    description="An API to get sales forecasts and manage the model lifecycle.",
    version="2.0.0"
)

# --- Prediction Endpoint ---

@app.get("/forecast/{category_id}", tags=["Forecasting"])
def get_forecast(category_id: str, days: int = 30):
    """
    Generates a sales forecast for a given category for a specified number of future days.
    """
    if days <= 0:
        raise HTTPException(status_code=400, detail="Number of days must be positive.")

    print(f"Received forecast request for category '{category_id}' for {days} days.")

    forecast_result = generate_forecast(category_id, days)

    if "error" in forecast_result:
        raise HTTPException(status_code=404, detail=forecast_result["error"])

    return forecast_result

# --- MLOps & Observability Endpoints ---

@app.post("/training/run", status_code=202, tags=["MLOps"])
def trigger_model_training(background_tasks: BackgroundTasks):
    """
    Triggers a background job to retrain all models for all categories.
    This uses the full MLOps pipeline: training, backtesting, versioning, and saving.
    """
    print("Received request to run model training job.")
    background_tasks.add_task(train_and_save_models)
    return {"message": "Model training job started in the background."}


@app.get("/observability/versions/{category_id}", response_model=List[Dict[str, Any]], tags=["Observability"])
def get_model_version_history(category_id: str):
    """
    Retrieves the complete version history for a specific category's model,
    including backtesting metrics.
    """
    versions = get_model_versions_for_category(category_id)
    if not versions:
        raise HTTPException(status_code=404, detail=f"No model versions found for category '{category_id}'.")
    return versions


@app.get("/observability/performance/{version_id}", response_model=List[Dict[str, Any]], tags=["Observability"])
def get_model_performance_history(version_id: int):
    """
    Retrieves the live performance (drift detection) history for a specific
    model version ID.
    """
    performance_data = get_performance_for_model_version(version_id)
    if not performance_data:
        raise HTTPException(status_code=404, detail=f"No performance data found for model version ID '{version_id}'.")
    return performance_data
