#!/usr/bin/env python3
import requests
import json
from datetime import datetime

def get_sales_forecast(category_id, forecast_horizon, period):
    """
    Get sales forecast for a specific category
    
    Args:
        category_id (str): The category ID (e.g., 'CAT_01')
        forecast_horizon (str): The granularity of the forecast ('daily', 'monthly', 'yearly')
        period (int): Number of periods to forecast
        
    Returns:
        dict: The forecast response
    """
    # Assuming the API is running locally on port 8000
    base_url = "http://localhost:8000"
    
    # Construct the API endpoint URL
    url = f"{base_url}/forecasts/{category_id}?forecast_horizon={forecast_horizon}&period={period}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for error status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching forecast: {e}")
        return None

def format_forecast_as_string(forecast_data):
    """
    Format the forecast data as a readable string
    
    Args:
        forecast_data (dict): The forecast response from the API
        
    Returns:
        str: Formatted string representation of the forecast
    """
    if not forecast_data:
        return "No forecast data available"
    
    category_id = forecast_data['category_id']
    forecasts = forecast_data['forecast']
    
    result = [f"Daily Sales Forecast for Category: {category_id}"]
    result.append("=" * 50)
    result.append(f"{'Date':<12} | {'Predicted Sales':>15} | {'Lower Bound':>12} | {'Upper Bound':>12}")
    result.append("-" * 50)
    
    for point in forecasts:
        date = point['date']
        predicted = f"{point['predicted_sales']:.2f}"
        lower = f"{point['lower_bound']:.2f}"
        upper = f"{point['upper_bound']:.2f}"
        
        result.append(f"{date:<12} | {predicted:>15} | {lower:>12} | {upper:>12}")
    
    return "\n".join(result)

def main():
    category_id = "CAT_01"
    forecast_horizon = "daily"
    period = 15  # Next 15 days
    
    print(f"Fetching {forecast_horizon} forecast for {category_id} for next {period} days...")
    
    # Get the forecast data
    forecast_data = get_sales_forecast(category_id, forecast_horizon, period)
    
    if forecast_data:
        # Convert to formatted string
        forecast_string = format_forecast_as_string(forecast_data)
        
        # Print the formatted string
        print(forecast_string)
        
        # Optionally save to a file
        with open(f"{category_id}_forecast_{datetime.now().strftime('%Y%m%d')}.txt", "w") as f:
            f.write(forecast_string)
            print(f"\nForecast saved to {category_id}_forecast_{datetime.now().strftime('%Y%m%d')}.txt")
    else:
        print("Failed to retrieve forecast data")

if __name__ == "__main__":
    main()