// services/forecasting-api/internal/temporal/workflows.go
package temporal

import (
	"forecasting-api/internal/models"
	"time"

	"go.temporal.io/sdk/workflow"
)

// GenerateForecastWorkflow is the main workflow for generating forecasts.
func GenerateForecastWorkflow(ctx workflow.Context, req models.ForecastJobRequest) (string, error) {
	// Set activity options
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	logger := workflow.GetLogger(ctx)
	logger.Info("Forecast generation workflow started.", "Category", req.CategoryID)

	// In a real workflow, you would have multiple activities:
	// 1. Validate historical data
	// 2. Trigger model training/prediction in the Python service
	// 3. Ingest results back into TimescaleDB
	
	var result string
	err := workflow.ExecuteActivity(ctx, GenerateForecastDataActivity, req).Get(ctx, &result)
	if err != nil {
		logger.Error("Activity failed.", "Error", err)
		return "", err
	}

	logger.Info("Workflow completed.", "Result", result)
	return result, nil
}
