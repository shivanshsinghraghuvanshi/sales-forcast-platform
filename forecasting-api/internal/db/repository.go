// services/forecasting-api/internal/db/repository.go
package db

import (
	"context"
	models "forecasting-api/internal/models"
)

// PostgresRepository defines the interface for interacting with the main PostgreSQL database.
type PostgresRepository interface {
	GetCategories(ctx context.Context) ([]models.Category, error)
	ListJobs(ctx context.Context) ([]models.JobStatusResponse, error)
	// Add methods to create/update job status
}

// TimescaleRepository defines the interface for interacting with the TimescaleDB database.
type TimescaleRepository interface {
	GetForecasts(ctx context.Context, params models.GetForecastsParams) ([]models.ForecastEntry, error)
	// Add method to insert forecast results
}
