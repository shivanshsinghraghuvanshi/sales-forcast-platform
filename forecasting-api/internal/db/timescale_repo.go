// services/forecasting-api/internal/db/timescale_repo.go
package db

import (
	"context"
	"fmt"
	"log"

	models "forecasting-api/internal/models"

	"github.com/jackc/pgx/v5/pgxpool"
)

type timescaleRepo struct {
	pool *pgxpool.Pool
}

func NewTimescaleRepo(pool *pgxpool.Pool) TimescaleRepository {
	return &timescaleRepo{pool: pool}
}

// GetForecasts retrieves forecast data from TimescaleDB based on query parameters.
func (r *timescaleRepo) GetForecasts(ctx context.Context, params models.GetForecastsParams) ([]models.ForecastEntry, error) {
	// Basic query - can be enhanced with time_bucket for different horizons
	query := `
		SELECT 
			date, category_id, forecast_value, upper_bound, lower_bound, unit
		FROM forecasts
		WHERE category_id = $1 AND date >= $2 AND date <= $3
		ORDER BY date
		LIMIT $4 OFFSET $5;
	`
	offset := (params.Page - 1) * params.PageSize

	rows, err := r.pool.Query(ctx, query, params.CategoryID, params.StartDate, params.EndDate, params.PageSize, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to execute forecast query: %w", err)
	}
	defer rows.Close()

	var forecasts []models.ForecastEntry
	for rows.Next() {
		var entry models.ForecastEntry
		if err := rows.Scan(&entry.Date, &entry.CategoryID, &entry.ForecastValue, &entry.UpperBound, &entry.LowerBound, &entry.Unit); err != nil {
			log.Printf("Error scanning forecast row: %v", err)
			continue
		}
		forecasts = append(forecasts, entry)
	}

	return forecasts, nil
}
