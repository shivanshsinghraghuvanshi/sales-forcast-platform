// services/forecasting-api/internal/db/postgres_repo.go
package db

import (
	"context"
	"log"

	models "forecasting-api/internal/models"

	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresRepo(pool *pgxpool.Pool) PostgresRepository {
	return &postgresRepo{pool: pool}
}

// GetCategories retrieves all distinct categories from the database.
func (r *postgresRepo) GetCategories(ctx context.Context) ([]models.Category, error) {
	query := `SELECT id, name FROM categories ORDER BY name;`
	
	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var categories []models.Category
	for rows.Next() {
		var cat models.Category
		if err := rows.Scan(&cat.ID, &cat.Name); err != nil {
			log.Printf("Error scanning category row: %v", err)
			continue
		}
		categories = append(categories, cat)
	}

	return categories, nil
}

// ListJobs retrieves a list of jobs from the database.
func (r *postgresRepo) ListJobs(ctx context.Context) ([]models.JobStatusResponse, error) {
	// This is a placeholder. You would build a proper query with filtering.
	query := `SELECT job_id, status, progress, message, created_at, updated_at FROM jobs ORDER BY created_at DESC LIMIT 20;`
	
	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []models.JobStatusResponse
	for rows.Next() {
		var job models.JobStatusResponse
		if err := rows.Scan(&job.JobID, &job.Status, &job.Progress, &job.Message, &job.CreatedAt, &job.UpdatedAt); err != nil {
			log.Printf("Error scanning job row: %v", err)
			continue
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}
