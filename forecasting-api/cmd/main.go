// services/forecasting-api/cmd/main.go
package main

import (
	"context"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"quickbooks-forecasting/internal/db"
	"quickbooks-forecasting/internal/handlers"
	temporal_client "quickbooks-forecasting/internal/temporal"
)

func main() {
	// --- Database Connections ---
	postgresDSN := os.Getenv("POSTGRES_DSN")
	if postgresDSN == "" {
		postgresDSN = "postgres://user:password@localhost:5432/quickbooks_main?sslmode=disable"
	}
	postgresPool, err := pgxpool.New(context.Background(), postgresDSN)
	if err != nil {
		log.Fatalf("Unable to connect to PostgreSQL: %v\n", err)
	}
	defer postgresPool.Close()
	log.Println("Successfully connected to PostgreSQL.")

	timescaleDSN := os.Getenv("TIMESCALEDB_DSN")
	if timescaleDSN == "" {
		timescaleDSN = "postgres://user:password@localhost:5433/quickbooks_ts?sslmode=disable"
	}
	timescalePool, err := pgxpool.New(context.Background(), timescaleDSN)
	if err != nil {
		log.Fatalf("Unable to connect to TimescaleDB: %v\n", err)
	}
	defer timescalePool.Close()
	log.Println("Successfully connected to TimescaleDB.")

	// --- Temporal Client ---
	temporalService, err := temporal_client.NewTemporalClient()
	if err != nil {
		log.Fatalf("Unable to create Temporal client: %v", err)
	}
	defer temporalService.Close()
	log.Println("Successfully connected to Temporal.")

	// --- Setup Repositories and Handlers ---
	postgresRepo := db.NewPostgresRepo(postgresPool)
	timescaleRepo := db.NewTimescaleRepo(timescalePool)
	apiHandlers := handlers.NewAPIHandler(postgresRepo, timescaleRepo, temporalService)

	// --- Gin Router Setup ---
	router := gin.Default()
	
	// --- API Documentation ---
	// Serve the static OpenAPI spec file
	router.StaticFile("/openapi.json", "./api/openapi.json") 
	// Setup Swagger UI
	url := ginSwagger.URL("/openapi.json")
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, url))


	v1 := router.Group("/v1")
	{
		// Forecasts Endpoints
		v1.GET("/forecasts", apiHandlers.GetCategoryForecasts)
		v1.GET("/forecasts/categories", apiHandlers.GetForecastCategories)
		v1.GET("/forecasts/metadata", apiHandlers.GetForecastSystemMetadata)

		// Jobs Endpoints
		v1.POST("/forecasts/generate", apiHandlers.CreateForecastJob)
		v1.GET("/jobs", apiHandlers.ListJobs)
		v1.GET("/jobs/:job_id", apiHandlers.GetJobStatus)
		v1.POST("/jobs/:job_id/cancel", apiHandlers.CancelJob)
	}

	log.Println("Starting server on port 8080...")
	log.Println("Access Swagger UI at http://localhost:8080/swagger/index.html")
	if err := router.Run(":8080"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}
