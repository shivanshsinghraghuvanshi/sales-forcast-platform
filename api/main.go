package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	// IMPORTANT: You need to run `swag init` for this import to work.
	_ "api/docs"
)

// --- Configuration ---
const (
	forecastingServiceBaseURL = "http://localhost:8000"
	preprocessorBinaryPath    = "../processors/processor"
	dbConnectionString        = "user=admin password=password dbname=sales_db sslmode=disable host=localhost port=5432"
)

// Global DB pool
var db *sql.DB

// --- Structs for API responses ---
type ForecastPoint struct {
	Date           string  `json:"forecast_date"`
	PredictedSales float64 `json:"predicted_sales"`
	LowerBound     float64 `json:"lower_bound"`
	UpperBound     float64 `json:"upper_bound"`
}

type JobStatusResponse struct {
	JobID        string `json:"job_id"`
	CategoryID   string `json:"category_id"`
	Status       string `json:"status"`
	ErrorMessage string `json:"error_message,omitempty"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

// NEW Structs for Catalog and ETL endpoints
type Category struct {
	ID   string `json:"category_id"`
	Name string `json:"category_name"`
}

type Product struct {
	ID          string `json:"product_id"`
	Name        string `json:"product_name"`
	Description string `json:"description"`
	CategoryID  string `json:"category_id"`
}

type Promotion struct {
	ID                 string `json:"promotion_id"`
	Name               string `json:"promotion_name"`
	StartDate          string `json:"start_date"`
	EndDate            string `json:"end_date"`
	DiscountPercentage int    `json:"discount_percentage"`
	TargetType         string `json:"target_type"`
	TargetID           string `json:"target_id"`
}

type ETLJobStatus struct {
	ID          int    `json:"id"`
	FileName    string `json:"file_name"`
	Status      string `json:"status"`
	LastUpdated string `json:"last_updated"`
}

type HistoricalForecast struct {
	ModelVersionID int             `json:"model_version_id"`
	Forecasts      []ForecastPoint `json:"forecasts"`
}

// proxyRequest is a helper function to forward a request to the downstream forecasting service.
func proxyRequest(c *gin.Context, method, downstreamPath string) {
	downstreamURL := fmt.Sprintf("%s%s", forecastingServiceBaseURL, downstreamPath)
	if c.Request.URL.RawQuery != "" {
		downstreamURL = fmt.Sprintf("%s?%s", downstreamURL, c.Request.URL.RawQuery)
	}

	log.Printf("Forwarding request to: %s", downstreamURL)

	req, err := http.NewRequest(method, downstreamURL, c.Request.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create downstream request"})
		return
	}

	req.Header = c.Request.Header

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "Failed to reach downstream service", "details": err.Error()})
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read downstream response"})
		return
	}

	c.Data(resp.StatusCode, resp.Header.Get("Content-Type"), body)
}

// --- Handler Functions ---

// @Summary      Get Sales Forecast
// @Description  Retrieves a sales forecast for a specific category. Tries to serve from a cache first. If the requested period is not fully cached, it creates an asynchronous job to generate the missing data.
// @Tags         Forecasting
// @Accept       json
// @Produce      json
// @Param        category_id      path      string  true  "Category ID (e.g., CAT_01)"
// @Param        forecast_horizon query     string  true  "Forecast Horizon (daily, monthly, yearly)" Enums(daily, monthly, yearly)
// @Param        period           query     int     true  "Number of periods to forecast"
// @Success      200              {object}  map[string]interface{} "A JSON object containing the forecast data."
// @Success      202              {object}  map[string]string "An asynchronous job has been created."
// @Failure      400              {object}  map[string]string "Invalid input parameters."
// @Failure      500              {object}  map[string]string "Internal server error."
// @Router       /forecasts/{category_id} [get]
func getForecastHandler(c *gin.Context) {
	categoryID := c.Param("category_id")
	horizon := c.Query("forecast_horizon")
	periodStr := c.Query("period")

	period, err := strconv.Atoi(periodStr)
	if err != nil || period <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'period' parameter."})
		return
	}
	if horizon != "daily" && horizon != "monthly" && horizon != "yearly" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'forecast_horizon'."})
		return
	}

	query := `
		SELECT forecast_date, predicted_sales, lower_bound, upper_bound
		FROM live_forecasts
		WHERE category_id = $1 AND granularity = $2
		ORDER BY forecast_date
		LIMIT $3
	`
	rows, err := db.Query(query, categoryID, horizon, period)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query forecast cache."})
		return
	}
	defer rows.Close()

	var forecasts []ForecastPoint
	for rows.Next() {
		var fp ForecastPoint
		var date time.Time
		if err := rows.Scan(&date, &fp.PredictedSales, &fp.LowerBound, &fp.UpperBound); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to scan forecast data."})
			return
		}
		fp.Date = date.Format("2006-01-02")
		forecasts = append(forecasts, fp)
	}

	if len(forecasts) >= period {
		log.Printf("Full cache hit for category %s. Returning %d points.", categoryID, len(forecasts))
		c.JSON(http.StatusOK, gin.H{"category_id": categoryID, "forecast": forecasts})
		return
	}

	log.Printf("Cache miss for category %s. Creating async job.", categoryID)
	jobID := uuid.New()
	requestParams := map[string]interface{}{"granularity": horizon, "count": period - len(forecasts)}
	paramsJSON, _ := json.Marshal(requestParams)

	insertQuery := `
		INSERT INTO async_forecast_jobs (job_id, category_id, request_params, status)
		VALUES ($1, $2, $3, 'PENDING')
	`
	_, err = db.Exec(insertQuery, jobID, categoryID, paramsJSON)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create forecast job."})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"message": "Forecast not available in cache. An asynchronous job has been created.",
		"job_id":  jobID,
	})
}

// @Summary      Get Historical Forecasts
// @Description  Retrieves historical forecasts for a category within a date range, grouped by the model version that generated them.
// @Tags         Forecasting
// @Accept       json
// @Produce      json
// @Param        category_id path      string  true  "Category ID"
// @Param        start_date  query     string  true  "Start date (YYYY-MM-DD)"
// @Param        end_date    query     string  true  "End date (YYYY-MM-DD)"
// @Success      200         {array}   HistoricalForecast
// @Failure      400         {object}  map[string]string "Invalid input parameters."
// @Failure      500         {object}  map[string]string "Internal server error."
// @Router       /forecasts/{category_id}/history [get]
func getHistoricalForecastsHandler(c *gin.Context) {
	categoryID := c.Param("category_id")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	query := `
		SELECT model_version_id, forecast_date, predicted_sales, lower_bound, upper_bound
		FROM historical_forecasts
		WHERE category_id = $1 AND forecast_date BETWEEN $2 AND $3
		ORDER BY model_version_id, forecast_date
	`
	rows, err := db.Query(query, categoryID, startDate, endDate)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query historical forecasts."})
		return
	}
	defer rows.Close()

	// Group results by model_version_id
	results := make(map[int][]ForecastPoint)
	for rows.Next() {
		var modelID int
		var fp ForecastPoint
		var date time.Time
		if err := rows.Scan(&modelID, &date, &fp.PredictedSales, &fp.LowerBound, &fp.UpperBound); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to scan historical forecast data."})
			return
		}
		fp.Date = date.Format("2006-01-02")
		results[modelID] = append(results[modelID], fp)
	}

	// Convert map to the final list structure
	var response []HistoricalForecast
	for modelID, forecasts := range results {
		response = append(response, HistoricalForecast{
			ModelVersionID: modelID,
			Forecasts:      forecasts,
		})
	}

	c.JSON(http.StatusOK, response)
}

// @Summary      Get Job Status
// @Description  Retrieves the status of an asynchronous forecast job.
// @Tags         Jobs
// @Accept       json
// @Produce      json
// @Param        job_id   path      string  true  "Job ID (UUID)"
// @Success      200      {object}  JobStatusResponse
// @Failure      404      {object}  map[string]string "Job not found."
// @Failure      500      {object}  map[string]string "Internal server error."
// @Router       /jobs/{job_id} [get]
func getJobStatusHandler(c *gin.Context) {
	jobID := c.Param("job_id")
	var status, errMsg, categoryID sql.NullString
	var createdAt, updatedAt time.Time

	query := "SELECT job_id, category_id, status, error_message, created_at, updated_at FROM async_forecast_jobs WHERE job_id = $1"
	err := db.QueryRow(query, jobID).Scan(&jobID, &categoryID, &status, &errMsg, &createdAt, &updatedAt)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found."})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve job status."})
		return
	}

	c.JSON(http.StatusOK, JobStatusResponse{
		JobID:        jobID,
		CategoryID:   categoryID.String,
		Status:       status.String,
		ErrorMessage: errMsg.String,
		CreatedAt:    createdAt.Format(time.RFC3339),
		UpdatedAt:    updatedAt.Format(time.RFC3339),
	})
}

// @Summary      List Recent Jobs
// @Description  Retrieves a list of the most recent asynchronous forecast jobs.
// @Tags         Jobs
// @Accept       json
// @Produce      json
// @Param        limit  query     int  false  "Number of jobs to return" default(20)
// @Success      200    {array}   JobStatusResponse
// @Failure      500    {object}  map[string]string "Internal server error."
// @Router       /jobs [get]
func listJobsHandler(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if limit > 100 {
		limit = 100 // Cap the limit for performance
	}

	query := "SELECT job_id, category_id, status, error_message, created_at, updated_at FROM async_forecast_jobs ORDER BY created_at DESC LIMIT $1"
	rows, err := db.Query(query, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve job list."})
		return
	}
	defer rows.Close()

	var jobs []JobStatusResponse
	for rows.Next() {
		var j JobStatusResponse
		var jobID uuid.UUID
		var status, errMsg, categoryID sql.NullString
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&jobID, &categoryID, &status, &errMsg, &createdAt, &updatedAt); err != nil {
			log.Printf("Error scanning job row: %v", err)
			continue
		}
		j.JobID = jobID.String()
		j.CategoryID = categoryID.String
		j.Status = status.String
		j.ErrorMessage = errMsg.String
		j.CreatedAt = createdAt.Format(time.RFC3339)
		j.UpdatedAt = updatedAt.Format(time.RFC3339)
		jobs = append(jobs, j)
	}

	c.JSON(http.StatusOK, jobs)
}

// @Summary      Cancel a Job
// @Description  Cancels a PENDING asynchronous forecast job.
// @Tags         Jobs
// @Accept       json
// @Produce      json
// @Param        job_id   path      string  true  "Job ID (UUID) to cancel"
// @Success      200      {object}  map[string]string "Job cancelled successfully."
// @Failure      404      {object}  map[string]string "Job not found."
// @Failure      409      {object}  map[string]string "Job is not in a cancellable state (must be PENDING)."
// @Failure      500      {object}  map[string]string "Internal server error."
// @Router       /jobs/{job_id}/cancel [post]
func cancelJobHandler(c *gin.Context) {
	jobID := c.Param("job_id")

	// Atomically update the job to CANCELLED only if it is currently PENDING.
	query := `
		UPDATE async_forecast_jobs
		SET status = 'CANCELLED', updated_at = NOW()
		WHERE job_id = $1 AND status = 'PENDING'
	`
	result, err := db.Exec(query, jobID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to execute cancel operation."})
		return
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check operation result."})
		return
	}

	if rowsAffected == 0 {
		// Check why it failed: was the job not found, or was it not in a PENDING state?
		var status string
		err := db.QueryRow("SELECT status FROM async_forecast_jobs WHERE job_id = $1", jobID).Scan(&status)
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Job not found."})
		} else {
			c.JSON(http.StatusConflict, gin.H{"error": fmt.Sprintf("Job is not in a cancellable state. Current status: %s", status)})
		}
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Job cancelled successfully."})
}

// --- Background Job Runner ---
func jobRunner() {
	fmt.Println("Starting job runner...")
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		var jobID uuid.UUID
		var categoryID string
		var requestParamsJSON []byte

		query := `
			UPDATE async_forecast_jobs
			SET status = 'RUNNING', updated_at = NOW()
			WHERE job_id = (
				SELECT job_id FROM async_forecast_jobs
				WHERE status = 'PENDING' ORDER BY created_at LIMIT 1
				FOR UPDATE SKIP LOCKED
			)
			RETURNING job_id, category_id, request_params;
		`
		err := db.QueryRow(query).Scan(&jobID, &categoryID, &requestParamsJSON)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			log.Printf("Job runner DB error: %v", err)
			continue
		}

		log.Printf("Job runner picked up job %s for category %s", jobID, categoryID)
		go processJob(jobID, categoryID, requestParamsJSON)
	}
}

// processJob handles the logic for a single async job
func processJob(jobID uuid.UUID, categoryID string, requestParamsJSON []byte) {
	var params map[string]interface{}
	json.Unmarshal(requestParamsJSON, &params)
	count := int(params["count"].(float64))
	granularity := params["granularity"].(string)

	deltaURL := fmt.Sprintf(
		"%s/forecasts/%s/generate-delta?count=%d&granularity=%s",
		forecastingServiceBaseURL, categoryID, count, url.QueryEscape(granularity),
	)
	resp, err := http.Post(deltaURL, "application/json", nil)

	if err != nil || resp.StatusCode != http.StatusOK {
		errMsg := "Failed to execute forecast in Python service."
		if err != nil {
			errMsg = err.Error()
		}
		db.Exec("UPDATE async_forecast_jobs SET status = 'FAILED', error_message = $1, updated_at = NOW() WHERE job_id = $2", errMsg, jobID)
		return
	}
	defer resp.Body.Close()

	var deltaForecasts []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&deltaForecasts); err != nil {
		db.Exec("UPDATE async_forecast_jobs SET status = 'FAILED', error_message = $1, updated_at = NOW() WHERE job_id = $2", "Failed to decode delta forecast response", jobID)
		return
	}

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare(pq.CopyIn("live_forecasts", "model_version_id", "category_id", "forecast_date", "predicted_sales", "lower_bound", "upper_bound", "granularity"))

	for _, fcst := range deltaForecasts {
		stmt.Exec(int64(fcst["model_version_id"].(float64)), fcst["category_id"], fcst["forecast_date"], fcst["predicted_sales"], fcst["lower_bound"], fcst["upper_bound"], fcst["granularity"])
	}

	if _, err = stmt.Exec(); err != nil {
		tx.Rollback()
		db.Exec("UPDATE async_forecast_jobs SET status = 'FAILED', error_message = $1, updated_at = NOW() WHERE job_id = $2", "Failed to bulk insert delta forecast", jobID)
		return
	}
	stmt.Close()
	tx.Commit()

	db.Exec("UPDATE async_forecast_jobs SET status = 'COMPLETED', updated_at = NOW() WHERE job_id = $1", jobID)
	log.Printf("Job %s completed successfully.", jobID)
}

// --- Extracted Handler Functions for Swagger ---

// @Summary      Get All Categories
// @Description  Retrieves a list of all product categories.
// @Tags         Catalog
// @Accept       json
// @Produce      json
// @Success      200      {array}   Category
// @Failure      500      {object}  map[string]string "Internal server error."
// @Router       /catalog/categories [get]
func getCategoriesHandler(c *gin.Context) {
	rows, err := db.Query("SELECT category_id, category_name FROM categories ORDER BY category_name")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query categories."})
		return
	}
	defer rows.Close()

	var items []Category
	for rows.Next() {
		var item Category
		if err := rows.Scan(&item.ID, &item.Name); err != nil {
			log.Printf("Error scanning category row: %v", err)
			continue
		}
		items = append(items, item)
	}
	c.JSON(http.StatusOK, items)
}

// @Summary      Get All Products
// @Description  Retrieves a list of all products, with optional filtering by category.
// @Tags         Catalog
// @Accept       json
// @Produce      json
// @Param        category_id query     string false "Filter by Category ID"
// @Success      200         {array}   Product
// @Failure      500         {object}  map[string]string "Internal server error."
// @Router       /catalog/products [get]
func getProductsHandler(c *gin.Context) {
	query := "SELECT product_id, product_name, description, category_id FROM products"
	var args []interface{}
	if categoryID := c.Query("category_id"); categoryID != "" {
		query += " WHERE category_id = $1"
		args = append(args, categoryID)
	}
	query += " ORDER BY product_name"

	rows, err := db.Query(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query products."})
		return
	}
	defer rows.Close()

	var items []Product
	for rows.Next() {
		var item Product
		if err := rows.Scan(&item.ID, &item.Name, &item.Description, &item.CategoryID); err != nil {
			log.Printf("Error scanning product row: %v", err)
			continue
		}
		items = append(items, item)
	}
	c.JSON(http.StatusOK, items)
}

// @Summary      Get All Promotions
// @Description  Retrieves a list of all promotions.
// @Tags         Catalog
// @Accept       json
// @Produce      json
// @Success      200      {array}   Promotion
// @Failure      500      {object}  map[string]string "Internal server error."
// @Router       /catalog/promotions [get]
func getPromotionsHandler(c *gin.Context) {
	query := "SELECT promotion_id, promotion_name, start_date, end_date, discount_percentage, target_type, target_id FROM promotions ORDER BY start_date"
	rows, err := db.Query(query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query promotions."})
		return
	}
	defer rows.Close()

	var items []Promotion
	for rows.Next() {
		var item Promotion
		var startDate, endDate time.Time
		if err := rows.Scan(&item.ID, &item.Name, &startDate, &endDate, &item.DiscountPercentage, &item.TargetType, &item.TargetID); err != nil {
			log.Printf("Error scanning promotion row: %v", err)
			continue
		}
		item.StartDate = startDate.Format("2006-01-02")
		item.EndDate = endDate.Format("2006-01-02")
		items = append(items, item)
	}
	c.JSON(http.StatusOK, items)
}

// @Summary      Get ETL Job Statuses
// @Description  Retrieves a list of the most recent ETL (data processing) job statuses.
// @Tags         ETL
// @Accept       json
// @Produce      json
// @Param        limit  query     int  false  "Number of jobs to return" default(50)
// @Success      200    {array}   ETLJobStatus
// @Failure      500    {object}  map[string]string "Internal server error."
// @Router       /etl/jobs [get]
func getETLJobsHandler(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if limit > 200 {
		limit = 200
	}

	query := "SELECT id, file_name, status, last_updated FROM etl_job_status ORDER BY last_updated DESC LIMIT $1"
	rows, err := db.Query(query, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve ETL job list."})
		return
	}
	defer rows.Close()

	var jobs []ETLJobStatus
	for rows.Next() {
		var j ETLJobStatus
		var lastUpdated time.Time
		if err := rows.Scan(&j.ID, &j.FileName, &j.Status, &lastUpdated); err != nil {
			log.Printf("Error scanning ETL job row: %v", err)
			continue
		}
		j.LastUpdated = lastUpdated.Format(time.RFC3339)
		jobs = append(jobs, j)
	}
	c.JSON(http.StatusOK, jobs)
}

// @Summary      List All Model Versions
// @Description  Retrieves a list of all model versions from the registry, ordered by training date.
// @Tags         Observability
// @Accept       json
// @Produce      json
// @Success      200      {object}  []map[string]interface{}
// @Failure      500      {object}  map[string]string "Internal server error."
// @Router       /mlops/observability/versions [get]
func listAllModelVersionsHandler(c *gin.Context) {
	proxyRequest(c, http.MethodGet, "/observability/versions")
}

// @Summary      Trigger Model Training
// @Description  Triggers a background job in the Python forecasting engine to retrain all models and refresh the forecast cache.
// @Tags         MLOps
// @Accept       json
// @Produce      json
// @Success      202      {object}  map[string]string "Training job accepted."
// @Failure      502      {object}  map[string]string "Failed to reach downstream service."
// @Router       /mlops/training/run [post]
func triggerTrainingHandler(c *gin.Context) {
	proxyRequest(c, http.MethodPost, "/training/run")
}

// @Summary      Get Model Version History
// @Description  Retrieves the complete version history for a specific category's model.
// @Tags         Observability
// @Accept       json
// @Produce      json
// @Param        category_id   path      string  true  "Category ID"
// @Success      200      {object}  []map[string]interface{}
// @Failure      404      {object}  map[string]string "Not Found"
// @Router       /mlops/observability/versions/{category_id} [get]
func getModelVersionsHandler(c *gin.Context) {
	proxyRequest(c, http.MethodGet, "/observability/versions/"+c.Param("category_id"))
}

// @Summary      Get Model Performance History
// @Description  Retrieves the live performance history for a specific model version ID.
// @Tags         Observability
// @Accept       json
// @Produce      json
// @Param        version_id   path      int  true  "Model Version ID"
// @Success      200      {object}  []map[string]interface{}
// @Failure      404      {object}  map[string]string "Not Found"
// @Router       /mlops/observability/performance/{version_id} [get]
func getModelPerformanceHandler(c *gin.Context) {
	proxyRequest(c, http.MethodGet, "/observability/performance/"+c.Param("version_id"))
}

// @Summary      Run Data Preprocessing
// @Description  Triggers a background job to run the Go-based data preprocessing service.
// @Tags         Data
// @Accept       json
// @Produce      json
// @Success      202      {object}  map[string]interface{} "Job started."
// @Failure      500      {object}  map[string]string "Failed to start job."
// @Router       /data/preprocess/run [post]
func runPreprocessingHandler(c *gin.Context) {
	log.Println("Received request to run the data preprocessing job.")
	cmd := exec.Command(preprocessorBinaryPath, "-type=all")
	cmd.Stdout = log.Writer()
	cmd.Stderr = log.Writer()

	err := cmd.Start()
	if err != nil {
		log.Printf("ERROR: Failed to start preprocessor binary: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to start preprocessing job."})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"message": "Data preprocessing job started in the background.",
		"pid":     cmd.Process.Pid,
	})

	go func() {
		err := cmd.Wait()
		if err != nil {
			log.Printf("Preprocessing job (PID: %d) finished with an error: %v", cmd.Process.Pid, err)
		} else {
			log.Printf("Preprocessing job (PID: %d) finished successfully.", cmd.Process.Pid)
		}
	}()
}


// @title           Sales Forecasting API Gateway (BFF)
// @version         1.2
// @description     This is the Backend-for-Frontend (BFF) API gateway for the sales forecasting platform. It serves cached forecasts, manages async jobs, and proxies requests to downstream MLOps services.
// @contact.name   API Support
// @contact.url    http://www.example.com/support
// @contact.email  support@example.com
// @license.name   Apache 2.0
// @license.url    http://www.apache.org/licenses/LICENSE-2.0.html
// @host           localhost:8080
// @BasePath       /api/v1
func main() {
	var err error
	db, err = sql.Open("postgres", dbConnectionString)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	db.SetMaxOpenConns(25)

	go jobRunner()

	router := gin.Default()

	// --- Swagger Endpoint ---
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	apiV1 := router.Group("/api/v1")
	{
		// --- Forecasting Endpoints ---
		forecastGroup := apiV1.Group("/forecasts")
		{
			forecastGroup.GET("/:category_id", getForecastHandler)
			forecastGroup.GET("/:category_id/history", getHistoricalForecastsHandler)
		}

		// --- Job Endpoints ---
		jobsGroup := apiV1.Group("/jobs")
		{
			jobsGroup.GET("", listJobsHandler)
			jobsGroup.GET("/:job_id", getJobStatusHandler)
			jobsGroup.POST("/:job_id/cancel", cancelJobHandler)
		}

		// --- MLOps Endpoints ---
		mlopsGroup := apiV1.Group("/mlops")
		{
			mlopsGroup.POST("/training/run", triggerTrainingHandler)
			mlopsGroup.GET("/observability/versions", listAllModelVersionsHandler)
			mlopsGroup.GET("/observability/versions/:category_id", getModelVersionsHandler)
			mlopsGroup.GET("/observability/performance/:version_id", getModelPerformanceHandler)
		}

		// --- Data Preprocessing Endpoint ---
		dataGroup := apiV1.Group("/data")
		{
			dataGroup.POST("/preprocess/run", runPreprocessingHandler)
		}

		// --- NEW: Catalog Endpoints ---
		catalogGroup := apiV1.Group("/catalog")
		{
			catalogGroup.GET("/categories", getCategoriesHandler)
			catalogGroup.GET("/products", getProductsHandler)
			catalogGroup.GET("/promotions", getPromotionsHandler)
		}

		// --- NEW: ETL Endpoints ---
		etlGroup := apiV1.Group("/etl")
		{
			etlGroup.GET("/jobs", getETLJobsHandler)
		}
	}

	log.Println("Starting Go API Gateway on port 8080...")
	router.Run(":8080")
}
