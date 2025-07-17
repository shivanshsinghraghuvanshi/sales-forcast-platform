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
	Status       string `json:"status"`
	ErrorMessage string `json:"error_message,omitempty"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
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
	var status, errMsg sql.NullString
	var createdAt, updatedAt time.Time

	query := "SELECT status, error_message, created_at, updated_at FROM async_forecast_jobs WHERE job_id = $1"
	err := db.QueryRow(query, jobID).Scan(&status, &errMsg, &createdAt, &updatedAt)
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
		Status:       status.String,
		ErrorMessage: errMsg.String,
		CreatedAt:    createdAt.Format(time.RFC3339),
		UpdatedAt:    updatedAt.Format(time.RFC3339),
	})
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
	resp, err := http.Get(deltaURL)

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

// @title           Sales Forecasting API Gateway (BFF)
// @version         1.0
// @description     This is the Backend-For-Frontend (BFF) API gateway for the sales forecasting platform. It serves cached forecasts, manages async jobs, and proxies requests to downstream MLOps services.
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
		}

		// --- Job Endpoints ---
		jobsGroup := apiV1.Group("/jobs")
		{
			jobsGroup.GET("/:job_id", getJobStatusHandler)
		}

		// --- MLOps Endpoints ---
		mlopsGroup := apiV1.Group("/mlops")
		{
			// @Summary      Trigger Model Training
			// @Description  Triggers a background job in the Python forecasting engine to retrain all models and refresh the forecast cache.
			// @Tags         MLOps
			// @Accept       json
			// @Produce      json
			// @Success      202      {object}  map[string]string "Training job accepted."
			// @Failure      502      {object}  map[string]string "Failed to reach downstream service."
			// @Router       /mlops/training/run [post]
			mlopsGroup.POST("/training/run", func(c *gin.Context) {
				proxyRequest(c, http.MethodPost, "/training/run")
			})

			// @Summary      Get Model Version History
			// @Description  Retrieves the complete version history for a specific category's model.
			// @Tags         Observability
			// @Accept       json
			// @Produce      json
			// @Param        category_id   path      string  true  "Category ID"
			// @Success      200      {object}  []map[string]interface{}
			// @Failure      404      {object}  map[string]string "Not Found"
			// @Router       /mlops/observability/versions/{category_id} [get]
			mlopsGroup.GET("/observability/versions/:category_id", func(c *gin.Context) {
				proxyRequest(c, http.MethodGet, "/observability/versions/"+c.Param("category_id"))
			})

			// @Summary      Get Model Performance History
			// @Description  Retrieves the live performance history for a specific model version ID.
			// @Tags         Observability
			// @Accept       json
			// @Produce      json
			// @Param        version_id   path      int  true  "Model Version ID"
			// @Success      200      {object}  []map[string]interface{}
			// @Failure      404      {object}  map[string]string "Not Found"
			// @Router       /mlops/observability/performance/{version_id} [get]
			mlopsGroup.GET("/observability/performance/:version_id", func(c *gin.Context) {
				proxyRequest(c, http.MethodGet, "/observability/performance/"+c.Param("version_id"))
			})
		}

		// --- Data Preprocessing Endpoint ---
		dataGroup := apiV1.Group("/data")
		{
			// @Summary      Run Data Preprocessing
			// @Description  Triggers a background job to run the Go-based data preprocessing service.
			// @Tags         Data
			// @Accept       json
			// @Produce      json
			// @Success      202      {object}  map[string]interface{} "Job started."
			// @Failure      500      {object}  map[string]string "Failed to start job."
			// @Router       /data/preprocess/run [post]
			dataGroup.POST("/preprocess/run", func(c *gin.Context) {
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
			})
		}
	}

	log.Println("Starting Go API Gateway on port 8080...")
	router.Run(":8080")
}
