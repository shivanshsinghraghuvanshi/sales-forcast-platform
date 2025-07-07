// services/forecasting-api/internal/handlers/job_handlers.go
package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	models "forecasting-api/internal/models"
	temporal "forecasting-api/internal/temporal"
)

// CreateForecastJob starts a new forecast generation workflow in Temporal.
func (h *APIHandler) CreateForecastJob(c *gin.Context) {
	var req models.ForecastJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body", "details": err.Error()})
		return
	}

	// Define workflow options
	workflowID := "forecast-job-" + uuid.New().String()
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: temporal.ForecastTaskQueue,
	}

	// Execute the workflow
	we, err := h.Temporal.ExecuteWorkflow(context.Background(), workflowOptions, temporal.GenerateForecastWorkflow, req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to start forecast job", "details": err.Error()})
		return
	}

	// Respond to the client
	response := models.JobResponse{
		JobID: we.GetID(),
		Status: "queued",
		Message: "Forecast generation job created and queued",
		PollURL: fmt.Sprintf("/jobs/%s", we.GetID()),
	}

	c.JSON(http.StatusCreated, response)
}

// GetJobStatus retrieves the status of a specific job from Temporal.
func (h *APIHandler) GetJobStatus(c *gin.Context) {
	jobID := c.Param("job_id")

	// In a real application, you would query the workflow for its status
	// or query your own database which is updated by the workflow.
	
	// Example response
	resp := models.JobStatusResponse{
		JobID:    jobID,
		Status:   "processing", // Placeholder
		Progress: 50,           // Placeholder
		Message:  "Generating forecasts...",
	}

	c.JSON(http.StatusOK, resp)
}

// ListJobs retrieves a list of jobs.
func (h *APIHandler) ListJobs(c *gin.Context) {
	// This would query your jobs table in PostgreSQL
	jobs, err := h.PostgresRepo.ListJobs(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list jobs", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"jobs": jobs})
}

// CancelJob sends a cancellation request to a running Temporal workflow.
func (h *APIHandler) CancelJob(c *gin.Context) {
	jobID := c.Param("job_id")

	err := h.Temporal.CancelWorkflow(context.Background(), jobID, "")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to cancel job", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Job cancellation request sent."})
}
