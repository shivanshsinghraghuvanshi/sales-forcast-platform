// services/forecasting-api/internal/models/models.go
package models

import "time"

// GetForecastsParams represents the query parameters for the /forecasts endpoint.
type GetForecastsParams struct {
	CategoryID      string `form:"category_id" binding:"required"`
	ForecastHorizon string `form:"forecast_horizon" binding:"required,oneof=daily monthly yearly"`
	StartDate       string `form:"start_date" binding:"required"`
	EndDate         string `form:"end_date" binding:"required"`
	Page            int    `form:"page,default=1"`
	PageSize        int    `form:"page_size,default=100"`
}

// ForecastEntry corresponds to the ForecastEntry schema object.
type ForecastEntry struct {
	Date          time.Time `json:"date"`
	CategoryID    string    `json:"category_id"`
	ForecastValue float64   `json:"forecast_value"`
	UpperBound    float64   `json:"upper_bound"`
	LowerBound    float64   `json:"lower_bound"`
	Unit          string    `json:"unit"`
}

// PaginationInfo corresponds to the PaginationInfo schema object.
type PaginationInfo struct {
	TotalItems   int64  `json:"total_items"`
	TotalPages   int    `json:"total_pages"`
	CurrentPage  int    `json:"current_page"`
	PageSize     int    `json:"page_size"`
	HasNextPage  bool   `json:"has_next_page"`
	HasPrevPage  bool   `json:"has_prev_page"`
	NextPageLink string `json:"next_page_link,omitempty"`
	PrevPageLink string `json:"prev_page_link,omitempty"`
}

// ForecastMetadataResponse corresponds to the metadata part of the ForecastResponse.
type ForecastMetadataResponse struct {
	ForecastGeneratedAt time.Time `json:"forecast_generated_at"`
	ModelVersion        string    `json:"model_version"`
	Currency            string    `json:"currency,omitempty"`
}

// ForecastResponse corresponds to the ForecastResponse schema object.
type ForecastResponse struct {
	Data       []ForecastEntry          `json:"data"`
	Pagination PaginationInfo           `json:"pagination"`
	Metadata   ForecastMetadataResponse `json:"metadata"`
}

// Category corresponds to the Category schema object.
type Category struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// ForecastSystemMetadata corresponds to the ForecastMetadata schema object.
type ForecastSystemMetadata struct {
	SupportedHorizons []string `json:"supported_horizons"`
	DefaultPageSize   int      `json:"default_page_size"`
	MaxPageSize       int      `json:"max_page_size"`
	// Add other fields as needed
}

// ForecastJobRequest corresponds to the ForecastJobRequest schema object.
type ForecastJobRequest struct {
	CategoryID      string `json:"category_id" binding:"required"`
	ForecastHorizon string `json:"forecast_horizon" binding:"required"`
	StartDate       string `json:"start_date" binding:"required"`
	EndDate         string `json:"end_date" binding:"required"`
	Priority        string `json:"priority"`
	WebhookURL      string `json:"webhook_url"`
}

// JobResponse corresponds to the JobResponse schema object.
type JobResponse struct {
	JobID                   string    `json:"job_id"`
	Status                  string    `json:"status"`
	Message                 string    `json:"message"`
	EstimatedCompletionTime time.Time `json:"estimated_completion_time,omitempty"`
	PollURL                 string    `json:"poll_url"`
	WebhookURL              string    `json:"webhook_url,omitempty"`
}

// JobStatusResponse corresponds to the JobStatusResponse schema object.
type JobStatusResponse struct {
	JobID       string    `json:"job_id"`
	Status      string    `json:"status"`
	Progress    int       `json:"progress"`
	Message     string    `json:"message"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	ResultURL   string    `json:"result_url,omitempty"`
	// Add other fields as needed
}
