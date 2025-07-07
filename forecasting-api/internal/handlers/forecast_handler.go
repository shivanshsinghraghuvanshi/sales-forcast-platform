// forecasting-api/internal/handlers/forecast_handlers.go
package handlers

import (
	"forecasting-api/internal/models"
	"net/http"

	"github.com/gin-gonic/gin"
)

// GetCategoryForecasts handles requests to retrieve forecast data.
func (h *APIHandler) GetCategoryForecasts(c *gin.Context) {
	// Parameter binding and validation
	var params models.GetForecastsParams
	if err := c.ShouldBindQuery(&params); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid query parameters", "details": err.Error()})
		return
	}

	// Fetch data from TimescaleDB
	forecasts, err := h.TimescaleRepo.GetForecasts(c.Request.Context(), params)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve forecasts", "details": err.Error()})
		return
	}

	if len(forecasts) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "No forecast data found for the specified criteria."})
		return
	}
	
	// In a real app, you would implement proper pagination logic
	pagination := models.PaginationInfo{
		TotalItems:  int64(len(forecasts)),
		TotalPages:  1,
		CurrentPage: params.Page,
		PageSize:    params.PageSize,
	}

	response := models.ForecastResponse{
		Data:       forecasts,
		Pagination: pagination,
		Metadata: models.ForecastMetadataResponse{
			ModelVersion: "v3.1.2", // Example metadata
		},
	}

	c.JSON(http.StatusOK, response)
}

// GetForecastCategories retrieves all available categories.
func (h *APIHandler) GetForecastCategories(c *gin.Context) {
	categories, err := h.PostgresRepo.GetCategories(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve categories", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"categories": categories})
}

// GetForecastSystemMetadata retrieves system metadata.
func (h *APIHandler) GetForecastSystemMetadata(c *gin.Context) {
	// This would fetch real data from a config store or database
	metadata := models.ForecastSystemMetadata{
		SupportedHorizons: []string{"daily", "monthly", "yearly"},
		DefaultPageSize:   100,
		MaxPageSize:       500,
	}
	c.JSON(http.StatusOK, metadata)
}
