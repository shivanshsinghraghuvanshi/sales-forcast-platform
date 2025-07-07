// services/forecasting-api/internal/handlers/handlers.go
package handlers

import (
	"go.temporal.io/sdk/client"

	"forecasting-api/internal/db"
)

// APIHandler holds the dependencies for the API handlers.
type APIHandler struct {
	PostgresRepo  db.PostgresRepository
	TimescaleRepo db.TimescaleRepository
	Temporal      client.Client
}

// NewAPIHandler creates a new APIHandler with database and temporal clients.
func NewAPIHandler(pr db.PostgresRepository, tr db.TimescaleRepository, tc client.Client) *APIHandler {
	return &APIHandler{
		PostgresRepo:  pr,
		TimescaleRepo: tr,
		Temporal:      tc,
	}
}
