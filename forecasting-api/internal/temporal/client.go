// services/forecasting-api/internal/temporal/client.go
package temporal

import (
	"log"
	"os"

	"go.temporal.io/sdk/client"
)

const ForecastTaskQueue = "FORECAST_GENERATION_QUEUE"

// NewTemporalClient creates and returns a new Temporal client.
func NewTemporalClient() (client.Client, error) {
	temporalHost := os.Getenv("TEMPORAL_ADDRESS")
	if temporalHost == "" {
		temporalHost = "localhost:7233" // Default for local dev
	}

	c, err := client.Dial(client.Options{
		HostPort: temporalHost,
	})

	if err != nil {
		log.Printf("Unable to create Temporal client: %v", err)
		return nil, err
	}
	return c, nil
}
