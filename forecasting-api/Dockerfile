# Dockerfile for the Go forecasting-api service

# --- Stage 1: Builder ---
# This stage compiles the Go application into a static binary.
FROM golang:1.24-alpine AS builder

# Set necessary environment variables
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files to download dependencies first
# This leverages Docker's layer caching to avoid re-downloading dependencies on every code change.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application source code
COPY . .

# Build the application
# The -o flag specifies the output file name.
# -ldflags="-w -s" strips debugging information, reducing the binary size.
RUN go build -ldflags="-w -s" -o forecasting-api ./cmd/main.go

# --- Stage 2: Final Image ---
# This stage creates the final, lightweight production image.
FROM alpine:3.18

# Set the working directory
WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/forecasting-api .

# Copy the OpenAPI specification file for the Swagger UI
# This assumes the spec file is in an 'api' subdirectory relative to the Dockerfile context.
COPY api/openapi.json .

# Expose the port the application runs on
EXPOSE 8080

# The command to run when the container starts
CMD ["./forecasting-api"]
