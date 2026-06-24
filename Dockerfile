# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Copy dependency files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o log_aggregator ./cmd/main.go

# Final stage
FROM alpine:latest

WORKDIR /app

# Copy binary from build stage
COPY --from=builder /app/log_aggregator .

# Create directory for logs
RUN mkdir -p /var/log/aggregator

# Set the entrypoint to run the aggregator with a default config
ENTRYPOINT ["./log_aggregator"]
CMD ["-config", "/app/config.json"]
