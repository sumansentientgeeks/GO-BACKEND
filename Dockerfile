# Build stage
FROM golang:alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the applications (Server & RPC Worker)
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/worker ./cmd/worker

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy binaries and startup script
COPY --from=builder /app/server .
COPY --from=builder /app/worker .
COPY start.sh .
RUN chmod +x start.sh

# Default environment variables
ENV RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/
ENV PORT=8080

# Expose the API server port
EXPOSE 8080

# Entrypoint script runs both Server & RPC Worker by default
ENTRYPOINT ["./start.sh"]

