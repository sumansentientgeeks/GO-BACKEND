# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/server ./cmd/server

# Final stage
FROM alpine:latest

WORKDIR /app

# Copy the binary from the build stage
COPY --from=builder /app/server .

# Expose the port the app runs on (adjust if your Go app uses a different HTTP port)
EXPOSE 8080
# WebRTC UDP ports (Pion default is often dynamic, but if you bind to specific ports, expose them here)
# EXPOSE 50000-50050/udp

# Command to run the executable
CMD ["./server"]
