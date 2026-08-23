# GO-BACKEND

This is the backend service for the application, written in Go.

## Project Structure

- `cmd/`: Main applications for this project.
- `internal/`: Private application and library code.
- `pkg/`: Library code that's ok to use by external applications.
- `migrations/`: Database migrations.
- `docs/`: Documentation.

## Setup

1. Copy `.env.example` to `.env` and fill in the required environment variables.
2. Run `go mod download` to install dependencies.
3. Run the application (e.g., using `go run cmd/<app>/main.go`).

## Architecture & Scaling

- 📖 **[1M+ Concurrent Users System Design & Discord-Quality Audio Architecture](file:///d:/GO/hello/backend/SYSTEM_DESIGN_1M_DISCORD_AUDIO.md)**: Comprehensive deep dive on scaling to 1M users, SFU clustering, Opus audio optimization, and distributed signaling.

## Build

You can build the application using:
```bash
go build -o server.exe ./cmd/<app>
```

Alternatively, you can build a Docker image using the provided `Dockerfile`.

