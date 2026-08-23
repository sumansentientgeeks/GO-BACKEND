package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)


// Connect establishes a connection to the PostgreSQL database
func Connect() (*sql.DB, error) {
	// In a real application, use configuration from environment variables
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://postgres:1234@localhost:5432/mychat?sslmode=disable"
		log.Println("DATABASE_URL not set, using default development connection string")
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Configure production connection pool settings
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(15 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	log.Println("Database connection pool established successfully (MaxOpen: 50, MaxIdle: 25)")
	return db, nil
}

