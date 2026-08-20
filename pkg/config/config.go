package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

// Load initializes the environment variables from a .env file if present, or uses system/Render environment variables.
func Load() {
	if _, err := os.Stat(".env"); err == nil {
		if err := godotenv.Load(); err != nil {
			log.Printf("[Config] Warning loading .env: %v (using system environment)", err)
		} else {
			log.Println("[Config] Successfully loaded environment variables from .env")
		}
	} else {
		log.Println("[Config] Running with system/Render environment variables.")
	}
}
