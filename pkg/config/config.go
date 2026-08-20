package config

import (
	"log"

	"github.com/joho/godotenv"
)

// Load initializes the environment variables from a .env file
func Load() {
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found or error loading it. Relying on system environment variables.")
	} else {
		log.Println("Environment variables loaded from .env file successfully.")
	}
}
