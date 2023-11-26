package main

import (
	"log"
	"net/http"
	"os"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	// POSTGRESQL="host=localhost port=5432 user=postgres password=first_iteration dbname=default sslmode=disable"
	db, err := gorm.Open(postgres.Open(os.Getenv("POSTGRESQL")), &gorm.Config{})
	if err != nil {
		log.Fatalf("gorm.Open(postgres.Open(%q): %s", os.Getenv("POSTGRESQL"), err.Error())
	}

	storage, err := NewStorage(db)
	if err != nil {
		log.Fatalf("NewStorage: %s", err.Error())
	}

	app := Application{storage: storage}
	http.HandleFunc("/healthcheck", app.HealthCheck)
	http.HandleFunc("/categories", app.GetCategories)
	http.HandleFunc("/partners", app.GetPartners)
	http.HandleFunc("/image", app.GetImage)
	http.HandleFunc("/promtions", app.GetPromotions)

	// LISTEN=:8080
	if err := http.ListenAndServe(os.Getenv("LISTEN"), nil); err != nil {
		log.Fatalf("http.ListenAndServe(%q): %s", os.Getenv("LISTEN"), err.Error())
	}
}
