package main

import (
	"fmt"
	"go-kafka-playground/consumer"
	"go-kafka-playground/routes"
	"log"
	"net/http"
)

func main() {
	// register exported routes
	routes.RegisterRoutes()

	// start kafka consumer group
	go consumer.ConsumeMessages()

	fmt.Printf("Starting server at port 8080...\n")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
