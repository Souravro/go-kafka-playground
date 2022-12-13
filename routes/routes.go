package routes

import (
	"go-kafka-playground/handler"
	"net/http"
)

func RegisterRoutes() {
	// accepts a message and pushes it to kafka topic along with other details
	http.HandleFunc("/registerMessage", handler.RegisterMessage)
}
