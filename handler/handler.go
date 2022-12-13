package handler

import (
	"encoding/json"
	"go-kafka-playground/producer"
	"log"
	"net/http"
)

type Message struct {
	Value string `json:"value"`
}

func RegisterMessage(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var value *Message
		err := json.NewDecoder(r.Body).Decode(&value)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("Received Message in request: [%v]", value)

		//produce message in kafka topic
		producer.ProduceMessages(value.Value)
		// set common header
		w.Header().Set("content-type", "application/json")
		_, er := w.Write([]byte("{\"status\": \"Success\"}"))
		if er != nil {
			return
		}
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
