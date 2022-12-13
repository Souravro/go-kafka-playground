## go-kafka-playground
This project implements a simple pub-sub architecture in kafka using Shopify's `sarama` package for Golang.

### Prerequisites
1. Docker and Docker Compose
2. Java runtime
3. Golang installed

### Getting Started
 - Clone the project 
 - Use the following command to start zookeeper and broker docker containers.    
   ```bash
   $ make run
    ```
 - Verify if the containers are successfully created or not. The following command should show 2 docker containers for zookeeper and docker respectively -
    ```bash
   $ docker ps
    ```
 - Run the go project using -
    ```bash
   $ go run go-kafka-playground
    ```
 - Once the local server has started on :8080, use the following POST endpoint to some message.
    ```
   localhost:8080/registerMessage
    ```
   Example json body to pass -
    ```
    {
        "value": "purchase 1"
    }
    ```
 - This message will be pushed via a producer, depending on the number of records (hardcoded in producer.go) to a kafka topic `purchases` (hardcoded).
 - The message will be consumed and will be visible under the `consumer/log/` location.