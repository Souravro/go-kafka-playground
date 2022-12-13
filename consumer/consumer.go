package consumer

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// Sarama configuration options
var (
	brokers  = "0.0.0.0:9092"
	version  = "0.11.0.0"
	topic    = "purchases"
	group    = "example"
	assignor = "range"
	oldest   = false
)

const (
	YYYYMMDD = "2006-01-02"
)

func createConfig() *sarama.Config {
	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = version

	return config
}

func ConsumeMessages() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log.Println("Starting a new Sarama consumer...")
	config := createConfig()

	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategySticky}
	case "roundrobin":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
	case "range":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRange}
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{
		ready: make(chan bool),
	}

	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side re-balance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, strings.Split(topic, ","), &consumer); err != nil {
				log.Panicf("Consumer: Error: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	}

	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Consumer: error closing client: %v", err)
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message := <-claim.Messages():
			//log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")
			processMessage(message)

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka re balance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

func processMessage(message *sarama.ConsumerMessage) {
	// log message and timestamp in a file
	// If the file doesn't exist, create it, or append to the file
	now := time.Now().UTC()
	f, err := os.OpenFile("consumer/log/consumption-"+now.Format(YYYYMMDD)+".log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Consumer: Unable to open file to log consumption result. Error: [%v]", err)
	}
	messageToWrite := message.Timestamp.String() + " - " + message.Topic + " :: " + string(message.Value) + "\n"
	_, writeErr := f.WriteString(messageToWrite)
	if writeErr != nil {
		log.Fatalf("Consumer: Unable to write to log consumption file. Error: [%v]", err)
	}
}
