package producer

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"strconv"
	"strings"
	"sync"
)

// Sarama configuration options
var (
	brokers             = "0.0.0.0:9092"
	version             = "0.11.0.0"
	topic               = "purchases"
	producers           = 1
	recordsNumber int64 = 1
)

// pool of producers that ensure transactional-id is unique.
type producerProvider struct {
	transactionIdGenerator int32
	producersLock          sync.Mutex
	producers              []sarama.AsyncProducer
	producerProvider       func() sarama.AsyncProducer
}

func createProducerProvider(brokers string, config *sarama.Config) *producerProvider {
	provider := &producerProvider{}
	provider.producerProvider = func() sarama.AsyncProducer {
		suffix := provider.transactionIdGenerator
		// Append transactionIdGenerator to current config.Producer.Transaction.ID to ensure transaction-id uniqueness.
		if config.Producer.Transaction.ID != "" {
			provider.transactionIdGenerator++
			config.Producer.Transaction.ID = config.Producer.Transaction.ID + "-" + fmt.Sprint(suffix)
		}
		producer, err := sarama.NewAsyncProducer(strings.Split(brokers, ","), config)
		if err != nil {
			return nil
		}
		return producer
	}
	return provider
}

func createConfig() *sarama.Config {
	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = version
	config.Producer.Idempotent = true
	config.Producer.Return.Errors = false
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.Transaction.Retry.Backoff = 10
	config.Producer.Transaction.ID = "txn_producer"
	config.Net.MaxOpenRequests = 1

	return config
}

func ProduceMessages(message string) {
	log.Println("Starting a new Sarama producer...")
	ctx, cancel := context.WithCancel(context.Background())

	config := createConfig()
	prodProvider := createProducerProvider(brokers, config)

	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			default:
				produceRecord(prodProvider, message)
			}

		}()
	}

	wg.Wait()
	cancel()

	prodProvider.clear()
}

func produceRecord(provider *producerProvider, message string) {
	producer := provider.borrow()
	defer provider.release(producer)

	// Start kafka transaction
	err := producer.BeginTxn()
	if err != nil {
		log.Printf("Producer: unable to start txn %s\n", err)
		return
	}

	// Produce some records in transaction
	for i := int64(0); i < recordsNumber; i++ {
		value := sarama.StringEncoder(message + " - app - " + strconv.Itoa(int(i)))
		producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: value}
	}

	// commit transaction
	err = producer.CommitTxn()
	if err != nil {
		log.Printf("Producer: unable to commit txn %s\n", err)
		for {
			if producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
				// fatal error. need to recreate producer.
				log.Printf("Producer: producer is in a fatal state, need to recreate it")
				break
			}
			// If producer is in abortable state, try to abort current transaction.
			if producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
				err = producer.AbortTxn()
				if err != nil {
					// If an error occurred just retry it.
					log.Printf("Producer: unable to abort transaction: %+v", err)
					continue
				}
				break
			}
			// if not you can retry
			err = producer.CommitTxn()
			if err != nil {
				log.Printf("Producer: unable to commit txn %s\n", err)
				continue
			}
		}
		return
	} else {
		log.Println("Producer: txn successfully committed.")
	}
}

func (p *producerProvider) clear() {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	for _, producer := range p.producers {
		err := producer.Close()
		if err != nil {
			return
		}
	}
	p.producers = p.producers[:0]
}

func (p *producerProvider) borrow() (producer sarama.AsyncProducer) {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	if len(p.producers) == 0 {
		for {
			producer = p.producerProvider()
			if producer != nil {
				return
			}
		}
	}

	index := len(p.producers) - 1
	producer = p.producers[index]
	p.producers = p.producers[:index]
	return
}

func (p *producerProvider) release(producer sarama.AsyncProducer) {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	// If released producer is erroneous close it and don't return it to the producer pool.
	if producer.TxnStatus()&sarama.ProducerTxnFlagInError != 0 {
		// Try to close it
		_ = producer.Close()
		return
	}
	p.producers = append(p.producers, producer)
}
