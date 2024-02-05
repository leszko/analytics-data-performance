package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"os"
	"os/signal"
	"strings"
)

type KafkaClient struct {
	producer *kafka.Producer
	consumer *kafka.Consumer
	topic    string
}

func CreateKafkaClient(topic string) *KafkaClient {
	bootstrap, _ := os.LookupEnv("KAFKA_BOOTSTRAP")
	username, _ := os.LookupEnv("KAFKA_USER")
	password, _ := os.LookupEnv("KAFKA_PASSWORD")
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"security.protocol":  "SASL_SSL",
		"sasl.mechanisms":    "PLAIN",
		"sasl.username":      username,
		"sasl.password":      password,
		"session.timeout.ms": 45000,
		"group.id":           1,
		"batch.num.messages": 1000,
		"auto.offset.reset":  "earliest",
		"client.id":          1,
	}
	p, err := kafka.NewProducer(cfg)
	if err != nil {
		panic(err)
	}
	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		panic(err)
	}

	client := &KafkaClient{
		producer: p,
		consumer: c,
		topic:    topic,
	}

	go func() {
		for e := range client.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					//fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	return client
}

func (c *KafkaClient) Send(msg string) {
	// Delivery report handler for produced messages
	msgShort := strings.Join(strings.Fields(msg), "")

	// Produce messages to topic (asynchronously)
	c.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &c.topic, Partition: kafka.PartitionAny},
		Key:            []byte(msgShort),
		Value:          []byte(msgShort),
	}, nil)

	// Wait for message deliveries before shutting down
	//c.producer.Flush(15 * 1000)
}

func (c *KafkaClient) Subscribe() chan string {
	err := c.consumer.SubscribeTopics([]string{c.topic}, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe to topic: %s\n", err)
		os.Exit(1)
	}

	// Set up a signal channel to handle termination gracefully
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	msgChan := make(chan string, 1)

	// Consume messages in a loop
	go func() {
		for {
			select {
			case sig := <-sigchan:
				fmt.Printf("Caught signal %v: terminating\n", sig)
				return
			default:
				// Poll for messages
				msg, err := c.consumer.ReadMessage(-1)
				if err == nil {
					// Process the received message
					msgChan <- string(msg.Value)
				} else {
					// Handle errors
					fmt.Printf("Error while consuming message: %v\n", err)
				}
			}
		}
	}()

	return msgChan
}
