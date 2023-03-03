package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"math/rand"
	"strconv"
	"time"
)

type KafkaMessage struct {
	Id    int    `json:"id"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

const TOPIC = "test-topic"

func main() {

	go prod()
	go consumer(1)
	select {}
}
func prod() {
	w := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  TOPIC,
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.LeastBytes{},
		WriteTimeout:           time.Second / 2,
	}
	defer w.Close()
	ctx := context.Background()
	for true {
		producerKafkaMessage(ctx, w, &KafkaMessage{
			Id:    time.Now().Nanosecond(),
			Key:   strconv.Itoa(rand.Int()),
			Value: "test",
		})

	}
}
func consumer(id int) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "consumer-group-id",
		Topic:    TOPIC,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  time.Second,
	})
	ctx := context.Background()
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			break
		}

		fmt.Printf("id %d message at offset %d: %s = %s\n", id, m.Offset, string(m.Key), string(m.Value))
		if err := r.CommitMessages(ctx, m); err != nil {
			log.Fatal("failed to commit messages:", err)
		}
	}
	defer r.Close()
}
func producerKafkaMessage(ctx context.Context, w *kafka.Writer, message *KafkaMessage) {
	jsonByte, _ := json.Marshal(message)
	m := kafka.Message{
		Key:   []byte(strconv.Itoa(message.Id)),
		Value: jsonByte,
	}
	producer(ctx, w, m)
}

func producer(ctx context.Context, w *kafka.Writer, message kafka.Message) {
	err := w.WriteMessages(ctx, message)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
}
