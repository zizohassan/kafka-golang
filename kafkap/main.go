package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

func main() {
	topic := "email"
	partion := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "127.0.0.1:9092", topic, partion)
	if err != nil {
		log.Fatal(err)
	}

	conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Key: []byte("email"), Value: []byte("zizo19999988@gmail.com,welcom in kafka")},
	)
	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close()
	if err != nil {
		log.Fatal(err)
	}
}
