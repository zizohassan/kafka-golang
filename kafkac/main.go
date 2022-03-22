package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"strings"
	"time"
)

func main() {

	topic := "email"
	partion := 0

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"127.0.0.1:9092"},
		Topic:     topic,
		Partition: partion,
		MaxWait:   3 * time.Second,
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err.Error())
			break
		}
		switch string(m.Key){
		case "email":
			st := strings.Split(string(m.Value), ",")
			fmt.Println("------ key " , string(m.Key) , "--------")
			fmt.Println("we will send email to : " , st[0])
			fmt.Println("content : " , st[1])
		}
	}
	err := r.Close()
	if err != nil {
		log.Fatal(err)
	}
}
