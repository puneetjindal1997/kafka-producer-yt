package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var brokers = []string{"localhost:29092", "localhost:29093", "localhost:29094"}

var adminClient *kadm.Client

func getAdminClient() {

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		panic(err)
	}

	adminClient = kadm.NewClient(client)

}

func getSimpleKafkaClient() *kgo.Client {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		panic(err)
	}

	return client
}

func main() {
	// topic defination
	topicName := "yt-kafka-lecture"

	//init admin client
	getAdminClient()

	// config
	// retention time use
	// properties set

	// creation of topic
	creationResp, err := adminClient.CreateTopic(context.Background(), 10, -1, nil, topicName)
	if err != nil {
		panic(err)
	}
	fmt.Println(creationResp)

	// Simple producer client init
	simpleClient := getSimpleKafkaClient()

	var wg sync.WaitGroup
	wg.Add(1)
	// prepare record to produce over kafka
	record := &kgo.Record{Topic: topicName, Value: []byte("Our first hello to kafka")}
	// produce
	simpleClient.Produce(context.Background(), record, func(_ *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
		}

	})
	wg.Wait()
}

func kafkaProducerClientClose(client *kadm.Client) {
	client.Close()
}
