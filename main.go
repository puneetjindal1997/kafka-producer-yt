package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const topicName string = "yt-kafka-lecture"

var brokers = []string{"localhost:29092", "localhost:29093", "localhost:29094"}

func getAdminClient() *kgo.Client {
	balancer := kgo.RoundRobinBalancer()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.Balancers(balancer),
	)
	if err != nil {
		panic(err)
	}

	adminClient := kadm.NewClient(client)
	_, err = adminClient.CreateTopic(context.Background(), 10, -1, nil, topicName)
	if err != nil {
		if !strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") {
			panic(err)
		}
	}
	return client
}

func main() {
	simpleClient := getAdminClient()

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		KafkaProducer(i, &wg, simpleClient)
	}
	wg.Wait()
}

func KafkaProducer(i int, wg *sync.WaitGroup, simpleClient *kgo.Client) {
	kafkaKey := strconv.Itoa(i)
	wg.Add(1)
	// prepare record to produce over kafka
	record := &kgo.Record{Topic: topicName, Key: []byte("kafka_" + kafkaKey), Value: []byte(fmt.Sprintf("Our %s hello to kafka", kafkaKey))}
	// produce
	simpleClient.Produce(context.Background(), record, func(_ *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
		}

	})
}

func kafkaProducerClientClose(client *kadm.Client) {
	client.Close()
}
