package clients

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Consumer struct {
}

var (
	eventsConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kafka_events_consumed_total",
		Help: "The total number of events consumed",
	})
)

func (c *Consumer) Consume(KafkaServer string, KafkaTopic string, tlsConfig *tls.Config) {

	config := sarama.NewConfig()
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig
	config.ClientID = "log-test-producer"
	config.Version = sarama.V2_4_0_0
	group, err := sarama.NewConsumerGroup([]string{KafkaServer}, "my-group", config)
	if err != nil {
		panic(err)
	}

	go func() {
		for err := range group.Errors() {
			panic(err)
		}
	}()

	func() {
		ctx := context.Background()
		for {
			topics := []string{KafkaTopic}
			err := group.Consume(ctx, topics, c)
			if err != nil {
				fmt.Printf("kafka consume failed: %v, sleeping and retry in a moment\n", err)
				time.Sleep(time.Second)
			}
		}
	}()
}

func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("consumed message: %v\n", string(msg.Value))
		eventsConsumed.Inc()
		sess.MarkMessage(msg, "")
	}
	return nil
}
