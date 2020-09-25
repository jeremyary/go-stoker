package clients

import (
	"crypto/tls"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"log"
	"os"
	"time"
)

var (
	eventsProduced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kafka_events_produced_total",
		Help: "The total number of events produced",
	})
	eventsProducedFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kafka_events_produced_failed_total",
		Help: "The total number of events failed to produce",
	})
)

func InitProducer(url string, clientId string, tlsConfig *tls.Config) (sarama.SyncProducer, error) {

	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	config := sarama.NewConfig()
	config.ClientID = clientId
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll // bit of an opinion here, tenant req. could vary?
	config.Producer.Return.Successes = true
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig

	producer, err := sarama.NewSyncProducer([]string{url}, config)
	return producer, err
}

func Publish(message string, producer sarama.SyncProducer, now time.Time, topic string) {

	fmt.Println("sending - [", now.String(), "] ", message)
	producerMsg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(now.String() + " :: " + message),
	}
	partition, offset, err := producer.SendMessage(producerMsg)
	if err != nil {
		fmt.Println("ERROR publishing: ", err.Error())
		eventsProducedFailed.Inc()
	}
	fmt.Println("sent - [", now.String(), "] partition: ", partition, ", offset: ", offset)
	eventsProduced.Inc()
}
