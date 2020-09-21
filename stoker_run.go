package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {

	kafkaConn, _ := os.LookupEnv("KAFKA_BOOTSTRAP_URL")
	topic, _ := os.LookupEnv("KAFKA_TEST_TOPIC")
	kafkaRate, _ := os.LookupEnv("KAFKA_SEND_RATE_IN_SEC")
	kafkaRateSec, _ := strconv.Atoi(kafkaRate)
	kafkaRateDuration := time.Duration(kafkaRateSec)

	producer, err := initProducer(kafkaConn)
	if err != nil {
		fmt.Println("Error in producer init: ", err.Error())
		os.Exit(1)
	}

	doEvery(kafkaRateDuration*time.Second, publish, "foo", producer, topic)
}

func initProducer(url string) (sarama.SyncProducer, error) {

	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	tlsConfig, err := NewTLSConfig(
		"/etc/client-ca-cert/ca.crt",
		"/etc/client-ca/ca.key",
		"/etc/cluster-ca/ca.crt")
	if err != nil {
		log.Fatal(err)
	}
	// This can be used on test server if domain does not match cert:
	tlsConfig.InsecureSkipVerify = true

	config := sarama.NewConfig()
	config.ClientID = "log-test-producer"
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig

	prd, err := sarama.NewSyncProducer([]string{url}, config)

	return prd, err
}

func doEvery(d time.Duration, f func(string, sarama.SyncProducer, time.Time, string),
	m string, p sarama.SyncProducer, t string) {

	for tick := range time.Tick(d) {
		f(m, p, tick, t)
	}
}

func publish(message string, producer sarama.SyncProducer, time time.Time, topic string) {

	fmt.Println("sending msg ", time.String()+" :: "+message)
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(time.String() + " :: " + message),
	}
	p, o, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error publishing: ", err.Error())
	}

	fmt.Println("Partition: ", p)
	fmt.Println("Offset: ", o)
}

func NewTLSConfig(clientCertFile, clientKeyFile, caCertFile string) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return &tlsConfig, err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return &tlsConfig, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	tlsConfig.BuildNameToCertificate()
	return &tlsConfig, err
}
