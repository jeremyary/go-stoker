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

	// fetch Enviro Vars
	bootstrap_url, _ := os.LookupEnv("KAFKA_BOOTSTRAP_URL")
	topic, _ := os.LookupEnv("KAFKA_TEST_TOPIC")
	rate, _ := os.LookupEnv("KAFKA_SEND_RATE_IN_SEC")
	clientId, _ := os.LookupEnv("CLIENT_ID")

	sendRateInSec, _ := strconv.Atoi(rate)
	sendRate := time.Duration(sendRateInSec)

	// set up TLS & grab a SyncProducer
	producer, err := initProducer(bootstrap_url, clientId)
	if err != nil {
		fmt.Println("ERROR in producer init: ", err.Error())

	}

	//TODO: do we care about message (byte) size? should we vary it?
	message := "traffic generator payload"

	// send regular messages wrt KAFKA_SEND_RATE_IN_SEC
	doEvery(sendRate*time.Second, publish, message, producer, topic)
}

func doEvery(duration time.Duration, callback func(string, sarama.SyncProducer, time.Time, string),
	message string, producer sarama.SyncProducer, topic string) {

	for range time.Tick(duration) {
		callback(message, producer, time.Now().UTC(), topic)
	}
}

func publish(message string, producer sarama.SyncProducer, now time.Time, topic string) {

	fmt.Println("sending - [", now.String(), "] ", message)
	producerMsg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(now.String() + " :: " + message),
	}
	partition, offset, err := producer.SendMessage(producerMsg)
	if err != nil {
		fmt.Println("ERROR publishing: ", err.Error())
	}
	fmt.Println("sent - [", now.String(), "] partition: ", partition, ", offset: ", offset)
}

func initProducer(url string, clientId string) (sarama.SyncProducer, error) {

	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	// assumes some Secret mount locations in Pod config!
	tlsConfig, err := NewTLSConfig(
		"/etc/client-ca-cert/ca.crt",
		"/etc/client-ca/ca.key",
		"/etc/cluster-ca/ca.crt")
	if err != nil {
		log.Fatal(err)
	}

	// TODO: verify if this is still needed in-cluster
	// If insecure is required
	tlsConfig.InsecureSkipVerify = true

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

func NewTLSConfig(clientCertPath, clientKeyPath, clusterCertPath string) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// Load client cert/key
	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		return &tlsConfig, err
	}
	tlsConfig.Certificates = []tls.Certificate{clientCert}

	// Load cluster cert
	clusterCert, err := ioutil.ReadFile(clusterCertPath)
	if err != nil {
		return &tlsConfig, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(clusterCert)
	tlsConfig.RootCAs = caCertPool

	tlsConfig.BuildNameToCertificate()
	return &tlsConfig, err
}
