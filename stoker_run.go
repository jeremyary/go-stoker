package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/jeremyary/go-stoker/clients"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {

	fmt.Println("Starting metrics server")
	go http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":8080", nil)

	// fetch Enviro Vars
	bootstrap_url, _ := os.LookupEnv("KAFKA_BOOTSTRAP_URL")
	topic, _ := os.LookupEnv("PRODUCER_TRAFFIC_TOPIC")
	rate, _ := os.LookupEnv("PRODUCER_TRAFFIC_SEND_RATE_IN_SEC")
	clientId, _ := os.LookupEnv("PRODUCER_CLIENT_ID")

	sendRateInSec, _ := strconv.Atoi(rate)
	sendRate := time.Duration(sendRateInSec)

	// setup tls
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

	// grab a SyncProducer and a consumer
	producer, err := clients.InitProducer(bootstrap_url, clientId, tlsConfig)
	if err != nil {
		fmt.Println("ERROR in producer init: ", err.Error())

	}
	consumer := clients.Consumer{}

	//TODO: do we care about message (byte) size? should we vary it?
	message := "traffic generator payload"

	// send regular messages wrt KAFKA_SEND_RATE_IN_SEC and consume them
	go doEvery(sendRate*time.Second, clients.Publish, message, producer, topic)
	consumer.Consume(bootstrap_url, topic, tlsConfig)
}

func doEvery(duration time.Duration, callback func(string, sarama.SyncProducer, time.Time, string),
	message string, producer sarama.SyncProducer, topic string) {

	for range time.Tick(duration) {
		callback(message, producer, time.Now().UTC(), topic)
	}
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
