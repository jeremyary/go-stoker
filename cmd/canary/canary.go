/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jeremyary/go-stoker/internal/clients"
	"github.com/jeremyary/go-stoker/internal/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	fmt.Println("Starting metrics server")
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":8080", nil)

	// fetch canary configuration
	config := config.NewCanaryConfig()

	var tlsConfig *tls.Config
	if config.TLSEnabled {
		// setup tls
		// assumes some Secret mount locations in Pod config!
		var err error
		tlsConfig, err = NewTLSConfig(
			"/etc/client-ca-cert/ca.crt",
			"/etc/client-ca/ca.key",
			"/etc/cluster-ca/ca.crt")
		if err != nil {
			log.Fatal(err)
		}
		// TODO: verify if this is still needed in-cluster
		// If insecure is required
		tlsConfig.InsecureSkipVerify = true
	}

	// grab a SyncProducer and a consumer
	producer, err := clients.InitProducer(config.BootstrapServers, config.ProducerClientID, tlsConfig)
	if err != nil {
		fmt.Println("ERROR in producer init: ", err.Error())

	}
	consumer := clients.Consumer{}

	//TODO: do we care about message (byte) size? should we vary it?
	message := "traffic generator payload"

	// send regular messages wrt KAFKA_SEND_RATE_IN_SEC and consume them
	go doEvery(config.SendRate*time.Second, clients.Publish, message, producer, config.Topic)
	consumer.Consume(config.BootstrapServers, config.Topic, tlsConfig)
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
