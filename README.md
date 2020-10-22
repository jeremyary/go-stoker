# Go-Stoker

Go script intended to run in a pod alongside Strimzi Kafka deployments and produce/consume regular traffic for testing 
log aggregation. Various metrics are exposed via [promhttp](https://godoc.org/github.com/prometheus/client_golang/prometheus/promhttp) 
for monitoring of broker availability/latency/etc.


- PoC project currently assumes:
  - usage of external `type:route` listener in Strimzi Kafka CR configuration
  - TLS authentication via Strimzi `bootstrap` route, port `443`
  - currently utilize secrets provided by Strimzi namespace
  - see [deployment](deployment) directory for example pod/deployment YAML files to deploy within the Strimzi Kafka 
  namespace.
- Dockerfile for script builds on `golang:latest` image

