# Go-Stoker

Simple script meant to run in a pod alongside Strimzi deployments and generate regular traffic for testing log aggregation. 

Currently configured to utilize the secrets provided by Strimzi to connect via bootstrap. See `pod.yaml` for example of what to apply within Strimzi (tenant) namespace.


#### TODO:
- figure out internal route/<blah>.svc.local DNS instead of external 443 route?
- alter messages sent to something parameterized for variance in payload content/byte size

These can be done in operator now:
- expose metrics port in deloyment
- create podmonitor for metrics
