# Go-Stoker

Simple script meant to run in a pod alongside Strimzi deployments and generate regular traffic for testing log aggregation. 

Currently configured to utilize the secrets provided by Strimzi to connect via bootstrap. See `pod.yaml` for example of what to apply within Strimzi (tenant) namespace.


#### TODO:
- figure out internal route/<blah>.svc.local DNS instead of external 443 route?
- consider Deployment for scaling
- consider operator for CR parameter capabilities
- alter messages sent to something other than timestamp/foo, maybe generate something dynamic