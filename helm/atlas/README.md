# atlas
This chart will install the apache atlas which use elasticsearch and cassandra.

## Local Kubernetes

Use the local profile when deploying to a single-node cluster (Docker Desktop, kind, minikube):

```bash
helm upgrade --install atlas-local ./helm/atlas \
  -n atlas-local \
  --create-namespace \
  -f helm/atlas/values.local.yaml
```

Notes:
- This profile disables ingress, PodMonitor, statsd job, and infrastructure subcharts.
- The default local `atlas.image` is `busybox` to validate chart deployment mechanics.
- Set `atlas.image.repository` and `atlas.image.tag` to a real Atlas image when you want the service itself to boot.
