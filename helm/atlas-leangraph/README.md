# Atlas Leangraph Helm Chart

## Overview

Atlas Leangraph is a lightweight graph database deployment that mirrors requests from the main Atlas and Atlas-Read services. It uses separate Cassandra keyspaces and Elasticsearch indices to maintain an independent data store while receiving the same write operations as the primary Atlas instance.

## Architecture

### High-Level Architecture

```mermaid
flowchart TB
    subgraph "Incoming Traffic"
        Client[Client Requests]
    end

    subgraph "Atlas Namespace"
        subgraph "Atlas Primary"
            AN[Atlas Nginx Deployment]
            AS[Atlas StatefulSet]
            ASvc[atlas-service-atlas]
            ARSvc[atlas-ratelimited]
        end

        subgraph "Atlas Read"
            ARN[Atlas-Read Nginx Deployment]
            ARS[Atlas-Read StatefulSet]
            ARSSvc[atlas-read-service-atlas]
            ARRSvc[atlas-read-ratelimited]
        end

        subgraph "Atlas Leangraph"
            LN[Leangraph Nginx Deployment]
            LS[Leangraph StatefulSet]
            LSvc[atlas-leangraph-service]
            LNSvc[atlas-leangraph-nginx-service]
        end
    end

    subgraph "Data Stores"
        subgraph "Cassandra"
            CK1[Keyspace: atlas]
            CK2[Keyspace: leangraph_atlas]
            CK3[Keyspace: leangraph_tags]
        end

        subgraph "Elasticsearch"
            EI1[Index: janus_*]
            EI2[Index: janus_leangraph]
        end
    end

    Client --> AN
    Client --> ARN
    AN -->|Primary Request| ASvc --> AS
    AN -.->|Async Mirror| LNSvc --> LN --> LSvc --> LS
    ARN -->|Primary Request| ARSSvc --> ARS
    ARN -.->|Async Mirror| LNSvc

    AS --> CK1
    AS --> EI1
    ARS --> CK1
    ARS --> EI1
    LS --> CK2
    LS --> CK3
    LS --> EI2
```

### Request Flow with Leangraph Enabled

```mermaid
sequenceDiagram
    participant C as Client
    participant AN as Atlas Nginx
    participant AS as Atlas Service
    participant LN as Leangraph Nginx
    participant LS as Leangraph Service

    C->>AN: HTTP Request
    AN->>AS: Primary Request
    AN-->>LN: Async Mirror (fire-and-forget)
    AS->>AN: Response
    AN->>C: Response
    
    Note over AN,LN: Mirror request does not<br/>wait for response
    
    LN->>LS: Forwarded Request
    LS-->>LN: Response (ignored)
```

### Component Deployment

```mermaid
flowchart LR
    subgraph "When global.leangraph.enabled=false"
        A1[Atlas STS with Nginx Sidecar]
        A2[Atlas-Read STS with Nginx Sidecar]
    end

    subgraph "When global.leangraph.enabled=true"
        B1[Atlas STS<br/>No Nginx Sidecar]
        B2[Atlas Nginx Deployment]
        B3[Atlas-Read STS<br/>No Nginx Sidecar]
        B4[Atlas-Read Nginx Deployment]
        B5[Leangraph STS]
        B6[Leangraph Nginx Deployment]
        
        B2 -->|mirrors to| B6
        B4 -->|mirrors to| B6
        B6 --> B5
    end
```

## Configuration

### Global Flag

The entire Leangraph deployment is controlled by a single global flag:

```yaml
global:
  leangraph:
    enabled: false  # Set to true to enable Leangraph
    service: "atlas-leangraph-nginx-service.atlas.svc.cluster.local"
```

### Leangraph-Specific Configuration

| Property | Default Value | Description |
|----------|---------------|-------------|
| `atlas.leangraph.tag_table_name` | `leangraph_tags_by_id` | Cassandra table for tags |
| `atlas.leangraph.propagated_tag_table_name` | `leangraph_propagated_tags_by_source` | Cassandra table for propagated tags |
| `atlas.leangraph.new_keyspace` | `leangraph_tags` | New Cassandra keyspace for tags |
| `atlas.leangraph.cql_keyspace` | `leangraph_atlas` | Main Cassandra keyspace |
| `atlas.leangraph.index_name` | `janus_leangraph` | Elasticsearch index name |

### OTEL Configuration

Leangraph uses distinct OTEL service names for observability:

```yaml
OTEL_SERVICE_NAME: leangraph_atlas
OTEL_RESOURCE_ATTRIBUTES: service.name=leangraph_atlas,...
```

## Resources Created

When `global.leangraph.enabled=true`, the following resources are created:

### Atlas-Leangraph Chart

| Resource | Name | Description |
|----------|------|-------------|
| ConfigMap | `atlas-leangraph-config` | Atlas application properties |
| ConfigMap | `atlas-leangraph-nginx-config` | Nginx configuration |
| StatefulSet | `atlas-leangraph` | Main Atlas Leangraph pods |
| Deployment | `atlas-leangraph-nginx` | Nginx proxy deployment |
| Service | `atlas-leangraph-service` | Service for StatefulSet |
| Service | `atlas-leangraph-nginx-service` | Service for Nginx deployment |

### Atlas Chart Changes

| Resource | Change |
|----------|--------|
| StatefulSet | Nginx sidecar removed |
| Deployment | New `atlas-nginx` deployment created |
| Service | `atlas-ratelimited` points to nginx deployment |
| ConfigMap | Nginx config updated with mirror directive |

### Atlas-Read Chart Changes

| Resource | Change |
|----------|--------|
| StatefulSet | Nginx sidecar removed |
| Deployment | New `atlas-read-nginx` deployment created |
| Service | `atlas-read-ratelimited` points to nginx deployment |
| ConfigMap | Nginx config updated with mirror directive |

## Nginx Mirror Configuration

The nginx mirror directive is used for async request forwarding:

```nginx
location /api/meta/ {
    mirror @mirror_leangraph;
    mirror_request_body on;
    
    # Primary request handling
    proxy_pass $atlas_upstream;
    ...
}

location @mirror_leangraph {
    internal;
    proxy_pass http://atlas-leangraph-nginx-service.atlas.svc.cluster.local$request_uri;
    proxy_connect_timeout 1s;
    proxy_read_timeout 1s;
    # Fire-and-forget: response is ignored
}
```

## Data Isolation

```mermaid
flowchart TB
    subgraph "Atlas Primary Data"
        A1[Cassandra: atlas keyspace]
        A2[ES: janus_* indices]
    end

    subgraph "Leangraph Data"
        L1[Cassandra: leangraph_atlas keyspace]
        L2[Cassandra: leangraph_tags keyspace]
        L3[ES: janus_leangraph index]
    end

    subgraph "Shared Infrastructure"
        C[Cassandra Cluster]
        E[Elasticsearch Cluster]
    end

    A1 --> C
    L1 --> C
    L2 --> C
    A2 --> E
    L3 --> E
```

## Backward Compatibility

This implementation is fully backward compatible:

| Scenario | Behavior |
|----------|----------|
| `global.leangraph.enabled=false` | No changes to existing behavior. Nginx runs as sidecar. No Leangraph resources created. |
| `global.leangraph.enabled=true` | Nginx moves to separate deployment. All requests mirrored to Leangraph. |

## Deployment

### Enable Leangraph

```yaml
# values.yaml override
global:
  leangraph:
    enabled: true
```

### Helm Install

```bash
helm upgrade --install atlas-leangraph ./helm/atlas-leangraph \
  --namespace atlas \
  --set global.leangraph.enabled=true
```

## Monitoring

### Key Metrics

- Leangraph nginx request rates and latencies
- Mirror request success/failure rates
- Cassandra keyspace metrics for `leangraph_atlas`
- Elasticsearch index metrics for `janus_leangraph`

### OTEL Traces

All Leangraph traces are tagged with:
- `service.name=leangraph_atlas`
- `k8s.namespace.name=atlas`

## Troubleshooting

### Mirror Requests Not Reaching Leangraph

1. Verify `global.leangraph.enabled=true` in all charts
2. Check nginx deployment logs for mirror errors
3. Verify `atlas-leangraph-nginx-service` is accessible

### Data Not Syncing

1. Check Leangraph StatefulSet pod logs
2. Verify Cassandra keyspace `leangraph_atlas` exists
3. Verify Elasticsearch index `janus_leangraph` exists

### Performance Issues

1. Mirror timeout is set to 1s - adjust if needed
2. Check Leangraph nginx deployment resource limits
3. Monitor Cassandra and Elasticsearch cluster health

