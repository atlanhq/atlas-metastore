# Atlas Prometheus Metrics Documentation

This document describes how Apache Atlas collects and publishes metrics in Prometheus format for monitoring and observability.

## Overview

Atlas uses Micrometer with Prometheus registry to collect and expose metrics. The metrics pipeline automatically tracks HTTP requests, JVM metrics, and supports custom business metrics.

## Architecture

```
HTTP Request → MetricsFilter → Business Logic → Response
      ↓              ↓               ↓             ↓
  Timer Start   Record URI    Custom Metrics  Timer Stop
                                    ↓
                            MetricsRegistry
                                    ↓
                         PrometheusMeterRegistry
                                    ↓
                    GET /api/atlas/admin/metrics/prometheus
```

## Core Components

### 1. MetricUtils

**Location**: `common/src/main/java/org/apache/atlas/service/metrics/MetricUtils.java`

Central utility that manages the Prometheus registry and provides metric creation methods:
- Configures percentile histograms (50th, 90th, 99th)
- Manages URI pattern canonicalization
- Sets up high cardinality detection
- Adds default tags (service="atlas-metastore", integration="local")

### 2. MetricsFilter

**Location**: `webapp/src/main/java/org/apache/atlas/web/filters/MetricsFilter.java`

Servlet filter that automatically records HTTP metrics:
- Intercepts all requests matching `/*` pattern
- Records request duration using Timer.Sample
- Tags metrics with method, status, and canonical URI
- Handles URI pattern matching from configuration

### 3. MetricsRegistryServiceImpl

**Location**: `common/src/main/java/org/apache/atlas/service/metrics/MetricsRegistryServiceImpl.java`

Service implementation for custom metrics:
- Provides counter and timer creation
- Supports method-level metrics with detailed timing
- Supports application-level metrics with custom tags
- Implements `scrape()` method for Prometheus export

### 4. RequestContext Integration

**Location**: `server-api/src/main/java/org/apache/atlas/RequestContext.java`

Request-scoped metrics collection:
- `startMetricRecord(String name)`: Begin timing an operation
- `endMetricRecord(MetricRecorder recorder)`: Complete timing and publish
- `addApplicationMetrics(String type, long value, Tag... tags)`: Add custom metrics
- Automatically publishes metrics at request completion

## Metrics Types

### HTTP Server Metrics

**Metric Name**: `http_server_requests`

**Type**: Timer with percentiles

**Tags**:
- `method`: HTTP method (GET, POST, PUT, DELETE, etc.)
- `status`: HTTP response code (200, 404, 500, etc.)
- `uri`: Canonicalized URI pattern
- `service`: Always "atlas-metastore"
- `integration`: Always "local"

**Percentiles**: 0.5 (50th), 0.9 (90th), 0.99 (99th)

**Example**:
```
http_server_requests_seconds{method="GET",status="200",uri="/api/atlas/v2/entity/guid/*",service="atlas-metastore",integration="local",quantile="0.5"} 0.123
http_server_requests_seconds_count{method="GET",status="200",uri="/api/atlas/v2/entity/guid/*"} 42
http_server_requests_seconds_sum{method="GET",status="200",uri="/api/atlas/v2/entity/guid/*"} 5.234
```

### Method-Level Metrics

**Metric Name**: `method_dist_summary`

**Type**: Timer with percentiles

**Tags**:
- `name`: Method name
- `uri`: Request URI
- `origin`: Origin identifier

**Configuration**:
- Enable with: `atlas.metrics.method_level.enable=true`
- Filter methods with: `atlas.metrics.method_patterns=["method1", "method2"]`

### Application-Level Metrics

**Metric Name**: `application_level_metrics_summary`

**Type**: Timer with Service Level Objectives (SLOs)

**SLO Buckets**: 500ms, 750ms, 1s, 1.2s, 1.5s, 2s, 3s, 4s, 5s, 7s, 10s, 15s, 20s, 25s, 30s, 40s, 60s, 90s, 120s, 180s

**Tags**: Custom tags provided by application code

### JVM Metrics

Atlas automatically includes:
- JVM memory metrics
- GC metrics
- Thread pool metrics
- Jetty connection pool metrics
- Jetty thread pool metrics

## Configuration

### Required Properties

```properties
# URI patterns to track (regex supported)
atlas.metrics.uri_patterns=["/api/atlas/v2/entity/.*", "/api/atlas/v2/search/.*"]

# Enable method-level metrics
atlas.metrics.method_level.enable=true

# Methods to track
atlas.metrics.method_patterns=["createOrUpdate", "searchUsingDSL", "getById"]
```

### URI Pattern Canonicalization

Atlas automatically canonicalizes URIs to reduce metric cardinality:
- `/api/atlas/v2/entity/guid/12345` → `/api/atlas/v2/entity/guid/*`
- `/api/atlas/v2/entity/bulk` → `/api/atlas/v2/entity/bulk`
- `/api/atlas/v2/search/dsl?query=...` → `/api/atlas/v2/search/dsl`

## Usage Patterns

### Recording Custom Metrics

#### Using RequestContext (Recommended)

```java
// Start timing an operation
MetricRecorder recorder = RequestContext.get().startMetricRecord("searchEntities");
try {
    // Perform operation
    List<AtlasEntity> results = performSearch();
    
    // Add custom metric
    RequestContext.get().addApplicationMetrics(
        "search_results", 
        results.size(), 
        Tag.of("search_type", "dsl")
    );
    
    return results;
} finally {
    // Stop timing
    RequestContext.get().endMetricRecord(recorder);
}
```

#### Using MetricsRegistry Directly

```java
@Inject
private MetricsRegistry metricsRegistry;

// Create a counter
metricsRegistry.counter("entities_created", Tag.of("type", "hive_table"));

// Create a timer
Timer.Sample sample = metricsRegistry.timer("import_duration");
// ... perform import ...
sample.stop(Tag.of("status", "success"));
```

### Method-Level Metrics

Method-level metrics are automatically recorded if:
1. `atlas.metrics.method_level.enable=true`
2. Method name matches patterns in `atlas.metrics.method_patterns`
3. Method uses RequestContext metric recording

### Performance Considerations

1. **URI Cardinality**: Use pattern matching to avoid high cardinality
2. **Tag Values**: Keep tag values bounded (avoid user IDs, GUIDs)
3. **Metric Names**: Use consistent naming conventions
4. **SLO Buckets**: Choose appropriate buckets for your use case

## Metrics Endpoint

### Accessing Metrics

```bash
# Get Prometheus metrics
curl http://localhost:21000/api/atlas/admin/metrics/prometheus

# Sample output
# HELP http_server_requests_seconds  
# TYPE http_server_requests_seconds summary
http_server_requests_seconds{method="GET",status="200",uri="/api/atlas/v2/types/typedefs",quantile="0.5",} 0.021
http_server_requests_seconds{method="GET",status="200",uri="/api/atlas/v2/types/typedefs",quantile="0.9",} 0.125
http_server_requests_seconds_count{method="GET",status="200",uri="/api/atlas/v2/types/typedefs",} 156.0
http_server_requests_seconds_sum{method="GET",status="200",uri="/api/atlas/v2/types/typedefs",} 5.234
```

### Prometheus Configuration

Add to your `prometheus.yml`:
```yaml
scrape_configs:
  - job_name: 'atlas'
    metrics_path: '/api/atlas/admin/metrics/prometheus'
    static_configs:
      - targets: ['localhost:21000']
```

## Best Practices

### Metric Naming
- Use lowercase with underscores for custom metrics
- Include unit in metric name (e.g., `_seconds`, `_bytes`)
- Be descriptive but concise

### Tagging
- Keep tag cardinality low
- Use static tag values when possible
- Avoid sensitive information in tags
- Common tags: status, type, operation, method

### Performance
- Use RequestContext for request-scoped metrics
- Batch metric updates when possible
- Avoid creating metrics in hot paths
- Reuse metric instances

### Monitoring Patterns
- Monitor error rates: Filter by status >= 400
- Track latency: Use percentiles, not averages
- Set up alerts on SLO violations
- Dashboard key metrics by URI pattern

## Common Metrics Queries

### Request Rate
```promql
rate(http_server_requests_seconds_count[5m])
```

### Error Rate
```promql
rate(http_server_requests_seconds_count{status=~"5.."}[5m])
```

### P99 Latency
```promql
http_server_requests_seconds{quantile="0.99"}
```

### Method Performance
```promql
rate(method_dist_summary_seconds_sum[5m]) / rate(method_dist_summary_seconds_count[5m])
```

## Troubleshooting

### Missing Metrics
1. Check if MetricsFilter is registered in web.xml
2. Verify URI patterns match your endpoints
3. Ensure metrics endpoint is accessible
4. Check application logs for metric registration

### High Cardinality
1. Review URI patterns for dynamic segments
2. Check tag values for unbounded sets
3. Use canonicalization for dynamic URIs
4. Monitor PrometheusMeterRegistry warnings

### Performance Impact
1. Metrics collection is generally low-overhead
2. Percentile calculation has slight overhead
3. Reduce percentiles if performance is critical
4. Use sampling for high-volume endpoints

## Summary

Atlas provides comprehensive metrics collection through:
- Automatic HTTP request/response metrics
- JVM and system metrics
- Custom business metrics
- Method-level performance tracking
- Flexible configuration options
- Standard Prometheus exposition format

This enables effective monitoring, alerting, and performance optimization of Atlas deployments.