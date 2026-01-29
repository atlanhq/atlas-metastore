# OpenLineage Integration

This document describes the OpenLineage event integration feature for Atlas Metastore.

## Overview

The OpenLineage integration allows Atlas to receive, store, and query OpenLineage events in Cassandra. This feature provides data lineage tracking capabilities following the OpenLineage specification without requiring JanusGraph or Elasticsearch.

## Architecture

### Components

1. **Model Layer** (`intg/src/main/java/org/apache/atlas/model/lineage/`)
   - `OpenLineageEvent`: Model representing an OpenLineage event

2. **Data Access Layer** (`repository/src/main/java/org/apache/atlas/repository/store/graph/v2/lineage/`)
   - `OpenLineageEventDAO`: Interface for Cassandra operations
   - `OpenLineageEventDAOCassandraImpl`: Cassandra implementation with singleton pattern, retry logic, and connection pooling

3. **Service Layer** (`repository/src/main/java/org/apache/atlas/repository/store/graph/v2/lineage/`)
   - `OpenLineageEventService`: Business logic for event processing, validation, and parsing

4. **REST API Layer** (`webapp/src/main/java/org/apache/atlas/web/rest/`)
   - `OpenLineageREST`: REST endpoints for event ingestion and retrieval

### Cassandra Schema

**Keyspace:** `openlineage`

**Table:** `events_by_run`

```cql
CREATE TABLE events_by_run (
    event_id uuid,
    source text,
    jobName text,
    runID text,
    eventTime timestamp,
    event text,
    status text,
    PRIMARY KEY ((runID), eventTime, event_id)
) WITH CLUSTERING ORDER BY (eventTime DESC);
```

**Design Rationale:**
- Partition key: `runID` - Groups all events for a single job run together
- Clustering columns: `eventTime DESC, event_id` - Orders events chronologically (newest first) and ensures uniqueness
- This design optimizes for the common query pattern: "Get all events for a specific run"

## API Endpoints

### 1. Ingest Single Event

**Endpoint:** `POST /api/v1/lineage`

**Request Body:** OpenLineage event JSON

**Example:**
```bash
curl -X POST http://localhost:21000/api/v1/lineage \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "START",
    "eventTime": "2024-01-15T10:30:00.000Z",
    "run": {
      "runId": "d46e465b-d358-4d32-83d4-df660ff614dd"
    },
    "job": {
      "namespace": "my-namespace",
      "name": "my-job"
    },
    "producer": "https://my-producer.com",
    "inputs": [],
    "outputs": []
  }'
```

**Response:**
- `201 Created` - Event successfully ingested
- `400 Bad Request` - Validation error
- `500 Internal Server Error` - Processing failure

### 2. Ingest Batch Events

**Endpoint:** `POST /api/v1/lineage/batch`

**Request Body:** Array of OpenLineage event JSON strings

**Example:**
```bash
curl -X POST http://localhost:21000/api/v1/lineage/batch \
  -H "Content-Type: application/json" \
  -d '[
    "{\"eventType\":\"START\",\"eventTime\":\"2024-01-15T10:30:00.000Z\",\"run\":{\"runId\":\"run-1\"},\"producer\":\"https://my-producer.com\"}",
    "{\"eventType\":\"COMPLETE\",\"eventTime\":\"2024-01-15T10:35:00.000Z\",\"run\":{\"runId\":\"run-1\"},\"producer\":\"https://my-producer.com\"}"
  ]'
```

**Response:**
```json
{
  "totalEvents": 2,
  "successfulEvents": 2,
  "failedEvents": 0,
  "errors": {}
}
```

### 3. Get Events by Run ID

**Endpoint:** `GET /api/v1/lineage/runs/{runId}`

**Example:**
```bash
curl http://localhost:21000/api/v1/lineage/runs/d46e465b-d358-4d32-83d4-df660ff614dd
```

**Response:**
```json
{
  "runId": "d46e465b-d358-4d32-83d4-df660ff614dd",
  "eventCount": 2,
  "events": [
    {
      "eventId": "550e8400-e29b-41d4-a716-446655440000",
      "source": "https://my-producer.com",
      "jobName": "my-job",
      "runId": "d46e465b-d358-4d32-83d4-df660ff614dd",
      "eventTime": "2024-01-15T10:30:00.000Z",
      "event": "{...}",
      "status": "START"
    }
  ]
}
```

### 4. Health Check

**Endpoint:** `GET /api/v1/lineage/health`

**Example:**
```bash
curl http://localhost:21000/api/v1/lineage/health
```

**Response:**
- `200 OK` - Service is healthy
- `503 Service Unavailable` - Service is unhealthy

## Configuration

Add the following properties to `atlas-application.properties`:

```properties
# Cassandra Configuration for OpenLineage
atlas.cassandra.hostname=localhost
atlas.cassandra.replication.factor=3
```

## OpenLineage Event Format

The service expects OpenLineage events following the [OpenLineage specification](https://openlineage.io/docs/spec/object-model):

**Required Fields:**
- `eventType`: Event status (START, RUNNING, COMPLETE, FAIL, ABORT)
- `eventTime`: ISO 8601 timestamp (e.g., "2024-01-15T10:30:00.000Z")
- `run.runId`: Unique identifier for the job run

**Optional Fields:**
- `producer`: URI of the producing system
- `job.name`: Name of the job
- `job.namespace`: Namespace of the job
- `inputs`: Array of input datasets
- `outputs`: Array of output datasets
- `run.facets`: Run-level metadata facets
- `job.facets`: Job-level metadata facets

## Error Handling

The service provides comprehensive error handling:

1. **Validation Errors** (400 Bad Request):
   - Missing required fields (eventType, eventTime, runId)
   - Invalid JSON format
   - Invalid date format

2. **Storage Errors** (500 Internal Server Error):
   - Cassandra connection failures
   - Timeout errors
   - Write failures

3. **Retry Logic**:
   - Automatic retry with exponential backoff (3 attempts)
   - Retry on timeout and connection errors

## Performance Considerations

1. **Connection Pooling**: Optimized based on CPU cores
   - Local pool: `min(max(CPU * 0.75, 4), 8)`
   - Remote pool: `max(local / 2, 2)`

2. **Singleton Pattern**: Single CqlSession per JVM instance

3. **Batch Operations**: Batch endpoint for high-throughput scenarios

4. **Consistency Level**: LOCAL_QUORUM for balance between consistency and performance

## Monitoring

The implementation includes performance metrics using `AtlasPerfMetrics`:
- `processOpenLineageEvent`: Single event processing time
- `processBatchOpenLineageEvents`: Batch processing time
- `getOpenLineageEventsByRunId`: Query execution time

## Testing

Run the test suite:

```bash
mvn test -Dtest=OpenLineageEventServiceTest
```

**Note:** Tests require a running Cassandra instance or can be configured to use an embedded Cassandra for CI/CD.

## Development Patterns

This implementation follows established Atlas patterns:

1. **Singleton DAO Pattern**: Similar to `TagDAOCassandraImpl`
2. **REST API Pattern**: Following `EntityREST` conventions
3. **Service Layer**: Business logic separation from persistence
4. **Error Handling**: Consistent with Atlas exception handling
5. **Performance Tracing**: Using `AtlasPerfTracer` for monitoring

## Future Enhancements

Potential improvements for production use:

1. **Secondary Indexes**: Add indexes for querying by jobName or status
2. **Pagination**: Implement pagination for large result sets
3. **TTL Configuration**: Add configurable TTL for automatic data cleanup
4. **Facet Extraction**: Parse and store facets in separate columns for efficient querying
5. **Lineage Graph Construction**: Build lineage graphs from dataset relationships
6. **Integration with Atlas Graph**: Optional synchronization with JanusGraph for unified lineage
7. **Authentication**: Add authentication/authorization for API endpoints
8. **Compression**: Enable compression for large event payloads

## Troubleshooting

### Cassandra Connection Issues

If you see connection errors:
1. Verify Cassandra is running: `nodetool status`
2. Check hostname configuration in `atlas-application.properties`
3. Verify network connectivity: `telnet localhost 9042`

### Event Ingestion Failures

Check logs for:
- JSON parsing errors
- Required field validation failures
- Cassandra write timeouts

### Performance Issues

Monitor:
- Connection pool utilization
- Query response times
- Cassandra cluster health

## References

- [OpenLineage Specification](https://openlineage.io/docs/spec/object-model)
- [Cassandra Data Modeling](https://cassandra.apache.org/doc/latest/cassandra/data-modeling/)
- [Atlas Architecture](https://atlas.apache.org/)
