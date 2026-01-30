# OpenLineage Integration

This document describes the OpenLineage event integration feature for Atlas Metastore.

## Overview

The OpenLineage integration allows Atlas to receive, store, and query OpenLineage events in Cassandra. This feature provides data lineage tracking capabilities following the OpenLineage specification without requiring JanusGraph or Elasticsearch.

OpenLineage is consumed from a Kafka queue.

```
atlas.kafka.openlineage.enabled=true
atlas.kafka.openlineage.topic=openlineage-topic
...
For full list of options, see KafkaEventConsumer.
```

Event IDs are timeuuid values derived from `eventTime`.

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
   - `OpenLineageREST`: REST endpoints for event retrieval and health

### Cassandra Schema

**Keyspace:** `openlineage`

**Table:** `events_by_run`

```cql
CREATE TABLE events_by_run (
    eventId timeuuid,
    source text,
    jobName text,
    runID text,
    eventTime timestamp,
    event text,
    status text,
    PRIMARY KEY ((runID), eventId)
) WITH CLUSTERING ORDER BY (eventId DESC);
```

**Design Rationale:**
- Partition key: `runID` - Groups all events for a single job run together
- Clustering columns: `eventId DESC` - Orders events chronologically (newest first) and ensures uniqueness
- This design optimizes for the common query pattern: "Get all events for a specific run"

## API Endpoints

### 1. Get Events by Run ID (paginated)

**Endpoint:** `GET /api/atlas/v2/openlineage/runs/{runId}`

**Query Parameters:**
- `pageSize` (optional, default `25`): Max events to return
- `pagingState` (optional): Opaque cursor returned from the previous page

**Example:**
```bash
curl -u admin:admin \
  "http://localhost:21000/api/atlas/v2/openlineage/runs/d46e465b-d358-4d32-83d4-df660ff614dd?pageSize=25"
```

**Response:**
```json
{
  "success": true,
  "runId": "d46e465b-d358-4d32-83d4-df660ff614dd",
  "eventCount": 2,
  "pageSize": 25,
  "nextPagingState": "AAAAALkAAAAjAAAAAQAAABcAAABwYWdlZC1ydW4tMTc2OTgwMjQ1MDQ0MQE...",
  "events": [
    {
      "eventId": "396c9c80-fe0b-11f0-b2d7-68fad00996a0",
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

**Next Page Example:**
```bash
curl -u admin:admin \
  "http://localhost:21000/api/atlas/v2/openlineage/runs/d46e465b-d358-4d32-83d4-df660ff614dd?pageSize=25&pagingState=AAAAALkAAAAjAAAAAQAAABcAAABwYWdlZC1ydW4tMTc2OTgwMjQ1MDQ0MQE..."
```

### 2. Get Event by Run ID + Event ID

**Endpoint:** `GET /api/atlas/v2/openlineage/runs/{runId}/events/{eventId}`

**Example:**
```bash
curl -u admin:admin \
  "http://localhost:21000/api/atlas/v2/openlineage/runs/d46e465b-d358-4d32-83d4-df660ff614dd/events/396c9c80-fe0b-11f0-b2d7-68fad00996a0"
```

**Response:**
```json
{
  "success": true,
  "runId": "d46e465b-d358-4d32-83d4-df660ff614dd",
  "eventCount": 1,
  "pageSize": 1,
  "events": [
    {
      "eventId": "396c9c80-fe0b-11f0-b2d7-68fad00996a0",
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

### 3. Health Check

**Endpoint:** `GET /api/atlas/v2/openlineage/health`

**Example:**
```bash
curl -u admin:admin \
  "http://localhost:21000/api/atlas/v2/openlineage/health"
```

**Response:**
```json
{
  "success": true,
  "message": "OpenLineage event storage is healthy"
}
```

### Response Shape

All OpenLineage REST endpoints return the same response envelope:
```json
{
  "success": true,
  "message": null,
  "error": null,
  "runId": "run-id",
  "eventCount": 2,
  "events": [ ... ],
  "pageSize": 25,
  "nextPagingState": "opaque-cursor-or-null"
}
```

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

The REST API always returns a response envelope with `success=false` and an `error` message.
Kafka ingestion errors are logged by the consumer.

## Performance Considerations

1. **Connection Pooling**: Optimized based on CPU cores
   - Local pool: `min(max(CPU * 0.75, 4), 8)`
   - Remote pool: `max(local / 2, 2)`

2. **Singleton Pattern**: Single CqlSession per JVM instance

3. **Batch Operations**: Ingestion is via Kafka; REST is read-only

4. **Consistency Level**: LOCAL_QUORUM for balance between consistency and performance

## Monitoring

The implementation includes performance metrics using `AtlasPerfMetrics`:
- `processOpenLineageEvent`: Single event processing time
- `getOpenLineageEventsByRunIdPaged`: Paginated query time
- `getOpenLineageEventByRunIdAndEventId`: Single event query time
- `getOpenLineageEventsByIdIterator`: Full-iterator query time

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
2. **Pagination**: Cursor-based pagination is supported via `pageSize` + `pagingState`
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
