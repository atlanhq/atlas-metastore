# GraphDB Module Documentation

This document provides detailed guidance for working with the Atlas graphdb module.

## Module Overview

The `graphdb` module provides a **database-agnostic abstraction layer** for graph database operations in Atlas. It isolates Atlas from specific graph database implementations, currently providing a JanusGraph implementation while maintaining flexibility for future backends.

## Module Structure

```
graphdb/
├── api/                      # Core interfaces and abstractions
│   └── src/main/java/org/apache/atlas/repository/graphdb/
│       ├── AtlasGraph.java           # Main graph interface
│       ├── AtlasVertex.java          # Vertex abstraction
│       ├── AtlasEdge.java            # Edge abstraction
│       ├── AtlasGraphQuery.java      # Query interface
│       └── AtlasGraphManagement.java # Schema management
├── common/                   # Shared utilities
│   └── src/main/java/org/apache/atlas/repository/graphdb/
│       └── utils/                    # Common utilities
└── janus/                    # JanusGraph implementation
    └── src/main/java/org/apache/atlas/repository/graphdb/janus/
        ├── AtlasJanusGraph.java      # JanusGraph implementation
        ├── query/                    # Query implementations
        ├── serializer/               # Custom serializers
        └── migration/                # Migration tools
```

## Core Interfaces

### AtlasGraph
The main graph database interface:
```java
public interface AtlasGraph<V extends AtlasVertex, E extends AtlasEdge> {
    // Element creation
    AtlasVertex<V, E> addVertex();
    AtlasEdge<V, E> addEdge(AtlasVertex<V, E> v1, AtlasVertex<V, E> v2, String label);
    
    // Element retrieval
    AtlasVertex<V, E> getVertex(String vertexId);
    Iterable<AtlasVertex<V, E>> getVertices();
    
    // Querying
    AtlasGraphQuery<V, E> query();
    AtlasIndexQuery<V, E> indexQuery(String indexName, String queryString);
    
    // Transaction management
    void commit();
    void rollback();
    
    // Management
    AtlasGraphManagement getManagementSystem();
    void clear();
    void shutdown();
}
```

### AtlasVertex
Vertex abstraction:
```java
public interface AtlasVertex<V, E> extends AtlasElement {
    // Edge operations
    Iterable<AtlasEdge<V, E>> getEdges(AtlasEdgeDirection direction, String... labels);
    Iterable<AtlasEdge<V, E>> getEdges(AtlasEdgeDirection direction);
    
    // Property management
    <T> void setProperty(String propertyName, T value);
    <T> T getProperty(String propertyName, Class<T> clazz);
    void removeProperty(String propertyName);
    
    // JSON property support
    void setJsonProperty(String propertyName, Object value);
    <T> T getJsonProperty(String propertyName);
    
    // Bulk property operations
    void setPropertyFromElementsIds(String propertyName, List<AtlasElement> values);
    void setPropertyFromElementIds(String propertyName, List<String> values);
}
```

### AtlasEdge
Edge abstraction:
```java
public interface AtlasEdge<V, E> extends AtlasElement {
    AtlasVertex<V, E> getOutVertex();
    AtlasVertex<V, E> getInVertex();
    String getLabel();
}
```

### AtlasGraphQuery
Query interface with fluent API:
```java
public interface AtlasGraphQuery<V, E> {
    // Property filters
    AtlasGraphQuery<V, E> has(String propertyKey, Object value);
    AtlasGraphQuery<V, E> has(String propertyKey, QueryOperator operator, Object value);
    AtlasGraphQuery<V, E> has(String propertyKey, Predicate predicate);
    
    // Text search
    AtlasGraphQuery<V, E> has(String propertyKey, AtlasTextPredicate predicate, Object value);
    
    // Logical operators
    AtlasGraphQuery<V, E> or(List<AtlasGraphQuery<V, E>> childQueries);
    
    // Edge filters
    AtlasGraphQuery<V, E> in(String edgeLabel);
    AtlasGraphQuery<V, E> out(String edgeLabel);
    
    // Execution
    Iterable<AtlasVertex<V, E>> vertices();
    Iterable<AtlasVertex<V, E>> vertices(int limit);
    Iterable<AtlasVertex<V, E>> vertices(int offset, int limit);
    Iterable<AtlasEdge<V, E>> edges();
}
```

## JanusGraph Implementation

### Configuration
Key configuration properties:
```properties
# Storage backend
atlas.graph.storage.backend=cql
atlas.graph.storage.cassandra.keyspace=atlas_janus
atlas.graph.storage.hostname=localhost

# Index backend
atlas.graph.index.search.backend=elasticsearch
atlas.graph.index.search.hostname=localhost
atlas.graph.index.search.elasticsearch.client-only=true

# Performance tuning
atlas.graph.cache.db-cache=true
atlas.graph.cache.db-cache-size=0.5
atlas.graph.cache.tx-cache-size=15000

# ID generation
atlas.graph.sonyflake.data-center-id=1
atlas.graph.sonyflake.machine-id=1
```

### Backend Support

#### Cassandra Backend
- Default production backend
- Dynamic vertex columns for flexible schema
- CQL session management
- Supports ID-only mode for performance

#### HBase Backend
- Hadoop ecosystem integration
- Distributed storage
- Coprocessor support

#### Elasticsearch Index
- Primary index backend
- Full-text search
- Range queries
- Aggregations
- Multi-cluster support

### Performance Features

#### Bulk Loading Mode
For high-performance data ingestion:
```java
AtlasJanusGraph graph = (AtlasJanusGraph) AtlasGraphProvider.getGraphInstance();
graph.getManagementSystem().createBulkLoadManager().enableBulkLoading();
// Perform bulk operations
graph.getManagementSystem().createBulkLoadManager().disableBulkLoading();
```

#### ID-Only Mode
Stores full data in Cassandra, only IDs in JanusGraph:
```properties
atlas.enable.entity.cud.on.cassandra=true
```

#### Direct Index Queries
Bypass graph traversal for performance:
```java
AtlasIndexQuery indexQuery = graph.indexQuery("vertexIndex", "v.\"__typeName\":hive_table");
Iterator<AtlasIndexQuery.Result> results = indexQuery.vertices();
```

## Index Management

### Index Types

#### Composite Index
For exact match queries:
```java
AtlasGraphManagement mgmt = graph.getManagementSystem();
AtlasPropertyKey propertyKey = mgmt.getPropertyKey("qualifiedName");
mgmt.createCompositeIndex("qualifiedNameIndex", propertyKey, true);
```

#### Mixed Index
For full-text and range queries:
```java
mgmt.createMixedIndex("vertexIndex", AtlasVertex.class, Arrays.asList(propertyKey));
```

#### Vertex-Centric Index
For efficient edge traversals:
```java
mgmt.createVertexCentricIndex("edgeIndex", edgeLabel, direction, sortKeys);
```

### Index Query Operators
- `TEXT_CONTAINS` - Full-text search
- `TEXT_PREFIX` - Prefix matching
- `TEXT_SUFFIX` - Suffix matching
- `TEXT_REGEX` - Regular expressions
- `EQ`, `NEQ`, `LT`, `GT`, `LTE`, `GTE` - Comparison operators

## Transaction Management

### Auto-commit Mode
Default behavior - each operation commits automatically:
```java
AtlasVertex vertex = graph.addVertex();
vertex.setProperty("name", "test"); // Auto-committed
```

### Explicit Transactions
For atomic operations:
```java
try {
    AtlasVertex v1 = graph.addVertex();
    AtlasVertex v2 = graph.addVertex();
    graph.addEdge(v1, v2, "link");
    graph.commit();
} catch (Exception e) {
    graph.rollback();
    throw e;
}
```

### Enhanced Commit Process
For Cassandra backend with Elasticsearch:
1. Prepare phase - validate changes
2. Commit to Cassandra
3. Update Elasticsearch
4. Finalize transaction

## Caching Mechanisms

### SearchContextCache
Redis-backed cache for search contexts:
```java
SearchContextCache cache = new SearchContextCache();
cache.put(key, searchContext);
SearchContext context = cache.get(key);
```

### APIKeySessionCache
In-memory cache for API sessions:
```java
APIKeySessionCache sessionCache = new APIKeySessionCache();
sessionCache.put(apiKey, sessionInfo);
```

### Graph-level Caching
JanusGraph internal caching:
- Database cache - caches graph data
- Transaction cache - caches within transaction

## Migration Tools

### Graph Migration
For upgrading between versions:
```java
GraphMigrator migrator = new GraphMigrator();
migrator.migrate(sourceGraph, targetGraph);
```

### GraphSON Export/Import
For data portability:
```java
// Export
GraphSONWriter.outputGraph(graph, outputStream);

// Import
GraphSONReader.inputGraph(graph, inputStream);
```

## Best Practices

### Query Optimization
1. Use indexes for property lookups
2. Leverage direct index queries for large result sets
3. Use vertex-centric indexes for relationship traversals
4. Limit result sets with pagination

### Transaction Management
1. Use explicit transactions for multi-step operations
2. Keep transactions small and focused
3. Handle rollback scenarios properly
4. Avoid long-running transactions

### Property Management
1. Use typed properties for consistency
2. Leverage JSON properties for complex objects
3. Use bulk property operations when possible
4. Index frequently queried properties

### Performance Tuning
1. Enable bulk loading for large imports
2. Consider ID-only mode for read-heavy workloads
3. Tune cache sizes based on workload
4. Monitor and optimize index usage

## Common Development Patterns

### Creating Vertices with Properties
```java
AtlasVertex vertex = graph.addVertex();
vertex.setProperty("__typeName", "hive_table");
vertex.setProperty("qualifiedName", "db.table");
vertex.setJsonProperty("attributes", complexObject);
```

### Querying with Filters
```java
Iterable<AtlasVertex> tables = graph.query()
    .has("__typeName", "hive_table")
    .has("owner", "user1")
    .has("name", TEXT_CONTAINS, "sales")
    .vertices();
```

### Traversing Relationships
```java
AtlasVertex table = graph.getVertex(tableId);
Iterable<AtlasEdge> columns = table.getEdges(OUT, "hive_table_columns");
for (AtlasEdge edge : columns) {
    AtlasVertex column = edge.getInVertex();
    // Process column
}
```

### Direct Index Query
```java
String query = "v.\"qualifiedName\":\"db.table\" AND v.\"__state\":\"ACTIVE\"";
AtlasIndexQuery indexQuery = graph.indexQuery("vertexIndex", query);
Iterator<Result> results = indexQuery.vertices();
```

## Debugging Tips

1. Enable debug logging:
```properties
log4j.logger.org.apache.atlas.repository.graphdb=DEBUG
```

2. Monitor JanusGraph metrics
3. Check Elasticsearch query performance
4. Use graph traversal profiling
5. Monitor cache hit rates

## Testing Support

The module provides test utilities:
- Mock implementations for unit testing
- In-memory backend for integration tests
- Test data generators
- Query validation helpers