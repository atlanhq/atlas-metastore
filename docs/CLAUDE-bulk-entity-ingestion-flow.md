# Bulk Entity Ingestion Flow Documentation

This document provides a detailed end-to-end flow of bulk entity ingestion in Apache Atlas, from HTTP endpoint to database storage.

## Overview

The bulk entity ingestion flow allows creating or updating multiple entities in a single atomic operation. This flow is optimized for performance while maintaining data consistency, authorization, and audit trails.

## Flow Diagram

```
HTTP Request → EntityREST → EntityMutationService → AtlasEntityStoreV2
     ↓                                                      ↓
Validation                                          Pre-processing
     ↓                                                      ↓
Auth Check                                         Entity Discovery
     ↓                                                      ↓
                                                  Diff Calculation
                                                          ↓
                                                  Pre-processors
                                                          ↓
                                                 EntityGraphMapper
                                                          ↓
                                                   Graph Storage
                                                          ↓
                                                    Audit Trail
                                                          ↓
                                                  Notifications
                                                          ↓
                                                  ES Indexing
                                                          ↓
                                                    Response
```

## Detailed Flow Steps

### 1. HTTP Endpoint Entry

**Location**: `webapp/src/main/java/org/apache/atlas/web/rest/EntityREST.java:811`

```java
@POST
@Path("/bulk")
@Consumes(Servlets.JSON_MEDIA_TYPE)
@Produces(Servlets.JSON_MEDIA_TYPE)
public EntityMutationResponse createOrUpdate(
    AtlasEntityStream entityStream,
    @QueryParam("replaceClassifications") @DefaultValue("false") boolean replaceClassifications,
    @QueryParam("replaceTags") @DefaultValue("false") boolean replaceTags,
    @QueryParam("replaceBusinessAttributes") @DefaultValue("false") boolean replaceBusinessAttributes,
    @QueryParam("overwriteBusinessAttributes") @DefaultValue("false") boolean overwriteBusinessAttributes,
    @QueryParam("appendTags") @DefaultValue("false") boolean appendTags,
    @QueryParam("skipProcessEdgeRestoration") @DefaultValue("false") boolean skipProcessEdgeRestoration) 
```

**Key Validations**:
- Entity count limit: Configured via `atlas.entities.allowed.in.bulk` (default: 1000)
- Attribute length validation: 100K chars default, 2M for allowlisted attributes
- Request timeout handling

### 2. Request Context Setup

**Location**: `EntityMutationService.createOrUpdate()`

```java
// Disable cache for bulk operations
RequestContext.get().setEnableCache(false);

// Create bulk request context
BulkRequestContext bulkRequestContext = new BulkRequestContext(
    entityStream,
    replaceTags || replaceClassifications,
    appendTags,
    skipProcessEdgeRestoration,
    replaceBusinessAttributes,
    overwriteBusinessAttributes
);
```

### 3. Entity Store Processing

**Location**: `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/AtlasEntityStoreV2.java:1604`

#### 3.1 Pre-processing Phase

```java
private EntityMutationResponse createOrUpdate(EntityStream entityStream, ...) {
    // Phase 1: Discovery
    EntityMutationContext context = preCreateOrUpdate(entityStream, ...);
    
    // Phase 2: Authorization
    for (AtlasEntity entity : context.getCreatedEntities()) {
        AtlasAuthorizationUtils.verifyAccess(CREATE_ENTITY, entity);
    }
    
    // Phase 3: Diff Calculation
    for (AtlasEntity entity : context.getUpdatedEntities()) {
        if (!AtlasEntityComparator.hasChanges(existingEntity, entity)) {
            context.addToSkippedEntities(entity);
            continue;
        }
    }
    
    // Phase 4: Processing
    EntityMutationResponse response = entityGraphMapper.mapAttributesAndClassifications(context);
    
    return response;
}
```

### 4. Entity Discovery

**Location**: `AtlasEntityGraphDiscoveryV2.discover()`

```java
public EntityMutationContext discover() throws AtlasBaseException {
    // Resolve entity references
    resolveReferences();
    
    // Discover entities
    for (AtlasEntity entity : entities) {
        AtlasVertex vertex = discoveryContext.getResolvedEntityVertex(entity.getGuid());
        
        if (vertex == null) {
            // New entity - create vertex
            vertex = entityGraphMapper.createVertex(entity);
            discoveryContext.addCreated(entity, vertex);
        } else {
            // Existing entity - prepare for update
            AtlasEntity existingEntity = entityRetriever.toAtlasEntity(vertex);
            discoveryContext.addUpdated(entity, existingEntity, vertex);
        }
    }
    
    return context;
}
```

### 5. Pre-processor Chain Execution

**Location**: `PreProcessorUtils.executePreProcessor()`

Pre-processors validate and enrich entities before storage:

```java
public static void executePreProcessor(List<AtlasEntity> entities, EntityOperationType operationType) {
    for (AtlasEntity entity : entities) {
        EntityPreProcessor preProcessor = getPreProcessor(entity.getTypeName());
        if (preProcessor != null) {
            switch (operationType) {
                case CREATE:
                    preProcessor.processCreate(entity);
                    break;
                case UPDATE:
                    preProcessor.processUpdate(entity);
                    break;
            }
        }
    }
}
```

**Common Pre-processors**:
- `ContractPreProcessor` - Validates data contracts
- `QueryFolderPreProcessor` - Processes query folders
- `GlossaryPreProcessor` - Handles glossary entities
- `AssetPreProcessor` - Generic asset processing
- `DataProductPreProcessor` - Data mesh support

### 6. Diff Calculation

**Location**: `AtlasEntityComparator.hasChanges()`

```java
public static boolean hasChanges(AtlasEntity existingEntity, AtlasEntity newEntity) {
    // Check attribute changes
    if (hasAttributeChanges(existingEntity, newEntity)) return true;
    
    // Check classification changes
    if (hasClassificationChanges(existingEntity, newEntity)) return true;
    
    // Check business attribute changes
    if (hasBusinessAttributeChanges(existingEntity, newEntity)) return true;
    
    // Check custom attribute changes
    if (hasCustomAttributeChanges(existingEntity, newEntity)) return true;
    
    return false;
}
```

### 7. Entity Graph Mapping

**Location**: `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/EntityGraphMapper.java`

#### 7.1 Main Processing Method

```java
public EntityMutationResponse mapAttributesAndClassifications(EntityMutationContext context) {
    MetricRecorder metric = RequestContext.get().startMetricRecord("mapAttributesAndClassifications");
    
    try {
        // Process created entities
        for (AtlasEntity entity : context.getCreatedEntities()) {
            AtlasVertex vertex = context.getVertex(entity.getGuid());
            mapAttributes(entity, vertex, CREATE);
            mapClassifications(entity, vertex);
            mapCustomAttributes(entity, vertex);
            mapBusinessAttributes(entity, vertex);
        }
        
        // Process updated entities
        for (AtlasEntity entity : context.getUpdatedEntities()) {
            AtlasVertex vertex = context.getVertex(entity.getGuid());
            mapAttributes(entity, vertex, UPDATE);
            updateClassifications(entity, vertex);
            updateCustomAttributes(entity, vertex);
            updateBusinessAttributes(entity, vertex);
        }
        
        // Handle deferred actions
        processDeferredActions(context);
        
        return createResponse(context);
    } finally {
        RequestContext.get().endMetricRecord(metric);
    }
}
```

#### 7.2 Attribute Mapping

```java
private void mapAttributes(AtlasEntity entity, AtlasVertex vertex, EntityOperation operation) {
    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());
    
    for (AtlasAttribute attribute : entityType.getAllAttributes().values()) {
        Object attrValue = entity.getAttribute(attribute.getName());
        
        if (attribute.getAttributeType() instanceof AtlasArrayType) {
            mapArrayAttribute(vertex, attribute, attrValue);
        } else if (attribute.getAttributeType() instanceof AtlasMapType) {
            mapMapAttribute(vertex, attribute, attrValue);
        } else if (attribute.getAttributeType() instanceof AtlasEntityType) {
            mapObjectIdAttribute(vertex, attribute, attrValue);
        } else {
            mapPrimitiveAttribute(vertex, attribute, attrValue);
        }
    }
}
```

#### 7.3 Relationship Mapping

```java
private AtlasEdge mapObjectIdAttribute(AtlasVertex vertex, AtlasAttribute attribute, AtlasObjectId objectId) {
    AtlasVertex endVertex = getVertexForGUID(objectId.getGuid());
    AtlasEdge edge = null;
    
    if (endVertex != null) {
        String edgeLabel = attribute.getRelationshipEdgeLabel();
        edge = graphHelper.getOrCreateEdge(vertex, endVertex, edgeLabel);
        
        // Set edge properties
        AtlasRelationshipType relationType = getRelationshipType(edgeLabel);
        if (relationType != null) {
            edge.setProperty(Constants.RELATIONSHIP_GUID_PROPERTY_KEY, UUID.randomUUID().toString());
            edge.setProperty(Constants.VERSION_PROPERTY_KEY, relationType.getTypeVersion());
        }
    }
    
    return edge;
}
```

### 8. Graph Storage Operations

**Location**: Uses JanusGraph implementation

```java
// Vertex creation (from EntityGraphMapper)
public AtlasVertex createVertex(AtlasEntity entity) {
    AtlasVertex vertex = graph.addVertex();
    
    // Set system properties
    vertex.setProperty(Constants.GUID_PROPERTY_KEY, entity.getGuid());
    vertex.setProperty(Constants.TYPE_NAME_PROPERTY_KEY, entity.getTypeName());
    vertex.setProperty(Constants.STATE_PROPERTY_KEY, entity.getStatus().name());
    vertex.setProperty(Constants.CREATED_BY_KEY, RequestContext.get().getUser());
    vertex.setProperty(Constants.CREATE_TIME_PROPERTY_KEY, System.currentTimeMillis());
    
    return vertex;
}

// Property updates
vertex.setProperty(propertyKey, value);
vertex.setJsonProperty(propertyKey, complexObject);
vertex.setListProperty(propertyKey, listValue);
```

### 9. Audit Trail Creation

**Location**: `audit/EntityAuditListenerV2.java`

```java
@Override
public void onEntitiesMutated(EntityMutationResponse response) {
    List<EntityAuditEventV2> auditEvents = new ArrayList<>();
    
    // Audit created entities
    for (AtlasEntityHeader entity : response.getCreatedEntities()) {
        EntityAuditEventV2 event = createAuditEvent(entity, ENTITY_CREATE);
        auditEvents.add(event);
    }
    
    // Audit updated entities (differential)
    for (AtlasEntityHeader entity : response.getUpdatedEntities()) {
        if (isDifferentialAuditEnabled) {
            // Only audit changed attributes
            Map<String, Object> changes = getChangedAttributes(entity);
            if (!changes.isEmpty()) {
                EntityAuditEventV2 event = createAuditEvent(entity, ENTITY_UPDATE, changes);
                auditEvents.add(event);
            }
        } else {
            // Full entity audit
            EntityAuditEventV2 event = createAuditEvent(entity, ENTITY_UPDATE);
            auditEvents.add(event);
        }
    }
    
    // Write to audit repository
    auditRepository.putEventsV2(auditEvents);
}
```

### 10. Notification Generation

**Location**: `notification/AtlasEntityChangeNotifier.java`

```java
public void onEntitiesMutated(EntityMutationResponse response) {
    // Generate entity notifications
    for (AtlasEntityHeader entity : response.getCreatedEntities()) {
        EntityNotificationV2 notification = new EntityNotificationV2(entity, ENTITY_CREATE);
        notificationInterface.send(notification);
    }
    
    // Full-text reindexing for search
    if (isFullTextSearchEnabled) {
        List<AtlasEntityHeader> allMutatedEntities = getAllMutatedEntities(response);
        fullTextMapperV2.updateFullTextMapping(allMutatedEntities);
    }
}
```

### 11. Transaction Management

**Location**: Uses `@GraphTransaction` annotation

```java
@GraphTransaction
public EntityMutationResponse createOrUpdate(EntityStream entityStream, ...) {
    try {
        // All operations within transaction
        EntityMutationResponse response = processEntities(entityStream);
        
        // Commit happens automatically on method return
        return response;
    } catch (Exception e) {
        // Rollback happens automatically on exception
        throw new AtlasBaseException(e);
    }
}
```

### 12. Performance Optimizations

#### 12.1 Batching
```java
// Batch audit events
public class FixedBufferList<T> {
    private final int maxSize;
    private final List<T> buffer;
    
    public void add(T item) {
        buffer.add(item);
        if (buffer.size() >= maxSize) {
            flush();
        }
    }
}
```

#### 12.2 Skip Unchanged Entities
```java
if (!AtlasEntityComparator.hasChanges(existingEntity, entity)) {
    context.addToSkippedEntities(entity);
    metricsRegistry.counter("entities.skipped").inc();
    continue;
}
```

#### 12.3 Deferred Operations
```java
// Classification propagation deferred
context.addToClassificationPropagation(vertex, classifications);

// Process after main transaction
processDeferredActions(context);
```

#### 12.4 Caching
```java
// Vertex cache in RequestContext
RequestContext requestContext = RequestContext.get();
requestContext.cacheEntity(guid, entity);
requestContext.cacheVertex(guid, vertex);
```

### 13. Elasticsearch Operations

**Location**: Post-transaction processing

```java
// After successful graph commit
public void handlePostTransaction(EntityMutationResponse response) {
    if (esEnabled) {
        // Update ES documents
        for (AtlasEntityHeader entity : response.getMutatedEntities()) {
            Document doc = createESDocument(entity);
            esClient.index(indexName, entity.getGuid(), doc);
        }
    }
}
```

### 14. Response Building

```java
private EntityMutationResponse createResponse(EntityMutationContext context) {
    EntityMutationResponse response = new EntityMutationResponse();
    
    // Add entity headers by operation type
    response.setCreatedEntities(getEntityHeaders(context.getCreatedEntities()));
    response.setUpdatedEntities(getEntityHeaders(context.getUpdatedEntities()));
    response.setDeletedEntities(getEntityHeaders(context.getDeletedEntities()));
    
    // Add metrics
    response.setGuidAssignments(context.getGuidAssignments());
    
    return response;
}
```

## Performance Considerations

### Configuration Parameters

```properties
# Bulk operation limits
atlas.entities.allowed.in.bulk=1000
atlas.attribute.value.max.length=100000

# Performance tuning
atlas.entity.diff.calculation.enabled=true
atlas.entity.audit.differential=true
atlas.graph.cache.tx-cache-size=15000

# Thread pool for parallel processing
atlas.entity.preprocessor.thread.count=10
```

### Metrics and Monitoring

Key metrics tracked:
- `entities.created.count` - Number of entities created
- `entities.updated.count` - Number of entities updated
- `entities.skipped.count` - Number of unchanged entities skipped
- `bulk.operation.duration` - Total operation time
- `graph.commit.duration` - Graph transaction time
- `es.indexing.duration` - Elasticsearch indexing time

## Error Handling

### Validation Errors
- Type validation failures
- Attribute constraint violations
- Missing required attributes

### Authorization Errors
- Insufficient permissions for CREATE/UPDATE
- Type-level access restrictions

### Storage Errors
- Graph database failures
- Elasticsearch indexing failures
- Transaction conflicts

### Recovery Mechanisms
- Automatic transaction rollback
- Retry logic for transient failures
- Error details in response

## Best Practices

1. **Batch Size**: Keep bulk operations under 500 entities for optimal performance
2. **Attribute Size**: Minimize large attribute values
3. **Relationship Loading**: Use minimal entity retrieval when possible
4. **Classification Usage**: Avoid excessive classifications per entity
5. **Monitoring**: Track metrics for performance optimization
6. **Error Handling**: Implement proper retry logic for client applications

## Summary

The bulk entity ingestion flow in Atlas is a sophisticated pipeline that:
- Validates and authorizes operations
- Optimizes performance through batching and caching
- Maintains data consistency through transactions
- Provides comprehensive audit trails
- Generates notifications for downstream systems
- Supports both synchronous and asynchronous operations

This flow ensures efficient processing of large-scale metadata ingestion while maintaining the integrity and governance features that Atlas provides.