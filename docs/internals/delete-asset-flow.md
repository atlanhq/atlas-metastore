# Delete Asset Flow Analysis

This document provides a comprehensive analysis of the delete asset operation in Atlas Metastore, including the current implementation, performance bottlenecks, and optimization strategies.

## Table of Contents

1. [Overview](#overview)
2. [Current Delete Flow](#current-delete-flow)
3. [Graph Traversals During Delete](#graph-traversals-during-delete)
4. [Entity Example: Table](#entity-example-table)
5. [Process Entity and HasLineage](#process-entity-and-haslineage)
6. [Optimization Strategy](#optimization-strategy)
7. [Trade-offs Analysis](#trade-offs-analysis)
8. [Implementation Recommendations](#implementation-recommendations)
9. [Key Code References](#key-code-references)

---

## Overview

The current delete operation in Atlas is slow because it synchronously traverses all edges connected to an entity, updates relationship statuses, handles classification propagation, and recalculates lineage attributes. This document analyzes the complete delete path and proposes optimizations.

### Key Files Involved

| File | Purpose |
|------|---------|
| `webapp/.../rest/EntityREST.java` | REST API endpoints |
| `repository/.../AtlasEntityStoreV2.java` | Entity store operations |
| `repository/.../DeleteHandlerV1.java` | Core delete logic |
| `repository/.../SoftDeleteHandlerV1.java` | Soft delete implementation |
| `repository/.../HardDeleteHandlerV1.java` | Hard delete implementation |

---

## Current Delete Flow

### Entry Points

```
REST API Endpoints:
├── DELETE /entity/guid/{guid}           → EntityREST.deleteByGuid()
├── DELETE /entity/bulk?guid=...         → EntityREST.deleteByGuids()
├── DELETE /entity/uniqueAttribute/...   → EntityREST.deleteByUniqueAttribute()
└── PUT    /admin/purge                  → AdminResource.purgeByIds()
```

### Complete Flow Diagram

```
EntityREST.deleteByGuid(guid)
    │
    ▼
EntityMutationService.deleteById(guid)
    │
    ▼
AtlasEntityStoreV2.deleteById(guid)  [line 595]
    ├── Find vertex by GUID
    ├── Verify authorization (ENTITY_DELETE)
    │
    ▼
AtlasEntityStoreV2.deleteVertices(candidates)  [line 2265]
    │
    ├── For each vertex:
    │   ├── updateModificationMetadata(vertex)
    │   ├── checkAndCreateProcessRelationshipsCleanupTaskNotification() [if enabled]
    │   └── preprocessor.processDelete(vertex)
    │
    ├── Separate glossary categories from other entities
    │
    ├── For categories:
    │   ├── entityGraphMapper.removeAttrForCategoryDelete()
    │   └── deleteDelegate.getHandler(HARD).deleteEntities()
    │
    └── For other entities:
        ├── deleteDelegate.getHandler().removeHasLineageOnDelete()  ◄── EXPENSIVE
        └── deleteDelegate.getHandler().deleteEntities()            ◄── EXPENSIVE
            │
            ▼
        DeleteHandlerV1.deleteEntities()  [line 139]
            │
            ├── getOwnedVertices() - Collect composite entities     ◄── TRAVERSAL 1
            │
            ├── For each deletion candidate:
            │   ├── deleteAllClassifications()                      ◄── TRAVERSAL 2
            │   ├── deleteTypeVertex()                              ◄── TRAVERSAL 3
            │   └── Queue classification refresh tasks
            │
            └── Return deleted entity headers
```

---

## Graph Traversals During Delete

### Traversal 1: Collect Owned/Composite Entities

**Location:** `DeleteHandlerV1.java:253` - `getOwnedVertices()`

**Purpose:** Finds all entities that should be deleted along with the parent (composite ownership).

**Algorithm:** Stack-based DFS traversal through owned reference attributes.

```java
// Pseudocode
Stack<AtlasVertex> vertices = new Stack<>();
vertices.push(entityVertex);

while (!vertices.isEmpty()) {
    vertex = vertices.pop();

    for (AtlasAttribute attr : entityType.getOwnedRefAttributes()) {
        // Get edges for this owned attribute
        // Push referenced vertices to stack
    }
}
```

**Impact:** O(owned_entities) - Usually small, but can be significant for entities with many owned children.

---

### Traversal 2: Delete All Classifications

**Location:** `DeleteHandlerV1.java:1357` - `deleteAllClassifications()`

**Purpose:** Removes all classifications from the entity and handles tag propagation.

```java
List<AtlasEdge> classificationEdges = getAllClassificationEdges(instanceVertex);

for (AtlasEdge edge : classificationEdges) {
    if (isClassificationEdge && removePropagations) {
        // Create CLASSIFICATION_PROPAGATION_DELETE task
        // OR call removeTagPropagation() synchronously
    }
    deleteEdgeReference(edge, CLASSIFICATION, ...);
}
```

**Impact:** O(classifications * propagated_entities) - Can be very expensive for widely propagated tags.

---

### Traversal 3: Delete Type Vertex (MOST EXPENSIVE)

**Location:** `DeleteHandlerV1.java:891` - `deleteTypeVertex()`

**Purpose:** Iterates through ALL attributes of the entity type and processes each relationship.

```java
for (AtlasAttribute attributeInfo : structType.getAllAttributes().values()) {
    switch (attrType.getTypeCategory()) {
        case OBJECT_ID_TYPE:
            deleteEdgeReference(vertex, edgeLabel, ...);
            break;

        case STRUCT:
            deleteEdgeReference(vertex, edgeLabel, ...);
            break;

        case ARRAY:
            List<AtlasEdge> edges = getActiveCollectionElementsUsingRelationship(vertex, attr);
            for (AtlasEdge edge : edges) {
                deleteEdgeReference(edge, ...);  // Updates referenced vertex!
            }
            break;

        case MAP:
            List<AtlasEdge> edges = getMapValuesUsingRelationship(vertex, attr);
            for (AtlasEdge edge : edges) {
                deleteEdgeReference(edge, ...);
            }
            break;
    }
}
```

**Impact:** O(attributes * edges_per_attribute) - This is the PRIMARY bottleneck.

---

### Traversal 4: Process Incoming/Outgoing Edges

**Location:** `DeleteHandlerV1.java:1187` - `deleteVertex()`

**Purpose:** Handles external references pointing to this entity.

```java
Iterable<AtlasEdge> incomingEdges = instanceVertex.getEdges(AtlasEdgeDirection.IN);
Iterable<AtlasEdge> outgoingEdges = instanceVertex.getEdges(AtlasEdgeDirection.OUT);

for (AtlasEdge edge : incomingEdges) {
    if (isRelationshipEdge(edge)) {
        deleteRelationship(edge);  // Updates edge status
    } else {
        deleteEdgeBetweenVertices(outVertex, inVertex, attribute);
    }
}
```

**Impact:** O(total_edges) - Updates modification timestamps on ALL referenced entities.

---

### Traversal 5: HasLineage Calculation

**Location:** `DeleteHandlerV1.java:1746` - `removeHasLineageOnDelete()`

**Purpose:** Recalculates `__hasLineage` attribute for Process and DataSet entities.

```java
for (AtlasVertex vertexToBeDeleted : vertices) {
    if (isProcess || isCatalog) {
        Iterator<AtlasEdge> edgeIterator = vertex.getEdges(BOTH, PROCESS_EDGE_LABELS);

        Set<AtlasEdge> edgesToBeDeleted = collectActiveEdges(edgeIterator);

        if (!distributedHasLineageCalculationEnabled) {
            resetHasLineageOnInputOutputDelete(edgesToBeDeleted, vertex);
        } else {
            // Queue for async processing
            RequestContext.get().getRemovedElementsMap().put(guid, edgesToBeDeleted);
        }
    }
}
```

**Impact:** O(lineage_edges * connected_assets) - Can trigger cascading updates.

---

## Entity Example: Table

A Table entity from `minimal.json` has the following relationship attributes that are traversed during delete:

### High-Impact Relationships (Array types - ALL edges traversed)

| Attribute | Type | Typical Count | Delete Impact |
|-----------|------|---------------|---------------|
| `columns` | `Array<Column>` | 10-1000+ | Each column edge processed |
| `partitions` | `Array<TablePartition>` | 0-1000+ | Each partition edge |
| `queries` | `Array<Query>` | 0-100 | Each query edge |
| `meanings` | `Array<AtlasGlossaryTerm>` | 0-50 | Each term link |
| `inputToProcesses` | `Array<Process>` | 0-100 | Lineage processes |
| `outputFromProcesses` | `Array<Process>` | 0-100 | Lineage processes |
| `mcMonitors` | `Array<MCMonitor>` | 0-20 | Monte Carlo monitors |
| `dbtModels` | `Array<DbtModel>` | 0-10 | DBT models |
| `sodaChecks` | `Array<SodaCheck>` | 0-50 | Soda checks |

### Low-Impact Relationships (Single references)

| Attribute | Type | Delete Impact |
|-----------|------|---------------|
| `atlanSchema` | `Schema` | Single edge lookup |
| `readme` | `Readme` | Single edge |
| `dataContractLatest` | `DataContract` | Single edge |

### Performance Example

A Table with:
- 500 columns
- 20 processes (input/output)
- 10 terms
- 5 partitions

**Current delete time:** 5-30+ seconds (traverses 535+ edges)

---

## Process Entity and HasLineage

### The `__hasLineage` Attribute

When a Process entity or its inputs/outputs are deleted, the `__hasLineage` derived attribute must be recalculated on connected assets.

### Current Flow

```
removeHasLineageOnDelete()
    │
    ▼
For each Process/DataSet vertex being deleted:
    │
    ├── Get all edges with labels: [__Process.inputs, __Process.outputs]
    │
    ▼
resetHasLineageOnInputOutputDelete()  [line 1803]
    │
    For each edge:
    │
    ├── Get connected asset vertex
    ├── Check if other ACTIVE lineage edges exist
    ├── If no active edges: set __hasLineage = false
    │
    └── Traverse OPPOSITE edge direction:
        └── Update __hasLineage on those assets too
```

### Async HasLineage (Already Implemented)

When `ENABLE_DISTRIBUTED_HAS_LINEAGE_CALCULATION=true`:

```java
// Instead of synchronous calculation:
if (distributedHasLineageCalculationEnabled) {
    // Collect edges for async processing
    RequestContext.get().getRemovedElementsMap().put(guid, edgesToBeDeleted);
}

// Later in deleteVertices():
Map<String, String> typeByVertexId = getRemovedInputOutputVertexTypeMap();
sendvertexIdsForHaslineageCalculation(typeByVertexId);
```

---

## Optimization Strategy

### Proposed Optimized Flow

```
deleteById(guid)
    │
    ▼
1. Update vertex status to DELETED (O(1))
2. Update modification timestamp (O(1))
3. Send vertexId to async queue for:
   ├── Relationship edge status cleanup
   ├── Classification propagation cleanup
   └── HasLineage recalculation
4. Return immediately
    │
    ▼
Total time: ~100-200ms (vs 5-30+ seconds)
```

### Existing Infrastructure to Leverage

| Component | Config Flag | Purpose |
|-----------|-------------|---------|
| `ATLAS_DISTRIBUTED_TASK_ENABLED` | Feature flag | Enable distributed tasks |
| `ENABLE_DISTRIBUTED_HAS_LINEAGE_CALCULATION` | Feature flag | Async lineage calculation |
| `ENABLE_RELATIONSHIP_CLEANUP` | Feature flag | Async relationship cleanup |
| `AtlasDistributedTaskNotificationSender` | Service | Send tasks to Kafka |
| `checkAndCreateProcessRelationshipsCleanupTaskNotification()` | Method | Creates cleanup tasks |

---

## Trade-offs Analysis

### Gains from Optimization

| Gain | Impact | Details |
|------|--------|---------|
| **Dramatically reduced latency** | HIGH | Delete becomes O(1) instead of O(edges) |
| **No graph traversal in main thread** | HIGH | All edge traversals moved to async |
| **Reduced lock contention** | MEDIUM | Shorter transaction = less contention |
| **Better scalability** | HIGH | Delete time independent of entity connectivity |
| **Consistent delete time** | MEDIUM | Table with 1000 columns same as table with 10 |
| **Reduced memory pressure** | MEDIUM | No loading of all edges into memory |
| **Batch processing opportunity** | HIGH | Async worker can batch relationship updates |

### Losses/Risks from Optimization

| Loss/Risk | Severity | Mitigation |
|-----------|----------|------------|
| **Inconsistent relationship state** | HIGH | UI should filter by entity status |
| **Delayed Kafka notifications** | MEDIUM | Acceptable for most use cases |
| **ES index inconsistency** | MEDIUM | Async worker updates ES |
| **`__hasLineage` temporarily incorrect** | MEDIUM | Already partially async |
| **Restore complexity** | HIGH | Need careful restore logic for edges |
| **Orphaned relationship edges** | LOW | Retry mechanism + monitoring |
| **Classification propagation delay** | MEDIUM | Already uses deferred actions |
| **Audit trail gaps** | LOW | Async worker generates events |
| **Complex debugging** | MEDIUM | Add task status visibility |

---

## Implementation Recommendations

### Phase 1: Quick Win - Enable Existing Flags

```properties
# Enable these in atlas-application.properties
atlas.distributed.task.enabled=true
atlas.enable.distributed.haslineage.calculation=true
atlas.enable.relationship.cleanup=true
```

### Phase 2: Expand Relationship Cleanup

Modify `deleteTypeVertex()` to skip edge traversal when async cleanup is enabled:

```java
// In DeleteHandlerV1.deleteTypeVertex()
if (ENABLE_RELATIONSHIP_CLEANUP.getBoolean() && ATLAS_DISTRIBUTED_TASK_ENABLED.getBoolean()) {
    // Skip attribute traversal - async worker will handle
    LOG.info("Skipping synchronous relationship cleanup for vertex: {}", vertexId);
    return;
}

// Existing code for synchronous cleanup...
```

### Phase 3: Full Optimization

1. Create comprehensive async cleanup task type
2. Skip `deleteTypeVertex()` attribute traversal entirely
3. Add reconciliation job for edge cases
4. Implement dead-letter queue for failed tasks

---

## Key Code References

### Delete Entry Points

| Location | Method | Line |
|----------|--------|------|
| `EntityREST.java` | `deleteByGuid()` | 464 |
| `EntityREST.java` | `deleteByGuids()` | 892 |
| `AtlasEntityStoreV2.java` | `deleteById()` | 595 |
| `AtlasEntityStoreV2.java` | `deleteVertices()` | 2265 |

### Core Delete Logic

| Location | Method | Line |
|----------|--------|------|
| `DeleteHandlerV1.java` | `deleteEntities()` | 139 |
| `DeleteHandlerV1.java` | `getOwnedVertices()` | 253 |
| `DeleteHandlerV1.java` | `deleteTypeVertex()` | 891 |
| `DeleteHandlerV1.java` | `deleteVertex()` | 1187 |
| `DeleteHandlerV1.java` | `deleteAllClassifications()` | 1357 |

### HasLineage Handling

| Location | Method | Line |
|----------|--------|------|
| `DeleteHandlerV1.java` | `removeHasLineageOnDelete()` | 1746 |
| `DeleteHandlerV1.java` | `resetHasLineageOnInputOutputDelete()` | 1803 |
| `AtlasEntityStoreV2.java` | `sendvertexIdsForHaslineageCalculation()` | 2335 |

### Soft/Hard Delete Handlers

| Location | Method | Line |
|----------|--------|------|
| `SoftDeleteHandlerV1.java` | `_deleteVertex()` | 53 |
| `SoftDeleteHandlerV1.java` | `deleteEdge()` | 72 |
| `HardDeleteHandlerV1.java` | `_deleteVertex()` | 47 |
| `HardDeleteHandlerV1.java` | `deleteEdge()` | 56 |

### Async Task Infrastructure

| Location | Method | Purpose |
|----------|--------|---------|
| `AtlasEntityStoreV2.java:1872` | `checkAndCreateProcessRelationshipsCleanupTaskNotification()` | Creates cleanup tasks |
| `AtlasEntityStoreV2.java:2323` | Distributed hasLineage block | Sends vertex IDs for async processing |
| `DeleteHandlerV1.java:1604` | `createAndQueueClassificationRefreshPropagationTask()` | Classification cleanup tasks |

---

## Configuration Flags

| Flag | Default | Purpose |
|------|---------|---------|
| `atlas.distributed.task.enabled` | false | Enable distributed task processing |
| `atlas.enable.distributed.haslineage.calculation` | false | Async lineage calculation |
| `atlas.enable.relationship.cleanup` | false | Async relationship cleanup |
| `atlas.tasks.use.enabled` | true | Enable deferred actions |

---

## Appendix: Delete Types

| Type | Behavior | Use Case |
|------|----------|----------|
| `SOFT` | Marks entity/edges as DELETED, preserves data | Default, supports restore |
| `HARD` | Physically removes vertices and edges | Permanent deletion |
| `PURGE` | Removes already soft-deleted entities | Cleanup operations |
| `DEFAULT` | Uses configured default handler | Fallback |

---

*Last Updated: January 2025*
*Author: Generated via Claude Code analysis*
