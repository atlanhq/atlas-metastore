# Tag Denorm Reconciliation: ES Sync from Cassandra Truth

## Problem

Elasticsearch stores 5 **denormalized attributes** on every asset vertex to power tag-based search and filtering:

| ES Attribute | Type | Example | Description |
|---|---|---|---|
| `__traitNames` | `List<String>` | `["PII", "Internal"]` | Direct tag type names |
| `__classificationNames` | `String` | `\|PII\|Internal\|` | Pipe-delimited direct tag names |
| `__propagatedTraitNames` | `List<String>` | `["Confidential"]` | Propagated tag type names |
| `__propagatedClassificationNames` | `String` | `\|Confidential` | Pipe-delimited propagated tag names |
| `__classificationsText` | `String` | `PII Internal Confidential` | Full-text search field (all tags) |

These attributes must stay in sync with the tags stored in Cassandra. The **old incremental approach** computed deltas (add/remove one tag at a time) and patched individual attributes. This was fragile and had a critical bug.

### The Bug (Old Incremental Logic)

`TagDeNormAttributesUtil.updateDenormAttributesForPropagatedTags()` had a fallback else-branch:

```java
if (CollectionUtils.isNotEmpty(propTraits)) {
    // normal path: set propagated attributes from the final list
    deNormAttrs.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, propTraits);
    ...
} else {
    // BUG: fallback re-introduces the DELETED tag when no other propagated tags remain
    deNormAttrs.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY,
        Collections.singletonList(propagatedTag.getTypeName()));  // ← the tag we just deleted!
    ...
}
```

**Scenario:** A column has 1 direct tag (`PII`) + 1 propagated tag (`Sensitive`). A task deletes the propagated tag from Cassandra. After deletion, `finalPropagatedTags` is empty, so the code falls into the else-branch and writes `["Sensitive"]` back into ES — the exact tag that was just deleted.

**Result:** ES permanently shows a ghost propagated tag that no longer exists in Cassandra.

## Solution: Reconciliation from Cassandra Truth

Instead of computing deltas, we **read the full current state from Cassandra and recompute all 5 attributes from scratch**. No delta logic means no opportunity for bugs.

```
Cassandra (tags_by_id)          Reconciliation             Elasticsearch
┌─────────────────────┐                                ┌──────────────────────┐
│ vertex=B             │    read all tags    compute    │ vertex=B             │
│  PII (direct)        │ ───────────────> ──────────> │  __traitNames=[PII]  │
│  Confidential (prop) │    for vertex B    all 5      │  __propTraitNames=   │
│                      │                    attrs      │   [Confidential]     │
└─────────────────────┘                                └──────────────────────┘
```

### Why This Works

1. **Single source of truth**: Cassandra's `tags_by_id` table stores both direct and propagated tags for every vertex. Each row has an `is_propagated` boolean column that definitively marks a tag as direct or propagated.

2. **No delta bugs**: Since we recompute from the full state every time, there are no edge cases around empty lists, fallback branches, or stale references to deleted tags.

3. **All 5 attributes always set**: The reconciliation method always populates all 5 attributes, even when a list is empty. This prevents partial updates that could leave ES in an inconsistent state.

## Cassandra Data Model (Relevant Tables)

### `tags_by_id` (effective tags)

```sql
CREATE TABLE tags.tags_by_id (
    bucket          int,
    id              text,        -- the asset vertex ID
    is_propagated   boolean,     -- direct (false) or propagated (true)
    source_id       text,        -- source vertex for propagated; self for direct
    tag_type_name   text,
    tag_meta_json   text,        -- full AtlasClassification JSON
    asset_metadata  text,
    is_deleted      boolean,     -- soft delete flag
    updated_at      timestamp,
    PRIMARY KEY ((bucket, id), is_propagated, source_id, tag_type_name)
);
```

- **Purpose**: All tags (direct + propagated) for a given vertex.
- **Used by reconciliation**: `getAllTagsByVertexIds()` reads from this table.
- **Deletes**: Soft delete (`is_deleted = true`). Non-deleted rows are the truth.

### `propagated_tags_by_source` (reverse lookup)

```sql
CREATE TABLE tags.propagated_tags_by_source (
    source_id           text,
    tag_type_name       text,
    propagated_asset_id text,
    asset_metadata      text,
    updated_at          timestamp,
    PRIMARY KEY ((source_id, tag_type_name), propagated_asset_id)
);
```

- **Purpose**: Reverse lookup — given a source vertex and tag type, find all targets it was propagated to. Used during BFS propagation and delete tasks.
- **Not used by reconciliation** (reconciliation reads by target vertex, not by source).
- **Deletes**: Hard delete (row removed entirely).

Both tables are written atomically in a `LOGGED` batch at `LOCAL_QUORUM` consistency.

## Code Flow

### 1. `EntityGraphMapper.reconcileTagDenormToES(vertexIds)`

Entry point. Called after every Cassandra write (add, delete, update text).

```
reconcileTagDenormToES(vertexIds)
│
├─ tagDAO.getAllTagsByVertexIds(vertexIds)      // batch read from Cassandra
│   └─ fires async queries in parallel          // one query per vertex, all concurrent
│       └─ filters out is_deleted=true rows
│
├─ for each vertex:
│   └─ TagDeNormAttributesUtil.reconcileDenormAttributes(tags, typeRegistry, fullTextMapper)
│       ├─ Tag.isPropagated() → segregate into direct vs propagated lists
│       ├─ compute __traitNames, __classificationNames (direct)
│       ├─ compute __propagatedTraitNames, __propagatedClassificationNames (propagated)
│       └─ compute __classificationsText (all tags combined)
│
└─ ESConnector.writeTagProperties(deNormMap)    // bulk write to ES
```

### 2. Call Sites (4 operations replaced)

| Operation | Location | Pattern |
|---|---|---|
| **Add propagation** | `processClassificationPropagationAdditionV2` | Write to Cassandra first, then reconcile |
| **Delete propagation** | `deleteClassificationPropagationV2` | Delete from Cassandra first, then reconcile |
| **Update classification text** | `updateClassificationTextPropagationV2` | Update Cassandra first, then reconcile |
| **Bulk delete propagations** | `processDeletions_new` | Delete from Cassandra first, then reconcile |

Every call site follows the same pattern:
```
1. Perform Cassandra write (add/delete/update)
2. reconcileTagDenormToES(affectedVertexIds)
```

## Batch Optimization

The old code made N sequential Cassandra point reads (one per vertex). The reconciliation uses `CqlSession.executeAsync()` to fire all reads in parallel:

```java
// Fire all queries concurrently
Map<String, CompletionStage<AsyncResultSet>> futures = new LinkedHashMap<>();
for (String vertexId : vertexIds) {
    BoundStatement bound = findAllTagDetailsForAssetStmt.bind(bucket, vertexId);
    futures.put(vertexId, cassSession.executeAsync(bound));
}

// Collect results
for (var entry : futures.entrySet()) {
    AsyncResultSet asyncRs = entry.getValue().toCompletableFuture().join();
    List<Tag> tags = asyncResultSetToTags(vertexId, asyncRs);
    result.put(vertexId, tags);
}
```

For a batch of 200 vertices, this turns 200 sequential round-trips into 200 concurrent queries that resolve in ~1 round-trip time.

## Key Design Decision: `Tag.isPropagated()` vs `entityGuid` Comparison

The old code used `entityGuid` comparison to determine direct vs propagated:
```java
if (sourceAssetGuid.equals(tag.getEntityGuid())) {
    traitNames.add(tag.getTypeName());       // direct
} else {
    propagatedTraitNames.add(tag.getTypeName()); // propagated
}
```

This was fragile because `entityGuid` might not be set during deserialization of `tag_meta_json`.

The reconciliation uses `Tag.isPropagated()`, which reads the `is_propagated` boolean column stored directly in Cassandra's `tags_by_id` table. This is always set correctly at write time and doesn't depend on JSON deserialization.

## Test Coverage

`TagDeNormAttributesUtilReconcileTest` covers:

| Test | What it verifies |
|---|---|
| `nullTagList_allAttributesEmpty` | Null input produces all 5 empty defaults |
| `emptyTagList_allAttributesEmpty` | Empty list produces all 5 empty defaults |
| `singleDirectTag_onlyDirectAttributesPopulated` | Single direct tag: direct attrs set, propagated empty |
| `singlePropagatedTag_onlyPropagatedAttributesPopulated` | Single propagated tag: propagated attrs set, direct empty |
| `directTagRemainsAfterPropagatedDeleted_propagatedAttributesMustBeEmpty` | **THE BUG SCENARIO**: after deleting the only propagated tag, propagated attrs must be empty |
| `mixedDirectAndPropagated_bothListsPopulatedCorrectly` | Mixed tags: both lists correctly populated |
| `multiplePropagatedFromDifferentSources_allPresent` | Multiple propagated from different sources all appear |
| `classificationNamesKey_usesCorrectDelimitedFormat` | `__classificationNames` uses `\|Tag1\|Tag2\|` format |
| `propagatedClassificationNamesKey_usesCorrectDelimitedFormat` | `__propagatedClassificationNames` uses `\|Tag1\|Tag2` format |
