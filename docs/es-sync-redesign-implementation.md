# ES Sync Redesign: Tag Denormalization — Implementation Details

## Problem Statement

The codebase had **9 separate ES sync paths** for tag denormalized attributes, split across two patterns:

| Pattern | Paths | Fields Written | Data Source |
|---------|-------|---------------|-------------|
| **Deferred writes** (direct tag ops) | 5 paths | Only `__traitNames`, `__classificationNames`, `classificationText` | Mix of in-memory and Cassandra |
| **Direct writes** (propagation tasks) | 4 paths | Only `__propagatedTraitNames`, `__propagatedClassificationNames`, `classificationText` | Cassandra via `getAllTagsByVertexId` |

**Root problems:**

1. **Partial field writes** — Direct tag ops never touched propagated fields. Propagation ops never touched direct fields. Stale values in untouched fields persisted indefinitely (MS-655).
2. **9 different computation methods** — Each path used a different utility method (`getDirectTagAttachmentAttributesForAddTag`, `getDirectTagAttachmentAttributesForDeleteTag`, `getPropagatedAttributesForTags`, `getPropagatedAttributesForNoTags`, etc.) with different assumptions.
3. **Fragile delta logic** — Each path computed what to write based on the operation type (add/delete/update), rather than reading the current truth and writing a full snapshot.
4. **Silent ES failures** — Deferred ops caught and swallowed exceptions; API returned 200 while ES stayed stale.
5. **No recovery** — No retry, no DLQ. Failed writes were lost until manual repair.

The ONE path that worked correctly was `repairClassificationMappingsV2` — it reads ALL tags from Cassandra via `getAllClassificationsForVertex`, computes ALL 5 fields via `getAllAttributesForAllTagsForRepair`, and writes them all to ES.

## Design Principles

1. **Cassandra is the source of truth.** ES is a derived view. On any tag change, ES should be overwritten to match Cassandra's current state.

2. **No delta logic.** Instead of computing "what changed" and patching ES, we read the full current state from Cassandra and overwrite all 5 denorm fields. This eliminates the entire class of partial-write bugs.

3. **Read-after-write.** Tag mutations commit to Cassandra first. The denorm computation then reads from Cassandra (using LOCAL_QUORUM in a single DC, giving read-after-write consistency). This ensures the ES write reflects committed state.

4. **Lazy buffer + flush.** Mutation code only touches Cassandra and buffers vertex IDs. Denorm computation + ES write happens lazily in a separate flush step. Benefits:
   - If multiple tag ops happen on the same entity in one request, we compute denorm once (not N times)
   - Clean separation: mutation code = Cassandra only, flush = ES sync
   - Single computation path for all 9 previously-scattered paths

5. **Batch-oriented.** Propagation tasks process assets in chunks (~200 at a time). ES sync happens per chunk, not per asset. Prevents memory issues for large propagations (100k+ assets).

6. **Observable.** Track cassandraCount vs esCount per task. Mismatches are logged and emit metrics, enabling alerting.

7. **Self-healing.** ES write failures emit failed vertex IDs + GUIDs to a DLQ Kafka topic. A dedicated consumer re-reads Cassandra truth and repairs ES. No manual intervention needed.

## Architecture

```
Tag Mutation (all 9 paths)
  → Cassandra write (add/delete/update/propagate)
  → RequestContext.addVertexNeedingTagDenorm(vertexId, guid)

Flush Step (two call sites)
  → EntityGraphMapper.flushTagDenormToES()
    → For each buffered (vertexId, guid):
        → tagDAO.getAllClassificationsForVertex(vertexId)
        → TagDeNormAttributesUtil.getAllAttributesForAllTagsForRepair(guid, tags, ...)
    → ESConnector.writeTagPropertiesWithResult(deNormMap)
        → Parse bulk response for partial failures
    → On success: accumulate counts in RequestContext
    → On failure: emit failed vertex IDs + GUIDs to ATLAS_TAG_DENORM_DLQ
    → Clear the buffer

Call site 1: EntityMutationService.executeESPostProcessing()
  → For deferred paths (direct tag add/delete/update + repair)
  → Called in finally block after graph transaction

Call site 2: Inline per-chunk in propagation methods
  → For propagation add/delete/update/refresh tasks
  → Called at the end of each ~200-asset batch
```

## Files Modified

### Core Infrastructure

| File | Changes |
|------|---------|
| `server-api/.../RequestContext.java` | Added `verticesNeedingTagDenorm` buffer (LinkedHashMap), ES success/failure counters, accessor methods, cleanup in `clearCache()` |
| `repository/.../EntityGraphMapper.java` | Added `flushTagDenormToES()`, `bufferTagDenormForTags()`. Refactored all 9 paths. Removed dead methods. Injected `TagDenormDLQProducer`. |
| `repository/.../EntityMutationService.java` | `executeESPostProcessing()` now calls `entityGraphMapper.flushTagDenormToES()` instead of the old `ESDeferredOperation` path |
| `repository/.../TagDeNormAttributesUtil.java` | Removed 5 dead methods. Only `getAllAttributesForAllTagsForRepair` + `getClassificationTextKey` remain. |

### ES Partial Failure Handling

| File | Changes |
|------|---------|
| `repository/.../ESConnector.java` | Added `writeTagPropertiesWithResult()` that parses ES bulk response for per-doc failures. Returns `TagDenormESWriteResult` with success count + failed vertex IDs. Added `parseBulkResponse()` helper. |

### Task-Level Observability

| File | Changes |
|------|---------|
| `repository/.../ClassificationTask.java` | Extended `TaskContext` with `cassandraCount`/`esSuccessCount`/`hasSyncMismatch()`. `perform()` logs mismatch warnings + MDC context. |
| `repository/.../ClassificationPropagationTasks.java` | All 4 V2 task handlers (Add, Delete, UpdateText, RefreshPropagation) wire cassandraCount/esSuccessCount from `RequestContext`. |

### DLQ Infrastructure

| File | Changes |
|------|---------|
| `repository/.../TagDenormDLQProducer.java` | **New.** Kafka producer that emits `{ type: TAG_DENORM_SYNC, vertices: { vertexId: guid, ... } }` to `ATLAS_TAG_DENORM_DLQ` topic. Lazy initialization, best-effort (never fails the caller). |
| `webapp/.../TagDenormDLQReplayService.java` | **New.** Kafka consumer that subscribes to `ATLAS_TAG_DENORM_DLQ`. For each message: reads Cassandra truth, computes full denorm, writes to ES. Configurable via `atlas.kafka.tag.denorm.dlq.*` properties. |

### Dead Code Removed

**TagDeNormAttributesUtil.java:**
- `getDirectTagAttachmentAttributesForAddTag` — was used by add/update direct tag paths
- `getDirectTagAttachmentAttributesForDeleteTag` — was used by delete direct tag paths
- `getPropagatedAttributesForNoTags` — was used by propagation paths for empty case
- `getPropagatedAttributesForTags` — was used by propagation paths
- `updateDenormAttributesForPropagatedTags` — private helper for propagation

**EntityGraphMapper.java:**
- `updateClassificationTextV2` (overload 1: `Collection<AtlasVertex>`) — was used by propagation add
- `updateClassificationTextV2` (overload 2: `List<Tag>`) — was used by propagation delete/update/refresh

## The 9 Paths — Before and After

### Deferred Paths (flushed by EntityMutationService)

| # | Method | Before | After |
|---|--------|--------|-------|
| 5 | `repairClassificationMappingsV2` | Read tags → compute denorm → `addESDeferredOperation` | `addVertexNeedingTagDenorm(vertexId, guid)` |
| 6 | `addClassificationsV2` | Build in-memory tag list → `getDirectTagAttachmentAttributesForAddTag` → `addESDeferredOperation` | `addVertexNeedingTagDenorm(vertexId, guid)` |
| 7 | `deleteClassificationV2` | Read tags → `getDirectTagAttachmentAttributesForDeleteTag` → `addESDeferredOperation` | `addVertexNeedingTagDenorm(vertexId, guid)` |
| 8 | `addEsDeferredOperation` | Create dummy classification → `getDirectTagAttachmentAttributesForDeleteTag` → `addESDeferredOperation` | `addVertexNeedingTagDenorm(vertexId, guid)` |
| 9 | `updateClassificationsV2` | Filter/replace in-memory tag list → `getDirectTagAttachmentAttributesForAddTag` → `addESDeferredOperation` | `addVertexNeedingTagDenorm(vertexId, guid)` |

### Direct Paths (flushed per chunk in propagation tasks)

| # | Method | Before | After |
|---|--------|--------|-------|
| 1 | `processClassificationPropagationAdditionV2` | `updateClassificationTextV2` (overload 1) → `putPropagatedTags` → `writeTagProperties` | `putPropagatedTags` → buffer vertexIds → `flushTagDenormToES()` |
| 2 | `deleteClassificationPropagationV2` | `deletePropagations` → `updateClassificationTextV2` (overload 2) → `writeTagProperties` | `deletePropagations` → buffer vertexIds → `flushTagDenormToES()` |
| 3 | `updateClassificationTextPropagationV2` | `putPropagatedTags` → `updateClassificationTextV2` (overload 2) → `writeTagProperties` | `putPropagatedTags` → buffer vertexIds → `flushTagDenormToES()` |
| 4 | `processDeletions_new` | `deleteTags` → `updateClassificationTextV2` (overload 2) → `writeTagProperties` | `deleteTags` → buffer vertexIds → `flushTagDenormToES()` |

**Key change for Path 1:** `putPropagatedTags` is now called BEFORE denorm computation (previously denorm was computed from in-memory state BEFORE the Cassandra write). This ensures the Cassandra read in `flushTagDenormToES` sees the committed propagated tag.

## Design Decisions

### Why buffer in RequestContext instead of computing eagerly?

If 3 tag operations happen on the same entity in one request (add tag A, delete tag B, update tag C), eager computation would read Cassandra 3 times and compute denorm 3 times. The old PostProcessor deduplication logic would then discard 2 of the 3 results. With the buffer, the same entity appears once in the LinkedHashMap, and we compute denorm once from the final Cassandra state.

### Why LinkedHashMap for the buffer?

Preserves insertion order (useful for logging). Same vertex appearing multiple times just overwrites the GUID entry (idempotent). No duplicates in the final flush.

### Why flush per-chunk for propagation paths?

Propagation tasks can affect 100k+ assets. Collecting all vertex IDs in memory and flushing at the end would be prohibitive. Flushing per chunk (~200 assets) bounds memory usage and provides incremental progress.

### Why a separate DLQ topic (not extending ATLAS_ES_DLQ)?

The existing `ATLAS_ES_DLQ` carries JanusGraph vertex index mutations with a different message format and replay logic. Mixing tag denorm repair messages would require type-based dispatch in the existing `DLQReplayService`. A separate topic with a dedicated consumer is cleaner: independent scaling, independent monitoring, independent failure handling.

### Why emit vertexId→GUID in DLQ messages?

`getAllAttributesForAllTagsForRepair` requires the entity GUID to separate direct vs propagated tags. The GUID cannot be reliably derived from the tag list alone (propagated tags have the source entity's GUID, not the target entity's). Including the GUID in the DLQ message ensures the consumer can repair without graph lookups.

### Why parse ES bulk response for partial failures?

ES bulk API returns HTTP 200 even when some documents fail. The previous code treated 200 as success and silently dropped per-doc failures. Now we parse the `items` array in the bulk response to identify which specific documents failed, enabling targeted DLQ emission and accurate success/failure counts.

## Configuration Properties

| Property | Default | Description |
|----------|---------|-------------|
| `atlas.kafka.tag.denorm.dlq.topic` | `ATLAS_TAG_DENORM_DLQ` | Kafka topic for tag denorm DLQ messages |
| `atlas.kafka.tag.denorm.dlq.consumerGroupId` | `atlas_tag_denorm_dlq_replay_group` | Consumer group for DLQ replay |
| `atlas.kafka.tag.denorm.dlq.enabled` | `true` | Enable/disable DLQ replay service |
| `atlas.kafka.tag.denorm.dlq.maxRetries` | `3` | Max retries per DLQ message |
| `atlas.kafka.tag.denorm.dlq.pollTimeoutSeconds` | `15` | Kafka poll timeout |

## Failure Scenarios

| Scenario | Behavior |
|----------|----------|
| **ES fully down** | `writeTagPropertiesWithResult` retries with exponential backoff, then returns all-failed result. All vertex IDs emitted to DLQ. Task records mismatch. DLQ consumer retries when ES recovers. |
| **ES partial failure** | Bulk response parsed for per-doc failures. Only failed docs emitted to DLQ. Success count reflects actual successes. |
| **Cassandra read failure** | `getAllClassificationsForVertex` throws. For deferred paths: caught in `executeESPostProcessing`, logged. For propagation paths: exception propagates, task fails and can be retried. |
| **DLQ emit failure** | Best-effort: logged but does not fail the caller. The failed vertex IDs are logged in the warn message for manual investigation. |
| **Task restart (pod kill)** | Idempotent by design. Re-running reads current Cassandra truth and overwrites ES. Multiple passes re-affirm the same state. |
| **Concurrent modifications** | Two requests touching the same entity both read full Cassandra state at their time. Last ES write wins with most current data. Correct by construction. |

## Observability

### Task-level metrics
After each V2 propagation task completes, if `cassandraCount != esSuccessCount`:
```
WARN Tag denorm sync mismatch for task <guid>: cassandraCount=1000, esSuccessCount=980
```

MDC context includes: `sync_mismatch=true`, `cassandra_count=1000`, `es_success_count=980`

### ES-level metrics
`RequestContext` accumulates `tagDenormEsSuccessCount` and `tagDenormEsFailureCount` across all flushes in a request/task.

### DLQ monitoring
`TagDenormDLQReplayService.getStatus()` returns:
```json
{
  "enabled": true,
  "running": true,
  "topic": "ATLAS_TAG_DENORM_DLQ",
  "processedCount": 42,
  "errorCount": 1
}
```
