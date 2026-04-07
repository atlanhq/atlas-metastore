# ES Sync Redesign: Tag Denormalization — Implementation Details

## Scope

**Phase 1 (PR #6481):** Migrate the **4 propagation paths** to the new buffer+flush pattern.
Direct attachment paths (5 paths) remain unchanged and use the existing `ESDeferredOperation` pattern.
They are marked with `TODO: Migrate to buffer+flush pattern` for Phase 2 once the propagation approach is validated in production.

**Follow-up PRs:**
- **PR #6496:** `TagDenormDLQReplayService` — Kafka consumer for DLQ replay using `repairClassificationMappingsV2`
- **Task ES observability:** `esStatus` / `esErrorMessage` fields on AtlasTask + Prometheus metrics with failure labels

## Problem Statement

The codebase had **9 separate ES sync paths** for tag denormalized attributes, split across two patterns:

| Pattern | Paths | Fields Written | Data Source |
|---------|-------|---------------|-------------|
| **Deferred writes** (direct tag ops) | 5 paths | Only `__traitNames`, `__classificationNames`, `classificationText` | Mix of in-memory and Cassandra |
| **Direct writes** (propagation tasks) | 4 paths | Only `__propagatedTraitNames`, `__propagatedClassificationNames`, `classificationText` | Cassandra via `getAllTagsByVertexId` |

**Root problems:**

1. **Partial field writes** — Direct tag ops never touched propagated fields. Propagation ops never touched direct fields. Stale values in untouched fields persisted indefinitely (MS-655).
2. **9 different computation methods** — Each path used a different utility method with different assumptions.
3. **Fragile delta logic** — Each path computed what to write based on the operation type, rather than reading the current truth and writing a full snapshot.
4. **Silent ES failures** — On master, `ESConnector.writeTagProperties()` throws directly on failure. The exception skips the notification call below it, so entity audits are lost along with the ES write. No DLQ, no retry, no recovery.
5. **No recovery** — On master, a failed ES write leaves Cassandra correct but ES stale and audit missing, with no mechanism to detect or repair this.

The ONE path that worked correctly was `repairClassificationMappingsV2` — it reads ALL tags from Cassandra, computes ALL 5 fields, and writes them all to ES.

## Design Principles

1. **Cassandra is the source of truth.** ES is a derived view. On any tag change, ES should be overwritten to match Cassandra's current state.

2. **No delta logic.** Instead of computing "what changed" and patching ES, we read the full current state from Cassandra and overwrite all 5 denorm fields. This eliminates the entire class of partial-write bugs.

3. **Read-after-write.** Tag mutations commit to Cassandra first. The denorm computation then reads from Cassandra (using LOCAL_QUORUM in a single DC, giving read-after-write consistency). This ensures the ES write reflects committed state.

4. **Lazy buffer + flush.** Mutation code only touches Cassandra and buffers vertex IDs. Denorm computation + ES write happens lazily in a separate flush step.

5. **Batch-oriented.** Propagation tasks process assets in chunks (~200 at a time). ES sync happens per chunk. Prevents memory issues for large propagations (100k+ assets).

6. **ES failures don't block audits.** `safeFlushTagDenormToES` catches all exceptions. The notification/audit call always fires after it. This is a key improvement over master where ES failure skipped audits.

7. **Self-healing via DLQ.** ES write failures emit failed vertex IDs + GUIDs to `ATLAS_TAG_DENORM_DLQ` Kafka topic. `TagDenormDLQReplayService` consumes these and repairs ES via `repairClassificationMappingsV2`.

8. **Observable.** Task-level `esStatus`/`esErrorMessage` fields track ES sync outcome. Prometheus metrics with labels (reason, dlq_status, error_type) enable debugging.

## Sequence Diagram — New Propagation Flow (Add)

```
┌──────────┐  ┌──────────────┐  ┌───────────┐  ┌────────────┐  ┌────┐  ┌─────┐  ┌──────────┐
│TaskConsumer│  │ClassifTask   │  │EntityGraph │  │  TagDAO    │  │ ES │  │ DLQ │  │Notifier  │
│           │  │  (Add)       │  │  Mapper    │  │ (Cassandra)│  │    │  │     │  │ (Audit)  │
└─────┬─────┘  └──────┬───────┘  └─────┬──────┘  └─────┬──────┘  └──┬─┘  └──┬──┘  └────┬─────┘
      │               │                │               │            │       │          │
      │ inProgress()  │                │               │            │       │          │
      │──────────────>│                │               │            │       │          │
      │  [commit IN_PROGRESS to graph] │               │            │       │          │
      │               │                │               │            │       │          │
      │               │  perform()     │               │            │       │          │
      │               │───────────────>│               │            │       │          │
      │               │                │               │            │       │          │
      │               │   ┌────────────┴─ FOR EACH CHUNK (200 vertices) ──────────────┐│
      │               │   │            │               │            │       │          ││
      │               │   │  1. putPropagatedTags()    │            │       │          ││
      │               │   │            │──────────────>│            │       │          ││
      │               │   │            │   Cassandra   │            │       │          ││
      │               │   │            │   write ✅    │            │       │          ││
      │               │   │            │<──────────────│            │       │          ││
      │               │   │            │               │            │       │          ││
      │               │   │  2. addVertexNeedingTagDenorm (buffer)  │       │          ││
      │               │   │            │               │            │       │          ││
      │               │   │  3. safeFlushTagDenormToES()            │       │          ││
      │               │   │            │               │            │       │          ││
      │               │   │            │  getAllTagsByVertexIds()    │       │          ││
      │               │   │            │──────────────>│            │       │          ││
      │               │   │            │  [50 async    │            │       │          ││
      │               │   │            │   reads/batch]│            │       │          ││
      │               │   │            │<──────────────│            │       │          ││
      │               │   │            │               │            │       │          ││
      │               │   │            │  computeAllDenormAttributes()      │          ││
      │               │   │            │               │            │       │          ││
      │               │   │            │  writeTagPropertiesWithResult()    │          ││
      │               │   │            │───────────────────────────>│       │          ││
      │               │   │            │               │            │       │          ││
      │               │   │            │   ┌───── ON ES FAILURE ───────────────────────┤│
      │               │   │            │   │           │            │       │          ││
      │               │   │            │   │  emitFailedVertices()  │       │          ││
      │               │   │            │   │───────────────────────────────>│          ││
      │               │   │            │   │  updateTaskEsStatus(PARTIAL_FAILURE)      ││
      │               │   │            │   │  [exception CAUGHT — not thrown]          ││
      │               │   │            │   └───────────────────────────────────────────┤│
      │               │   │            │               │            │       │          ││
      │               │   │  4. onClassificationPropagationAdded() ← ALWAYS FIRES     ││
      │               │   │            │───────────────────────────────────────────────>││
      │               │   │            │               │            │       │  audit ✅ ││
      │               │   │            │               │            │       │          ││
      │               │   └────────────┴─ END CHUNK LOOP ─────────────────────────────┘│
      │               │                │               │            │       │          │
      │               │  return count  │               │            │       │          │
      │               │<───────────────│               │            │       │          │
      │               │                │               │            │       │          │
      │ complete()    │                │               │            │       │          │
      │<──────────────│                │               │            │       │          │
      │  [persist esStatus, esErrorMessage, COMPLETE to graph]      │       │          │
      │               │                │               │            │       │          │
```

## Cassandra Async Read Resilience

**Problem found in production (workiva, vc-elastic):** `getAllTagsByVertexIds` used `cassSession.executeAsync()` + `.join()` without retry. A single transient `DriverTimeoutException` (2s) would fail the entire batch.

Every other Cassandra read in the codebase uses `executeWithRetry` (3 retries + exponential backoff). Our async path was the only one without retry.

**Fix (`TagDAOCassandraImpl.getAllTagsByVertexIds`):**

```
Phase 1 — Fast async path: Fire 50 concurrent reads per sub-batch (ASYNC_READ_BATCH_SIZE)
Phase 2 — Sync retry fallback: Any async timeout → retry via getAllTagsByVertexId() (uses executeWithRetry: 3 retries, 100ms/200ms/400ms backoff)
Phase 3 — Graceful skip: If sync retry also fails → vertex omitted from result map → caller DLQs only that vertex
```

**Before:** 1 transient timeout → entire batch of 10,000 vertices fails → all go to DLQ
**After:** 1 transient timeout → retry with backoff → if still fails → only 1 vertex to DLQ, 9,999 succeed

## Task Lifecycle & Retry Behavior

### Task State Machine

```
PENDING → IN_PROGRESS → COMPLETE (success)
                      → FAILED   (exception in Cassandra write or notification)
```

### Key Facts

- **`MAX_ATTEMPT_COUNT = 3`** — defined in `AtlasTask.java` and `TaskExecutor.java`
- **FAILED tasks are NOT automatically retried.** The re-queue query (`TaskRegistry.getTasksForReQueueGraphQuery/IndexSearch`) only matches `PENDING` and `IN_PROGRESS` tasks. FAILED tasks stay FAILED permanently.
- **`MAX_ATTEMPT_COUNT` is a crash-recovery safety net.** If a pod crashes during execution, the task stays `IN_PROGRESS` in the graph → re-queued on restart → re-executed. If it keeps crashing, `attemptCount` increments each time. After 3 crash-recovery cycles, permanently FAILED.
- **Manual retry:** `PUT /api/atlas/task/retry/{guid}` resets a FAILED task to PENDING.
- **`updateStatusFromAttemptCount()`** exists in `AtlasTask.java` but has **zero callers** — dead code.

### What Can Fail a Task

| What fails | Task status | Why |
|-----------|-------------|-----|
| Cassandra WRITE (`putPropagatedTags`) | FAILED | Exception propagates through to AbstractTask.run() catch |
| ES write (any) | COMPLETE | `safeFlushTagDenormToES` catches exception, task continues |
| DLQ emit | COMPLETE | DLQ producer is best-effort, never throws |
| Notification (`entityChangeNotifier`) | FAILED | Exception re-thrown in propagation methods |
| Maintenance mode | IN_PROGRESS (preserved) | Special handling in TaskConsumer, no attemptCount increment |

### ES Failures Don't Fail the Task — By Design

On master, `ESConnector.writeTagProperties()` throws directly → exception propagates → task FAILED. But FAILED tasks are never re-queued. So on master, an ES failure meant:
- Cassandra has the tags ✅
- ES is stale ❌
- Audit is missing ❌ (notification skipped by exception)
- **No recovery mechanism**

In our new code, `safeFlushTagDenormToES` catches exceptions:
- Cassandra has the tags ✅
- ES failure → DLQ → async repair ✅
- Audit is created ✅ (notification fires after safe flush)
- Task completes successfully ✅

## Failure Scenarios — Complete Analysis

### Scenario 1: Cassandra write fails

```
tagDAO.putPropagatedTags() throws
  → exception propagates → AbstractTask.run() catches
    → task.setStatus(FAILED), task.incrementAttemptCount()
    → TaskConsumer persists FAILED to graph
```

| Aspect | State |
|--------|-------|
| Task status | FAILED (permanently — not auto-retried) |
| Cassandra | Nothing written |
| ES | Not attempted |
| Audit | Not created |
| Recovery | Manual retry via `PUT /task/retry/{guid}` |

### Scenario 2: ES write fails (partial or total)

```
tagDAO.putPropagatedTags() ✅
safeFlushTagDenormToES():
  flushTagDenormToES() → ES write fails
  → DLQ emit ✅
  → updateTaskEsStatus(PARTIAL_FAILURE/FAILED, "Events added to DLQ: ...")
  → exception CAUGHT, not rethrown
entityChangeNotifier.onClassificationPropagation*() ✅ audit created
→ Task completes: status=COMPLETE, esStatus=PARTIAL_FAILURE/FAILED
```

| Aspect | State |
|--------|-------|
| Task status | COMPLETE |
| esStatus | PARTIAL_FAILURE or FAILED |
| Cassandra | Tags written correctly |
| ES | Stale (temporarily) |
| Audit | Created ✅ |
| DLQ | Failed vertices emitted |
| Recovery | TagDenormDLQReplayService repairs ES automatically |

### Scenario 3: ES write fails AND DLQ emit also fails

```
flushTagDenormToES() → ES write throws
  → tagDenormDLQProducer.emitFailedVertices() also throws
    → inner catch: LOG.error("Failed to emit to DLQ as well. Vertices needing repair: {vertexIds}")
  → updateTaskEsStatus(FAILED, "DLQ emit also failed: ...")
```

| Aspect | State |
|--------|-------|
| Task status | COMPLETE |
| esStatus | FAILED |
| esErrorMessage | "DLQ emit also failed: ..." with total failed count |
| Cassandra | Tags written correctly |
| ES | Stale |
| DLQ | Message lost |
| Recovery | **Manual** — vertex IDs logged in error message, use `repairClassificationMappings` API |

### Scenario 4: Pod restart after partial progress

Example: 4 chunks, chunk 1 succeeds, pod crashes before chunk 2.

| Aspect | State |
|--------|-------|
| Task status in graph | IN_PROGRESS (committed before execution) |
| Cassandra | Chunk 1 tags written |
| ES | Chunk 1 may be written |
| On restart | TaskQueueWatcher finds IN_PROGRESS task → re-executes from beginning |
| Re-execution | All chunks replay. Cassandra writes idempotent (upsert). ES writes idempotent (full snapshot). Safe. |
| Side effect | Duplicate notifications for chunk 1 vertices |

### Scenario 5: Cassandra async read fails for some vertices

```
flushTagDenormToES():
  tagDAO.getAllTagsByVertexIds(200 vertices):
    Phase 1 (async): 198 succeed, 2 timeout
    Phase 2 (sync retry): 1 succeeds, 1 still fails
  → Result map: 199 vertices. 1 missing.
  → 1 vertex → DLQ
  → 199 vertices → compute denorm → write to ES ✅
```

| Aspect | State |
|--------|-------|
| Task status | COMPLETE |
| esStatus | PARTIAL_FAILURE |
| Cassandra | All 200 tags correct |
| ES | 199 updated, 1 stale |
| Recovery | DLQ replay service repairs the 1 vertex |

### Scenario 6: Maintenance mode enabled mid-task

```
checkMaintenanceModeOrInterrupt() throws MAINTENANCE_MODE_ENABLED
  → TaskConsumer catches specially (not a failure)
  → task stays IN_PROGRESS, attemptCount NOT incremented
```

| Aspect | State |
|--------|-------|
| Task status | IN_PROGRESS (preserved) |
| On MM disable | TaskQueueWatcher re-queues → task re-executes from beginning |

## Audit Behavior — Old vs New

| Scenario | Master (Old) | New Code |
|----------|-------------|----------|
| ES write succeeds | Audit created ✅ | Audit created ✅ |
| ES write fails | Audit **skipped** ❌ (exception propagates past notification) | Audit created ✅ (`safeFlush` catches, notification fires) |
| Cassandra write fails | Audit skipped ❌ | Audit skipped ❌ |

On master, the order was: Cassandra write → ES write (throws) → notification (skipped).
In new code: Cassandra write → `safeFlushTagDenormToES` (catches) → notification (always fires).

## Task ES Status Fields

Two new fields on the `AtlasTask` vertex track ES sync outcome independently from task status:

### `esStatus` (enum: `AtlasTask.EsStatus`)

```
NOT_ATTEMPTED(0) → COMPLETE(1) → PARTIAL_FAILURE(2) → FAILED(3)
```

Ordinal-based escalation: status only gets worse across chunks, never better. A `PARTIAL_FAILURE` from chunk 1 is not overwritten by `COMPLETE` from chunk 2.

### `esErrorMessage`

Human-readable message with cumulative failed vertex count:
```
"Events added to DLQ: ES write failed for 5 vertices (total failed vertices: 12)"
"DLQ emit also failed: DriverTimeoutException (total failed vertices: 200)"
```

### Graph Properties

| Property | Key | Type |
|----------|-----|------|
| ES status | `__task_esStatus` | String (enum name) |
| ES error message | `__task_esErrorMessage` | String |

## Prometheus Metrics

### Existing counters (vertex counts)

| Metric | Description |
|--------|-------------|
| `atlas_metastore_tag_denorm_es_flush_success_total` | Vertices successfully written to ES |
| `atlas_metastore_tag_denorm_es_flush_failure_total` | Vertices that failed ES write |

### New labeled counter (debugging)

`atlas_metastore_tag_denorm_es_flush_failure_detail_total` with labels:

| Label | Values | Purpose |
|-------|--------|---------|
| `reason` | `cassandra_read_failed`, `es_write_partial_failure`, `total_failure` | What went wrong |
| `dlq_status` | `emitted`, `lost` | Whether DLQ message was sent |
| `error_type` | Exception class name (only on `total_failure`) | e.g., `DriverTimeoutException` |

### Example PromQL

```promql
# Total failures by reason
sum by (reason) (rate(atlas_metastore_tag_denorm_es_flush_failure_detail_total[5m]))

# Vertices where DLQ emit also failed (lost — need manual repair)
atlas_metastore_tag_denorm_es_flush_failure_detail_total{dlq_status="lost"}

# Failures by error type
sum by (error_type) (rate(atlas_metastore_tag_denorm_es_flush_failure_detail_total{reason="total_failure"}[5m]))
```

### DLQ metrics (producer side)

| Metric | Description |
|--------|-------------|
| `atlas_metastore_tag_denorm_dlq_producer_send_success_total` | Successful DLQ Kafka publishes |
| `atlas_metastore_tag_denorm_dlq_producer_send_failure_total` | Failed DLQ Kafka publishes |

## DLQ Infrastructure

### Producer: `TagDenormDLQProducer`

Emits failed vertex IDs + GUIDs to `ATLAS_TAG_DENORM_DLQ` topic.

**Message format:**
```json
{
  "type": "TAG_DENORM_SYNC",
  "timestamp": 1712400000000,
  "vertices": {
    "8894791808": "guid-abc-123",
    "8894791809": "guid-def-456"
  }
}
```

Best-effort: catches all exceptions internally, never fails the caller. `acks=all` for durability.

### Consumer: `TagDenormDLQReplayService` (PR #6496)

Kafka consumer modeled on the mature `DLQReplayService` (ATLAS_ES_DLQ consumer). Delegates repair to existing `AtlasEntityStore.repairClassificationMappingsV2(guids)`.

**Patterns from DLQReplayService:**
- Pause/resume for long processing
- Seek-back on failure
- Retry tracking with timestamps per partition-offset
- Exponential backoff for transient errors (AtlasBaseException)
- Poison pill handling (skip after maxRetries=3)
- ConsumerRebalanceListener cleanup
- Health check + status endpoint at `GET /api/atlas/dlq/tag-denorm-replay/status`

## Retry Budget

Per vertex across the full retry chain:

```
1 async read attempt (Phase 1)
+ 3 sync retries with backoff (Phase 2: 100ms, 200ms, 400ms)
= 4 Cassandra read attempts per task execution

× 3 task executions (crash recovery via MAX_ATTEMPT_COUNT)
= up to 12 Cassandra reads before permanent failure
```

If all 12 fail → vertex goes to DLQ → `TagDenormDLQReplayService` retries with its own retry budget (3 Kafka retries + exponential backoff).

## The 9 Paths — Before and After

### Propagation Paths — CHANGED (Phase 1)

| # | Method | Before (Master) | After |
|---|--------|-----------------|-------|
| 1 | `processClassificationPropagationAdditionV2` | `updateClassificationTextV2` → `putPropagatedTags` → `writeTagProperties` | `putPropagatedTags` → buffer → `safeFlushTagDenormToES` |
| 2 | `deleteClassificationPropagationV2` | `deletePropagations` → `updateClassificationTextV2` → `writeTagProperties` | `deletePropagations` → `bufferAndFlushTagDenormToES` |
| 3 | `updateClassificationTextPropagationV2` | `putPropagatedTags` → `updateClassificationTextV2` → `writeTagProperties` | `putPropagatedTags` → `bufferAndFlushTagDenormToES` |
| 4 | `processDeletions_new` | `deleteTags` → `updateClassificationTextV2` → `writeTagProperties` | `deleteTags` → `bufferAndFlushTagDenormToES` |

**Key ordering change for Path 1:** `putPropagatedTags` now runs BEFORE denorm computation (was after on master). Ensures the Cassandra read in `flushTagDenormToES` sees the committed tag.

### Direct Attachment Paths — UNCHANGED (Phase 2)

| # | Method | Current (unchanged) |
|---|--------|---------------------|
| 5 | `repairClassificationMappingsV2` | Read tags → `getAllAttributesForAllTagsForRepair` → `addESDeferredOperation` |
| 6 | `addClassificationsV2` | Build in-memory tag list → `getDirectTagAttachmentAttributesForAddTag` → `addESDeferredOperation` |
| 7 | `deleteClassificationV2` | Read tags → `getDirectTagAttachmentAttributesForDeleteTag` → `addESDeferredOperation` |
| 8 | `addEsDeferredOperation` | Create dummy classification → `getDirectTagAttachmentAttributesForDeleteTag` → `addESDeferredOperation` |
| 9 | `updateClassificationsV2` | Filter/replace in-memory tag list → `getDirectTagAttachmentAttributesForAddTag` → `addESDeferredOperation` |

## Files Modified

### Core Infrastructure

| File | Changes |
|------|---------|
| `server-api/.../RequestContext.java` | Added `verticesNeedingTagDenorm` buffer (LinkedHashMap), ES success/failure counters, accessor methods, cleanup in `clearCache()` |
| `repository/.../EntityGraphMapper.java` | Added `flushTagDenormToES()`, `safeFlushTagDenormToES()`, `bufferTagDenormForTags()`, `bufferAndFlushTagDenormToES()`, `updateTaskEsStatus()`, `shouldEscalateEsStatus()`, `emitEsFlushFailureMetric()`. Refactored 4 propagation paths. Injected `TagDenormDLQProducer`. |
| `repository/.../EntityMutationService.java` | `executeESPostProcessing()` now runs both: deferred ops (direct paths) + `flushTagDenormToES()` (propagation safety net, usually no-op) |
| `repository/.../TagDeNormAttributesUtil.java` | Added `computeAllDenormAttributes` (from Tag objects with normalization). Direct-path methods retained for Phase 2. |

### Task ES Observability

| File | Changes |
|------|---------|
| `intg/.../model/tasks/AtlasTask.java` | Added `EsStatus` enum (NOT_ATTEMPTED, COMPLETE, PARTIAL_FAILURE, FAILED), `esStatus`/`esErrorMessage` fields, getters/setters |
| `common/.../repository/Constants.java` | Added `TASK_ES_STATUS`, `TASK_ES_ERROR_MESSAGE` property keys |
| `repository/.../tasks/TaskRegistry.java` | Persist `esStatus`/`esErrorMessage` in `updateStatus()`, read in `toAtlasTask()` |
| `repository/.../tasks/AtlasTaskService.java` | Write `esStatus`/`esErrorMessage` in `createTaskVertex()` |
| `repository/.../tasks/ClassificationTask.java` | Extended `TaskContext` with `cassandraCount`/`esSuccessCount`/`hasSyncMismatch()`. `perform()` logs mismatch warnings. |
| `repository/.../tasks/ClassificationPropagationTasks.java` | All 4 V2 task handlers wire cassandraCount/esSuccessCount from `RequestContext`. |

### ES Partial Failure Handling

| File | Changes |
|------|---------|
| `repository/.../ESConnector.java` | Added `writeTagPropertiesWithResult()` — parses ES bulk response for per-doc failures. Returns `TagDenormESWriteResult`. |

### DLQ Infrastructure

| File | Changes |
|------|---------|
| `repository/.../TagDenormDLQProducer.java` | **New.** Kafka producer for `ATLAS_TAG_DENORM_DLQ`. Best-effort, never fails caller. |
| `webapp/.../TagDenormDLQReplayService.java` | **New (PR #6496).** Kafka consumer using `repairClassificationMappingsV2`. DLQReplayService-grade patterns. |
| `webapp/.../DLQAdminController.java` | Added `GET /dlq/tag-denorm-replay/status` endpoint. |

### Cassandra Read Resilience

| File | Changes |
|------|---------|
| `repository/.../tags/TagDAOCassandraImpl.java` | `getAllTagsByVertexIds()` — Phase 1 async + Phase 2 sync retry via `executeWithRetry` + Phase 3 graceful skip. |

## Configuration Properties

| Property | Default | Description |
|----------|---------|-------------|
| `atlas.kafka.tag.denorm.dlq.topic` | `ATLAS_TAG_DENORM_DLQ` | Kafka topic for DLQ messages |
| `atlas.kafka.tag.denorm.dlq.enabled` | `true` | Enable/disable DLQ replay consumer |
| `atlas.kafka.tag.denorm.dlq.consumerGroupId` | `atlas_tag_denorm_dlq_replay_group` | Consumer group for replay |
| `atlas.kafka.tag.denorm.dlq.maxRetries` | `3` | Max retries before poison pill skip |
| `atlas.kafka.tag.denorm.dlq.exponentialBackoff.baseDelayMs` | `1000` | Exponential backoff base delay |

## Design Decisions

### Why `safeFlushTagDenormToES` catches all exceptions

On master, `ESConnector.writeTagProperties()` throws → exception propagates → notification/audit skipped → task FAILED. But FAILED tasks are never auto-retried (`TaskQueueWatcher` only queries PENDING/IN_PROGRESS). So an ES failure on master means: Cassandra correct, ES stale, audit missing, no recovery.

By catching the exception, we ensure: (1) audit always fires, (2) task completes with correct Cassandra state, (3) DLQ handles ES repair asynchronously.

### Why ordinal-based ES status escalation

`flushTagDenormToES` is called once per chunk. A task with 50 chunks may see: chunk 1 COMPLETE, chunk 2 PARTIAL_FAILURE, chunks 3-50 COMPLETE. Without escalation, the final status would be COMPLETE (last chunk wins), hiding the failure. With `shouldEscalateEsStatus()`, the final status correctly reflects the worst outcome: PARTIAL_FAILURE.

### Why buffer in RequestContext instead of computing eagerly

If 3 tag operations happen on the same entity in one request, eager computation would read Cassandra 3 times. With the buffer, same entity appears once in the LinkedHashMap, denorm computed once from final Cassandra state.

### Why a separate DLQ topic (not extending ATLAS_ES_DLQ)

Different message format, different replay logic, different consumer. Independent scaling and monitoring.

### Why batch async Cassandra reads with sync retry fallback

The old approach made N sequential point reads. New approach fires 50 concurrent async reads per sub-batch, with sync retry (`executeWithRetry`) as fallback for any that fail. Best of both worlds: performance + resilience.
