# Bulk Purge — Delete by Connection

## Overview

BulkPurgeService provides a fast-path deletion mechanism for purging all entities belonging to a connection (or matching a qualifiedName prefix). It is designed for scenarios where tens of thousands to millions of entities need to be removed from a connection — an operation that would take hours via the standard single-entity delete path.

The service deletes graph vertices in parallel using a bulk-loading JanusGraph instance, then runs post-purge repair phases to maintain correctness of external entities (lineage, tag propagation, Cassandra tag records).

**Key files:**

| File | Role |
|------|------|
| `repository/.../BulkPurgeService.java` | Core service — all purge logic |
| `webapp/.../BulkPurgeREST.java` | REST API endpoints |
| `repository/.../BulkPurgeServiceTest.java` | Unit tests (Mockito-based) |
| `dev-support/bulk-purge-test/load_test_bulk_purge.py` | E2E load test script |
| `intg/.../AtlasConfiguration.java` | Configuration knobs |

---

## REST API

Base path: `/api/meta/bulk-purge` (staging) or `/api/atlas/v2/bulk-purge` (local dev)

All endpoints require `ADMIN_PURGE` authorization.

### 1. Purge by Connection

```
POST /bulk-purge/connection
    ?connectionQualifiedName=default/snowflake/1234567890
    &deleteConnection=false
    &workerCount=0
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `connectionQualifiedName` | string | required | QualifiedName of the connection whose child assets to purge |
| `deleteConnection` | boolean | `false` | If `true`, delete the Connection entity itself after all children are purged |
| `workerCount` | int | `0` (auto) | Override worker count (0 = auto-scale based on entity count) |

**Response:**
```json
{
  "requestId": "uuid",
  "purgeKey": "default/snowflake/1234567890",
  "purgeMode": "CONNECTION",
  "deleteConnection": false,
  "message": "Bulk purge submitted successfully"
}
```

### 2. Purge by QualifiedName Prefix

```
POST /bulk-purge/qualifiedName
    ?prefix=default/snowflake/1234567890/mydb
    &workerCount=0
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `prefix` | string | required | QualifiedName prefix to match (min 10 chars for safety) |
| `workerCount` | int | `0` (auto) | Override worker count |

### 3. Check Status

```
GET /bulk-purge/status?requestId=<uuid>
```

**Response:**
```json
{
  "requestId": "uuid",
  "purgeKey": "default/snowflake/1234567890",
  "purgeMode": "CONNECTION",
  "status": "RUNNING",
  "currentPhase": "VERTEX_DELETION",
  "totalDiscovered": 50000,
  "deletedCount": 25000,
  "failedCount": 0,
  "completedBatches": 50,
  "workerCount": 4,
  "batchSize": 500,
  "lastHeartbeat": 1700000000000,
  "deleteConnection": false,
  "connectionDeleted": false
}
```

**Status values:** `PENDING` → `RUNNING` → `COMPLETED` | `CANCELLED` | `FAILED`

**Phase values (when status=RUNNING):**
`VERTEX_DELETION` → `ES_CLEANUP` → `ES_VERIFICATION` → `LINEAGE_REPAIR` → `TAG_CLEANUP_SOURCES` → `TAG_CLEANUP_EXTERNAL` → `RELAY_PROPAGATION_REFRESH` → `DELETE_CONNECTION` → `AUDIT`

### 4. Cancel Purge

```
POST /bulk-purge/cancel?requestId=<uuid>
```

Cancel is best-effort — the current batch will complete before the purge stops.

---

## Architecture

### Execution Flow

```
  API Request
      │
      ▼
  BulkPurgeREST.purgeByConnection()
      │  validates params, auth check
      ▼
  BulkPurgeService.submitPurge()
      │  checks Redis for existing purge
      │  writes PENDING status to Redis
      │  submits to coordinatorExecutor
      ▼
  executePurge() [coordinator thread]
      │
      ├─ Acquire distributed Redis lock
      ├─ Start heartbeat thread (30s interval)
      │
      ├─ Phase 1+2: streamAndDelete()
      │     ├─ ES _count to get total entities
      │     ├─ Auto-scale worker count
      │     ├─ Create bulk-loading JanusGraph (batch-loading=true)
      │     ├─ Start N worker threads
      │     ├─ ES scroll → BlockingQueue → workers consume batches
      │     └─ Workers: getVertices() → collect lineage/tags → removeVertex() → commit
      │
      ├─ Phase 3a: ES_CLEANUP
      │     └─ _delete_by_query (safety net, async for >10K entities)
      │
      ├─ Phase 3a': ES_VERIFICATION
      │     └─ _count to verify 0 remaining
      │
      ├─ Phase 3b: LINEAGE_REPAIR
      │     └─ Check external vertices, set __hasLineage=false if no active edges
      │
      ├─ Phase 3c-1: TAG_CLEANUP_SOURCES (V2 only)
      │     └─ Page through Cassandra propagated_tags_by_source, delete propagated copies
      │
      ├─ Phase 3c-2: TAG_CLEANUP_EXTERNAL
      │     └─ Check external vertices for stale propagated tags from dead sources
      │
      ├─ Phase 3c-3: RELAY_PROPAGATION_REFRESH
      │     └─ Create CLASSIFICATION_REFRESH_PROPAGATION tasks for relay scenarios
      │
      ├─ Phase 4: DELETE_CONNECTION (optional)
      │     └─ Delete the Connection vertex itself
      │
      ├─ AUDIT: Write summary EntityAuditEventV2
      │
      └─ Release lock, cleanup
```

### Key Design Decisions

**Bulk-loading graph.** A separate JanusGraph instance is created with `storage.batch-loading=true`, which disables the ConsistentKeyLocker. This eliminates per-vertex lock contention between workers. Safe because all vertices within the purge scope are being deleted — no external writes to those vertices.

**ES scroll + BlockingQueue.** The coordinator streams ES scroll results into a bounded `BlockingQueue<BatchWork>` (capacity = workerCount * 2). Workers pull batches from the queue. This provides natural backpressure — if workers are slow, the coordinator blocks on `put()`.

**Batch vertex retrieval.** Workers call `workerGraph.getVertices(ids)` (single round-trip) instead of N individual `getVertex()` calls per batch.

**Poison pill shutdown.** After the ES scroll completes (or fails), the coordinator sends `POISON_PILL` sentinel values to the queue — one per worker. Each worker exits its loop when it receives the poison pill. This guarantees clean shutdown even on ES scroll failure.

**No per-entity Kafka notifications.** Unlike the regular delete path, BulkPurge does not emit per-entity `ENTITY_DELETE` Kafka events. ES cleanup is handled directly via `_delete_by_query`. This is an intentional trade-off for performance.

**No preprocessor invocations.** Connection-scoped entity types (Table, Column, Process, etc.) do not have meaningful `processDelete` preprocessor logic, so this is safe for connection purge.

---

## Worker Auto-Scaling

Workers are auto-scaled based on the total entity count discovered from ES:

| Entity Count | Workers |
|-------------|---------|
| < 1,000 | 1 |
| 1,000 – 9,999 | 2 |
| 10,000 – 49,999 | 4 |
| 50,000 – 499,999 | min(configuredMax, 8) |
| >= 500,000 | configuredMax |

The `workerCount` query parameter overrides auto-scaling (capped at `atlas.bulk.purge.worker.count`).

---

## Post-Purge Correctness Phases

### Lineage Repair (`repairExternalLineage`)

When a Process entity in the purged connection had lineage edges to entities in other connections, those external entities may now have stale `__hasLineage=true`. For each external lineage vertex collected during deletion:
1. Check if any active `__processInputs` / `__processOutputs` edges remain
2. If none, set `__hasLineage = false`
3. Per-vertex commit with 3 retries + exponential backoff

### Tag Cleanup — Deleted Sources (`cleanPropagatedTagsFromDeletedSources`, V2 only)

When entity A (purged) had direct tag "PII" that propagated to B, C, D at any hop distance:
1. For each deleted entity with direct tags, query Cassandra's `propagated_tags_by_source` table
2. Page through ALL downstream entities that received the tag
3. Delete the propagated tag entries from Cassandra
4. Update graph properties (`__propagatedTraitNames`, `__propagatedClassificationNames`) on receiving vertices
5. Clear `__classificationsText` (rebuilt on next read/reindex)
6. Clean zombie Cassandra entries for the deleted entity itself

### Tag Cleanup — External Vertices (`repairExternalPropagatedClassifications`)

Safety net for Phase 3c-1. For each external lineage vertex:
- **V1 (graph-based tags):** Walk classification edges, find propagated edges whose source entity no longer exists, remove those edges
- **V2 (Cassandra-based tags):** Query TagDAO for all tags on vertex, find propagated tags whose source vertex no longer exists, delete from Cassandra

### Relay Propagation Refresh (`triggerRelayPropagationRefresh`)

Handles the relay scenario: `E(external) → A(purged) → B(external)`. E has direct tag "PII" that propagated through A to B. After A is deleted:
- B still has propagated tag "PII" with source=E
- `cleanPropagatedTagsFromDeletedSources` won't catch it (source=E is alive, source=A only tracks A's direct tags)
- `repairExternalPropagatedClassifications` won't catch it (source vertex E still exists)

Fix: Create `CLASSIFICATION_REFRESH_PROPAGATION` tasks for each unique (sourceGuid, tagTypeName) found on external vertices where the source is still alive. The task handler runs full BFS from source, detects the broken path, and removes stale tags.

Deduplication: Uses a `Set<String>` of `sourceGuid|tagTypeName` keys to avoid creating duplicate tasks when multiple external vertices share the same relay source.

---

## Concurrency & Crash Recovery

### Distributed Lock

Redis distributed lock (`bulk_purge_lock:<purgeKey>`) prevents concurrent purges of the same connection. If a purge is already `RUNNING` with a recent heartbeat (< 5 min), submission is rejected.

### Heartbeat

A background thread writes the current timestamp to Redis every 30 seconds. This serves two purposes:
1. Proves the purge is still alive
2. Enables orphan detection when the process crashes

### Orphan Checker

A `ScheduledExecutorService` runs every 5 minutes (configurable) and scans the active purge keys registry:
1. For each `RUNNING` purge with stale heartbeat (> 5 min old):
   - Query ES to check if entities remain
   - If 0 remaining → mark as `COMPLETED` (orphan recovery)
   - If entities remain → auto-resubmit (up to 3 attempts)
2. For `COMPLETED` / `CANCELLED` / `FAILED` purges → remove from registry

### Cancel

Sets `cancelRequested = true` on the PurgeContext. Workers check this flag before each batch and exit early. The ES scroll coordinator also checks it between pages. Cancel is best-effort — the current batch completes before stopping.

---

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `atlas.bulk.purge.batch.size` | 500 | Vertices per batch per worker |
| `atlas.bulk.purge.worker.count` | 10 | Maximum worker threads |
| `atlas.bulk.purge.es.page.size` | 5000 | ES scroll page size |
| `atlas.bulk.purge.commit.max.retries` | 3 | Graph commit retry attempts per batch |
| `atlas.bulk.purge.heartbeat.interval.ms` | 30000 | Heartbeat write interval (ms) |
| `atlas.bulk.purge.redis.ttl.seconds` | 86400 | Redis key TTL (24 hours) |
| `atlas.bulk.purge.scroll.timeout.minutes` | 30 | ES scroll context timeout |
| `atlas.bulk.purge.orphan.check.enabled` | true | Enable orphan checker background thread |
| `atlas.bulk.purge.orphan.check.interval.ms` | 300000 | Orphan checker interval (5 min) |
| `atlas.bulk.purge.orphan.max.resubmits` | 3 | Max auto-recovery attempts for crashed purges |

---

## Audit Trail

On completion, a summary `EntityAuditEventV2` is written with action `ENTITY_PURGE` containing:
- `purgeKey`, `purgeMode`, `totalDeleted`, `totalFailed`, `totalDiscovered`, `durationMs`, `requestId`, `workerCount`

This provides traceability for who deleted what and when, without the overhead of per-entity audit events.

---

## Test Cases

### Unit Tests (`BulkPurgeServiceTest.java`)

#### Validation Tests
| Test | What it verifies |
|------|-----------------|
| `testBulkPurgeByConnection_nullConnectionQN_throwsException` | Null connectionQN is rejected |
| `testBulkPurgeByConnection_emptyConnectionQN_throwsException` | Empty connectionQN is rejected |
| `testBulkPurgeByConnection_connectionNotFound_throwsException` | Non-existent connection in graph is rejected |
| `testBulkPurgeByQualifiedName_nullPrefix_throwsException` | Null prefix is rejected |
| `testBulkPurgeByQualifiedName_shortPrefix_throwsException` | Prefix < 10 chars is rejected (safety guard) |

#### Submission & Concurrency Tests
| Test | What it verifies |
|------|-----------------|
| `testBulkPurgeByConnection_validInput_returnsRequestId` | Valid input returns a UUID requestId |
| `testBulkPurgeByQualifiedName_validInput_returnsRequestId` | QN prefix mode returns a requestId |
| `testBulkPurgeByConnection_alreadyRunning_throwsException` | Concurrent purge of same connection is rejected when heartbeat is fresh |
| `testBulkPurgeByConnection_staleRunning_allowsResubmit` | Stale purge (heartbeat > 5 min old) allows re-submission |

#### Status & Cancel Tests
| Test | What it verifies |
|------|-----------------|
| `testGetStatus_notFound_returnsNull` | Unknown requestId returns null |
| `testGetStatus_fromRedis_returnsMap` | Completed status is readable from Redis |
| `testCancelPurge_noActivePurge_returnsFalse` | Cancel on unknown requestId returns false |
| `testCancelPurge_activePurge_returnsTrue` | Cancel on active purge sets cancelRequested flag |

#### Batch Processing & Vertex Deletion Tests
| Test | What it verifies |
|------|-----------------|
| `testProcessBatch_deletesVerticesViaBulkLoadingGraph` | Vertices are deleted via bulk-loading graph (not regular graph). Batch vertex retrieval (`getVertices()`) is used. No explicit `removeEdge` calls — JanusGraph handles edges internally. Bulk-loading graph is shut down after completion. |
| `testProcessBatch_nullVertex_skippedGracefully` | Vertices already deleted (null from `getVertices`) are skipped without error. Commit still succeeds. |
| `testProcessBatch_lockFailure_setsFailed` | When distributed lock acquisition fails, status is set to `FAILED` |

#### Worker Going Down / ES Failure Tests
| Test | What it verifies |
|------|-----------------|
| `testWorkerCleanup_onEsScrollFailure_workersStillShutDown` | When the ES scroll connection fails mid-stream (`IOException`), workers still receive poison pills and shut down cleanly. The bulk-loading graph is shut down in the `finally` block. No thread leaks. |

This test simulates the coordinator's ES scroll throwing an `IOException` after the `_count` succeeds. The `finally` block in `streamAndDelete()` ensures:
1. Poison pills are sent to the queue (one per worker)
2. Worker futures are awaited with a timeout
3. Worker pool is shut down
4. Bulk-loading graph is shut down

#### Lineage Repair Tests
| Test | What it verifies |
|------|-----------------|
| `testCollectExternalLineageVertices_crossConnection` | External vertices (different `connectionQualifiedName`) are collected during vertex deletion. After purge, `__hasLineage` is set to `false` on external vertices that have no remaining active lineage edges. |

#### Worker Count Auto-Scaling Tests
| Test | What it verifies |
|------|-----------------|
| `testWorkerCountAutoScaling` | 5000 entities → 2 workers (1K–10K tier) verified via Redis status |
| `testWorkerCountAutoScaling_allThresholds` | All tier boundaries tested: <1K→1, 1K→2, 10K→4, 50K→8, 500K→configuredMax |
| `testWorkerCountAutoScaling_lowConfiguredMax` | When configuredMax < tier value, it caps correctly |
| `testWorkerCountOverride_usedInsteadOfAutoScaling` | Override of 5 is used even when auto-scaling would pick 1 |

#### Orphan Checker / Crash Recovery Tests
| Test | What it verifies |
|------|-----------------|
| `testOrphanChecker_stalePurge_resubmits` | Stale RUNNING purge (heartbeat 10 min ago) with remaining entities is auto-resubmitted with `PENDING` status and a new requestId |
| `testOrphanChecker_completedPurge_removedFromRegistry` | `COMPLETED` purges are cleaned from the active keys registry |
| `testOrphanChecker_maxResubmitsExceeded_doesNotResubmit` | When `resubmitCount` reaches max (3), no further resubmission is attempted |

#### Tag Propagation Cleanup Tests (V2)
| Test | What it verifies |
|------|-----------------|
| `testPurgeContext_isTagV2_defaultsFalse` | Tag V2 flag defaults to false |
| `testPurgeContext_entitiesWithDirectTags_concurrentSafe` | `entitiesWithDirectTags` uses `ConcurrentHashMap.newKeySet()` — thread-safe, deduplicates |
| `testCleanPropagatedTagsFromDeletedSources_skippedWhenV1` | V1 mode (isTagV2=false) skips Cassandra-based cleanup entirely |
| `testCleanPropagatedTagsFromDeletedSources_skippedWhenNoDirectTags` | No entities with direct tags → cleanup skipped |
| `testProcessBatch_collectsEntitiesWithDirectTags` | When V2 enabled, vertices with `__traitNames` are collected into `entitiesWithDirectTags` for later Cassandra cleanup |
| `testProcessBatch_doesNotCollectEntitiesWithoutDirectTags` | Vertices without direct tags are still deleted but not collected |

#### Tag Propagation V1 Tests
| Test | What it verifies |
|------|-----------------|
| `testRepairPropagatedClassificationsV1_stillWorksWithExistingLogic` | V1 path uses graph classification edges (not Cassandra) for cleanup. External vertices get `__hasLineage=false` when no active lineage edges remain. |

#### Relay Propagation Tests
| Test | What it verifies |
|------|-----------------|
| `testTriggerRelayPropagationRefresh_skippedWhenNoExternalVertices` | No external vertices → no tasks created |
| `testTriggerRelayPropagationRefresh_V2_createsTasksForAliveSource` | For the relay scenario (E→A→B, A purged), a `CLASSIFICATION_REFRESH_PROPAGATION` task is created with the alive source's GUID and tag type name |
| `testTriggerRelayPropagationRefresh_V2_deduplicatesSameSource` | When two external vertices have propagated tags from the same source with the same tag type, only ONE refresh task is created (deduplication via `sourceGuid\|tagTypeName` key) |

#### Classification Text Cleanup Tests
| Test | What it verifies |
|------|-----------------|
| `testRemovePropagatedTraitFromVertex_clearsClassificationText` | When a propagated trait is removed: `__propagatedTraitNames` is updated, `__propagatedClassificationNames` is cleared (if last tag), and `__classificationsText` is cleared (rebuilt on next read) |

#### V1 Relay Source Collection Tests
| Test | What it verifies |
|------|-----------------|
| `testCollectRelaySourcesV1_findsAliveSource` | V1 path correctly identifies propagated classification edges where the source entity still exists, and collects (sourceGuid, tagTypeName) pairs for task creation |

#### ES Query Builder Tests
| Test | What it verifies |
|------|-----------------|
| `testBulkPurgeByConnection_generatesPrefixQueryWithTrailingSlash` | Connection purge appends `/` to the QN prefix so the Connection entity itself is excluded from the ES query, and prefix collisions between connections (e.g., `123` vs `1234`) are prevented |

#### PurgeContext Serialization Tests
| Test | What it verifies |
|------|-----------------|
| `testPurgeContext_toStatusMap_containsAllFields` | All expected fields present in status map |
| `testPurgeContext_toStatusMap_includesRemainingAfterCleanup` | `remainingAfterCleanup` included when >= 0 |
| `testPurgeContext_toStatusMap_includesResubmitCount` | `resubmitCount` included when > 0 |
| `testPurgeContext_toStatusMap_includesErrorWhenSet` | Error message included when set |
| `testPurgeContext_toJson_producesValidJson` | JSON output is parseable and contains expected fields |

#### BatchWork Tests
| Test | What it verifies |
|------|-----------------|
| `testBatchWork_poisonPill_isIdentifiable` | Poison pill has batchIndex=-1 and empty vertexIds |
| `testBatchWork_holdsVertexIds` | Normal BatchWork holds vertex IDs and batch index |

#### Verification Tests
| Test | What it verifies |
|------|-----------------|
| `testVerification_zeroRemaining_statusShowsSuccess` | After purge, ES count returns 0 and status includes `remainingAfterCleanup=0` |

