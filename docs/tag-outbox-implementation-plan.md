# Tag Outbox — Implementation Plan

> **Scope:** Extend MS-1010 asset-sync outbox pattern to cover tag-denorm ES write failures on JanusGraph + Tags V2.
> **Relation to PR #6568:** Additive. Zero modification to PR #6568 files. Imports only stable interfaces/data classes from PR #6568.
> **Backend:** JanusGraph + Tags V2 only (zero-graph out of scope).

---

## 1. Context

### The problem being solved

`EntityGraphMapper.flushTagDenormToES()` performs inline partial-updates to ES for the 5 denorm fields (`__classificationText`, `__classificationNames`, `__traitNames`, `__propagatedClassificationNames`, `__propagatedTraitNames`) during tag propagation. On ES failure today, failed vertex IDs are emitted to a Kafka DLQ topic (`ATLAS_TAG_DENORM_DLQ`) consumed by `TagDenormDLQReplayService`.

This PR replaces the Kafka DLQ with a Cassandra-backed outbox, following the MS-1010 (PR #6568) pattern but as a **fully separate subsystem** in its own keyspace, with its own processor, reconciler, and lease. Shared concerns (lease management, metrics, pod identity) are extracted to a new **neutral shared package** so the tag code has zero coupling to any concrete asset-sync class.

### Why separate subsystems (not reuse PR #6568's table)

Reusing `asset_sync_outbox` would introduce:
- **PK collisions** — the same entity GUID could be enqueued both as a verify-miss and as a tag failure, with one write silently overwriting the other (last-write-wins on `((status), entity_guid)`).
- **Coupling of concerns** — tag-specific logic would have to live in PR #6568 files, violating modularity and complicating reviews.
- **Harder independent evolution** — retry behavior, reconciliation semantics, and observability surfaces would be joined for two genuinely different failure modes.

---

## 2. Architecture Decisions

### 2.1 Data layer — separate keyspace, separate tables

- **Keyspace:** new keyspace `atlas_tag_outbox` (distinct from PR #6568's `atlas_asset_sync`).
- **Primary table:** `tag_outbox` — PK `((status), entity_guid)`, column-identical to `asset_sync_outbox`.
- **Lease table:** `tag_outbox_lease` — mirrors `asset_sync_lease`.
- Every table/keyspace name is **configurable** via `TagOutboxConfig` record — no literals in prepared statements. If someone renames the table or moves the keyspace later, they change config only.

### 2.2 Processing layer — Option C (full separation)

- Own `TagOutboxProcessor` instance + own `TagOutboxReconciler` instance
- Own scheduler threads: `tag-outbox-relay` and `tag-outbox-reconciler` daemon threads
- Own lease `tag-outbox-relay` (distinct from `asset-sync-relay`); reconciler shares it via `isHeldByMe` (mirrors PR #6568's internal structure)
- **4 total outbox-related daemon threads per pod** (2 for asset-sync, 2 for tag). Each thread idles 30s between polls when no work — negligible resource cost.

**Alternatives considered and rejected:**

| Option | Why rejected |
|---|---|
| A — shared processor with internal strategy dispatch | Forces modifying `AssetSyncOutboxProcessor` to be multi-outbox-aware. Re-introduces coupling inside PR #6568 files. |
| B — separate classes, shared scheduler | Requires changing `AssetSyncOutboxProcessor` constructor to accept external scheduler. Less invasive than A but still touches PR #6568. |

### 2.3 Consumer replay action — `repairClassificationMappingsV2` (NOT `restoreByIds`)

The `TagOutboxConsumer` replays failed entries by calling the **denorm-only repair path**, NOT a full vertex reindex. Mirrors exactly what `TagDenormDLQReplayService.replayTagDenormDLQEntry` does today:

```java
for (chunk in guidList.batchesOf(REPAIR_BATCH_SIZE)) {
    try {
        Map<String, String> errors = entityStore.repairClassificationMappingsV2(chunk);
        // Classify GUIDs by presence in errors map
        List<ESDeferredOperation> deferredOps = RequestContext.get().getESDeferredOperations();
        if (!deferredOps.isEmpty()) {
            postProcessor.executeESOperations(deferredOps);
        }
    } finally {
        RequestContext.get().getESDeferredOperations().clear();
    }
}
```

**Rationale:** tag-denorm failures only need the 5 denorm fields refreshed in ES. A full `RepairIndex.restoreByIds` call would reindex the entire ES document via mixed-index rebuild — wasteful and potentially overwrites non-denorm fields. The denorm-only path:

1. `entityStore.repairClassificationMappingsV2(batch)` → reads current tags from Cassandra, computes 5 denorm fields, stages `ESDeferredOperation(TAG_DENORM_FOR_ADD_CLASSIFICATIONS, ...)`.
2. `postProcessor.executeESOperations(deferredOps)` → flushes the deferred op via `ESConnector.writeTagProperties` (partial-update of 5 denorm fields).
3. `RequestContext.get().getESDeferredOperations().clear()` in `finally` — prevents leakage between batches.

### 2.4 Reconciler for tag rows — always replay, no ES presence short-circuit

Unlike `AssetSyncReconciler`, which drops FAILED rows when the doc is present in ES, `TagOutboxReconciler` **always** calls `repairClassificationMappingsV2` for FAILED / stuck-PENDING rows. A doc's presence in ES says nothing about whether its denorm fields are current, so the presence check would cause false-drops for tag rows.

### 2.5 Shared neutral package — `org.apache.atlas.repository.outbox.shared`

To eliminate the ~475 LOC of structural duplication that would result from cloning `AssetSyncPodId`, `AssetSyncLeaseManager`, and `AssetSyncOutboxMetrics`, we introduce a **new neutral package** with three well-designed, parameterized, reusable classes:

| New shared class | Purpose | Replaces (for tag use) |
|---|---|---|
| `OutboxPodId` | Centralized pod-identity resolver (HOSTNAME env + PID fallback). Neutral name. | `AssetSyncPodId` |
| `ConfigurableLeaseManager` | Implements `LeaseManager`. Takes `(CqlSession, keyspace, leaseTableName, podId)` in constructor. LWT-based acquire/heartbeat/release logic is identical to `AssetSyncLeaseManager` but table name is a parameter. | `AssetSyncLeaseManager` |
| `OutboxMetrics` | Instance-based Micrometer metrics class. Takes `(MeterRegistry, String prefix)` in constructor. Registers 30+ counters/timers/gauges under the given prefix. | `AssetSyncOutboxMetrics` (static) |

**Key properties:**
- Asset-sync classes (PR #6568) are **not modified** — they continue using their hardcoded counterparts.
- Tag code uses the shared classes directly — zero clones, zero duplication on these concerns.
- Future asset-sync refactor CAN migrate to these shared classes (asset-sync's team decides when). Nothing forces it.
- Tag effectively leads by example: a well-designed template asset-sync can adopt later.

### 2.6 Tag code is independent of asset-sync refactor

Tag code's dependencies on `repository.assetsync` are limited to **7 stable interfaces/data classes** (see §4.3). No concrete class imports. Any asset-sync refactor that renames concrete classes (AssetSyncOutboxProcessor etc.) leaves tag code untouched. If asset-sync ever relocates the generic interfaces to a neutral package, tag updates only the import paths (trivial 1-line fixes).

### 2.7 Zero touch to PR #6568 concrete files

The following are **not modified** by this PR:
- `AssetSyncOutbox.java`, `AssetSyncSchema.java`, `AssetSyncSink.java`
- `AssetSyncOutboxProcessor.java`, `AssetSyncReconciler.java`
- `AssetSyncOutboxService.java`, `AssetSyncReindexConsumer.java`
- `AssetSyncLeaseManager.java`, `AssetSyncPodId.java`, `AssetSyncOutboxMetrics.java`
- `PostCommitEsVerifier.java`, `EntityGuidRef.java`

The following existing Atlas files are modified:
- `EntityGraphMapper.java` — three call sites inside `flushTagDenormToES` (the propagation failure path) switch from `tagDenormDLQProducer.emitFailedVertices(...)` to `TagOutboxSink.enqueue(toGuids(...))`. `tagDenormDLQProducer` bean stays wired (unused) for rollback.
- `AtlasConfiguration.java` — ~15 new config entries under `atlas.tag.outbox.*`.

### 2.8 Functional invariants — tag mutation methods unchanged

This PR is strictly **additive on the failure path only**. The business logic of all seven tag mutation methods — 4 propagation + 3 direct attachment — is untouched. Only the sink that receives ES-sync failures is swapped.

**Methods with zero behavioral change:**

Propagation paths (all 4 continue to route through `flushTagDenormToES`):
- `EntityGraphMapper.processClassificationPropagationAdditionV2`
- `EntityGraphMapper.deleteClassificationPropagationV2`
- `EntityGraphMapper.updateClassificationTextPropagationV2`
- `EntityGraphMapper.classificationRefreshPropagationV2_new`

Direct tag attachment paths (all 3 continue to stage `ESDeferredOperation` consumed by `EntityMutationService.executeESPostProcessing`):
- `EntityGraphMapper.addClassificationsV2`
- `EntityGraphMapper.deleteClassificationV2`
- `EntityGraphMapper.updateClassificationsV2`

Same CHUNK_SIZE, same Cassandra write ordering, same `ESDeferredOperation.OperationType` staging, same per-batch flush semantics, same error logging, same task-status updates. Diff for these seven methods: **zero lines**.

### 2.9 Two failure surfaces, one outbox

Failures in tag ES sync surface at two distinct points in the existing code. Both get rerouted to the same `TagOutboxSink`:

**Surface 1 — propagation failures** (inside `flushTagDenormToES`):
- Today: `tagDenormDLQProducer.emitFailedVertices(failedVertexIds, snapshotMap)` → Kafka `ATLAS_TAG_DENORM_DLQ` topic → `TagDenormDLQReplayService` consumer.
- After this PR: `TagOutboxSink.enqueue(toGuids(failedVertexIds, snapshotMap))` → outbox PENDING partition → relay replays via `repairClassificationMappingsV2`.
- 3 call sites changed in-file (Cassandra-read failure, ES partial failure, total exception). DLQ producer stays wired for 1-commit revert.

**Surface 2 — direct attachment failures** (inside `EntityMutationService.executeESPostProcessing`):
- This PR introduces a new **tag-scoped registry** `TagESWriteFailureRegistry` in `org.apache.atlas.repository.tagoutbox`, structurally parallel to PR #6568's `ESWriteFailureRegistry` but fully independent (separate class, separate sink slot, separate `TagESWriteFailure` payload type).
- `EntityMutationService.executeESPostProcessing`'s catch block is redirected — it now records to `TagESWriteFailureRegistry` instead of the shared `ESWriteFailureRegistry`. Similarly, `EntityGraphMapper.safeFlushTagDenormToES` is redirected.
- `TagOutboxService.init()` installs `TagOutboxFailureSink` on `TagESWriteFailureRegistry`.
- **Why a separate registry:** prevents any possibility of cross-routing between the tag outbox and the asset-sync outbox. The shared `ESWriteFailureRegistry` stays untouched and remains available for asset-sync's own future use. Two independent failure pipes, two independent sinks — mechanically impossible for a tag failure to land in `asset_sync_outbox`, or vice versa.

**Net effect for direct attachment paths:** failures that were recorded on a no-op sink today are now durably captured in the tag outbox and eventually replayed. Pure addition of recovery capability on a dedicated failure pipe.

---

## 3. Schema

`TagOutboxSchema.bootstrap(session, config)` runs idempotently on every pod startup.

```sql
CREATE KEYSPACE IF NOT EXISTS atlas_tag_outbox
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': <rf>}
    AND durable_writes = true;

CREATE TABLE IF NOT EXISTS atlas_tag_outbox.tag_outbox (
    status            text,
    entity_guid       text,
    attempt_count     int,
    created_at        timestamp,
    last_attempted_at timestamp,
    next_attempt_at   timestamp,
    claimed_by        text,
    claimed_until     timestamp,
    PRIMARY KEY ((status), entity_guid)
) WITH gc_grace_seconds = 3600
  AND default_time_to_live = 604800;  -- 7 days

CREATE TABLE IF NOT EXISTS atlas_tag_outbox.tag_outbox_lease (
    job_name      text PRIMARY KEY,
    owner         text,
    acquired_at   timestamp,
    heartbeat_at  timestamp
);
```

All identifiers (keyspace, table names) come from `TagOutboxConfig` — no literals inside prepared statements.

---

## 4. Components

### 4.1 New shared package: `org.apache.atlas.repository.outbox.shared`

Path: `repository/src/main/java/org/apache/atlas/repository/outbox/shared/`

| File | LOC | Role |
|---|---|---|
| `OutboxPodId.java` | ~25 | Process-wide pod identity resolver. Singleton static accessor. |
| `ConfigurableLeaseManager.java` | ~140 | Implements `org.apache.atlas.repository.assetsync.LeaseManager`. Constructor: `(CqlSession, keyspace, leaseTableName, podId)`. All LWT logic identical to `AssetSyncLeaseManager`, table name is a parameter. |
| `OutboxMetrics.java` | ~250 | Instance-based Micrometer metrics. Constructor: `(MeterRegistry, String prefix)`. Exposes `recordWrite()`, `recordRelayPoll()`, `setPendingCount(int)`, etc. — 30+ methods. |

### 4.2 New tag package: `org.apache.atlas.repository.tagoutbox`

Path: `repository/src/main/java/org/apache/atlas/repository/tagoutbox/`

| File | LOC | Role |
|---|---|---|
| `TagOutboxConfig.java` | ~120 | Record bundling every tunable (poll intervals, batch sizes, lease TTL/heartbeat, retry/backoff, keyspace, table names, replication factor, TTL, reconciler interval/jitter/batch/stuck-threshold, thread names). Static `fromAtlasConfiguration()` factory. |
| `TagOutbox.java` | ~310 | Cassandra CRUD for `tag_outbox` table. Implements `Outbox<EntityGuidRef>`. Constructor: `(CqlSession, TagOutboxConfig)`. All prepared statements built from config — no table-name literals. Also exposes `retryFailed(OutboxEntryId)` for admin-triggered FAILED→PENDING promotion. |
| `TagOutboxSchema.java` | ~80 | Idempotent `CREATE KEYSPACE` + `CREATE TABLE IF NOT EXISTS` bootstrap. Takes `TagOutboxConfig`. |
| `TagOutboxSink.java` | ~100 | Write side. Exposes static `install(sink)` + `enqueue(Set<String> guids)` for `EntityGraphMapper` (Surface 1), and instance method `enqueueInternal(guids)` used by `TagOutboxFailureSink` (Surface 2). |
| `TagOutboxConsumer.java` | ~150 | `OutboxConsumer<EntityGuidRef>`. Replays batch via `entityStore.repairClassificationMappingsV2(batch)` + `postProcessor.executeESOperations(deferredOps)` + clear-in-finally. Constructor: `(AtlasEntityStore, EntityCreateOrUpdateMutationPostProcessor, TagOutboxConfig)`. |
| `TagOutboxProcessor.java` | ~280 | Relay thread. Constructor: `(Outbox<EntityGuidRef>, OutboxConsumer<EntityGuidRef>, LeaseManager, OutboxMetrics, TagOutboxConfig)`. Lease-guarded single-thread scheduler, adaptive idle/drain polling. |
| `TagOutboxReconciler.java` | ~200 | Hourly sweeper. Constructor: `(TagOutbox, OutboxConsumer<EntityGuidRef>, LeaseManager, OutboxMetrics, TagOutboxConfig)`. Scans FAILED + stuck-PENDING, ALWAYS replays via consumer (no `findPresentInEs` short-circuit). |
| `TagESWriteFailureRegistry.java` | ~95 | **Tag-scoped failure registry.** Parallel to PR #6568's `ESWriteFailureRegistry` but fully independent: separate class, separate sink slot, separate `TagESWriteFailure` payload. Producers in `EntityMutationService.executeESPostProcessing` and `EntityGraphMapper.safeFlushTagDenormToES` record tag failures here; the tag-outbox sink is the single registered consumer. Mechanical isolation between tag and asset-sync failure pipes. |
| `TagOutboxFailureSink.java` | ~110 | **Surface 2 bridge.** Implements `TagESWriteFailureRegistry.FailureSink`. Installed by `TagOutboxService` on `TagESWriteFailureRegistry`. Extracts vertex IDs from `ESDeferredOperation` payloads (plus `failedVertexIds`), resolves to entity GUIDs via `AtlasGraph.getVertex().getGuid()`, calls `TagOutboxSink.enqueueInternal`. No stage filter needed — the tag-scoped registry only carries tag events. |
| `TagOutboxService.java` | ~170 | `@Service`-annotated Spring bean. `@PostConstruct` bootstraps schema → builds config → builds lease manager → builds outbox → builds metrics → builds consumer → builds processor → builds reconciler → installs `TagOutboxSink` singleton (Surface 1 integration point) → installs `FailureSink` on `TagESWriteFailureRegistry` (Surface 2 integration point) → starts processor + reconciler. Exposes `getOutbox()` / `getActiveConfig()` / `isStarted()` for the admin controller. Gated by `atlas.tag.outbox.enabled` (default true). |

### 4.3 Reused from PR #6568 (`repository.assetsync` package)

Tag package imports these 7 files unchanged. All are stable interfaces or small data classes.

| File | Import why |
|---|---|
| `Outbox.java` | Interface — `TagOutbox` implements `Outbox<EntityGuidRef>` |
| `OutboxConsumer.java` | Interface — `TagOutboxConsumer` implements `OutboxConsumer<EntityGuidRef>` |
| `OutboxEntry.java` | Generic wrapper `OutboxEntry<T>` |
| `OutboxEntryId.java` | Small data class, stable |
| `ConsumeResult.java` | Result of `consume(batch)` |
| `LeaseManager.java` | Interface — `ConfigurableLeaseManager` implements it |
| `EntityGuidRef.java` | Payload — small, neutral (wraps a GUID string) |

### 4.4 Observability files

| File | Role |
|---|---|
| `webapp/.../TagOutboxAdminController.java` | REST endpoints for control-plane ops (see §7.5). Spring `@RestController`, admin-auth protected. |
| `docs/grafana-tag-outbox-dashboard.json` | Grafana dashboard JSON mirroring MS-1010 asset-sync layout, repointed to `atlas_tag_outbox_*` metrics. |
| `docs/tag-outbox-alerts.yaml` | Prometheus alert rules (6 alerts, see §7.4). |
| `docs/tag-outbox-runbook.md` | On-call runbook. What each alert means. What to do. |

### 4.5 Modified existing files

| File | Change | Scope of change |
|---|---|---|
| `repository/.../store/graph/v2/EntityGraphMapper.java` | (1) Replace 3 `tagDenormDLQProducer.emitFailedVertices(...)` call sites inside `flushTagDenormToES` with `TagOutboxSink.enqueue(toGuids(...))`; add private `toGuids` helper. (2) Redirect the `safeFlushTagDenormToES` catch-block `record(...)` call from the shared `ESWriteFailureRegistry` to the tag-scoped `TagESWriteFailureRegistry`. `tagDenormDLQProducer` field and constructor param retained for rollback. | Surface 1 sink swap + Surface 2 registry redirect. **Zero change** to the 7 tag mutation methods themselves — see §2.8. |
| `repository/.../store/graph/v2/EntityMutationService.java` | Redirect the `executeESPostProcessing` catch-block `record(...)` call from `ESWriteFailureRegistry` to `TagESWriteFailureRegistry` (payload type updated correspondingly). Add one import. | Surface 2 registry redirect. Catch-block body only; the happy path and method shape are untouched. |
| `intg/.../AtlasConfiguration.java` | Add ~22 new config entries under `atlas.tag.outbox.*`. | Config registration only. |

### 4.6 Unchanged from PR #6568 and the existing codebase

- All `AssetSync*` concrete classes.
- `ESWriteFailureRegistry` — **source-unchanged, runtime-dormant after this PR** (no production producers reference it anymore; asset-sync may wire it to their own sink later, at which point it serves asset-sync exclusively).
- The 7 tag mutation methods listed in §2.8 — zero lines changed.
- `TagDenormDLQProducer` stays as a Spring bean (injected but uncalled — rollback safety).
- Kafka topic `ATLAS_TAG_DENORM_DLQ` and `TagDenormDLQReplayService` stay in place (cleaned up in Phase 2 follow-up once outbox is proven in staging).

---

## 5. Data Flow

### 5.1 Happy path (unchanged)

```
Tag mutation / propagation
        │
        ▼
flushTagDenormToES
        │
        ├─ Read tags for vertexIds from Cassandra (async batch)
        ├─ Compute 5 denorm fields via computeAllDenormAttributes
        └─ ESConnector.writeTagPropertiesWithResult
               │
               └─ All success → return
```

### 5.2 Failure paths (two surfaces, one outbox)

Both tag ES-sync failure surfaces converge on the same `TagOutboxSink.enqueue` entry point.

**Surface 1 — propagation failures:**
```
Propagation task (one of 4 propagation methods, unchanged)
        │
        ▼
flushTagDenormToES detects failure (ES partial failure, Cassandra read failure, or total exception)
        │
        ▼
TagOutboxSink.enqueue(failedGuids)        ← REPLACES  tagDenormDLQProducer.emitFailedVertices
```

**Surface 2 — direct tag attachment failures:**
```
addClassificationsV2 / deleteClassificationV2 / updateClassificationsV2 (unchanged)
        │
        ├─ stages ESDeferredOperation in RequestContext (unchanged)
        ▼
EntityMutationService.executeESPostProcessing (unchanged)
        │
        ├─ entityMutationPostProcessor.executeESOperations(deferredOps) throws
        ▼
TagESWriteFailureRegistry.record(failure)  ← this PR redirects the call here (was ESWriteFailureRegistry in PR #6568)
        │
        ▼
[FailureSink installed by TagOutboxService.init]
        │
        ├─ extract vertex IDs from ESDeferredOperation.payload keys + entityId
        ├─ resolve vertex IDs → GUIDs via graph lookup
        ▼
TagOutboxSink.enqueue(guids)              ← SAME entry point as Surface 1
```

**Common path (both surfaces converge):**
```
TagOutboxSink.enqueue(guids)
        │
        ▼
INSERT INTO atlas_tag_outbox.tag_outbox (status=PENDING, entity_guid=G, ...)
        │
        ▼
TagOutboxProcessor (relay thread)         ← polls PENDING, lease-guarded via tag-outbox-relay
        │
        ├─ Claim batch (up to 500 entries in drain mode)
        ├─ TagOutboxConsumer.consume(batch)
        │      │
        │      └─ For each chunk:
        │             try {
        │                 errors = entityStore.repairClassificationMappingsV2(chunk);
        │                 postProcessor.executeESOperations(deferredOps);
        │             } finally {
        │                 RequestContext.get().getESDeferredOperations().clear();
        │             }
        │
        └─ Per-entry outcome handling:
               succeeded    → outbox.markDone(id)
               retryable    → outbox.releaseForRetry(id, attempt+1)
               permanent    → outbox.markFailed(id, attempt, cause)
```

### 5.3 Reconciliation (new)

```
TagOutboxReconciler (every ~60 min, lease-guarded)
        │
        ├─ scanFailed(limit) — up to 500 entries from FAILED partition
        ├─ scanStuckPending(stuckThreshold, limit) — orphaned PENDING rows
        │
        └─ For each batch:
               └─ consumer.consume(batch)    ← SAME consumer as relay, no ES presence check
                      ├─ Success  → drop row
                      ├─ Retry    → leave in place for next run
                      └─ Permanent → leave in place (manual intervention needed)
```

---

## 6. Lifecycle

### 6.1 Startup (`TagOutboxService.@PostConstruct`)

1. Check `atlas.tag.outbox.enabled` — if false, log and return.
2. Build `TagOutboxConfig.fromAtlasConfiguration()`.
3. Acquire shared Cassandra session via `CassandraSessionProvider.getSharedSession(...)`.
4. `TagOutboxSchema.bootstrap(session, config)` — idempotent CREATE KEYSPACE + CREATE TABLES.
5. Build `OutboxMetrics` instance with prefix `atlas_tag_outbox_`.
6. Build `ConfigurableLeaseManager(session, config.keyspace(), config.leaseTableName(), OutboxPodId.get())`.
7. Build `TagOutbox(session, config)`.
8. Build `TagOutboxConsumer(entityStore, postProcessor)`.
9. Build `TagOutboxProcessor(outbox, consumer, lease, metrics, config)`.
10. Build `TagOutboxSink(outbox)` and `TagOutboxSink.install(sink)` — Surface 1 integration point (propagation failure path in `EntityGraphMapper.flushTagDenormToES`).
11. Install a `FailureSink` on `TagESWriteFailureRegistry` — Surface 2 integration point (direct attachment failure path in `EntityMutationService.executeESPostProcessing` + propagation-flush safety net in `EntityGraphMapper.safeFlushTagDenormToES`). The sink extracts vertex IDs from `ESDeferredOperation` payloads, resolves them to GUIDs via graph lookup, and calls `TagOutboxSink.enqueue(guids)`. The tag-scoped registry is separate from asset-sync's `ESWriteFailureRegistry` — tag failures and asset-sync failures flow through independent registries with independent sink slots.
12. `processor.start()` — schedules first poll with jittered delay.
13. If `atlas.tag.outbox.reconciler.enabled` (default true): build and start `TagOutboxReconciler`.

### 6.2 Shutdown (`@PreDestroy`)

1. `TagOutboxSink.install(null)` — uninstall Surface 1 sink.
2. `TagESWriteFailureRegistry.setSink(null)` — uninstall Surface 2 sink (reverts to default no-op).
3. Stop reconciler (cancels scheduled task, graceful thread join).
4. Stop processor (cancels poll task, releases lease immediately for fast failover).

### 6.3 Runtime invariants

- Exactly one pod per tenant holds `tag-outbox-relay` at any moment (via LWT `IF NOT EXISTS` on the lease row).
- Lease TTL is 60s; leader heartbeats every 30s. If leader crashes, another pod takes over within 60s.
- Reconciler shares the relay lease — only the leader pod runs the reconciler tick.
- `asset-sync-relay` lease and `tag-outbox-relay` lease are **completely independent**. Different pods can lead each.

---

## 7. Observability — Control Plane for Tag ES Operations

This section fulfills the observability requirements: control plane, eliminated manual scripts, visibility into issues, reduced clueless debugging.

### 7.1 Metrics surface (`atlas_tag_outbox_*`)

All metrics registered via the shared `OutboxMetrics` class. Parallel to MS-1010 asset-sync surface.

**Write side** (producer, recorded from `TagOutboxSink`):
- `atlas_tag_outbox_writes_total` — counter, enqueue attempts
- `atlas_tag_outbox_write_errors_total{reason}` — counter, enqueue errors (by reason)
- `atlas_tag_outbox_write_latency_seconds` — timer, write latency
- `atlas_tag_outbox_payload_bytes` — distribution, payload size

**Storage gauges** (refreshed by relay leader every 30s):
- `atlas_tag_outbox_pending_count` — PENDING entries
- `atlas_tag_outbox_processing_count` — currently-claimed (in flight)
- `atlas_tag_outbox_failed_count` — FAILED entries (awaiting reconciliation)
- `atlas_tag_outbox_oldest_pending_age_seconds` — age of oldest PENDING (early stall indicator)

**Relay** (per poll cycle):
- `atlas_tag_outbox_relay_polls_total` — counter, poll cycles
- `atlas_tag_outbox_relay_batches_processed_total` — counter, non-empty batches
- `atlas_tag_outbox_relay_processed_total` — counter, successfully replayed entries
- `atlas_tag_outbox_relay_permanently_failed_total` — counter, FAILED transitions
- `atlas_tag_outbox_relay_reclaimed_processing_total` — counter, stuck-PROCESSING rows reclaimed
- `atlas_tag_outbox_relay_lag_seconds` — timer, enqueue-to-delivery lag
- `atlas_tag_outbox_relay_batch_size` — distribution, entries per batch
- `atlas_tag_outbox_relay_failures_total{reason}` — counter, failures (by reason)

**Leader election:**
- `atlas_tag_outbox_relay_leader` — gauge (0/1 per pod)
- `atlas_tag_outbox_lease_handovers_total` — counter, leadership transitions
- `atlas_tag_outbox_lease_acquire_attempts_total{result}` — counter, acquisition attempts (acquired/held_by_other/reacquired/heartbeat_lost)

**Reconciler:**
- `atlas_tag_outbox_reconciler_runs_total` — counter, ticks (only on leader pod)
- `atlas_tag_outbox_reconciler_repaired_total` — counter, entries replayed via consumer
- `atlas_tag_outbox_reconciler_still_missing_total` — counter, entries left in place
- `atlas_tag_outbox_reconciler_tick_errors_total` — counter, ticks that threw (non-zero = investigate)
- `atlas_tag_outbox_reconciler_scanned_total{status}` — counter, entries scanned
- `atlas_tag_outbox_reconciler_healthy` — gauge (0/1)
- `atlas_tag_outbox_reconciler_last_run_timestamp_seconds` — gauge, Unix ts of last tick

### 7.2 Structured logs

Every log line from `TagOutbox*` classes includes standard fields:
- `subsystem=tag-outbox`
- `pod_id` (from `OutboxPodId.get()`)
- `guid` (when processing a specific entry)
- `attempt_count` (when retrying)
- `batch_size` (for batch operations)
- `lease_status` (relay only: `leader|standby|contested`)

Enables filterable debugging: "show me all tag-outbox events for guid=X" becomes a single log query.

### 7.3 Grafana dashboard (`docs/grafana-tag-outbox-dashboard.json`)

Panel layout (mirrors MS-1010 asset-sync dashboard, repointed PromQL):

- **Row 1 — Storage health:** 4 stat panels (pending, processing, failed, oldest-pending-age). Color-graded thresholds (green/yellow/red).
- **Row 2 — Throughput:** time-series of `relay_processed_total` and `relay_permanently_failed_total` rates.
- **Row 3 — Failure analysis:** top-10 `relay_failures_total{reason}` breakdown, top-10 `write_errors_total{reason}`.
- **Row 4 — Leader election:** per-pod `relay_leader` gauge panel, `lease_handovers_total` rate.
- **Row 5 — Reconciler:** `reconciler_runs_total` rate, `reconciler_repaired_total`, `reconciler_still_missing_total`, `reconciler_healthy` gauge, `time() - reconciler_last_run_timestamp_seconds` (time-since-last-run).

### 7.4 Prometheus alert rules (`docs/tag-outbox-alerts.yaml`)

```yaml
groups:
- name: tag-outbox
  rules:
  - alert: TagOutboxBacklog
    expr: atlas_tag_outbox_failed_count > 100
    for: 15m
    labels:
      severity: warning
    annotations:
      summary: "Tag outbox has {{$value}} FAILED entries — manual review needed"
      runbook_url: "docs/tag-outbox-runbook.md#backlog"

  - alert: TagOutboxStuckEntry
    expr: atlas_tag_outbox_oldest_pending_age_seconds > 3600
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Tag outbox has entry stuck PENDING >1h — relay may be stalled"
      runbook_url: "docs/tag-outbox-runbook.md#stuck-entry"

  - alert: TagOutboxNoLeader
    expr: max(atlas_tag_outbox_relay_leader) == 0
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "No pod holds tag-outbox-relay lease — relay is dead cluster-wide"
      runbook_url: "docs/tag-outbox-runbook.md#no-leader"

  - alert: TagOutboxReconcilerDown
    expr: max(atlas_tag_outbox_reconciler_healthy) == 0
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Tag outbox reconciler is unhealthy on all pods"
      runbook_url: "docs/tag-outbox-runbook.md#reconciler-down"

  - alert: TagOutboxReconcilerStalled
    expr: time() - atlas_tag_outbox_reconciler_last_run_timestamp_seconds > 7200
    for: 15m
    labels:
      severity: warning
    annotations:
      summary: "Tag outbox reconciler hasn't run in 2h"
      runbook_url: "docs/tag-outbox-runbook.md#reconciler-stalled"

  - alert: TagOutboxFailureRateSpike
    expr: rate(atlas_tag_outbox_relay_failures_total[5m]) > 10
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Tag outbox relay failure rate spiking ({{$value}}/s)"
      runbook_url: "docs/tag-outbox-runbook.md#failure-spike"
```

### 7.5 Admin REST endpoints (`TagOutboxAdminController`)

Spring `@RestController` in webapp layer. Admin-auth protected (same pattern as existing `DLQAdminController`). Endpoints are under `/api/meta/tag-outbox/`.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/status` | JSON summary: `{pending, processing, failed, oldestPendingAgeSeconds, leaderPodId, reconcilerHealthy, lastReconcilerRunTimestamp}` |
| `GET` | `/entries?status={PENDING\|FAILED}&limit=50` | List entries with metadata: `[{entityGuid, attemptCount, createdAt, lastAttemptedAt, nextAttemptAt}]` |
| `POST` | `/entries/{guid}/retry` | Move a FAILED row back to PENDING for immediate retry. Returns 404 if not found. |

**Deferred to Phase 2 (follow-up PR):** force-reconcile trigger, manual row delete, dry-run mode.

### 7.6 Runbook (`docs/tag-outbox-runbook.md`)

Sections per alert:

- **`#backlog`** — what `TagOutboxBacklog` means, how to triage. Diagnostic queries (Cassandra + Grafana). Manual-retry procedure via admin API.
- **`#stuck-entry`** — what `TagOutboxStuckEntry` means. How to check whether relay is stalled vs. genuinely hard-to-repair entry. Steps to investigate individual stuck entry.
- **`#no-leader`** — critical escalation steps. How to check lease row state. Pod-restart vs. Cassandra lease corruption.
- **`#reconciler-down`** — pod restart check. Config check. Log grep.
- **`#reconciler-stalled`** — same as #reconciler-down but with time-component focus.
- **`#failure-spike`** — how to check `relay_failures_total{reason}` breakdown. Common reasons (ES outage, mapping error, Cassandra read timeout) and remedies.

### 7.7 What operators can do without SSH

| Scenario | Before this PR | After |
|---|---|---|
| "Is tag ES sync healthy?" | SSH pods, grep logs | One glance at Grafana Row 1 |
| "How many failures pending?" | Query Kafka DLQ topic | Grafana stat panel |
| "Which pod is the leader?" | grep logs | Grafana per-pod gauge |
| "Is the reconciler running?" | grep logs for "reconciler" | Grafana `reconciler_healthy` + `time-since-last-run` |
| "Replay a specific failed GUID" | Write a Kafka script | `POST /api/meta/tag-outbox/entries/{guid}/retry` |
| "Why did GUID X fail?" | grep logs across pods | Filter logs by `guid=X` (structured fields) |
| "Silent data loss risk?" | Hope the Kafka DLQ consumer is alive | Alert fires within 15m if backlog grows or relay dies |

---

## 8. Testing

### 8.1 Unit tests (required)

- `TagOutboxTest` — CRUD smoke tests (enqueue, claim, markDone, markFailed, releaseForRetry) against an in-memory Cassandra fake or embedded Cassandra, using a customizable `TagOutboxConfig`.
- `TagOutboxConsumerTest` — mock `AtlasEntityStore` + `EntityCreateOrUpdateMutationPostProcessor`. Verify per-GUID success/failure classification and deferred-op flush ordering.
- `ConfigurableLeaseManagerTest` — LWT acquire/heartbeat/release semantics with table-name parameter.
- `OutboxMetricsTest` — instantiate with prefix, verify all counters/gauges register with the expected names.

### 8.2 Integration test (nice-to-have for MVP)

End-to-end:
1. Start `TagOutboxService` with a test Cassandra keyspace.
2. Enqueue 10 fake failures via `TagOutboxSink.enqueue(guids)`.
3. Verify rows appear in `tag_outbox` PENDING partition.
4. Mock `repairClassificationMappingsV2` to succeed for 8 guids, fail for 2.
5. Let relay tick once.
6. Assert 8 rows gone, 2 rows show `attempt_count=1` with `next_attempt_at` in future.

### 8.3 Manual staging verification

1. Deploy with `atlas.tag.outbox.enabled=true` and Kafka DLQ producer calls removed.
2. Simulate ES outage (blocklist vertex_index writes).
3. Trigger tag propagation → verify rows appear in `atlas_tag_outbox.tag_outbox`.
4. Restore ES → verify rows drain within 30s (Grafana pending_count drops).
5. Confirm ES docs have correct denorm fields post-recovery via spot-check.
6. Force a permanent failure (e.g., bad tag attribute) → verify row moves to FAILED after 10 retries → verify `TagOutboxBacklog` alert fires after 15m.
7. Exercise admin API: `GET /status`, `GET /entries?status=FAILED`, `POST /entries/{guid}/retry`.

---

## 9. Rollout

### 9.1 Phase 1 (this PR)

- Ship with `atlas.tag.outbox.enabled=true` on staging.
- `TagDenormDLQProducer` bean left wired (unused). Kafka topic + `TagDenormDLQReplayService` untouched.
- Monitor for one release cycle:
  - `atlas_tag_outbox_pending_count` normal range
  - `atlas_tag_outbox_failed_count` near zero
  - `atlas_tag_outbox_reconciler_still_missing_total` near zero
  - No alerts firing
  - Grafana dashboard reflects real activity

### 9.2 Phase 2 (follow-up PR, once steady)

- Remove `TagDenormDLQProducer` class entirely.
- Remove `@Autowired` injection from `EntityGraphMapper`.
- Delete `ATLAS_TAG_DENORM_DLQ` Kafka topic.
- Remove `TagDenormDLQReplayService` consumer.
- Optionally: extract generic `OutboxProcessor<T>` refactor (asset-sync team's call).
- Optionally: add advanced admin endpoints (force-reconcile, row-delete).

### 9.3 Rollback paths

| Scenario | Rollback |
|---|---|
| Outbox subsystem has a runtime issue | Set `atlas.tag.outbox.enabled=false` — subsystem disabled, sink is no-op. Failures become silent again (pre-DLQ-era behavior). |
| Need to re-enable DLQ producer path | Single-commit revert of the 3 call-site changes in `EntityGraphMapper`. `tagDenormDLQProducer` bean is still wired. |
| Keyspace corruption | Drop `atlas_tag_outbox` keyspace; re-enable → schema bootstraps fresh. |

---

## 10. File Inventory

### New files — shared package (3)
```
repository/src/main/java/org/apache/atlas/repository/outbox/shared/
├── OutboxPodId.java                      ~25 LOC
├── ConfigurableLeaseManager.java         ~140 LOC
└── OutboxMetrics.java                    ~250 LOC
```

### New files — tag package (10)
```
repository/src/main/java/org/apache/atlas/repository/tagoutbox/
├── TagOutboxConfig.java                  ~120 LOC
├── TagOutbox.java                        ~310 LOC
├── TagOutboxSchema.java                  ~80 LOC
├── TagOutboxSink.java                    ~100 LOC
├── TagOutboxConsumer.java                ~150 LOC
├── TagOutboxProcessor.java               ~280 LOC
├── TagOutboxReconciler.java              ~200 LOC
├── TagESWriteFailureRegistry.java        ~95  LOC
├── TagOutboxFailureSink.java             ~110 LOC
└── TagOutboxService.java                 ~170 LOC
```

### New files — observability (4)
```
webapp/src/main/java/org/apache/atlas/web/rest/
└── TagOutboxAdminController.java         ~180 LOC
docs/
├── grafana-tag-outbox-dashboard.json     ~400 LOC (JSON)
├── tag-outbox-alerts.yaml                ~60 LOC
└── tag-outbox-runbook.md                 ~200 LOC (markdown)
```

### Modified existing files (3)
```
repository/src/main/java/org/apache/atlas/repository/store/graph/v2/EntityGraphMapper.java
    3 flushTagDenormToES call sites swapped + toGuids helper (~30 LOC)
    + safeFlushTagDenormToES redirect to TagESWriteFailureRegistry (~7 LOC)
repository/src/main/java/org/apache/atlas/repository/store/graph/v2/EntityMutationService.java
    executeESPostProcessing catch-block record() redirect to TagESWriteFailureRegistry
    + 1 import (~3 LOC)
intg/src/main/java/org/apache/atlas/AtlasConfiguration.java
    +22 new config entries under atlas.tag.outbox.*, ~40 LOC added
```

### Reused from PR #6568 (unchanged, integration points only)

Imported for type resolution:
```
repository/src/main/java/org/apache/atlas/repository/assetsync/
├── Outbox.java              (interface)
├── OutboxConsumer.java      (interface)
├── OutboxEntry.java         (data class)
├── OutboxEntryId.java       (data class)
├── ConsumeResult.java       (data class)
├── LeaseManager.java        (interface)
└── EntityGuidRef.java       (payload data class)
```

Read for payload extraction (not modified):
```
intg/src/main/java/org/apache/atlas/model/
└── ESDeferredOperation.java
    — the Surface 2 sink reads getPayload() + getEntityId() to extract vertex IDs
      before resolving them to GUIDs for outbox enqueue.
```

**Not used by this PR (but adjacent):** PR #6568's `ESWriteFailureRegistry` stays in
place, unreferenced by production code after this PR's redirect. It remains
available for asset-sync to wire up their own consumer in a future change,
without any interaction with the tag subsystem.

### Unchanged from PR #6568 (zero touch)
All `AssetSync*` concrete classes, `PostCommitEsVerifier`, `EntityMutationService.executeESPostProcessing`.

### Unchanged (rollback safety)
- `TagDenormDLQProducer.java` (field wired, no longer called)
- `TagDenormDLQReplayService.java` (consumer still running; no-op if no messages)
- Kafka topic `ATLAS_TAG_DENORM_DLQ`

### New files — unit tests (2)
```
repository/src/test/java/org/apache/atlas/repository/outbox/shared/
└── OutboxMetricsTest.java                 ~140 LOC
repository/src/test/java/org/apache/atlas/repository/tagoutbox/
└── TagOutboxConsumerTest.java             ~190 LOC
```

Integration tests for `TagOutbox` CRUD and `ConfigurableLeaseManager` LWT
semantics require an embedded Cassandra harness and are deferred to a
follow-up PR in the test-infra track.

**Estimated total diff: ~2,500 LOC new code + ~70 LOC modified.**

---

## 11. Configuration Keys

All under `atlas.tag.outbox.*` namespace.

| Key | Default | Purpose |
|---|---|---|
| `atlas.tag.outbox.enabled` | `true` | Master kill-switch |
| `atlas.tag.outbox.reconciler.enabled` | `true` | Disable reconciler only |
| `atlas.tag.outbox.keyspace` | `atlas_tag_outbox` | Cassandra keyspace name |
| `atlas.tag.outbox.replication.factor` | `3` | Replication factor for keyspace |
| `atlas.tag.outbox.outbox.table.name` | `tag_outbox` | Primary table name |
| `atlas.tag.outbox.lease.table.name` | `tag_outbox_lease` | Lease table name |
| `atlas.tag.outbox.ttl.seconds` | `604800` | Row TTL (7 days) |
| `atlas.tag.outbox.max.attempts` | `10` | Max relay retries before FAILED |
| `atlas.tag.outbox.relay.idle.poll.seconds` | `30` | Poll interval when PENDING is empty |
| `atlas.tag.outbox.relay.drain.poll.seconds` | `2` | Poll interval when draining |
| `atlas.tag.outbox.relay.idle.batch.size` | `100` | Batch size when idle |
| `atlas.tag.outbox.relay.drain.batch.size` | `500` | Batch size when draining |
| `atlas.tag.outbox.relay.lease.ttl.seconds` | `60` | Lease TTL |
| `atlas.tag.outbox.relay.lease.heartbeat.seconds` | `30` | Lease heartbeat interval |
| `atlas.tag.outbox.relay.backoff.base.ms` | `1000` | Exponential backoff base |
| `atlas.tag.outbox.relay.backoff.max.ms` | `300000` | Exponential backoff cap (5min) |
| `atlas.tag.outbox.relay.claim.ttl.seconds` | `120` | Claim TTL (row in-flight timeout) |
| `atlas.tag.outbox.reconciler.interval.seconds` | `3600` | Reconciler tick interval (1h) |
| `atlas.tag.outbox.reconciler.jitter.seconds` | `60` | Initial-delay jitter |
| `atlas.tag.outbox.reconciler.batch.size` | `500` | Entries scanned per reconciler batch |
| `atlas.tag.outbox.reconciler.stuck.pending.threshold.seconds` | `7200` | Age threshold for stuck-PENDING detection (2h) |
| `atlas.tag.outbox.consumer.repair.batch.size` | `300` | Batch size for repairClassificationMappingsV2 calls |

All loaded once in `TagOutboxConfig.fromAtlasConfiguration()` → passed to every component via the config record. No inline `AtlasConfiguration.get()` calls inside the component classes.

