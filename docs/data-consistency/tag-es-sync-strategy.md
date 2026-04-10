# Tag ES Sync Strategy — Eliminating Cassandra-ES Drift

## Problem

Atlas stores tag (classification) data in Cassandra (`tags.tags_by_id`) and derives five denormalized fields in Elasticsearch for search and UI. These two systems can drift apart — tags exist in Cassandra but `__traitNames`/`__classificationNames` are stale or missing in ES.

| ES Field | Source |
|----------|--------|
| `__traitNames` | Direct tag type names (list) |
| `__classificationNames` | Direct tag type names (pipe-delimited: `\|tag1\|tag2\|`) |
| `__propagatedTraitNames` | Propagated tag type names (list) |
| `__propagatedClassificationNames` | Propagated tag type names (pipe-delimited) |
| `__classificationsText` | All tag names concatenated for full-text search |

### Root cause

The Cassandra tag write and ES denorm write are **two separate operations that aren't atomically coupled**:

```
1. Tag write to Cassandra      → in transaction, committed
2. ES denorm write              → deferred, post-commit, can fail silently
```

If step 2 fails (pod crash, ES unavailable, OOM, silent exception), step 1 is already committed. Drift.

### Historical bugs that caused drift

| Bug | Description | Status |
|-----|-------------|--------|
| MS-456 | HashMap ordering caused DELETE to overwrite ADD in deferred ES ops | Fixed |
| MS-601 | `__traitNames` vertex property not updated in V2 tag path — JanusGraph auto-indexing fallback was disabled | Fixed (PR #6046) |
| MS-655 | Propagation delete not writing empty list to ES denorm fields | Fixed |

All three are fixed. But **historical drift from Nov 2025 persists** on tenants where tags were written while these bugs existed. No automated repair has been run fleet-wide.

### Scale of the problem

- 500+ production tenants
- Customer-reported drift on `dg.atlan.com` (Deutsche Glasfaser): columns with Snowflake tags in Cassandra but not in ES, surfacing repeatedly (MS-880, MS-970)
- Cross-tenant analysis shows empty-tagTypeName propagation tasks on multiple large tenants (autodesk-vc2: 149K, inovalon01: 60K, lendingclub-prod: 8.7K)
- On newsuk: 83% of propagated tags were orphaned (source entity lost direct tag, downstream not cleaned up)

## Strategy — Three Layers

### Layer 1: Ring PR — full-snapshot ES writes (hot path)

**PR**: [#6481](https://github.com/atlanhq/atlas-metastore/pull/6481) (Pratham)

Redesigns the propagation ES sync path from incremental field updates to full-snapshot writes.

**Old approach**: Read tags from Cassandra → manually splice in the new tag → compute partial denorm fields → write to Cassandra → write partial fields to ES.

**New approach**: Write to Cassandra FIRST → buffer vertex IDs in `RequestContext` → `flushTagDenormToES()` batch-reads ALL tags from committed Cassandra state → computes ALL 5 denorm fields → writes full snapshot to ES.

Key improvements:
- **Read-after-write correctness**: Cassandra write happens before denorm read — no manual tag splicing needed
- **Full-snapshot writes**: All 5 denorm fields written on every sync — eliminates partial-field stale data
- **Per-document failure detection**: `writeTagPropertiesWithResult()` parses ES bulk response per-doc (previously HTTP 200 = assumed full success)
- **Kafka DLQ**: Failed ES writes emit vertex IDs + GUIDs to `ATLAS_TAG_DENORM_DLQ` topic for replay

**Scope**: Propagation paths only (Phase 1). Direct attachment paths (add/delete/update classification) still use old deferred ES op pattern — marked with TODOs for Phase 2.

**Status**: GA — merged and deployed to all tenants. Phase 2 (direct attachment paths) is pending.

### Layer 2: Cassandra outbox — atomic delivery guarantee (safety net)

**Purpose**: Close the gap that the ring PR can't — pod crash between Cassandra write and flush, Kafka DLQ producer failure, direct attachment paths not yet migrated.

**Mechanism**: LOGGED BATCH couples tag write with a `pending_es_sync` marker atomically.

```
Tag write + pending_es_sync INSERT  →  LOGGED BATCH (atomic)
                                        ↓
                                    Buffer + flush (ring PR hot path)
                                        ↓
                                    ES write succeeds → DELETE pending_es_sync
                                    ES write fails → pending_es_sync persists
                                        ↓
                                    Background sweep (every 5 min)
                                    picks up stale entries, reconciles
```

#### Table design

```sql
CREATE TABLE tags.pending_es_sync (
    vertex_id text PRIMARY KEY,
    created_at timestamp
) WITH default_time_to_live = 86400
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
```

Design decisions:
- **Simple PRIMARY KEY on vertex_id**: Table is near-empty in steady state. Full scan returns 0-10 rows. No partitioning needed.
- **STCS compaction**: Appropriate for a tiny table with insert/delete churn. No TWCS needed.
- **1-day TTL**: Safety net for orphaned entries (pod crash before explicit DELETE). Entries are normally deleted within seconds.
- **`gc_grace_seconds` can be reduced to 1 day**: This is a transient work queue, not a long-lived data table. Short GC grace is safe.

#### Tombstone management

Not a concern because:
- Table is designed to be empty in steady state
- Entries live for seconds (INSERT → DELETE after ES flush)
- TTL-expired entries are compacted by STCS
- Full table scan on a near-empty table is unaffected by tombstones
- Fundamentally different from `tags_by_id` (millions of rows, long-lived)

#### Code changes (~100 lines)

1. **Table creation** in `TagDAOCassandraImpl.initializeSchema()` — `CREATE TABLE IF NOT EXISTS`
2. **LOGGED BATCH** in tag write methods (`putDirectTag`, `deleteDirectTag`, `putPropagatedTag`, `deletePropagatedTag`, etc.):
   ```java
   BatchStatement batch = BatchStatement.builder(BatchType.LOGGED)
       .addStatement(existingTagWriteStmt.bind(...))
       .addStatement(insertPendingSyncStmt.bind(vertexId, Instant.now()))
       .build();
   executeWithRetry(batch);
   ```
   Both statements are in the `tags` keyspace — cheap single-keyspace LOGGED BATCH.
3. **DELETE after successful flush** in `flushTagDenormToES()` after ES write succeeds.
4. **Background sweep** — `@Scheduled` bean, every 5 minutes, scans `pending_es_sync`, reconciles stale entries.

#### What the outbox covers that the ring PR doesn't

| Failure mode | Ring PR alone | Ring PR + Outbox |
|-------------|---------------|-----------------|
| ES 5xx/timeout | DLQ replay | DLQ replay + outbox sweep |
| Pod crash between C* write and flush | **Lost** (buffer is in-memory) | **Covered** (LOGGED BATCH persists marker) |
| Kafka DLQ producer failure | **Lost** (logged but not tracked) | **Covered** (marker persists regardless of Kafka) |
| Direct attachment paths (Phase 2 TODOs) | Old deferred ES op (can fail silently) | **Covered** (BATCH includes marker) |

#### Race condition analysis

Race between sweep and live tag write:
1. Sweep reads vertex V → sees tags [X]
2. Live write adds tag Y → BATCH(tag write + pending_es_sync INSERT for V)
3. Sweep writes ES with [X] (stale, missing Y)
4. Deferred ES op from step 2 writes [X, Y] → correct. If this also fails, pending_es_sync still has V → next sweep reads [X, Y] → correct.

**Self-correcting**: Every tag mutation re-inserts into `pending_es_sync` atomically. Transient inconsistency window is seconds.

#### Open question: Outbox vs Kafka DLQ

With the outbox in place, `pending_es_sync` serves the same retry purpose as the Kafka DLQ (`ATLAS_TAG_DENORM_DLQ` + `TagDenormDLQReplayService`). Consider whether both are needed:

| Aspect | Kafka DLQ | Cassandra outbox |
|--------|-----------|-----------------|
| Delivery guarantee | At-least-once (Kafka acks=all) | Atomic with tag write (LOGGED BATCH) |
| Retry mechanism | Consumer + replay service (701 lines) | Background sweep (~50 lines) |
| Observability | Consumer lag metrics | Table row count |
| Infrastructure | Requires Kafka topic + consumer | Just Cassandra (already have it) |
| Failure mode | If Kafka is down → entries lost | If C* is down → tag write itself fails |

The outbox is strictly stronger (atomic guarantee) and simpler (no Kafka dependency for retries). The Kafka DLQ could be removed in favor of the outbox, simplifying the ring PR. Decision point for the team.

### Layer 3: Reconciler — one-time historical cleanup

**PR**: [#6448](https://github.com/atlanhq/atlas-metastore/pull/6448)

Change-log-driven reconciliation service that drains a `tags.tag_change_log` table and, for each vertex, reads Cassandra, computes denorm, writes full snapshot to ES, and cleans orphaned propagated tags.

**Purpose**: Fix pre-existing drift from before the ring PR and outbox were deployed.

**Deployment**: Per-tenant seed job + enable via config. Runs within configurable nightly window. Resumes from saved cursor. Single-pod via Redis lock.

**After the ring PR + outbox are deployed**: The reconciler becomes a run-once tool for historical cleanup. New drift is prevented by the outbox. The reconciler can be disabled after its initial pass completes.

## Deployment plan

The three layers are **independently deployable**:

| Phase | What | Scope | Risk | Status |
|-------|------|-------|------|--------|
| 1 | **Ring PR** (#6481) | All tenants | Medium | **GA — deployed** |
| 2a | **Ring PR Phase 2** — migrate direct attachment paths to buffer+flush | All tenants | Low | Pending — 5 paths marked with TODOs |
| 2b | **Outbox** (new PR) | All tenants (code deploy, auto-creates table) | Low | Pending — ~100 lines on top of ring PR |
| 3 | **Reconciler** (#6448) | Per-tenant via mothership automation | Low | PR open, pending review fixes |

### Remaining work from Ring PR (Phase 2)

Five direct attachment paths still use the old `ESDeferredOperation` pattern and need migration to the buffer+flush pattern:

1. `addClassificationsV2` — direct tag add
2. `deleteClassificationV2` — direct tag delete
3. `updateClassificationsV2` — direct tag update
4. `repairClassificationMappingsV2` — repair path
5. `addEsDeferredOperation` — fallback for entity not in Cassandra

## Task processor improvements

The task executor (`TaskExecutor.java`, `TaskQueueWatcher.java`, `TaskRegistry.java`) has systemic issues that compound tag propagation problems:

### Critical

1. **Infinite retry on pod crash**: `attemptCount` is only incremented in the catch block of `AbstractTask.run()`. Pod crash/OOM kill means the catch never runs — task stays `IN_PROGRESS` with `attemptCount=0` forever, crashing the pod in a loop.
2. **No task deduplication**: `TaskRegistry.createVertex()` does blind INSERTs. The V2 path (`createAndQueueTaskWithoutCheckV2`) skips dedup entirely. Observed: 91.4% duplicate tasks on dgwhgm1p02 (287K tasks, 24.7K unique).
3. **Single-threaded executor**: `Executors.newSingleThreadExecutor()` — one task at a time. An 80-minute Table lineage traversal blocks 1,574 lightweight tasks. Caused 45-hour drain estimate for 4.5K tasks.

### High

4. **attemptCount not persisted before execution**: If pod dies between execution start and the catch block's `registry.updateStatus()`, the increment is lost.
5. **No task timeout**: IN_PROGRESS tasks have no TTL. A pathological task (corrupted graph, circular reference) blocks the executor permanently.
6. **No Prometheus metrics**: Only MDC-based logging to ClickHouse. No real-time gauges for queue depth, execution duration, or failure rate. The `classification-tag-propagation` dashboard is useful for post-mortem but too slow for alerting.

### Medium

7. **FIFO ordering with no priority**: Tasks sorted by `TASK_CREATED_TIME ASC`. A batch of 94K connector tasks blocks customer-initiated propagation. No way to prioritize by urgency.
8. **ES/graph status mismatch**: `TaskRegistry` has built-in repair logic (`repairMismatchedTask`) with `retry_on_conflict: 10` — indicates a known consistency issue that's been papered over.
9. **Graph commit inside setStatus()**: `ClassificationTask.setStatus()` calls `graph.commit()` inline — no transaction spanning task vertex update and entity edge cleanup.

## Decision log

| Decision | Rationale |
|----------|-----------|
| Full-snapshot ES writes over incremental | Eliminates partial-field stale data. One code path to reason about. Read-after-write from Cassandra guarantees correctness. |
| LOGGED BATCH for outbox over separate INSERT | Atomic coupling between tag write and sync marker. Separate INSERT can fail independently — same gap we're trying to close. |
| `pending_es_sync` with simple PRIMARY KEY over day-bucketed partitions | Table is near-empty in steady state. No need for time-based partitioning or cursor management. Full scan of 0-10 rows is free. |
| TTL + STCS over explicit cleanup | Self-cleaning. No GC overhead. STCS handles the insert/delete churn pattern. |
| Continuous sweep over nightly window | Drift is fixed in minutes, not hours. No operational scheduling. Near-empty table means sweep cost is negligible. |
| Reconciler as one-time tool, not permanent infrastructure | Once outbox prevents new drift, the reconciler's ongoing value is zero. Permanent infrastructure has ongoing operational cost. |
| Outbox could replace Kafka DLQ | Strictly stronger guarantee (atomic vs best-effort), simpler (no Kafka consumer), fewer moving parts. Open for team discussion. |

## Related tickets

- [MS-880](https://linear.app/atlan-epd/issue/MS-880) — Original Snowflake tag drift report (Deutsche Glasfaser, Nov 2025)
- [MS-970](https://linear.app/atlan-epd/issue/MS-970) — Follow-up: 3 more columns with same drift
- [MS-922](https://linear.app/atlan-epd/issue/MS-922) — 443K stuck propagation tasks (empty tagTypeName)
- [MS-907](https://linear.app/atlan-epd/issue/MS-907) — Orphaned propagated tags not cleaned up
