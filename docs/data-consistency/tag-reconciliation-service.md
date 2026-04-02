# Tag Reconciliation Service

## Problem

Atlas stores tag (classification) data in two systems that can drift apart:

1. **Cassandra `tags.tags_by_id`** — source of truth for tag ownership (direct and propagated)
2. **Elasticsearch denormalized fields** — derived view used for search and UI display

Five ES fields are derived from Cassandra tag state:

| ES Field | Source |
|----------|--------|
| `__traitNames` | Direct tag type names (list) |
| `__classificationNames` | Direct tag type names (pipe-delimited string) |
| `__propagatedTraitNames` | Propagated tag type names (list) |
| `__propagatedClassificationNames` | Propagated tag type names (pipe-delimited string) |
| `__classificationsText` | All tag names concatenated for full-text search |

### How drift occurs

| Failure mode | Effect |
|-------------|--------|
| ES write fails silently during tag propagation | Asset shows stale tags in search/UI |
| Propagation cleanup task fails or is lost | Orphaned propagated rows in Cassandra; ES shows tags the source no longer has |
| Partial ES bulk write | Some assets in a propagation batch get updated, others don't |
| Race condition between concurrent tag operations | ES reflects an intermediate state |
| Pod restart during propagation task | Task may be retried but ES state is already partially written |

### Scale of the problem

On a production tenant (newsuk), a dry-run scan of one tag type found:
- 7,000 assets checked (of 15,309 with the tag)
- **5,824 assets (83%) had orphaned propagated tags**
- 20,254 orphaned propagated rows total
- 572 unique source vertices had lost their direct tag without cleaning up downstream propagations

## Architecture

```
                        ┌──────────────────────────┐
                        │  K8s Seed Job (one-time)  │
                        │  cqlsh COPY TO/FROM       │
                        └────────────┬─────────────┘
                                     │ populates
                                     ▼
┌─────────────────┐          ┌──────────────────┐          ┌───────────────┐
│ Tag Write Paths │──INSERT──│ tags.tag_change_  │──READ────│ TagReconcilia │
│ (EntityGraph    │          │ log              │          │ tionService   │
│  Mapper)        │          │                  │          │ (@Scheduled)  │
└─────────────────┘          │ day_bucket (PK)  │          └───────┬───────┘
                             │ created_at (CK)  │                  │
                             │ vertex_id  (CK)  │                  │ for each vertex:
                             │ tag_type_name    │                  │
                             │ operation        │                  ▼
                             │ TTL: 7 days      │      ┌──────────────────────┐
                             └──────────────────┘      │ 1. Read all tags     │
                                                       │    from C* (truth)   │
                                                       │ 2. Detect orphaned   │
                                                       │    propagated tags   │
                                                       │ 3. Clean orphans     │
                                                       │    (C* soft-delete)  │
                                                       │ 4. Compute 5 denorm  │
                                                       │    fields            │
                                                       │ 5. Write full ES     │
                                                       │    snapshot          │
                                                       └──────────────────────┘
```

### Three components

**1. `tags.tag_change_log` table** — a Cassandra work queue of vertex IDs that need reconciliation.

```sql
CREATE TABLE tags.tag_change_log (
    day_bucket text,        -- partition key, e.g. '2026-03-31'
    created_at timestamp,   -- clustering key, write time
    vertex_id text,         -- clustering key, the asset vertex ID
    tag_type_name text,     -- which tag changed (for debugging)
    operation text,         -- ADD, DELETE, PROPAGATE_ADD, etc. (for debugging)
    PRIMARY KEY ((day_bucket), created_at, vertex_id)
) WITH CLUSTERING ORDER BY (created_at ASC, vertex_id ASC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy', ...}
  AND default_time_to_live = 604800;  -- 7 days
```

**2. Seed Job** — a K8s Job that populates the change log from existing `tags_by_id` using native cqlsh `COPY TO/FROM`. Runs once per tenant.

**3. `TagReconciliationService`** — a Spring `@Scheduled` bean inside Atlas that drains the change log. Runs within a configurable nightly window. Single-pod execution via Redis lock.

## Why a change log (not a full table scan)

| Approach | Pros | Cons |
|----------|------|------|
| Full scan of `tags_by_id` every cycle | Simple, no extra table | Slow for large tenants (millions of vertices); re-reads unchanged data; race condition with live writes |
| Change log | Only processes what changed; no race (reads C* after the write); self-cleaning via TTL | Requires instrumenting tag write paths; extra INSERT per tag operation |

The change log solves the race condition (#10 in the tradeoffs analysis): the log entry is written AFTER the Cassandra tag write. When the reconciler reads the entry and then reads Cassandra, it sees the committed state. If another write happens concurrently, that write adds its own log entry — the reconciler catches it in the next cycle.

## How it works

### Initial backfill (one-time per tenant)

```bash
# 1. Run the seed job
kubectl apply -f tools/tag-reconciler-seed-job.yaml -n atlas

# 2. Enable the reconciler
cqlsh $HOST -e "INSERT INTO config_store.configs
  (app_name, config_key, config_value, updated_by, last_updated)
  VALUES ('atlas', 'TAG_RECONCILER_ENABLED', 'true', 'admin', toTimestamp(now()));"
```

The seed job:
1. Exports all vertex IDs from `tags.tags_by_id` using `cqlsh COPY TO`
2. Deduplicates with `sort -u`
3. Loads into `tag_change_log` using `cqlsh COPY FROM`

For a tenant with 3.2M rows / 1M unique vertices, this takes ~2 minutes.

### Steady-state (after initial backfill)

1. Every 15 minutes, the reconciler checks: enabled? within window? can acquire Redis lock?
2. If yes, it reads the change log from its last saved position (day_bucket + timestamp + vertex_id)
3. For each vertex in the page:
   - Reads all tags from `tags_by_id` (direct and propagated, active only)
   - For each propagated tag, checks if the source vertex still has an active direct tag
   - If source has no direct tag: **orphan** — soft-deletes the propagated row from `tags_by_id`, hard-deletes from `propagated_tags_by_source`
   - Computes the 5 denorm fields from surviving tags
   - Writes full snapshot to ES via `ESConnector.writeTagProperties()`
4. Saves cursor position to `config_store.configs`
5. Stops when the window expires or the log is exhausted
6. Next night, resumes from saved position

### Forward path (future PR)

Every tag write operation (add, delete, propagate) inserts the affected vertex ID into the change log:

```java
tagDAO.logTagChange(vertexId, tagTypeName, "ADD");
```

This is a single Cassandra INSERT — best-effort, non-blocking. If it fails, the change is not logged and will only be caught on the next full backfill (or if the vertex is touched again).

## Orphan detection

A propagated tag is orphaned when the source vertex no longer has an active direct tag of that type. This happens when:

1. Direct tag is removed from source entity
2. Propagation cleanup task runs but fails partway through
3. Downstream assets retain stale propagated rows

The reconciler detects orphans by checking each propagated tag's source:

```
For each tag on vertex V:
  if tag.sourceVertexId != V (propagated):
    check: does sourceVertexId have active direct tag of this type?
    if no: orphan — delete from tags_by_id and propagated_tags_by_source
    if yes: legitimate propagation — include in denorm
```

A source cache (`sourceVertexId|tagTypeName -> boolean`) avoids repeated lookups for the same source across vertices in a page.

## Configuration

All configuration is per-tenant via `config_store.configs` or JVM properties.

### Dynamic config (per-tenant, no redeploy)

| Key | Default | Description |
|-----|---------|-------------|
| `TAG_RECONCILER_ENABLED` | `false` | Master switch. Set to `true` to enable. |
| `TAG_RECONCILER_STATE` | `null` | JSON cursor state. Managed by the reconciler. Do not edit manually unless resetting. |

### JVM properties (set in Atlas container env or atlas-application.properties)

| Property | Default | Description |
|----------|---------|-------------|
| `atlas.tag.reconciler.poll.interval` | `900000` (15 min) | How often the scheduler ticks to check if it should run |
| `atlas.tag.reconciler.window.start.hour` | `0` (midnight UTC) | Window start hour (0-23, UTC) |
| `atlas.tag.reconciler.window.duration.hours` | `4` | Window duration in hours |
| `atlas.tag.reconciler.page.size` | `500` | Change log entries per page |
| `atlas.tag.reconciler.es.batch.size` | `50` | ES bulk write batch size |

### Resetting state

To restart reconciliation from the beginning (e.g., after a re-seed):

```sql
DELETE FROM config_store.configs
WHERE app_name = 'atlas' AND config_key = 'TAG_RECONCILER_STATE';
```

### Disabling

```sql
UPDATE config_store.configs
SET config_value = 'false', updated_by = 'admin', last_updated = toTimestamp(now())
WHERE app_name = 'atlas' AND config_key = 'TAG_RECONCILER_ENABLED';
```

Takes effect within 15 minutes (next tick).

## Tradeoffs and known limitations

### 1. No CDC/audit/notification for orphan cleanup

The reconciler deletes orphaned tags directly via `tagDAO.deleteTags()` and writes ES directly via `ESConnector.writeTagProperties()`. This bypasses:
- Entity audit entries
- Kafka CDC notifications
- Downstream consumer updates

This is intentional — the reconciler is a **data repair** tool, not a business operation. The orphaned data should never have existed. Adding audit/notification would create false signals.

### 2. Forward path not yet instrumented

Until every tag write path in `EntityGraphMapper` is instrumented with `tagDAO.logTagChange()`, the change log only captures what the seed job populated. Any tag changes after the seed but before instrumentation will be invisible until the next re-seed.

Planned for a follow-up PR. Tag write paths to instrument:
- `addClassificationsV2` / `deleteClassificationV2` / `updateClassificationsV2`
- `deleteClassificationPropagationV2`
- `classificationRefreshPropagationV2_new`
- `addTagPropagationV2`

### 3. Duplicate processing

The same vertex ID may appear multiple times in the change log (e.g., multiple tag operations in one day). The reconciler deduplicates within a page but not across pages. Processing the same vertex twice is harmless (idempotent ES write) but wastes a Cassandra read.

### 4. N+1 Cassandra reads

For each vertex, the reconciler makes:
- 1 call to `getAllTagsByVertexId` (all tags for the vertex)
- 1 call per unique propagated source (orphan check) — cached across vertices in the page

For vertices with many propagated tags from many unique sources, this can be slow. The source cache mitigates this for common sources.

### 5. TTL tombstones

The 7-day TTL on `tag_change_log` creates Cassandra tombstones when entries expire. The `TimeWindowCompactionStrategy` is chosen to compact tombstones efficiently (one SSTable per day window, old windows compact away cleanly).

### 6. Window-based execution

The reconciler only runs during the configured window (default: midnight–4am UTC). Changes during the day are not reconciled until the next window. This is by design — the nightly window minimizes impact on production traffic.

For urgent reconciliation, temporarily change the window:
```
-Datlas.tag.reconciler.window.start.hour=10 -Datlas.tag.reconciler.window.duration.hours=24
```

### 7. Single-threaded, single-pod

Only one pod holds the Redis lock and processes the change log. No parallelism within a cycle. For large backlogs, this means the initial backfill takes multiple nightly windows to complete.

At ~500 vertices/page and ~2 CQL calls/vertex at ~2ms each:
- 1M vertices ≈ 2 hours (fits in one 4-hour window)
- 10M vertices ≈ 20 hours (spans ~5 nightly windows)

## Monitoring

### Logs

```bash
# Follow reconciler activity
kubectl logs -f atlas-0 -c atlas-main -n atlas | grep TagReconciler
```

Key log lines:
- `TagReconciler: acquired lock, starting cycle` — cycle started
- `TagReconciler: cycle done — entries=X, vertices=Y, orphans=Z, elapsed=Nms` — cycle summary
- `TagReconciler: cleaned orphan tag X on vertex Y from source Z` — individual orphan cleanup
- `TagReconciler: change log is empty, nothing to do` — fully caught up

### State inspection

```sql
-- Current cursor position
SELECT config_value FROM config_store.configs
WHERE app_name = 'atlas' AND config_key = 'TAG_RECONCILER_STATE';

-- Change log size per day
SELECT day_bucket, count(*) FROM tags.tag_change_log GROUP BY day_bucket;
```

## Deployment runbook

### First-time setup on a tenant

1. **Deploy Atlas** with the reconciler code (creates the `tag_change_log` table on startup)

2. **Run the seed job:**
   ```bash
   kubectl apply -f tools/tag-reconciler-seed-job.yaml -n atlas
   kubectl logs -f job/tag-reconciler-seed -n atlas
   ```

3. **Enable the reconciler:**
   ```bash
   kubectl exec -n atlas atlas-cassandra-0 -c atlas-cassandra -- cqlsh localhost -e \
     "INSERT INTO config_store.configs (app_name, config_key, config_value, updated_by, last_updated) \
      VALUES ('atlas', 'TAG_RECONCILER_ENABLED', 'true', 'admin', toTimestamp(now()));"
   ```

4. **Monitor** the next nightly window for reconciler activity in logs.

5. **Clean up** the seed job:
   ```bash
   kubectl delete job tag-reconciler-seed -n atlas
   ```

### Re-seeding (if needed)

If the forward path is not yet instrumented and you want to catch up on changes since the last seed:

1. Disable the reconciler
2. Reset state: `DELETE FROM config_store.configs WHERE app_name = 'atlas' AND config_key = 'TAG_RECONCILER_STATE';`
3. Run the seed job again
4. Re-enable the reconciler
