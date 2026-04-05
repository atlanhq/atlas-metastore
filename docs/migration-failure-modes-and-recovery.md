# ZeroGraph Migrator: Failure Modes, Recovery & Rollback Guide

**Comprehensive reference for operators — what can go wrong, how to detect it, and how to fix it.**

> **Companion docs:**
> - [Tenant Migration Runbook](tenant-migration-runbook.md) — step-by-step migration guide
> - [Tenant Migration Operations](tenant-migration-operations.md) — field-tested procedures

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Migration Capabilities](#2-migration-capabilities)
3. [Migration Scripts Reference](#3-migration-scripts-reference)
4. [Failure Modes by Component](#4-failure-modes-by-component)
5. [Failure Modes by Migration Phase](#5-failure-modes-by-migration-phase)
6. [Post-Migration Discrepancies](#6-post-migration-discrepancies)
7. [Rollback & Revert Procedures](#7-rollback--revert-procedures)
8. [Troubleshooting Decision Tree](#8-troubleshooting-decision-tree)
9. [Environment Variables Reference](#9-environment-variables-reference)

---

## 1. Architecture Overview

The ZeroGraph migrator copies data from JanusGraph's binary `edgestore` table (in Cassandra) into a new human-readable Cassandra schema (`atlas_graph` keyspace) and re-indexes everything into Elasticsearch.

```
JanusGraph (Source)                    Cassandra + ES (Target)
┌─────────────────────┐               ┌──────────────────────────┐
│ atlas.edgestore      │──[Scanner]──>│ atlas_graph.vertices     │
│ (binary blobs)      │              │ atlas_graph.edges_out    │
│                     │              │ atlas_graph.edges_in     │
│                     │              │ atlas_graph.edges_by_id  │
│                     │              │ atlas_graph.vertex_index │
│                     │              │ atlas_graph.type_defs    │
└─────────────────────┘              │ ...9+ tables             │
                                     └──────────────────────────┘
┌─────────────────────┐               ┌──────────────────────────┐
│ janusgraph_vertex_  │──[Indexer]──>│ atlas_graph_vertex_index │
│ index (ES)          │              │ (ES)                     │
└─────────────────────┘              └──────────────────────────┘
```

**Key safety guarantee:** JanusGraph data is **never modified** — migration is copy-not-move. Rollback is always possible by flipping one config property.

### ID Strategy: Migration vs Runtime

The ID strategy has **two distinct phases** with different requirements:

| Phase | Property | Value | Why |
|-------|----------|-------|-----|
| **Migration** | `ID_STRATEGY` (env var) / `migration.id.strategy` | **`legacy`** (always) | Preserves original JanusGraph UUIDs for migrated data |
| **Runtime** (post-switch) | `atlas.graph.id.strategy` (in ConfigMap) | **`deterministic`** (always) | New entities get content-addressed SHA-256 IDs |

**Why this combination is correct:**
- During migration, `legacy` copies the original JanusGraph vertex IDs as-is. This ensures all existing references (GUIDs, edges, ES docs) remain consistent.
- After the backend switch, `deterministic` ensures new entities created at runtime get deterministic SHA-256 IDs based on `typeName + qualifiedName`. This enables idempotent entity creation and efficient edge lookups for new data.
- The two ID formats (UUID for migrated data, SHA-256 for new data) coexist safely in the same Cassandra tables. Vertex lookups are by primary key (`vertex_id`), which works regardless of ID format.

**Never migrate with `deterministic`.** The deterministic strategy during migration requires every vertex to have a `qualifiedName`. JanusGraph internal vertices (type definitions, index entries) may not have one, causing fallback to `hash-jg` which creates IDs that don't match runtime expectations.

### ES Alias Architecture

Atlas uses an **ES alias** (`atlas_vertex_index`) as the unified entry point for all index queries. The alias must point to the correct underlying index based on the active backend:

| Backend | Alias `atlas_vertex_index` points to | Persona aliases live on |
|---------|--------------------------------------|------------------------|
| JanusGraph | `janusgraph_vertex_index` | `janusgraph_vertex_index` |
| Cassandra | `atlas_graph_vertex_index` | `atlas_graph_vertex_index` |

**If the alias points to the wrong index:** Search works but returns data from the wrong backend. Persona aliases (filtered views per user role) won't match the active index, causing lock icons on assets or empty search results.

The ES index prefix is auto-derived from `atlas.graphdb.backend` at startup:
- `cassandra` → prefix `atlas_graph_` → index `atlas_graph_vertex_index`
- `janus` → prefix `janusgraph_` → index `janusgraph_vertex_index`

### Migration Phases

| Phase | Name | What It Does |
|-------|------|--------------|
| **1** | Scan + Write | CQL token-range scan of `edgestore`, decode binary, write to new Cassandra tables + parallel ES indexing |
| **1.5** | Auxiliary Keyspaces | Migrate `config_store` and `tags` tables (if on same cluster) |
| **2** | ES Reindex | Bulk reindex from new Cassandra tables to ES (skipped if Phase 1 did parallel ES) |
| **3** | Validation | 17+ checks comparing source vs target counts, sampling, cross-table integrity |

---

## 2. Migration Capabilities

### Modes

| Mode | Flag | Description |
|------|------|-------------|
| **Full migration** | (default) | Scan + Write + ES + Validate |
| **Fresh start** | `--fresh` | Clear all previous migration state, drop target data, start from scratch |
| **Resume** | (no flag) | Resume from where a previous run left off (using `migration_state` table) |
| **ES-only** | `--es-only` | Re-index ES from already-migrated Cassandra data (no CQL scan) |
| **Validate-only** | `--validate-only` | Run 17+ validation checks without migrating |
| **Analyze** | `--analyze` | Analyze source JanusGraph data (vertex/edge distribution, super vertices) |
| **Dry run** | `--dry-run` | Generate config and check connectivity without migrating |

### Auto-Sizing

The migrator probes ES for vertex count and auto-scales thread pools:

| Vertices | Tier | Scanner Threads | Writer Threads | ES Bulk Size | JVM Heap |
|----------|------|-----------------|----------------|--------------|----------|
| < 500K | SMALL | cores | cores | 5,000 | 2g |
| 500K – 5M | MEDIUM | cores × 2 | cores × 2 | 5,000 | 4g |
| 5M – 20M | LARGE | cores × 2 | cores × 2 | 10,000 | 10g |
| > 20M | XLARGE | cores × 2 | cores × 2 | 10,000 | 48g |

Auto-sizing is overridden by explicit env vars (e.g., `SCANNER_THREADS=32`).

### Resume Support

The `migration_state` table in Cassandra tracks token ranges with states: `PENDING`, `IN_PROGRESS`, `COMPLETED`, `FAILED`. On re-run:
- `COMPLETED` ranges are skipped
- `FAILED` and `IN_PROGRESS` ranges are retried
- New ranges (from config changes) are scanned

### Validation Checks (Phase 3)

| # | Check | Severity | What It Detects |
|---|-------|----------|-----------------|
| 1 | Vertex count | FAIL if < 95% of source | Missing vertices |
| 2 | Edge out/in consistency | FAIL if mismatch | edges_out vs edges_in count mismatch |
| 3 | Edge by ID consistency | WARN | edges_by_id vs edges_out mismatch |
| 4 | GUID index sample | FAIL if < 95% found | Missing GUID index entries |
| 5 | TypeDef cross-table | FAIL if mismatch | type_definitions vs type_definitions_by_category |
| 6 | Deep vertex correctness | FAIL | Sampled vertices with corrupt properties |
| 7 | Cross-table integrity | FAIL | Vertex in edges_out but not in vertices table |
| 8 | Property corruption | WARN | Non-parseable JSON in vertex properties |
| 9 | ES doc count | FAIL if < 90% | Missing ES documents |
| 10 | Orphan edges | WARN | Edges pointing to non-existent vertices |
| 11 | Super vertex detection | INFO | Vertices with > 10K edges (performance risk) |
| 12 | Edge index check | WARN | Missing edge_index entries |
| 13 | ES edge count | WARN | Edge ES docs vs edges_by_id mismatch |
| 14 | Config store check | WARN | config_store table migration status |
| 15 | Tags check | WARN | Tags table migration status |
| 16 | Token range completion | FAIL if incomplete | Unfinished token ranges in migration_state |
| 17 | Type comparison | WARN | TypeDef differences between source and target |

---

## 3. Migration Scripts Reference

| Script | Type | Use Case | Key Feature |
|--------|------|----------|-------------|
| `atlas_migrate.sh` | Inner launcher | Runs inside pod, reads atlas-application.properties | Simple, no orchestration |
| `atlas_migrate_dev.sh` | Dev orchestrator | Development/testing, uploads local JAR | Thin JAR upload, auto-sizing, pod cleanup |
| `atlas_migrate_tenant.sh` | Production orchestrator | Scheduled production migrations | 8 phases, maintenance mode, retry, reporting |
| `atlas_migrate_and_switch.sh` | Simplified orchestrator | One-command production migration | Token generation, guaranteed maintenance OFF |
| `manage_es_aliases.sh` | Utility | Post-migration ES alias management | Persona alias migration, dry-run |
| `revert_es_aliases.sh` | Utility | Rollback ES aliases to JanusGraph | Inverse of manage_es_aliases.sh, dry-run |
| `verify_backup.sh` | Utility | Pre-migration backup verification | Checks Temporal backup schedule |

### Script Selection Guide

- **Development/testing:** `atlas_migrate_dev.sh` — can upload local JAR, auto-cleanup
- **Production (full control):** `atlas_migrate_tenant.sh` — maintenance mode, disk checks, retry
- **Production (simple):** `atlas_migrate_and_switch.sh` — one command, guaranteed maintenance OFF
- **Post-migration aliases:** `manage_es_aliases.sh` — switch ES alias + migrate persona aliases
- **Rollback aliases:** `revert_es_aliases.sh` — revert ES aliases back to JanusGraph index

### Exit Codes (Orchestrator Scripts)

| Code | Meaning | Recovery |
|------|---------|----------|
| 0 | Success | N/A |
| 1 | Pre-flight failure | Fix prerequisites (kubectl, pod health, connectivity) |
| 2 | Cleanup failure | Manual cleanup (drop keyspace, delete ES index) |
| 3 | Migration failure | Check migrator logs, retry with `--max-retries` |
| 4 | Backend switch failure | Restore ConfigMap backup |
| 5 | Verification failure | Migration data exists but post-checks failed; manual review |

---

## 4. Failure Modes by Component

### 4.1 JanusGraphScanner (Source CQL Scan)

| Failure | Retryable | Data Impact | Detection | Recovery |
|---------|-----------|-------------|-----------|----------|
| **JanusGraph schema load fail** | No | None (no data written) | Stack trace at startup | Fix config (Cassandra host, credentials), re-run |
| **Token range CQL timeout** | Yes | Range marked FAILED, no partial data | ERROR log + `migration_state` entry | Re-run without `--fresh` (auto-resumes) |
| **Edge decode error** | No (entry skipped) | Some edges missing from vertex | WARN log (first 10 only, then TRACE) | Check `decode_errors` metric; < 0.01% is typically safe |
| **Super vertex chunking** | N/A | Large vertices split into chunks | INFO log | Normal behavior, no action needed |

**Silent risk:** After the first 10 decode errors, subsequent errors are only logged at TRACE level. The `decode_errors` metric counter is always accurate — check it in the final summary.

### 4.2 CassandraTargetWriter (Target Writes)

| Failure | Retryable | Data Impact | Detection | Recovery |
|---------|-----------|-------------|-----------|----------|
| **Schema creation fail** | No | None (halts migration) | Stack trace | Fix Cassandra connectivity/replication, re-run |
| **Queue full + interrupt** | No | Vertex/edges lost | RuntimeException in scanner | Increase `QUEUE_CAPACITY`, re-run with `--fresh` |
| **Async write fail** | Yes (exponential backoff) | **Partial data in Cassandra** | ERROR log (first 10, then silent) | Check `write_errors` metric; re-run with `--fresh` if > 0 |
| **Batch size exceeded** | Yes (auto-unbatch) | None (handled) | WARN log for large vertices | Normal behavior for > 50KB vertices |
| **Claim ID conflict** | No (fallback) | Possible duplicate IDs | DEBUG log | Only relevant with `CLAIM_ENABLED=true` |

**Critical risk: Partial data corruption.** Async write failures leave partial data committed to Cassandra. There is no atomic rollback per vertex. If `write_errors > 0`, strongly consider re-running with `--fresh`.

### 4.3 ElasticsearchReindexer / ParallelEsIndexer

| Failure | Retryable | Data Impact | Detection | Recovery |
|---------|-----------|-------------|-----------|----------|
| **ES connection fail** | No | No docs indexed | Stack trace | Fix ES connectivity, re-run |
| **Index creation fail** | No (fallback to template) | Index may not exist | WARN log | Manually create index, re-run with `--es-only` |
| **Bulk item failure** | No | **Partial docs in ES** | `totalBulkItemFailures` counter | Re-run with `--es-only` |
| **Queue overflow (parallel)** | No | Docs lost | ERROR log | Increase `QUEUE_CAPACITY` or reduce `SCANNER_THREADS` |
| **Field mapping conflict** | No (silent transform) | Dot-fields converted to underscore | No visible log | Normal behavior |

**Recovery pattern for ES issues:** ES can always be rebuilt from Cassandra data using `--es-only` mode. This is the safest recovery for any ES inconsistency.

### 4.4 MigrationValidator

| Failure | Impact | Recovery |
|---------|--------|----------|
| **Validation check FAIL** | Migration exit code 1 | Review check details, re-run with `--fresh` or fix specific issue |
| **Report file write fail** | Report printed to stdout instead | Non-critical; capture from console output |
| **Cassandra query timeout during validation** | Check shows WARN/FAIL | Increase read timeout, re-run `--validate-only` |

### 4.5 AuxiliaryKeyspaceMigrator (Config Store & Tags)

| Failure | Impact | Recovery |
|---------|--------|----------|
| **Cross-cluster detection wrong** | Skips migration silently | Verify `atlas.cassandra.graph.hostname` matches source Cassandra |
| **Keyspace creation fail** | Halts migration | Fix replication factor vs node count |
| **Batch write fail** | Partial config_store data | Re-run (idempotent writes) |
| **Tags migration fail** | Tags missing post-switch | Re-run (token range resume supported) |

### 4.6 MigrationSizer (Auto-Sizing)

| Failure | Impact | Recovery |
|---------|--------|----------|
| **ES query fail** | Uses default config (may be undersized) | Set threads/heap explicitly via env vars |
| **Wrong tier classification** | Over/under-provisioned threads | Override with explicit `SCANNER_THREADS`/`WRITER_THREADS` |

---

## 5. Failure Modes by Migration Phase

### Phase 0: Pre-flight

| Symptom | Cause | Fix |
|---------|-------|-----|
| `atlas_migrate.sh: not found` | Build doesn't include migrator | Deploy from `switchable-graph-provider` branch |
| `Migrator JAR not found` | JAR not in expected location | Check `/opt/apache-atlas/tools/atlas-graphdb-migrator.jar` |
| `Cannot reach Cassandra` | Network/auth issue | Verify: `cqlsh <host> <port>` from within pod |
| `Cannot reach ES` | Network issue | Verify: `curl localhost:9200` from within pod |
| `Backup verification failed` | No recent backup | Trigger backup or set `SKIP_BACKUP_CHECK=true` (dev only) |

### Phase 1: Scan + Write

| Symptom | Cause | Fix |
|---------|-------|-----|
| OOM / `java.lang.OutOfMemoryError` | JVM heap too small for estate size | Increase `MIGRATOR_JVM_HEAP` (e.g., `24g` for 10M+ vertices) |
| `ReadTimeoutException` on source | Cassandra overloaded or slow | Reduce `SCANNER_THREADS`, increase `SOURCE_CONSISTENCY=ONE` |
| `WriteTimeoutException` on target | Cassandra write overloaded | Reduce `WRITER_THREADS`, ensure `TARGET_CONSISTENCY=LOCAL_QUORUM` |
| Progress stalls at 0 vertices/s | Queue full, writer blocked | Reduce `SCANNER_THREADS` or increase `QUEUE_CAPACITY` |
| `write_errors > 0` in summary | Async writes failed after retries | Re-run with `--fresh` (partial data is unsafe) |
| `decode_errors > 0` in summary | Some JanusGraph rows unreadable | Check log for vertex IDs; < 0.01% usually safe |

### Phase 1.5: Auxiliary Keyspaces

| Symptom | Cause | Fix |
|---------|-------|-----|
| `config_store migration skipped` | Different Cassandra cluster detected | Set `atlas.cassandra.graph.hostname` to match source |
| `ReadTimeoutException on tags_by_id` | Tags table large or slow | Non-critical; classifications still work via main migration |

### Phase 2: ES Reindex

| Symptom | Cause | Fix |
|---------|-------|-----|
| `Limit of total fields [5000] exceeded` | Too many unique attribute keys | Increase: `curl -X PUT ES/_settings -d '{"index.mapping.total_fields.limit": 10000}'` |
| Bulk failures in ES | ES cluster overloaded | Reduce `ES_BULK_SIZE`, retry with `--es-only` |
| Zero ES docs indexed | ES index creation failed | Check ES logs, create index manually, retry `--es-only` |

### Phase 3: Validation

| Symptom | Cause | Fix |
|---------|-------|-----|
| `Vertex count: FAIL (target < 95%)` | Scan didn't complete or write errors | Check `migration_state` for FAILED ranges; re-run |
| `Edge consistency: FAIL` | edges_out vs edges_in mismatch | Re-run with `--fresh` |
| `GUID index: FAIL (< 95% found)` | vertex_index not fully populated | Re-run with `--fresh` |
| `ES doc count: FAIL (< 90%)` | ES indexing had failures | Re-run with `--es-only` |
| `Token range completion: FAIL` | Some ranges still FAILED/IN_PROGRESS | Re-run without `--fresh` to resume failed ranges |
| `TypeDef comparison: WARN` | Source/target typedef mismatch | Usually safe; check for critical missing types |

---

## 6. Post-Migration Discrepancies

These issues may appear **after** a successful migration and backend switch.

### 6.1 Search Returns Empty Results

**Cause:** ES alias not pointing to new index, or ES reindex incomplete.

**Diagnosis:**
```bash
# Check which index the alias points to
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  curl -s "localhost:9200/_alias/atlas_vertex_index" | python -m json.tool

# Check doc count in new index
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  curl -s "localhost:9200/atlas_graph_vertex_index/_count"
```

**Fix:**
```bash
# Option A: Switch alias to new index
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  curl -s -X POST "localhost:9200/_aliases" \
  -H "Content-Type: application/json" \
  -d '{"actions":[{"remove":{"index":"*","alias":"atlas_vertex_index"}},{"add":{"index":"atlas_graph_vertex_index","alias":"atlas_vertex_index"}}]}'

# Option B: Rebuild ES from Cassandra data
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  /opt/apache-atlas/bin/atlas_migrate.sh --es-only
```

### 6.2 Missing Entities (Specific Vertices Not Found)

**Cause:** Decode errors during scan, or vertex skipped due to edge-only row.

**Diagnosis:**
```bash
# Check if vertex exists in Cassandra
kubectl exec atlas-cassandra-0 -n atlas -- \
  cqlsh -e "SELECT vertex_id FROM atlas_graph.vertices WHERE vertex_id = '<guid>' LIMIT 1;"

# Check if vertex exists in ES
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  curl -s "localhost:9200/atlas_graph_vertex_index/_doc/<guid>"
```

**Fix:** If vertex is in Cassandra but not ES: `--es-only`. If vertex is not in Cassandra: re-run full migration with `--fresh`.

### 6.3 Lock Icons on Assets (Authorization Issues)

**Cause:** Persona/Purpose aliases are on the wrong ES index. This happens when:
- **(Forward)** Switched to Cassandra but `manage_es_aliases.sh` not run — persona aliases still on `janusgraph_vertex_index`
- **(After rollback)** Reverted to JanusGraph but persona aliases still on `atlas_graph_vertex_index`

**Diagnosis:**
```bash
# Check which index the main alias points to
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  curl -s "localhost:9200/_alias/atlas_vertex_index" | python -m json.tool

# Check persona aliases on both indexes
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  curl -s "localhost:9200/atlas_graph_vertex_index/_alias" | python -m json.tool

kubectl exec atlas-0 -n atlas -c atlas-main -- \
  curl -s "localhost:9200/janusgraph_vertex_index/_alias" | python -m json.tool
```

**Fix:**
```bash
# If running Cassandra backend (forward case): migrate aliases forward
./tools/manage_es_aliases.sh --vcluster <tenant-name>

# If reverted to JanusGraph (rollback case): revert aliases back
./tools/revert_es_aliases.sh --vcluster <tenant-name>
```

### 6.4 Lineage Not Working

**Cause:** edges_out/edges_in tables empty or incomplete.

**Diagnosis:**
```bash
kubectl exec atlas-cassandra-0 -n atlas -- \
  cqlsh -e "SELECT COUNT(*) FROM atlas_graph.edges_out LIMIT 1000000;"

kubectl exec atlas-cassandra-0 -n atlas -- \
  cqlsh -e "SELECT COUNT(*) FROM atlas_graph.edges_in LIMIT 1000000;"
```

**Fix:** If counts are zero or very low: re-run migration with `--fresh`.

### 6.5 TypeDefs Missing or Stale

**Cause:** TypeDef migration partial, or `type_definitions` vs `type_definitions_by_category` mismatch.

**Diagnosis:**
```bash
kubectl exec atlas-cassandra-0 -n atlas -- \
  cqlsh -e "SELECT COUNT(*) FROM atlas_graph.type_definitions;"

kubectl exec atlas-cassandra-0 -n atlas -- \
  cqlsh -e "SELECT COUNT(*) FROM atlas_graph.type_definitions_by_category;"
```

**Fix:** Re-run migration with `--fresh` (TypeDefs are migrated in Phase 1).

### 6.6 ES/Cassandra Count Mismatch

**Cause:** ES bulk failures during parallel indexing, or ES index was not refreshed.

**Diagnosis:**
```bash
# Cassandra vertex count
kubectl exec atlas-cassandra-0 -n atlas -- \
  cqlsh -e "SELECT COUNT(*) FROM atlas_graph.vertices LIMIT 10000000;"

# ES doc count
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  curl -s "localhost:9200/atlas_graph_vertex_index/_count"
```

**Fix:**
```bash
# Force ES refresh
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  curl -s -X POST "localhost:9200/atlas_graph_vertex_index/_refresh"

# If still mismatched, rebuild ES
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  /opt/apache-atlas/bin/atlas_migrate.sh --es-only
```

### 6.7 Slow Startup After Switch

**Expected behavior:** Cassandra backend startup should be ~50% faster (~22s vs ~45s). If slower:

**Cause:** ES index verification retrying (ES not ready), or Cassandra schema verification slow.

**Diagnosis:**
```bash
kubectl logs atlas-0 -n atlas -c atlas-main --tail=50 | grep -i "retry\|timeout\|slow"
```

### 6.8 Data Drift (Stale Data Post-Migration)

**Cause:** Writes occurred between migration start and backend switch.

**Prevention:** Use `--maintenance-mode` flag to block writes during migration:
```bash
./tools/atlas_migrate_tenant.sh --vcluster <name> --fresh --maintenance-mode
```

**Fix if already happened:** Re-run migration with `--fresh` while in maintenance mode, then switch.

---

## 7. Rollback & Revert Procedures

### 7.1 Instant Rollback (Backend Switch Only)

**When:** Migration + backend switch completed, but issues found. JanusGraph data is intact.

**Time:** ~30 seconds (config change + pod restart).

```bash
# Step 1: Restore ConfigMap backup (saved during Phase 4)
kubectl apply -f /tmp/atlas-config-backup-<TIMESTAMP>.yaml

# Step 2: Restart pod
kubectl delete pod atlas-0 -n atlas
kubectl wait --for=condition=Ready pod/atlas-0 -n atlas --timeout=300s

# Step 3: Verify
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  curl -s localhost:21000/api/atlas/admin/status
# Expected: {"Status":"ACTIVE"}
```

### 7.2 Manual Rollback (If Backup Lost)

```bash
# Step 1: Extract current config
kubectl get configmap atlas-config -n atlas \
  -o jsonpath='{.data.atlas-application\.properties}' > /tmp/atlas-props.txt

# Step 2: Remove Cassandra backend properties
sed -i '' '/^atlas.graphdb.backend=/d' /tmp/atlas-props.txt
sed -i '' '/^atlas.graph.index.search.es.prefix=/d' /tmp/atlas-props.txt
sed -i '' '/^atlas.cassandra.graph\./d' /tmp/atlas-props.txt
sed -i '' '/^atlas.graph.id.strategy=/d' /tmp/atlas-props.txt
sed -i '' '/^atlas.graph.claim.enabled=/d' /tmp/atlas-props.txt
sed -i '' '/^# === Zero Graph/d' /tmp/atlas-props.txt

# Step 3: Set backend back to JanusGraph
echo "atlas.graphdb.backend=janus" >> /tmp/atlas-props.txt

# Step 4: Apply and restart
kubectl create configmap atlas-config -n atlas \
  --from-file=atlas-application.properties=/tmp/atlas-props.txt \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl delete pod atlas-0 -n atlas
kubectl wait --for=condition=Ready pod/atlas-0 -n atlas --timeout=300s
```

### 7.3 ES Alias Rollback

If ES aliases were switched to the new index (via `manage_es_aliases.sh`), revert them using the rollback script:

```bash
# Dry-run first to preview changes
./tools/revert_es_aliases.sh --vcluster <tenant-name> --dry-run

# Execute the revert
./tools/revert_es_aliases.sh --vcluster <tenant-name>
```

The script performs three operations:
1. **Revert vertex alias:** `atlas_vertex_index` → `janusgraph_vertex_index` (atomic swap)
2. **Move persona aliases:** From `atlas_graph_vertex_index` back to `janusgraph_vertex_index` (preserving filters)
3. **Clean up UUID aliases:** Remove `atlas_graph_` prefixed aliases from `atlas_graph_vertex_index`

> **Manual fallback** (if the script is unavailable):
> ```bash
> kubectl exec atlas-0 -n atlas -c atlas-main -- \
>   curl -s -X POST "localhost:9200/_aliases" \
>   -H "Content-Type: application/json" \
>   -d '{"actions":[{"remove":{"index":"*","alias":"atlas_vertex_index"}},{"add":{"index":"janusgraph_vertex_index","alias":"atlas_vertex_index"}}]}'
> ```
> Note: Manual fallback only reverts the main alias. Persona aliases must be moved individually.

### 7.4 Stage-Specific Alias State & Recovery

ES alias state varies depending on **where in the migration lifecycle** a failure occurs. This determines what recovery actions are needed:

| Failure Point | `atlas_vertex_index` Points To | Persona Aliases On | UUID Aliases | Recovery Actions |
|---------------|-------------------------------|-------------------|--------------|-----------------|
| **Phase 1–3 fails** (before any alias work) | `janusgraph_vertex_index` | `janusgraph_vertex_index` | Not copied | **None needed** — aliases untouched. Just fix and re-run migration. |
| **Backend switch fails** (ConfigMap not applied) | `janusgraph_vertex_index` | `janusgraph_vertex_index` | Not copied | **None needed** — still running JanusGraph. |
| **Backend switch succeeds, `manage_es_aliases.sh` not run** | `janusgraph_vertex_index` | `janusgraph_vertex_index` | Not copied | Rollback: restore ConfigMap, restart pod. No alias work needed. |
| **`manage_es_aliases.sh` partially run** (main alias switched, persona aliases not moved) | `atlas_graph_vertex_index` | `janusgraph_vertex_index` | May be partially copied | Rollback: `./tools/revert_es_aliases.sh --vcluster <name>` + restore ConfigMap + restart. |
| **`manage_es_aliases.sh` completed** (all aliases migrated) | `atlas_graph_vertex_index` | `atlas_graph_vertex_index` | Copied to `atlas_graph_vertex_index` | Rollback: `./tools/revert_es_aliases.sh --vcluster <name>` + restore ConfigMap + restart. |
| **`copy_vertex_uuid_aliases_to_edge.sh` run** | `atlas_graph_vertex_index` | `atlas_graph_vertex_index` | `atlas_graph_` prefixed on `atlas_graph_vertex_index` | Rollback: `./tools/revert_es_aliases.sh --vcluster <name>` (handles all three) + restore ConfigMap + restart. |

**Key principle:** If you haven't run `manage_es_aliases.sh`, no alias recovery is needed — just restore the ConfigMap and restart. If you have, use `revert_es_aliases.sh` before or after the ConfigMap restore.

**Order of rollback operations:**
1. Run `revert_es_aliases.sh` (if aliases were switched)
2. Restore ConfigMap backup (`atlas.graphdb.backend=janus`)
3. Restart pod (`kubectl delete pod atlas-0 -n atlas`)
4. Verify: search works, no lock icons, lineage intact

### 7.5 Maintenance Mode Emergency Disable

If the script exits without disabling maintenance mode:

```bash
# Method 1: Via Atlas REST API (if pod is ACTIVE)
TOKEN=$(kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s \
  -d "grant_type=client_credentials&client_id=<client>&client_secret=<secret>" \
  "<keycloak-url>/realms/default/protocol/openid-connect/token" \
  | python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

kubectl exec atlas-0 -n atlas -c atlas-main -- \
  curl -s -X PUT -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${TOKEN}" \
  -d '{"value": "false"}' \
  "localhost:21000/api/meta/configs/MAINTENANCE_MODE"

# Method 2: Direct Cassandra update (if pod is down)
kubectl exec atlas-cassandra-0 -n atlas -- \
  cqlsh -e "UPDATE config_store.configs SET value = 'false' WHERE key = 'MAINTENANCE_MODE';"
```

### 7.6 Full Cleanup (Remove All Migration Data)

To completely remove migration artifacts and start fresh:

```bash
# 1. Ensure backend is JanusGraph (do rollback first if needed)

# 2. Drop target keyspace
kubectl exec atlas-cassandra-0 -n atlas -- \
  cqlsh -e "DROP KEYSPACE IF EXISTS atlas_graph;"

# 3. Delete target ES index
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  curl -s -X DELETE "localhost:9200/atlas_graph_vertex_index"

# 4. Delete edge ES index (if exists)
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  curl -s -X DELETE "localhost:9200/atlas_graph_edge_index"

# 5. Verify cleanup
kubectl exec atlas-cassandra-0 -n atlas -- cqlsh -e "DESCRIBE KEYSPACES;"
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  curl -s "localhost:9200/_cat/indices?v" | grep atlas_graph
```

### 7.7 Rollback Decision Matrix

| Situation | Aliases Touched? | Action | Downtime |
|-----------|-----------------|--------|----------|
| Migration failed (not switched yet) | No | No action needed — JanusGraph still active | None |
| Migration succeeded, switch failed | No | Restore ConfigMap backup, restart | ~30s |
| Switch succeeded, aliases **not** migrated, search broken | No | Rollback ConfigMap, restart | ~30s |
| Switch succeeded, aliases migrated, search broken | Yes | `revert_es_aliases.sh` + restore ConfigMap + restart | ~1min |
| Switch succeeded, lineage broken | Maybe | Rollback ConfigMap (+ `revert_es_aliases.sh` if aliases migrated) + restart | ~30s–1min |
| Switch succeeded, lock icons on assets (forward) | N/A | Run `manage_es_aliases.sh` (no rollback — just complete alias migration) | None |
| Switch succeeded, lock icons on assets (after rollback) | Yes | Run `revert_es_aliases.sh` (persona aliases on wrong index) | None |
| Switch succeeded, some entities missing | Maybe | Re-run `--es-only` or full rollback | Varies |
| Everything works but performance regression | Maybe | Rollback ConfigMap (+ `revert_es_aliases.sh` if aliases migrated) + restart | ~30s–1min |
| Switch succeeded, data drift detected | Maybe | Re-run with `--fresh --maintenance-mode`, then re-switch | 30min+ |
| `manage_es_aliases.sh` failed mid-way | Partial | Run `revert_es_aliases.sh` to clean up partial state | None |

---

## 8. Troubleshooting Decision Tree

```
Migration failed?
├── Exit code 1 (preflight)
│   ├── JAR not found → Deploy from switchable-graph-provider branch
│   ├── Cassandra unreachable → Check pod, network, port 9042
│   └── ES unreachable → Check pod, network, port 9200
│
├── Exit code 3 (migration error)
│   ├── OOM → Increase MIGRATOR_JVM_HEAP
│   ├── Write timeout → Reduce WRITER_THREADS or SCANNER_THREADS
│   ├── Read timeout → Reduce SCANNER_THREADS, check Cassandra load
│   └── Other → Check /opt/apache-atlas/logs/migrator-*.log
│
├── Exit code 1 (validation failed)
│   ├── Vertex count < 95% → Check migration_state for FAILED ranges, re-run
│   ├── Edge mismatch → Re-run with --fresh
│   ├── ES count < 90% → Re-run with --es-only
│   ├── GUID index < 95% → Re-run with --fresh
│   └── Token ranges incomplete → Re-run without --fresh (resume)
│
├── Exit code 0 but post-switch issues
│   ├── Search empty → Check ES alias, run manage_es_aliases.sh
│   ├── Lock icons → Run manage_es_aliases.sh (persona aliases)
│   ├── Entity not found → Check Cassandra + ES for specific GUID
│   ├── Lineage broken → Check edges_out/edges_in counts
│   └── Performance regression → Check auto-sizing, ES field limit
│
└── Need to rollback to JanusGraph?
    ├── Were aliases migrated (manage_es_aliases.sh run)?
    │   ├── Yes → Run revert_es_aliases.sh --vcluster <name> FIRST
    │   └── No → Skip alias revert
    ├── Restore ConfigMap backup (or manually set atlas.graphdb.backend=janus)
    ├── Restart pod (kubectl delete pod atlas-0 -n atlas)
    └── After rollback, issues?
        ├── Search empty → Check atlas_vertex_index alias points to janusgraph_vertex_index
        ├── Lock icons → Run revert_es_aliases.sh (persona aliases still on wrong index)
        └── Stale data → Expected if writes occurred after migration; re-run migration later
```

---

## 9. Environment Variables Reference

### Migration Tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `SCANNER_THREADS` | auto-sized | Parallel CQL scan threads |
| `WRITER_THREADS` | auto-sized | Parallel write threads |
| `ES_BULK_SIZE` | auto-sized | Documents per ES bulk request |
| `QUEUE_CAPACITY` | `20000` | Scanner → Writer in-memory queue |
| `MAX_INFLIGHT_PER_THREAD` | `50` | Max async writes per writer thread |
| `MIGRATOR_JVM_HEAP` | auto-sized | JVM max heap (`-Xmx`) |
| `MIGRATOR_JVM_MIN_HEAP` | auto-sized | JVM initial heap (`-Xms`) |

### ID Generation

| Variable | Default | Description |
|----------|---------|-------------|
| `ID_STRATEGY` | `legacy` | **Always use `legacy` for migration.** Preserves original JanusGraph UUIDs. Other values (`hash-jg`, `deterministic`) exist but are not recommended. |
| `CLAIM_ENABLED` | `false` | LWT dedup claims (INSERT IF NOT EXISTS) |

### Cassandra Tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `SOURCE_CONSISTENCY` | `ONE` | Read consistency for source (fast) |
| `TARGET_CONSISTENCY` | `LOCAL_QUORUM` | Write consistency for target (durable) |
| `SOURCE_KEYSPACE` | `atlas` | JanusGraph keyspace |
| `TARGET_KEYSPACE` | `atlas_graph` | New Cassandra keyspace |

### ES Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SOURCE_ES_INDEX` | `janusgraph_vertex_index` | Source ES index (copy mappings from) |
| `TARGET_ES_INDEX` | `atlas_graph_vertex_index` | Target ES index (write docs to) |
| `ES_FIELD_LIMIT` | `10000` | ES `index.mapping.total_fields.limit` |

### Skip Flags

| Variable | Default | Description |
|----------|---------|-------------|
| `SKIP_ES_REINDEX` | `false` | Skip ES reindex phase |
| `SKIP_CLASSIFICATIONS` | `false` | Skip classification vertices |
| `SKIP_TASKS` | `false` | Skip task vertices |
| `SKIP_BACKUP_CHECK` | `false` | Skip pre-migration backup verification |

### Monitoring

| Variable | Default | Description |
|----------|---------|-------------|
| `MIXPANEL_TOKEN` | (not set) | Mixpanel project token for telemetry |
| `MIXPANEL_API_SECRET` | (not set) | Mixpanel API secret for event tracking |
| `DOMAIN_NAME` | (from VCLUSTER_NAME) | Tenant identifier for Mixpanel |

---

## Appendix A: Key Metrics to Monitor

After migration completes, check these metrics in the final summary:

| Metric | Healthy | Warning | Action if Warning |
|--------|---------|---------|-------------------|
| `decode_errors` | 0 | > 0 but < 0.01% | Review log for affected vertex IDs |
| `write_errors` | 0 | > 0 | **Re-run with `--fresh`** |
| `vertices_written` | Matches source | < 95% of source | Check FAILED token ranges |
| `edges_written` | > 0 | 0 | Check JanusGraph data integrity |
| `es_docs_indexed` | ≈ vertices_written | < 90% | Re-run with `--es-only` |
| `token_ranges_done` | = `token_ranges_total` | < total | Re-run to resume |

## Appendix B: File Locations in Pod

| File | Path |
|------|------|
| Migration script | `/opt/apache-atlas/bin/atlas_migrate.sh` |
| Migrator JAR | `/opt/apache-atlas/tools/atlas-graphdb-migrator.jar` |
| Atlas config | `/opt/apache-atlas/conf/atlas-application.properties` |
| Migration logs | `/opt/apache-atlas/logs/migrator-*.log` |
| Generated properties | `/tmp/migrator.properties` |
| Validation report | `/opt/apache-atlas/logs/validation-report-*.json` |

## Appendix C: Backend Switch Properties

Properties added to `atlas-application.properties` when switching to Cassandra:

```properties
# === Zero Graph Backend (Cassandra + ES direct) ===
atlas.graphdb.backend=cassandra
atlas.graph.index.search.es.prefix=atlas_graph_
atlas.cassandra.graph.hostname=<cassandra-host>
atlas.cassandra.graph.port=9042
atlas.cassandra.graph.keyspace=atlas_graph
atlas.cassandra.graph.datacenter=<datacenter-name>
atlas.graph.id.strategy=deterministic
atlas.graph.claim.enabled=false
```

> **ID Strategy note:** The runtime property `atlas.graph.id.strategy=deterministic` is correct for **post-migration operation**. During the **migration itself**, the migrator uses `ID_STRATEGY=legacy` (env var → `migration.id.strategy` property), which is a separate code path (`IdStrategy` enum in the migrator vs `RuntimeIdStrategy` enum in `CassandraGraphDatabase`). These two strategies serve different purposes:
> - **Migration (`legacy`):** Preserves original JanusGraph UUIDs for all migrated vertices/edges
> - **Runtime (`deterministic`):** Generates SHA-256 IDs for new entities created after the switch
>
> Both ID formats coexist safely in the same Cassandra tables. Never set the runtime property to `legacy` — that would cause new entities to get random UUIDs instead of deterministic content-addressed IDs.

Properties to remove when reverting to JanusGraph:
- All `atlas.cassandra.graph.*` lines
- `atlas.graph.index.search.es.prefix`
- `atlas.graph.id.strategy`
- `atlas.graph.claim.enabled`
- Set `atlas.graphdb.backend=janus` (or remove the line; default is `janus`)
