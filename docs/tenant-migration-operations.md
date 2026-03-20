# Tenant Migration Operations Reference

**Field-tested procedures from production migrations (miralgup02, 398K vertices, 11m14s)**

---

## Quick Reference

```bash
# Pre-flight check
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s localhost:21000/api/atlas/admin/status | python -m json.tool

# Dry-run migration (verify config without migrating)
kubectl exec atlas-0 -n atlas -c atlas-main -- /opt/apache-atlas/bin/atlas_migrate.sh --dry-run

# Full migration (fresh start)
kubectl exec atlas-0 -n atlas -c atlas-main -- env ID_STRATEGY=legacy CLAIM_ENABLED=false /opt/apache-atlas/bin/atlas_migrate.sh --fresh

# Validation only (after migration)
kubectl exec atlas-0 -n atlas -c atlas-main -- /opt/apache-atlas/bin/atlas_migrate.sh --validate-only

# ES reindex only (fix ES without re-scanning Cassandra)
kubectl exec atlas-0 -n atlas -c atlas-main -- /opt/apache-atlas/bin/atlas_migrate.sh --es-only

# Automated end-to-end migration
./tools/atlas_migrate_tenant.sh --vcluster mytenantname --fresh

# Automated migration with maintenance mode + retry
./tools/atlas_migrate_tenant.sh --vcluster mytenantname --fresh \
  --maintenance-mode --max-retries 3

# Large tenant with custom JVM heap + sizing
./tools/atlas_migrate_tenant.sh --vcluster mytenantname --fresh \
  --maintenance-mode --jvm-heap 48g --jvm-min-heap 16g \
  --scanner-threads 32 --writer-threads 16

# Custom JVM heap (manual kubectl exec)
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  env ID_STRATEGY=legacy CLAIM_ENABLED=false \
  MIGRATOR_JVM_HEAP=24g MIGRATOR_JVM_MIN_HEAP=8g \
  /opt/apache-atlas/bin/atlas_migrate.sh --fresh
```

---

## Prerequisites

1. **kubectl access** to the target vcluster/namespace
2. **Build version** includes `switchable-graph-provider` code (graphdb/cassandra + graphdb/migrator modules)
3. **Atlas pod** is healthy (`ACTIVE` status, 3/3 containers Running)
4. **Cassandra** is accessible from the Atlas pod (port 9042)
5. **Elasticsearch** is accessible from the Atlas pod (port 9200)

---

## Phase 1: Pre-flight Checks

### 1.1 Verify pod health

```bash
kubectl get pods -n atlas -l app=atlas
```

Expected output:
```
NAME      READY   STATUS    RESTARTS   AGE
atlas-0   3/3     Running   0          2d
```

### 1.2 Verify Atlas is ACTIVE

```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s localhost:21000/api/atlas/admin/status | python -m json.tool
```

Expected output:
```json
{
    "Status": "ACTIVE"
}
```

### 1.3 Verify migrator tools are present

```bash
# Check migration script
kubectl exec atlas-0 -n atlas -c atlas-main -- ls -la /opt/apache-atlas/bin/atlas_migrate.sh

# Check migrator JAR
kubectl exec atlas-0 -n atlas -c atlas-main -- ls -la /opt/apache-atlas/tools/atlas-graphdb-migrator.jar
```

### 1.4 Check current backend

```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- grep 'atlas.graphdb.backend' /opt/apache-atlas/conf/atlas-application.properties
```

Expected: `atlas.graphdb.backend=janus`

### 1.5 Check Cassandra pod

```bash
kubectl get pods -n atlas -l app=cassandra
```

---

## Phase 2: Cleanup (for remigration only)

**Skip this phase for first-time migrations.** Only needed if you're re-running migration after a previous attempt.

### 2.1 Drop target keyspace

```bash
kubectl exec atlas-cassandra-0 -n atlas -- cqlsh -e "DROP KEYSPACE IF EXISTS atlas_graph;"
```

Verify:
```bash
kubectl exec atlas-cassandra-0 -n atlas -- cqlsh -e "DESCRIBE KEYSPACES;"
```

The output should NOT contain `atlas_graph`.

### 2.2 Delete target ES index

```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s -X DELETE "localhost:9200/atlas_graph_vertex_index"
```

Expected: `{"acknowledged":true}`

Verify:
```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s "localhost:9200/_cat/indices?v" | grep atlas_graph
```

Should return no results.

### 2.3 If currently on Cassandra backend

If the tenant is already switched to Cassandra and you need to remigrate:

1. Switch back to JanusGraph first (see [Rollback Procedure](#rollback-procedure))
2. Wait for pod to restart and become ACTIVE
3. Then drop the keyspace and ES index as above
4. Proceed with migration

---

## Phase 3: Migration

### 3.1 Dry-run (verify configuration)

```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- /opt/apache-atlas/bin/atlas_migrate.sh --dry-run
```

Expected output shows all derived configuration:
```
[2026-03-18 06:20:00] Migrator JAR: /opt/apache-atlas/tools/atlas-graphdb-migrator.jar
[2026-03-18 06:20:00] Reading atlas-application.properties at /opt/apache-atlas/conf
[2026-03-18 06:20:00] Configuration:
[2026-03-18 06:20:00]   Cassandra host: atlas-cassandra
[2026-03-18 06:20:00]   Cassandra port: 9042
[2026-03-18 06:20:00]   Datacenter: datacenter1
[2026-03-18 06:20:00]   ES host: atlas-elasticsearch
[2026-03-18 06:20:00]   ES port: 9200
[2026-03-18 06:20:00]   Source keyspace: atlas (edgestore)
[2026-03-18 06:20:00]   Target keyspace: atlas_graph
[2026-03-18 06:20:00]   Source ES index: janusgraph_vertex_index
[2026-03-18 06:20:00]   Target ES index: atlas_graph_vertex_index
...
[2026-03-18 06:20:00] Dry run complete. Properties at /tmp/migrator.properties
```

### 3.2 Full migration

```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- \
  env ID_STRATEGY=legacy CLAIM_ENABLED=false \
  /opt/apache-atlas/bin/atlas_migrate.sh --fresh
```

**Real output from miralgup02 (398K vertices, 647K edges):**
```
==========================================
  JanusGraph -> Cassandra Migrator
  Mode: --fresh
  JAR:  /opt/apache-atlas/tools/atlas-graphdb-migrator.jar
  Log:  /opt/apache-atlas/logs/migrator-20260318-062504.log
==========================================

[INFO] Starting migration...
[INFO] Scanning token ranges: 4 ranges across 3 nodes
[INFO] Scanner threads: 16, Writer threads: 8
...
[INFO] Vertices migrated: 398110
[INFO] Edges migrated: 647577
[INFO] ES documents indexed: 398110
[INFO] Duration: 11m14s

=== Validation Results ===
14/14 checks PASSED
Overall: PASSED
```

Migration typically completes in:
- ~100K vertices: 1-2 minutes
- ~400K vertices: 10-15 minutes
- ~1M vertices: 20-40 minutes

### 3.3 Validate migration

```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- /opt/apache-atlas/bin/atlas_migrate.sh --validate-only
```

This runs 14 validation checks and produces a JSON report.

### 3.4 Using a dedicated migration Pod (Helm)

For large tenants or when you don't want to compete with the running Atlas process for resources:

```bash
# Enable migration Pod with helm upgrade
helm upgrade atlas ./helm/atlas \
  --set migration.enabled=true \
  --set migration.mode="--fresh" \
  --set migration.idStrategy=legacy \
  --set migration.claimEnabled=false \
  --set migration.jvmHeap=4g \
  --set migration.jvmMinHeap=2g \
  --set migration.postCompletionSleepSeconds=3600 \
  --set migration.resources.requests.cpu=4 \
  --set migration.resources.requests.memory=8Gi \
  --set migration.resources.limits.cpu=4 \
  --set migration.resources.limits.memory=8Gi

# For large tenants (>10M vertices):
# --set migration.jvmHeap=48g --set migration.jvmMinHeap=16g
# --set migration.scannerThreads=32 --set migration.writerThreads=16
# --set migration.resources.requests.cpu=32 --set migration.resources.requests.memory=64Gi

# Monitor the Pod
kubectl get pods -n atlas -l app=atlas-migrator
kubectl logs -f -l app=atlas-migrator -n atlas

# Pod stays Running for 1hr after migration for inspection:
kubectl exec -it <migrator-pod> -n atlas -- bash
kubectl exec -it <migrator-pod> -n atlas -- tail -100 /opt/apache-atlas/logs/migrator-*.log

# After inspection, clean up:
helm upgrade atlas ./helm/atlas --set migration.enabled=false
kubectl delete pod -l app=atlas-migrator -n atlas
```

The Pod uses the same Atlas image as the main StatefulSet, so the migrator JAR is already present (no need to copy a 204MB file). After migration finishes, the pod sleeps for `postCompletionSleepSeconds` (default 1 hour) to allow log inspection and `kubectl exec` access, then auto-exits.

---

## Phase 4: Backend Switch

### 4.1 Backup ConfigMap

```bash
kubectl get configmap atlas-config -n atlas -o yaml > /tmp/atlas-config-backup-$(date +%Y%m%d-%H%M%S).yaml
```

### 4.2 Patch ConfigMap

The switch requires adding 8 properties to `atlas-application.properties`. Extract the current config, add the Cassandra block, and re-apply:

```bash
# Extract current properties
kubectl get configmap atlas-config -n atlas -o jsonpath='{.data.atlas-application\.properties}' > /tmp/atlas-props.txt

# Remove any existing Zero Graph lines (if remigrating)
grep -v '^atlas.graphdb.backend\|^atlas.graph.index.search.es.prefix\|^atlas.cassandra.graph\.\|^atlas.graph.id.strategy\|^atlas.graph.claim.enabled' /tmp/atlas-props.txt > /tmp/atlas-props-clean.txt

# Append Cassandra backend properties
cat >> /tmp/atlas-props-clean.txt << 'EOF'

# === Zero Graph Backend (Cassandra + ES direct) ===
atlas.graphdb.backend=cassandra
atlas.graph.index.search.es.prefix=atlas_graph_
atlas.cassandra.graph.hostname=atlas-cassandra
atlas.cassandra.graph.port=9042
atlas.cassandra.graph.keyspace=atlas_graph
atlas.cassandra.graph.datacenter=datacenter1
atlas.graph.id.strategy=deterministic
atlas.graph.claim.enabled=false
EOF

# Create patched ConfigMap
kubectl create configmap atlas-config -n atlas \
  --from-file=atlas-application.properties=/tmp/atlas-props-clean.txt \
  --dry-run=client -o yaml | kubectl apply -f -
```

**The 8 properties explained:**

| Property | Value | Purpose |
|----------|-------|---------|
| `atlas.graphdb.backend` | `cassandra` | Select Cassandra graph backend |
| `atlas.graph.index.search.es.prefix` | `atlas_graph_` | ES index prefix (instead of `janusgraph_`) |
| `atlas.cassandra.graph.hostname` | `atlas-cassandra` | Cassandra service hostname |
| `atlas.cassandra.graph.port` | `9042` | Cassandra CQL port |
| `atlas.cassandra.graph.keyspace` | `atlas_graph` | Target keyspace name |
| `atlas.cassandra.graph.datacenter` | `datacenter1` | Cassandra datacenter |
| `atlas.graph.id.strategy` | `deterministic` | ID generation strategy post-switch |
| `atlas.graph.claim.enabled` | `false` | LWT dedup claims (disable for normal operation) |

### 4.3 Restart pod

```bash
kubectl delete pod atlas-0 -n atlas
```

Wait for pod to become ready:
```bash
kubectl wait --for=condition=Ready pod/atlas-0 -n atlas --timeout=300s
```

### 4.4 Verify CassandraGraph initialization

```bash
kubectl logs atlas-0 -n atlas -c atlas-main --tail=200 | grep -i "cassandra\|CassandraGraph\|idStrategy\|field mappings"
```

Expected log lines:
```
CassandraGraph initialized: keyspace=atlas_graph, idStrategy=DETERMINISTIC
ES field mappings preloaded: 4595 fields
```

---

## Phase 5: Post-Switch Verification

All verification steps require an authentication token. Acquire one first.

### 5.1 Acquire Keycloak token

```bash
# Extract Keycloak credentials from the pod
KC_JSON=$(kubectl exec atlas-0 -n atlas -c atlas-main -- python -c "
import json
with open('/opt/apache-atlas/keycloak.json') as f:
    d = json.load(f)
print(json.dumps({'url': d.get('auth-server-url',''), 'realm': d.get('realm',''), 'client': d.get('resource',''), 'secret': d['credentials']['secret']}))
")

KC_URL=$(echo "$KC_JSON" | python -c "import sys,json; print(json.load(sys.stdin)['url'])")
KC_REALM=$(echo "$KC_JSON" | python -c "import sys,json; print(json.load(sys.stdin)['realm'])")
KC_CLIENT=$(echo "$KC_JSON" | python -c "import sys,json; print(json.load(sys.stdin)['client'])")
KC_SECRET=$(echo "$KC_JSON" | python -c "import sys,json; print(json.load(sys.stdin)['secret'])")

# Request token
TOKEN=$(kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s \
  -d "grant_type=client_credentials&client_id=${KC_CLIENT}&client_secret=${KC_SECRET}" \
  "${KC_URL}/realms/${KC_REALM}/protocol/openid-connect/token" \
  | python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
```

### 5.2 Test index search — Tables

```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"dsl":{"from":0,"size":5,"query":{"bool":{"must":[{"term":{"__typeName.keyword":"Table"}},{"term":{"__state":"ACTIVE"}}]}}},"attributes":["qualifiedName","name","__hasLineage","__isIncomplete"]}' \
  "localhost:21000/api/meta/search/indexsearch" | python -m json.tool
```

**miralgup02 result:** 5 results returned, ~2222 total Tables, 0 lock icons (all scrubbed).

### 5.3 Test index search — Connections

```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"dsl":{"from":0,"size":10,"query":{"bool":{"must":[{"term":{"__typeName.keyword":"Connection"}},{"term":{"__state":"ACTIVE"}}]}}},"attributes":["qualifiedName","name","connectorName"]}' \
  "localhost:21000/api/meta/search/indexsearch" | python -m json.tool
```

**miralgup02 result:** 4 connections, 0 locked.

### 5.4 Verify AuthPolicy in ES

```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s "localhost:9200/atlas_graph_vertex_index/_count?q=__typeName.keyword:AuthPolicy"
```

**miralgup02 result:** `{"count":210,...}` — matches source AuthPolicy count (207 + 3 system policies).

### 5.5 Verify AuthService in ES

```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s "localhost:9200/atlas_graph_vertex_index/_count?q=__typeName.keyword:AuthService"
```

**miralgup02 result:** `{"count":4,...}`

### 5.6 Check PolicyRefresher evaluator count

```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s \
  -H "Authorization: Bearer ${TOKEN}" \
  "localhost:21000/api/atlas/admin/refreshPolicyInfo" | python -m json.tool
```

**miralgup02 result:** 410 evaluators loaded (covers Personas, Purposes, and system policies).

### 5.7 Test Heka policy download

```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s \
  -H "Authorization: Bearer ${TOKEN}" \
  "localhost:21000/api/atlas/heka/policy" | python -m json.tool | head -20
```

**miralgup02 result:** 9 policies returned.

### 5.8 Test TypeDef headers

```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s \
  -H "Authorization: Bearer ${TOKEN}" \
  "localhost:21000/api/atlas/v2/types/typedefs/headers" | python -c "import sys,json; d=json.load(sys.stdin); print(len(d))"
```

**miralgup02 result:** 1042 types.

### 5.9 Test lineage traversal

```bash
# Pick any Process entity GUID from search results, then:
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s \
  -H "Authorization: Bearer ${TOKEN}" \
  "localhost:21000/api/atlas/v2/lineage/<GUID>?depth=5&direction=BOTH" | python -m json.tool | head -5
```

**miralgup02 result:** 12 nodes, 14 relations.

### 5.10 Write test (create + delete)

```bash
# Create test entity
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s -X POST \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"entity":{"typeName":"Table","attributes":{"qualifiedName":"migration-test-delete-me","name":"migration-test-delete-me"}}}' \
  "localhost:21000/api/atlas/v2/entity"

# Get the GUID from response, then delete:
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s -X DELETE \
  -H "Authorization: Bearer ${TOKEN}" \
  "localhost:21000/api/atlas/v2/entity/guid/<GUID>?type=HARD"
```

---

## Rollback Procedure

If verification fails or you need to revert:

### Option A: Restore ConfigMap backup (fastest)

```bash
# Apply the backup saved in Phase 4.1
kubectl apply -f /tmp/atlas-config-backup-TIMESTAMP.yaml

# Restart pod
kubectl delete pod atlas-0 -n atlas
kubectl wait --for=condition=Ready pod/atlas-0 -n atlas --timeout=300s
```

### Option B: Manual revert

```bash
# Extract current config
kubectl get configmap atlas-config -n atlas -o jsonpath='{.data.atlas-application\.properties}' > /tmp/atlas-props.txt

# Remove Cassandra lines, set backend back to janus
sed -i '' '/^atlas.graphdb.backend=/d' /tmp/atlas-props.txt
sed -i '' '/^atlas.graph.index.search.es.prefix=/d' /tmp/atlas-props.txt
sed -i '' '/^atlas.cassandra.graph\./d' /tmp/atlas-props.txt
sed -i '' '/^atlas.graph.id.strategy=/d' /tmp/atlas-props.txt
sed -i '' '/^atlas.graph.claim.enabled=/d' /tmp/atlas-props.txt
sed -i '' '/^# === Zero Graph/d' /tmp/atlas-props.txt
echo "atlas.graphdb.backend=janus" >> /tmp/atlas-props.txt

# Apply
kubectl create configmap atlas-config -n atlas \
  --from-file=atlas-application.properties=/tmp/atlas-props.txt \
  --dry-run=client -o yaml | kubectl apply -f -

# Restart
kubectl delete pod atlas-0 -n atlas
```

### Safety note

JanusGraph data is **never modified** during migration. Rolling back simply points Atlas back to the original JanusGraph+ES indexes. No data is lost.

---

## Troubleshooting

### 204MB JAR copy timeout

**Symptom:** Attempting to use a lightweight JDK pod for migration fails because `kubectl cp` of the 204MB migrator JAR times out.

**Solution:** Use the Atlas image itself (via Helm Job template or atlas-0 pod). The migrator JAR is pre-installed at `/opt/apache-atlas/tools/atlas-graphdb-migrator.jar`.

### `python3: not found`

**Symptom:** Commands using `python3` fail inside the Atlas pod.

**Solution:** The Atlas pod (Ubuntu 22.04) only has `python` (Python 2.7). Use `python` instead of `python3` for all in-pod commands. The `json` module works identically in Python 2.

### ES field limit 5000 exceeded

**Symptom:** Warning during migration: `Limit of total fields [5000] has been exceeded`

**Impact:** Non-blocking. ES will still index documents, but fields beyond 5000 won't be indexed. This is typically fine for search — only exotic/rarely-used attributes are affected.

**Fix (if needed):**
```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- curl -s -X PUT \
  "localhost:9200/atlas_graph_vertex_index/_settings" \
  -H "Content-Type: application/json" \
  -d '{"index.mapping.total_fields.limit": 10000}'
```

### Cassandra timeout on `tags_by_id`

**Symptom:** `WARN` log line: `ReadTimeoutException querying tags_by_id`

**Impact:** Non-critical. The `tags_by_id` table is only used for tag-based migration (classification transfer). If classifications are not being migrated, this is safely ignored.

### Keycloak token in chained commands

**Symptom:** Token acquisition fails when trying to chain `python` and `curl` in a single `kubectl exec` command.

**Solution:** Always split Keycloak operations into separate `kubectl exec` calls. Extract credentials first, then use the token in a second call. The orchestration script handles this automatically.

### Migration appears stuck

**Symptom:** No output for > 5 minutes during migration.

**Check:** Migration may be processing large token ranges. Monitor the log file:
```bash
kubectl exec atlas-0 -n atlas -c atlas-main -- tail -20 /opt/apache-atlas/logs/migrator-*.log
```

### Pod fails to start after backend switch

**Symptom:** Atlas pod enters CrashLoopBackOff after ConfigMap change.

**Common causes:**
1. Typo in ConfigMap properties — check for extra spaces or wrong hostnames
2. `atlas_graph` keyspace doesn't exist — migration must complete before switching
3. Cassandra pod is down — check `kubectl get pods -n atlas -l app=cassandra`

**Fix:** Rollback the ConfigMap and investigate.

---

## Validation Report Format

The migrator produces a JSON report (see `validation-report-miralgup02.json` for a real example). Key fields:

```json
{
  "tenant_id": "miralgup02",
  "start_time": "2026-03-18T06:36:24Z",
  "end_time": "2026-03-18T06:37:00Z",
  "overall_passed": true,
  "vertex_count": 398110,
  "edge_out_count": 647577,
  "edge_in_count": 647577,
  "es_doc_count": 398110,
  "typedef_count": 1042,
  "checks": [
    {"check_name": "vertex_count", "passed": true, ...},
    {"check_name": "edge_out_in_consistency", "passed": true, ...},
    {"check_name": "edge_by_id_consistency", "passed": true, ...},
    {"check_name": "guid_index_sample", "passed": true, ...},
    {"check_name": "typedef_consistency", "passed": true, ...},
    {"check_name": "deep_vertex_correctness", "passed": true, ...},
    {"check_name": "cross_table_integrity", "passed": true, ...},
    {"check_name": "property_corruption", "passed": true, ...},
    {"check_name": "es_vertex_count", "passed": true, ...},
    {"check_name": "orphan_edge_detection", "passed": true, ...},
    {"check_name": "super_vertex_detection", "passed": true, ...}
  ],
  "vertex_count_by_type": {
    "Column": 136947,
    "PopularityInsights": 130027,
    "ColumnProcess": 101984,
    ...
  }
}
```

### Validation checks (14 total)

| # | Check | What it verifies |
|---|-------|-----------------|
| 1 | `vertex_count` | Target vertex count matches source baseline |
| 2 | `edge_out_in_consistency` | edges_out == edges_in (bidirectional consistency) |
| 3 | `edge_by_id_consistency` | edges_by_id == edges_out (index consistency) |
| 4 | `guid_index_sample` | GUID index completeness (sampled lookup) |
| 5 | `typedef_consistency` | TypeDef tables are consistent and non-empty |
| 6 | `deep_vertex_correctness` | Properties, indexes, edges on sampled vertices |
| 7 | `cross_table_integrity` | vertex_index entries point to existing vertices |
| 8 | `property_corruption` | No `__type.Asset.*` corrupted property names |
| 9 | `es_vertex_count` | Source ES docs vs target Cassandra vertices |
| 10 | `orphan_edge_detection` | Both edge endpoints exist in vertices table |
| 11 | `super_vertex_detection` | No vertices exceed edge threshold (100K) |
| 12 | `edge_index_completeness` | Edge indexes are complete |
| 13 | `config_store_accessible` | Config store keyspace is accessible |
| 14 | `tags_table_accessible` | Tags keyspace is accessible |

---

## Real Numbers: miralgup02

| Metric | Value |
|--------|-------|
| Tenant | miralgup02 |
| Total vertices | 398,110 |
| Total edges | 647,577 |
| Migration time | 11m 14s |
| Validation checks | 14/14 PASS |
| Top entity types | Column (136K), PopularityInsights (130K), ColumnProcess (101K) |
| AuthPolicies | 210 |
| AuthServices | 4 |
| PolicyRefresher evaluators | 410 |
| TypeDefs | 1,042 |
| Connections | 4 |
| Tables | 2,222 |
| Super vertices | 0 (max edge count: 540) |
