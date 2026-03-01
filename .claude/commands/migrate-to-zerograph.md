---
description: Migrate an Atlas tenant from JanusGraph to Cassandra (Zero Graph) — runs migrator, reindexes ES, switches backend, validates
argument-hint: <vcluster-name>
allowed-tools: [Bash, Read, Grep, Glob, Task, AskUserQuestion, Skill]
---

# Migrate Tenant to Zero Graph (Cassandra Backend)

You are an operator running the full JanusGraph → Cassandra migration for an Atlas tenant. This is a multi-stage process that can take minutes to hours depending on tenant size. You MUST execute each phase sequentially and report progress after each step.

**Target vcluster:** $ARGUMENTS

---

## Phase 0: Collect Inputs and Validate Prerequisites

### 0.1 Set variables from arguments

Parse the vcluster name from `$ARGUMENTS`. If not provided, use AskUserQuestion to ask for it.

Set the namespace. For vclusters, the Atlas namespace is typically `atlas` inside the vcluster. Confirm with the user if unclear.

```bash
VCLUSTER_NAME="<from arguments>"
NS="atlas"
POD="atlas-0"
CONTAINER="atlas-main"
```

### 0.2 Ask user: pre-flight checklist (single AskUserQuestion, multi-select)

Use AskUserQuestion with ALL of these as a multi-select checklist:
- Question: "Please confirm these prerequisites before we begin. Select all that apply:"
- Header: "Pre-flight"
- multiSelect: true
- Options:
  - "Current build supports Zero Graph switching" — The Atlas build deployed to this vcluster includes `graphdb/cassandra/`, `graphdb/migrator/`, and `tools/atlas_migrate.sh`. This could be `switchable-graph-provider`, `beta`, `staging`, `master`, or any branch with the Cassandra backend code.
  - "ArgoCD is disabled for this tenant" — ArgoCD auto-sync is paused/disabled so ConfigMap changes made during migration are not overwritten by ArgoCD reconciliation.
  - "I have kubectl access to the vcluster" — Can run `kubectl exec` against the atlas pod in this vcluster.

**All three MUST be selected to proceed.**

If "Current build supports Zero Graph switching" is NOT selected:
> Deploy a build that includes the Cassandra backend to this vcluster first. The build must include `graphdb/cassandra/`, `graphdb/migrator/`, and `tools/atlas_migrate.sh`. Re-run `/migrate-to-zerograph` after deployment.

If "ArgoCD is disabled for this tenant" is NOT selected:
> **CRITICAL**: Disable ArgoCD auto-sync for this tenant before proceeding. ConfigMap changes made during migration (switching `atlas.graphdb.backend=cassandra` and `atlas.graph.index.search.es.prefix=atlas_graph_`) will be reverted by ArgoCD if sync is active, causing the pod to restart on the old JanusGraph backend and losing the migration switch.

If any prerequisite is not met, STOP.

### 0.3 Ask user: migration pod strategy

Use AskUserQuestion:
- Question: "Where should the migrator run? For large tenants (>10M assets), a dedicated pod avoids competing with Atlas for CPU/memory."
- Header: "Pod strategy"
- Options:
  - "Atlas pod (Recommended)" — run directly on atlas-0 (simple, good for small/medium tenants)
  - "Dedicated pod" — create a separate pod with 4 CPU / 8Gi memory + PodDisruptionBudget (large tenants)

Store the choice for later phases.

### 0.4 Verify kubectl access and atlas pod health

```bash
# Connect to the vcluster context
kubectl config use-context "$VCLUSTER_NAME" 2>/dev/null || true

# Check pod is running
kubectl get pods -n $NS -l app=atlas

# Check Atlas is healthy
kubectl exec -n $NS $POD -c $CONTAINER -- curl -s http://localhost:21000/api/atlas/admin/status
```

Expected: `{"Status":"ACTIVE"}`

If the pod is not running or not healthy, STOP and report the issue.

### 0.5 Verify migrator is available

```bash
# Check migration script
kubectl exec -n $NS $POD -c $CONTAINER -- ls -la /opt/apache-atlas/bin/atlas_migrate.sh

# Check migrator JAR exists somewhere
kubectl exec -n $NS $POD -c $CONTAINER -- bash -c \
  'find /opt/apache-atlas -name "atlas-graphdb-migrator*" -not -name "original-*" 2>/dev/null | head -3'
```

If neither the script nor JAR is found, STOP:
> The migration tools are not present in this build. The build must include the `graphdb/migrator` module. Re-deploy with a build that has Zero Graph support.

---

## Phase 1: Set Up Migration Environment

### If user chose "Atlas pod" — skip to Phase 2

No setup needed. Migration runs directly on atlas-0.

Set variables:
```bash
MIGRATE_POD="$POD"
MIGRATE_CONTAINER="$CONTAINER"
```

### If user chose "Dedicated pod" — create migration pod + PDB

#### 1.1 Identify the node zone for pod affinity (so it lands near Cassandra)

```bash
# Get the zone where atlas-0 runs
ATLAS_NODE=$(kubectl get pod $POD -n $NS -o jsonpath='{.spec.nodeName}')
ATLAS_ZONE=$(kubectl get node $ATLAS_NODE -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}' 2>/dev/null || echo "")
echo "Atlas node: $ATLAS_NODE, zone: $ATLAS_ZONE"
```

#### 1.2 Create PodDisruptionBudget (prevents eviction during migration)

```bash
kubectl apply -n $NS -f - <<'EOF'
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: atlas-migrator-pdb
  labels:
    app: atlas-migrator
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: atlas-migrator
EOF
```

#### 1.3 Create the dedicated migration pod

Use a lightweight JDK image — the migrator is a fat JAR with all dependencies bundled. Only needs Java 17 and network access to Cassandra + ES within the cluster.

```bash
cat <<PODEOF | kubectl apply -n $NS -f -
apiVersion: v1
kind: Pod
metadata:
  name: atlas-migrator
  labels:
    app: atlas-migrator
spec:
  ${ATLAS_ZONE:+nodeSelector:}
  ${ATLAS_ZONE:+  topology.kubernetes.io/zone: "$ATLAS_ZONE"}
  containers:
  - name: migrator
    image: eclipse-temurin:17-jre
    command: ["sleep", "86400"]
    resources:
      requests:
        cpu: "4"
        memory: "8Gi"
      limits:
        cpu: "4"
        memory: "8Gi"
  restartPolicy: Never
PODEOF
```

Wait for pod to be ready:
```bash
kubectl wait --for=condition=Ready pod/atlas-migrator -n $NS --timeout=120s
```

#### 1.4 Copy migrator fat JAR, script, and config from atlas pod

```bash
# Create directory structure on the migrator pod
kubectl exec -n $NS atlas-migrator -c migrator -- mkdir -p /opt/apache-atlas/bin /opt/apache-atlas/conf /opt/apache-atlas/tools /opt/apache-atlas/logs

# Copy atlas-application.properties (needed for connection config)
kubectl cp $NS/$POD:/opt/apache-atlas/conf/atlas-application.properties /tmp/atlas-application.properties -c $CONTAINER
kubectl cp /tmp/atlas-application.properties $NS/atlas-migrator:/opt/apache-atlas/conf/atlas-application.properties -c migrator

# Copy migration script
kubectl cp $NS/$POD:/opt/apache-atlas/bin/atlas_migrate.sh /tmp/atlas_migrate.sh -c $CONTAINER
kubectl cp /tmp/atlas_migrate.sh $NS/atlas-migrator:/opt/apache-atlas/bin/atlas_migrate.sh -c migrator
kubectl exec -n $NS atlas-migrator -c migrator -- chmod +x /opt/apache-atlas/bin/atlas_migrate.sh

# Find and copy migrator fat JAR
MIGRATOR_JAR=$(kubectl exec -n $NS $POD -c $CONTAINER -- bash -c \
  'find /opt/apache-atlas -name "atlas-graphdb-migrator*" -not -name "original-*" 2>/dev/null | head -1')
echo "Found migrator JAR: $MIGRATOR_JAR"

MIGRATOR_JAR_NAME=$(basename "$MIGRATOR_JAR")
kubectl cp "$NS/$POD:$MIGRATOR_JAR" "/tmp/$MIGRATOR_JAR_NAME" -c $CONTAINER
kubectl cp "/tmp/$MIGRATOR_JAR_NAME" "$NS/atlas-migrator:$MIGRATOR_JAR" -c migrator

# Also try tools directory
kubectl exec -n $NS $POD -c $CONTAINER -- bash -c 'ls /opt/apache-atlas/tools/*.jar 2>/dev/null' && {
  for jar in $(kubectl exec -n $NS $POD -c $CONTAINER -- bash -c 'ls /opt/apache-atlas/tools/*.jar 2>/dev/null'); do
    jarname=$(basename "$jar")
    kubectl cp "$NS/$POD:$jar" "/tmp/$jarname" -c $CONTAINER
    kubectl cp "/tmp/$jarname" "$NS/atlas-migrator:$jar" -c migrator
  done
} || echo "No tools JARs to copy"

# Verify Java works and JAR is accessible
kubectl exec -n $NS atlas-migrator -c migrator -- java -version
kubectl exec -n $NS atlas-migrator -c migrator -- ls -lh "$MIGRATOR_JAR"
```

#### 1.5 Set variables for remaining phases

```bash
MIGRATE_POD="atlas-migrator"
MIGRATE_CONTAINER="migrator"
```

---

## Phase 2: Run Migration (Scan + Write + ES Reindex + Validate)

### 2.1 Dry run — verify config

```bash
kubectl exec -n $NS $MIGRATE_POD -c $MIGRATE_CONTAINER -- \
  /opt/apache-atlas/bin/atlas_migrate.sh --dry-run
```

**Report to user:**
- Cassandra host, port, datacenter
- ES host, port
- Source keyspace and target keyspace
- Preflight connectivity results

If connectivity fails, STOP and report the issue.

### 2.2 Run full migration

```bash
kubectl exec -n $NS $MIGRATE_POD -c $MIGRATE_CONTAINER -- \
  /opt/apache-atlas/bin/atlas_migrate.sh 2>&1
```

**IMPORTANT**: This can take minutes to hours. Run it and monitor output. The migration logs progress every 10 seconds with:
- Vertex/edge counts
- Token ranges completed (A/B)
- Throughput rate
- ETA
- Error counts

Report progress to the user as it runs.

### 2.3 Check for incomplete token ranges

After the migration command finishes, check if ALL token ranges completed successfully:

```bash
# Check migration state in Cassandra — find the cassandra pod first
CASS_POD=$(kubectl get pods -n $NS -l app=cassandra -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "atlas-cassandra-0")
CASS_CONTAINER=$(kubectl get pod $CASS_POD -n $NS -o jsonpath='{.spec.containers[0].name}' 2>/dev/null || echo "atlas-cassandra")

kubectl exec -n $NS $CASS_POD -c $CASS_CONTAINER -- cqlsh -e "
  SELECT status, COUNT(*) as cnt FROM atlas_graph.migration_state GROUP BY status;
" 2>/dev/null || echo "Cannot query migration_state directly"
```

Also check the migration log output for:
- `Decode errors: 0`
- `Write errors: 0`
- All validation checks `PASS`

### 2.4 Rerun if token ranges are incomplete

If there are FAILED or PENDING token ranges, rerun the migration. It is **resumable** — it picks up from where it left off:

```bash
kubectl exec -n $NS $MIGRATE_POD -c $MIGRATE_CONTAINER -- \
  /opt/apache-atlas/bin/atlas_migrate.sh 2>&1
```

Keep rerunning until ALL token ranges are COMPLETED. Report each rerun attempt to the user.

If the same ranges keep failing after 3 retries, use AskUserQuestion to ask:
- "Migration has failed token ranges after 3 retries. How should we proceed?"
- Options:
  - "Retry again" — try once more
  - "Run with --fresh" — clear all state and restart from scratch
  - "Stop" — abort migration

### 2.5 Verify ES reindex completed

```bash
# Check ES doc count in the target index
kubectl exec -n $NS $POD -c $CONTAINER -- curl -s \
  'http://atlas-elasticsearch-master:9200/atlas_graph_vertex_index/_count' 2>/dev/null

# Also check the old index for comparison
kubectl exec -n $NS $POD -c $CONTAINER -- curl -s \
  'http://atlas-elasticsearch-master:9200/janusgraph_vertex_index/_count' 2>/dev/null
```

If ES doc counts are significantly different (>5% discrepancy), run ES-only reindex:

```bash
kubectl exec -n $NS $MIGRATE_POD -c $MIGRATE_CONTAINER -- \
  /opt/apache-atlas/bin/atlas_migrate.sh --es-only 2>&1
```

### 2.6 Run validation separately to confirm

```bash
kubectl exec -n $NS $MIGRATE_POD -c $MIGRATE_CONTAINER -- \
  /opt/apache-atlas/bin/atlas_migrate.sh --validate-only 2>&1
```

**Report validation results to user:**
- Vertex count: PASS/FAIL
- Edge count: PASS/FAIL
- GUID index: PASS/FAIL
- TypeDef presence: PASS/FAIL
- Sample properties: PASS/FAIL

If ANY validation check fails, STOP and report details. Use AskUserQuestion to decide whether to proceed or investigate.

---

## Phase 3: Switch Backend Configuration

### 3.1 Read current config to understand the setup

```bash
kubectl exec -n $NS $POD -c $CONTAINER -- bash -c \
  'grep -E "atlas.graphdb|atlas.cassandra|atlas.graph.index.search" /opt/apache-atlas/conf/atlas-application.properties' 2>/dev/null
```

### 3.2 Read Cassandra connection details from atlas-application.properties

```bash
# Get cassandra host (try new keys first, then JG keys)
CASS_HOST=$(kubectl exec -n $NS $POD -c $CONTAINER -- bash -c \
  'grep -E "^atlas.cassandra.graph.hostname|^atlas.graph.storage.hostname" /opt/apache-atlas/conf/atlas-application.properties | tail -1 | cut -d= -f2' | tr -d '[:space:]')

CASS_DC=$(kubectl exec -n $NS $POD -c $CONTAINER -- bash -c \
  'grep -E "^atlas.cassandra.graph.datacenter|^atlas.graph.storage.cql.local-datacenter" /opt/apache-atlas/conf/atlas-application.properties | tail -1 | cut -d= -f2' | tr -d '[:space:]')

echo "Cassandra host: $CASS_HOST, datacenter: $CASS_DC"
```

### 3.3 Edit the ConfigMap to switch backend

```bash
# Show what we're about to add/change
echo "Adding/updating these properties in ConfigMap atlas-config:"
echo "  atlas.graphdb.backend=cassandra"
echo "  atlas.cassandra.graph.hostname=$CASS_HOST"
echo "  atlas.cassandra.graph.port=9042"
echo "  atlas.cassandra.graph.keyspace=atlas_graph"
echo "  atlas.cassandra.graph.datacenter=$CASS_DC"
echo "  atlas.graph.index.search.es.prefix=atlas_graph_"
```

Use AskUserQuestion to confirm:
- Question: "Ready to switch the backend config to Cassandra? Editing the ConfigMap may trigger an implicit pod restart if the ConfigMap is mounted as a volume. Rollback is instant — flip back to `janus` and restart."
- Header: "Switch backend"
- Options:
  - "Yes, switch to Cassandra" — proceed
  - "No, stop here" — abort (migration data remains, can switch later)

If confirmed:

**Step 1: Back up the current ConfigMap**
```bash
kubectl get configmap atlas-config -n $NS -o yaml > /tmp/atlas-config-backup.yaml
echo "ConfigMap backed up to /tmp/atlas-config-backup.yaml"
```

**Step 2: Patch the ConfigMap**

The ConfigMap stores `atlas-application.properties` as a key. Patch it with the new properties:

```bash
# Read current properties, add/replace Zero Graph lines, write back
kubectl exec -n $NS $POD -c $CONTAINER -- bash -c '
  CONF=/opt/apache-atlas/conf/atlas-application.properties
  cat "$CONF"
' > /tmp/atlas-application-current.properties

# Remove any existing Zero Graph properties and add new ones
grep -v "^atlas.graphdb.backend=" /tmp/atlas-application-current.properties | \
grep -v "^atlas.cassandra.graph.hostname=" | \
grep -v "^atlas.cassandra.graph.port=" | \
grep -v "^atlas.cassandra.graph.keyspace=" | \
grep -v "^atlas.cassandra.graph.datacenter=" | \
grep -v "^atlas.graph.index.search.es.prefix=" > /tmp/atlas-application-new.properties

cat >> /tmp/atlas-application-new.properties <<PROPEOF

# ---- Zero Graph: Cassandra Backend ----
atlas.graphdb.backend=cassandra
atlas.cassandra.graph.hostname=$CASS_HOST
atlas.cassandra.graph.port=9042
atlas.cassandra.graph.keyspace=atlas_graph
atlas.cassandra.graph.datacenter=$CASS_DC
atlas.graph.index.search.es.prefix=atlas_graph_
PROPEOF

# Update ConfigMap with the new properties file
kubectl create configmap atlas-config -n $NS \
  --from-file=atlas-application.properties=/tmp/atlas-application-new.properties \
  --dry-run=client -o yaml | kubectl apply -f -

echo "ConfigMap updated."
```

**NOTE**: If the ConfigMap contains other keys beyond `atlas-application.properties`, use `kubectl edit configmap atlas-config -n $NS` instead of the replace approach above, to avoid clobbering other keys. Check first:
```bash
kubectl get configmap atlas-config -n $NS -o jsonpath='{.data}' | python3 -c "import json,sys; print('\n'.join(json.load(sys.stdin).keys()))"
```

### 3.4 Wait for pod restart (may be implicit from ConfigMap change)

ConfigMap changes may trigger an automatic pod restart if the ConfigMap is mounted as a volume. Check if the pod is already restarting:

```bash
sleep 5
POD_STATUS=$(kubectl get pod $POD -n $NS -o jsonpath='{.status.phase}' 2>/dev/null)
POD_READY=$(kubectl get pod $POD -n $NS -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)

if [ "$POD_READY" = "True" ]; then
  echo "Pod is still running. ConfigMap change did not trigger auto-restart."
  echo "Manually restarting..."
  kubectl delete pod $POD -n $NS
fi

echo "Waiting for pod to be ready..."
kubectl wait --for=condition=Ready pod/$POD -n $NS --timeout=300s
echo "Pod is ready."
```

### 3.5 Verify Cassandra backend loaded

```bash
# Check Atlas status
kubectl exec -n $NS $POD -c $CONTAINER -- curl -s http://localhost:21000/api/atlas/admin/status

# Check startup logs for Cassandra backend confirmation
kubectl logs $POD -n $NS -c $CONTAINER 2>/dev/null | grep -iE "graphdb|cassandra|backend|CassandraGraph" | head -10
```

Expected:
- `{"Status":"ACTIVE"}`
- Log lines showing `CassandraGraphDatabase` initialized
- No JanusGraph/TinkerPop/RepairIndex initialization

---

## Phase 4: Validation — Keycloak Token + Search + Auth Policy Check

### 4.1 Acquire Keycloak token

```bash
# Get credentials from pod environment
KC_SECRET=$(kubectl exec -n $NS $POD -c $CONTAINER -- printenv KEYCLOAK_CLIENT_SECRET 2>/dev/null)
KC_CLIENT=$(kubectl exec -n $NS $POD -c $CONTAINER -- printenv KEYCLOAK_CLIENT_ID 2>/dev/null)

# Fallback: read from keycloak.json
if [ -z "$KC_SECRET" ]; then
  KC_SECRET=$(kubectl exec -n $NS $POD -c $CONTAINER -- python3 -c \
    "import json; d=json.load(open('/opt/apache-atlas/conf/keycloak.json')); print(d['credentials']['secret'])" 2>/dev/null)
  KC_CLIENT=$(kubectl exec -n $NS $POD -c $CONTAINER -- python3 -c \
    "import json; d=json.load(open('/opt/apache-atlas/conf/keycloak.json')); print(d['resource'])" 2>/dev/null)
fi

# Fallback: hardcoded defaults
KC_CLIENT="${KC_CLIENT:-atlan-backend}"

echo "Keycloak client: $KC_CLIENT"

# Get token
TOKEN=$(kubectl exec -n $NS $POD -c $CONTAINER -- curl -s -X POST \
  'http://keycloak-http.keycloak.svc.cluster.local/auth/realms/default/protocol/openid-connect/token' \
  -d "client_id=$KC_CLIENT" \
  -d "client_secret=$KC_SECRET" \
  -d 'grant_type=client_credentials' \
  -d 'scope=openid' 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")

if [ -z "$TOKEN" ] || [ "$TOKEN" = "None" ]; then
  echo "ERROR: Failed to acquire Keycloak token. Trying alternate credentials..."
  TOKEN=$(kubectl exec -n $NS $POD -c $CONTAINER -- curl -s -X POST \
    'http://keycloak-http.keycloak.svc.cluster.local/auth/realms/default/protocol/openid-connect/token' \
    -d "client_id=atlan-backend" \
    -d "client_secret=28421fa2-6ec7-45f7-ccfa-0a05c1c4246b" \
    -d 'grant_type=client_credentials' \
    -d 'scope=openid' 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")
fi

echo "Token acquired: ${TOKEN:0:20}..."
```

### 4.2 Test index search — search for Table entities

```bash
kubectl exec -n $NS $POD -c $CONTAINER -- curl -s \
  -H "Authorization: Bearer $TOKEN" \
  'http://localhost:21000/api/atlas/v2/search/basic?typeName=Table&limit=5' | \
  python3 -c "
import json, sys
data = json.load(sys.stdin)
entities = data.get('entities', [])
count = data.get('approximateCount', 0)
print(f'Search returned {len(entities)} entities (approx total: {count})')
scrubbed_count = 0
for e in entities:
    a = e.get('attributes', {})
    scrubbed = e.get('scrubbed', False)
    if scrubbed:
        scrubbed_count += 1
    icon = ' [LOCK]' if scrubbed else ''
    print(f'  {a.get(\"qualifiedName\",\"?\")[:60]} | guid={e.get(\"guid\")[:12]}... | name={a.get(\"name\",\"?\")}{icon}')
if scrubbed_count > 0:
    print(f'\nWARNING: {scrubbed_count}/{len(entities)} entities have lock icons!')
else:
    print(f'\nAll entities accessible (no lock icons)')
"
```

**Key checks:**
- Search returns entities (not empty)
- Entities have attributes populated (not empty/null)
- No entities are marked `scrubbed` (which means lock icons)

### 4.3 Test index search — search for Connection entities

```bash
kubectl exec -n $NS $POD -c $CONTAINER -- curl -s \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -X POST 'http://localhost:21000/api/meta/search/indexsearch' \
  -d '{"dsl":{"from":0,"size":20,"query":{"bool":{"must":[{"term":{"__typeName.keyword":"Connection"}},{"term":{"__state":"ACTIVE"}}]}}}}' | \
  python3 -c "
import json, sys
data = json.load(sys.stdin)
entities = data.get('entities', [])
print(f'Connections found: {len(entities)}')
scrubbed_count = 0
for e in entities:
    a = e.get('attributes', {})
    scrubbed = e.get('scrubbed', False)
    if scrubbed:
        scrubbed_count += 1
    icon = ' [LOCK]' if scrubbed else ''
    print(f'  {a.get(\"name\",\"?\")} | guid={e.get(\"guid\")[:12]}... | policies={a.get(\"assetPoliciesCount\",0)}{icon}')
if scrubbed_count > 0:
    print(f'\nWARNING: {scrubbed_count} connections have lock icons!')
else:
    print(f'\nAll connections accessible (no lock icons)')
"
```

### 4.4 Verify AuthPolicy entities exist in ES

AuthPolicies must be in ES for Heka and PolicyRefresher to work. If they're missing, downstream services report "invalid connection".

```bash
# Count AuthPolicy docs in the NEW ES index
kubectl exec -n $NS $POD -c $CONTAINER -- curl -s \
  'http://atlas-elasticsearch-master:9200/atlas_graph_vertex_index/_count' \
  -H 'Content-Type: application/json' \
  -d '{"query":{"term":{"__typeName.keyword":"AuthPolicy"}}}' | \
  python3 -c "import json,sys; print(f'AuthPolicy docs in atlas_graph index: {json.load(sys.stdin).get(\"count\",0)}')"

# Count AuthService docs
kubectl exec -n $NS $POD -c $CONTAINER -- curl -s \
  'http://atlas-elasticsearch-master:9200/atlas_graph_vertex_index/_count' \
  -H 'Content-Type: application/json' \
  -d '{"query":{"term":{"__typeName.keyword":"AuthService"}}}' | \
  python3 -c "import json,sys; print(f'AuthService docs in atlas_graph index: {json.load(sys.stdin).get(\"count\",0)}')"
```

**Expected**: AuthPolicy count should be > 0 (typically 150+ for a tenant with connections). AuthService should be >= 1.

### 4.5 Handle lock icons or missing AuthPolicy — delegate to /debug-policies

If ANY of the following are true:
- Entities have lock icons (scrubbed=true) in steps 4.2 or 4.3
- AuthPolicy count is 0 in step 4.4
- Connections show `assetPoliciesCount=0`

Then there is a policy issue. **Invoke the `/debug-policies` skill** to diagnose and fix:

```
/debug-policies
```

The `debug-policies` skill will:
1. Check PolicyRefresher health
2. List connections and their policy counts
3. Verify bootstrap policies exist for each connection
4. Check Keycloak roles
5. Offer to create missing policies / roles

After debug-policies completes, re-run steps 4.2-4.4 to verify the fix.

### 4.6 Verify PolicyRefresher loaded policies

```bash
kubectl exec -n $NS $POD -c $CONTAINER -- bash -c \
  'grep -E "policy engine|policy count|PolicyRefresher" /opt/apache-atlas/logs/atlas.*.out 2>/dev/null | tail -10'
```

Expected: `This policy engine contains N policy evaluators` where N > 0.

### 4.7 Check Heka policy download endpoint

```bash
kubectl exec -n $NS $POD -c $CONTAINER -- curl -s \
  -H "Authorization: Bearer $TOKEN" \
  'http://localhost:21000/api/atlas/v2/auth/download/policies/heka?usePolicyDelta=false&lastUpdatedTime=0' | \
  python3 -c "
import json, sys
data = json.load(sys.stdin)
policies = data.get('policies', [])
print(f'Heka policies downloadable: {len(policies)}')
if len(policies) == 0:
    print('WARNING: No Heka policies found! Data queries via Heka will fail.')
else:
    print('Heka policy download OK')
"
```

### 4.8 Additional smoke tests

```bash
# Test TypeDefs
kubectl exec -n $NS $POD -c $CONTAINER -- curl -s \
  -H "Authorization: Bearer $TOKEN" \
  'http://localhost:21000/api/atlas/v2/types/typedefs/headers?limit=10' | \
  python3 -c "
import json, sys
headers = json.load(sys.stdin)
print(f'TypeDef headers returned: {len(headers)} types')
"
```

---

## Phase 5: Cleanup

### 5.1 If dedicated migration pod was created — clean it up

```bash
# Delete the migration pod
kubectl delete pod atlas-migrator -n $NS --force --grace-period=0 2>/dev/null
echo "Migration pod deleted."

# Delete the PDB
kubectl delete pdb atlas-migrator-pdb -n $NS 2>/dev/null
echo "PodDisruptionBudget deleted."

# Clean up local temp files
rm -f /tmp/atlas_migrate.sh /tmp/atlas-graphdb-migrator.jar /tmp/atlas-application.properties \
      /tmp/atlas-application-current.properties /tmp/atlas-application-new.properties \
      /tmp/atlas-config-backup.yaml 2>/dev/null
```

### 5.2 Clean up temp files from atlas pod

```bash
kubectl exec -n $NS $POD -c $CONTAINER -- rm -f /tmp/migrator.properties 2>/dev/null
```

---

## Phase 6: Final Report

Present a summary table to the user:

```
=== Zero Graph Migration Report ===

Vcluster:        <name>
Migration mode:  <atlas pod / dedicated pod>

| Phase                        | Status | Detail                              |
|------------------------------|--------|-------------------------------------|
| Prerequisites                | OK/FAIL | Build verified, migrator found      |
| ArgoCD disabled              | OK     | Confirmed by user                   |
| Migration (Scan+Write)       | OK/FAIL | X vertices, Y edges, Z token ranges |
| ES Reindex                   | OK/FAIL | N docs indexed                      |
| Validation                   | OK/FAIL | All checks PASS/FAIL                |
| Backend switch (ConfigMap)   | OK/FAIL | ConfigMap updated, pod restarted    |
| Atlas health                 | OK/FAIL | {"Status":"ACTIVE"}                 |
| Search works                 | OK/FAIL | N entities returned, no lock icons  |
| AuthPolicy in ES             | OK/FAIL | N policies found                    |
| PolicyRefresher              | OK/FAIL | N evaluators loaded                 |
| Heka policy download         | OK/FAIL | N policies downloadable             |
| Cleanup                      | OK/SKIP | Pod + PDB deleted                   |

Rollback command (if needed):
  # 1. Restore ConfigMap backup
  kubectl apply -f /tmp/atlas-config-backup.yaml
  # 2. Restart pod
  kubectl delete pod atlas-0 -n atlas
  # JanusGraph data was never modified — rollback is instant.
```

---

## Error Handling

Throughout execution, if any phase fails:

1. **Do NOT proceed to the next phase** — stop and report
2. **Always offer rollback** if backend was already switched:
   - Restore ConfigMap from backup (`kubectl apply -f /tmp/atlas-config-backup.yaml`)
   - Or manually set `atlas.graphdb.backend=janus` and `atlas.graph.index.search.es.prefix=janusgraph_`
   - Pod will restart (auto or manual `kubectl delete pod`)
   - JanusGraph data is never modified, so rollback is instant
3. **Clean up dedicated pod + PDB** even on failure — don't leave orphaned pods
4. **Migration is always resumable** — if it failed mid-way, rerun the same command
5. **If lock icons appear** — invoke `/debug-policies` skill to diagnose and fix

## Important Notes

- **Never modify JanusGraph data** — migration is copy-not-move
- **ES index prefix MUST match backend** — `atlas_graph_` for cassandra, `janusgraph_` for janus
- **Keycloak client secret may change** on pod restart — always read from env/config, don't hardcode
- **Large tenants (50M+ assets)** may need 4-8 hours for migration — use dedicated pod
- **ArgoCD**: If ArgoCD is re-enabled after migration, ensure the Helm values / source-of-truth config includes the new Cassandra properties, otherwise the next sync will revert to JanusGraph
- **ConfigMap edits may trigger implicit pod restart** — the skill handles this by checking pod status after the edit
- **The `deploy-jars.sh` script should NOT be used** — jars are already in the pod from the deployed build
- Reference docs: `docs/tenant-migration-runbook.md`, `graphdb/migrator/MIGRATION-STATUS.md`
