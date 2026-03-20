#!/usr/bin/env bash
#
# atlas_migrate_and_switch.sh — End-to-end JanusGraph -> CassandraGraph migration
#
# Architecture:
#   - A single migrator pod handles: token generation, preflight, maintenance ON,
#     migration, and ES alias switching (all cluster-internal work).
#   - The outer script (this file) handles: pod lifecycle, ConfigMap patching,
#     rolling restart, and guaranteed maintenance OFF.
#
# Maintenance mode safety:
#   Maintenance mode is ALWAYS disabled at the end, even on failure.
#   It is only disabled after Atlas is confirmed ACTIVE to avoid disabling
#   maintenance on a broken instance.
#
# Usage:
#   ./atlas_migrate_and_switch.sh \
#     --tenant <name> \
#     --client-secret <secret> \
#     --vcluster <name> \
#     [OPTIONS]
#

set -euo pipefail

# ============================================================================
# Constants
# ============================================================================

readonly SCRIPT_NAME="$(basename "$0")"
readonly TIMESTAMP="$(date +%Y%m%d-%H%M%S)"

readonly EXIT_SUCCESS=0
readonly EXIT_PREFLIGHT=1
readonly EXIT_MIGRATION=3
readonly EXIT_SWITCH=4

if [ -t 1 ]; then
    readonly C_RED='\033[0;31m' C_GREEN='\033[0;32m' C_YELLOW='\033[0;33m'
    readonly C_BLUE='\033[0;34m' C_CYAN='\033[0;36m' C_BOLD='\033[1m' C_RESET='\033[0m'
else
    readonly C_RED='' C_GREEN='' C_YELLOW='' C_BLUE='' C_CYAN='' C_BOLD='' C_RESET=''
fi

# ============================================================================
# Defaults
# ============================================================================

TENANT=""
TENANT_URL=""
CLIENT_SECRET=""
VCLUSTER=""
NAMESPACE="atlas"
POD="atlas-0"
CONTAINER="atlas-main"
CASSANDRA_POD="atlas-cassandra-0"
ID_STRATEGY="legacy"
CLAIM_ENABLED="false"
SCANNER_THREADS="16"
WRITER_THREADS="8"
ES_BULK_SIZE=""
ES_FIELD_LIMIT="10000"
JVM_HEAP="4g"
JVM_MIN_HEAP="2g"
MIGRATION_MODE="--fresh"
MIGRATOR_CPU="4"
MIGRATOR_MEMORY="8Gi"
MAX_RETRIES=3
WAIT_TIMEOUT=300
OLD_ES_INDEX="janusgraph_vertex_index"
NEW_ES_INDEX="atlas_graph_vertex_index"
VERTEX_ALIAS="atlas_vertex_index"
DRY_RUN="false"

# Derived at runtime
TENANT_FQDN=""
CONFIGMAP_BACKUP=""
LOG_FILE=""
MAINTENANCE_ENABLED="false"   # tracks whether we turned maintenance ON

# Phase tracking
PHASE_NAMES=()
PHASE_RESULTS=()

# ============================================================================
# Logging
# ============================================================================

log()  { echo -e "${C_CYAN}[$(date +%H:%M:%S)]${C_RESET} $*"; }
step() { echo -e "\n${C_BOLD}${C_BLUE}=== $* ===${C_RESET}"; }
ok()   { echo -e "${C_GREEN}  [PASS]${C_RESET} $*"; }
warn() { echo -e "${C_YELLOW}  [WARN]${C_RESET} $*" >&2; }
err()  { echo -e "${C_RED}  [FAIL]${C_RESET} $*" >&2; }
die()  { echo -e "${C_RED}[FATAL]${C_RESET} $*" >&2; exit "${2:-1}"; }

record_phase() {
    PHASE_NAMES+=("$1")
    PHASE_RESULTS+=("$2")
}

py_extract() {
    local script="$1"
    python3 -c "$script" 2>/dev/null || python -c "$script" 2>/dev/null || echo ""
}

# ============================================================================
# Usage
# ============================================================================

show_help() {
    cat <<'USAGE'
Usage: atlas_migrate_and_switch.sh --tenant <name> --client-secret <secret> --vcluster <name> [OPTIONS]

End-to-end JanusGraph -> CassandraGraph migration.
Creates a migrator pod that runs preflight, maintenance ON, migration, and alias
switching. Then patches ConfigMap and restarts Atlas. Maintenance mode is ALWAYS
disabled at the end (after Atlas is confirmed ACTIVE).

Required:
  --tenant <name>           Tenant name (e.g. "mycompany" -> mycompany.atlan.com)
  --client-secret <secret>  Keycloak client secret for atlan-argo
  --vcluster <name>         Target vcluster name (sets kubectl context)

Options:
  --tenant-url <fqdn>        Override tenant FQDN (default: <tenant>.atlan.com)
  --namespace <ns>          Kubernetes namespace (default: atlas)
  --pod <name>              Atlas pod for exec fallback (default: atlas-0)
  --container <name>        Container name (default: atlas-main)
  --cassandra-pod <name>    Cassandra pod name (default: atlas-cassandra-0)
  --id-strategy <s>         legacy|deterministic (default: legacy)
  --claim-enabled           Enable LWT dedup claims
  --scanner-threads <n>     Scanner parallelism (default: 16)
  --writer-threads <n>      Writer parallelism (default: 8)
  --es-bulk-size <n>        ES bulk batch size
  --es-field-limit <n>      ES field limit (default: 10000)
  --jvm-heap <size>         JVM max heap (default: 4g)
  --jvm-min-heap <size>     JVM initial heap (default: 2g)
  --fresh                   Clear state, start from scratch (default)
  --resume                  Resume from last checkpoint
  --es-only                 ES reindex only
  --validate-only           Validation only
  --migrator-cpu <n>        CPU for migrator pod (default: 4)
  --migrator-memory <sz>    Memory for migrator pod (default: 8Gi)
  --max-retries <n>         Max retries (default: 3)
  --wait-timeout <secs>     Pod readiness timeout (default: 300)
  --dry-run                 Show config, don't execute
  --help                    Show this help

Examples:
  # Standard migration
  ./atlas_migrate_and_switch.sh \
    --tenant mycompany --client-secret "abc123..." --vcluster mycompany --fresh

  # Custom tenant URL (when FQDN != <tenant>.atlan.com)
  ./atlas_migrate_and_switch.sh \
    --tenant miralgup02 --tenant-url miral-new.atlan.com \
    --client-secret "abc123..." --vcluster miralgup02 --fresh

  # Large tenant
  ./atlas_migrate_and_switch.sh \
    --tenant mycompany --client-secret "abc123..." --vcluster mycompany \
    --jvm-heap 48g --jvm-min-heap 16g \
    --scanner-threads 32 --writer-threads 16 \
    --migrator-cpu 16 --migrator-memory 64Gi
USAGE
}

# ============================================================================
# Argument parsing
# ============================================================================

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --tenant)          TENANT="$2"; shift 2 ;;
            --tenant-url)      TENANT_URL="$2"; shift 2 ;;
            --client-secret)   CLIENT_SECRET="$2"; shift 2 ;;
            --vcluster)        VCLUSTER="$2"; shift 2 ;;
            --namespace)       NAMESPACE="$2"; shift 2 ;;
            --pod)             POD="$2"; shift 2 ;;
            --container)       CONTAINER="$2"; shift 2 ;;
            --cassandra-pod)   CASSANDRA_POD="$2"; shift 2 ;;
            --id-strategy)     ID_STRATEGY="$2"; shift 2 ;;
            --claim-enabled)   CLAIM_ENABLED="true"; shift ;;
            --scanner-threads) SCANNER_THREADS="$2"; shift 2 ;;
            --writer-threads)  WRITER_THREADS="$2"; shift 2 ;;
            --es-bulk-size)    ES_BULK_SIZE="$2"; shift 2 ;;
            --es-field-limit)  ES_FIELD_LIMIT="$2"; shift 2 ;;
            --jvm-heap)        JVM_HEAP="$2"; shift 2 ;;
            --jvm-min-heap)    JVM_MIN_HEAP="$2"; shift 2 ;;
            --fresh)           MIGRATION_MODE="--fresh"; shift ;;
            --resume)          MIGRATION_MODE=""; shift ;;
            --es-only)         MIGRATION_MODE="--es-only"; shift ;;
            --validate-only)   MIGRATION_MODE="--validate-only"; shift ;;
            --migrator-cpu)    MIGRATOR_CPU="$2"; shift 2 ;;
            --migrator-memory) MIGRATOR_MEMORY="$2"; shift 2 ;;
            --max-retries)     MAX_RETRIES="$2"; shift 2 ;;
            --wait-timeout)    WAIT_TIMEOUT="$2"; shift 2 ;;
            --dry-run)         DRY_RUN="true"; shift ;;
            --help|-h)         show_help; exit 0 ;;
            *)                 die "Unknown option: $1 (use --help)" ;;
        esac
    done

    [ -z "$TENANT" ] && die "Missing required: --tenant <name>"
    [ -z "$CLIENT_SECRET" ] && die "Missing required: --client-secret <secret>"
    [ -z "$VCLUSTER" ] && die "Missing required: --vcluster <name>"

    if [ -n "$TENANT_URL" ]; then
        TENANT_FQDN="$TENANT_URL"
    else
        TENANT_FQDN="${TENANT}.atlan.com"
    fi
}

# ============================================================================
# Token generation (from operator workstation — for maintenance OFF)
# Token TTL is 15 minutes. Always generate fresh before each API call.
# ============================================================================

generate_token() {
    local response access_token
    response=$(curl -s --location "https://${TENANT_FQDN}/auth/realms/default/protocol/openid-connect/token" \
        --header 'Content-Type: application/x-www-form-urlencoded' \
        --data-urlencode 'client_id=atlan-argo' \
        --data-urlencode "client_secret=${CLIENT_SECRET}" \
        --data-urlencode 'grant_type=client_credentials' 2>/dev/null || echo "")

    access_token=$(echo "$response" | py_extract "import sys,json; print(json.load(sys.stdin).get('access_token',''))")

    if [ -z "$access_token" ]; then
        warn "Token generation failed (response: ${response:0:200})"
        echo ""
        return 1
    fi

    echo "$access_token"
}

# ============================================================================
# Wait for Atlas ACTIVE (via kubectl exec — reliable, no LB dependency)
# ============================================================================

wait_for_atlas_active() {
    local max_attempts="${1:-60}"  # default 10 minutes (60 x 10s)
    log "Waiting for Atlas to become ACTIVE (max ${max_attempts} attempts)..."

    for attempt in $(seq 1 "$max_attempts"); do
        local status
        status=$(kubectl exec "$POD" -n "$NAMESPACE" -c "$CONTAINER" -- \
            curl -s localhost:21000/api/atlas/admin/status 2>/dev/null | \
            py_extract "import sys,json; print(json.load(sys.stdin).get('Status','UNKNOWN'))" || echo "UNREACHABLE")

        if [ "$status" = "ACTIVE" ]; then
            ok "Atlas is ACTIVE (attempt $attempt)"
            return 0
        fi

        if [ "$((attempt % 6))" -eq 0 ]; then
            log "  Atlas status: $status (attempt $attempt/$max_attempts)"
        fi
        sleep 10
    done

    warn "Atlas did not reach ACTIVE after $max_attempts attempts"
    return 1
}

# ============================================================================
# Disable maintenance mode (from operator workstation — guaranteed execution)
# ============================================================================

disable_maintenance_mode() {
    if [ "$MAINTENANCE_ENABLED" != "true" ]; then
        log "Maintenance mode was not enabled by this script — skipping disable"
        return 0
    fi

    step "DISABLING MAINTENANCE MODE"

    # Wait for Atlas to be ACTIVE before disabling — critical safety check
    log "Waiting for Atlas to be ACTIVE before disabling maintenance mode..."
    local atlas_up="false"
    for retry in $(seq 1 3); do
        if wait_for_atlas_active 60; then
            atlas_up="true"
            break
        fi
        warn "Atlas not ACTIVE yet (retry $retry/3), waiting 30s..."
        sleep 30
    done

    if [ "$atlas_up" != "true" ]; then
        err "Atlas is NOT ACTIVE — cannot safely disable maintenance mode."
        err "MANUAL ACTION REQUIRED: disable maintenance mode once Atlas is healthy."
        err "  curl -X PUT 'https://${TENANT_FQDN}/api/meta/configs/MAINTENANCE_MODE' \\"
        err "    -H 'Content-Type: application/json' -H 'Authorization: Bearer <token>' \\"
        err "    -d '{\"value\": \"false\"}'"
        return 1
    fi

    # Generate fresh token (old one from pod may have expired)
    log "Generating fresh token for maintenance mode disable..."
    local token=""
    for retry in $(seq 1 5); do
        token=$(generate_token) && [ -n "$token" ] && break
        warn "Token generation attempt $retry/5 failed, retrying in 10s..."
        sleep 10
    done

    if [ -z "$token" ]; then
        err "Could not generate token after 5 attempts."
        err "MANUAL ACTION REQUIRED: disable maintenance mode."
        err "  curl -X PUT 'https://${TENANT_FQDN}/api/meta/configs/MAINTENANCE_MODE' \\"
        err "    -H 'Content-Type: application/json' -H 'Authorization: Bearer <token>' \\"
        err "    -d '{\"value\": \"false\"}'"
        return 1
    fi

    # Disable with retries
    for retry in $(seq 1 5); do
        local http_code
        http_code=$(curl -s -o /dev/null -w "%{http_code}" \
            -X PUT "https://${TENANT_FQDN}/api/meta/configs/MAINTENANCE_MODE" \
            -H "Accept: application/json" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer ${token}" \
            -d '{"value": "false"}' 2>/dev/null || echo "000")

        if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ] 2>/dev/null; then
            ok "Maintenance mode DISABLED (HTTP $http_code)"
            MAINTENANCE_ENABLED="false"

            # Verify
            local verify_resp
            verify_resp=$(curl -s \
                "https://${TENANT_FQDN}/api/meta/admin/config/MAINTENANCE_MODE" \
                -H "Accept: application/json" \
                -H "Authorization: Bearer ${token}" 2>/dev/null || echo "")
            log "  Verification response: $verify_resp"
            return 0
        fi

        warn "Maintenance OFF attempt $retry/5 returned HTTP $http_code, retrying in 10s..."
        sleep 10
    done

    err "Failed to disable maintenance mode after 5 attempts."
    err "MANUAL ACTION REQUIRED."
    return 1
}

# ============================================================================
# EXIT trap — guarantees maintenance mode is disabled
# ============================================================================

cleanup_on_exit() {
    local exit_code=$?

    if [ "$MAINTENANCE_ENABLED" = "true" ]; then
        echo ""
        warn "============================================================"
        warn "  Script exiting (code: $exit_code) — disabling maintenance"
        warn "============================================================"
        disable_maintenance_mode || true
    fi

    if [ -n "${CONFIGMAP_BACKUP:-}" ]; then
        log "ConfigMap backup: $CONFIGMAP_BACKUP"
        log "Rollback: kubectl apply -f $CONFIGMAP_BACKUP && kubectl rollout restart statefulset/atlas -n $NAMESPACE"
    fi
}

trap cleanup_on_exit EXIT

# ============================================================================
# ConfigMap patching helper
# ============================================================================

patch_configmap_properties() {
    local new_props_file="$1"

    local key_count
    key_count=$(kubectl get configmap atlas-config -n "$NAMESPACE" -o json | \
        py_extract "import sys,json; print(len(json.load(sys.stdin).get('data',{})))")

    if [ "$key_count" -gt 1 ] 2>/dev/null; then
        log "  ConfigMap has $key_count keys — using JSON merge patch"
        local escaped_content
        escaped_content=$(py_extract "
import json
with open('$new_props_file') as f:
    content = f.read()
print(json.dumps(content))
")
        kubectl patch configmap atlas-config -n "$NAMESPACE" \
            --type merge -p "{\"data\":{\"atlas-application.properties\":${escaped_content}}}"
    else
        kubectl create configmap atlas-config -n "$NAMESPACE" \
            --from-file=atlas-application.properties="$new_props_file" \
            --dry-run=client -o yaml | kubectl apply -f -
    fi
}

# ============================================================================
# Phase 0: Basic connectivity (from operator workstation)
# ============================================================================

phase_connectivity() {
    step "Phase 0: Connectivity Checks"

    # kubectl context
    log "Setting kubectl context to vcluster: $VCLUSTER"
    if ! kubectl config use-context "$VCLUSTER" >/dev/null 2>&1; then
        if command -v vcluster >/dev/null 2>&1; then
            vcluster platform connect "$VCLUSTER" >/dev/null 2>&1 || true
        fi
        if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
            die "Cannot access namespace '$NAMESPACE' in vcluster '$VCLUSTER'" "$EXIT_PREFLIGHT"
        fi
    fi
    ok "kubectl context set"

    # Pod exists and is running
    local pod_status
    pod_status=$(kubectl get pod "$POD" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "NotFound")
    if [ "$pod_status" != "Running" ]; then
        die "Pod $POD is not Running (status: $pod_status)" "$EXIT_PREFLIGHT"
    fi
    ok "Pod $POD is Running"

    # Token generation test
    log "Testing token generation for ${TENANT_FQDN}..."
    local test_token
    test_token=$(generate_token)
    if [ -n "$test_token" ]; then
        ok "Token generation successful"
    else
        die "Cannot generate token for ${TENANT_FQDN} — check --client-secret" "$EXIT_PREFLIGHT"
    fi

    # Config summary
    step "Configuration Summary"
    log "  Tenant:            ${TENANT_FQDN}"
    log "  Vcluster:          $VCLUSTER"
    log "  Namespace:         $NAMESPACE"
    log "  Pod:               $POD"
    log "  ID Strategy:       $ID_STRATEGY"
    log "  Claim Enabled:     $CLAIM_ENABLED"
    log "  Migration Mode:    ${MIGRATION_MODE:-resume}"
    log "  Migrator CPU:      $MIGRATOR_CPU"
    log "  Migrator Memory:   $MIGRATOR_MEMORY"
    log "  Scanner Threads:   $SCANNER_THREADS"
    log "  Writer Threads:    $WRITER_THREADS"
    log "  JVM Heap:          $JVM_HEAP (min: $JVM_MIN_HEAP)"
    log "  Max Retries:       $MAX_RETRIES"
    echo ""

    if [ "$DRY_RUN" = "true" ]; then
        log "Dry-run mode — exiting."
        MAINTENANCE_ENABLED="false"  # prevent cleanup trap from firing
        exit "$EXIT_SUCCESS"
    fi

    record_phase "Phase 0: Connectivity" "PASS"
}

# ============================================================================
# Phase 1: Create migrator pod (preflight + maintenance ON + migration + alias)
# ============================================================================

phase_migration_pod() {
    step "Phase 1: Migration Pod (preflight + maintenance + migration + alias)"

    # ---- Read runtime info from Atlas pod ----
    log "Reading runtime info from $POD..."
    local atlas_image
    atlas_image=$(kubectl get pod "$POD" -n "$NAMESPACE" \
        -o jsonpath="{.spec.containers[?(@.name=='${CONTAINER}')].image}" 2>/dev/null || echo "")
    [ -z "$atlas_image" ] && die "Could not determine container image from pod $POD" "$EXIT_MIGRATION"
    log "  Image: $atlas_image"

    # imagePullSecrets
    local image_pull_secrets_yaml=""
    local pull_secret_names
    pull_secret_names=$(kubectl get pod "$POD" -n "$NAMESPACE" \
        -o jsonpath='{.spec.imagePullSecrets[*].name}' 2>/dev/null || echo "")
    if [ -n "$pull_secret_names" ]; then
        image_pull_secrets_yaml="  imagePullSecrets:"
        for secret_name in $pull_secret_names; do
            image_pull_secrets_yaml="${image_pull_secrets_yaml}
  - name: ${secret_name}"
        done
    fi

    # Tolerations
    local tolerations_yaml=""
    local tolerations_json
    tolerations_json=$(kubectl get pod "$POD" -n "$NAMESPACE" \
        -o jsonpath='{.spec.tolerations}' 2>/dev/null || echo "")
    if [ -n "$tolerations_json" ] && [ "$tolerations_json" != "[]" ] && [ "$tolerations_json" != "null" ]; then
        tolerations_yaml=$(py_extract "
import json
tolerations = json.loads('''${tolerations_json}''')
lines = ['  tolerations:']
for t in tolerations:
    first = True
    for k in ('key', 'operator', 'value', 'effect', 'tolerationSeconds'):
        if k in t and t[k] is not None:
            prefix = '  - ' if first else '    '
            lines.append(f'{prefix}{k}: \"{t[k]}\"' if isinstance(t[k], str) else f'{prefix}{k}: {t[k]}')
            first = False
    if first:
        lines.append('  - operator: \"Exists\"')
print('\n'.join(lines))
")
    fi

    # Multitenant envFrom
    local multitenant_env_yaml=""
    if kubectl get secret atlas-secret-manager -n "$NAMESPACE" >/dev/null 2>&1; then
        multitenant_env_yaml="    - secretRef:
        name: atlas-secret-manager
    - secretRef:
        name: atlas-secret-parameter-store"
        log "  Multitenant: yes"
    fi

    # ---- Retry loop ----
    local attempt=0
    local migration_passed="false"
    local pod_manifest="/tmp/migrator-pod-${VCLUSTER}-${TIMESTAMP}.yaml"

    while [ "$attempt" -lt "$MAX_RETRIES" ] && [ "$migration_passed" = "false" ]; do
        attempt=$((attempt + 1))
        log "Migration attempt $attempt/$MAX_RETRIES"

        local mode_flag="$MIGRATION_MODE"
        if [ "$attempt" -gt 1 ]; then
            mode_flag=""  # Resume from failed token ranges
        fi

        # Clean up existing migrator pods
        kubectl delete pods -n "$NAMESPACE" -l app=atlas-migrator --ignore-not-found 2>/dev/null || true
        sleep 3

        local migrator_pod_name="atlas-migrator-${attempt}"

        # ES_BULK_SIZE env (conditional)
        local es_bulk_env=""
        if [ -n "$ES_BULK_SIZE" ]; then
            es_bulk_env="      - name: ES_BULK_SIZE
        value: \"${ES_BULK_SIZE}\""
        fi

        # ---- Generate Pod manifest ----
        # The pod's inline script handles:
        #   Step 1: Generate Keycloak token
        #   Step 2: Preflight checks (ES, Cassandra, migrator JAR)
        #   Step 3: Enable maintenance mode (external API)
        #   Step 4: Run migration (atlas_migrate.sh)
        #   Step 5: ES alias switching
        cat > "$pod_manifest" <<EOYAML
apiVersion: v1
kind: Pod
metadata:
  name: ${migrator_pod_name}
  namespace: ${NAMESPACE}
  labels:
    app: atlas-migrator
spec:
  restartPolicy: Never
${image_pull_secrets_yaml}
${tolerations_yaml}
  affinity:
    nodeAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        preference:
          matchExpressions:
          - key: eks.amazonaws.com/capacityType
            operator: In
            values:
            - ON_DEMAND
      - weight: 100
        preference:
          matchExpressions:
          - key: lifecycle
            operator: In
            values:
            - ondemand
      - weight: 100
        preference:
          matchExpressions:
          - key: cloud.google.com/gke-provisioning
            operator: In
            values:
            - standard
  containers:
  - name: atlas-migrator
    image: "${atlas_image}"
    imagePullPolicy: IfNotPresent
    command:
    - /bin/bash
    - -c
    - |
      set -eo pipefail

      TENANT_FQDN="\${MIGRATOR_TENANT_FQDN}"
      CLIENT_SECRET="\${MIGRATOR_CLIENT_SECRET}"

      echo "=========================================="
      echo "  Atlas Migrator Pod"
      echo "=========================================="
      echo "Tenant:      \${TENANT_FQDN}"
      echo "Mode:        ${mode_flag:-resume}"
      echo "ID Strategy: ${ID_STRATEGY}"
      echo ""

      # ============================================================
      # STEP 1: Verify Keycloak Token Generation
      # ============================================================
      echo "=== Step 1: Verifying Keycloak connectivity ==="

      # Token TTL is 15 minutes. We regenerate a fresh token before
      # every API call rather than caching one at the start.
      generate_token() {
          local resp token
          resp=\$(curl -s --location "https://\${TENANT_FQDN}/auth/realms/default/protocol/openid-connect/token" \
              --header 'Content-Type: application/x-www-form-urlencoded' \
              --data-urlencode 'client_id=atlan-argo' \
              --data-urlencode "client_secret=\${CLIENT_SECRET}" \
              --data-urlencode 'grant_type=client_credentials' 2>/dev/null || echo "")
          token=\$(echo "\$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null || echo "")
          echo "\$token"
      }

      # Verify that token generation works (don't cache — will regenerate before use)
      TEST_TOKEN=\$(generate_token)
      if [ -z "\$TEST_TOKEN" ]; then
          echo "FATAL: Could not generate Keycloak token"
          exit 1
      fi
      echo "Keycloak token generation verified."

      # ============================================================
      # STEP 2: Preflight Checks
      # ============================================================
      echo ""
      echo "=== Step 2: Preflight Checks ==="

      # Read ES config from atlas-application.properties
      PROPS_FILE="/opt/apache-atlas/conf/atlas-application.properties"
      ES_HOSTPORT=\$(grep '^atlas.graph.index.search.hostname=' "\$PROPS_FILE" 2>/dev/null | cut -d= -f2 | tr -d '[:space:]' || echo "localhost:9200")
      ES_HOST=\$(echo "\$ES_HOSTPORT" | cut -d: -f1)
      ES_PORT=\$(echo "\$ES_HOSTPORT" | cut -d: -f2 -s)
      ES_PORT=\${ES_PORT:-9200}
      ES_BASE="http://\${ES_HOST}:\${ES_PORT}"

      CASS_HOST=\$(grep '^atlas.graph.storage.hostname=' "\$PROPS_FILE" 2>/dev/null | cut -d= -f2 | tr -d '[:space:]' || echo "")
      if [ -z "\$CASS_HOST" ]; then
          CASS_HOST=\$(grep '^atlas.cassandra.graph.hostname=' "\$PROPS_FILE" 2>/dev/null | cut -d= -f2 | tr -d '[:space:]' || echo "atlas-cassandra")
      fi

      # Check ES
      ES_STATUS=\$(curl -s -o /dev/null -w "%{http_code}" "\${ES_BASE}/" 2>/dev/null || echo "000")
      if [ "\$ES_STATUS" = "200" ]; then
          echo "[PASS] Elasticsearch reachable at \${ES_BASE}"
      else
          echo "[FAIL] Elasticsearch not reachable at \${ES_BASE} (HTTP \$ES_STATUS)"
          exit 1
      fi

      # Check Cassandra
      if nc -z -w3 "\$CASS_HOST" 9042 2>/dev/null; then
          echo "[PASS] Cassandra reachable at \${CASS_HOST}:9042"
      else
          echo "[WARN] Cassandra port 9042 not reachable via nc (migration may still work via driver)"
      fi

      # Check migrator JAR
      MIGRATOR_JAR=""
      for candidate in \
          /opt/apache-atlas/tools/atlas-graphdb-migrator.jar \
          /opt/apache-atlas/libext/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar \
          /opt/jar-overrides/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar; do
          if [ -f "\$candidate" ]; then
              MIGRATOR_JAR="\$candidate"
              break
          fi
      done
      if [ -z "\$MIGRATOR_JAR" ] && [ ! -f /opt/apache-atlas/bin/atlas_migrate.sh ]; then
          echo "[FAIL] Migrator tools not found"
          exit 1
      fi
      echo "[PASS] Migrator tools present"

      echo "Preflight checks passed."

      # ============================================================
      # STEP 3: Enable Maintenance Mode
      # ============================================================
      echo ""
      echo "=== Step 3: Enabling Maintenance Mode ==="

      # Generate fresh token (15-min TTL — preflight may have consumed time)
      echo "Generating fresh token for maintenance mode..."
      TOKEN=\$(generate_token)
      if [ -z "\$TOKEN" ]; then
          echo "FATAL: Could not generate token for maintenance mode"
          exit 1
      fi

      MAINT_HTTP=\$(curl -s -o /dev/null -w "%{http_code}" \
          -X PUT "https://\${TENANT_FQDN}/api/meta/configs/MAINTENANCE_MODE" \
          -H "Accept: application/json" \
          -H "Content-Type: application/json" \
          -H "Authorization: Bearer \${TOKEN}" \
          -d '{"value": "true"}' 2>/dev/null || echo "000")

      if [ "\$MAINT_HTTP" -ge 200 ] && [ "\$MAINT_HTTP" -lt 300 ] 2>/dev/null; then
          echo "[PASS] Maintenance mode enabled (HTTP \$MAINT_HTTP)"
      else
          echo "[WARN] Maintenance mode API returned HTTP \$MAINT_HTTP — proceeding anyway"
      fi

      # Verify
      MAINT_VERIFY=\$(curl -s \
          "https://\${TENANT_FQDN}/api/meta/admin/config/MAINTENANCE_MODE" \
          -H "Accept: application/json" \
          -H "Authorization: Bearer \${TOKEN}" 2>/dev/null || echo "")
      echo "Maintenance mode verification: \$MAINT_VERIFY"

      # Write a marker file so the outer script knows maintenance was enabled
      echo "true" > /tmp/maintenance_enabled

      # ============================================================
      # STEP 4: Run Migration
      # ============================================================
      echo ""
      echo "=== Step 4: Running Migration ==="

      export ID_STRATEGY="${ID_STRATEGY}"
      export CLAIM_ENABLED="${CLAIM_ENABLED}"
      export SCANNER_THREADS="${SCANNER_THREADS}"
      export WRITER_THREADS="${WRITER_THREADS}"
      export ES_BULK_SIZE="${ES_BULK_SIZE:-1000}"
      export MIGRATOR_JVM_HEAP="${JVM_HEAP}"
      export MIGRATOR_JVM_MIN_HEAP="${JVM_MIN_HEAP}"
      export SOURCE_CONSISTENCY="ONE"
      export TARGET_CONSISTENCY="LOCAL_QUORUM"

      MIGRATOR_EXIT=0
      /opt/apache-atlas/bin/atlas_migrate.sh ${mode_flag} || MIGRATOR_EXIT=\$?

      if [ "\$MIGRATOR_EXIT" -ne 0 ]; then
          echo "[FAIL] Migration failed with exit code: \$MIGRATOR_EXIT"
          exit \$MIGRATOR_EXIT
      fi
      echo "[PASS] Migration completed successfully."

      # ============================================================
      # STEP 5: ES Alias Switching
      # ============================================================
      echo ""
      echo "=== Step 5: ES Alias Switching ==="

      # Check target index exists
      INDEX_STATUS=\$(curl -s -o /dev/null -w "%{http_code}" "\${ES_BASE}/${NEW_ES_INDEX}" 2>/dev/null || echo "000")
      if [ "\$INDEX_STATUS" != "200" ]; then
          echo "[WARN] Target index ${NEW_ES_INDEX} not found (HTTP \$INDEX_STATUS) — skipping alias switching"
      else
          # Switch vertex alias
          echo "Switching vertex alias (${VERTEX_ALIAS} -> ${NEW_ES_INDEX})..."
          ALIAS_RESP=\$(curl -s -X POST "\${ES_BASE}/_aliases" \
              -H 'Content-Type: application/json' \
              -d '{
                  "actions": [
                      {"remove": {"index": "*", "alias": "${VERTEX_ALIAS}"}},
                      {"add": {"index": "${NEW_ES_INDEX}", "alias": "${VERTEX_ALIAS}"}}
                  ]
              }' 2>/dev/null || echo '{}')
          echo "Vertex alias response: \$ALIAS_RESP"

          # Verify
          VERIFY=\$(curl -s "\${ES_BASE}/_alias/${VERTEX_ALIAS}" 2>/dev/null || echo "{}")
          echo "Vertex alias now points to: \$(echo "\$VERIFY" | python3 -c "import sys,json; print(','.join(json.load(sys.stdin).keys()))" 2>/dev/null || echo "unknown")"

          # Migrate persona aliases from old index to new index
          echo ""
          echo "Migrating persona aliases from ${OLD_ES_INDEX} to ${NEW_ES_INDEX}..."

          OLD_STATUS=\$(curl -s -o /dev/null -w "%{http_code}" "\${ES_BASE}/${OLD_ES_INDEX}" 2>/dev/null || echo "000")
          if [ "\$OLD_STATUS" != "200" ]; then
              echo "Old index ${OLD_ES_INDEX} not found — skipping persona alias migration"
          else
              cat > /tmp/migrate_aliases.py << 'PYEOF'
      import json, sys, urllib.request
      es_base = sys.argv[1]
      old_index = sys.argv[2]
      new_index = sys.argv[3]
      skip_aliases = set(sys.argv[4].split(','))
      try:
          req = urllib.request.Request(f'{es_base}/{old_index}/_alias/*')
          with urllib.request.urlopen(req, timeout=30) as resp:
              data = json.loads(resp.read())
      except Exception as e:
          print(f'Failed to fetch aliases: {e}')
          sys.exit(0)
      aliases = data.get(old_index, {}).get('aliases', {})
      persona_aliases = {n: c for n, c in aliases.items() if n not in skip_aliases}
      print(f'Found {len(persona_aliases)} persona aliases to migrate')
      if not persona_aliases:
          sys.exit(0)
      migrated = failed = 0
      items = list(persona_aliases.items())
      for i in range(0, len(items), 50):
          batch = items[i:i+50]
          actions = []
          for name, cfg in batch:
              actions.append({'remove': {'index': old_index, 'alias': name}})
              add_action = {'index': new_index, 'alias': name}
              if 'filter' in cfg:
                  add_action['filter'] = cfg['filter']
              actions.append({'add': add_action})
          payload = json.dumps({'actions': actions}).encode()
          try:
              req = urllib.request.Request(f'{es_base}/_aliases', data=payload,
                  headers={'Content-Type': 'application/json'}, method='POST')
              with urllib.request.urlopen(req, timeout=60) as resp:
                  result = json.loads(resp.read())
              if result.get('acknowledged'):
                  migrated += len(batch)
              else:
                  for name, cfg in batch:
                      sa = [{'remove': {'index': old_index, 'alias': name}},
                            {'add': {'index': new_index, 'alias': name, **({'filter': cfg['filter']} if 'filter' in cfg else {})}}]
                      try:
                          r2 = urllib.request.Request(f'{es_base}/_aliases',
                              data=json.dumps({'actions': sa}).encode(),
                              headers={'Content-Type': 'application/json'}, method='POST')
                          with urllib.request.urlopen(r2, timeout=30) as resp2:
                              if json.loads(resp2.read()).get('acknowledged'):
                                  migrated += 1
                              else:
                                  failed += 1
                      except:
                          failed += 1
          except Exception as e:
              failed += len(batch)
              print(f'Batch failed: {e}')
      print(f'Persona aliases: migrated={migrated}, failed={failed}')
      PYEOF
              python3 /tmp/migrate_aliases.py "\${ES_BASE}" "${OLD_ES_INDEX}" "${NEW_ES_INDEX}" "${VERTEX_ALIAS},${OLD_ES_INDEX},${NEW_ES_INDEX}" 2>/dev/null || echo "Persona alias script error (non-fatal)"
          fi

          # Final count
          echo ""
          ALIAS_COUNT=\$(curl -s "\${ES_BASE}/${NEW_ES_INDEX}/_alias/*" 2>/dev/null | \
              python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('${NEW_ES_INDEX}',{}).get('aliases',{})))" 2>/dev/null || echo "?")
          echo "Total aliases on ${NEW_ES_INDEX}: \$ALIAS_COUNT"
      fi

      echo ""
      echo "=========================================="
      echo "  All in-pod work complete. Exiting."
      echo "=========================================="
      exit 0
    env:
    - name: MIGRATOR_TENANT_FQDN
      value: "${TENANT_FQDN}"
    - name: MIGRATOR_CLIENT_SECRET
      value: "${CLIENT_SECRET}"
    - name: ATLAS_SERVER_OPTS
      value: '-XX:MaxRAMPercentage=80.0 -XX:InitialRAMPercentage=50.0'
    - name: K8S_POD_NAME
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
    - name: Namespace
      valueFrom:
        fieldRef:
          fieldPath: metadata.namespace
${es_bulk_env}
    envFrom:
    - secretRef:
        name: atlas-keycloak-config
${multitenant_env_yaml}
    resources:
      requests:
        cpu: "${MIGRATOR_CPU}"
        memory: "${MIGRATOR_MEMORY}"
      limits:
        cpu: "${MIGRATOR_CPU}"
        memory: "${MIGRATOR_MEMORY}"
    volumeMounts:
    - name: atlas-config
      mountPath: /opt/apache-atlas/conf/atlas-application.properties
      subPath: atlas-application.properties
    - name: atlas-logback-config
      mountPath: /opt/apache-atlas/conf/atlas-logback.xml
      subPath: atlas-logback.xml
    - name: atlas-logs
      mountPath: /opt/apache-atlas/logs
  volumes:
  - name: atlas-config
    configMap:
      name: atlas-config
  - name: atlas-logback-config
    configMap:
      name: atlas-logback-config
  - name: atlas-logs
    emptyDir: {}
EOYAML

        # ---- Deploy pod ----
        log "Applying migrator pod manifest..."
        if ! kubectl apply -f "$pod_manifest"; then
            warn "kubectl apply failed on attempt $attempt"
            if [ "$attempt" -lt "$MAX_RETRIES" ]; then
                sleep 10; continue
            fi
            die "kubectl apply failed after $MAX_RETRIES attempts" "$EXIT_MIGRATION"
        fi
        ok "Pod $migrator_pod_name applied"

        # Mark that maintenance mode will be enabled by the pod
        # (so EXIT trap knows to disable it)
        MAINTENANCE_ENABLED="true"

        # ---- Wait for pod to start ----
        log "Waiting for migrator pod to start..."
        local job_pod=""
        for i in $(seq 1 60); do
            job_pod=$(kubectl get pods -n "$NAMESPACE" -l app=atlas-migrator \
                --field-selector=status.phase=Running \
                -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
            if [ -n "$job_pod" ]; then break; fi
            if [ "$((i % 6))" -eq 0 ]; then
                log "  Waiting for pod to start... (${i}x5s)"
            fi
            sleep 5
        done

        if [ -z "$job_pod" ]; then
            warn "Migrator pod did not start within 5 minutes"
            if [ "$attempt" -lt "$MAX_RETRIES" ]; then
                sleep 10; continue
            fi
            die "Migrator pod did not start after $MAX_RETRIES attempts" "$EXIT_MIGRATION"
        fi

        # ---- Stream logs and wait for completion ----
        log "Streaming logs from $job_pod..."
        kubectl logs -f "$job_pod" -n "$NAMESPACE" 2>/dev/null || true

        # ---- Check outcome ----
        # Wait briefly for pod status to settle
        sleep 5
        local job_status exit_code
        job_status=$(kubectl get pod "$job_pod" -n "$NAMESPACE" \
            -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
        exit_code=$(kubectl get pod "$job_pod" -n "$NAMESPACE" \
            -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || echo "")

        if [ "$exit_code" = "0" ] || [ "$job_status" = "Succeeded" ]; then
            migration_passed="true"
            ok "Migration pod completed successfully (attempt $attempt)"
        else
            warn "Attempt $attempt failed (status: $job_status, exit: ${exit_code:-unknown})"
            if [ "$attempt" -lt "$MAX_RETRIES" ]; then
                log "Retrying... (resumable — picks up from failed token ranges)"
                sleep 10
            fi
        fi
    done

    rm -f "$pod_manifest"

    if [ "$migration_passed" = "false" ]; then
        record_phase "Phase 1: Migration Pod" "FAIL (after $MAX_RETRIES attempts)"
        die "Migration failed after $MAX_RETRIES attempts" "$EXIT_MIGRATION"
    fi

    record_phase "Phase 1: Migration Pod" "PASS (attempt $attempt/$MAX_RETRIES)"
}

# ============================================================================
# Phase 2: Backend Switch (ConfigMap + Rolling Restart)
# ============================================================================

phase_switch() {
    step "Phase 2: Backend Switch"

    # Backup ConfigMap
    log "Backing up ConfigMap..."
    CONFIGMAP_BACKUP="/tmp/atlas-config-backup-${VCLUSTER}-${TIMESTAMP}.yaml"
    kubectl get configmap atlas-config -n "$NAMESPACE" -o yaml > "$CONFIGMAP_BACKUP"
    ok "ConfigMap backed up to $CONFIGMAP_BACKUP"

    # Extract and patch properties
    log "Patching ConfigMap with Cassandra backend properties..."

    local props_file="/tmp/atlas-props-${VCLUSTER}-${TIMESTAMP}.txt"
    local props_clean="/tmp/atlas-props-clean-${VCLUSTER}-${TIMESTAMP}.txt"

    kubectl get configmap atlas-config -n "$NAMESPACE" \
        -o jsonpath='{.data.atlas-application\.properties}' > "$props_file"

    # Remove existing graph backend lines
    grep -v \
        -e '^atlas\.graphdb\.backend=' \
        -e '^atlas\.graph\.index\.search\.es\.prefix=' \
        -e '^atlas\.cassandra\.graph\.' \
        -e '^atlas\.graph\.id\.strategy=' \
        -e '^atlas\.graph\.claim\.enabled=' \
        -e '^# === Zero Graph' \
        -e '^# ---- Zero Graph' \
        -e '^# ---- Reverted to JanusGraph' \
        -e '^# ---- Temporarily reverted' \
        "$props_file" > "$props_clean" || true

    # Read Cassandra hostname
    local cass_hostname
    cass_hostname=$(grep '^atlas.graph.storage.hostname=' "$props_file" | cut -d= -f2 | head -1 || echo "")
    [ -z "$cass_hostname" ] && cass_hostname=$(grep '^atlas.cassandra.graph.hostname=' "$props_file" | cut -d= -f2 | head -1 || echo "atlas-cassandra")

    # Read datacenter
    local cass_dc
    cass_dc=$(grep '^atlas.cassandra.graph.datacenter=' "$props_file" | cut -d= -f2 | head -1 || echo "")
    [ -z "$cass_dc" ] && cass_dc=$(grep '^atlas.graph.storage.cql.local-datacenter=' "$props_file" | cut -d= -f2 | head -1 || echo "")
    [ -z "$cass_dc" ] && cass_dc="datacenter1"
    log "  Cassandra: host=$cass_hostname, dc=$cass_dc"

    cat >> "$props_clean" << EOF

# === Zero Graph Backend (Cassandra + ES direct) ===
atlas.graphdb.backend=cassandra
atlas.graph.index.search.es.prefix=atlas_graph_
atlas.cassandra.graph.hostname=${cass_hostname}
atlas.cassandra.graph.port=9042
atlas.cassandra.graph.keyspace=atlas_graph
atlas.cassandra.graph.datacenter=${cass_dc}
atlas.graph.id.strategy=${ID_STRATEGY}
atlas.graph.claim.enabled=${CLAIM_ENABLED}
EOF

    patch_configmap_properties "$props_clean"
    ok "ConfigMap patched"
    rm -f "$props_file" "$props_clean"

    # Verify
    local verify_backend
    verify_backend=$(kubectl get configmap atlas-config -n "$NAMESPACE" \
        -o jsonpath='{.data.atlas-application\.properties}' | \
        grep '^atlas.graphdb.backend=' | cut -d= -f2 | head -1 || echo "")
    if [ "$verify_backend" = "cassandra" ]; then
        ok "ConfigMap verified: atlas.graphdb.backend=cassandra"
    else
        warn "ConfigMap verification: backend='$verify_backend' (expected 'cassandra')"
    fi

    # Rolling restart
    log "Triggering rolling restart..."
    if ! kubectl rollout restart statefulset/atlas -n "$NAMESPACE"; then
        warn "rollout restart failed — falling back to pod delete"
        kubectl delete pod "$POD" -n "$NAMESPACE" || true
    fi

    log "Waiting for rollout to complete (timeout: 600s)..."
    if ! kubectl rollout status statefulset/atlas -n "$NAMESPACE" --timeout=600s; then
        warn "ConfigMap backup: $CONFIGMAP_BACKUP"
        die "Rollout did not complete within 600s" "$EXIT_SWITCH"
    fi
    ok "All pods updated"

    # Verify Atlas ACTIVE on all replicas
    log "Verifying Atlas is ACTIVE on all pods..."
    local replicas
    replicas=$(kubectl get statefulset atlas -n "$NAMESPACE" \
        -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "1")
    local all_active="true"

    for i in $(seq 0 $((replicas - 1))); do
        local pod_name="atlas-${i}"
        local status="UNKNOWN"
        for attempt in $(seq 1 30); do
            status=$(kubectl exec "$pod_name" -n "$NAMESPACE" -c "$CONTAINER" -- \
                curl -s localhost:21000/api/atlas/admin/status 2>/dev/null | \
                py_extract "import sys,json; print(json.load(sys.stdin).get('Status','UNKNOWN'))" || echo "STARTING")
            if [ "$status" = "ACTIVE" ]; then break; fi
            sleep 10
        done
        if [ "$status" = "ACTIVE" ]; then
            ok "  $pod_name: ACTIVE"
        else
            warn "  $pod_name: $status"
            all_active="false"
        fi
    done

    if [ "$all_active" = "true" ]; then
        record_phase "Phase 2: Backend Switch" "PASS"
    else
        record_phase "Phase 2: Backend Switch" "WARN (not all pods ACTIVE)"
    fi
}

# ============================================================================
# Phase 3: Disable Maintenance Mode (guaranteed by EXIT trap, also called explicitly)
# ============================================================================

phase_maintenance_off() {
    step "Phase 3: Disable Maintenance Mode"
    disable_maintenance_mode
    record_phase "Phase 3: Maintenance OFF" "DONE"
}

# ============================================================================
# Summary
# ============================================================================

phase_summary() {
    step "Migration Summary"
    echo ""
    echo "============================================================"
    echo "  MIGRATION REPORT: $VCLUSTER ($TENANT_FQDN)"
    echo "============================================================"
    echo ""
    for i in "${!PHASE_NAMES[@]}"; do
        printf "  %-35s %s\n" "${PHASE_NAMES[$i]}" "${PHASE_RESULTS[$i]}"
    done
    echo ""
    if [ -n "${CONFIGMAP_BACKUP:-}" ]; then
        echo "  Rollback:"
        echo "    kubectl apply -f $CONFIGMAP_BACKUP"
        echo "    kubectl rollout restart statefulset/atlas -n $NAMESPACE"
    fi
    echo "  Log file: ${LOG_FILE}"
    echo "============================================================"
}

# ============================================================================
# Main
# ============================================================================

main() {
    parse_args "$@"

    LOG_FILE="/tmp/atlas-migrate-${VCLUSTER}-${TIMESTAMP}.log"
    exec > >(tee -a "$LOG_FILE") 2>&1
    log "Log file: $LOG_FILE"

    step "Atlas Migration: $VCLUSTER ($TENANT_FQDN)"
    log "Started at $(date -u +%Y-%m-%dT%H:%M:%SZ)"

    phase_connectivity       # Phase 0: kubectl + token test
    phase_migration_pod      # Phase 1: Pod does preflight + maint ON + migration + alias
    phase_switch             # Phase 2: ConfigMap patch + rolling restart
    phase_maintenance_off    # Phase 3: Disable maintenance mode (explicit)
    phase_summary

    ok "Migration complete for $VCLUSTER"
    exit "$EXIT_SUCCESS"
}

main "$@"
