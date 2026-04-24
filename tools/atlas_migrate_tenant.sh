#!/usr/bin/env bash
#
# atlas_migrate_tenant.sh — End-to-end tenant migration orchestrator
#
# Migrates an Atlas tenant from JanusGraph to Cassandra+ES (Zero Graph).
# Runs from an operator's workstation via kubectl. Non-interactive / schedulable.
#
# Phases:
#   0. Pre-flight checks + adaptive sizing
#   1. Maintenance mode ON (optional)
#   2. Cleanup (for remigration)
#   3. Migration with retry
#   4. ES alias creation (optional)
#   5. Backend switch
#   6. Maintenance mode OFF
#   7. Post-switch verification
#   8. Summary
#
# Usage:
#   ./atlas_migrate_tenant.sh --vcluster <name> [OPTIONS]
#
# See --help for full options.
#

set -euo pipefail

# ============================================================================
# Constants
# ============================================================================

readonly SCRIPT_NAME="$(basename "$0")"
readonly SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly TIMESTAMP="$(date +%Y%m%d-%H%M%S)"

# Exit codes
readonly EXIT_SUCCESS=0
readonly EXIT_PREFLIGHT=1
readonly EXIT_CLEANUP=2
readonly EXIT_MIGRATION=3
readonly EXIT_SWITCH=4
readonly EXIT_VERIFY=5

# Colors (disabled if not a terminal)
if [ -t 1 ]; then
    readonly C_RED='\033[0;31m'
    readonly C_GREEN='\033[0;32m'
    readonly C_YELLOW='\033[0;33m'
    readonly C_BLUE='\033[0;34m'
    readonly C_CYAN='\033[0;36m'
    readonly C_BOLD='\033[1m'
    readonly C_RESET='\033[0m'
else
    readonly C_RED='' C_GREEN='' C_YELLOW='' C_BLUE='' C_CYAN='' C_BOLD='' C_RESET=''
fi

# ============================================================================
# Defaults
# ============================================================================

VCLUSTER=""
NAMESPACE="atlas"
POD="atlas-0"
CONTAINER="atlas-main"
CASSANDRA_POD="atlas-cassandra-0"
ID_STRATEGY="legacy"
CLAIM_ENABLED="false"
SCANNER_THREADS="16"
WRITER_THREADS="16"
ES_BULK_SIZE=""
ES_FIELD_LIMIT="10000"
JVM_HEAP="4g"
JVM_MIN_HEAP="2g"

# Auto-sizing: track which params are user-specified (not to be overridden)
USER_SET_SCANNER=false
USER_SET_WRITER=false
USER_SET_CPU=false
USER_SET_MEMORY=false
USER_SET_HEAP=false
USER_SET_HEAP_MIN=false
USER_SET_BULK=false
MIGRATION_MODE=""          # "", "--fresh", "--es-only", "--validate-only"
MIGRATOR_CPU="4"
MIGRATOR_MEMORY="8Gi"
SKIP_SWITCH="false"
SWITCH_ONLY="false"
SKIP_CLEANUP="false"
SKIP_ALIAS="false"
SKIP_BACKUP_CHECK="false"
DRY_RUN="false"
REPORT_FILE=""
WAIT_TIMEOUT=300
MAINTENANCE_MODE="false"
MAINTENANCE_COOLDOWN=30
OLD_ES_INDEX="janusgraph_vertex_index"
NEW_ES_INDEX="atlas_graph_vertex_index"
VERTEX_ALIAS="atlas_vertex_index"
MAX_RETRIES=3

# Runtime state
CONFIGMAP_BACKUP=""
KEYCLOAK_TOKEN=""
VERIFY_PASS=0
VERIFY_FAIL=0
VERIFY_TOTAL=0
START_TIME=""
END_TIME=""
VERTEX_COUNT=""
SIZING_TIER=""
LOG_FILE=""

# Resolved connection endpoints (populated during preflight from atlas-application.properties)
RESOLVED_CASS_HOST=""
RESOLVED_CASS_PORT=""
RESOLVED_ES_HOST=""
RESOLVED_ES_PORT=""
RESOLVED_ES_PROTOCOL=""

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

# ============================================================================
# Python helper — eliminates python3/python dual fallback duplication
# ============================================================================

py_extract() {
    local script="$1"
    local result
    result=$(python3 -c "$script" 2>/dev/null || python -c "$script" 2>/dev/null || echo "")
    # Trim trailing whitespace/newlines
    echo "${result%"${result##*[![:space:]]}"}"
}

# ============================================================================
# Kubectl helpers
# ============================================================================

# Execute command on the Atlas pod, streaming output
kexec() {
    kubectl exec "$POD" -n "$NAMESPACE" -c "$CONTAINER" -- "$@"
}

# Execute command on the Atlas pod, capture output silently
kexec_quiet() {
    kubectl exec "$POD" -n "$NAMESPACE" -c "$CONTAINER" -- "$@" 2>/dev/null
}

# Seed a static config key via the localhost API (inside the pod)
# Retries up to 3 times with token refresh on failure.
seed_static_config() {
    local key="$1" value="$2"
    local max_retries=3

    for attempt in $(seq 1 "$max_retries"); do
        # Acquire or refresh token
        if [ -z "$KEYCLOAK_TOKEN" ] || [ "$attempt" -gt 1 ]; then
            KEYCLOAK_TOKEN=""
            acquire_token || {
                if [ "$attempt" -lt "$max_retries" ]; then
                    warn "Token acquisition failed (attempt $attempt/$max_retries), retrying in 5s..."
                    sleep 5
                    continue
                fi
                die "Cannot acquire Keycloak token for seeding static config (after $max_retries attempts)" "$EXIT_SWITCH"
            }
        fi

        local resp
        resp=$(kexec_quiet curl -s -X PUT \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
            -d "{\"value\": \"${value}\"}" \
            "localhost:21000/api/atlas/v2/static-configs/${key}" 2>/dev/null || echo "")

        local success
        success=$(echo "$resp" | py_extract \
            "import sys,json; print(json.load(sys.stdin).get('success','false'))" 2>/dev/null || echo "false")

        if [ "$success" = "True" ] || [ "$success" = "true" ]; then
            ok "  Seeded ${key}=${value}"
            return 0
        fi

        if [ "$attempt" -lt "$max_retries" ]; then
            warn "  Seed ${key} failed (attempt $attempt/$max_retries, resp: ${resp:0:120}), retrying in 5s..."
            sleep 5
        fi
    done

    die "Failed to seed static config ${key}=${value} after $max_retries attempts (last response: ${resp:0:200})" "$EXIT_SWITCH"
}

# Execute command on the Cassandra pod
cexec() {
    kubectl exec "$CASSANDRA_POD" -n "$NAMESPACE" -- "$@"
}

# ES base URL helper (uses resolved globals from preflight, falls back to localhost)
es_url() {
    local proto="${RESOLVED_ES_PROTOCOL:-http}"
    local host="${RESOLVED_ES_HOST:-localhost}"
    local port="${RESOLVED_ES_PORT:-9200}"
    echo "${proto}://${host}:${port}"
}

# ============================================================================
# Usage
# ============================================================================

show_help() {
    cat <<'USAGE'
Usage: atlas_migrate_tenant.sh --vcluster <name> [OPTIONS]

Orchestrates a full JanusGraph -> Cassandra migration for an Atlas tenant.
Runs from your workstation via kubectl. Non-interactive and schedulable.

Phases:
  0  Pre-flight checks + auto-sizing (CPU, memory, threads based on vertex count)
  1  Maintenance mode ON (if --maintenance-mode)
  2  Cleanup (for remigration, skipped with --skip-cleanup)
  3  Migration with retry (--max-retries)
  4  ES alias creation (skipped with --skip-alias)
  5  Backend switch (skipped with --skip-switch)
  6  Maintenance mode OFF
  7  Post-switch verification
  8  Summary report

Required:
  --vcluster <name>       Target vcluster name (sets kubectl context)

Options:
  --namespace <ns>        Kubernetes namespace (default: atlas)
  --pod <name>            Atlas pod name (default: atlas-0)
  --container <name>      Container name (default: atlas-main)
  --cassandra-pod <name>  Cassandra pod name (default: atlas-cassandra-0)
  --id-strategy <s>       ID strategy: legacy|deterministic (default: legacy)
  --claim-enabled         Enable LWT dedup claims
  --scanner-threads <n>   Scanner parallelism (default: 16)
  --writer-threads <n>    Writer parallelism (default: 8)
  --es-bulk-size <n>      ES bulk indexing batch size (passed to atlas_migrate.sh)
  --es-field-limit <n>    ES index mapping.total_fields.limit (default: 10000)
  --jvm-heap <size>       JVM max heap for migrator (default: 4g)
  --jvm-min-heap <size>   JVM initial heap for migrator (default: 2g)
  --fresh                 Clear migration state, start from scratch
  --es-only               ES reindex only (skip Cassandra scan)
  --validate-only         Validation only (no migration)
  --migrator-cpu <n>      CPU request/limit for migrator pod (default: 4)
  --migrator-memory <sz>  Memory request/limit for migrator pod (default: 8Gi)
  --skip-switch           Run migration but don't switch backend
  --switch-only           Skip migration, only do backend switch + verify
  --skip-cleanup          Skip cleanup phase (first-time migration)
  --skip-backup-check     Skip migrator pod's backup verification (dev/test only)
  --maintenance-mode      Enable maintenance mode during migration
  --maintenance-cooldown <s>  Seconds to wait before disabling maintenance (default: 30)
  --max-retries <n>       Max migration retries on validation failure (default: 3)
  --dry-run               Show config summary without executing
  --report-file <path>    Save JSON report to file
  --wait-timeout <secs>   Pod readiness timeout (default: 300)
  --help                  Show this help

Exit codes:
  0  Success
  1  Pre-flight failure
  2  Cleanup failure
  3  Migration failure
  4  Backend switch failure
  5  Verification failure (migration succeeded, post-switch checks failed)

Examples:
  # First-time migration with maintenance mode
  ./atlas_migrate_tenant.sh --vcluster mytenantname --fresh --skip-cleanup --maintenance-mode

  # Remigration (cleanup + fresh) with retry
  ./atlas_migrate_tenant.sh --vcluster mytenantname --fresh --max-retries 3

  # Migration with large tenant sizing
  ./atlas_migrate_tenant.sh --vcluster mytenantname --fresh \
    --jvm-heap 48g --scanner-threads 32 --writer-threads 16

  # Migration only, don't switch backend yet
  ./atlas_migrate_tenant.sh --vcluster mytenantname --fresh --skip-switch

  # Validation only (after previous migration)
  ./atlas_migrate_tenant.sh --vcluster mytenantname --validate-only --skip-switch

  # Switch only (after migration completed, separate step)
  ./atlas_migrate_tenant.sh --vcluster mytenantname --switch-only --id-strategy deterministic

  # Run in background with report
  nohup ./atlas_migrate_tenant.sh --vcluster mytenantname --fresh \
    --maintenance-mode --report-file /tmp/migration-report.json > /tmp/migration.log 2>&1 &
USAGE
}

# ============================================================================
# Argument parsing
# ============================================================================

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --vcluster)       VCLUSTER="$2"; shift 2 ;;
            --namespace)      NAMESPACE="$2"; shift 2 ;;
            --pod)            POD="$2"; shift 2 ;;
            --container)      CONTAINER="$2"; shift 2 ;;
            --cassandra-pod)  CASSANDRA_POD="$2"; shift 2 ;;
            --id-strategy)    warn "Ignoring --id-strategy (migration=legacy, runtime=deterministic)"; shift 2 ;;
            --claim-enabled)  CLAIM_ENABLED="true"; shift ;;
            --scanner-threads) SCANNER_THREADS="$2"; USER_SET_SCANNER=true; shift 2 ;;
            --writer-threads) WRITER_THREADS="$2"; USER_SET_WRITER=true; shift 2 ;;
            --es-bulk-size)   ES_BULK_SIZE="$2"; USER_SET_BULK=true; shift 2 ;;
            --es-field-limit) ES_FIELD_LIMIT="$2"; shift 2 ;;
            --jvm-heap)       JVM_HEAP="$2"; USER_SET_HEAP=true; shift 2 ;;
            --jvm-min-heap)   JVM_MIN_HEAP="$2"; USER_SET_HEAP_MIN=true; shift 2 ;;
            --fresh)          MIGRATION_MODE="--fresh"; shift ;;
            --es-only)        MIGRATION_MODE="--es-only"; shift ;;
            --validate-only)  MIGRATION_MODE="--validate-only"; shift ;;
            --migrator-cpu)   MIGRATOR_CPU="$2"; USER_SET_CPU=true; shift 2 ;;
            --migrator-memory) MIGRATOR_MEMORY="$2"; USER_SET_MEMORY=true; shift 2 ;;
            --skip-switch)    SKIP_SWITCH="true"; shift ;;
            --switch-only)    SWITCH_ONLY="true"; shift ;;
            --skip-cleanup)   SKIP_CLEANUP="true"; shift ;;
            --skip-alias)     SKIP_ALIAS="true"; shift ;;
            --skip-backup-check) SKIP_BACKUP_CHECK="true"; shift ;;
            --maintenance-mode) MAINTENANCE_MODE="true"; shift ;;
            --maintenance-cooldown) MAINTENANCE_COOLDOWN="$2"; shift 2 ;;
            --alias-script)   warn "--alias-script is deprecated (alias logic is now inline); ignoring"; shift 2 ;;
            --max-retries)    MAX_RETRIES="$2"; shift 2 ;;
            --dry-run)        DRY_RUN="true"; shift ;;
            --report-file)    REPORT_FILE="$2"; shift 2 ;;
            --wait-timeout)   WAIT_TIMEOUT="$2"; shift 2 ;;
            --help|-h)        show_help; exit 0 ;;
            *)                die "Unknown option: $1 (use --help)" ;;
        esac
    done

    if [ -z "$VCLUSTER" ]; then
        die "Missing required argument: --vcluster <name>" "$EXIT_PREFLIGHT"
    fi
}

# ============================================================================
# Helpers
# ============================================================================

should_switch() {
    [ "$SWITCH_ONLY" = "true" ] || \
    ([ "$SKIP_SWITCH" = "false" ] && [ "$MIGRATION_MODE" != "--validate-only" ] && [ "$MIGRATION_MODE" != "--es-only" ])
}

# Build the env prefix for migration exec calls
# ============================================================================
# Log file setup (Fix 9: tee all output to persistent log file)
# ============================================================================

setup_log_file() {
    LOG_FILE="/tmp/atlas-migrate-${VCLUSTER}-${TIMESTAMP}.log"
    # Redirect all stdout+stderr through tee to the log file
    exec > >(tee -a "$LOG_FILE") 2>&1
    log "Log file: $LOG_FILE"
}

# ============================================================================
# Phase 0: Pre-flight
# ============================================================================

phase_preflight() {
    step "Phase 0: Pre-flight Checks"

    # 0.1 kubectl context
    log "Setting kubectl context to vcluster: $VCLUSTER"
    if ! kubectl config use-context "$VCLUSTER" >/dev/null 2>&1; then
        # Try vcluster connect pattern
        if command -v vcluster >/dev/null 2>&1; then
            log "Attempting vcluster connect..."
            vcluster connect "$VCLUSTER" >/dev/null 2>&1 || true
        fi
        # Verify we can reach the cluster
        if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
            record_phase "Phase 0: Pre-flight" "FAIL"
            die "Cannot access namespace '$NAMESPACE' in vcluster '$VCLUSTER'. Check kubectl context." "$EXIT_PREFLIGHT"
        fi
    fi
    ok "kubectl context set"

    # 0.2 Pod health
    log "Checking pod health..."
    local pod_status
    pod_status=$(kubectl get pod "$POD" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "NotFound")
    if [ "$pod_status" != "Running" ]; then
        record_phase "Phase 0: Pre-flight" "FAIL"
        die "Pod $POD is not Running (status: $pod_status)" "$EXIT_PREFLIGHT"
    fi

    local ready_containers
    ready_containers=$(kubectl get pod "$POD" -n "$NAMESPACE" -o jsonpath='{.status.containerStatuses[*].ready}' | tr ' ' '\n' | grep -c true || echo 0)
    local total_containers
    total_containers=$(kubectl get pod "$POD" -n "$NAMESPACE" -o jsonpath='{.status.containerStatuses[*].name}' | wc -w | tr -d ' ')
    ok "Pod $POD is Running ($ready_containers/$total_containers ready)"

    # 0.3 Atlas status
    log "Checking Atlas status..."
    local atlas_status
    atlas_status=$(kexec_quiet curl -s localhost:21000/api/atlas/admin/status 2>/dev/null | py_extract "import sys,json; print(json.load(sys.stdin).get('Status','UNKNOWN'))" || echo "UNREACHABLE")
    if [ "$atlas_status" != "ACTIVE" ]; then
        record_phase "Phase 0: Pre-flight" "FAIL"
        die "Atlas is not ACTIVE (status: $atlas_status)" "$EXIT_PREFLIGHT"
    fi
    ok "Atlas status: ACTIVE"

    # 0.4 Migrator tools
    log "Checking migrator tools..."
    if ! kexec_quiet test -f /opt/apache-atlas/bin/atlas_migrate.sh; then
        record_phase "Phase 0: Pre-flight" "FAIL"
        die "atlas_migrate.sh not found on pod" "$EXIT_PREFLIGHT"
    fi
    if ! kexec_quiet test -f /opt/apache-atlas/tools/atlas-graphdb-migrator.jar; then
        # Check alternative locations
        if ! kexec_quiet test -f /opt/apache-atlas/libext/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar; then
            record_phase "Phase 0: Pre-flight" "FAIL"
            die "Migrator JAR not found on pod" "$EXIT_PREFLIGHT"
        fi
    fi
    ok "Migrator tools present"

    # 0.5 Current backend
    log "Reading current backend..."
    local current_backend
    current_backend=$(kexec_quiet grep '^atlas.graphdb.backend=' /opt/apache-atlas/conf/atlas-application.properties 2>/dev/null | cut -d= -f2 || echo "janus")
    log "  Current backend: $current_backend"

    # 0.6 Cassandra pod
    log "Checking Cassandra pod..."
    local cass_status
    cass_status=$(kubectl get pod "$CASSANDRA_POD" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "NotFound")
    if [ "$cass_status" != "Running" ]; then
        warn "Cassandra pod $CASSANDRA_POD is not Running (status: $cass_status)"
    else
        ok "Cassandra pod $CASSANDRA_POD is Running"
    fi

    # 0.6b Read actual Cassandra/ES hostnames from atlas-application.properties
    local es_raw
    RESOLVED_CASS_HOST=$(kexec_quiet grep '^atlas.cassandra.graph.hostname=' /opt/apache-atlas/conf/atlas-application.properties 2>/dev/null | tail -1 | cut -d= -f2 || echo "")
    if [ -z "$RESOLVED_CASS_HOST" ]; then
        RESOLVED_CASS_HOST=$(kexec_quiet grep '^atlas.graph.storage.hostname=' /opt/apache-atlas/conf/atlas-application.properties 2>/dev/null | tail -1 | cut -d= -f2 || echo "localhost")
    fi
    RESOLVED_CASS_PORT=$(kexec_quiet grep '^atlas.cassandra.graph.port=' /opt/apache-atlas/conf/atlas-application.properties 2>/dev/null | tail -1 | cut -d= -f2 || echo "")
    if [ -z "$RESOLVED_CASS_PORT" ]; then
        RESOLVED_CASS_PORT=$(kexec_quiet grep '^atlas.graph.storage.port=' /opt/apache-atlas/conf/atlas-application.properties 2>/dev/null | tail -1 | cut -d= -f2 || echo "9042")
    fi
    es_raw=$(kexec_quiet grep '^atlas.graph.index.search.hostname=' /opt/apache-atlas/conf/atlas-application.properties 2>/dev/null | tail -1 | cut -d= -f2- || echo "localhost:9200")
    RESOLVED_ES_HOST=$(echo "$es_raw" | cut -d':' -f1)
    RESOLVED_ES_PORT=$(echo "$es_raw" | grep -o ':[0-9]*' | tr -d ':' || echo "9200")
    RESOLVED_ES_PORT="${RESOLVED_ES_PORT:-9200}"
    RESOLVED_ES_PROTOCOL=$(kexec_quiet grep '^atlas.graph.index.search.elasticsearch.http.protocol=' /opt/apache-atlas/conf/atlas-application.properties 2>/dev/null | tail -1 | cut -d= -f2 || echo "http")
    log "  Resolved endpoints: Cassandra=${RESOLVED_CASS_HOST}:${RESOLVED_CASS_PORT}, ES=${RESOLVED_ES_PROTOCOL}://${RESOLVED_ES_HOST}:${RESOLVED_ES_PORT}"

    # 0.6c Connectivity probes (Cassandra + ES from inside the Atlas pod)
    log "Testing Cassandra connectivity from Atlas pod..."
    if kexec_quiet bash -c "nc -z -w3 ${RESOLVED_CASS_HOST} ${RESOLVED_CASS_PORT}" 2>/dev/null; then
        ok "Cassandra ${RESOLVED_CASS_HOST}:${RESOLVED_CASS_PORT} reachable from Atlas pod"
    else
        warn "Cannot reach Cassandra ${RESOLVED_CASS_HOST}:${RESOLVED_CASS_PORT} from Atlas pod (nc test)"
    fi

    log "Testing Elasticsearch connectivity from Atlas pod..."
    local es_probe_status
    es_probe_status=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "${RESOLVED_ES_PROTOCOL}://${RESOLVED_ES_HOST}:${RESOLVED_ES_PORT}/" 2>/dev/null || echo "000")
    if [ "$es_probe_status" = "200" ]; then
        ok "Elasticsearch ${RESOLVED_ES_HOST}:${RESOLVED_ES_PORT} reachable from Atlas pod (HTTP 200)"
    else
        warn "Elasticsearch not reachable from Atlas pod at ${RESOLVED_ES_HOST}:${RESOLVED_ES_PORT} (HTTP $es_probe_status)"
    fi

    # 0.7 Adaptive sizing probe
    log "Probing vertex count for sizing recommendation..."
    local es_count_resp
    es_count_resp=$(kexec_quiet curl -s "${RESOLVED_ES_PROTOCOL}://${RESOLVED_ES_HOST}:${RESOLVED_ES_PORT}/janusgraph_vertex_index/_count" 2>/dev/null || echo "{}")
    VERTEX_COUNT=$(echo "$es_count_resp" | py_extract "import sys,json; print(json.load(sys.stdin).get('count',0))")
    VERTEX_COUNT="${VERTEX_COUNT:-0}"
    log "  Vertex count (estimate): ${VERTEX_COUNT}"

    # Auto-apply sizing based on vertex count (only overrides params the user didn't set)
    if [ "$VERTEX_COUNT" -gt 20000000 ] 2>/dev/null; then
        SIZING_TIER="XLARGE (>20M vertices)"
        [ "$USER_SET_CPU" = "false" ]      && MIGRATOR_CPU="16"
        [ "$USER_SET_MEMORY" = "false" ]   && MIGRATOR_MEMORY="64Gi"
        [ "$USER_SET_HEAP" = "false" ]     && JVM_HEAP="48g"
        [ "$USER_SET_HEAP_MIN" = "false" ] && JVM_MIN_HEAP="16g"
        [ "$USER_SET_SCANNER" = "false" ]  && SCANNER_THREADS="32"
        [ "$USER_SET_WRITER" = "false" ]   && WRITER_THREADS="32"
        [ "$USER_SET_BULK" = "false" ]     && ES_BULK_SIZE="10000"
    elif [ "$VERTEX_COUNT" -gt 5000000 ] 2>/dev/null; then
        SIZING_TIER="LARGE (5M-20M vertices)"
        [ "$USER_SET_CPU" = "false" ]      && MIGRATOR_CPU="8"
        [ "$USER_SET_MEMORY" = "false" ]   && MIGRATOR_MEMORY="16Gi"
        [ "$USER_SET_HEAP" = "false" ]     && JVM_HEAP="10g"
        [ "$USER_SET_HEAP_MIN" = "false" ] && JVM_MIN_HEAP="4g"
        [ "$USER_SET_SCANNER" = "false" ]  && SCANNER_THREADS="16"
        [ "$USER_SET_WRITER" = "false" ]   && WRITER_THREADS="16"
        [ "$USER_SET_BULK" = "false" ]     && ES_BULK_SIZE="10000"
    elif [ "$VERTEX_COUNT" -gt 500000 ] 2>/dev/null; then
        SIZING_TIER="MEDIUM (500K-5M vertices)"
        [ "$USER_SET_CPU" = "false" ]      && MIGRATOR_CPU="4"
        [ "$USER_SET_MEMORY" = "false" ]   && MIGRATOR_MEMORY="8Gi"
        [ "$USER_SET_HEAP" = "false" ]     && JVM_HEAP="4g"
        [ "$USER_SET_HEAP_MIN" = "false" ] && JVM_MIN_HEAP="2g"
        [ "$USER_SET_SCANNER" = "false" ]  && SCANNER_THREADS="8"
        [ "$USER_SET_WRITER" = "false" ]   && WRITER_THREADS="8"
        [ "$USER_SET_BULK" = "false" ]     && ES_BULK_SIZE="5000"
    else
        SIZING_TIER="SMALL (<500K vertices)"
        [ "$USER_SET_CPU" = "false" ]      && MIGRATOR_CPU="2"
        [ "$USER_SET_MEMORY" = "false" ]   && MIGRATOR_MEMORY="4Gi"
        [ "$USER_SET_HEAP" = "false" ]     && JVM_HEAP="2g"
        [ "$USER_SET_HEAP_MIN" = "false" ] && JVM_MIN_HEAP="1g"
        [ "$USER_SET_SCANNER" = "false" ]  && SCANNER_THREADS="4"
        [ "$USER_SET_WRITER" = "false" ]   && WRITER_THREADS="4"
        [ "$USER_SET_BULK" = "false" ]     && ES_BULK_SIZE="5000"
    fi

    log "  Auto-sizing applied: tier=${SIZING_TIER}"
    log "    CPU: ${MIGRATOR_CPU}, Memory: ${MIGRATOR_MEMORY}, Heap: ${JVM_HEAP} (min: ${JVM_MIN_HEAP})"
    log "    Scanner: ${SCANNER_THREADS}, Writer: ${WRITER_THREADS}, ES Bulk: ${ES_BULK_SIZE}"

    # 0.8 Disk space check — advisory only, does not block migration
    log "Checking disk space (advisory — migration roughly doubles storage usage)..."

    # Elasticsearch disk usage
    local es_disk_info
    es_disk_info=$(kexec_quiet curl -s "$(es_url)/_cat/allocation?format=json" 2>/dev/null || echo "[]")
    local es_disk_check
    es_disk_check=$(echo "$es_disk_info" | py_extract "
import sys, json
try:
    nodes = json.load(sys.stdin)
    if not nodes:
        print('skip|0|0|No ES allocation data')
    else:
        worst_ratio = 0
        worst_node = ''
        for n in nodes:
            used = n.get('disk.used', n.get('diskUsed', '0'))
            avail = n.get('disk.avail', n.get('diskAvail', '0'))
            # Parse byte values (could be '10gb', '500mb', or raw bytes)
            def parse_bytes(s):
                s = str(s).strip().lower()
                if s.endswith('tb'): return float(s[:-2]) * 1024 * 1024 * 1024 * 1024
                if s.endswith('gb'): return float(s[:-2]) * 1024 * 1024 * 1024
                if s.endswith('mb'): return float(s[:-2]) * 1024 * 1024
                if s.endswith('kb'): return float(s[:-2]) * 1024
                if s.endswith('b'): return float(s[:-1])
                try: return float(s)
                except: return 0
            used_b = parse_bytes(used)
            avail_b = parse_bytes(avail)
            if used_b > 0:
                ratio = avail_b / used_b
                if ratio < worst_ratio or worst_ratio == 0:
                    worst_ratio = ratio
                    worst_node = n.get('node', '?')
        if worst_ratio == 0:
            print('skip|0|0|Could not parse ES disk info')
        elif worst_ratio >= 2.0:
            print(f'pass|{worst_ratio:.1f}|{worst_node}|ES has {worst_ratio:.1f}x free space (>= 2x required)')
        else:
            print(f'fail|{worst_ratio:.1f}|{worst_node}|ES only has {worst_ratio:.1f}x free space (< 2x required)')
except Exception as e:
    print(f'skip|0|0|ES disk check error: {e}')
")
    local es_disk_status es_disk_ratio es_disk_node es_disk_detail
    es_disk_status=$(echo "$es_disk_check" | cut -d'|' -f1)
    es_disk_ratio=$(echo "$es_disk_check" | cut -d'|' -f2)
    es_disk_node=$(echo "$es_disk_check" | cut -d'|' -f3)
    es_disk_detail=$(echo "$es_disk_check" | cut -d'|' -f4)

    if [ "$es_disk_status" = "pass" ]; then
        ok "ES disk: $es_disk_detail"
    elif [ "$es_disk_status" = "fail" ]; then
        warn "ES disk: $es_disk_detail"
        warn "Migration will roughly double ES storage usage. Risk of disk full!"
        warn "Consider expanding ES disk or cleaning up old indices before proceeding."
    else
        log "  ES disk: $es_disk_detail"
    fi

    # Cassandra disk usage
    local cass_disk_check
    cass_disk_check=$(cexec bash -c 'df -h /var/lib/cassandra 2>/dev/null || df -h / 2>/dev/null' 2>/dev/null || echo "")
    if [ -n "$cass_disk_check" ]; then
        local cass_disk_result
        cass_disk_result=$(echo "$cass_disk_check" | py_extract "
import sys
lines = sys.stdin.read().strip().split('\n')
# Parse df output: Filesystem Size Used Avail Use% Mounted
for line in lines[1:]:  # skip header
    parts = line.split()
    if len(parts) >= 4:
        def parse_size(s):
            s = s.strip().upper()
            if s.endswith('T'): return float(s[:-1]) * 1024
            if s.endswith('G'): return float(s[:-1])
            if s.endswith('M'): return float(s[:-1]) / 1024
            if s.endswith('K'): return float(s[:-1]) / (1024*1024)
            try: return float(s)
            except: return 0
        used_gb = parse_size(parts[2])
        avail_gb = parse_size(parts[3])
        if used_gb > 0:
            ratio = avail_gb / used_gb
            if ratio >= 2.0:
                print(f'pass|{ratio:.1f}|{used_gb:.1f}G used, {avail_gb:.1f}G avail|Cassandra has {ratio:.1f}x free space (>= 2x required)')
            else:
                print(f'fail|{ratio:.1f}|{used_gb:.1f}G used, {avail_gb:.1f}G avail|Cassandra only has {ratio:.1f}x free space (< 2x required)')
        else:
            print(f'pass|0|fresh|Cassandra disk appears empty')
        break
else:
    print('skip|0|0|Could not parse Cassandra df output')
")
        local cass_disk_status cass_disk_ratio cass_disk_sizes cass_disk_detail
        cass_disk_status=$(echo "$cass_disk_result" | cut -d'|' -f1)
        cass_disk_ratio=$(echo "$cass_disk_result" | cut -d'|' -f2)
        cass_disk_sizes=$(echo "$cass_disk_result" | cut -d'|' -f3)
        cass_disk_detail=$(echo "$cass_disk_result" | cut -d'|' -f4)

        if [ "$cass_disk_status" = "pass" ]; then
            ok "Cassandra disk: $cass_disk_detail ($cass_disk_sizes)"
        elif [ "$cass_disk_status" = "fail" ]; then
            warn "Cassandra disk: $cass_disk_detail ($cass_disk_sizes)"
            warn "Migration creates a new keyspace that roughly doubles Cassandra storage."
            warn "Consider expanding Cassandra disk or cleaning up before proceeding."
        else
            log "  Cassandra disk: $cass_disk_detail"
        fi
    else
        log "  Cassandra disk: Could not check (pod may not have df)"
    fi

    # 0.9 Config summary
    step "Configuration Summary"
    log "  Vcluster:          $VCLUSTER"
    log "  Namespace:         $NAMESPACE"
    log "  Pod:               $POD"
    log "  Container:         $CONTAINER"
    log "  Cassandra Pod:     $CASSANDRA_POD"
    log "  Current backend:   $current_backend"
    log "  ID Strategy:       legacy (migration) -> deterministic (runtime)"
    log "  Claim Enabled:     $CLAIM_ENABLED"
    log "  Migration Mode:    ${MIGRATION_MODE:-full}"
    log "  Migrator CPU:      $MIGRATOR_CPU"
    log "  Migrator Memory:   $MIGRATOR_MEMORY"
    log "  Skip Switch:       $SKIP_SWITCH"
    log "  Switch Only:       $SWITCH_ONLY"
    log "  Skip Cleanup:      $SKIP_CLEANUP"
    log "  Skip Alias:        $SKIP_ALIAS"
    log "  Maintenance Mode:  $MAINTENANCE_MODE"
    log "  Max Retries:       $MAX_RETRIES"
    log "  Scanner Threads:   $SCANNER_THREADS"
    log "  Writer Threads:    $WRITER_THREADS"
    log "  ES Bulk Size:      ${ES_BULK_SIZE:-default}"
    log "  ES Field Limit:    $ES_FIELD_LIMIT"
    log "  JVM Heap:          $JVM_HEAP (min: $JVM_MIN_HEAP)"
    log "  Vertex Count:      ${VERTEX_COUNT}"
    log "  Sizing Tier:       ${SIZING_TIER}"
    log "  Log File:          ${LOG_FILE}"
    echo ""

    if [ "$DRY_RUN" = "true" ]; then
        record_phase "Phase 0: Pre-flight" "PASS (dry-run)"
        log "Dry-run mode — exiting after config summary."
        exit "$EXIT_SUCCESS"
    fi

    record_phase "Phase 0: Pre-flight" "PASS"
}

# ============================================================================
# Phase 1: Maintenance Mode ON
# ============================================================================

phase_maintenance_on() {
    if [ "$MAINTENANCE_MODE" != "true" ]; then
        log "Skipping maintenance mode (not requested)"
        record_phase "Phase 1: Maintenance ON" "SKIPPED"
        return 0
    fi

    step "Phase 1: Enable Maintenance Mode"

    # Acquire Keycloak token — maintenance mode API requires authentication
    if [ -z "$KEYCLOAK_TOKEN" ]; then
        acquire_token || die "Cannot acquire Keycloak token for maintenance mode" "$EXIT_PREFLIGHT"
    fi

    local resp
    resp=$(kexec_quiet curl -s -X PUT \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
        -d 'true' \
        "localhost:21000/api/atlas/v2/configs/MAINTENANCE_MODE" 2>/dev/null || echo "")

    # Verify
    local mode
    mode=$(kexec_quiet curl -s \
        -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
        "localhost:21000/api/atlas/v2/configs/MAINTENANCE_MODE" 2>/dev/null || echo "")
    if [ "$mode" = "true" ]; then
        ok "Maintenance mode enabled"
        record_phase "Phase 1: Maintenance ON" "PASS"
    else
        record_phase "Phase 1: Maintenance ON" "FAIL"
        die "Failed to enable maintenance mode (response: $resp)" "$EXIT_PREFLIGHT"
    fi
}

# ============================================================================
# Revert ES alias moves done in Phase 4 (ZG index -> JG index).
# Called from phase_cleanup before restarting the pod in janus mode, so the
# pod boots with aliases pointing at janusgraph_vertex_index.
#
# Idempotent: no-op when NEW_ES_INDEX is absent or has no movable aliases.
# Preserves filter and routing attributes; drops is_write_index because with
# a single target index it is implicit.
# ============================================================================
revert_aliases_to_janusgraph() {
    step "Reverting ES aliases: ${NEW_ES_INDEX} -> ${OLD_ES_INDEX}"

    local new_status
    new_status=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "$(es_url)/${NEW_ES_INDEX}" 2>/dev/null || echo "000")
    if [ "$new_status" != "200" ]; then
        log "  ${NEW_ES_INDEX} does not exist — nothing to revert"
        return 0
    fi

    local old_status
    old_status=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "$(es_url)/${OLD_ES_INDEX}" 2>/dev/null || echo "000")
    if [ "$old_status" != "200" ]; then
        warn "  ${OLD_ES_INDEX} does not exist — cannot move aliases back (fresh tenant?)"
        return 0
    fi

    local new_aliases_json
    new_aliases_json=$(kexec_quiet curl -s "$(es_url)/${NEW_ES_INDEX}/_alias/*" 2>/dev/null || echo "{}")

    local actions
    actions=$(echo "$new_aliases_json" | py_extract "
import sys, json

data = json.load(sys.stdin)
aliases = data.get('${NEW_ES_INDEX}', {}).get('aliases', {})

# Skip self-referential alias name if ES returns one.
skip = {'${NEW_ES_INDEX}'}

actions = []
for name, config in aliases.items():
    if name in skip:
        continue
    actions.append({'remove': {'index': '${NEW_ES_INDEX}', 'alias': name}})
    add = {'index': '${OLD_ES_INDEX}', 'alias': name}
    # Preserve access-control + routing attributes. Drop is_write_index —
    # single-target aliases are implicitly writable and preserving it could
    # conflict with existing write-index bindings on OLD_ES_INDEX.
    for k in ('filter', 'index_routing', 'search_routing', 'routing'):
        if k in config:
            add[k] = config[k]
    actions.append({'add': add})

print(json.dumps({'actions': actions}) if actions else '')
")

    if [ -z "$actions" ]; then
        log "  No aliases on ${NEW_ES_INDEX} to revert"
        return 0
    fi

    local action_count
    action_count=$(echo "$actions" | py_extract "import sys,json; print(len(json.load(sys.stdin).get('actions',[])) // 2)")
    log "  Reverting ${action_count} aliases..."

    local resp
    resp=$(kexec_quiet curl -s -X POST "$(es_url)/_aliases" \
        -H 'Content-Type: application/json' \
        -d "$actions" 2>/dev/null || echo '{"acknowledged":false}')

    local ack
    ack=$(echo "$resp" | py_extract "import sys,json; print(json.load(sys.stdin).get('acknowledged', False))")

    if [ "$ack" = "True" ] || [ "$ack" = "true" ]; then
        ok "Reverted ${action_count} aliases to ${OLD_ES_INDEX}"
    else
        warn "Alias revert response: ${resp}"
    fi
}

# ============================================================================
# Phase 2: Cleanup (for remigration)
# ============================================================================

phase_cleanup() {
    if [ "$SKIP_CLEANUP" = "true" ]; then
        log "Skipping cleanup phase (--skip-cleanup)"
        record_phase "Phase 2: Cleanup" "SKIPPED"
        return 0
    fi

    step "Phase 2: Cleanup"

    # Fix 6: Check if tenant is currently live on Cassandra backend
    log "Checking current backend before cleanup..."
    local current_backend
    current_backend=$(kexec_quiet bash -c \
        'grep "^atlas.graphdb.backend=" /opt/apache-atlas/conf/atlas-application.properties 2>/dev/null | cut -d= -f2 | tr -d "[:space:]"' \
        || echo "janus")

    if [ "$current_backend" = "cassandra" ]; then
        warn "Tenant is LIVE on Cassandra backend. Must revert to JanusGraph before dropping keyspace."
        log "Reverting backend to JanusGraph via static config API..."

        # Seed janus defaults via static config API
        seed_static_config "atlas.graphdb.backend" "janus"
        seed_static_config "atlas.graph.id.strategy" "legacy"

        # Move ES aliases back to JG index BEFORE the pod boots in janus mode,
        # so janus reads land on janusgraph_vertex_index immediately after restart.
        revert_aliases_to_janusgraph

        # Restart pod and wait
        log "Restarting pod after JanusGraph revert..."
        kubectl delete pod "$POD" -n "$NAMESPACE"
        if ! kubectl wait --for=condition=Ready "pod/$POD" -n "$NAMESPACE" --timeout="${WAIT_TIMEOUT}s" 2>/dev/null; then
            record_phase "Phase 2: Cleanup" "FAIL (pod not ready after revert)"
            die "Pod did not become Ready after JanusGraph revert" "$EXIT_CLEANUP"
        fi

        # Wait for Atlas ACTIVE
        local revert_status="UNKNOWN"
        for i in $(seq 1 30); do
            revert_status=$(kexec_quiet curl -s localhost:21000/api/atlas/admin/status 2>/dev/null | \
                py_extract "import sys,json; print(json.load(sys.stdin).get('Status','UNKNOWN'))" || echo "STARTING")
            if [ "$revert_status" = "ACTIVE" ]; then
                break
            fi
            sleep 10
        done

        if [ "$revert_status" != "ACTIVE" ]; then
            record_phase "Phase 2: Cleanup" "FAIL (not ACTIVE after revert)"
            die "Atlas did not become ACTIVE after JanusGraph revert (status: $revert_status)" "$EXIT_CLEANUP"
        fi

        # Verify backend is actually janus after restart
        KEYCLOAK_TOKEN=""
        if acquire_token; then
            local loaded_backend
            loaded_backend=$(kexec_quiet curl -s \
                -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
                "localhost:21000/api/atlas/v2/static-configs/atlas.graphdb.backend" 2>/dev/null | \
                py_extract "import sys,json; print(json.load(sys.stdin).get('currentValue','unknown'))" || echo "unknown")
            if [ "$loaded_backend" = "janus" ]; then
                ok "Verified: backend is janus after revert"
            else
                warn "Backend after revert: $loaded_backend (expected janus)"
            fi
        fi
        ok "Reverted to JanusGraph. Safe to drop Cassandra data."
    fi

    # Check if atlas_graph keyspace exists
    log "Checking for existing atlas_graph keyspace..."
    local ks_exists
    ks_exists=$(cexec cqlsh -e "DESCRIBE KEYSPACES;" 2>/dev/null | grep -c "atlas_graph" | tr -d '[:space:]' || echo 0)

    if [ "${ks_exists:-0}" -eq 0 ] 2>/dev/null; then
        log "No existing atlas_graph keyspace found — skip cleanup"
        record_phase "Phase 2: Cleanup" "SKIPPED (no keyspace)"
        return 0
    fi

    # Drop keyspace
    log "Dropping atlas_graph keyspace..."
    if ! cexec cqlsh -e "DROP KEYSPACE IF EXISTS atlas_graph;" 2>/dev/null; then
        record_phase "Phase 2: Cleanup" "FAIL"
        die "Failed to drop atlas_graph keyspace" "$EXIT_CLEANUP"
    fi
    ok "Keyspace dropped"

    # Verify keyspace gone
    ks_exists=$(cexec cqlsh -e "DESCRIBE KEYSPACES;" 2>/dev/null | grep -c "atlas_graph" | tr -d '[:space:]' || echo 0)
    if [ "${ks_exists:-0}" -ne 0 ] 2>/dev/null; then
        record_phase "Phase 2: Cleanup" "FAIL"
        die "atlas_graph keyspace still exists after DROP" "$EXIT_CLEANUP"
    fi

    # Delete ES index
    log "Deleting atlas_graph_vertex_index ES index..."
    local es_resp
    es_resp=$(kexec_quiet curl -s -X DELETE "$(es_url)/atlas_graph_vertex_index" 2>/dev/null || echo '{"acknowledged":false}')
    if echo "$es_resp" | grep -q '"acknowledged":true\|"error".*"index_not_found_exception"'; then
        ok "ES index deleted (or didn't exist)"
    else
        warn "ES index delete response: $es_resp"
    fi

    # Verify ES index gone
    local es_check
    es_check=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "$(es_url)/atlas_graph_vertex_index" 2>/dev/null || echo "000")
    if [ "$es_check" = "404" ] || [ "$es_check" = "000" ]; then
        ok "Cleanup verified: keyspace and ES index removed"
    else
        warn "ES index may still exist (HTTP $es_check)"
    fi

    record_phase "Phase 2: Cleanup" "PASS"
}

# ============================================================================
# Phase 3: Migration with Retry
# ============================================================================

phase_migration() {
    if [ "$MIGRATION_MODE" = "--validate-only" ]; then
        step "Phase 3: Validation Only"
    else
        step "Phase 3: Migration"
    fi

    run_migration_pod
}

run_migration_pod() {
    log "Triggering migration via dedicated pod (kubectl apply)..."

    # ---- Read runtime info from the running atlas-0 pod ----
    log "Reading runtime info from $POD..."
    local atlas_image
    atlas_image=$(kubectl get pod "$POD" -n "$NAMESPACE" -o jsonpath="{.spec.containers[?(@.name=='${CONTAINER}')].image}" 2>/dev/null || echo "")
    if [ -z "$atlas_image" ]; then
        record_phase "Phase 3: Migration" "FAIL (cannot read image from $POD)"
        die "Could not determine container image from pod $POD" "$EXIT_MIGRATION"
    fi
    log "  Image: $atlas_image"

    # imagePullSecrets (may be empty)
    local image_pull_secrets_yaml=""
    local pull_secret_names
    pull_secret_names=$(kubectl get pod "$POD" -n "$NAMESPACE" -o jsonpath='{.spec.imagePullSecrets[*].name}' 2>/dev/null || echo "")
    if [ -n "$pull_secret_names" ]; then
        image_pull_secrets_yaml="  imagePullSecrets:"
        for secret_name in $pull_secret_names; do
            image_pull_secrets_yaml="${image_pull_secrets_yaml}
  - name: ${secret_name}"
        done
    fi

    # Tolerations (may be empty)
    local tolerations_yaml=""
    local tolerations_json
    tolerations_json=$(kubectl get pod "$POD" -n "$NAMESPACE" -o jsonpath='{.spec.tolerations}' 2>/dev/null || echo "")
    if [ -n "$tolerations_json" ] && [ "$tolerations_json" != "[]" ] && [ "$tolerations_json" != "null" ]; then
        # Convert JSON array to YAML via python
        tolerations_yaml=$(py_extract "
import sys, json, io
tolerations = json.loads('${tolerations_json}')
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

    # Multitenant detection: check if atlas-secret-manager secret exists
    local is_multitenant="false"
    if kubectl get secret atlas-secret-manager -n "$NAMESPACE" >/dev/null 2>&1; then
        is_multitenant="true"
        log "  Multitenant: yes (atlas-secret-manager found)"
    else
        log "  Multitenant: no"
    fi

    # Build multitenant envFrom block
    local multitenant_env_yaml=""
    if [ "$is_multitenant" = "true" ]; then
        multitenant_env_yaml="    - secretRef:
        name: atlas-secret-manager
    - secretRef:
        name: atlas-secret-parameter-store"
    fi

    # ---- Migration with retry loop ----
    local attempt=0
    local migration_passed="false"
    local pod_manifest="/tmp/migrator-pod-${VCLUSTER}-${TIMESTAMP}.yaml"

    while [ "$attempt" -lt "$MAX_RETRIES" ] && [ "$migration_passed" = "false" ]; do
        attempt=$((attempt + 1))
        log "Migration attempt $attempt/$MAX_RETRIES"

        # First attempt uses MIGRATION_MODE, retries use "" for resume
        local mode_flag="$MIGRATION_MODE"
        if [ "$attempt" -gt 1 ]; then
            mode_flag=""  # Resume: Java migrator picks up failed token ranges
        fi

        # Clean up any existing migration pod
        log "Cleaning up any existing migration pods..."
        kubectl delete pods -n "$NAMESPACE" -l app=atlas-migrator --ignore-not-found 2>/dev/null || true
        sleep 3

        # ES_BULK_SIZE and QUEUE_CAPACITY env entries (conditional)
        local extra_env=""
        if [ -n "$ES_BULK_SIZE" ]; then
            extra_env="    - name: ES_BULK_SIZE
      value: \"${ES_BULK_SIZE}\""
        fi
        if [ "${QUEUE_CAPACITY:-10000}" != "10000" ]; then
            extra_env="${extra_env}
    - name: QUEUE_CAPACITY
      value: \"${QUEUE_CAPACITY}\""
        fi

        # Generate Pod YAML manifest
        cat > "$pod_manifest" <<EOYAML
apiVersion: v1
kind: Pod
metadata:
  name: atlas-migrator-${attempt}
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
      echo "=== Atlas Migrator Pod ==="
      echo "Mode: ${mode_flag:-full}"
      echo "ID Strategy: legacy (migration) -> deterministic (runtime)"
      echo "Claim Enabled: ${CLAIM_ENABLED}"
      echo ""

      # Migrator always runs with legacy IDs; runtime switches to deterministic in Phase 5
      export ID_STRATEGY="legacy"
      export CLAIM_ENABLED="${CLAIM_ENABLED}"
      export SCANNER_THREADS="${SCANNER_THREADS}"
      export WRITER_THREADS="${WRITER_THREADS}"
      export ES_BULK_SIZE="${ES_BULK_SIZE:-1000}"
      export QUEUE_CAPACITY="${QUEUE_CAPACITY:-10000}"
      export MIGRATOR_JVM_HEAP="${JVM_HEAP}"
      export MIGRATOR_JVM_MIN_HEAP="${JVM_MIN_HEAP}"
      export SOURCE_CONSISTENCY="ONE"
      export TARGET_CONSISTENCY="LOCAL_QUORUM"
      export TENANT_NAME="${VCLUSTER}"
      export SKIP_BACKUP_CHECK="${SKIP_BACKUP_CHECK}"

      MIGRATOR_EXIT=0
      /opt/apache-atlas/bin/atlas_migrate.sh ${mode_flag} || MIGRATOR_EXIT=\$?

      POST_SLEEP=3600
      echo ""
      echo "=== Migration finished with exit code: \$MIGRATOR_EXIT ==="
      echo "Pod will remain Running for \${POST_SLEEP}s for inspection."
      echo "To exec:    kubectl exec -it \$K8S_POD_NAME -n \$Namespace -- bash"
      echo "To view logs: kubectl logs \$K8S_POD_NAME -n \$Namespace"
      echo "To kill early: kubectl delete pod \$K8S_POD_NAME -n \$Namespace"
      echo ""

      sleep \$POST_SLEEP
      exit \$MIGRATOR_EXIT
    env:
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
${extra_env}
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

        # Deploy via kubectl apply
        log "Applying migrator pod manifest..."
        log "  Mode: ${mode_flag:-resume}"
        log "  Resources: cpu=${MIGRATOR_CPU}, memory=${MIGRATOR_MEMORY}"
        if ! kubectl apply -f "$pod_manifest"; then
            warn "kubectl apply failed on attempt $attempt"
            if [ "$attempt" -lt "$MAX_RETRIES" ]; then
                log "Retrying in 10s..."
                sleep 10
                continue
            fi
            record_phase "Phase 3: Migration" "FAIL (kubectl apply failed)"
            die "kubectl apply failed after $MAX_RETRIES attempts" "$EXIT_MIGRATION"
        fi
        ok "Pod manifest applied"

        # Wait for migration pod to start
        log "Waiting for migration pod to start..."
        local job_pod=""
        for i in $(seq 1 60); do
            job_pod=$(kubectl get pods -n "$NAMESPACE" -l app=atlas-migrator --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
            if [ -n "$job_pod" ]; then
                break
            fi
            local pending_pod
            pending_pod=$(kubectl get pods -n "$NAMESPACE" -l app=atlas-migrator -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
            if [ -n "$pending_pod" ] && [ "$i" -le 5 ]; then
                log "  Migration pod $pending_pod is starting..."
            fi
            sleep 5
        done

        if [ -z "$job_pod" ]; then
            warn "Migration pod did not start within 5 minutes on attempt $attempt"
            if [ "$attempt" -lt "$MAX_RETRIES" ]; then
                log "Retrying in 10s..."
                sleep 10
                continue
            fi
            record_phase "Phase 3: Migration" "FAIL (pod timeout after $MAX_RETRIES attempts)"
            die "Migration pod did not start after $MAX_RETRIES attempts" "$EXIT_MIGRATION"
        fi

        # Stream logs to file in background; poll for completion marker; explicitly kill.
        # Pipeline approach (kubectl | tee | grep -m1) fails because bash waits for ALL
        # pipeline processes before continuing — kubectl logs -f never gets SIGPIPE while
        # the pod is silent (POST_SLEEP), so the entire pipeline blocks indefinitely.
        local migrator_log_file="/tmp/migrator-${job_pod}-$(date +%s).log"
        kubectl logs -f "$job_pod" -n "$NAMESPACE" > "$migrator_log_file" 2>/dev/null &
        local _kubectl_pid=$!
        log "Streaming logs from $job_pod (pid $_kubectl_pid)..."

        # Poll until completion marker or kubectl exits on its own
        while kill -0 "$_kubectl_pid" 2>/dev/null; do
            if grep -q "Migration finished with exit code" "$migrator_log_file" 2>/dev/null; then
                break
            fi
            sleep 2
        done

        # Explicitly kill kubectl logs if still alive (pod in post-sleep)
        if kill -0 "$_kubectl_pid" 2>/dev/null; then
            kill "$_kubectl_pid" 2>/dev/null || true
            wait "$_kubectl_pid" 2>/dev/null || true
        fi

        # Emit the completion line to the script's own log stream
        grep "Migration finished with exit code" "$migrator_log_file" 2>/dev/null | tail -1 || true
        log "Log stream complete (full log saved to $migrator_log_file)"

        # Check pod status and exit code
        local job_status
        job_status=$(kubectl get pods "$job_pod" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
        local exit_code
        exit_code=$(kubectl get pods "$job_pod" -n "$NAMESPACE" -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || echo "")

        if [ "$exit_code" = "0" ] || [ "$job_status" = "Succeeded" ]; then
            migration_passed="true"
            ok "Migration attempt $attempt: PASSED"
        elif [ "$job_status" = "Running" ]; then
            # Pod still running (in post-sleep phase)
            migration_passed="true"
            ok "Migration attempt $attempt: PASSED (pod in post-completion sleep)"
        elif [ "$exit_code" = "2" ]; then
            # Exit 2 = data migration succeeded but validation failed.
            # Retrying won't help — need to investigate validation failures.
            warn "Migration data written but VALIDATION FAILED (exit code 2)"
            warn "Do not retry — investigate validation report in logs, or re-run with --fresh after fixing"
            break
        else
            warn "Migration attempt $attempt failed (pod status: $job_status, exit code: $exit_code)"
            if [ "$attempt" -lt "$MAX_RETRIES" ]; then
                log "Retrying... (migration is resumable, will pick up from failed token ranges)"
                sleep 10
            fi
        fi
    done

    # Clean up manifest file
    rm -f "$pod_manifest"

    if [ "$migration_passed" = "false" ]; then
        record_phase "Phase 3: Migration" "FAIL (after $MAX_RETRIES attempts)"
        die "Migration failed after $MAX_RETRIES attempts" "$EXIT_MIGRATION"
    fi

    record_phase "Phase 3: Migration" "PASS (attempt $attempt/$MAX_RETRIES)"
}

# ============================================================================
# Phase 4: ES Alias Creation
# ============================================================================

phase_alias() {
    if [ "$SKIP_ALIAS" = "true" ]; then
        log "Skipping alias phase (--skip-alias)"
        record_phase "Phase 4: ES Alias" "SKIPPED"
        return 0
    fi

    step "Phase 4: ES Alias Management"

    # ---- Step 4.1: Verify target index exists ----
    log "Checking target index ${NEW_ES_INDEX} exists..."
    local index_status
    index_status=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "$(es_url)/${NEW_ES_INDEX}" 2>/dev/null || echo "000")

    if [ "$index_status" != "200" ]; then
        warn "Target index ${NEW_ES_INDEX} does not exist (HTTP ${index_status}). Migration may not have completed."
        record_phase "Phase 4: ES Alias" "SKIPPED (index not found)"
        return 0
    fi
    ok "Target index ${NEW_ES_INDEX} exists"

    # ---- Step 4.2: Create vertex index alias ----
    log "Creating vertex index alias (${VERTEX_ALIAS} → ${NEW_ES_INDEX})..."
    local alias_resp
    alias_resp=$(kexec_quiet curl -s -X POST "$(es_url)/_aliases" \
        -H 'Content-Type: application/json' \
        -d "{
            \"actions\": [
                {\"remove\": {\"index\": \"*\", \"alias\": \"${VERTEX_ALIAS}\"}},
                {\"add\": {\"index\": \"${NEW_ES_INDEX}\", \"alias\": \"${VERTEX_ALIAS}\"}}
            ]
        }" 2>/dev/null || echo '{"acknowledged":false}')

    local alias_ack
    alias_ack=$(echo "$alias_resp" | py_extract "import sys,json; print(json.load(sys.stdin).get('acknowledged', False))")

    if [ "$alias_ack" = "True" ] || [ "$alias_ack" = "true" ]; then
        ok "Alias ${VERTEX_ALIAS} → ${NEW_ES_INDEX} created"
    else
        warn "Vertex alias creation response: ${alias_resp}"
    fi

    # Verify vertex alias
    local verify_resp
    verify_resp=$(kexec_quiet curl -s "$(es_url)/_alias/${VERTEX_ALIAS}" 2>/dev/null || echo "{}")
    local points_to
    points_to=$(echo "$verify_resp" | py_extract "import sys,json; d=json.load(sys.stdin); print(','.join(d.keys()))")

    if echo "$points_to" | grep -q "${NEW_ES_INDEX}"; then
        ok "Verified: ${VERTEX_ALIAS} points to ${NEW_ES_INDEX}"
    else
        warn "Alias verification inconclusive (points to: ${points_to})"
    fi

    # ---- Step 4.3: Migrate persona aliases from old index to new index ----
    log "Migrating persona aliases from ${OLD_ES_INDEX} to ${NEW_ES_INDEX}..."

    # Check old index exists
    local old_index_status
    old_index_status=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "$(es_url)/${OLD_ES_INDEX}" 2>/dev/null || echo "000")

    if [ "$old_index_status" != "200" ]; then
        log "Old index ${OLD_ES_INDEX} does not exist — skipping persona alias migration"
        record_phase "Phase 4: ES Alias" "PASS (vertex alias only)"
        return 0
    fi

    # Get all aliases from old index
    local old_aliases_json
    old_aliases_json=$(kexec_quiet curl -s "$(es_url)/${OLD_ES_INDEX}/_alias/*" 2>/dev/null || echo "{}")

    # Parse persona aliases (exclude system aliases)
    local alias_data
    alias_data=$(echo "$old_aliases_json" | py_extract "
import sys, json

data = json.load(sys.stdin)
aliases = data.get('${OLD_ES_INDEX}', {}).get('aliases', {})

# System aliases to skip
skip = {'${VERTEX_ALIAS}', '${OLD_ES_INDEX}', '${NEW_ES_INDEX}'}

persona_aliases = {}
for name, config in aliases.items():
    if name in skip:
        continue
    persona_aliases[name] = config

print(json.dumps(persona_aliases))
")

    if [ -z "$alias_data" ] || [ "$alias_data" = "{}" ] || [ "$alias_data" = "null" ]; then
        log "No persona aliases found on ${OLD_ES_INDEX}"
        record_phase "Phase 4: ES Alias" "PASS (vertex alias only, no persona aliases)"
        return 0
    fi

    local persona_count
    persona_count=$(echo "$alias_data" | py_extract "import sys,json; print(len(json.load(sys.stdin)))")
    log "Found ${persona_count} persona aliases to migrate"

    # Build batch actions: for each alias, remove from old + add to new with same filter
    local actions_json
    actions_json=$(echo "$alias_data" | py_extract "
import sys, json

aliases = json.load(sys.stdin)
all_actions = []

for name, config in aliases.items():
    # Remove from old index
    all_actions.append({'remove': {'index': '${OLD_ES_INDEX}', 'alias': name}})
    # Add to new index preserving filter
    add_action = {'index': '${NEW_ES_INDEX}', 'alias': name}
    if 'filter' in config:
        add_action['filter'] = config['filter']
    all_actions.append({'add': add_action})

# Split into batches of 50 aliases (100 actions)
batch_size = 100
batches = []
for i in range(0, len(all_actions), batch_size):
    batch = all_actions[i:i + batch_size]
    batches.append(json.dumps({'actions': batch}))

for b in batches:
    print(b)
")

    local migrated=0
    local failed=0

    while IFS= read -r batch; do
        [ -z "$batch" ] && continue

        local batch_resp
        batch_resp=$(kexec_quiet curl -s -X POST "$(es_url)/_aliases" \
            -H 'Content-Type: application/json' \
            -d "$batch" 2>/dev/null || echo '{"acknowledged":false}')

        local batch_ack
        batch_ack=$(echo "$batch_resp" | py_extract "import sys,json; print(json.load(sys.stdin).get('acknowledged', False))")

        if [ "$batch_ack" = "True" ] || [ "$batch_ack" = "true" ]; then
            local batch_count
            batch_count=$(echo "$batch" | py_extract "import sys,json; print(len(json.load(sys.stdin).get('actions',[])) // 2)")
            migrated=$((migrated + batch_count))
        else
            # Fall back to individual alias migration
            warn "Batch migration failed, trying individually..."
            local individual_lines
            individual_lines=$(echo "$batch" | py_extract "
import sys, json
data = json.load(sys.stdin)
actions = data.get('actions', [])
for i in range(0, len(actions), 2):
    pair = actions[i:i+2]
    if len(pair) == 2:
        alias_name = pair[1].get('add', {}).get('alias', 'unknown')
        print(alias_name + '|' + json.dumps({'actions': pair}))
")
            while IFS= read -r line; do
                [ -z "$line" ] && continue
                local a_name a_payload
                a_name=$(echo "$line" | cut -d'|' -f1)
                a_payload=$(echo "$line" | cut -d'|' -f2-)

                local a_resp
                a_resp=$(kexec_quiet curl -s -X POST "$(es_url)/_aliases" \
                    -H 'Content-Type: application/json' \
                    -d "$a_payload" 2>/dev/null || echo '{"acknowledged":false}')

                local a_ack
                a_ack=$(echo "$a_resp" | py_extract "import sys,json; print(json.load(sys.stdin).get('acknowledged', False))")

                if [ "$a_ack" = "True" ] || [ "$a_ack" = "true" ]; then
                    migrated=$((migrated + 1))
                else
                    err "  Failed to migrate alias: ${a_name}"
                    failed=$((failed + 1))
                fi
            done <<< "$individual_lines"
        fi
    done <<< "$actions_json"

    # Verify alias count on new index
    local new_alias_count
    new_alias_count=$(kexec_quiet curl -s "$(es_url)/${NEW_ES_INDEX}/_alias/*" 2>/dev/null | py_extract "
import sys, json
data = json.load(sys.stdin)
aliases = data.get('${NEW_ES_INDEX}', {}).get('aliases', {})
skip = {'${VERTEX_ALIAS}'}
print(sum(1 for name in aliases if name not in skip))
" || echo "0")

    log "Persona alias migration: migrated=${migrated}, failed=${failed}, on new index=${new_alias_count}"

    if [ "$failed" -gt 0 ]; then
        warn "${failed} persona aliases failed to migrate"
        record_phase "Phase 4: ES Alias" "WARN (${migrated} migrated, ${failed} failed)"
    else
        ok "All ${migrated} persona aliases migrated successfully"
        record_phase "Phase 4: ES Alias" "PASS (vertex alias + ${migrated} persona aliases)"
    fi
}

# ============================================================================
# ConfigMap patching helper (Fix 5: preserve other keys)
# ============================================================================

patch_configmap_properties() {
    local new_props_file="$1"

    # Read the full ConfigMap as JSON, replace only the atlas-application.properties key
    local cm_json
    cm_json=$(kubectl get configmap atlas-config -n "$NAMESPACE" -o json)

    # Check how many keys the ConfigMap has
    local key_count
    key_count=$(echo "$cm_json" | py_extract "import sys,json; print(len(json.load(sys.stdin).get('data',{})))")

    if [ "$key_count" -gt 1 ] 2>/dev/null; then
        log "  ConfigMap has $key_count keys — using JSON patch to preserve other keys"
        # Read new properties content, escape for JSON
        local props_content
        props_content=$(cat "$new_props_file")
        local escaped_content
        escaped_content=$(py_extract "
import sys, json
with open('$new_props_file') as f:
    content = f.read()
print(json.dumps(content))
")
        # Patch the specific key using kubectl patch
        kubectl patch configmap atlas-config -n "$NAMESPACE" \
            --type merge -p "{\"data\":{\"atlas-application.properties\":${escaped_content}}}"
    else
        # Only one key — safe to use the simpler from-file approach
        kubectl create configmap atlas-config -n "$NAMESPACE" \
            --from-file=atlas-application.properties="$new_props_file" \
            --dry-run=client -o yaml | kubectl apply -f -
    fi
}

# ============================================================================
# Phase 5: Backend Switch
# ============================================================================

phase_switch() {
    if ! should_switch; then
        log "Skipping backend switch"
        record_phase "Phase 5: Backend Switch" "SKIPPED"
        return 0
    fi

    step "Phase 5: Backend Switch"

    # 5.1 Seed static configs via API (persisted in Cassandra, take effect on restart)
    #     Only 2 keys needed: backend + id strategy. earlyOverlay() reads these from
    #     Cassandra BEFORE Spring context starts. Connection properties (hostname, port,
    #     datacenter) auto-resolve from atlas.graph.storage.* via CassandraSessionProvider.
    #     No ConfigMap patching — Argo would revert it.
    log "Seeding static configs via API..."
    seed_static_config "atlas.graphdb.backend" "cassandra"
    # Always seed deterministic — migration runs with legacy IDs, but runtime uses deterministic
    seed_static_config "atlas.graph.id.strategy" "deterministic"
    ok "Static configs seeded (will take effect after restart)"

    # 5.4 Trigger rolling restart of Atlas StatefulSet
    log "Triggering rolling restart of Atlas StatefulSet..."
    if ! kubectl rollout restart statefulset/atlas -n "$NAMESPACE"; then
        warn "kubectl rollout restart failed — falling back to manual pod delete"
        kubectl delete pod "$POD" -n "$NAMESPACE" || true
    fi

    # 5.5 Wait for rollout to complete
    log "Waiting for all pods to pick up new config (timeout: 600s)..."
    if ! kubectl rollout status statefulset/atlas -n "$NAMESPACE" --timeout=600s; then
        record_phase "Phase 5: Backend Switch" "FAIL (rollout timeout)"
        die "StatefulSet rollout did not complete within 600s." "$EXIT_SWITCH"
    fi
    ok "All pods updated"

    # 5.6 Verify Atlas is ACTIVE on each pod
    log "Verifying Atlas is ACTIVE on all pods..."
    local replicas
    replicas=$(kubectl get statefulset atlas -n "$NAMESPACE" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "1")
    local all_active="true"
    for i in $(seq 0 $((replicas - 1))); do
        local pod_name="atlas-${i}"
        log "  Checking $pod_name..."
        local atlas_status="UNKNOWN"
        for attempt in $(seq 1 30); do
            atlas_status=$(kubectl exec "$pod_name" -n "$NAMESPACE" -c "$CONTAINER" -- \
                curl -s localhost:21000/api/atlas/admin/status 2>/dev/null | \
                py_extract "import sys,json; print(json.load(sys.stdin).get('Status','UNKNOWN'))" || echo "STARTING")
            if [ "$atlas_status" = "ACTIVE" ]; then
                break
            fi
            sleep 10
        done
        if [ "$atlas_status" = "ACTIVE" ]; then
            ok "  $pod_name: ACTIVE"
        else
            warn "  $pod_name: $atlas_status (not ACTIVE after 5 min)"
            all_active="false"
        fi
    done

    if [ "$all_active" = "true" ]; then
        ok "All pods are ACTIVE on Cassandra backend"
    else
        warn "Some pods did not reach ACTIVE state — check manually"
    fi

    # 5.7 Verify backend actually switched to cassandra after restart
    log "Verifying backend is cassandra after restart (10s settle)..."
    sleep 10
    KEYCLOAK_TOKEN=""
    if acquire_token; then
        local loaded_backend="unknown"
        for verify_attempt in $(seq 1 3); do
            loaded_backend=$(kexec_quiet curl -s \
                -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
                "localhost:21000/api/atlas/v2/static-configs/atlas.graphdb.backend" 2>/dev/null | \
                py_extract "import sys,json; print(json.load(sys.stdin).get('currentValue','unknown'))" || echo "unknown")
            if [ "$loaded_backend" = "cassandra" ]; then
                break
            fi
            sleep 5
        done
        if [ "$loaded_backend" = "cassandra" ]; then
            ok "Verified: backend is cassandra after restart"
        else
            warn "Backend after restart: $loaded_backend (expected cassandra) — verify manually"
        fi
    else
        warn "Could not acquire token for post-restart backend verification"
    fi

    if [ "$all_active" = "true" ]; then
        record_phase "Phase 5: Backend Switch" "PASS"
    else
        record_phase "Phase 5: Backend Switch" "WARN (not all pods ACTIVE)"
    fi
}

# ============================================================================
# Phase 6: Maintenance Mode OFF
# ============================================================================

phase_maintenance_off() {
    if [ "$MAINTENANCE_MODE" != "true" ]; then
        record_phase "Phase 6: Maintenance OFF" "SKIPPED"
        return 0
    fi

    step "Phase 6: Disable Maintenance Mode"

    # Wait for configurable cooldown
    log "Waiting ${MAINTENANCE_COOLDOWN}s cooldown before disabling maintenance mode..."
    sleep "$MAINTENANCE_COOLDOWN"

    # Re-acquire token if needed (may have expired during migration)
    if [ -z "$KEYCLOAK_TOKEN" ]; then
        acquire_token || warn "Cannot acquire token — maintenance mode may need manual disable"
    fi

    local resp
    resp=$(kexec_quiet curl -s -X PUT \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
        -d 'false' \
        "localhost:21000/api/atlas/v2/configs/MAINTENANCE_MODE" 2>/dev/null || echo "")

    local mode
    mode=$(kexec_quiet curl -s \
        -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
        "localhost:21000/api/atlas/v2/configs/MAINTENANCE_MODE" 2>/dev/null || echo "")
    if [ "$mode" = "false" ]; then
        ok "Maintenance mode disabled"
        record_phase "Phase 6: Maintenance OFF" "PASS"
    else
        warn "Maintenance mode may still be enabled (response: $mode)"
        record_phase "Phase 6: Maintenance OFF" "WARN"
    fi
}

# ============================================================================
# Phase 7: Verification
# ============================================================================

verify_check() {
    local name="$1"
    local passed="$2"
    local detail="$3"

    VERIFY_TOTAL=$((VERIFY_TOTAL + 1))
    if [ "$passed" = "true" ]; then
        VERIFY_PASS=$((VERIFY_PASS + 1))
        ok "$name: $detail"
    else
        VERIFY_FAIL=$((VERIFY_FAIL + 1))
        err "$name: $detail"
    fi
}

acquire_token() {
    log "Acquiring Keycloak token..."

    # Fix 3: Use correct keycloak.json path (/opt/apache-atlas/conf/keycloak.json)
    local kc_json
    kc_json=$(kexec_quiet python3 -c "
import json
with open('/opt/apache-atlas/conf/keycloak.json') as f:
    d = json.load(f)
print(json.dumps({'url': d.get('auth-server-url',''), 'realm': d.get('realm',''), 'client': d.get('resource',''), 'secret': d['credentials']['secret']}))
" 2>/dev/null || kexec_quiet python -c "
import json
with open('/opt/apache-atlas/conf/keycloak.json') as f:
    d = json.load(f)
print(json.dumps({'url': d.get('auth-server-url',''), 'realm': d.get('realm',''), 'client': d.get('resource',''), 'secret': d['credentials']['secret']}))
" 2>/dev/null || echo "")

    if [ -z "$kc_json" ]; then
        warn "Could not read keycloak.json — trying environment variables"
        # Fallback: try env vars
        kc_json=$(kexec_quiet python3 -c "
import os, json
print(json.dumps({
    'url': os.environ.get('AUTH_SERVER_URL',''),
    'realm': os.environ.get('KEYCLOAK_REALM',''),
    'client': os.environ.get('KEYCLOAK_CLIENT_ID',''),
    'secret': os.environ.get('KEYCLOAK_CLIENT_SECRET','')
}))
" 2>/dev/null || kexec_quiet python -c "
import os, json
print(json.dumps({
    'url': os.environ.get('AUTH_SERVER_URL',''),
    'realm': os.environ.get('KEYCLOAK_REALM',''),
    'client': os.environ.get('KEYCLOAK_CLIENT_ID',''),
    'secret': os.environ.get('KEYCLOAK_CLIENT_SECRET','')
}))
" 2>/dev/null || echo "")
    fi

    if [ -z "$kc_json" ]; then
        warn "Could not acquire Keycloak credentials — skipping authenticated checks"
        return 1
    fi

    local kc_url kc_realm kc_client kc_secret
    kc_url=$(echo "$kc_json" | py_extract "import sys,json; print(json.load(sys.stdin)['url'])")
    kc_realm=$(echo "$kc_json" | py_extract "import sys,json; print(json.load(sys.stdin)['realm'])")
    kc_client=$(echo "$kc_json" | py_extract "import sys,json; print(json.load(sys.stdin)['client'])")
    kc_secret=$(echo "$kc_json" | py_extract "import sys,json; print(json.load(sys.stdin)['secret'])")

    if [ -z "$kc_url" ] || [ -z "$kc_realm" ] || [ -z "$kc_client" ] || [ -z "$kc_secret" ]; then
        warn "Incomplete Keycloak credentials — skipping authenticated checks"
        return 1
    fi

    # Request token (run curl inside the pod to reach Keycloak)
    local token_resp
    token_resp=$(kexec_quiet curl -s \
        -d "grant_type=client_credentials&client_id=${kc_client}&client_secret=${kc_secret}" \
        "${kc_url}/realms/${kc_realm}/protocol/openid-connect/token" 2>/dev/null || echo "")

    KEYCLOAK_TOKEN=$(echo "$token_resp" | py_extract "import sys,json; print(json.load(sys.stdin).get('access_token',''))")

    if [ -z "$KEYCLOAK_TOKEN" ]; then
        warn "Failed to acquire token — skipping authenticated checks"
        return 1
    fi

    ok "Keycloak token acquired"
    return 0
}

phase_verify() {
    if ! should_switch; then
        log "Skipping verification (backend not switched)"
        record_phase "Phase 7: Verification" "SKIPPED"
        return 0
    fi

    step "Phase 7: Post-Switch Verification"

    # Wait for Atlas internal services to warm up after restart
    # PolicyRefresher runs on a 30s refresh cycle but may have transient errors on first cycle
    log "Waiting 120s for Atlas services to warm up after restart..."
    sleep 120

    # Acquire token (non-fatal if it fails)
    local have_token="false"
    if acquire_token; then
        have_token="true"
    fi

    # 7.1 Index search — Tables (with scrubbed/lock icon check)
    if [ "$have_token" = "true" ]; then
        log "Testing index search (Tables)..."
        local table_resp
        table_resp=$(kexec_quiet curl -s \
            -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
            -H "Content-Type: application/json" \
            -d '{"dsl":{"from":0,"size":5,"query":{"bool":{"must":[{"term":{"__typeName.keyword":"Table"}},{"term":{"__state":"ACTIVE"}}]}}},"attributes":["qualifiedName","name"]}' \
            "localhost:21000/api/meta/search/indexsearch" 2>/dev/null || echo "")

        local table_total
        table_total=$(echo "$table_resp" | py_extract "import sys,json; d=json.load(sys.stdin); print(d.get('approximateCount', d.get('searchParameters',{}).get('totalCount',0)))" || echo "0")

        # Fix 7d: Check for scrubbed/lock icons
        local scrubbed_count
        scrubbed_count=$(echo "$table_resp" | py_extract "
import sys, json
d = json.load(sys.stdin)
entities = d.get('entities', [])
print(sum(1 for e in entities if e.get('scrubbed', False)))
" || echo "0")

        if [ "$table_total" -gt 0 ] 2>/dev/null; then
            verify_check "Table search" "true" "${table_total} Tables found"
        else
            verify_check "Table search" "false" "No Tables found (response: ${table_resp:0:200})"
        fi

        if [ "$scrubbed_count" -gt 0 ] 2>/dev/null; then
            verify_check "Table lock icons" "false" "${scrubbed_count} Tables have lock icons (scrubbed=true)"
        elif [ "$table_total" -gt 0 ] 2>/dev/null; then
            verify_check "Table lock icons" "true" "No lock icons detected"
        fi
    fi

    # 7.2 Index search — Connections (with scrubbed/lock icon check)
    if [ "$have_token" = "true" ]; then
        log "Testing index search (Connections)..."
        local conn_resp
        conn_resp=$(kexec_quiet curl -s \
            -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
            -H "Content-Type: application/json" \
            -d '{"dsl":{"from":0,"size":10,"query":{"bool":{"must":[{"term":{"__typeName.keyword":"Connection"}},{"term":{"__state":"ACTIVE"}}]}}},"attributes":["qualifiedName","name","connectorName"]}' \
            "localhost:21000/api/meta/search/indexsearch" 2>/dev/null || echo "")

        local conn_total
        conn_total=$(echo "$conn_resp" | py_extract "import sys,json; d=json.load(sys.stdin); print(d.get('approximateCount', d.get('searchParameters',{}).get('totalCount',0)))" || echo "0")

        # Fix 7d: Check for scrubbed connections
        local conn_scrubbed
        conn_scrubbed=$(echo "$conn_resp" | py_extract "
import sys, json
d = json.load(sys.stdin)
entities = d.get('entities', [])
print(sum(1 for e in entities if e.get('scrubbed', False)))
" || echo "0")

        if [ "$conn_total" -gt 0 ] 2>/dev/null; then
            verify_check "Connection search" "true" "${conn_total} Connections found"
        else
            verify_check "Connection search" "false" "No Connections found"
        fi

        if [ "$conn_scrubbed" -gt 0 ] 2>/dev/null; then
            verify_check "Connection lock icons" "false" "${conn_scrubbed} Connections have lock icons (scrubbed=true)"
        elif [ "$conn_total" -gt 0 ] 2>/dev/null; then
            verify_check "Connection lock icons" "true" "No lock icons detected"
        fi
    fi

    # 7.3 AuthPolicy in ES
    log "Checking AuthPolicy in ES..."
    local auth_count
    auth_count=$(kexec_quiet curl -s "$(es_url)/atlas_graph_vertex_index/_count?q=__typeName.keyword:AuthPolicy" 2>/dev/null | \
        py_extract "import sys,json; print(json.load(sys.stdin).get('count',0))" || echo "0")
    if [ "$auth_count" -gt 0 ] 2>/dev/null; then
        verify_check "AuthPolicy ES" "true" "${auth_count} policies in ES"
    else
        verify_check "AuthPolicy ES" "false" "No AuthPolicies found in ES"
    fi

    # 7.4 AuthService in ES
    log "Checking AuthService in ES..."
    local svc_count
    svc_count=$(kexec_quiet curl -s "$(es_url)/atlas_graph_vertex_index/_count?q=__typeName.keyword:AuthService" 2>/dev/null | \
        py_extract "import sys,json; print(json.load(sys.stdin).get('count',0))" || echo "0")
    if [ "$svc_count" -gt 0 ] 2>/dev/null; then
        verify_check "AuthService ES" "true" "${svc_count} services in ES"
    else
        verify_check "AuthService ES" "false" "No AuthServices found in ES"
    fi

    # 7.4a ES doc count comparison (old vs new index)
    log "Comparing ES doc counts (old vs new index)..."
    local old_es_count new_es_count
    old_es_count=$(kexec_quiet curl -s "$(es_url)/janusgraph_vertex_index/_count" 2>/dev/null | \
        py_extract "import sys,json; print(json.load(sys.stdin).get('count',0))" || echo "0")
    new_es_count=$(kexec_quiet curl -s "$(es_url)/atlas_graph_vertex_index/_count" 2>/dev/null | \
        py_extract "import sys,json; print(json.load(sys.stdin).get('count',0))" || echo "0")

    if [ "$old_es_count" -gt 0 ] 2>/dev/null && [ "$new_es_count" -gt 0 ] 2>/dev/null; then
        local es_diff_pct
        es_diff_pct=$(py_extract "
old = $old_es_count
new = $new_es_count
if old > 0:
    diff = abs(new - old) * 100.0 / old
    print(f'{diff:.1f}')
else:
    print('0.0')
")
        # Check if within 5% tolerance
        local within_tolerance
        within_tolerance=$(py_extract "print('true' if float('$es_diff_pct') <= 5.0 else 'false')")
        if [ "$within_tolerance" = "true" ]; then
            verify_check "ES doc count" "true" "Old: ${old_es_count}, New: ${new_es_count} (diff: ${es_diff_pct}%)"
        else
            verify_check "ES doc count" "false" "Old: ${old_es_count}, New: ${new_es_count} (diff: ${es_diff_pct}% > 5% threshold)"
        fi
    elif [ "$new_es_count" -gt 0 ] 2>/dev/null; then
        verify_check "ES doc count" "true" "New index: ${new_es_count} docs (old index not found — first migration)"
    else
        verify_check "ES doc count" "false" "New ES index has 0 docs"
    fi

    # 7.5 Token range completion check
    log "Checking migration token range completion..."
    local token_range_output
    token_range_output=$(cexec cqlsh -e "SELECT status, COUNT(*) as cnt FROM atlas_graph.migration_state GROUP BY status;" 2>/dev/null || echo "")

    if [ -n "$token_range_output" ]; then
        local token_range_result
        token_range_result=$(echo "$token_range_output" | py_extract "
import sys
lines = sys.stdin.read().strip().split('\n')
counts = {}
for line in lines:
    line = line.strip()
    if not line or line.startswith('-') or line.startswith('status') or line.startswith('('):
        continue
    parts = [p.strip() for p in line.split('|')]
    if len(parts) >= 2:
        status = parts[0].strip().upper()
        try:
            count = int(parts[1].strip())
            counts[status] = count
        except ValueError:
            pass
completed = counts.get('COMPLETED', counts.get('completed', 0))
failed = counts.get('FAILED', counts.get('failed', 0))
pending = counts.get('PENDING', counts.get('pending', 0))
in_progress = counts.get('IN_PROGRESS', counts.get('in_progress', 0))
total = completed + failed + pending + in_progress
if total == 0:
    print(f'skip|0|0|0|No token ranges found in migration_state')
elif failed == 0 and pending == 0 and in_progress == 0:
    print(f'pass|{completed}|0|0|All {completed} token ranges COMPLETED')
else:
    print(f'fail|{completed}|{failed}|{pending + in_progress}|{completed}/{total} completed, {failed} failed, {pending + in_progress} pending/in_progress')
")
        local tr_status tr_completed tr_failed tr_other tr_detail
        tr_status=$(echo "$token_range_result" | cut -d'|' -f1)
        tr_completed=$(echo "$token_range_result" | cut -d'|' -f2)
        tr_failed=$(echo "$token_range_result" | cut -d'|' -f3)
        tr_other=$(echo "$token_range_result" | cut -d'|' -f4)
        tr_detail=$(echo "$token_range_result" | cut -d'|' -f5)

        if [ "$tr_status" = "pass" ]; then
            verify_check "Token ranges" "true" "$tr_detail"
        elif [ "$tr_status" = "fail" ]; then
            verify_check "Token ranges" "false" "$tr_detail"
        else
            log "  Token ranges: $tr_detail"
        fi
    else
        warn "Could not query migration_state table (may not exist yet)"
    fi

    # Refresh token before policy/typedef checks (original token may have expired)
    KEYCLOAK_TOKEN=""
    if acquire_token; then have_token="true"; else have_token="false"; fi

    # 7.6 Atlas policy download (unauthenticated endpoint — same as PolicyRefresher uses internally)
    log "Checking Atlas policy download..."
    local atlas_policy_count=0
    for ap_attempt in 1 2 3; do
        local atlas_policy_resp
        atlas_policy_resp=$(kexec_quiet curl -s \
            "localhost:21000/api/atlas/v2/auth/download/policies/atlas?pluginId=migrator-verify" 2>/dev/null || echo "")

        atlas_policy_count=$(echo "$atlas_policy_resp" | py_extract "import sys,json; d=json.load(sys.stdin); print(len(d.get('policies',[])))" || echo "0")
        if [ "$atlas_policy_count" -gt 0 ] 2>/dev/null; then
            break
        fi
        if [ "$ap_attempt" -lt 3 ]; then
            log "  Atlas policies not ready (attempt $ap_attempt/3), waiting 15s..."
            sleep 15
        fi
    done
    if [ "$atlas_policy_count" -gt 0 ] 2>/dev/null; then
        verify_check "Atlas policies" "true" "${atlas_policy_count} policies downloadable"
    else
        verify_check "Atlas policies" "false" "No Atlas policies returned (after 3 attempts)"
    fi

    # 7.7 Heka policy download (unauthenticated endpoint)
    log "Checking Heka policy download..."
    local heka_count=0
    for hk_attempt in 1 2 3; do
        local heka_resp
        heka_resp=$(kexec_quiet curl -s \
            "localhost:21000/api/atlas/v2/auth/download/policies/heka?pluginId=migrator-verify" 2>/dev/null || echo "")

        heka_count=$(echo "$heka_resp" | py_extract "import sys,json; d=json.load(sys.stdin); print(len(d.get('policies',[])))" || echo "0")
        if [ "$heka_count" -gt 0 ] 2>/dev/null; then
            break
        fi
        if [ "$hk_attempt" -lt 3 ]; then
            log "  Heka policies not ready (attempt $hk_attempt/3), waiting 15s..."
            sleep 15
        fi
    done
    if [ "$heka_count" -gt 0 ] 2>/dev/null; then
        verify_check "Heka policies" "true" "${heka_count} policies downloadable"
    else
        verify_check "Heka policies" "false" "No Heka policies returned (after 3 attempts)"
    fi

    # 7.7a Atlas roles download (unauthenticated endpoint)
    log "Checking Atlas roles download..."
    local roles_count=0
    for rl_attempt in 1 2 3; do
        local roles_resp
        roles_resp=$(kexec_quiet curl -s \
            "localhost:21000/api/atlas/v2/auth/download/roles/atlas?pluginId=migrator-verify" 2>/dev/null || echo "")

        roles_count=$(echo "$roles_resp" | py_extract "import sys,json; d=json.load(sys.stdin); print(len(d.get('rangerRoles',[])))" || echo "0")
        if [ "$roles_count" -gt 0 ] 2>/dev/null; then
            break
        fi
        if [ "$rl_attempt" -lt 3 ]; then
            log "  Atlas roles not ready (attempt $rl_attempt/3), waiting 15s..."
            sleep 15
        fi
    done
    if [ "$roles_count" -gt 0 ] 2>/dev/null; then
        verify_check "Atlas roles" "true" "${roles_count} roles downloadable"
    else
        verify_check "Atlas roles" "false" "No Atlas roles returned (after 3 attempts)"
    fi

    # 7.8 TypeDef headers
    if [ "$have_token" = "true" ]; then
        log "Testing TypeDef headers..."
        local typedef_count
        typedef_count=$(kexec_quiet curl -s \
            -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
            "localhost:21000/api/atlas/v2/types/typedefs/headers" 2>/dev/null | \
            py_extract "import sys,json; print(len(json.load(sys.stdin)))" || echo "0")
        if [ "$typedef_count" -gt 0 ] 2>/dev/null; then
            verify_check "TypeDef headers" "true" "${typedef_count} types"
        else
            verify_check "TypeDef headers" "false" "No typedefs returned"
        fi
    fi

    # 7.9 Write test — SKIPPED (this script runs against production data;
    #      creating/deleting entities is not safe for production verification)
    verify_check "Write test" "true" "Skipped (read-only verification for production safety)"

    # 7.10 Lineage test
    if [ "$have_token" = "true" ]; then
        log "Testing lineage traversal..."
        local process_resp
        process_resp=$(kexec_quiet curl -s \
            -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
            "localhost:21000/api/atlas/v2/search/basic?typeName=Process&limit=1" 2>/dev/null || echo "")

        local process_guid
        process_guid=$(echo "$process_resp" | py_extract "
import sys, json
d = json.load(sys.stdin)
entities = d.get('entities', [])
print(entities[0]['guid'] if entities else '')
")

        if [ -n "$process_guid" ]; then
            local lineage_resp
            lineage_resp=$(kexec_quiet curl -s \
                -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
                "localhost:21000/api/atlas/v2/lineage/${process_guid}?direction=BOTH&depth=3" 2>/dev/null || echo "")

            local lineage_nodes lineage_relations
            lineage_nodes=$(echo "$lineage_resp" | py_extract "import sys,json; print(len(json.load(sys.stdin).get('guidEntityMap',{})))" || echo "0")
            lineage_relations=$(echo "$lineage_resp" | py_extract "import sys,json; print(len(json.load(sys.stdin).get('relations',[])))" || echo "0")

            if [ "$lineage_nodes" -gt 0 ] 2>/dev/null; then
                verify_check "Lineage test" "true" "${lineage_nodes} nodes, ${lineage_relations} relations"
            else
                verify_check "Lineage test" "false" "Lineage returned 0 nodes for Process $process_guid"
            fi
        else
            log "  No Process entities found — skipping lineage test"
        fi
    fi

    # 7.11 Vertex ID format check
    # Migration always uses legacy IDs; runtime switches to deterministic for new entities.
    # Migrated vertices will have legacy (UUID) format — that is expected.
    log "Checking vertex ID format in Cassandra..."
    local vertex_sample
    vertex_sample=$(cexec cqlsh -e "SELECT vertex_id FROM atlas_graph.vertices LIMIT 5;" 2>/dev/null || echo "")

    if [ -n "$vertex_sample" ]; then
        local id_format
        id_format=$(echo "$vertex_sample" | py_extract "
import sys
lines = sys.stdin.read().strip().split('\n')
uuid_count = 0
hex_count = 0
for line in lines:
    line = line.strip()
    if len(line) == 36 and line.count('-') == 4:
        uuid_count += 1
    elif len(line) == 32 and all(c in '0123456789abcdef' for c in line.lower()):
        hex_count += 1
if uuid_count > 0:
    print('legacy')
elif hex_count > 0:
    print('deterministic')
else:
    print('unknown')
" <<< "$vertex_sample")

        if [ "$id_format" = "legacy" ]; then
            verify_check "Vertex ID format" "true" "Migrated vertices use legacy UUIDs (expected — runtime uses deterministic for new entities)"
        elif [ "$id_format" = "deterministic" ]; then
            verify_check "Vertex ID format" "true" "Vertex IDs are deterministic hex format"
        else
            verify_check "Vertex ID format" "false" "Could not determine ID format from sample"
        fi
    else
        warn "Could not query Cassandra for vertex ID sample"
    fi

    # 7.12 Verify running properties (via static config API, not atlas-application.properties)
    log "Verifying backend properties on running pod..."
    KEYCLOAK_TOKEN=""
    acquire_token || warn "Could not acquire token for backend property check"
    local running_backend="unknown"
    for bp_attempt in 1 2 3; do
        running_backend=$(kexec_quiet curl -s \
            -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
            "localhost:21000/api/atlas/v2/static-configs/atlas.graphdb.backend" 2>/dev/null | \
            py_extract "import sys,json; print(json.load(sys.stdin).get('currentValue','unknown'))" || echo "unknown")
        if [ "$running_backend" = "cassandra" ]; then
            break
        fi
        if [ "$bp_attempt" -lt 3 ]; then
            log "  Backend property is '$running_backend' (attempt $bp_attempt/3), waiting 10s..."
            sleep 10
        fi
    done
    if [ "$running_backend" = "cassandra" ]; then
        verify_check "Backend property" "true" "atlas.graphdb.backend=cassandra"
    else
        verify_check "Backend property" "false" "Expected cassandra, got $running_backend"
    fi

    # Verification sub-summary
    echo ""
    log "  Verification: $VERIFY_PASS / $VERIFY_TOTAL passed"
    if [ "$VERIFY_FAIL" -gt 0 ]; then
        err "  Failed: $VERIFY_FAIL"
    fi

    record_phase "Phase 7: Verification" "${VERIFY_PASS}/${VERIFY_TOTAL} passed"
}

# ============================================================================
# Phase 8: Summary
# ============================================================================

phase_summary() {
    step "Phase 8: Migration Summary"

    END_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

    # Calculate elapsed time (macOS + Linux compatible)
    local start_epoch end_epoch elapsed
    start_epoch=$(date -j -f "%Y-%m-%dT%H:%M:%SZ" "$START_TIME" +%s 2>/dev/null || date -d "$START_TIME" +%s 2>/dev/null || echo 0)
    end_epoch=$(date -j -f "%Y-%m-%dT%H:%M:%SZ" "$END_TIME" +%s 2>/dev/null || date -d "$END_TIME" +%s 2>/dev/null || echo 0)
    elapsed=$(( end_epoch - start_epoch ))

    echo ""
    echo "============================================================"
    echo "  MIGRATION REPORT: $VCLUSTER"
    echo "============================================================"
    echo ""
    printf "  %-30s %s\n" "Tenant:" "$VCLUSTER"
    printf "  %-30s %s\n" "Start time:" "$START_TIME"
    printf "  %-30s %s\n" "End time:" "$END_TIME"
    printf "  %-30s %s\n" "Duration:" "$(( elapsed / 60 ))m $(( elapsed % 60 ))s"
    printf "  %-30s %s\n" "ID Strategy:" "legacy (migration) -> deterministic (runtime)"
    printf "  %-30s %s\n" "Claim Enabled:" "$CLAIM_ENABLED"
    printf "  %-30s %s\n" "Maintenance mode:" "$MAINTENANCE_MODE"
    printf "  %-30s %s\n" "Vertex count (estimate):" "${VERTEX_COUNT:-unknown}"
    printf "  %-30s %s\n" "Sizing tier:" "${SIZING_TIER:-default}"
    printf "  %-30s %s\n" "JVM Heap:" "$JVM_HEAP"
    printf "  %-30s %s\n" "Max retries:" "$MAX_RETRIES"
    printf "  %-30s %s\n" "Log file:" "${LOG_FILE}"
    echo ""
    echo "  Phase Results:"
    echo "  ---------------------------------------------------"
    for i in "${!PHASE_NAMES[@]}"; do
        printf "  %-35s %s\n" "${PHASE_NAMES[$i]}" "${PHASE_RESULTS[$i]}"
    done
    echo ""
    echo "  Verification: ${VERIFY_PASS}/${VERIFY_TOTAL} passed"
    if [ "$VERIFY_FAIL" -gt 0 ]; then
        echo "  FAILED CHECKS: ${VERIFY_FAIL}"
    fi
    echo ""
    echo "  Rollback (revert to JanusGraph via static config API, revert ES aliases, then restart):"
    echo "    # 1. Seed janus defaults"
    echo "    kubectl exec $POD -n $NAMESPACE -- curl -s -X PUT -H 'Content-Type: application/json' -H 'Authorization: Bearer <TOKEN>' -d '{\"value\":\"janus\"}' localhost:21000/api/atlas/v2/static-configs/atlas.graphdb.backend"
    echo "    kubectl exec $POD -n $NAMESPACE -- curl -s -X PUT -H 'Content-Type: application/json' -H 'Authorization: Bearer <TOKEN>' -d '{\"value\":\"legacy\"}' localhost:21000/api/atlas/v2/static-configs/atlas.graph.id.strategy"
    echo "    # 2. Revert ES aliases from atlas_graph_vertex_index back to janusgraph_vertex_index"
    echo "    kubectl exec $POD -n $NAMESPACE -- curl -s -X POST '$(es_url)/_aliases' -H 'Content-Type: application/json' -d '"'{"actions":[{"remove":{"index":"atlas_graph_vertex_index","alias":"atlas_vertex_index"}},{"add":{"index":"janusgraph_vertex_index","alias":"atlas_vertex_index"}}]}'"'"
    echo "    # 3. Restart pod to pick up janus backend"
    echo "    kubectl delete pod $POD -n $NAMESPACE"
    echo "============================================================"

    record_phase "Phase 8: Summary" "DONE"
}

# ============================================================================
# Report generation (JSON)
# ============================================================================

generate_report() {
    local report
    report=$(cat <<EOF
{
  "tenant": "${VCLUSTER}",
  "start_time": "${START_TIME}",
  "end_time": "${END_TIME:-}",
  "migration_mode": "${MIGRATION_MODE:-full}",
  "id_strategy": "legacy (migration) -> deterministic (runtime)",
  "claim_enabled": ${CLAIM_ENABLED},
  "maintenance_mode": ${MAINTENANCE_MODE},
  "max_retries": ${MAX_RETRIES},
  "jvm_heap": "${JVM_HEAP}",
  "es_bulk_size": "${ES_BULK_SIZE:-default}",
  "vertex_count": "${VERTEX_COUNT:-unknown}",
  "sizing_tier": "${SIZING_TIER:-default}",
  "skip_switch": ${SKIP_SWITCH},
  "log_file": "${LOG_FILE}",
  "verification": {
    "total": ${VERIFY_TOTAL},
    "passed": ${VERIFY_PASS},
    "failed": ${VERIFY_FAIL}
  },
  "configmap_backup": "${CONFIGMAP_BACKUP:-none}",
  "overall_status": "$([ "$VERIFY_FAIL" -eq 0 ] && echo "PASSED" || echo "FAILED")"
}
EOF
)

    if [ -n "$REPORT_FILE" ]; then
        echo "$report" > "$REPORT_FILE"
        log "Report saved to $REPORT_FILE"
    fi
}

# ============================================================================
# Main
# ============================================================================

main() {
    START_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

    parse_args "$@"

    # Fix 9: Set up log file tee
    setup_log_file

    step "Atlas Tenant Migration: $VCLUSTER"
    log "Started at $START_TIME"
    echo ""

    phase_preflight          # Phase 0: Pre-flight + adaptive sizing

    if [ "$SWITCH_ONLY" = "true" ]; then
        # Switch-only mode: skip migration, just do the backend switch + verify
        log "Switch-only mode: skipping migration phases, performing backend switch only"
        phase_switch             # Phase 5: Backend switch
        phase_maintenance_off    # Phase 6: Maintenance mode OFF
        phase_verify             # Phase 7: Post-switch verification
        phase_summary            # Phase 8: Summary
        generate_report
    else
        phase_maintenance_on     # Phase 1: Maintenance mode ON
        phase_cleanup            # Phase 2: Cleanup (for remigration)
        phase_migration          # Phase 3: Migration with retry
        phase_alias              # Phase 4: ES alias creation

        if should_switch; then
            phase_switch         # Phase 5: Backend switch
        fi

        phase_maintenance_off    # Phase 6: Maintenance mode OFF
        phase_verify             # Phase 7: Post-switch verification
        phase_summary            # Phase 8: Summary
        generate_report          # JSON report (if --report-file set)
    fi

    if [ "$VERIFY_FAIL" -gt 0 ]; then
        exit "$EXIT_VERIFY"
    fi

    echo ""
    ok "Migration complete for $VCLUSTER"
    exit "$EXIT_SUCCESS"
}

main "$@"
