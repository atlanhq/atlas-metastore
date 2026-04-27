#!/usr/bin/env bash
#
# atlas_migrate_dev.sh — Dev/test migration orchestrator with local JAR upload
#
# Like atlas_migrate_tenant.sh but designed for development:
#   - Uploads a locally-built migrator JAR to a dedicated pod (--jar required)
#   - No maintenance mode
#   - Cleanup always runs before migration
#   - No ES alias management
#   - Pod auto-dies after 24h if connection drops (activeDeadlineSeconds)
#   - Pod is deleted on script exit (trap)
#
# Phases:
#   0. Pre-flight checks + adaptive sizing
#   1. Cleanup (always)
#   2. Spawn migrator pod
#   3. Upload JAR
#   4. Migration with retry
#   5. Backend switch (optional, --skip-switch to skip)
#   6. Post-switch verification (optional, --skip-verify to skip)
#   7. Summary
#
# Usage:
#   ./atlas_migrate_dev.sh --vcluster <name> --jar <path> [OPTIONS]
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
LOCAL_JAR=""
ID_STRATEGY="legacy"
CLAIM_ENABLED="false"
SCANNER_THREADS="8"
WRITER_THREADS="16"
ES_BULK_SIZE="10000"
ES_FIELD_LIMIT="10000"
QUEUE_CAPACITY="100000"
JVM_HEAP="8g"
JVM_MIN_HEAP="4g"

# Auto-sizing tracking
USER_SET_SCANNER=false
USER_SET_WRITER=false
USER_SET_CPU=false
USER_SET_MEMORY=false
USER_SET_HEAP=false
USER_SET_HEAP_MIN=false
USER_SET_BULK=false

MIGRATION_MODE=""          # "", "--fresh", "--es-only", "--validate-only"
MIGRATOR_CPU="6"
MIGRATOR_MEMORY="12Gi"
SKIP_SWITCH="false"
SKIP_VERIFY="false"
DRY_RUN="false"
REPORT_FILE=""
WAIT_TIMEOUT=300
MAX_RETRIES=3

# ES index names
OLD_ES_INDEX="janusgraph_vertex_index"
NEW_ES_INDEX="atlas_graph_vertex_index"

# Runtime state
MIGRATOR_POD_NAME=""
KEYCLOAK_TOKEN=""
VERIFY_PASS=0
VERIFY_FAIL=0
VERIFY_TOTAL=0
START_TIME=""
END_TIME=""
VERTEX_COUNT=""
SIZING_TIER=""
LOG_FILE=""
CONFIGMAP_BACKUP=""

# Resolved connection endpoints
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
# Python helper
# ============================================================================

py_extract() {
    local script="$1"
    local result
    result=$(python3 -c "$script" 2>/dev/null || python -c "$script" 2>/dev/null || echo "")
    echo "${result%"${result##*[![:space:]]}"}"
}

# ============================================================================
# Kubectl helpers
# ============================================================================

kexec() {
    kubectl exec "$POD" -n "$NAMESPACE" -c "$CONTAINER" -- "$@"
}

kexec_quiet() {
    kubectl exec "$POD" -n "$NAMESPACE" -c "$CONTAINER" -- "$@" 2>/dev/null
}

cexec() {
    kubectl exec "$CASSANDRA_POD" -n "$NAMESPACE" -c atlas-cassandra -- "$@"
}

es_url() {
    local proto="${RESOLVED_ES_PROTOCOL:-http}"
    local host="${RESOLVED_ES_HOST:-localhost}"
    local port="${RESOLVED_ES_PORT:-9200}"
    echo "${proto}://${host}:${port}"
}

# ============================================================================
# Trap: auto-delete migrator pod on exit
# ============================================================================

cleanup_pod() {
    if [ -n "$MIGRATOR_POD_NAME" ]; then
        log "Cleaning up migrator pod: $MIGRATOR_POD_NAME"
        kubectl delete pod "$MIGRATOR_POD_NAME" -n "$NAMESPACE" --ignore-not-found 2>/dev/null || true
        MIGRATOR_POD_NAME=""
    fi
}

trap cleanup_pod EXIT INT TERM

# ============================================================================
# Usage
# ============================================================================

show_help() {
    cat <<'USAGE'
Usage: atlas_migrate_dev.sh --vcluster <name> --jar <path> [OPTIONS]

Dev/test migration orchestrator. Uploads a local migrator JAR to a dedicated
pod, runs migration, and cleans up the pod on exit.

Phases:
  0  Pre-flight checks + auto-sizing
  1  Cleanup (always runs — drop atlas_graph keyspace + ES index)
  2  Spawn dedicated migrator pod
  3  Upload migrator JAR to pod
  4  Migration with retry
  5  Backend switch (skip with --skip-switch)
  6  Post-switch verification (skip with --skip-verify)
  7  Summary report

Required:
  --vcluster <name>       Target vcluster name (sets kubectl context)
  --jar <path>            Local path to migrator JAR file

Options:
  --namespace <ns>        Kubernetes namespace (default: atlas)
  --pod <name>            Atlas pod name (default: atlas-0)
  --container <name>      Container name (default: atlas-main)
  --cassandra-pod <name>  Cassandra pod name (default: atlas-cassandra-0)
  --id-strategy <s>       ID strategy: legacy|deterministic (default: legacy)
  --claim-enabled         Enable LWT dedup claims
  --scanner-threads <n>   Scanner parallelism (default: 8, auto-sized for large)
  --writer-threads <n>    Writer parallelism (default: 16, auto-sized for large)
  --es-bulk-size <n>      ES bulk indexing batch size (default: 10000)
  --es-field-limit <n>    ES index mapping.total_fields.limit (default: 10000)
  --queue-capacity <n>    Scanner→Writer queue capacity (default: 100000)
  --jvm-heap <size>       JVM max heap for migrator (default: 10g)
  --jvm-min-heap <size>   JVM initial heap for migrator (default: 4g)
  --fresh                 Clear migration state, start from scratch
  --es-only               ES reindex only (skip Cassandra scan)
  --validate-only         Validation only (no migration)
  --migrator-cpu <n>      CPU request/limit for migrator pod (default: 4)
  --migrator-memory <sz>  Memory request/limit for migrator pod (default: 8Gi)
  --skip-switch           Run migration but don't switch backend
  --skip-verify           Skip post-switch verification
  --max-retries <n>       Max migration retries on failure (default: 3)
  --dry-run               Show config summary without executing
  --report-file <path>    Save JSON report to file
  --wait-timeout <secs>   Pod readiness timeout (default: 300)
  --help                  Show this help

Pod Lifecycle:
  - Pod is deleted automatically when this script exits (trap on EXIT/INT/TERM)
  - If connection drops mid-job, pod auto-terminates after 24 hours
    (activeDeadlineSeconds: 86400)

Exit codes:
  0  Success
  1  Pre-flight failure
  2  Cleanup failure
  3  Migration failure
  4  Backend switch failure
  5  Verification failure

Examples:
  # First-time migration (fresh, don't switch backend yet)
  ./atlas_migrate_dev.sh --vcluster mytenantname \
      --jar ./graphdb/migrator/target/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar \
      --fresh --skip-switch

  # Remigration with retry
  ./atlas_migrate_dev.sh --vcluster mytenantname \
      --jar /tmp/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar \
      --fresh --max-retries 3

  # Dry run — show config without running
  ./atlas_migrate_dev.sh --vcluster mytenantname \
      --jar ./migrator.jar --dry-run

  # Full migration with switch + verify
  ./atlas_migrate_dev.sh --vcluster mytenantname \
      --jar ./migrator.jar --fresh
USAGE
}

# ============================================================================
# Argument parsing
# ============================================================================

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --vcluster)       VCLUSTER="$2"; shift 2 ;;
            --jar)            LOCAL_JAR="$2"; shift 2 ;;
            --namespace)      NAMESPACE="$2"; shift 2 ;;
            --pod)            POD="$2"; shift 2 ;;
            --container)      CONTAINER="$2"; shift 2 ;;
            --cassandra-pod)  CASSANDRA_POD="$2"; shift 2 ;;
            --id-strategy)    ID_STRATEGY="$2"; shift 2 ;;
            --claim-enabled)  CLAIM_ENABLED="true"; shift ;;
            --scanner-threads) SCANNER_THREADS="$2"; USER_SET_SCANNER=true; shift 2 ;;
            --writer-threads) WRITER_THREADS="$2"; USER_SET_WRITER=true; shift 2 ;;
            --es-bulk-size)   ES_BULK_SIZE="$2"; USER_SET_BULK=true; shift 2 ;;
            --es-field-limit) ES_FIELD_LIMIT="$2"; shift 2 ;;
            --queue-capacity) QUEUE_CAPACITY="$2"; shift 2 ;;
            --jvm-heap)       JVM_HEAP="$2"; USER_SET_HEAP=true; shift 2 ;;
            --jvm-min-heap)   JVM_MIN_HEAP="$2"; USER_SET_HEAP_MIN=true; shift 2 ;;
            --fresh)          MIGRATION_MODE="--fresh"; shift ;;
            --es-only)        MIGRATION_MODE="--es-only"; shift ;;
            --validate-only)  MIGRATION_MODE="--validate-only"; shift ;;
            --migrator-cpu)   MIGRATOR_CPU="$2"; USER_SET_CPU=true; shift 2 ;;
            --migrator-memory) MIGRATOR_MEMORY="$2"; USER_SET_MEMORY=true; shift 2 ;;
            --skip-switch)    SKIP_SWITCH="true"; shift ;;
            --skip-verify)    SKIP_VERIFY="true"; shift ;;
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
    if [ -z "$LOCAL_JAR" ]; then
        die "Missing required argument: --jar <path>" "$EXIT_PREFLIGHT"
    fi
}

# ============================================================================
# Helpers
# ============================================================================

should_switch() {
    [ "$SKIP_SWITCH" = "false" ] && [ "$MIGRATION_MODE" != "--validate-only" ] && [ "$MIGRATION_MODE" != "--es-only" ]
}

should_verify() {
    [ "$SKIP_VERIFY" = "false" ] && should_switch
}

# ============================================================================
# Log file setup
# ============================================================================

setup_log_file() {
    LOG_FILE="/tmp/atlas-migrate-dev-${VCLUSTER}-${TIMESTAMP}.log"
    exec > >(tee -a "$LOG_FILE") 2>&1
    log "Log file: $LOG_FILE"
}

# ============================================================================
# Phase 0: Pre-flight
# ============================================================================

phase_preflight() {
    step "Phase 0: Pre-flight Checks"

    # 0.1 Local JAR validation — must be the thin/original JAR, not the fat JAR
    log "Validating local migrator JAR..."

    # Auto-resolve: if user points to the fat JAR, look for the thin original-* beside it
    if [ -f "$LOCAL_JAR" ]; then
        local jar_dir jar_base
        jar_dir="$(dirname "$LOCAL_JAR")"
        jar_base="$(basename "$LOCAL_JAR")"
        local jar_size_mb
        jar_size_mb=$(( $(wc -c < "$LOCAL_JAR" | tr -d '[:space:]') / 1024 / 1024 ))

        if [ "$jar_size_mb" -gt 10 ] && [ ! "$(echo "$jar_base" | grep '^original-')" ]; then
            # Fat JAR detected — try to find the thin original-* JAR
            local thin_jar="${jar_dir}/original-${jar_base}"
            if [ -f "$thin_jar" ]; then
                warn "Provided JAR is the fat JAR (${jar_size_mb}MB) — too large for kubectl cp"
                log "Auto-resolved to thin JAR: $thin_jar"
                LOCAL_JAR="$thin_jar"
            else
                record_phase "Phase 0: Pre-flight" "FAIL"
                die "Provided JAR is the fat JAR (${jar_size_mb}MB). Use the thin JAR instead: original-${jar_base}" "$EXIT_PREFLIGHT"
            fi
        fi
    fi

    if [ ! -f "$LOCAL_JAR" ]; then
        record_phase "Phase 0: Pre-flight" "FAIL"
        die "Migrator JAR not found at: $LOCAL_JAR" "$EXIT_PREFLIGHT"
    fi
    local jar_size
    jar_size=$(wc -c < "$LOCAL_JAR" | tr -d '[:space:]')
    if [ "$jar_size" -lt 1000 ] 2>/dev/null; then
        record_phase "Phase 0: Pre-flight" "FAIL"
        die "Migrator JAR is suspiciously small (${jar_size} bytes): $LOCAL_JAR" "$EXIT_PREFLIGHT"
    fi
    ok "Local JAR: $LOCAL_JAR ($(( jar_size / 1024 ))KB)"

    # 0.2 kubectl context
    log "Setting kubectl context to vcluster: $VCLUSTER"
    if ! kubectl config use-context "$VCLUSTER" >/dev/null 2>&1; then
        if command -v vcluster >/dev/null 2>&1; then
            log "Attempting vcluster connect..."
            vcluster connect "$VCLUSTER" >/dev/null 2>&1 || true
        fi
        if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
            record_phase "Phase 0: Pre-flight" "FAIL"
            die "Cannot access namespace '$NAMESPACE' in vcluster '$VCLUSTER'. Check kubectl context." "$EXIT_PREFLIGHT"
        fi
    fi
    ok "kubectl context set"

    # 0.3 Pod health
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

    # 0.4 Atlas status
    log "Checking Atlas status..."
    local atlas_status
    atlas_status=$(kexec_quiet curl -s localhost:21000/api/atlas/admin/status 2>/dev/null | py_extract "import sys,json; print(json.load(sys.stdin).get('Status','UNKNOWN'))" || echo "UNREACHABLE")
    if [ "$atlas_status" != "ACTIVE" ]; then
        record_phase "Phase 0: Pre-flight" "FAIL"
        die "Atlas is not ACTIVE (status: $atlas_status)" "$EXIT_PREFLIGHT"
    fi
    ok "Atlas status: ACTIVE"

    # 0.5 atlas_migrate.sh present on pod (JAR will be uploaded)
    log "Checking atlas_migrate.sh on pod..."
    if ! kexec_quiet test -f /opt/apache-atlas/bin/atlas_migrate.sh; then
        record_phase "Phase 0: Pre-flight" "FAIL"
        die "atlas_migrate.sh not found on pod" "$EXIT_PREFLIGHT"
    fi
    ok "atlas_migrate.sh present on pod"

    # 0.6 Current backend
    log "Reading current backend..."
    local current_backend
    current_backend=$(kexec_quiet grep '^atlas.graphdb.backend=' /opt/apache-atlas/conf/atlas-application.properties 2>/dev/null | cut -d= -f2 || echo "janus")
    log "  Current backend: $current_backend"

    # 0.7 Cassandra pod
    log "Checking Cassandra pod..."
    local cass_status
    cass_status=$(kubectl get pod "$CASSANDRA_POD" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "NotFound")
    if [ "$cass_status" != "Running" ]; then
        warn "Cassandra pod $CASSANDRA_POD is not Running (status: $cass_status)"
    else
        ok "Cassandra pod $CASSANDRA_POD is Running"
    fi

    # 0.8 Resolve Cassandra/ES endpoints from atlas-application.properties
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

    # 0.9 Connectivity probes
    log "Testing Cassandra connectivity from Atlas pod..."
    if kexec_quiet bash -c "nc -z -w3 ${RESOLVED_CASS_HOST} ${RESOLVED_CASS_PORT}" 2>/dev/null; then
        ok "Cassandra ${RESOLVED_CASS_HOST}:${RESOLVED_CASS_PORT} reachable"
    else
        warn "Cannot reach Cassandra ${RESOLVED_CASS_HOST}:${RESOLVED_CASS_PORT}"
    fi

    log "Testing Elasticsearch connectivity from Atlas pod..."
    local es_probe_status
    es_probe_status=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "${RESOLVED_ES_PROTOCOL}://${RESOLVED_ES_HOST}:${RESOLVED_ES_PORT}/" 2>/dev/null || echo "000")
    if [ "$es_probe_status" = "200" ]; then
        ok "Elasticsearch ${RESOLVED_ES_HOST}:${RESOLVED_ES_PORT} reachable (HTTP 200)"
    else
        warn "Elasticsearch not reachable (HTTP $es_probe_status)"
    fi

    # 0.10 Adaptive sizing
    log "Probing vertex count for sizing recommendation..."
    local es_count_resp
    es_count_resp=$(kexec_quiet curl -s "${RESOLVED_ES_PROTOCOL}://${RESOLVED_ES_HOST}:${RESOLVED_ES_PORT}/janusgraph_vertex_index/_count" 2>/dev/null || echo "{}")
    VERTEX_COUNT=$(echo "$es_count_resp" | py_extract "import sys,json; print(json.load(sys.stdin).get('count',0))")
    VERTEX_COUNT="${VERTEX_COUNT:-0}"
    log "  Vertex count (estimate): ${VERTEX_COUNT}"

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
        # Defaults (CPU=4, Memory=8Gi, Heap=6g, Scanner=10, Writer=10) are fine for MEDIUM
    else
        SIZING_TIER="SMALL (<500K vertices)"
        # Dev script: keep defaults as-is for SMALL tenants (no downgrade)
    fi

    log "  Auto-sizing applied: tier=${SIZING_TIER}"
    log "    CPU: ${MIGRATOR_CPU}, Memory: ${MIGRATOR_MEMORY}, Heap: ${JVM_HEAP} (min: ${JVM_MIN_HEAP})"
    log "    Scanner: ${SCANNER_THREADS}, Writer: ${WRITER_THREADS}, ES Bulk: ${ES_BULK_SIZE}, Queue: ${QUEUE_CAPACITY}"

    # 0.11 Config summary
    step "Configuration Summary"
    log "  Vcluster:          $VCLUSTER"
    log "  Namespace:         $NAMESPACE"
    log "  Pod:               $POD"
    log "  Container:         $CONTAINER"
    log "  Cassandra Pod:     $CASSANDRA_POD"
    log "  Current backend:   $current_backend"
    log "  Local JAR:         $LOCAL_JAR"
    log "  ID Strategy:       $ID_STRATEGY"
    log "  Claim Enabled:     $CLAIM_ENABLED"
    log "  Migration Mode:    ${MIGRATION_MODE:-full}"
    log "  Migrator CPU:      $MIGRATOR_CPU"
    log "  Migrator Memory:   $MIGRATOR_MEMORY"
    log "  Skip Switch:       $SKIP_SWITCH"
    log "  Skip Verify:       $SKIP_VERIFY"
    log "  Max Retries:       $MAX_RETRIES"
    log "  Scanner Threads:   $SCANNER_THREADS"
    log "  Writer Threads:    $WRITER_THREADS"
    log "  ES Bulk Size:      ${ES_BULK_SIZE:-default}"
    log "  ES Field Limit:    $ES_FIELD_LIMIT"
    log "  Queue Capacity:    $QUEUE_CAPACITY"
    log "  JVM Heap:          $JVM_HEAP (min: $JVM_MIN_HEAP)"
    log "  Vertex Count:      ${VERTEX_COUNT}"
    log "  Sizing Tier:       ${SIZING_TIER}"
    log "  Log File:          ${LOG_FILE}"
    echo ""

    if [ "$DRY_RUN" = "true" ]; then
        record_phase "Phase 0: Pre-flight" "PASS (dry-run)"
        log "Dry-run mode -- exiting after config summary."
        exit "$EXIT_SUCCESS"
    fi

    record_phase "Phase 0: Pre-flight" "PASS"
}

# ============================================================================
# Phase 1: Cleanup (always runs)
# ============================================================================

phase_cleanup() {
    step "Phase 1: Cleanup"

    # Check if tenant is currently live on Cassandra backend
    log "Checking current backend before cleanup..."
    local current_backend
    current_backend=$(kexec_quiet bash -c \
        'grep "^atlas.graphdb.backend=" /opt/apache-atlas/conf/atlas-application.properties 2>/dev/null | cut -d= -f2 | tr -d "[:space:]"' \
        || echo "janus")

    if [ "$current_backend" = "cassandra" ]; then
        warn "Tenant is LIVE on Cassandra backend. Must revert to JanusGraph before dropping keyspace."
        log "Reverting backend to JanusGraph..."

        local revert_props_file="/tmp/atlas-props-revert-${VCLUSTER}-${TIMESTAMP}.txt"
        kexec_quiet cat /opt/apache-atlas/conf/atlas-application.properties > "$revert_props_file"

        local revert_clean="/tmp/atlas-props-revert-clean-${VCLUSTER}-${TIMESTAMP}.txt"
        grep -v \
            -e '^atlas\.graphdb\.backend=' \
            -e '^atlas\.graph\.index\.search\.es\.prefix=' \
            -e '^atlas\.cassandra\.graph\.' \
            -e '^atlas\.graph\.id\.strategy=' \
            -e '^atlas\.graph\.claim\.enabled=' \
            -e '^# === Zero Graph' \
            -e '^# ---- Zero Graph' \
            -e '^# ---- Temporarily reverted' \
            "$revert_props_file" > "$revert_clean" || true

        cat >> "$revert_clean" << 'EOF'

# ---- Reverted to JanusGraph for remigration cleanup ----
atlas.graphdb.backend=janus
atlas.graph.index.search.es.prefix=janusgraph_
EOF

        patch_configmap_properties "$revert_clean"
        rm -f "$revert_props_file" "$revert_clean"

        log "Restarting pod after JanusGraph revert..."
        kubectl delete pod "$POD" -n "$NAMESPACE"
        if ! kubectl wait --for=condition=Ready "pod/$POD" -n "$NAMESPACE" --timeout="${WAIT_TIMEOUT}s" 2>/dev/null; then
            record_phase "Phase 1: Cleanup" "FAIL (pod not ready after revert)"
            die "Pod did not become Ready after JanusGraph revert" "$EXIT_CLEANUP"
        fi

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
            record_phase "Phase 1: Cleanup" "FAIL (not ACTIVE after revert)"
            die "Atlas did not become ACTIVE after JanusGraph revert (status: $revert_status)" "$EXIT_CLEANUP"
        fi
        ok "Reverted to JanusGraph. Safe to drop Cassandra data."
    fi

    # Check if atlas_graph keyspace exists
    log "Checking for existing atlas_graph keyspace..."
    local ks_output ks_exists
    ks_output=$(cexec cqlsh -e "DESCRIBE KEYSPACES;" 2>&1 || echo "CQLSH_ERROR")
    log "  DESCRIBE KEYSPACES output: $ks_output"
    if echo "$ks_output" | grep -q "CQLSH_ERROR\|Connection error\|Could not connect"; then
        warn "cqlsh failed on $CASSANDRA_POD — cannot check keyspace existence"
        ks_exists=0
    else
        ks_exists=$(echo "$ks_output" | grep -c "atlas_graph" | tr -d '[:space:]' || echo 0)
    fi
    log "  atlas_graph keyspace exists: ${ks_exists}"

    # Check if ES index exists
    log "Checking for existing atlas_graph_vertex_index ES index..."
    local es_check
    es_check=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "$(es_url)/atlas_graph_vertex_index" 2>/dev/null || echo "000")
    local es_exists="false"
    if [ "$es_check" = "200" ]; then
        es_exists="true"
        log "  ES index atlas_graph_vertex_index exists (HTTP 200)"
    else
        log "  ES index atlas_graph_vertex_index not found (HTTP $es_check)"
    fi

    if [ "${ks_exists:-0}" -eq 0 ] 2>/dev/null && [ "$es_exists" = "false" ]; then
        log "No existing atlas_graph keyspace or ES index found -- skip cleanup"
        record_phase "Phase 1: Cleanup" "SKIPPED (nothing to clean)"
        return 0
    fi

    # Drop keyspace if it exists
    if [ "${ks_exists:-0}" -ne 0 ] 2>/dev/null; then
        log "Dropping atlas_graph keyspace..."
        if ! cexec cqlsh -e "DROP KEYSPACE IF EXISTS atlas_graph;" 2>/dev/null; then
            record_phase "Phase 1: Cleanup" "FAIL"
            die "Failed to drop atlas_graph keyspace" "$EXIT_CLEANUP"
        fi
        ok "Keyspace dropped"

        # Verify keyspace gone
        ks_exists=$(cexec cqlsh -e "DESCRIBE KEYSPACES;" 2>/dev/null | grep -c "atlas_graph" | tr -d '[:space:]' || echo 0)
        if [ "${ks_exists:-0}" -ne 0 ] 2>/dev/null; then
            record_phase "Phase 1: Cleanup" "FAIL"
            die "atlas_graph keyspace still exists after DROP" "$EXIT_CLEANUP"
        fi
    else
        log "No atlas_graph keyspace to drop"
    fi

    # Delete ES index if it exists
    if [ "$es_exists" = "true" ]; then
        log "Deleting atlas_graph_vertex_index ES index..."
        local es_resp
        es_resp=$(kexec_quiet curl -s -X DELETE "$(es_url)/atlas_graph_vertex_index" 2>/dev/null || echo '{"acknowledged":false}')
        if echo "$es_resp" | grep -q '"acknowledged":true\|"error".*"index_not_found_exception"'; then
            ok "ES index deleted"
        else
            warn "ES index delete response: $es_resp"
        fi

        # Verify ES index gone
        es_check=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "$(es_url)/atlas_graph_vertex_index" 2>/dev/null || echo "000")
        if [ "$es_check" = "404" ] || [ "$es_check" = "000" ]; then
            ok "ES index deletion verified"
        else
            warn "ES index may still exist (HTTP $es_check)"
        fi
    else
        log "No ES index to delete"
    fi

    ok "Cleanup complete: keyspace and ES index removed"
    record_phase "Phase 1: Cleanup" "PASS"
}

# ============================================================================
# Phase 2 + 3: Spawn migrator pod + upload JAR
# ============================================================================

spawn_migrator_pod() {
    step "Phase 2: Spawn Migrator Pod"

    # Read runtime info from the running atlas-0 pod
    log "Reading runtime info from $POD..."
    local atlas_image
    atlas_image=$(kubectl get pod "$POD" -n "$NAMESPACE" -o jsonpath="{.spec.containers[?(@.name=='${CONTAINER}')].image}" 2>/dev/null || echo "")
    if [ -z "$atlas_image" ]; then
        record_phase "Phase 2: Spawn Pod" "FAIL"
        die "Could not determine container image from pod $POD" "$EXIT_MIGRATION"
    fi
    log "  Image: $atlas_image"

    # imagePullSecrets
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

    # Tolerations
    local tolerations_yaml=""
    local tolerations_json
    tolerations_json=$(kubectl get pod "$POD" -n "$NAMESPACE" -o jsonpath='{.spec.tolerations}' 2>/dev/null || echo "")
    if [ -n "$tolerations_json" ] && [ "$tolerations_json" != "[]" ] && [ "$tolerations_json" != "null" ]; then
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

    # Multitenant detection
    local is_multitenant="false"
    if kubectl get secret atlas-secret-manager -n "$NAMESPACE" >/dev/null 2>&1; then
        is_multitenant="true"
        log "  Multitenant: yes"
    else
        log "  Multitenant: no"
    fi

    local multitenant_env_yaml=""
    if [ "$is_multitenant" = "true" ]; then
        multitenant_env_yaml="    - secretRef:
        name: atlas-secret-manager
    - secretRef:
        name: atlas-secret-parameter-store"
    fi

    # Clean up any existing dev migration pods
    log "Cleaning up any existing dev migration pods..."
    kubectl delete pods -n "$NAMESPACE" -l app=atlas-migrator --ignore-not-found 2>/dev/null || true
    sleep 3

    MIGRATOR_POD_NAME="atlas-migrator"
    local pod_manifest="/tmp/migrator-dev-pod-${VCLUSTER}-${TIMESTAMP}.yaml"

    cat > "$pod_manifest" <<EOYAML
apiVersion: v1
kind: Pod
metadata:
  name: ${MIGRATOR_POD_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: atlas-migrator
spec:
  activeDeadlineSeconds: 86400
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
    command: ["sleep", "86400"]
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

    log "Applying migrator pod manifest..."
    if ! kubectl apply -f "$pod_manifest"; then
        record_phase "Phase 2: Spawn Pod" "FAIL"
        die "kubectl apply failed for migrator pod" "$EXIT_MIGRATION"
    fi
    rm -f "$pod_manifest"
    ok "Pod manifest applied"

    # Wait for pod to be Running
    log "Waiting for migrator pod to start..."
    local pod_ready="false"
    for i in $(seq 1 60); do
        local phase
        phase=$(kubectl get pod "$MIGRATOR_POD_NAME" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
        if [ "$phase" = "Running" ]; then
            pod_ready="true"
            break
        fi
        if [ "$((i % 6))" -eq 0 ]; then
            log "  Migrator pod status: $phase (waiting...)"
        fi
        sleep 5
    done

    if [ "$pod_ready" = "false" ]; then
        record_phase "Phase 2: Spawn Pod" "FAIL (pod not running after 5min)"
        die "Migrator pod did not reach Running state" "$EXIT_MIGRATION"
    fi
    ok "Migrator pod $MIGRATOR_POD_NAME is Running"
    record_phase "Phase 2: Spawn Pod" "PASS"

    # Phase 3: Upload JAR + scripts
    step "Phase 3: Upload JAR + Scripts"
    log "Uploading thin migrator JAR to pod..."
    log "  Source: $LOCAL_JAR"
    log "  Target: ${MIGRATOR_POD_NAME}:/opt/apache-atlas/libext/atlas-graphdb-migrator.jar"

    # Upload to libext/ (not tools/) — thin JAR will be added to classpath
    kubectl exec "$MIGRATOR_POD_NAME" -n "$NAMESPACE" -c atlas-migrator -- \
        mkdir -p /opt/apache-atlas/libext 2>/dev/null || true

    if ! kubectl cp "$LOCAL_JAR" "${NAMESPACE}/${MIGRATOR_POD_NAME}:/opt/apache-atlas/libext/atlas-graphdb-migrator.jar" -c atlas-migrator; then
        record_phase "Phase 3: Upload JAR" "FAIL"
        die "Failed to upload JAR to migrator pod" "$EXIT_MIGRATION"
    fi

    # Verify JAR landed
    local remote_size
    remote_size=$(kubectl exec "$MIGRATOR_POD_NAME" -n "$NAMESPACE" -c atlas-migrator -- \
        wc -c /opt/apache-atlas/libext/atlas-graphdb-migrator.jar 2>/dev/null | awk '{print $1}' || echo "0")
    local local_size
    local_size=$(wc -c < "$LOCAL_JAR" | tr -d '[:space:]')

    if [ "$remote_size" = "$local_size" ]; then
        ok "JAR uploaded and verified (${local_size} bytes)"
    else
        warn "JAR size mismatch: local=${local_size}, remote=${remote_size}"
    fi
    # Also upload local atlas_migrate.sh so any local changes take effect
    local local_migrate_sh
    local_migrate_sh="$(cd "$(dirname "$0")" && pwd)/atlas_migrate.sh"
    if [ -f "$local_migrate_sh" ]; then
        log "Uploading local atlas_migrate.sh to pod..."
        if kubectl cp "$local_migrate_sh" "${NAMESPACE}/${MIGRATOR_POD_NAME}:/opt/apache-atlas/bin/atlas_migrate.sh" -c atlas-migrator; then
            kubectl exec "$MIGRATOR_POD_NAME" -n "$NAMESPACE" -c atlas-migrator -- \
                chmod +x /opt/apache-atlas/bin/atlas_migrate.sh 2>/dev/null || true
            ok "atlas_migrate.sh uploaded"
        else
            warn "Failed to upload atlas_migrate.sh — using version from pod image"
        fi
    fi

    record_phase "Phase 3: Upload JAR" "PASS"
}

# ============================================================================
# Phase 4: Migration with retry (kubectl exec)
# ============================================================================

phase_migration() {
    if [ "$MIGRATION_MODE" = "--validate-only" ]; then
        step "Phase 4: Validation Only"
    else
        step "Phase 4: Migration"
    fi

    # Step 1: Generate migrator.properties using atlas_migrate.sh --dry-run
    # This reads atlas-application.properties and produces /tmp/migrator.properties
    log "Generating migrator.properties via atlas_migrate.sh --dry-run..."
    local gen_exit=0
    kubectl exec "$MIGRATOR_POD_NAME" -n "$NAMESPACE" -c atlas-migrator -- bash -c "
        export ID_STRATEGY='${ID_STRATEGY}'
        export CLAIM_ENABLED='${CLAIM_ENABLED}'
        export SCANNER_THREADS='${SCANNER_THREADS}'
        export WRITER_THREADS='${WRITER_THREADS}'
        export ES_BULK_SIZE='${ES_BULK_SIZE:-1000}'
        export QUEUE_CAPACITY='${QUEUE_CAPACITY}'
        export MIGRATOR_JVM_HEAP='${JVM_HEAP}'
        export MIGRATOR_JVM_MIN_HEAP='${JVM_MIN_HEAP}'
        export SOURCE_CONSISTENCY='ONE'
        export TARGET_CONSISTENCY='LOCAL_QUORUM'
        export ES_FIELD_LIMIT='${ES_FIELD_LIMIT}'
        export SKIP_BACKUP_CHECK=true
        /opt/apache-atlas/bin/atlas_migrate.sh --dry-run
    " || gen_exit=$?

    if [ "$gen_exit" -ne 0 ]; then
        record_phase "Phase 4: Migration" "FAIL (properties generation)"
        die "Failed to generate migrator.properties (exit: $gen_exit)" "$EXIT_MIGRATION"
    fi
    ok "migrator.properties generated"

    # Append extra tuning properties not covered by atlas_migrate.sh
    kubectl exec "$MIGRATOR_POD_NAME" -n "$NAMESPACE" -c atlas-migrator -- bash -c "
        echo '' >> /tmp/migrator.properties
        echo '# Extra tuning (appended by atlas_migrate_dev.sh)' >> /tmp/migrator.properties
        echo 'migration.writer.max.inflight.per.thread=100' >> /tmp/migrator.properties
        echo 'migration.writer.batch.size=1000' >> /tmp/migrator.properties
        echo 'migration.es.vertex.indexer.threads=3' >> /tmp/migrator.properties
        echo 'migration.writer.max.edges.per.batch=5' >> /tmp/migrator.properties
    "
    ok "Extra tuning properties appended"

    # Step 2: Write custom logback config with DEBUG + rolling file
    log "Writing migrator logback config (DEBUG + rolling file)..."
    kubectl exec "$MIGRATOR_POD_NAME" -n "$NAMESPACE" -c atlas-migrator -- bash -c 'cat > /tmp/migrator-logback.xml << '\''LOGBACK'\''
<configuration>
  <property name="LOG_DIR" value="/opt/apache-atlas/logs" />

  <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
    <encoder>
      <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
    </encoder>
  </appender>

  <appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>${LOG_DIR}/migrator.log</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy">
      <fileNamePattern>${LOG_DIR}/migrator.%d{yyyy-MM-dd}.%i.log.gz</fileNamePattern>
      <maxFileSize>100MB</maxFileSize>
      <maxHistory>5</maxHistory>
      <totalSizeCap>500MB</totalSizeCap>
    </rollingPolicy>
    <encoder>
      <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
    </encoder>
  </appender>

  <!-- Migrator: DEBUG -->
  <logger name="org.apache.atlas.repository.graphdb.migrator" level="DEBUG" />

  <!-- Cassandra driver: WARN (very noisy at INFO) -->
  <logger name="com.datastax" level="WARN" />

  <!-- ES REST client: WARN -->
  <logger name="org.elasticsearch" level="WARN" />

  <!-- JanusGraph: INFO -->
  <logger name="org.janusgraph" level="INFO" />

  <root level="INFO">
    <appender-ref ref="CONSOLE" />
    <appender-ref ref="FILE" />
  </root>
</configuration>
LOGBACK'
    ok "Logback config written to /tmp/migrator-logback.xml"

    # Step 3: Run migration using java -cp (thin JAR + existing WEB-INF/lib deps)
    local attempt=0
    local migration_passed="false"

    while [ "$attempt" -lt "$MAX_RETRIES" ] && [ "$migration_passed" = "false" ]; do
        attempt=$((attempt + 1))
        log "Migration attempt $attempt/$MAX_RETRIES"

        # First attempt uses MIGRATION_MODE, retries resume
        local mode_flag="$MIGRATION_MODE"
        if [ "$attempt" -gt 1 ]; then
            mode_flag=""
        fi

        local exec_exit=0
        kubectl exec "$MIGRATOR_POD_NAME" -n "$NAMESPACE" -c atlas-migrator -- bash -c "
            ATLAS_HOME=/opt/apache-atlas
            LIB_DIR=\${ATLAS_HOME}/server/webapp/atlas/WEB-INF/lib
            MIGRATOR_JAR=\${ATLAS_HOME}/libext/atlas-graphdb-migrator.jar
            CLASSPATH=\"\${MIGRATOR_JAR}:\${LIB_DIR}/*\"

            export DOMAIN_NAME='${VCLUSTER}'
            [ -n '${MIXPANEL_TOKEN:-}' ] && export MIXPANEL_TOKEN='${MIXPANEL_TOKEN}'
            [ -n '${MIXPANEL_API_SECRET:-}' ] && export MIXPANEL_API_SECRET='${MIXPANEL_API_SECRET}'

            echo '=== Atlas Migrator (thin JAR + classpath) ==='
            echo \"Mode: ${mode_flag:-full}\"
            echo \"Tenant: \${DOMAIN_NAME}\"
            echo \"Classpath: \${MIGRATOR_JAR} + \${LIB_DIR}/*\"
            echo 'Log level: DEBUG (org.apache.atlas.repository.graphdb.migrator)'
            echo 'Log file:  /opt/apache-atlas/logs/migrator.log (rolling, 100MB x 5)'
            echo ''

            java \\
                -Xmx${JVM_HEAP} -Xms${JVM_MIN_HEAP} \\
                --add-opens java.base/java.lang=ALL-UNNAMED \\
                -Dlogback.configurationFile=file:/tmp/migrator-logback.xml \\
                -cp \"\${CLASSPATH}\" \\
                org.apache.atlas.repository.graphdb.migrator.MigratorMain \\
                /tmp/migrator.properties \\
                ${mode_flag}
        " || exec_exit=$?

        if [ "$exec_exit" -eq 0 ]; then
            migration_passed="true"
            ok "Migration attempt $attempt: PASSED"
        else
            warn "Migration attempt $attempt failed (exit code: $exec_exit)"
            if [ "$attempt" -lt "$MAX_RETRIES" ]; then
                log "Retrying... (migration is resumable, will pick up from failed token ranges)"
                sleep 10
            fi
        fi
    done

    if [ "$migration_passed" = "false" ]; then
        record_phase "Phase 4: Migration" "FAIL (after $MAX_RETRIES attempts)"
        die "Migration failed after $MAX_RETRIES attempts" "$EXIT_MIGRATION"
    fi

    record_phase "Phase 4: Migration" "PASS (attempt $attempt/$MAX_RETRIES)"
}

# ============================================================================
# ConfigMap patching helper
# ============================================================================

patch_configmap_properties() {
    local new_props_file="$1"

    local cm_json
    cm_json=$(kubectl get configmap atlas-config -n "$NAMESPACE" -o json)

    local key_count
    key_count=$(echo "$cm_json" | py_extract "import sys,json; print(len(json.load(sys.stdin).get('data',{})))")

    if [ "$key_count" -gt 1 ] 2>/dev/null; then
        log "  ConfigMap has $key_count keys -- using JSON patch to preserve other keys"
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
# Phase 5: Backend Switch
# ============================================================================

phase_switch() {
    if ! should_switch; then
        log "Skipping backend switch (--skip-switch or mode incompatible)"
        record_phase "Phase 5: Backend Switch" "SKIPPED"
        return 0
    fi

    step "Phase 5: Backend Switch"

    # Backup ConfigMap
    log "Backing up ConfigMap..."
    CONFIGMAP_BACKUP="/tmp/atlas-config-backup-${VCLUSTER}-${TIMESTAMP}.yaml"
    kubectl get configmap atlas-config -n "$NAMESPACE" -o yaml > "$CONFIGMAP_BACKUP"
    ok "ConfigMap backed up to $CONFIGMAP_BACKUP"

    # Extract and patch properties
    log "Patching ConfigMap with Cassandra backend properties..."
    local props_file="/tmp/atlas-props-${VCLUSTER}-${TIMESTAMP}.txt"
    local props_clean="/tmp/atlas-props-clean-${VCLUSTER}-${TIMESTAMP}.txt"

    kubectl get configmap atlas-config -n "$NAMESPACE" -o jsonpath='{.data.atlas-application\.properties}' > "$props_file"

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

    local cass_hostname
    cass_hostname=$(grep '^atlas.graph.storage.hostname=' "$props_file" | cut -d= -f2 | head -1 || echo "")
    if [ -z "$cass_hostname" ]; then
        cass_hostname=$(grep '^atlas.cassandra.graph.hostname=' "$props_file" | cut -d= -f2 | head -1 || echo "atlas-cassandra")
    fi

    local cass_dc
    cass_dc=$(grep '^atlas.cassandra.graph.datacenter=' "$props_file" | cut -d= -f2 | head -1 || echo "")
    if [ -z "$cass_dc" ]; then
        cass_dc=$(grep '^atlas.graph.storage.cql.local-datacenter=' "$props_file" | cut -d= -f2 | head -1 || echo "")
    fi
    if [ -z "$cass_dc" ]; then
        cass_dc="datacenter1"
        warn "Could not read Cassandra datacenter from config -- using default: $cass_dc"
    fi
    log "  Cassandra host: $cass_hostname, datacenter: $cass_dc"

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

    # Verify ConfigMap
    log "Verifying ConfigMap after patch..."
    local verify_props
    verify_props=$(kubectl get configmap atlas-config -n "$NAMESPACE" -o jsonpath='{.data.atlas-application\.properties}')

    local verify_ok="true"
    local verify_backend
    verify_backend=$(echo "$verify_props" | grep '^atlas.graphdb.backend=' | cut -d= -f2 | head -1 || echo "")
    if [ "$verify_backend" = "cassandra" ]; then
        ok "  atlas.graphdb.backend=cassandra"
    else
        warn "  atlas.graphdb.backend='$verify_backend' (expected 'cassandra')"
        verify_ok="false"
    fi

    local verify_prefix
    verify_prefix=$(echo "$verify_props" | grep '^atlas.graph.index.search.es.prefix=' | cut -d= -f2 | head -1 || echo "")
    if [ -n "$verify_prefix" ]; then
        ok "  atlas.graph.index.search.es.prefix=$verify_prefix"
    else
        warn "  atlas.graph.index.search.es.prefix not found"
        verify_ok="false"
    fi

    if [ "$verify_ok" = "true" ]; then
        ok "ConfigMap verification passed"
    else
        warn "ConfigMap verification had issues -- review manually"
    fi

    # Rolling restart
    log "Triggering rolling restart of Atlas StatefulSet..."
    if ! kubectl rollout restart statefulset/atlas -n "$NAMESPACE"; then
        warn "kubectl rollout restart failed -- falling back to manual pod delete"
        kubectl delete pod "$POD" -n "$NAMESPACE" || true
    fi

    log "Waiting for all pods to pick up new config (timeout: 600s)..."
    if ! kubectl rollout status statefulset/atlas -n "$NAMESPACE" --timeout=600s; then
        record_phase "Phase 5: Backend Switch" "FAIL (rollout timeout)"
        warn "ConfigMap backup available at: $CONFIGMAP_BACKUP"
        die "StatefulSet rollout did not complete within 600s" "$EXIT_SWITCH"
    fi
    ok "All pods updated"

    # Verify Atlas ACTIVE on all pods
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

    warn "ConfigMap backup available at: $CONFIGMAP_BACKUP"

    if [ "$all_active" = "true" ]; then
        ok "All pods are ACTIVE on Cassandra backend"
        record_phase "Phase 5: Backend Switch" "PASS"
    else
        warn "Some pods did not reach ACTIVE state -- check manually"
        record_phase "Phase 5: Backend Switch" "WARN (not all pods ACTIVE)"
    fi
}

# ============================================================================
# Phase 6: Verification
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
        warn "Could not read keycloak.json -- trying environment variables"
        kc_json=$(kexec_quiet python3 -c "
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
        warn "Could not acquire Keycloak credentials -- skipping authenticated checks"
        return 1
    fi

    local kc_url kc_realm kc_client kc_secret
    kc_url=$(echo "$kc_json" | py_extract "import sys,json; print(json.load(sys.stdin)['url'])")
    kc_realm=$(echo "$kc_json" | py_extract "import sys,json; print(json.load(sys.stdin)['realm'])")
    kc_client=$(echo "$kc_json" | py_extract "import sys,json; print(json.load(sys.stdin)['client'])")
    kc_secret=$(echo "$kc_json" | py_extract "import sys,json; print(json.load(sys.stdin)['secret'])")

    if [ -z "$kc_url" ] || [ -z "$kc_realm" ] || [ -z "$kc_client" ] || [ -z "$kc_secret" ]; then
        warn "Incomplete Keycloak credentials -- skipping authenticated checks"
        return 1
    fi

    local token_resp
    token_resp=$(kexec_quiet curl -s \
        -d "grant_type=client_credentials&client_id=${kc_client}&client_secret=${kc_secret}" \
        "${kc_url}/realms/${kc_realm}/protocol/openid-connect/token" 2>/dev/null || echo "")

    KEYCLOAK_TOKEN=$(echo "$token_resp" | py_extract "import sys,json; print(json.load(sys.stdin).get('access_token',''))")

    if [ -z "$KEYCLOAK_TOKEN" ]; then
        warn "Failed to acquire token -- skipping authenticated checks"
        return 1
    fi

    ok "Keycloak token acquired"
    return 0
}

phase_verify() {
    if ! should_verify; then
        log "Skipping verification (--skip-verify or backend not switched)"
        record_phase "Phase 6: Verification" "SKIPPED"
        return 0
    fi

    step "Phase 6: Post-Switch Verification"

    local have_token="false"
    if acquire_token; then
        have_token="true"
    fi

    # 6.1 Table search
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
            verify_check "Table search" "false" "No Tables found"
        fi

        if [ "$scrubbed_count" -gt 0 ] 2>/dev/null; then
            verify_check "Table lock icons" "false" "${scrubbed_count} Tables have lock icons (scrubbed=true)"
        elif [ "$table_total" -gt 0 ] 2>/dev/null; then
            verify_check "Table lock icons" "true" "No lock icons detected"
        fi
    fi

    # 6.2 Connection search
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

        if [ "$conn_total" -gt 0 ] 2>/dev/null; then
            verify_check "Connection search" "true" "${conn_total} Connections found"
        else
            verify_check "Connection search" "false" "No Connections found"
        fi
    fi

    # 6.3 AuthPolicy in ES
    log "Checking AuthPolicy in ES..."
    local auth_count
    auth_count=$(kexec_quiet curl -s "$(es_url)/atlas_graph_vertex_index/_count?q=__typeName.keyword:AuthPolicy" 2>/dev/null | \
        py_extract "import sys,json; print(json.load(sys.stdin).get('count',0))" || echo "0")
    if [ "$auth_count" -gt 0 ] 2>/dev/null; then
        verify_check "AuthPolicy ES" "true" "${auth_count} policies in ES"
    else
        verify_check "AuthPolicy ES" "false" "No AuthPolicies found in ES"
    fi

    # 6.4 ES doc count comparison
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
        local within_tolerance
        within_tolerance=$(py_extract "print('true' if float('$es_diff_pct') <= 5.0 else 'false')")
        if [ "$within_tolerance" = "true" ]; then
            verify_check "ES doc count" "true" "Old: ${old_es_count}, New: ${new_es_count} (diff: ${es_diff_pct}%)"
        else
            verify_check "ES doc count" "false" "Old: ${old_es_count}, New: ${new_es_count} (diff: ${es_diff_pct}% > 5%)"
        fi
    elif [ "$new_es_count" -gt 0 ] 2>/dev/null; then
        verify_check "ES doc count" "true" "New index: ${new_es_count} docs"
    else
        verify_check "ES doc count" "false" "New ES index has 0 docs"
    fi

    # 6.5 Token range completion
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
    print(f'skip|0|0|0|No token ranges found')
elif failed == 0 and pending == 0 and in_progress == 0:
    print(f'pass|{completed}|0|0|All {completed} token ranges COMPLETED')
else:
    print(f'fail|{completed}|{failed}|{pending + in_progress}|{completed}/{total} completed, {failed} failed, {pending + in_progress} pending')
")
        local tr_status tr_detail
        tr_status=$(echo "$token_range_result" | cut -d'|' -f1)
        tr_detail=$(echo "$token_range_result" | cut -d'|' -f5)

        if [ "$tr_status" = "pass" ]; then
            verify_check "Token ranges" "true" "$tr_detail"
        elif [ "$tr_status" = "fail" ]; then
            verify_check "Token ranges" "false" "$tr_detail"
        else
            log "  Token ranges: $tr_detail"
        fi
    else
        warn "Could not query migration_state table"
    fi

    # 6.6 Write test (create + delete)
    if [ "$have_token" = "true" ]; then
        log "Running write test (create + delete entity)..."
        local create_resp
        create_resp=$(kexec_quiet curl -s -X POST \
            -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
            -H "Content-Type: application/json" \
            -d '{"entity":{"typeName":"Table","attributes":{"qualifiedName":"zg-migration-write-test-delete-me","name":"zg-migration-write-test-delete-me"}}}' \
            "localhost:21000/api/atlas/v2/entity" 2>/dev/null || echo "")

        local test_guid
        test_guid=$(echo "$create_resp" | py_extract "
import sys, json
d = json.load(sys.stdin)
guids = d.get('guidAssignments', d.get('mutatedEntities', {}).get('CREATE', []))
if isinstance(guids, dict) and guids:
    print(list(guids.values())[0])
elif isinstance(guids, list) and guids:
    print(guids[0].get('guid', ''))
else:
    print('')
")

        if [ -n "$test_guid" ]; then
            kexec_quiet curl -s -X DELETE \
                -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
                "localhost:21000/api/atlas/v2/entity/guid/${test_guid}?type=HARD" 2>/dev/null || true
            verify_check "Write test" "true" "Created and deleted entity $test_guid"
        else
            verify_check "Write test" "false" "Could not create test entity"
        fi
    fi

    # 6.7 Backend property
    log "Verifying backend properties on running pod..."
    local running_backend
    running_backend=$(kexec_quiet grep '^atlas.graphdb.backend=' /opt/apache-atlas/conf/atlas-application.properties 2>/dev/null | cut -d= -f2 || echo "unknown")
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
        warn "ConfigMap backup available at: $CONFIGMAP_BACKUP"
    fi

    record_phase "Phase 6: Verification" "${VERIFY_PASS}/${VERIFY_TOTAL} passed"
}

# ============================================================================
# Phase 7: Summary
# ============================================================================

phase_summary() {
    step "Phase 7: Migration Summary"

    END_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

    local start_epoch end_epoch elapsed
    start_epoch=$(date -j -f "%Y-%m-%dT%H:%M:%SZ" "$START_TIME" +%s 2>/dev/null || date -d "$START_TIME" +%s 2>/dev/null || echo 0)
    end_epoch=$(date -j -f "%Y-%m-%dT%H:%M:%SZ" "$END_TIME" +%s 2>/dev/null || date -d "$END_TIME" +%s 2>/dev/null || echo 0)
    elapsed=$(( end_epoch - start_epoch ))

    echo ""
    echo "============================================================"
    echo "  DEV MIGRATION REPORT: $VCLUSTER"
    echo "============================================================"
    echo ""
    printf "  %-30s %s\n" "Tenant:" "$VCLUSTER"
    printf "  %-30s %s\n" "Start time:" "$START_TIME"
    printf "  %-30s %s\n" "End time:" "$END_TIME"
    printf "  %-30s %s\n" "Duration:" "$(( elapsed / 60 ))m $(( elapsed % 60 ))s"
    printf "  %-30s %s\n" "Local JAR:" "$LOCAL_JAR"
    printf "  %-30s %s\n" "ID Strategy:" "$ID_STRATEGY"
    printf "  %-30s %s\n" "Migration Mode:" "${MIGRATION_MODE:-full}"
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
    if [ -n "$CONFIGMAP_BACKUP" ]; then
        echo "  Rollback: kubectl apply -f $CONFIGMAP_BACKUP && kubectl delete pod $POD -n $NAMESPACE"
    fi
    echo "============================================================"

    record_phase "Phase 7: Summary" "DONE"
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
  "local_jar": "${LOCAL_JAR}",
  "id_strategy": "${ID_STRATEGY}",
  "claim_enabled": ${CLAIM_ENABLED},
  "max_retries": ${MAX_RETRIES},
  "jvm_heap": "${JVM_HEAP}",
  "es_bulk_size": "${ES_BULK_SIZE:-default}",
  "vertex_count": "${VERTEX_COUNT:-unknown}",
  "sizing_tier": "${SIZING_TIER:-default}",
  "skip_switch": ${SKIP_SWITCH},
  "skip_verify": ${SKIP_VERIFY},
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
    setup_log_file

    step "Atlas Dev Migration: $VCLUSTER"
    log "Started at $START_TIME"
    echo ""

    phase_preflight          # Phase 0: Pre-flight + adaptive sizing
    phase_cleanup            # Phase 1: Cleanup (always runs)
    spawn_migrator_pod       # Phase 2 + 3: Spawn pod + upload JAR
    phase_migration          # Phase 4: Migration with retry
    phase_switch             # Phase 5: Backend switch (optional)
    phase_verify             # Phase 6: Verification (optional)
    phase_summary            # Phase 7: Summary
    generate_report

    if [ "$VERIFY_FAIL" -gt 0 ]; then
        exit "$EXIT_VERIFY"
    fi

    echo ""
    ok "Dev migration complete for $VCLUSTER"
    exit "$EXIT_SUCCESS"
}

main "$@"
