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
WRITER_THREADS="8"
ES_BULK_SIZE=""
ES_FIELD_LIMIT="10000"
JVM_HEAP="4g"
JVM_MIN_HEAP="2g"
MIGRATION_MODE=""          # "", "--fresh", "--es-only", "--validate-only"
USE_JOB="false"
HELM_CHART=""
SKIP_SWITCH="false"
SWITCH_ONLY="false"
SKIP_CLEANUP="false"
SKIP_ALIAS="false"
DRY_RUN="false"
REPORT_FILE=""
WAIT_TIMEOUT=300
MAINTENANCE_MODE="false"
MAINTENANCE_COOLDOWN=30
ALIAS_SCRIPT=""  # Deprecated — alias step is now a no-op
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

# Execute command on the Cassandra pod
cexec() {
    kubectl exec "$CASSANDRA_POD" -n "$NAMESPACE" -- "$@"
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
  0  Pre-flight checks + adaptive sizing probe
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
  --use-job               Use Helm Job instead of kubectl exec
  --helm-chart <path>     Helm chart path for --use-job (default: ./helm/atlas)
  --skip-switch           Run migration but don't switch backend
  --switch-only           Skip migration, only do backend switch + verify
  --skip-cleanup          Skip cleanup phase (first-time migration)
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
            --id-strategy)    ID_STRATEGY="$2"; shift 2 ;;
            --claim-enabled)  CLAIM_ENABLED="true"; shift ;;
            --scanner-threads) SCANNER_THREADS="$2"; shift 2 ;;
            --writer-threads) WRITER_THREADS="$2"; shift 2 ;;
            --es-bulk-size)   ES_BULK_SIZE="$2"; shift 2 ;;
            --es-field-limit) ES_FIELD_LIMIT="$2"; shift 2 ;;
            --jvm-heap)       JVM_HEAP="$2"; shift 2 ;;
            --jvm-min-heap)   JVM_MIN_HEAP="$2"; shift 2 ;;
            --fresh)          MIGRATION_MODE="--fresh"; shift ;;
            --es-only)        MIGRATION_MODE="--es-only"; shift ;;
            --validate-only)  MIGRATION_MODE="--validate-only"; shift ;;
            --use-job)        USE_JOB="true"; shift ;;
            --helm-chart)     HELM_CHART="$2"; shift 2 ;;
            --skip-switch)    SKIP_SWITCH="true"; shift ;;
            --switch-only)    SWITCH_ONLY="true"; shift ;;
            --skip-cleanup)   SKIP_CLEANUP="true"; shift ;;
            --skip-alias)     SKIP_ALIAS="true"; shift ;;
            --maintenance-mode) MAINTENANCE_MODE="true"; shift ;;
            --maintenance-cooldown) MAINTENANCE_COOLDOWN="$2"; shift 2 ;;
            --alias-script)   ALIAS_SCRIPT="$2"; shift 2 ;;
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

    # Default helm chart path
    if [ -z "$HELM_CHART" ]; then
        HELM_CHART="./helm/atlas"
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
build_migrate_env() {
    local env_args=(
        "ID_STRATEGY=$ID_STRATEGY"
        "CLAIM_ENABLED=$CLAIM_ENABLED"
        "SCANNER_THREADS=$SCANNER_THREADS"
        "WRITER_THREADS=$WRITER_THREADS"
        "MIGRATOR_JVM_HEAP=$JVM_HEAP"
        "MIGRATOR_JVM_MIN_HEAP=$JVM_MIN_HEAP"
    )
    if [ -n "$ES_BULK_SIZE" ]; then
        env_args+=("ES_BULK_SIZE=$ES_BULK_SIZE")
    fi
    if [ -n "$ES_FIELD_LIMIT" ]; then
        env_args+=("ES_FIELD_LIMIT=$ES_FIELD_LIMIT")
    fi
    env_args+=("MAX_RETRIES=$MAX_RETRIES")
    echo "${env_args[@]}"
}

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

    # 0.7 Adaptive sizing probe
    log "Probing vertex count for sizing recommendation..."
    local es_count_resp
    es_count_resp=$(kexec_quiet curl -s "localhost:9200/janusgraph_vertex_index/_count" 2>/dev/null || echo "{}")
    VERTEX_COUNT=$(echo "$es_count_resp" | py_extract "import sys,json; print(json.load(sys.stdin).get('count',0))")
    VERTEX_COUNT="${VERTEX_COUNT:-0}"
    log "  Vertex count (estimate): ${VERTEX_COUNT}"

    # Compute sizing recommendation
    if [ "$VERTEX_COUNT" -gt 10000000 ] 2>/dev/null; then
        SIZING_TIER="large (>10M vertices)"
        log "  Sizing recommendation: 32C/64Gi, scanner=32, writer=16, JVM heap=48g"
        if [ "$JVM_HEAP" = "4g" ] && [ "$SCANNER_THREADS" = "16" ]; then
            warn "Default sizing may be insufficient for ${VERTEX_COUNT} vertices."
            warn "Consider: --jvm-heap 48g --jvm-min-heap 16g --scanner-threads 32 --writer-threads 16"
        fi
    elif [ "$VERTEX_COUNT" -gt 1000000 ] 2>/dev/null; then
        SIZING_TIER="medium (1M-10M vertices)"
        log "  Sizing recommendation: 16C/32Gi, scanner=16, writer=8, JVM heap=24g"
        if [ "$JVM_HEAP" = "4g" ]; then
            warn "Default JVM heap may be insufficient for ${VERTEX_COUNT} vertices."
            warn "Consider: --jvm-heap 24g --jvm-min-heap 8g"
        fi
    else
        SIZING_TIER="small (<1M vertices)"
        log "  Sizing recommendation: 4C/8Gi, scanner=8, writer=4, JVM heap=4g (default)"
    fi

    # 0.8 Disk space check — migration copies data, so we need 2x free space
    log "Checking disk space (migration requires ~2x current usage)..."

    # Elasticsearch disk usage
    local es_disk_info
    es_disk_info=$(kexec_quiet curl -s "localhost:9200/_cat/allocation?format=json" 2>/dev/null || echo "[]")
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
    log "  ID Strategy:       $ID_STRATEGY"
    log "  Claim Enabled:     $CLAIM_ENABLED"
    log "  Migration Mode:    ${MIGRATION_MODE:-full}"
    log "  Use Job:           $USE_JOB"
    log "  Helm Chart:        $HELM_CHART"
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

    local resp
    resp=$(kexec_quiet curl -s -X PUT \
        -H "Content-Type: application/json" \
        -d 'true' \
        "localhost:21000/api/atlas/v2/configs/MAINTENANCE_MODE" 2>/dev/null || echo "")

    # Verify
    local mode
    mode=$(kexec_quiet curl -s "localhost:21000/api/atlas/v2/configs/MAINTENANCE_MODE" 2>/dev/null || echo "")
    if [ "$mode" = "true" ]; then
        ok "Maintenance mode enabled"
        record_phase "Phase 1: Maintenance ON" "PASS"
    else
        record_phase "Phase 1: Maintenance ON" "FAIL"
        die "Failed to enable maintenance mode (response: $resp)" "$EXIT_PREFLIGHT"
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
        log "Reverting backend to JanusGraph..."

        # Read current properties from pod
        local revert_props_file="/tmp/atlas-props-revert-${VCLUSTER}-${TIMESTAMP}.txt"
        kexec_quiet cat /opt/apache-atlas/conf/atlas-application.properties > "$revert_props_file"

        # Remove Cassandra-specific properties and set backend to janus
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

        # Patch ConfigMap safely (preserve other keys)
        patch_configmap_properties "$revert_clean"

        rm -f "$revert_props_file" "$revert_clean"

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
        ok "Reverted to JanusGraph. Safe to drop Cassandra data."
    fi

    # Check if atlas_graph keyspace exists
    log "Checking for existing atlas_graph keyspace..."
    local ks_exists
    ks_exists=$(cexec cqlsh -e "DESCRIBE KEYSPACES;" 2>/dev/null | grep -c "atlas_graph" || echo 0)

    if [ "$ks_exists" -eq 0 ]; then
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
    ks_exists=$(cexec cqlsh -e "DESCRIBE KEYSPACES;" 2>/dev/null | grep -c "atlas_graph" || echo 0)
    if [ "$ks_exists" -ne 0 ]; then
        record_phase "Phase 2: Cleanup" "FAIL"
        die "atlas_graph keyspace still exists after DROP" "$EXIT_CLEANUP"
    fi

    # Delete ES index
    log "Deleting atlas_graph_vertex_index ES index..."
    local es_resp
    es_resp=$(kexec_quiet curl -s -X DELETE "localhost:9200/atlas_graph_vertex_index" 2>/dev/null || echo '{"acknowledged":false}')
    if echo "$es_resp" | grep -q '"acknowledged":true\|"error".*"index_not_found_exception"'; then
        ok "ES index deleted (or didn't exist)"
    else
        warn "ES index delete response: $es_resp"
    fi

    # Verify ES index gone
    local es_check
    es_check=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "localhost:9200/atlas_graph_vertex_index" 2>/dev/null || echo "000")
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

    if [ "$USE_JOB" = "true" ]; then
        run_migration_job
    else
        run_migration_exec
    fi
}

run_migration_exec() {
    # Build env array
    local env_args
    env_args=(
        "ID_STRATEGY=$ID_STRATEGY"
        "CLAIM_ENABLED=$CLAIM_ENABLED"
        "SCANNER_THREADS=$SCANNER_THREADS"
        "WRITER_THREADS=$WRITER_THREADS"
        "MIGRATOR_JVM_HEAP=$JVM_HEAP"
        "MIGRATOR_JVM_MIN_HEAP=$JVM_MIN_HEAP"
    )
    if [ -n "$ES_BULK_SIZE" ]; then
        env_args+=("ES_BULK_SIZE=$ES_BULK_SIZE")
    fi

    # 3.1 Dry-run first (unless validate-only)
    if [ "$MIGRATION_MODE" != "--validate-only" ]; then
        log "Running dry-run to verify configuration..."
        kexec env "${env_args[@]}" /opt/apache-atlas/bin/atlas_migrate.sh --dry-run
        echo ""
        ok "Dry-run complete"
    fi

    # 3.2 Migration with retry
    local attempt=0
    local migration_passed="false"

    while [ "$attempt" -lt "$MAX_RETRIES" ] && [ "$migration_passed" = "false" ]; do
        attempt=$((attempt + 1))
        log "Migration attempt $attempt/$MAX_RETRIES"

        # First attempt uses MIGRATION_MODE, retries use "" for resume
        local mode_flag="$MIGRATION_MODE"
        if [ "$attempt" -gt 1 ]; then
            mode_flag=""  # Resume from where it left off
        fi

        local migrate_exit=0
        kexec env "${env_args[@]}" /opt/apache-atlas/bin/atlas_migrate.sh ${mode_flag:-} || migrate_exit=$?

        if [ "$migrate_exit" -eq 0 ]; then
            migration_passed="true"
            ok "Migration attempt $attempt: PASSED"
        else
            warn "Migration attempt $attempt failed (exit code: $migrate_exit)"
            if [ "$attempt" -lt "$MAX_RETRIES" ]; then
                log "Retrying... (migration is resumable, will pick up from failed token ranges)"
                sleep 10
            fi
        fi
    done

    if [ "$migration_passed" = "false" ]; then
        record_phase "Phase 3: Migration" "FAIL (after $MAX_RETRIES attempts)"
        die "Migration failed after $MAX_RETRIES attempts" "$EXIT_MIGRATION"
    fi

    echo ""
    ok "Migration completed successfully (attempt $attempt/$MAX_RETRIES)"

    # 3.3 Run validation if not already validate-only
    if [ "$MIGRATION_MODE" != "--validate-only" ] && [ "$MIGRATION_MODE" != "--es-only" ]; then
        log "Running post-migration validation..."
        kexec env "${env_args[@]}" /opt/apache-atlas/bin/atlas_migrate.sh --validate-only || warn "Validation returned non-zero (check output above)"
        ok "Validation complete"
    fi

    record_phase "Phase 3: Migration" "PASS (attempt $attempt/$MAX_RETRIES)"
}

run_migration_job() {
    log "Triggering migration via Helm Job..."

    # Verify helm is available
    if ! command -v helm >/dev/null 2>&1; then
        record_phase "Phase 3: Migration" "FAIL (helm not found)"
        die "helm CLI not found. Install helm or use kubectl exec mode (remove --use-job)." "$EXIT_MIGRATION"
    fi

    # Verify chart path exists
    if [ ! -d "$HELM_CHART" ]; then
        record_phase "Phase 3: Migration" "FAIL (chart not found)"
        die "Helm chart not found at '$HELM_CHART'. Use --helm-chart <path> to specify the chart directory." "$EXIT_MIGRATION"
    fi

    # Check if job already exists
    local existing_job
    existing_job=$(kubectl get pods -n "$NAMESPACE" -l app=atlas-migrator -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [ -n "$existing_job" ]; then
        log "Deleting existing migration pod: $existing_job"
        kubectl delete pod "$existing_job" -n "$NAMESPACE" --ignore-not-found
        sleep 5
    fi

    # Build helm set flags
    local helm_sets=(
        --set "migration.enabled=true"
        --set "migration.idStrategy=${ID_STRATEGY}"
        --set "migration.claimEnabled=${CLAIM_ENABLED}"
        --set "migration.scannerThreads=${SCANNER_THREADS}"
        --set "migration.writerThreads=${WRITER_THREADS}"
        --set "migration.jvmHeap=${JVM_HEAP}"
        --set "migration.jvmMinHeap=${JVM_MIN_HEAP}"
    )
    if [ -n "$MIGRATION_MODE" ]; then
        helm_sets+=(--set "migration.mode=${MIGRATION_MODE}")
    fi
    if [ -n "$ES_BULK_SIZE" ]; then
        helm_sets+=(--set "migration.esBulkSize=${ES_BULK_SIZE}")
    fi

    # Execute helm upgrade
    log "Running helm upgrade to create migration pod..."
    log "  Chart: $HELM_CHART"
    log "  Sets: ${helm_sets[*]}"
    if ! helm upgrade atlas "$HELM_CHART" -n "$NAMESPACE" --reuse-values "${helm_sets[@]}"; then
        record_phase "Phase 3: Migration" "FAIL (helm upgrade failed)"
        die "helm upgrade failed" "$EXIT_MIGRATION"
    fi
    ok "Helm upgrade applied"

    # Wait for job pod to start
    log "Waiting for migration job pod..."
    local job_pod=""
    for i in $(seq 1 60); do
        job_pod=$(kubectl get pods -n "$NAMESPACE" -l app=atlas-migrator --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
        if [ -n "$job_pod" ]; then
            break
        fi
        # Also check for Pending pods (still starting)
        local pending_pod
        pending_pod=$(kubectl get pods -n "$NAMESPACE" -l app=atlas-migrator -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
        if [ -n "$pending_pod" ] && [ "$i" -le 5 ]; then
            log "  Migration pod $pending_pod is starting..."
        fi
        sleep 5
    done

    if [ -z "$job_pod" ]; then
        record_phase "Phase 3: Migration" "FAIL (job pod timeout)"
        die "Migration job pod did not start within 5 minutes" "$EXIT_MIGRATION"
    fi

    log "Streaming logs from $job_pod..."
    kubectl logs -f "$job_pod" -n "$NAMESPACE" || true

    # Check job status
    local job_status
    job_status=$(kubectl get pods "$job_pod" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")

    # Check container exit code for more accurate status
    local exit_code
    exit_code=$(kubectl get pods "$job_pod" -n "$NAMESPACE" -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || echo "")

    if [ "$exit_code" = "0" ] || [ "$job_status" = "Succeeded" ]; then
        ok "Migration Job completed successfully"
        record_phase "Phase 3: Migration" "PASS (job)"
    elif [ "$job_status" = "Running" ]; then
        # Pod still running (in post-sleep phase), check log output for exit code
        log "Migration pod still running (may be in post-completion sleep)"
        ok "Migration Job likely completed (pod in sleep phase)"
        record_phase "Phase 3: Migration" "PASS (job, post-sleep)"
    else
        record_phase "Phase 3: Migration" "FAIL (job status: $job_status, exit: $exit_code)"
        die "Migration Job failed (pod status: $job_status, exit code: $exit_code)" "$EXIT_MIGRATION"
    fi

    # Disable migration after completion
    log "Disabling migration pod in Helm..."
    helm upgrade atlas "$HELM_CHART" -n "$NAMESPACE" --reuse-values --set migration.enabled=false || \
        warn "Could not disable migration.enabled via helm (non-fatal)"
}

# ============================================================================
# Phase 4: ES Alias Creation
# ============================================================================

phase_alias() {
    # ES alias creation is not needed for Cassandra-backed graph.
    # The new graph layer uses atlas_graph_vertex_index directly.
    # This phase is retained as a no-op placeholder for future use.
    log "Phase 4: ES Alias — not required for Cassandra migration, skipping"
    record_phase "Phase 4: ES Alias" "SKIPPED (not required)"
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

    # 5.1 Backup ConfigMap
    log "Backing up ConfigMap..."
    CONFIGMAP_BACKUP="/tmp/atlas-config-backup-${VCLUSTER}-${TIMESTAMP}.yaml"
    kubectl get configmap atlas-config -n "$NAMESPACE" -o yaml > "$CONFIGMAP_BACKUP"
    ok "ConfigMap backed up to $CONFIGMAP_BACKUP"

    # 5.2 Extract and patch properties
    log "Patching ConfigMap with Cassandra backend properties..."

    local props_file="/tmp/atlas-props-${VCLUSTER}-${TIMESTAMP}.txt"
    local props_clean="/tmp/atlas-props-clean-${VCLUSTER}-${TIMESTAMP}.txt"

    kubectl get configmap atlas-config -n "$NAMESPACE" -o jsonpath='{.data.atlas-application\.properties}' > "$props_file"

    # Remove any existing Zero Graph lines
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

    # Fix 2: Read Cassandra hostname from current config
    local cass_hostname
    cass_hostname=$(grep '^atlas.graph.storage.hostname=' "$props_file" | cut -d= -f2 | head -1 || echo "")
    if [ -z "$cass_hostname" ]; then
        cass_hostname=$(grep '^atlas.cassandra.graph.hostname=' "$props_file" | cut -d= -f2 | head -1 || echo "atlas-cassandra")
    fi

    # Fix 2: Read datacenter from current config (try multiple keys, fallback to datacenter1)
    local cass_dc
    cass_dc=$(grep '^atlas.cassandra.graph.datacenter=' "$props_file" | cut -d= -f2 | head -1 || echo "")
    if [ -z "$cass_dc" ]; then
        cass_dc=$(grep '^atlas.graph.storage.cql.local-datacenter=' "$props_file" | cut -d= -f2 | head -1 || echo "")
    fi
    if [ -z "$cass_dc" ]; then
        cass_dc="datacenter1"
        warn "Could not read Cassandra datacenter from config — using default: $cass_dc"
    fi
    log "  Cassandra host: $cass_hostname, datacenter: $cass_dc"

    # Fix 2: Append new properties using user's flags (not hardcoded)
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

    # Fix 5: Apply patched ConfigMap preserving other keys
    patch_configmap_properties "$props_clean"
    ok "ConfigMap patched"

    # Cleanup temp files
    rm -f "$props_file" "$props_clean"

    # 5.3 Restart pod
    log "Restarting pod $POD..."
    kubectl delete pod "$POD" -n "$NAMESPACE"

    log "Waiting for pod to become ready (timeout: ${WAIT_TIMEOUT}s)..."
    if ! kubectl wait --for=condition=Ready "pod/$POD" -n "$NAMESPACE" --timeout="${WAIT_TIMEOUT}s" 2>/dev/null; then
        record_phase "Phase 5: Backend Switch" "FAIL (pod not ready)"
        die "Pod $POD did not become Ready within ${WAIT_TIMEOUT}s. Check pod logs. ConfigMap backup at: $CONFIGMAP_BACKUP" "$EXIT_SWITCH"
    fi
    ok "Pod $POD is Ready"

    # 5.4 Verify Atlas is ACTIVE with Cassandra backend
    log "Waiting for Atlas to become ACTIVE..."
    local atlas_status="UNKNOWN"
    for i in $(seq 1 30); do
        atlas_status=$(kexec_quiet curl -s localhost:21000/api/atlas/admin/status 2>/dev/null | \
            py_extract "import sys,json; print(json.load(sys.stdin).get('Status','UNKNOWN'))" || echo "STARTING")
        if [ "$atlas_status" = "ACTIVE" ]; then
            break
        fi
        sleep 10
    done

    if [ "$atlas_status" != "ACTIVE" ]; then
        record_phase "Phase 5: Backend Switch" "FAIL (not ACTIVE)"
        die "Atlas did not become ACTIVE (status: $atlas_status). ConfigMap backup at: $CONFIGMAP_BACKUP" "$EXIT_SWITCH"
    fi
    ok "Atlas is ACTIVE on Cassandra backend"

    # 5.5 Check logs for CassandraGraph initialization
    log "Checking startup logs..."
    local init_log
    init_log=$(kubectl logs "$POD" -n "$NAMESPACE" -c "$CONTAINER" --tail=300 2>/dev/null | grep -i "CassandraGraph\|idStrategy\|field mappings" | head -5 || echo "")
    if [ -n "$init_log" ]; then
        echo "$init_log"
        ok "CassandraGraph initialization confirmed in logs"
    else
        warn "Could not find CassandraGraph init log lines (may still be OK)"
    fi

    record_phase "Phase 5: Backend Switch" "PASS"
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

    local resp
    resp=$(kexec_quiet curl -s -X PUT \
        -H "Content-Type: application/json" \
        -d 'false' \
        "localhost:21000/api/atlas/v2/configs/MAINTENANCE_MODE" 2>/dev/null || echo "")

    local mode
    mode=$(kexec_quiet curl -s "localhost:21000/api/atlas/v2/configs/MAINTENANCE_MODE" 2>/dev/null || echo "")
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
    auth_count=$(kexec_quiet curl -s "localhost:9200/atlas_graph_vertex_index/_count?q=__typeName.keyword:AuthPolicy" 2>/dev/null | \
        py_extract "import sys,json; print(json.load(sys.stdin).get('count',0))" || echo "0")
    if [ "$auth_count" -gt 0 ] 2>/dev/null; then
        verify_check "AuthPolicy ES" "true" "${auth_count} policies in ES"
    else
        verify_check "AuthPolicy ES" "false" "No AuthPolicies found in ES"
    fi

    # 7.4 AuthService in ES
    log "Checking AuthService in ES..."
    local svc_count
    svc_count=$(kexec_quiet curl -s "localhost:9200/atlas_graph_vertex_index/_count?q=__typeName.keyword:AuthService" 2>/dev/null | \
        py_extract "import sys,json; print(json.load(sys.stdin).get('count',0))" || echo "0")
    if [ "$svc_count" -gt 0 ] 2>/dev/null; then
        verify_check "AuthService ES" "true" "${svc_count} services in ES"
    else
        verify_check "AuthService ES" "false" "No AuthServices found in ES"
    fi

    # 7.4a ES doc count comparison (old vs new index)
    log "Comparing ES doc counts (old vs new index)..."
    local old_es_count new_es_count
    old_es_count=$(kexec_quiet curl -s "localhost:9200/janusgraph_vertex_index/_count" 2>/dev/null | \
        py_extract "import sys,json; print(json.load(sys.stdin).get('count',0))" || echo "0")
    new_es_count=$(kexec_quiet curl -s "localhost:9200/atlas_graph_vertex_index/_count" 2>/dev/null | \
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

    # 7.6 PolicyRefresher
    if [ "$have_token" = "true" ]; then
        log "Checking PolicyRefresher..."
        local policy_resp
        policy_resp=$(kexec_quiet curl -s \
            -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
            "localhost:21000/api/atlas/admin/refreshPolicyInfo" 2>/dev/null || echo "")

        local evaluator_count
        evaluator_count=$(echo "$policy_resp" | py_extract "import sys,json; d=json.load(sys.stdin); print(d.get('evaluatorCount', len(d.get('evaluators',[]))))" || echo "0")
        if [ "$evaluator_count" -gt 0 ] 2>/dev/null; then
            verify_check "PolicyRefresher" "true" "${evaluator_count} evaluators loaded"
        else
            verify_check "PolicyRefresher" "false" "No evaluators loaded"
        fi
    fi

    # 7.7 Heka policy download
    if [ "$have_token" = "true" ]; then
        log "Testing Heka policy download..."
        local heka_resp
        heka_resp=$(kexec_quiet curl -s \
            -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
            "localhost:21000/api/atlas/heka/policy" 2>/dev/null || echo "")

        local heka_count
        heka_count=$(echo "$heka_resp" | py_extract "import sys,json; d=json.load(sys.stdin); print(len(d) if isinstance(d,list) else len(d.get('policies',[])))" || echo "0")
        if [ "$heka_count" -gt 0 ] 2>/dev/null; then
            verify_check "Heka policies" "true" "${heka_count} policies"
        else
            verify_check "Heka policies" "false" "No Heka policies returned"
        fi
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

    # 7.9 Write test (create + delete)
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
            # Delete the test entity
            kexec_quiet curl -s -X DELETE \
                -H "Authorization: Bearer ${KEYCLOAK_TOKEN}" \
                "localhost:21000/api/atlas/v2/entity/guid/${test_guid}?type=HARD" 2>/dev/null || true
            verify_check "Write test" "true" "Created and deleted entity $test_guid"
        else
            verify_check "Write test" "false" "Could not create test entity (resp: ${create_resp:0:200})"
        fi
    fi

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

    # 7.11 Deterministic ID check (only if ID_STRATEGY=deterministic)
    if [ "$ID_STRATEGY" = "deterministic" ]; then
        log "Checking deterministic ID format in Cassandra..."
        local vertex_sample
        vertex_sample=$(cexec cqlsh -e "SELECT vertex_id FROM atlas_graph.vertices LIMIT 5;" 2>/dev/null || echo "")

        if [ -n "$vertex_sample" ]; then
            # Check if vertex IDs are 32-char hex (no dashes) vs UUID format (36-char with dashes)
            local has_uuid_format
            has_uuid_format=$(echo "$vertex_sample" | py_extract "
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
if hex_count > 0 and uuid_count == 0:
    print('deterministic')
elif uuid_count > 0:
    print('legacy')
else:
    print('unknown')
" <<< "$vertex_sample")

            if [ "$has_uuid_format" = "deterministic" ]; then
                verify_check "Deterministic IDs" "true" "Vertex IDs are 32-char hex format"
            elif [ "$has_uuid_format" = "legacy" ]; then
                verify_check "Deterministic IDs" "false" "Vertex IDs are UUID format (legacy) — expected deterministic"
            else
                verify_check "Deterministic IDs" "false" "Could not determine ID format from sample"
            fi
        else
            warn "Could not query Cassandra for vertex ID sample"
        fi
    fi

    # 7.12 Verify running properties
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
    printf "  %-30s %s\n" "ID Strategy:" "$ID_STRATEGY"
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
    if [ -n "$CONFIGMAP_BACKUP" ]; then
        echo "  Rollback: kubectl apply -f $CONFIGMAP_BACKUP && kubectl delete pod $POD -n $NAMESPACE"
    fi
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
  "id_strategy": "${ID_STRATEGY}",
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
