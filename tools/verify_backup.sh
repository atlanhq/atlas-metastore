#!/bin/bash
# =============================================================================
# Backup Verification for Zero Graph Migration
#
# Queries Prometheus/Victoria Metrics (via Grafana API) to verify that recent
# successful backups exist for a tenant before allowing migration to proceed.
#
# Checks:
#   1. Cassandra backup completed in the last N hours (default: 24)
#   2. Elasticsearch backup completed in the last N hours (default: 24)
#   3. No backup failures in the same window (warning, not blocking)
#
# Usage:
#   ./verify_backup.sh --tenant <name>
#   ./verify_backup.sh --tenant <name> --recency 12
#   ./verify_backup.sh --tenant <name> --skip-backup-check   # dev/test override
#
# Exit codes:
#   0  All backup checks passed
#   1  One or more backup checks failed
#   2  Configuration / connectivity error
#
# Environment variables:
#   GRAFANA_URL       Grafana base URL (default: https://observability.atlan.com)
#   GRAFANA_API_KEY   Grafana service account token (required unless --skip-backup-check)
#   VM_DATASOURCE_UID Victoria Metrics datasource UID in Grafana
#   BACKUP_RECENCY_HOURS  Max age of backup in hours (default: 24, overridden by --recency)
# =============================================================================

set -euo pipefail

# ---- Defaults ----

GRAFANA_URL="${GRAFANA_URL:-https://observability.atlan.com}"
GRAFANA_API_KEY="${GRAFANA_API_KEY:-}"
VM_DATASOURCE_UID="${VM_DATASOURCE_UID:-cd27b984-0b24-4adf-8bf8-1711c6b21061}"
BACKUP_RECENCY_HOURS="${BACKUP_RECENCY_HOURS:-24}"

TENANT=""
SKIP_CHECK=false
QUIET=false

# ---- Helpers ----

log()  { $QUIET || echo "[backup-check] $*"; }
warn() { echo "[backup-check] WARN: $*"; }
err()  { echo "[backup-check] ERROR: $*"; exit 2; }

# ---- Parse args ----

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tenant)
            TENANT="$2"; shift 2 ;;
        --recency)
            BACKUP_RECENCY_HOURS="$2"; shift 2 ;;
        --skip-backup-check)
            SKIP_CHECK=true; shift ;;
        --quiet|-q)
            QUIET=true; shift ;;
        --grafana-url)
            GRAFANA_URL="$2"; shift 2 ;;
        --grafana-api-key)
            GRAFANA_API_KEY="$2"; shift 2 ;;
        --help|-h)
            head -30 "$0" | grep '^#' | sed 's/^# \?//'
            exit 0 ;;
        *)
            err "Unknown option: $1 (use --help)" ;;
    esac
done

# ---- Validate ----

if [ -z "$TENANT" ]; then
    err "Missing required --tenant <name>"
fi

if $SKIP_CHECK; then
    warn "╔══════════════════════════════════════════════════════════════╗"
    warn "║  BACKUP CHECK SKIPPED (--skip-backup-check)                ║"
    warn "║  You are proceeding WITHOUT verified backups.              ║"
    warn "║  This is acceptable only for dev/test environments.        ║"
    warn "╚══════════════════════════════════════════════════════════════╝"
    exit 0
fi

if [ -z "$GRAFANA_API_KEY" ]; then
    err "GRAFANA_API_KEY is required. Set it as an environment variable or use --skip-backup-check for dev/test."
fi

if ! command -v curl >/dev/null 2>&1; then
    err "curl is required but not found in PATH"
fi

if ! command -v python3 >/dev/null 2>&1; then
    err "python3 is required but not found in PATH (used for JSON parsing)"
fi

# ---- Query Victoria Metrics via Grafana proxy ----

# Queries Grafana's datasource proxy endpoint for Victoria Metrics.
# Args: $1 = PromQL query
# Returns: JSON result array (or exits on error)
vm_query() {
    local query="$1"
    local response
    response=$(curl -s --max-time 30 -G \
        -H "Authorization: Bearer ${GRAFANA_API_KEY}" \
        "${GRAFANA_URL}/api/datasources/proxy/uid/${VM_DATASOURCE_UID}/api/v1/query" \
        --data-urlencode "query=${query}" 2>&1)

    local status
    status=$(echo "$response" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")

    if [ "$status" != "success" ]; then
        warn "Query failed: $query"
        warn "Response: $(echo "$response" | head -c 500)"
        echo "[]"
        return 1
    fi

    echo "$response" | python3 -c "
import json, sys
data = json.load(sys.stdin)
results = data.get('data', {}).get('result', [])
print(json.dumps(results))
" 2>/dev/null || echo "[]"
}

# Extract a single numeric value from a VM instant query result.
# Returns "0" if no result.
vm_scalar() {
    local query="$1"
    local result
    result=$(vm_query "$query") || { echo "0"; return; }

    echo "$result" | python3 -c "
import json, sys
results = json.loads(sys.stdin.read())
if results:
    print(results[0].get('value', [0, '0'])[1])
else:
    print('0')
" 2>/dev/null || echo "0"
}

# ---- Check connectivity ----

log "Verifying Grafana API connectivity..."
http_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 \
    -H "Authorization: Bearer ${GRAFANA_API_KEY}" \
    "${GRAFANA_URL}/api/health" 2>/dev/null || echo "000")

if [ "$http_code" != "200" ]; then
    err "Cannot reach Grafana at ${GRAFANA_URL} (HTTP ${http_code}). Check GRAFANA_URL and network."
fi
log "Grafana reachable."

# ---- Run checks ----

WINDOW="${BACKUP_RECENCY_HOURS}h"
PASS=true

log ""
log "╔══════════════════════════════════════════════════════════════╗"
log "║  Backup Verification for: ${TENANT}"
log "║  Recency window: ${BACKUP_RECENCY_HOURS} hours"
log "╚══════════════════════════════════════════════════════════════╝"
log ""

# --- 1. Cassandra backup completions ---
cassandra_completions=$(vm_scalar "sum(changes(temporal_workflow_completed_total{workflow_type=\"CassandraBackup\", clusterName=\"${TENANT}\"}[${WINDOW}]))")
cassandra_completions_int=$(printf "%.0f" "$cassandra_completions" 2>/dev/null || echo "0")

if [ "$cassandra_completions_int" -gt 0 ] 2>/dev/null; then
    log "  ✓ Cassandra backup:       ${cassandra_completions_int} completion(s) in last ${WINDOW}"
else
    warn "  ✗ Cassandra backup:       NO completions in last ${WINDOW}"
    PASS=false
fi

# --- 2. Elasticsearch backup completions ---
es_completions=$(vm_scalar "sum(changes(temporal_workflow_completed_total{workflow_type=\"ElasticsearchBackup\", clusterName=\"${TENANT}\"}[${WINDOW}]))")
es_completions_int=$(printf "%.0f" "$es_completions" 2>/dev/null || echo "0")

if [ "$es_completions_int" -gt 0 ] 2>/dev/null; then
    log "  ✓ Elasticsearch backup:   ${es_completions_int} completion(s) in last ${WINDOW}"
else
    warn "  ✗ Elasticsearch backup:   NO completions in last ${WINDOW}"
    PASS=false
fi

# --- 3. Check for failures (warning, non-blocking) ---
cassandra_failures=$(vm_scalar "sum(changes(temporal_workflow_failed_total{workflow_type=\"CassandraBackup\", clusterName=\"${TENANT}\"}[${WINDOW}]))")
cassandra_failures_int=$(printf "%.0f" "$cassandra_failures" 2>/dev/null || echo "0")

es_failures=$(vm_scalar "sum(changes(temporal_workflow_failed_total{workflow_type=\"ElasticsearchBackup\", clusterName=\"${TENANT}\"}[${WINDOW}]))")
es_failures_int=$(printf "%.0f" "$es_failures" 2>/dev/null || echo "0")

if [ "$cassandra_failures_int" -gt 0 ] 2>/dev/null; then
    warn "  ⚠ Cassandra backup:       ${cassandra_failures_int} failure(s) in last ${WINDOW}"
fi
if [ "$es_failures_int" -gt 0 ] 2>/dev/null; then
    warn "  ⚠ Elasticsearch backup:   ${es_failures_int} failure(s) in last ${WINDOW}"
fi

# --- 4. Check last backup timestamp (informational) ---
cassandra_ts=$(vm_scalar "max(timestamp(temporal_workflow_completed_total{workflow_type=\"CassandraBackup\", clusterName=\"${TENANT}\"}))")
es_ts=$(vm_scalar "max(timestamp(temporal_workflow_completed_total{workflow_type=\"ElasticsearchBackup\", clusterName=\"${TENANT}\"}))")

cassandra_age=""
es_age=""
if [ "$cassandra_ts" != "0" ]; then
    cassandra_age=$(python3 -c "
import time
ts = float('${cassandra_ts}')
age_h = (time.time() - ts) / 3600
print(f'{age_h:.1f}h ago')
" 2>/dev/null || echo "unknown")
    log "  ℹ Last Cassandra backup data point: ${cassandra_age}"
fi
if [ "$es_ts" != "0" ]; then
    es_age=$(python3 -c "
import time
ts = float('${es_ts}')
age_h = (time.time() - ts) / 3600
print(f'{age_h:.1f}h ago')
" 2>/dev/null || echo "unknown")
    log "  ℹ Last ES backup data point:        ${es_age}"
fi

# ---- Summary ----

log ""
if $PASS; then
    log "╔══════════════════════════════════════════════════════════════╗"
    log "║  BACKUP VERIFICATION: PASSED                               ║"
    log "║  Both Cassandra and Elasticsearch backups verified.         ║"
    log "╚══════════════════════════════════════════════════════════════╝"
    exit 0
else
    warn "╔══════════════════════════════════════════════════════════════╗"
    warn "║  BACKUP VERIFICATION: FAILED                               ║"
    warn "║  Migration BLOCKED — missing recent backup(s).             ║"
    warn "║                                                            ║"
    warn "║  Options:                                                  ║"
    warn "║    1. Trigger a backup and re-run this check               ║"
    warn "║    2. Increase --recency window (e.g., --recency 48)       ║"
    warn "║    3. Use --skip-backup-check for dev/test ONLY            ║"
    warn "╚══════════════════════════════════════════════════════════════╝"
    exit 1
fi
