#!/bin/bash
# =============================================================================
# Backup Verification for Zero Graph Migration
#
# Queries Temporal schedule API to verify that a recent successful backup
# exists for a tenant before allowing migration to proceed.
#
# Connects to Temporal at temporal-server.atlan.com:443 (TLS, no credentials)
# and checks the schedule: daily-backup-schedule-<tenant>
#
# Usage:
#   ./verify_backup.sh --tenant <name>
#   ./verify_backup.sh --tenant <name> --recency 12
#   ./verify_backup.sh --tenant <name> --skip-backup-check   # dev/test override
#
# Exit codes:
#   0  Backup verification passed
#   1  Backup verification failed
#   2  Configuration / connectivity error
#
# Environment variables:
#   TEMPORAL_ADDRESS   Temporal server address (default: temporal-server.atlan.com:443)
#   TEMPORAL_NAMESPACE Temporal namespace (default: default)
#   BACKUP_RECENCY_HOURS  Max age of backup in hours (default: 24, overridden by --recency)
# =============================================================================

set -euo pipefail

# ---- Find the migrator JAR (contains BackupVerifier) ----

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ATLAS_HOME="${ATLAS_HOME:-/opt/apache-atlas}"
MIGRATOR_JAR=""

find_migrator_jar() {
    local candidates=(
        "${ATLAS_HOME}/tools/atlas-graphdb-migrator.jar"
        "${ATLAS_HOME}/server/webapp/atlas/WEB-INF/lib/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar"
        "${ATLAS_HOME}/libext/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar"
        "/opt/jar-overrides/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar"
    )
    for candidate in "${candidates[@]}"; do
        if [ -f "$candidate" ]; then
            MIGRATOR_JAR="$candidate"
            return
        fi
    done

    # Fallback: search ATLAS_HOME
    local found
    found=$(find "${ATLAS_HOME}" -maxdepth 4 \
        -name "atlas-graphdb-migrator*.jar" -not -name "original-*" 2>/dev/null | head -1)
    if [ -n "$found" ]; then
        MIGRATOR_JAR="$found"
    fi
}

find_migrator_jar

if [ -z "$MIGRATOR_JAR" ]; then
    echo "[backup-check] ERROR: Cannot find atlas-graphdb-migrator JAR. Set ATLAS_HOME or place the JAR in ${ATLAS_HOME}/tools/" >&2
    exit 2
fi

# ---- Forward all arguments to BackupVerifier Java class ----

exec java -cp "$MIGRATOR_JAR" \
    org.apache.atlas.repository.graphdb.migrator.BackupVerifier \
    "$@"
