#!/bin/bash
# =============================================================================
# Atlas JanusGraph → Cassandra Migrator
#
# Packaged at /opt/apache-atlas/bin/atlas_migrate.sh inside the Atlas Docker image.
# Reads Cassandra/ES connection config from atlas-application.properties automatically.
# The migrator fat JAR ships in the WEB-INF/lib/ classpath.
#
# Usage:
#   /opt/apache-atlas/bin/atlas_migrate.sh                  # Full migration
#   /opt/apache-atlas/bin/atlas_migrate.sh --es-only        # ES reindex only
#   /opt/apache-atlas/bin/atlas_migrate.sh --validate-only  # Validate only
#   /opt/apache-atlas/bin/atlas_migrate.sh --fresh          # Clear state, restart
#   /opt/apache-atlas/bin/atlas_migrate.sh --dry-run        # Show config, don't run
#
# From kubectl:
#   kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
#     /opt/apache-atlas/bin/atlas_migrate.sh --dry-run
# =============================================================================

set -euo pipefail

# ---- Configuration ----

ATLAS_HOME="${ATLAS_HOME:-/opt/apache-atlas}"
ATLAS_CONF="${ATLAS_HOME}/conf/atlas-application.properties"
MIGRATOR_JAR=""
MIGRATOR_JAR_IS_OVERRIDE="false"
PROPERTIES_FILE="/tmp/migrator.properties"
LOG_DIR="${ATLAS_HOME}/logs"
LOG_FILE="${LOG_DIR}/migrator-$(date +%Y%m%d-%H%M%S).log"

# Cassandra / ES hostnames — read from atlas-application.properties if available
CASSANDRA_HOST=""
CASSANDRA_PORT=""
CASSANDRA_DC=""
ES_HOST=""
ES_PORT=""
ES_PROTOCOL="http"

# Source JanusGraph keyspace (where edgestore lives)
SOURCE_KEYSPACE="${SOURCE_KEYSPACE:-atlas}"
SOURCE_EDGESTORE_TABLE="${SOURCE_EDGESTORE_TABLE:-edgestore}"

# Target keyspace (new Cassandra graph schema)
TARGET_KEYSPACE="${TARGET_KEYSPACE:-atlas_graph}"

# ES index names
# Source = JanusGraph index (copy mappings from), Target = CassandraGraph index (write docs to)
SOURCE_ES_INDEX="${SOURCE_ES_INDEX:-janusgraph_vertex_index}"
TARGET_ES_INDEX="${TARGET_ES_INDEX:-atlas_graph_vertex_index}"

# Tuning (in-cluster = fast, can be aggressive)
SCANNER_THREADS="${SCANNER_THREADS:-16}"
WRITER_THREADS="${WRITER_THREADS:-16}"
ES_BULK_SIZE="${ES_BULK_SIZE:-5000}"
QUEUE_CAPACITY="${QUEUE_CAPACITY:-20000}"
ES_FIELD_LIMIT="${ES_FIELD_LIMIT:-10000}"
MAX_RETRIES="${MAX_RETRIES:-3}"

# ID strategy and dedup claims
ID_STRATEGY="${ID_STRATEGY:-legacy}"          # "legacy" (UUID), "hash-jg" (SHA-256 of JG ID), or "deterministic" (identity-based SHA-256)
CLAIM_ENABLED="${CLAIM_ENABLED:-false}"       # LWT dedup claims during migration

# Backup verification
SKIP_BACKUP_CHECK="${SKIP_BACKUP_CHECK:-false}"
BACKUP_RECENCY_HOURS="${BACKUP_RECENCY_HOURS:-24}"
TENANT_NAME="${TENANT_NAME:-}"               # Tenant/cluster name for backup verification

# Cassandra consistency levels
SOURCE_CONSISTENCY="${SOURCE_CONSISTENCY:-ONE}"            # Read consistency (ONE = fast scans)
TARGET_CONSISTENCY="${TARGET_CONSISTENCY:-LOCAL_QUORUM}"   # Write consistency (LOCAL_QUORUM = durable)

# Skip flags
SKIP_ES_REINDEX="${SKIP_ES_REINDEX:-false}"
SKIP_CLASSIFICATIONS="${SKIP_CLASSIFICATIONS:-false}"
SKIP_TASKS="${SKIP_TASKS:-false}"

# Validation tenant ID — auto-derive from hostname if not set
VALIDATION_TENANT_ID="${VALIDATION_TENANT_ID:-}"

# ---- Helpers ----

log()  { echo "[migrator] $*"; }
warn() { echo "[migrator] WARN: $*"; }
err()  { echo "[migrator] ERROR: $*"; exit 1; }

# Read a property value from atlas-application.properties
read_prop() {
    local key="$1"
    local default="${2:-}"
    if [ -f "$ATLAS_CONF" ]; then
        local val
        val=$(grep -E "^${key}=" "$ATLAS_CONF" 2>/dev/null | tail -1 | cut -d'=' -f2- | tr -d '[:space:]')
        if [ -n "$val" ]; then
            echo "$val"
            return
        fi
    fi
    echo "$default"
}

# ---- Find migrator JAR ----

find_migrator_jar() {
    local LIB_DIR="${ATLAS_HOME}/server/webapp/atlas/WEB-INF/lib"

    # Check for local JAR override first (uploaded via kubectl cp from --local-jar).
    # Override JAR is a thin JAR (classes only, no shaded deps) — must run via -cp mode
    # with the image's fat JAR providing dependencies.
    local OVERRIDE_JAR="/opt/jar-overrides/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar"
    if [ -f "$OVERRIDE_JAR" ]; then
        log "Found local JAR override at $OVERRIDE_JAR"
        MIGRATOR_JAR="$OVERRIDE_JAR"
        MIGRATOR_JAR_IS_OVERRIDE="true"
        return
    fi

    # Search in standard locations (priority order)
    local candidates=(
        "${ATLAS_HOME}/tools/atlas-graphdb-migrator.jar"
        "${LIB_DIR}/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar"
        "${ATLAS_HOME}/libext/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar"
    )

    for candidate in "${candidates[@]}"; do
        if [ -f "$candidate" ]; then
            MIGRATOR_JAR="$candidate"
            return
        fi
    done

    # Glob search in WEB-INF/lib
    local found
    found=$(find "${ATLAS_HOME}/tools" "$LIB_DIR" "${ATLAS_HOME}/libext" \
            -name "atlas-graphdb-migrator*.jar" -not -name "original-*" 2>/dev/null | head -1)
    if [ -n "$found" ]; then
        MIGRATOR_JAR="$found"
        return
    fi

    err "Migrator JAR not found in ${LIB_DIR}/ or ${ATLAS_HOME}/libext/.
  Ensure the atlas-graphdb-migrator module is included in the build."
}

# ---- Read connection config from atlas-application.properties ----

read_atlas_config() {
    if [ ! -f "$ATLAS_CONF" ]; then
        err "atlas-application.properties not found at $ATLAS_CONF"
    fi

    log "Reading connection config from $ATLAS_CONF"

    # Cassandra — try the new lean-graph config keys first, then fall back to JanusGraph keys
    CASSANDRA_HOST=$(read_prop "atlas.cassandra.graph.hostname" "")
    if [ -z "$CASSANDRA_HOST" ]; then
        # Fall back to JanusGraph storage config
        CASSANDRA_HOST=$(read_prop "atlas.graph.storage.hostname" "atlas-cassandra")
    fi

    CASSANDRA_PORT=$(read_prop "atlas.cassandra.graph.port" "")
    if [ -z "$CASSANDRA_PORT" ]; then
        CASSANDRA_PORT=$(read_prop "atlas.graph.storage.port" "9042")
    fi

    CASSANDRA_DC=$(read_prop "atlas.cassandra.graph.datacenter" "")
    if [ -z "$CASSANDRA_DC" ]; then
        CASSANDRA_DC=$(read_prop "atlas.graph.storage.cql.local-datacenter" "datacenter1")
    fi

    # Elasticsearch
    local es_raw
    es_raw=$(read_prop "atlas.graph.index.search.hostname" "atlas-elasticsearch-master:9200")
    # Could be "host:port" or just "host"
    ES_HOST=$(echo "$es_raw" | cut -d':' -f1)
    local es_port_from_host
    es_port_from_host=$(echo "$es_raw" | grep -o ':[0-9]*' | tr -d ':' || true)
    ES_PORT="${es_port_from_host:-9200}"

    ES_PROTOCOL=$(read_prop "atlas.graph.index.search.elasticsearch.http.protocol" "http")

    # Target keyspace from lean-graph config
    local tgt_ks
    tgt_ks=$(read_prop "atlas.cassandra.graph.keyspace" "")
    if [ -n "$tgt_ks" ]; then
        TARGET_KEYSPACE="$tgt_ks"
    fi

    log "  Cassandra: ${CASSANDRA_HOST}:${CASSANDRA_PORT} (dc=${CASSANDRA_DC})"
    log "  ES:        ${ES_PROTOCOL}://${ES_HOST}:${ES_PORT}"
    log "  Source keyspace: ${SOURCE_KEYSPACE} (table: ${SOURCE_EDGESTORE_TABLE})"
    log "  Target keyspace: ${TARGET_KEYSPACE}"
    log "  Source ES index: ${SOURCE_ES_INDEX} (copy mappings from)"
    log "  Target ES index: ${TARGET_ES_INDEX} (write docs to)"
}

# ---- Generate migrator.properties ----

generate_properties() {
    # Auto-derive tenant ID if not set
    if [ -z "$VALIDATION_TENANT_ID" ]; then
        # Try VCLUSTER_NAME env, then hostname, then fallback
        if [ -n "${VCLUSTER_NAME:-}" ]; then
            VALIDATION_TENANT_ID="$VCLUSTER_NAME"
        else
            VALIDATION_TENANT_ID=$(hostname 2>/dev/null | sed 's/\..*$//' || echo "unknown")
        fi
    fi

    log "Generating $PROPERTIES_FILE"

    cat > "$PROPERTIES_FILE" << EOF
# Auto-generated by atlas_migrate.sh at $(date)
# Source: atlas-application.properties at $ATLAS_CONF

# Source: JanusGraph schema resolution
source.janusgraph.config=${ATLAS_CONF}

# Source Cassandra (JanusGraph edgestore)
source.cassandra.hostname=${CASSANDRA_HOST}
source.cassandra.port=${CASSANDRA_PORT}
source.cassandra.keyspace=${SOURCE_KEYSPACE}
source.cassandra.datacenter=${CASSANDRA_DC}
source.cassandra.username=
source.cassandra.password=
source.cassandra.edgestore.table=${SOURCE_EDGESTORE_TABLE}

# Source Elasticsearch (copy mappings from JanusGraph index)
source.elasticsearch.index=${SOURCE_ES_INDEX}

# Target Cassandra (new schema)
target.cassandra.hostname=${CASSANDRA_HOST}
target.cassandra.port=${CASSANDRA_PORT}
target.cassandra.keyspace=${TARGET_KEYSPACE}
target.cassandra.datacenter=${CASSANDRA_DC}
target.cassandra.username=
target.cassandra.password=

# Target Elasticsearch (write migrated docs here)
target.elasticsearch.hostname=${ES_HOST}
target.elasticsearch.port=${ES_PORT}
target.elasticsearch.protocol=${ES_PROTOCOL}
target.elasticsearch.index=${TARGET_ES_INDEX}
target.elasticsearch.username=
target.elasticsearch.password=
target.elasticsearch.field.limit=${ES_FIELD_LIMIT}

# Tuning
migration.scanner.threads=${SCANNER_THREADS}
migration.writer.threads=${WRITER_THREADS}
migration.es.bulk.size=${ES_BULK_SIZE}
migration.max.retries=${MAX_RETRIES}
migration.scan.fetch.size=5000
migration.queue.capacity=${QUEUE_CAPACITY}
migration.resume=true

# ID strategy / dedup
migration.id.strategy=${ID_STRATEGY}
migration.claim.enabled=${CLAIM_ENABLED}

# Cassandra consistency levels
source.cassandra.consistency=${SOURCE_CONSISTENCY}
target.cassandra.consistency=${TARGET_CONSISTENCY}

# Skip flags
migration.skip.es.reindex=${SKIP_ES_REINDEX}
migration.skip.classifications=${SKIP_CLASSIFICATIONS}
migration.skip.tasks=${SKIP_TASKS}

# Validation
validation.tenant.id=${VALIDATION_TENANT_ID}
EOF

    log "Properties written to $PROPERTIES_FILE"
}

# ---- Preflight checks ----

preflight() {
    log "Running preflight checks..."

    # Check Java
    if ! java -version 2>&1 | head -1; then
        err "Java not found in PATH"
    fi

    # Check Cassandra connectivity
    if command -v nc >/dev/null 2>&1; then
        if nc -z -w3 "$CASSANDRA_HOST" "$CASSANDRA_PORT" 2>/dev/null; then
            log "  Cassandra reachable at ${CASSANDRA_HOST}:${CASSANDRA_PORT}"
        else
            warn "Cannot reach Cassandra at ${CASSANDRA_HOST}:${CASSANDRA_PORT}"
        fi
    fi

    # Check ES connectivity
    if command -v curl >/dev/null 2>&1; then
        local es_status
        es_status=$(curl -s -o /dev/null -w "%{http_code}" "${ES_PROTOCOL}://${ES_HOST}:${ES_PORT}/" 2>/dev/null || echo "000")
        if [ "$es_status" = "200" ]; then
            log "  ES reachable at ${ES_PROTOCOL}://${ES_HOST}:${ES_PORT}"
        else
            warn "ES returned HTTP $es_status at ${ES_PROTOCOL}://${ES_HOST}:${ES_PORT}"
        fi
    fi

    log "Preflight complete"
}

# ---- Backup verification ----

verify_backup() {
    local SCRIPT_DIR
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    local VERIFY_SCRIPT="${SCRIPT_DIR}/verify_backup.sh"

    if [ "$SKIP_BACKUP_CHECK" = "true" ]; then
        warn "Backup verification SKIPPED (SKIP_BACKUP_CHECK=true)"
        return 0
    fi

    # Auto-detect tenant name from atlas-application.properties if not set
    if [ -z "$TENANT_NAME" ]; then
        TENANT_NAME=$(read_prop "atlas.cluster.name" "")
    fi
    if [ -z "$TENANT_NAME" ]; then
        TENANT_NAME=$(read_prop "atlas.tenant.name" "")
    fi
    if [ -z "$TENANT_NAME" ]; then
        err "Cannot determine tenant name. Set TENANT_NAME env var or add atlas.cluster.name to atlas-application.properties.
  To skip: set SKIP_BACKUP_CHECK=true (dev/test only)"
    fi

    if [ ! -f "$VERIFY_SCRIPT" ]; then
        err "Backup verification script not found at ${VERIFY_SCRIPT}"
    fi

    log "Running backup verification for tenant: ${TENANT_NAME}"

    local verify_args=(--tenant "$TENANT_NAME" --recency "$BACKUP_RECENCY_HOURS")
    if ! "$VERIFY_SCRIPT" "${verify_args[@]}"; then
        err "Backup verification FAILED. Migration cannot proceed.
  To override for dev/test: set SKIP_BACKUP_CHECK=true"
    fi

    log "Backup verification passed."
}

# ---- Run migrator ----

run() {
    local extra_args="${1:-}"

    log ""
    log "=========================================="
    log "  JanusGraph -> Cassandra Migrator"
    if [ -n "$extra_args" ]; then
        log "  Mode: $extra_args"
    else
        log "  Mode: full migration"
    fi
    log "  JAR:  $MIGRATOR_JAR"
    log "  Log:  $LOG_FILE"
    log "=========================================="
    log ""

    # Use pipefail + PIPESTATUS to capture the JAR's exit code through tee
    set +e
    if [ "$MIGRATOR_JAR_IS_OVERRIDE" = "true" ]; then
        # Override JAR is a thin JAR (classes only). Run with -cp: override first,
        # then the image's fat JAR for all shaded dependencies.
        local LIB_DIR="${ATLAS_HOME}/server/webapp/atlas/WEB-INF/lib"
        local fat_jar=""
        for f in "${ATLAS_HOME}/tools/atlas-graphdb-migrator.jar" \
                 "${LIB_DIR}/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar" \
                 "${ATLAS_HOME}/libext/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar"; do
            if [ -f "$f" ]; then fat_jar="$f"; break; fi
        done
        if [ -z "$fat_jar" ]; then
            fat_jar=$(find "${ATLAS_HOME}" -name "atlas-graphdb-migrator*.jar" \
                      -not -name "original-*" -not -path "*/jar-overrides/*" 2>/dev/null | head -1)
        fi
        log "Using thin JAR override with classpath: $MIGRATOR_JAR + $fat_jar"
        java \
            -Xmx${MIGRATOR_JVM_HEAP:-4g} -Xms${MIGRATOR_JVM_MIN_HEAP:-2g} \
            --add-opens java.base/java.lang=ALL-UNNAMED \
            -cp "${MIGRATOR_JAR}:${fat_jar}" \
            org.apache.atlas.repository.graphdb.migrator.MigratorMain \
            "$PROPERTIES_FILE" \
            $extra_args \
            2>&1 | tee "$LOG_FILE"
    else
        java \
            -Xmx${MIGRATOR_JVM_HEAP:-4g} -Xms${MIGRATOR_JVM_MIN_HEAP:-2g} \
            --add-opens java.base/java.lang=ALL-UNNAMED \
            -jar "$MIGRATOR_JAR" \
            "$PROPERTIES_FILE" \
            $extra_args \
            2>&1 | tee "$LOG_FILE"
    fi
    local jar_exit=${PIPESTATUS[0]}
    set -e

    log ""
    log "Migration log saved to $LOG_FILE"

    if [ "$jar_exit" -ne 0 ]; then
        err "Migrator exited with code $jar_exit — validation failed or migration encountered errors. Review log at $LOG_FILE"
    fi
}

# ---- Help ----

show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Run from inside an Atlas pod. Reads Cassandra/ES config from atlas-application.properties."
    echo ""
    echo "Options:"
    echo "  (no args)          Full migration: scan + ES reindex + validate"
    echo "  --es-only          Re-index Elasticsearch only"
    echo "  --validate-only    Validate migration completeness"
    echo "  --fresh            Clear resume state, start from scratch"
    echo "  --dry-run          Generate properties and show config without running"
    echo "  --skip-backup-check  Skip backup verification (dev/test only)"
    echo "  --help             Show this help"
    echo ""
    echo "Environment overrides:"
    echo "  SOURCE_KEYSPACE          Source JanusGraph keyspace (default: atlas)"
    echo "  SOURCE_EDGESTORE_TABLE   Edgestore table name (default: edgestore)"
    echo "  TARGET_KEYSPACE          Target keyspace (default: atlas_graph)"
    echo "  SOURCE_ES_INDEX          Source ES index to copy mappings from (default: janusgraph_vertex_index)"
    echo "  TARGET_ES_INDEX          Target ES index to write docs to (default: atlas_graph_vertex_index)"
    echo "  SCANNER_THREADS          Scanner parallelism (default: 16, auto-sized by Java)"
    echo "  WRITER_THREADS           Writer parallelism (default: 16, auto-sized by Java)"
    echo "  ID_STRATEGY              ID generation: legacy, hash-jg, or deterministic (default: legacy)"
    echo "  CLAIM_ENABLED            LWT dedup claims during migration: true/false (default: false)"
    echo "  SKIP_ES_REINDEX          Skip ES reindexing phase: true/false (default: false)"
    echo "  SKIP_CLASSIFICATIONS     Skip classification vertices: true/false (default: false)"
    echo "  SKIP_TASKS               Skip task vertices: true/false (default: false)"
    echo "  VALIDATION_TENANT_ID     Tenant ID for validation report (default: auto-derived from hostname)"
    echo "  TENANT_NAME              Tenant/cluster name for backup check (auto-detected from config)"
    echo "  SKIP_BACKUP_CHECK        Skip backup verification: true/false (default: false)"
    echo "  BACKUP_RECENCY_HOURS     Max backup age in hours (default: 24)"
    echo "  TEMPORAL_ADDRESS         Temporal server address (default: temporal-server.atlan.com:443)"
    echo "  TEMPORAL_NAMESPACE       Temporal namespace (default: default)"
    echo "  MIGRATOR_JVM_HEAP        JVM max heap (default: 4g)"
    echo "  MIGRATOR_JVM_MIN_HEAP    JVM initial heap (default: 2g)"
    echo ""
    echo "Quick start (from kubectl):"
    echo "  kubectl exec -it atlas-0 -n atlas -c atlas-main -- /opt/apache-atlas/bin/atlas_migrate.sh --dry-run"
    echo "  kubectl exec -it atlas-0 -n atlas -c atlas-main -- /opt/apache-atlas/bin/atlas_migrate.sh"
}

# ---- Main ----

case "${1:-}" in
    --help|-h)
        show_help
        exit 0
        ;;
    --skip-backup-check)
        SKIP_BACKUP_CHECK=true
        shift
        # Fall through to run with remaining args
        find_migrator_jar
        read_atlas_config
        generate_properties
        preflight
        verify_backup
        run "${1:-}"
        ;;
    --dry-run)
        find_migrator_jar
        read_atlas_config
        generate_properties
        preflight
        verify_backup
        log ""
        log "Dry run complete. Properties at $PROPERTIES_FILE"
        log "To run: java -Xmx${MIGRATOR_JVM_HEAP:-4g} -jar $MIGRATOR_JAR $PROPERTIES_FILE"
        ;;
    --es-only|--validate-only|--fresh|"")
        find_migrator_jar
        read_atlas_config
        generate_properties
        preflight
        verify_backup
        run "${1:-}"
        ;;
    *)
        err "Unknown option: $1 (use --help)"
        ;;
esac
