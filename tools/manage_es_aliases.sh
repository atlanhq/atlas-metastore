#!/usr/bin/env bash
#
# manage_es_aliases.sh — Manage ES aliases after JanusGraph → Cassandra migration
#
# Creates the unified vertex index alias and migrates persona aliases
# from the old JanusGraph index to the new Cassandra graph index.
#
# Usage:
#   ./manage_es_aliases.sh --vcluster <name> [--namespace atlas] [--pod atlas-0] [--container atlas-main]
#
# This script runs curl commands inside the Atlas pod (via kubectl exec)
# to reach Elasticsearch at localhost:9200.
#
# Steps:
#   1. Create vertex index alias: atlas_vertex_index → atlas_graph_vertex_index
#   2. Migrate persona aliases from janusgraph_vertex_index to atlas_graph_vertex_index
#   3. Report summary
#

set -euo pipefail

# ============================================================================
# Constants
# ============================================================================

readonly SCRIPT_NAME="$(basename "$0")"

readonly OLD_INDEX="janusgraph_vertex_index"
readonly NEW_INDEX="atlas_graph_vertex_index"
readonly VERTEX_ALIAS="atlas_vertex_index"

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
DRY_RUN="false"

# ============================================================================
# Logging
# ============================================================================

log()  { echo -e "${C_CYAN}[$(date +%H:%M:%S)]${C_RESET} $*"; }
step() { echo -e "\n${C_BOLD}${C_BLUE}=== $* ===${C_RESET}"; }
ok()   { echo -e "${C_GREEN}  [PASS]${C_RESET} $*"; }
warn() { echo -e "${C_YELLOW}  [WARN]${C_RESET} $*" >&2; }
err()  { echo -e "${C_RED}  [FAIL]${C_RESET} $*" >&2; }
die()  { echo -e "${C_RED}[FATAL]${C_RESET} $*" >&2; exit 1; }

# ============================================================================
# Helpers
# ============================================================================

kexec_quiet() {
    kubectl exec "$POD" -n "$NAMESPACE" -c "$CONTAINER" -- "$@" 2>/dev/null
}

py_extract() {
    local script="$1"
    local result
    result=$(python3 -c "$script" 2>/dev/null || python -c "$script" 2>/dev/null || echo "")
    echo "${result%"${result##*[![:space:]]}"}"
}

# ============================================================================
# Usage
# ============================================================================

show_help() {
    cat <<'USAGE'
Usage: manage_es_aliases.sh --vcluster <name> [OPTIONS]

Manages ES aliases after JanusGraph → Cassandra migration.
Creates the vertex index alias and migrates persona aliases.

Required:
  --vcluster <name>       Target vcluster name (sets kubectl context)

Options:
  --namespace <ns>        Kubernetes namespace (default: atlas)
  --pod <name>            Atlas pod name (default: atlas-0)
  --container <name>      Container name (default: atlas-main)
  --dry-run               Show what would be done without making changes
  --help                  Show this help

Examples:
  # Run after migration to set up aliases
  ./manage_es_aliases.sh --vcluster mytenantname

  # Dry-run to preview changes
  ./manage_es_aliases.sh --vcluster mytenantname --dry-run
USAGE
}

# ============================================================================
# Argument parsing
# ============================================================================

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --vcluster)    VCLUSTER="$2"; shift 2 ;;
            --namespace)   NAMESPACE="$2"; shift 2 ;;
            --pod)         POD="$2"; shift 2 ;;
            --container)   CONTAINER="$2"; shift 2 ;;
            --dry-run)     DRY_RUN="true"; shift ;;
            --help|-h)     show_help; exit 0 ;;
            *)             die "Unknown option: $1 (use --help)" ;;
        esac
    done

    if [ -z "$VCLUSTER" ]; then
        die "Missing required argument: --vcluster <name>"
    fi
}

# ============================================================================
# Step 1: Create vertex index alias
# ============================================================================

create_vertex_index_alias() {
    step "Step 1: Vertex Index Alias"

    # Check target index exists
    log "Checking target index ${NEW_INDEX} exists..."
    local index_status
    index_status=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "localhost:9200/${NEW_INDEX}" || echo "000")

    if [ "$index_status" != "200" ]; then
        err "Target index ${NEW_INDEX} does not exist (HTTP ${index_status}). Migration may not have completed."
        return 1
    fi
    ok "Target index ${NEW_INDEX} exists"

    if [ "$DRY_RUN" = "true" ]; then
        log "[DRY-RUN] Would create alias: ${VERTEX_ALIAS} → ${NEW_INDEX}"
        return 0
    fi

    # Create alias using atomic remove+add (idempotent)
    log "Creating alias ${VERTEX_ALIAS} → ${NEW_INDEX}..."
    local alias_resp
    alias_resp=$(kexec_quiet curl -s -X POST "localhost:9200/_aliases" \
        -H 'Content-Type: application/json' \
        -d "{
            \"actions\": [
                {\"remove\": {\"index\": \"*\", \"alias\": \"${VERTEX_ALIAS}\"}},
                {\"add\": {\"index\": \"${NEW_INDEX}\", \"alias\": \"${VERTEX_ALIAS}\"}}
            ]
        }" || echo '{"acknowledged":false}')

    local acknowledged
    acknowledged=$(echo "$alias_resp" | py_extract "import sys,json; print(json.load(sys.stdin).get('acknowledged', False))")

    if [ "$acknowledged" = "True" ] || [ "$acknowledged" = "true" ]; then
        ok "Alias ${VERTEX_ALIAS} → ${NEW_INDEX} created"
    else
        err "Failed to create alias (response: ${alias_resp})"
        return 1
    fi

    # Verify
    local verify_resp
    verify_resp=$(kexec_quiet curl -s "localhost:9200/_alias/${VERTEX_ALIAS}" || echo "{}")
    local points_to
    points_to=$(echo "$verify_resp" | py_extract "import sys,json; d=json.load(sys.stdin); print(','.join(d.keys()))")

    if echo "$points_to" | grep -q "${NEW_INDEX}"; then
        ok "Verified: ${VERTEX_ALIAS} points to ${NEW_INDEX}"
    else
        warn "Alias verification inconclusive (points to: ${points_to})"
    fi

    return 0
}

# ============================================================================
# Step 2: Migrate persona aliases
# ============================================================================

migrate_persona_aliases() {
    step "Step 2: Persona Alias Migration"

    # Check old index exists (to read existing alias filters from)
    local old_index_status
    old_index_status=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "localhost:9200/${OLD_INDEX}" || echo "000")

    if [ "$old_index_status" != "200" ]; then
        warn "Old index ${OLD_INDEX} does not exist (HTTP ${old_index_status}). Skipping persona alias migration."
        return 0
    fi

    # Get all aliases from the old index
    log "Fetching aliases from ${OLD_INDEX}..."
    local old_aliases_json
    old_aliases_json=$(kexec_quiet curl -s "localhost:9200/${OLD_INDEX}/_alias/*" || echo "{}")

    # Parse alias definitions, filtering out system aliases
    local alias_data
    alias_data=$(echo "$old_aliases_json" | py_extract "
import sys, json

data = json.load(sys.stdin)
aliases = data.get('${OLD_INDEX}', {}).get('aliases', {})

# System aliases to skip
skip = {'${VERTEX_ALIAS}', '${OLD_INDEX}', '${NEW_INDEX}'}

persona_aliases = {}
for name, config in aliases.items():
    if name in skip:
        continue
    persona_aliases[name] = config

print(json.dumps(persona_aliases))
")

    if [ -z "$alias_data" ] || [ "$alias_data" = "{}" ] || [ "$alias_data" = "null" ]; then
        log "No persona aliases found on ${OLD_INDEX}"
        ok "Persona alias migration: nothing to migrate"
        return 0
    fi

    local alias_count
    alias_count=$(echo "$alias_data" | py_extract "import sys,json; print(len(json.load(sys.stdin)))")
    log "Found ${alias_count} persona aliases to migrate"

    if [ "$DRY_RUN" = "true" ]; then
        log "[DRY-RUN] Would migrate ${alias_count} aliases from ${OLD_INDEX} to ${NEW_INDEX}"
        echo "$alias_data" | py_extract "
import sys, json
aliases = json.load(sys.stdin)
for name in sorted(aliases.keys()):
    has_filter = 'filter' in aliases[name]
    print(f'  {name} (filter: {has_filter})')
"
        return 0
    fi

    # Build batch alias actions — move each alias from old to new index
    # We process in batches to avoid overly large requests
    local batch_size=50
    local migrated=0
    local failed=0
    local skipped=0

    local actions_json
    actions_json=$(echo "$alias_data" | py_extract "
import sys, json

aliases = json.load(sys.stdin)
batch_size = ${batch_size}
all_actions = []

for name, config in aliases.items():
    actions = []
    # Remove from old index
    actions.append({'remove': {'index': '${OLD_INDEX}', 'alias': name}})
    # Add to new index with same filter
    add_action = {'index': '${NEW_INDEX}', 'alias': name}
    if 'filter' in config:
        add_action['filter'] = config['filter']
    actions.append({'add': add_action})
    all_actions.extend(actions)

# Split into batches
batches = []
for i in range(0, len(all_actions), batch_size * 2):  # *2 because each alias has remove+add
    batch = all_actions[i:i + batch_size * 2]
    batches.append(json.dumps({'actions': batch}))

# Output batches separated by newline
for b in batches:
    print(b)
")

    if [ -z "$actions_json" ]; then
        warn "Failed to build alias migration actions"
        return 1
    fi

    # Execute each batch
    while IFS= read -r batch; do
        [ -z "$batch" ] && continue

        local batch_resp
        batch_resp=$(kexec_quiet curl -s -X POST "localhost:9200/_aliases" \
            -H 'Content-Type: application/json' \
            -d "$batch" || echo '{"acknowledged":false}')

        local batch_ack
        batch_ack=$(echo "$batch_resp" | py_extract "import sys,json; print(json.load(sys.stdin).get('acknowledged', False))")

        if [ "$batch_ack" = "True" ] || [ "$batch_ack" = "true" ]; then
            # Count aliases in this batch (each alias = 2 actions: remove + add)
            local batch_count
            batch_count=$(echo "$batch" | py_extract "import sys,json; print(len(json.load(sys.stdin).get('actions',[])) // 2)")
            migrated=$((migrated + batch_count))
        else
            # Try individual aliases from this batch
            warn "Batch failed, trying individual aliases..."
            local individual_aliases
            individual_aliases=$(echo "$batch" | py_extract "
import sys, json
data = json.load(sys.stdin)
actions = data.get('actions', [])
# Group by pairs (remove + add)
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
                a_resp=$(kexec_quiet curl -s -X POST "localhost:9200/_aliases" \
                    -H 'Content-Type: application/json' \
                    -d "$a_payload" || echo '{"acknowledged":false}')

                local a_ack
                a_ack=$(echo "$a_resp" | py_extract "import sys,json; print(json.load(sys.stdin).get('acknowledged', False))")

                if [ "$a_ack" = "True" ] || [ "$a_ack" = "true" ]; then
                    migrated=$((migrated + 1))
                else
                    err "  Failed to migrate alias: ${a_name} (${a_resp})"
                    failed=$((failed + 1))
                fi
            done <<< "$individual_aliases"
        fi
    done <<< "$actions_json"

    # Verify aliases on new index
    log "Verifying aliases on ${NEW_INDEX}..."
    local new_alias_count
    new_alias_count=$(kexec_quiet curl -s "localhost:9200/${NEW_INDEX}/_alias/*" | py_extract "
import sys, json
data = json.load(sys.stdin)
aliases = data.get('${NEW_INDEX}', {}).get('aliases', {})
# Exclude the vertex index alias from count
skip = {'${VERTEX_ALIAS}'}
count = sum(1 for name in aliases if name not in skip)
print(count)
" || echo "0")

    echo ""
    ok "Persona alias migration complete"
    log "  Migrated: ${migrated}"
    log "  Failed:   ${failed}"
    log "  Aliases on new index: ${new_alias_count}"

    if [ "$failed" -gt 0 ]; then
        return 1
    fi
    return 0
}

# ============================================================================
# Step 3: Summary
# ============================================================================

print_summary() {
    step "Summary"

    # Count aliases on new index
    local alias_list
    alias_list=$(kexec_quiet curl -s "localhost:9200/${NEW_INDEX}/_alias/*" || echo "{}")

    echo "$alias_list" | py_extract "
import sys, json

data = json.load(sys.stdin)
aliases = data.get('${NEW_INDEX}', {}).get('aliases', {})

print(f'  Total aliases on ${NEW_INDEX}: {len(aliases)}')
for name in sorted(aliases.keys()):
    has_filter = 'filter' in aliases[name]
    suffix = ' (filtered)' if has_filter else ''
    print(f'    - {name}{suffix}')
"
}

# ============================================================================
# Main
# ============================================================================

main() {
    parse_args "$@"

    step "ES Alias Management: ${VCLUSTER}"

    # Set kubectl context
    log "Setting kubectl context to vcluster: ${VCLUSTER}"
    if ! kubectl config use-context "$VCLUSTER" >/dev/null 2>&1; then
        if command -v vcluster >/dev/null 2>&1; then
            vcluster connect "$VCLUSTER" >/dev/null 2>&1 || true
        fi
        if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
            die "Cannot access namespace '$NAMESPACE' in vcluster '$VCLUSTER'"
        fi
    fi
    ok "kubectl context set"

    local exit_code=0

    create_vertex_index_alias || exit_code=1
    migrate_persona_aliases || exit_code=1
    print_summary

    if [ "$exit_code" -ne 0 ]; then
        err "Some alias operations failed"
        exit 1
    fi

    echo ""
    ok "All alias operations completed successfully"
    exit 0
}

main "$@"
