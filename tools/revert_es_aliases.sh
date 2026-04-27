#!/usr/bin/env bash
#
# revert_es_aliases.sh — Revert ES aliases after a failed or rolled-back migration
#
# This is the INVERSE of manage_es_aliases.sh. It restores ES aliases so that
# Atlas works seamlessly in JanusGraph mode after a rollback.
#
# What it does:
#   1. Revert vertex index alias: atlas_vertex_index → janusgraph_vertex_index
#   2. Migrate persona aliases from atlas_graph_vertex_index back to janusgraph_vertex_index
#   3. Remove atlas_graph_ prefixed UUID aliases from atlas_graph_vertex_index
#   4. Report summary
#
# Usage:
#   ./revert_es_aliases.sh --vcluster <name> [OPTIONS]
#
# When to use:
#   - After a failed migration when you need to revert to JanusGraph
#   - After a successful migration + switch, but you discover issues and roll back
#   - When manage_es_aliases.sh was run but you need to undo it
#   - When lock icons appear after rollback (persona aliases on wrong index)
#

set -euo pipefail

# ============================================================================
# Constants
# ============================================================================

readonly SCRIPT_NAME="$(basename "$0")"

readonly JG_INDEX="janusgraph_vertex_index"
readonly CG_INDEX="atlas_graph_vertex_index"
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
Usage: revert_es_aliases.sh --vcluster <name> [OPTIONS]

Reverts ES aliases after a failed migration or rollback to JanusGraph.
This is the INVERSE of manage_es_aliases.sh.

What it does:
  1. Points atlas_vertex_index alias back to janusgraph_vertex_index
  2. Moves persona aliases from atlas_graph_vertex_index to janusgraph_vertex_index
  3. Removes atlas_graph_ prefixed UUID aliases from atlas_graph_vertex_index
  4. Prints summary of all aliases on janusgraph_vertex_index

Required:
  --vcluster <name>       Target vcluster name (sets kubectl context)

Options:
  --namespace <ns>        Kubernetes namespace (default: atlas)
  --pod <name>            Atlas pod name (default: atlas-0)
  --container <name>      Container name (default: atlas-main)
  --dry-run               Show what would be done without making changes
  --help                  Show this help

When to use:
  - After rolling back from Cassandra to JanusGraph backend
  - When search returns empty results after rollback
  - When lock icons appear on assets after rollback
  - When manage_es_aliases.sh was run but migration was reverted

Examples:
  # Revert aliases after rollback
  ./revert_es_aliases.sh --vcluster mytenantname

  # Dry-run to preview changes
  ./revert_es_aliases.sh --vcluster mytenantname --dry-run
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
# Step 1: Revert vertex index alias to JanusGraph index
# ============================================================================

revert_vertex_index_alias() {
    step "Step 1: Revert Vertex Index Alias"

    # Check JanusGraph index exists
    log "Checking JanusGraph index ${JG_INDEX} exists..."
    local jg_status
    jg_status=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "localhost:9200/${JG_INDEX}" || echo "000")

    if [ "$jg_status" != "200" ]; then
        err "JanusGraph index ${JG_INDEX} does not exist (HTTP ${jg_status}). Cannot revert aliases."
        return 1
    fi
    ok "JanusGraph index ${JG_INDEX} exists"

    # Check current alias state
    local current_alias
    current_alias=$(kexec_quiet curl -s "localhost:9200/_alias/${VERTEX_ALIAS}" || echo "{}")
    local current_target
    current_target=$(echo "$current_alias" | py_extract "import sys,json; d=json.load(sys.stdin); print(','.join(d.keys()))")

    if echo "$current_target" | grep -q "${JG_INDEX}"; then
        ok "Alias ${VERTEX_ALIAS} already points to ${JG_INDEX} — no change needed"
        return 0
    fi

    log "Current alias target: ${current_target:-none}"

    if [ "$DRY_RUN" = "true" ]; then
        log "[DRY-RUN] Would revert alias: ${VERTEX_ALIAS} → ${JG_INDEX}"
        return 0
    fi

    # Revert alias using atomic remove+add (idempotent)
    log "Reverting alias ${VERTEX_ALIAS} → ${JG_INDEX}..."
    local alias_resp
    alias_resp=$(kexec_quiet curl -s -X POST "localhost:9200/_aliases" \
        -H 'Content-Type: application/json' \
        -d "{
            \"actions\": [
                {\"remove\": {\"index\": \"*\", \"alias\": \"${VERTEX_ALIAS}\"}},
                {\"add\": {\"index\": \"${JG_INDEX}\", \"alias\": \"${VERTEX_ALIAS}\"}}
            ]
        }" || echo '{"acknowledged":false}')

    local acknowledged
    acknowledged=$(echo "$alias_resp" | py_extract "import sys,json; print(json.load(sys.stdin).get('acknowledged', False))")

    if [ "$acknowledged" = "True" ] || [ "$acknowledged" = "true" ]; then
        ok "Alias ${VERTEX_ALIAS} → ${JG_INDEX} reverted"
    else
        err "Failed to revert alias (response: ${alias_resp})"
        return 1
    fi

    # Verify
    local verify_resp
    verify_resp=$(kexec_quiet curl -s "localhost:9200/_alias/${VERTEX_ALIAS}" || echo "{}")
    local points_to
    points_to=$(echo "$verify_resp" | py_extract "import sys,json; d=json.load(sys.stdin); print(','.join(d.keys()))")

    if echo "$points_to" | grep -q "${JG_INDEX}"; then
        ok "Verified: ${VERTEX_ALIAS} points to ${JG_INDEX}"
    else
        warn "Alias verification inconclusive (points to: ${points_to})"
    fi

    return 0
}

# ============================================================================
# Step 2: Move persona aliases back to JanusGraph index
# ============================================================================

revert_persona_aliases() {
    step "Step 2: Revert Persona Aliases to JanusGraph Index"

    # Check CG index exists (to read existing aliases from)
    local cg_status
    cg_status=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "localhost:9200/${CG_INDEX}" || echo "000")

    if [ "$cg_status" != "200" ]; then
        warn "Cassandra graph index ${CG_INDEX} does not exist (HTTP ${cg_status}). Skipping persona alias revert."
        return 0
    fi

    # Get all aliases from the Cassandra graph index
    log "Fetching aliases from ${CG_INDEX}..."
    local cg_aliases_json
    cg_aliases_json=$(kexec_quiet curl -s "localhost:9200/${CG_INDEX}/_alias/*" || echo "{}")

    # Parse alias definitions, filtering out system aliases and atlas_graph_ prefixed UUID aliases
    local alias_data
    alias_data=$(echo "$cg_aliases_json" | py_extract "
import sys, json

data = json.load(sys.stdin)
aliases = data.get('${CG_INDEX}', {}).get('aliases', {})

# System aliases to skip — these are not persona aliases
skip = {'${VERTEX_ALIAS}', '${JG_INDEX}', '${CG_INDEX}'}

# Also skip atlas_graph_ prefixed aliases (created by copy_vertex_uuid_aliases_to_edge.sh)
# These are copies, not originals — they'll be cleaned up in Step 3
persona_aliases = {}
for name, config in aliases.items():
    if name in skip:
        continue
    if name.startswith('atlas_graph_'):
        continue
    persona_aliases[name] = config

print(json.dumps(persona_aliases))
")

    if [ -z "$alias_data" ] || [ "$alias_data" = "{}" ] || [ "$alias_data" = "null" ]; then
        log "No persona aliases found on ${CG_INDEX}"
        ok "Persona alias revert: nothing to migrate"
        return 0
    fi

    local alias_count
    alias_count=$(echo "$alias_data" | py_extract "import sys,json; print(len(json.load(sys.stdin)))")
    log "Found ${alias_count} persona aliases to move back to ${JG_INDEX}"

    if [ "$DRY_RUN" = "true" ]; then
        log "[DRY-RUN] Would move ${alias_count} aliases from ${CG_INDEX} to ${JG_INDEX}"
        echo "$alias_data" | py_extract "
import sys, json
aliases = json.load(sys.stdin)
for name in sorted(aliases.keys()):
    has_filter = 'filter' in aliases[name]
    print(f'  {name} (filter: {has_filter})')
"
        return 0
    fi

    # Build batch alias actions — move each alias from CG index back to JG index
    local batch_size=50
    local migrated=0
    local failed=0

    local actions_json
    actions_json=$(echo "$alias_data" | py_extract "
import sys, json

aliases = json.load(sys.stdin)
batch_size = ${batch_size}
all_actions = []

for name, config in aliases.items():
    actions = []
    # Remove from Cassandra graph index
    actions.append({'remove': {'index': '${CG_INDEX}', 'alias': name}})
    # Add back to JanusGraph index with same filter
    add_action = {'index': '${JG_INDEX}', 'alias': name}
    if 'filter' in config:
        add_action['filter'] = config['filter']
    actions.append({'add': add_action})
    all_actions.extend(actions)

# Split into batches
batches = []
for i in range(0, len(all_actions), batch_size * 2):  # *2 because each alias has remove+add
    batch = all_actions[i:i + batch_size * 2]
    batches.append(json.dumps({'actions': batch}))

for b in batches:
    print(b)
")

    if [ -z "$actions_json" ]; then
        warn "Failed to build alias revert actions"
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
                    err "  Failed to revert alias: ${a_name} (${a_resp})"
                    failed=$((failed + 1))
                fi
            done <<< "$individual_aliases"
        fi
    done <<< "$actions_json"

    echo ""
    ok "Persona alias revert complete"
    log "  Reverted: ${migrated}"
    log "  Failed:   ${failed}"

    if [ "$failed" -gt 0 ]; then
        return 1
    fi
    return 0
}

# ============================================================================
# Step 3: Clean up atlas_graph_ prefixed UUID aliases
# ============================================================================

cleanup_uuid_aliases() {
    step "Step 3: Clean Up atlas_graph_ Prefixed UUID Aliases"

    # Check CG index exists
    local cg_status
    cg_status=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "localhost:9200/${CG_INDEX}" || echo "000")

    if [ "$cg_status" != "200" ]; then
        log "Cassandra graph index ${CG_INDEX} does not exist. Nothing to clean up."
        return 0
    fi

    # Get atlas_graph_ prefixed aliases (created by copy_vertex_uuid_aliases_to_edge.sh)
    log "Scanning for atlas_graph_ prefixed UUID aliases on ${CG_INDEX}..."
    local prefixed_aliases
    prefixed_aliases=$(kexec_quiet curl -s "localhost:9200/${CG_INDEX}/_alias/*" | py_extract "
import sys, json

data = json.load(sys.stdin)
aliases = data.get('${CG_INDEX}', {}).get('aliases', {})

# Find atlas_graph_ prefixed aliases (UUID copies)
prefixed = [name for name in aliases.keys() if name.startswith('atlas_graph_') and name != '${CG_INDEX}']
print(json.dumps(prefixed))
" || echo "[]")

    local count
    count=$(echo "$prefixed_aliases" | py_extract "import sys,json; print(len(json.load(sys.stdin)))")

    if [ "$count" = "0" ] || [ -z "$count" ]; then
        log "No atlas_graph_ prefixed UUID aliases found"
        ok "UUID alias cleanup: nothing to clean"
        return 0
    fi

    log "Found ${count} atlas_graph_ prefixed aliases to remove"

    if [ "$DRY_RUN" = "true" ]; then
        log "[DRY-RUN] Would remove ${count} aliases from ${CG_INDEX}"
        echo "$prefixed_aliases" | py_extract "
import sys, json
aliases = json.load(sys.stdin)
for name in sorted(aliases):
    print(f'  {name}')
"
        return 0
    fi

    # Build remove actions in batches
    local batch_size=50
    local removed=0
    local failed=0

    local actions_json
    actions_json=$(echo "$prefixed_aliases" | py_extract "
import sys, json

aliases = json.load(sys.stdin)
batch_size = ${batch_size}
all_actions = [{'remove': {'index': '${CG_INDEX}', 'alias': name}} for name in aliases]

batches = []
for i in range(0, len(all_actions), batch_size):
    batch = all_actions[i:i + batch_size]
    batches.append(json.dumps({'actions': batch}))

for b in batches:
    print(b)
")

    while IFS= read -r batch; do
        [ -z "$batch" ] && continue

        local batch_resp
        batch_resp=$(kexec_quiet curl -s -X POST "localhost:9200/_aliases" \
            -H 'Content-Type: application/json' \
            -d "$batch" || echo '{"acknowledged":false}')

        local batch_ack
        batch_ack=$(echo "$batch_resp" | py_extract "import sys,json; print(json.load(sys.stdin).get('acknowledged', False))")

        if [ "$batch_ack" = "True" ] || [ "$batch_ack" = "true" ]; then
            local batch_count
            batch_count=$(echo "$batch" | py_extract "import sys,json; print(len(json.load(sys.stdin).get('actions',[])))")
            removed=$((removed + batch_count))
        else
            warn "Batch removal failed (response: ${batch_resp})"
            failed=$((failed + 1))
        fi
    done <<< "$actions_json"

    echo ""
    ok "UUID alias cleanup complete"
    log "  Removed: ${removed}"
    log "  Failed:  ${failed}"

    if [ "$failed" -gt 0 ]; then
        return 1
    fi
    return 0
}

# ============================================================================
# Step 4: Summary
# ============================================================================

print_summary() {
    step "Summary"

    # Count aliases on JanusGraph index
    log "Aliases on ${JG_INDEX}:"
    local jg_alias_list
    jg_alias_list=$(kexec_quiet curl -s "localhost:9200/${JG_INDEX}/_alias/*" || echo "{}")

    echo "$jg_alias_list" | py_extract "
import sys, json

data = json.load(sys.stdin)
aliases = data.get('${JG_INDEX}', {}).get('aliases', {})

print(f'  Total aliases on ${JG_INDEX}: {len(aliases)}')
for name in sorted(aliases.keys()):
    has_filter = 'filter' in aliases[name]
    suffix = ' (filtered)' if has_filter else ''
    print(f'    - {name}{suffix}')
"

    # Check remaining aliases on CG index
    local cg_status
    cg_status=$(kexec_quiet curl -s -o /dev/null -w "%{http_code}" "localhost:9200/${CG_INDEX}" || echo "000")

    if [ "$cg_status" = "200" ]; then
        echo ""
        log "Remaining aliases on ${CG_INDEX}:"
        local cg_alias_list
        cg_alias_list=$(kexec_quiet curl -s "localhost:9200/${CG_INDEX}/_alias/*" || echo "{}")

        echo "$cg_alias_list" | py_extract "
import sys, json

data = json.load(sys.stdin)
aliases = data.get('${CG_INDEX}', {}).get('aliases', {})

if not aliases:
    print('  (none)')
else:
    print(f'  Total: {len(aliases)}')
    for name in sorted(aliases.keys()):
        print(f'    - {name}')
"
    fi
}

# ============================================================================
# Main
# ============================================================================

main() {
    parse_args "$@"

    step "ES Alias Revert: ${VCLUSTER}"
    log "This script reverts ES aliases for JanusGraph mode (inverse of manage_es_aliases.sh)"

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

    revert_vertex_index_alias || exit_code=1
    revert_persona_aliases || exit_code=1
    cleanup_uuid_aliases || exit_code=1
    print_summary

    if [ "$exit_code" -ne 0 ]; then
        err "Some alias revert operations failed"
        exit 1
    fi

    echo ""
    ok "All alias revert operations completed successfully"
    log "Atlas is ready to run in JanusGraph mode."
    exit 0
}

main "$@"
