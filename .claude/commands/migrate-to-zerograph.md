---
description: Schedule and monitor a JanusGraph -> Cassandra (Zero Graph) migration for an Atlas tenant
argument-hint: <vcluster-name>
allowed-tools: [Bash, Read, AskUserQuestion]
---

# Migrate Tenant to Zero Graph (Cassandra Backend)

You are a thin scheduler that collects inputs, runs `tools/atlas_migrate_tenant.sh` (the self-sufficient migration orchestrator), and streams its output. **All migration logic lives in the script** — you do NOT run kubectl commands, patch ConfigMaps, or make migration decisions yourself.

**CRITICAL: DO NOT SWITCH to CassandraGraph without explicit user confirmation.**

**Target vcluster:** $ARGUMENTS

---

## Phase 0: Collect Inputs

### 0.1 Parse vcluster name

Parse the vcluster name from `$ARGUMENTS`. If not provided, use AskUserQuestion to ask for it.

### 0.2 Pre-flight checklist

Use AskUserQuestion (multi-select):
- Question: "Please confirm these prerequisites before we begin. Select all that apply:"
- Header: "Pre-flight"
- multiSelect: true
- Options:
  - "Current build supports Zero Graph switching" — The deployed build includes `graphdb/cassandra/`, `graphdb/migrator/`, and `tools/atlas_migrate.sh`.
  - "ArgoCD is disabled for this tenant" — ArgoCD auto-sync is paused so ConfigMap changes are not overwritten.
  - "I have kubectl access to the vcluster" — Can run `kubectl exec` against the atlas pod.

**All three MUST be selected to proceed.** If any is missing, STOP and explain what's needed.

### 0.3 Migration mode

Use AskUserQuestion:
- Question: "Is this a first-time migration or a remigration?"
- Header: "Mode"
- Options:
  - "First-time migration (Recommended)" — No existing `atlas_graph` keyspace. Uses `--skip-cleanup --fresh`.
  - "Remigration" — Drops existing keyspace + ES index, then migrates fresh. Uses `--fresh` (no `--skip-cleanup`).

### 0.4 Migration ID strategy

Use AskUserQuestion:
- Question: "Which ID strategy should be used during the MIGRATION phase? (Note: the switch-to-CassandraGraph phase has its own ID strategy question later.)"
- Header: "Migration ID"
- Options:
  - "Legacy (Recommended)" — UUID-based IDs during migration. Uses `--id-strategy legacy`.
  - "Deterministic" — SHA-256 based IDs. Uses `--id-strategy deterministic`.

### 0.5 Claim resolution

Use AskUserQuestion:
- Question: "Enable claim resolution? (Maps legacy IDs to deterministic IDs for entity resolution.)"
- Header: "Claim"
- Options:
  - "Disabled (Recommended)" — No claim resolution during migration. Uses default (claim disabled).
  - "Enabled" — Adds `--claim-enabled`.

### 0.6 Maintenance mode

Use AskUserQuestion:
- Question: "Enable maintenance mode during migration? Blocks writes to prevent data drift."
- Header: "Maintenance"
- Options:
  - "Yes, enable maintenance mode (Recommended)" — Adds `--maintenance-mode`.
  - "No, keep tenant writable" — Omit flag.

### 0.7 Execution method

Use AskUserQuestion:
- Question: "How should the migration run?"
- Header: "Execution"
- Options:
  - "Local execution (Recommended)" — Runs `tools/atlas_migrate_tenant.sh` directly from your workstation (requires kubectl context configured).
  - "Helm Job" — Uses `--use-job` to create a dedicated migration pod via helm upgrade.

---

## Phase 1: Confirm Migration Config and Execute

### 1.1 Show configuration summary

Before executing, display the configuration summary to the user:
```
=== Migration Configuration ===
  Vcluster:          <VCLUSTER>
  Mode:              <first-time/remigration>
  ID Strategy:       <legacy/deterministic>
  Claim Enabled:     <true/false>
  Maintenance Mode:  <true/false>
  ES Field Limit:    10000
  Max Retries:       3
  Execution:         <local/helm-job>
  Skip Switch:       true (switch is a separate step requiring confirmation)
```

### 1.2 Confirm before migration

Use AskUserQuestion:
- Question: "Confirm the above migration configuration? The migration will copy data but will NOT switch the backend yet."
- Header: "Confirm"
- Options:
  - "Yes, start migration" — Proceed.
  - "No, abort" — Stop.

### 1.3 Build and execute command

```bash
# Base command — ALWAYS use --skip-switch so migration doesn't auto-switch
CMD="tools/atlas_migrate_tenant.sh --vcluster <VCLUSTER> --skip-switch"

# Mode flags (from 0.3)
# First-time: --fresh --skip-cleanup
# Remigration: --fresh

# ID strategy (from 0.4)
# Legacy: --id-strategy legacy
# Deterministic: --id-strategy deterministic

# Claim (from 0.5)
# Enabled: --claim-enabled

# Maintenance (from 0.6)
# Yes: --maintenance-mode

# ES field limit and retries
CMD="$CMD --es-field-limit 10000 --max-retries 3"

# Always add report file
CMD="$CMD --report-file /tmp/migration-report-<VCLUSTER>.json"
```

Execute based on method (from 0.7):

**Local execution:**
```bash
bash tools/atlas_migrate_tenant.sh --vcluster <VCLUSTER> --skip-switch <FLAGS> 2>&1
```

**Helm Job:**
```bash
bash tools/atlas_migrate_tenant.sh --vcluster <VCLUSTER> --skip-switch --use-job --helm-chart ./helm/atlas <FLAGS> 2>&1
```

**IMPORTANT**: Run the command and stream the output. The script handles all migration phases automatically but will NOT switch the backend (--skip-switch). Do NOT interrupt or add your own kubectl commands.

---

## Phase 2: Monitor Migration

While the script runs:
- Stream output directly to the user
- Watch for `[FATAL]` or `[FAIL]` messages
- The script will report token range completion and retry any failed ranges
- The script outputs a structured summary at the end

If the script exits with a non-zero code, report which phase failed and show the relevant error output.

---

## Phase 3: Post-Migration Report

After the migration script finishes:

1. Show the script's summary output
2. Read and display the JSON report file:
   ```bash
   cat /tmp/migration-report-<VCLUSTER>.json
   ```
3. Highlight any verification failures (especially token range completion and disk space checks)
4. If there were failures, show the log file location and suggest re-running without `--fresh` to resume

---

## Phase 4: Switch to CassandraGraph (REQUIRES EXPLICIT CONFIRMATION)

**Only proceed to this phase if the migration completed successfully AND the user explicitly requests the switch.**

### 4.1 Collect switch configuration

Use AskUserQuestion:
- Question: "Migration is complete. What ID strategy should be used for the SWITCH to CassandraGraph? (This configures how Atlas generates IDs going forward.)"
- Header: "Switch ID"
- Options:
  - "Deterministic (Recommended)" — SHA-256 based IDs after switch. Uses `--id-strategy deterministic`.
  - "Legacy" — UUID-based IDs after switch. Uses `--id-strategy legacy`.

### 4.2 Switch claim setting

Use AskUserQuestion:
- Question: "Enable claim resolution after switching?"
- Header: "Switch Claim"
- Options:
  - "Disabled (Recommended)" — Claim disabled after switch.
  - "Enabled" — Claim enabled after switch.

### 4.3 Show switch configuration and confirm

Display the switch configuration:
```
=== Backend Switch Configuration ===
  Vcluster:      <VCLUSTER>
  Action:        Switch atlas.graphdb.backend from janus → cassandra
  ID Strategy:   <deterministic/legacy> (for CassandraGraph going forward)
  Claim Enabled: <true/false>
```

Use AskUserQuestion:
- Question: "CONFIRM: Switch this tenant from JanusGraph to CassandraGraph? This will restart the atlas pod. The tenant will be briefly unavailable."
- Header: "SWITCH"
- Options:
  - "Yes, switch to CassandraGraph" — Proceed with the switch.
  - "No, do not switch yet" — Abort. Migration data is preserved for later switch.

### 4.4 Execute switch

Run the script with switch-only flags:
```bash
bash tools/atlas_migrate_tenant.sh --vcluster <VCLUSTER> --switch-only --id-strategy <STRATEGY> <CLAIM_FLAG> --report-file /tmp/switch-report-<VCLUSTER>.json 2>&1
```

### 4.5 Post-switch verification

The script runs verification checks after switching. Show the results and highlight any failures.

---

## Error Handling

- If the script fails, do NOT attempt to fix things manually. Show the error and the log file path.
- If verification fails (exit code 5), the migration succeeded but post-switch checks had issues. Suggest running `/debug-policies` if lock icons are detected.
- The script creates ConfigMap backups automatically. Show the rollback command from the summary.
- Migration is resumable. If it fails mid-way, suggest re-running without `--fresh` to resume.
- Token range failures are automatically retried up to 3 times by the Java migrator.
