---
description: Trigger, monitor, and manage the ZeroGraph deployer agent — JanusGraph→Cassandra mass tenant migration running on mothership-service-cluster
argument-hint: <vcluster-name> [--dry-run] [--fresh] [--skip-switch]
allowed-tools: [Bash, Read, AskUserQuestion, Task]
---

# ZeroGraph Deployer Agent

You are an operator interacting with the **mothership-zerograph** deployment — the dedicated agent that migrates Atlas tenants from JanusGraph to Cassandra (ZeroGraph).

**Agent base URL:** `https://mothership-zerograph.atlan.com`
**Namespace / cluster:** `mothership-zerograph` on `mothership-service-cluster`
**ArgoCD app:** `mothership-zerograph` (source: `atlanhq/cloud-common`, branch: `mothership_beta`)

Parse `$ARGUMENTS` for a vcluster name and optional flags (`--dry-run`, `--fresh`, `--skip-switch`).

If no arguments provided, use AskUserQuestion to collect them before proceeding.

---

## Step 1: Get Auth Token

The API requires a Bearer token. Fetch it from the `mothership` secret in the cluster:

```bash
# Switch to the right cluster first
loft use cluster mothership-service-cluster

INTEGRATION_SECRET=$(kubectl get secret mothership -n mothership-zerograph \
  -o jsonpath='{.data.INTEGRATION_INGRESS_SECRET}' | base64 -d)
echo "Token: $INTEGRATION_SECRET"
```

Set `BASE_URL=https://mothership-zerograph.atlan.com` and `TOKEN=$INTEGRATION_SECRET` for all subsequent calls.

---

## Step 2: Health Check

Verify the agent is up before triggering:

```bash
curl -s https://mothership-zerograph.atlan.com/health | jq .
# Expected: {"status":"healthy"}

curl -s https://mothership-zerograph.atlan.com/ok | jq .
# Deeper check including DB connectivity
```

If unhealthy, check pod logs:
```bash
kubectl logs -n mothership-zerograph deploy/mothership-zerograph-deployment \
  -c mothership-zerograph-container --tail=50
```

---

## Step 3: Connectivity Test (before migrating)

Always verify vcluster connectivity before triggering a migration:

```bash
kubectl exec -n mothership-zerograph deploy/mothership-zerograph-deployment \
  -c mothership-zerograph-container -- sh -c '
vcluster platform login https://onboarding-tenant.atlan.com \
  --access-key "$VCLUSTER_PLATFORM_ACCESS_KEY" && \
vcluster platform connect vcluster <VCLUSTER_NAME> --print > /tmp/test.kubeconfig && \
kubectl --kubeconfig /tmp/test.kubeconfig get nodes
'
```

Expected: nodes listed as `Ready`. If this fails, do NOT proceed — the agent cannot reach the tenant.

---

## Step 4: Trigger Migration

### Async (recommended for real migrations)

Returns immediately with HTTP 202. Migration runs in background. Max 5 concurrent.

```bash
curl -s -X POST https://mothership-zerograph.atlan.com/api/zerograph/deploy \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_name": "<VCLUSTER_NAME>",
    "linear_ticket_id": "MS-XXX",
    "dry_run": false,
    "fresh": false,
    "skip_switch": false
  }' | jq .
```

Response:
```json
{
  "tenant_name": "cusucwap02",
  "status": "accepted",
  "message": "Migration queued for cusucwap02. Poll status endpoint for progress.",
  "poll_url": "/api/zerograph/status/cusucwap02"
}
```

### Dry-run (safe, non-destructive)

Runs the full migration script with `--dry-run` — validates, probes, but makes no changes:

```bash
curl -s -X POST https://mothership-zerograph.atlan.com/api/zerograph/deploy \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_name": "<VCLUSTER_NAME>",
    "dry_run": true
  }' | jq .
```

### Sync (testing / dry-runs only — blocks until complete)

```bash
curl -s -X POST https://mothership-zerograph.atlan.com/api/zerograph/deploy/sync \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"tenant_name": "<VCLUSTER_NAME>", "dry_run": true}' | jq .
```

---

## Step 5: Monitor Progress

### Poll status

```bash
curl -s https://mothership-zerograph.atlan.com/api/zerograph/status/<VCLUSTER_NAME> \
  -H "Authorization: Bearer $TOKEN" | jq .
```

Status values: `queued` → `running` → `success` | `failed` | `rolled_back` | `dry_run`

### List all active migrations

```bash
curl -s https://mothership-zerograph.atlan.com/api/zerograph/active \
  -H "Authorization: Bearer $TOKEN" | jq .
```

### View migration history for a tenant

```bash
curl -s https://mothership-zerograph.atlan.com/api/zerograph/history/<VCLUSTER_NAME> \
  -H "Authorization: Bearer $TOKEN" | jq .
```

### Tail live pod logs

```bash
kubectl logs -n mothership-zerograph deploy/mothership-zerograph-deployment \
  -c mothership-zerograph-container -f | grep -i "<VCLUSTER_NAME>"
```

---

## Input Parameters Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tenant_name` | string | required | vcluster name (e.g. `cusucwap02`) |
| `linear_ticket_id` | string | null | Linear issue ID — agent posts progress updates |
| `dry_run` | bool | false | Run script with `--dry-run` — no changes made |
| `fresh` | bool | false | `--fresh` flag — clears state and restarts migration |
| `skip_switch` | bool | false | Migrate data but don't flip backend to Cassandra |
| `skip_backup_check` | bool | false | Skip pre-migration backup verification |
| `scanner_threads` | int | null | Override scanner thread count |
| `writer_threads` | int | null | Override writer thread count |
| `jvm_heap` | string | null | Override JVM heap e.g. `"24g"` |
| `migrator_cpu` | string | null | Override migrator pod CPU e.g. `"16"` |
| `migrator_memory` | string | null | Override migrator pod memory e.g. `"32Gi"` |
| `max_retries` | int | null | Override max retry count |

---

## Output Reference

On completion, `GET /api/zerograph/status/<tenant>` returns:

| Field | Description |
|-------|-------------|
| `status` | `success` / `failed` / `rolled_back` / `dry_run` |
| `exit_code` | Script exit code (0 = success) |
| `duration_seconds` | Total wall-clock time |
| `vertex_count` | Asset count probed from ES |
| `sizing_tier` | `small` / `medium` / `large` (drives pod sizing) |
| `verification_passed` | Post-migration checks passed |
| `verification_total` | Total post-migration checks |
| `error_message` | Failure reason if status != success |
| `diagnosis` | LLM-powered root cause analysis on failure |

---

## Infrastructure Reference

| Component | Details |
|-----------|---------|
| Pod | `mothership-zerograph-deployment-*` in `mothership-zerograph` ns |
| Image | `ghcr.io/atlanhq/mothership-zerograph-deployer:<tag>` |
| Image source | `atlanhq/mothership`, branch `zerograph-deployer` |
| Postgres DB | `zerograph` on `mothership-beta` RDS (table: `zerograph_deployments`) |
| Redis | Sidecar on port 6379 (session state) |
| vcluster platform | `https://onboarding-tenant.atlan.com` |
| Secret: vcluster key | `vcluster-platform` → `VCLUSTER_PLATFORM_ACCESS_KEY` |
| Max concurrent | 5 migrations at once (`ZG_MAX_CONCURRENT=5`) |
| Script timeout | 4 hours (`ZG_SCRIPT_TIMEOUT=14400`) |

---

## CI / GitOps Pipeline

To deploy a new image version:

1. Push to `zerograph-deployer` branch in `atlanhq/mothership`
2. CI builds `ghcr.io/atlanhq/mothership-zerograph-deployer:<sha>`
3. `repository_dispatch` fires to `atlanhq/cloud-common` (default `prod` branch)
4. `mothership-zerograph-deploy.yml` bumps image tag in `mothership_beta` values.yaml
5. ArgoCD auto-syncs to `mothership-service-cluster`

To manually bump the image tag:
```bash
# In atlanhq/cloud-common, branch mothership_beta
# Edit platform/k8/mothership-zerograph/values.yaml
# Update: mothership.image.tag: <new-sha>
```

---

## Common Operations

### Check if pod is healthy
```bash
loft use cluster mothership-service-cluster
kubectl get pods -n mothership-zerograph
kubectl describe pod -n mothership-zerograph -l app=mothership-zerograph
```

### Force restart the pod
```bash
kubectl rollout restart deployment/mothership-zerograph-deployment -n mothership-zerograph
kubectl rollout status deployment/mothership-zerograph-deployment -n mothership-zerograph
```

### Check which migrations are in-flight
```bash
curl -s https://mothership-zerograph.atlan.com/api/zerograph/active \
  -H "Authorization: Bearer $TOKEN" | jq '.[].tenant_name'
```

### Emergency: stop a running migration
There is no cancel API — the migration script runs inside the vcluster. To abort:
1. Identify the migrator pod inside the tenant vcluster:
   ```bash
   kubectl --kubeconfig /tmp/<tenant>.kubeconfig get pods -n atlas | grep migrat
   ```
2. Delete the migrator pod to kill the script
3. The agent will detect the failure, diagnose, and report back

---

## Tested Tenants

| Tenant | Cluster | Status |
|--------|---------|--------|
| `staging-preprod` | AWS ap-south-1 | Connectivity verified ✅ |
| `cusucwap02` | AWS ap-south-1 (itau-scale) | Connectivity verified ✅ |
| `cmegroupuat` | GCP | In cohort, not yet tested |
