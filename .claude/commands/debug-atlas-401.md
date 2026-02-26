---
description: Debug Atlas 401/auth failures on a live Atlan tenant using kubectl + Keycloak inspection
argument-hint: <TENANT_ID> (e.g. enpla9up53)
allowed-tools: [Bash, Read, AskUserQuestion]
---

# Debug Atlas on Tenant: $ARGUMENTS

This skill diagnoses Atlas authentication failures (401 Unauthorized, impersonation stuck,
indexsearch failing) on a live tenant. All steps are **read-only** — no writes to the cluster.

---

## Phase 1: Confirm kubectl Access to Tenant vCluster

```bash
kubectl config get-contexts 2>/dev/null | grep -i "$ARGUMENTS"
```

If no context matches, the tenant vCluster kubeconfig may not be loaded. Stop and ask the user
to run `loft vcluster connect $ARGUMENTS` or fetch the kubeconfig via the onboarding platform.

Set a shell variable for convenience — use the matching context name from above (typically
`loft-vcluster_$ARGUMENTS_default`):

```bash
TCTX="loft-vcluster_$ARGUMENTS_default"
```

---

## Phase 2: Check Pod Health

Run all three in parallel:

```bash
kubectl --context=$TCTX get pods -n atlas
```
```bash
kubectl --context=$TCTX get pods -n keycloak
```
```bash
kubectl --context=$TCTX get pods -n ranger
```

**What to look for:**
- Atlas pods (`atlas-0`, `atlas-1`): should be `3/3 Running`. Note the `AGE` — a recent restart
  (< 10 min) means Atlas loaded its Keycloak config at a time Keycloak may have been broken.
- Keycloak pods: should be `1/1 Running`. If restarted very recently, Keycloak is likely the
  cause of the outage.
- CrashLoopBackOff or high restart counts on any pod: escalate to the infra team.

---

## Phase 3: Reproduce the 401 Directly

Get a fresh token from the Argo client (works for all tenants):

```bash
# Get Argo client secret from Kubernetes secret
ARGO_SECRET=$(kubectl --context=$TCTX get secret keycloak-credentials -n argo \
  -o jsonpath='{.data.KEYCLOAK_PASSWORD}' | base64 -d)

# Get token from default realm
ARGO_TOKEN=$(curl -s -X POST \
  "https://$ARGUMENTS.atlan.com/auth/realms/default/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=atlan-argo&client_secret=${ARGO_SECRET}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('access_token','ERROR: '+str(d))[:80])")

echo "Token (first 80 chars): $ARGO_TOKEN"
```

Test indexsearch:

```bash
curl -s -o /dev/null -w "HTTP Status: %{http_code}" \
  -X POST "https://$ARGUMENTS.atlan.com/api/meta/search/indexsearch" \
  -H "Authorization: Bearer $ARGO_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"dsl":{"query":{"match_all":{}},"size":1}}'
```

- **200** → Atlas is working fine, the user's token may be expired.
- **401** → Proceed to Phase 4.
- **503 / timeout** → Atlas pod is not healthy; check logs in Phase 6.

---

## Phase 4: Inspect the JWT Token Issuer

Decode the Argo token payload (no verification needed):

```bash
echo $ARGO_TOKEN | python3 -c "
import sys, base64, json
t = sys.stdin.read().strip()
p = t.split('.')[1]
p += '=' * (4 - len(p) % 4)
d = json.loads(base64.b64decode(p).decode())
print('iss :', d.get('iss'))
print('azp :', d.get('azp'))
print('exp :', d.get('exp'))
print('realm:', d.get('realm'))
"
```

**What to check:**

| `iss` value | Meaning | Action |
|---|---|---|
| `https://<tenant>.atlan.com/auth/realms/default` | Correct ✅ | Token is healthy; skip to Phase 6 |
| `https:/auth/realms/default` (missing hostname) | **Broken** ❌ | Keycloak `frontendUrl` misconfigured → Phase 5 |
| `https://*.com/auth/realms/default` | **Broken** ❌ | Same — wildcard frontendUrl → Phase 5 |
| `http://keycloak-http.keycloak.svc.cluster.local/...` | Internal URL leaking | frontendUrl not set → Phase 5 |

---

## Phase 5: Diagnose & Fix Keycloak frontendUrl

### 5a. Check OIDC Discovery (external URL)

```bash
curl -s "https://$ARGUMENTS.atlan.com/auth/realms/default/.well-known/openid-configuration" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('issuer  :', d.get('issuer'))
print('jwks_uri:', d.get('jwks_uri'))
"
```

If `issuer` is not `https://<tenant>.atlan.com/auth/realms/default` the frontendUrl is wrong.

### 5b. Check realm attribute in the database

```bash
# Find the postgres primary pod
kubectl --context=$TCTX get pods -n cloudnative-postgres

# Query (replace postgres-cluster-1 with the primary pod name)
kubectl --context=$TCTX exec -n cloudnative-postgres postgres-cluster-1 -- \
  psql -U postgres -d keycloak \
  -c "SELECT realm_id, name, value FROM realm_attribute WHERE name='frontendUrl';"
```

Expected: `value = https://<tenant>.atlan.com/auth`
Broken example: `value = https://*.com/auth`

### 5c. Check the env var (should always be correct)

```bash
kubectl --context=$TCTX get statefulset keycloak -n keycloak \
  -o jsonpath='{.spec.template.spec.containers[0].env}' \
  | python3 -c "
import sys, json
envs = json.load(sys.stdin)
for e in envs:
    if 'FRONTEND' in e.get('name',''):
        print(e['name'], '=', e.get('value'))
"
```

**The realm-level DB attribute overrides the env var.** If the DB value is wrong, the env var
being correct is not enough — the realm attribute must be fixed.

### 5d. Fix (write action — requires approval)

> ⚠️ This modifies Keycloak configuration. Confirm with the user before proceeding.

Get a Keycloak admin token:

```bash
KC_PASS=$(kubectl --context=$TCTX get secret keycloak-secret-manager -n keycloak \
  -o jsonpath='{.data.KEYCLOAK_PASSWORD}' | base64 -d)

ADMIN_TOKEN=$(curl -s -X POST \
  "https://$ARGUMENTS.atlan.com/auth/realms/master/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password&client_id=admin-cli&username=batman&password=${KC_PASS}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('access_token',''))")
```

Update the realm frontendUrl:

```bash
curl -s -X PUT "https://$ARGUMENTS.atlan.com/auth/admin/realms/default" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"attributes\": {\"frontendUrl\": \"https://$ARGUMENTS.atlan.com/auth\"}}"
```

Verify the fix:

```bash
curl -s "https://$ARGUMENTS.atlan.com/auth/realms/default/.well-known/openid-configuration" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('issuer:', d.get('issuer'))"
```

After fixing the `frontendUrl`:
- New tokens will have the correct `iss`
- Keycloak impersonation will work again
- **Atlas still needs a restart** (Phase 7) because it cached the old JWKS URI at startup

---

## Phase 6: Check Atlas Logs

```bash
# Get log file name (format: atlas.YYYYMMDD-HHMMSS.out)
kubectl --context=$TCTX exec -n atlas atlas-0 -c atlas-main -- ls /opt/apache-atlas/logs/

# Check for auth/Keycloak errors
kubectl --context=$TCTX exec -n atlas atlas-0 -c atlas-main -- \
  tail -n 300 /opt/apache-atlas/logs/atlas.*.out \
  | grep -i "keycloak\|401\|unauthorized\|token\|jwks\|error\|exception\|ranger\|auth" \
  | grep -v "PolicyDelta\|PolicyRefresher\|AtlasAuthRESTClient\|downloadPolicies" \
  | tail -40
```

**Common error patterns and their meanings:**

| Log pattern | Meaning |
|---|---|
| `getAdminEvents ... HTTP 401 Unauthorized` | `atlan-backend` service account missing `realm-management` roles in Keycloak → Ranger user store can't refresh (see Phase 8) |
| `RangerBasePlugin ... fetched service policies contains no policies` | Ranger policy engine not loading policies — separate from auth |
| `JWKS` or `public key` fetch errors | Stale JWKS cache — restart Atlas (Phase 7) |
| No 401-related errors at all | The 401 is at the Spring Security filter level (before Atlas logs it) — always means Keycloak adapter issue |

---

## Phase 7: Verify JWKS Reachability from Atlas Pod

After fixing `frontendUrl`, confirm Atlas can now reach the JWKS endpoint:

```bash
# Test internal JWKS (always works if Keycloak is up)
kubectl --context=$TCTX exec -n atlas atlas-0 -c atlas-main -- \
  curl -s "http://keycloak-http.keycloak.svc.cluster.local/auth/realms/default/protocol/openid-connect/certs" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('Keys found:', len(d.get('keys',[])))"

# Test external JWKS (used by the Keycloak adapter via OIDC discovery)
kubectl --context=$TCTX exec -n atlas atlas-0 -c atlas-main -- \
  curl -s "https://$ARGUMENTS.atlan.com/auth/realms/default/protocol/openid-connect/certs" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('Keys found:', len(d.get('keys',[])))"
```

If the external JWKS returns `0 keys` or fails, there is a network/DNS issue between Atlas pods
and the tenant ingress (unlikely but possible with VPN/network splits).

**When Atlas needs a restart:**
- Keycloak `frontendUrl` was fixed while Atlas was already running
- The Keycloak adapter caches the OIDC configuration (including JWKS URI) at startup
- The stale broken JWKS URI remains cached until Atlas restarts
- A restart forces the adapter to re-fetch OIDC discovery and get the correct JWKS URI

```bash
# Restart Atlas (write action — confirm with user first)
kubectl --context=$TCTX rollout restart statefulset/atlas -n atlas

# Watch the rollout
kubectl --context=$TCTX rollout status statefulset/atlas -n atlas
```

After restart, re-run Phase 3 to confirm indexsearch works.

---

## Phase 8: Check `atlan-backend` Service Account Roles (Ranger User Store Issue)

If the logs show `getAdminEvents → 401` repeatedly, the `atlan-backend` Keycloak service account
is missing `realm-management` permissions.

```bash
KC_PASS=$(kubectl --context=$TCTX get secret keycloak-secret-manager -n keycloak \
  -o jsonpath='{.data.KEYCLOAK_PASSWORD}' | base64 -d)

ADMIN_TOKEN=$(curl -s -X POST \
  "https://$ARGUMENTS.atlan.com/auth/realms/master/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password&client_id=admin-cli&username=batman&password=${KC_PASS}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('access_token',''))")

# Get atlan-backend service account user ID
BACKEND_SA_ID=$(curl -s \
  "https://$ARGUMENTS.atlan.com/auth/admin/realms/default/clients?clientId=atlan-backend" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  | python3 -c "
import sys, json
clients = json.load(sys.stdin)
print(clients[0]['id'] if clients else 'NOT_FOUND')
")

curl -s \
  "https://$ARGUMENTS.atlan.com/auth/admin/realms/default/clients/${BACKEND_SA_ID}/service-account-user" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('SA user ID:', d.get('id'))"

# Then check its realm-management roles (replace USER_ID with value from above)
REALM_MGMT_ID=$(curl -s \
  "https://$ARGUMENTS.atlan.com/auth/admin/realms/default/clients?clientId=realm-management" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  | python3 -c "import sys,json; c=json.load(sys.stdin); print(c[0]['id'] if c else '')")

curl -s \
  "https://$ARGUMENTS.atlan.com/auth/admin/realms/default/users/<SA_USER_ID>/role-mappings/clients/${REALM_MGMT_ID}" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('realm-management roles:', [r['name'] for r in d])"
```

Expected roles: at minimum `view-events` and `view-users`. If empty — this is a Keycloak
misconfiguration that needs to be fixed via the Keycloak admin console or via the tenant's
ArgoCD app (keycloak-config).

---

## Phase 9: Token Introspection Sanity Check

This verifies Keycloak can validate a token end-to-end (independent of Atlas):

```bash
ARGO_SECRET=$(kubectl --context=$TCTX get secret keycloak-credentials -n argo \
  -o jsonpath='{.data.KEYCLOAK_PASSWORD}' | base64 -d)

ARGO_TOKEN=$(curl -s -X POST \
  "https://$ARGUMENTS.atlan.com/auth/realms/default/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=atlan-argo&client_secret=${ARGO_SECRET}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('access_token',''))")

BACKEND_SECRET=$(kubectl --context=$TCTX get secret atlas-secret-manager -n atlas \
  -o jsonpath='{.data.KEYCLOAK_CLIENT_SECRET}' | base64 -d)

curl -s -X POST \
  "https://$ARGUMENTS.atlan.com/auth/realms/default/protocol/openid-connect/token/introspect" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "client_id=atlan-backend&client_secret=${BACKEND_SECRET}&token=${ARGO_TOKEN}" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('active:', d.get('active'))
print('iss   :', d.get('iss'))
print('user  :', d.get('preferred_username'))
"
```

- `active: True` + correct `iss` → Keycloak is healthy; Atlas needs restart to pick up fix
- `active: False` → Token is invalid or Keycloak itself is broken
- Error response → Keycloak is not reachable or `atlan-backend` credentials are wrong

---

## Diagnostic Summary Checklist

After running through the phases, summarize findings:

```
Tenant: $ARGUMENTS
kubectl context: [ available / not available ]

Pod health:
  Atlas:     [ Running / CrashLoop / Restarting ]  age: ___
  Keycloak:  [ Running / CrashLoop / Restarting ]  age: ___

Token issuer (iss):  [ correct / BROKEN → https:/auth/... ]
OIDC discovery issuer: [ correct / BROKEN ]
JWKS URI reachable (external): [ yes / no ]
JWKS URI reachable (internal): [ yes / no ]

Keycloak frontendUrl (DB): [ correct / BROKEN → value: ___ ]
Keycloak frontendUrl (env): [ correct / BROKEN ]

Atlas logs show:
  - getAdminEvents 401:  [ yes (atlan-backend missing roles) / no ]
  - Other auth errors:   [ describe ]

Atlas restart needed: [ yes / no ]
```

---

## Common Runbooks

| Symptom | Root Cause | Fix |
|---|---|---|
| All API calls → 401, impersonation stuck | Keycloak `frontendUrl` DB attribute wrong (e.g. `https://*.com/auth`) | Fix frontendUrl in Keycloak realm → restart Atlas |
| 401 persists after frontendUrl fix | Atlas cached old (broken) JWKS URI at startup | Restart Atlas: `kubectl rollout restart statefulset/atlas -n atlas` |
| `getAdminEvents` 401 in Atlas logs | `atlan-backend` SA missing `realm-management` roles | Add `view-events` + `view-users` to SA in Keycloak admin console |
| Heracles works but Atlas doesn't | Atlas uses Bearer JWT validation; Heracles uses a different auth path | Check Keycloak token `iss` and JWKS reachability |
| 302 redirect to `https:/auth/...` from Atlas | Same as frontendUrl issue — redirect URL built from broken OIDC discovery | Fix frontendUrl |
| Atlas healthy but 403 (not 401) | Ranger policy not loaded or user not in any policy | Check Ranger logs and policy bootstrap |
