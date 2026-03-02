# MS-699: Intermittent 403s Due to Policy Refresh Race Condition

## Problem

When `POST /entity/bulk` creates AuthPolicy entities (e.g., during Connection setup), the policies are persisted to JanusGraph but the in-memory Ranger policy engine is not updated until the next `PolicyRefresher` cycle (every 30 seconds). Requests arriving in this 0–30s gap receive `403 (policyId=-1)` because no matching policy exists in memory yet.

## Impact

- Intermittent 403 errors on freshly created Connections, Personas, and Purposes
- Affects automated workflows (GH Actions, Terraform) that create access control entities and immediately use them
- Validated on: dev-experience.atlan.com, developer-experience.atlan.com, and GH Actions CI runs

## Root Causes

### 1. Race Condition (P0)
After AuthPolicy entities are committed to JanusGraph, there is no mechanism to notify the in-memory Ranger policy engine. The `PolicyRefresher` thread polls every 30 seconds, leaving a window where requests are authorized against stale policy state.

### 2. Hardcoded `policyVersion = -1` (P1)
`CachePolicyTransformerImpl` sets `servicePolicies.setPolicyVersion(-1L)` in both `getPoliciesAll()` and `getPoliciesDelta()`. This means `lastKnownVersion` stays at `-1` forever, making version-based change detection meaningless and obscuring logs.

### 3. Startup ConnectException on First Delta (P1)
`PolicyRefresher` starts before Jetty is ready, so the first delta attempt fails with `ConnectException: Failed to connect to localhost:21000`. This leaves stale/bootstrap policies active for ~70 seconds (until the next successful refresh cycle) instead of immediately falling back to a full cache load.

## Fixes

### Fix 1: Post-commit async policy refresh trigger
- **`RangerBasePlugin.java`**: Added `getRefresher()` getter to expose the `PolicyRefresher` instance
- **`RangerAtlasAuthorizer.java`**: Added `triggerPolicyRefresh()` static method that calls `refresher.syncPoliciesWithAdmin()` to force an immediate policy download
- **`AtlasEntityStoreV2.java`**: Registered a `PostTransactionHook` for `POLICY_ENTITY_TYPE` that, on successful commit, schedules an async policy refresh with a 2-second delay (to allow ES indexing to complete)

### Fix 2: Use monotonic timestamps for policy version
- **`CachePolicyTransformerImpl.java`**: Changed `setPolicyVersion(-1L)` to `setPolicyVersion(System.currentTimeMillis())` in both `getPoliciesAll()` and `getPoliciesDelta()`, so version tracking advances meaningfully

### Fix 3: ConnectException fallback to full load
- **`PolicyRefresher.java`**: When a `ConnectException` is caught during delta load and policies are already set in the plugin, fall back to `CachePolicyTransformerImpl.getPoliciesAll()` instead of silently failing. Added `isConnectException()` helper that walks the cause chain.

## Files Changed

| File | Change |
|------|--------|
| `auth-agents-common/.../plugin/service/RangerBasePlugin.java` | Added `getRefresher()` getter |
| `auth-plugin-atlas/.../authorizer/RangerAtlasAuthorizer.java` | Added `triggerPolicyRefresh()` static method |
| `repository/.../store/graph/v2/AtlasEntityStoreV2.java` | PostTransactionHook for POLICY_ENTITY_TYPE |
| `auth-agents-common/.../policytransformer/CachePolicyTransformerImpl.java` | Fixed policyVersion=-1 (2 locations) |
| `auth-agents-common/.../plugin/util/PolicyRefresher.java` | ConnectException fallback + `isConnectException()` helper |

## Testing

1. Build: `mvn compile -pl repository,auth-agents-common,auth-plugin-atlas -am -DskipTests -Drat.skip=true`
2. Manual verification: Create a Connection via API and immediately query it — should no longer get 403
3. Log verification: `lastKnownVersion` should advance from -1 after first refresh; ConnectException on first delta should trigger full load fallback

## Iteration 2: Post-Deployment Fixes

After initial deployment, DataMeshTest showed new intermittent 403 failures (`createView`, `createCol1`, `purgeConnection` failing while earlier steps pass). Root cause: three bugs in our initial implementation.

### Bug 1: PostTransactionHook registered per-entity instead of per-transaction
`getPreProcessors()` is called once per entity. For 7 bootstrap AuthPolicies in a bulk create, 7 separate `PostTransactionHook` instances were registered, each with its own `AtomicBoolean`. All 7 passed the `compareAndSet` check, scheduling 7 redundant refresh tasks.

**Fix**: Replaced per-entity `AtomicBoolean` with a `ThreadLocal<Boolean>` guard. Only 1 hook is registered per thread (= per transaction). The ThreadLocal resets in `onComplete()` for the next transaction.

### Bug 2: `isConnectException()` too narrow
OkHttp throws `SocketTimeoutException`, `SocketException`, or `IOException` (not `ConnectException`) when the server isn't ready. The fallback to full cache load never triggered.

**Fix**: Renamed to `isConnectionException()` and broadened to also match `SocketTimeoutException`, `SocketException`, `UnknownHostException`, and message-based patterns ("Connection refused", "Failed to connect").

### Bug 3: 2-second delay too short for ES indexing
The PostTransactionHook scheduled refresh 2s after commit. If ES hadn't indexed audit events within 2s, the delta load found 0 events.

**Fix**: Increased delay from 2s to 5s.

## What This Does NOT Change

- ABAC policy loading (separate data/config investigation needed)
- MS-700/702 (Keycloak authentication 401s — separate root cause)
- No retry-on-deny in `AtlasAuthorizationUtils` (post-commit trigger should be sufficient)
