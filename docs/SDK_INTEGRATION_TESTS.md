# Atlan-Java SDK Integration Tests

**Status:** ✅ Active  
**Target:** AWS vCluster (Full Atlan Deployment)  
**Trigger:** Runs after smoke tests pass on protected branches

## Overview

The SDK integration tests validate that the `atlan-java` SDK remains compatible with `atlas-metastore` changes by running critical path tests against a live Atlan tenant deployment.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ CI Pipeline (maven.yml)                                     │
├─────────────────────────────────────────────────────────────┤
│ 1. build          → Build atlas-metastore + Docker image   │
│                                                             │
│ 2. smoke-test     → Deploy to vclusters (AWS/Azure/GCP)    │
│                   → Run smoke tests (parallel)              │
│                   → Run SDK tests against AWS       ← NEW   │
│                                                             │
│ 3. helm-publish   → Publish Helm charts                    │
└─────────────────────────────────────────────────────────────┘

Note: SDK tests run in the smoke-test job to preserve VPN and
vCluster connections from earlier steps.
```

## Implementation

### Script: `scripts/atlan-java-sdk-test.sh`

**Purpose:** Execute atlan-java SDK tests against a deployed Atlan tenant

**Usage:**
```bash
./scripts/atlan-java-sdk-test.sh <atlan-domain> <client-id> <client-secret>
```

**What it does:**
1. Authenticates with Keycloak (OAuth client credentials flow)
2. Clones `atlan-java` repository (patches `FIXED_USER` if configured)
3. Verifies tenant connectivity
4. Runs 5 critical path tests **in parallel** (using background jobs)
5. Collects and reports results (informational only)

### Workflow Steps: SDK Tests in `smoke-test` Job

**Location:** `.github/workflows/maven.yml` (smoke-test job, after parallel smoke tests)

**Why In Same Job?**
- Reuses VPN connection from earlier steps
- Reuses vCluster kubeconfig connections
- No need to re-establish authentication
- More efficient (saves ~2-3 minutes setup time)

**Triggers When:**
- Branch: `beta`, `staging`, or `master`
- After: Parallel smoke tests complete successfully
- Condition: AWS tenant deployment is healthy

**Configuration:**
```yaml
env:
  ATLAN_DOMAIN: ${{ secrets.VCLUSTER_AWS_ATLAN_DOMAIN }}
  CLIENT_ID: ${{ secrets.VCLUSTER_AWS_CLIENT_ID }}
  CLIENT_SECRET: ${{ secrets.VCLUSTER_AWS_CLIENT_SECRET }}
  ATLAN_FIXED_USER: ${{ secrets.VCLUSTER_AWS_FIXED_USER }}  # Optional: User for ownership tests
```

## Tests Run

### Critical Path Tests (5 total)

| Test | Purpose | Duration (Parallel) |
|------|---------|----------|
| `GlossaryTest` | Business glossary operations | ~2-3 min |
| `CustomMetadataTest` | Custom attribute management | ~2-3 min |
| `ConnectionTest` | Connection management | ~2-3 min |
| `LineageTest` | Lineage tracking | ~2-3 min |
| `SearchTest` | Asset search functionality | ~2-3 min |

**Total Duration:** ~3-5 minutes (tests run in parallel)

> 🚀 **Performance Note:** Tests run in parallel using background jobs, reducing total execution time from 10-15 minutes (sequential) to 3-5 minutes (parallel).

### Why These Tests?

These tests were selected because they:
- ✅ Cover core Atlan product features
- ✅ Work with standard Atlan deployments (no cloud-specific dependencies)
- ✅ Exercise multiple services (Atlas, Heracles, Heka)
- ✅ Are relatively fast and stable
- ✅ Provide meaningful signal about SDK compatibility

## Authentication

**Method:** OAuth 2.0 Client Credentials Grant

**Flow:**
```bash
# 1. Request token from Keycloak
POST https://${ATLAN_DOMAIN}/auth/realms/default/protocol/openid-connect/token
Content-Type: application/x-www-form-urlencoded

client_id=${CLIENT_ID}
client_secret=${CLIENT_SECRET}
grant_type=client_credentials

# 2. Use token with SDK
export ATLAN_BASE_URL=https://${ATLAN_DOMAIN}
export ATLAN_API_KEY=${ACCESS_TOKEN}
```

**Credentials:** Stored in GitHub Secrets
- `VCLUSTER_AWS_ATLAN_DOMAIN` - Tenant URL (e.g., `meta02.atlan.com`)
- `VCLUSTER_AWS_CLIENT_ID` - OAuth client ID (e.g., `atlan-backend`)
- `VCLUSTER_AWS_CLIENT_SECRET` - OAuth client secret
- `VCLUSTER_AWS_FIXED_USER` - (Optional) Verified user for ownership/assignment tests (defaults to `chris`)

## Failure Handling

**Policy:** **Informational Only - Does Not Block Releases**

### Why Non-Blocking?

1. **SDK is downstream dependency** - atlas-metastore should not be blocked by SDK issues
2. **Initial rollout** - Establishing baseline metrics before enforcing
3. **External dependencies** - Tenant availability, network issues, etc.
4. **Early warning system** - Catch issues before they reach production

### When a Test Fails

- ✅ Workflow continues (helm-publish still runs)
- ⚠️ GitHub job summary shows failure
- 📊 Logs uploaded as artifacts (7-day retention)
- 📧 Team can review and investigate

### Future Enforcement

After establishing baseline reliability, we may:
1. Block releases on **critical test failures**
2. Require manual approval for SDK test failures
3. Auto-create issues for persistent failures

## Artifacts

The job uploads two artifacts on every run:

### 1. SDK Test Logs (`sdk-test-logs-{run_id}`)
```
sdk-test-logs/
├── clone.log              # Git clone output
├── GlossaryTest.log       # Test execution log
├── CustomMetadataTest.log
├── AtlanTagTest.log
├── LineageTest.log
└── SearchTest.log
```

### 2. SDK Test Results (`sdk-test-results-{run_id}`)
```
atlan-java-sdk-tests/integration-tests/build/test-results/
└── test/
    ├── TEST-com.atlan.java.sdk.GlossaryTest.xml
    ├── TEST-com.atlan.java.sdk.CustomMetadataTest.xml
    └── ... (JUnit XML reports)
```

## Monitoring & Observability

### GitHub Job Summary

Every run produces a summary in the workflow:

```markdown
## 🧪 SDK Integration Tests

**Target**: AWS Tenant (`meta02.atlan.com`)

**Status**: ✅ All tests passed
(or)
**Status**: ⚠️ Some tests failed (informational only)

> **Note**: SDK test failures do not block releases.
> This is a quality signal for SDK compatibility.

**Tests Run**: GlossaryTest, CustomMetadataTest, AtlanTagTest, LineageTest, SearchTest

📊 [View detailed logs in artifacts](#)
```

### Metrics to Track

- **Pass Rate**: % of runs where all tests pass
- **Test Duration**: Track test execution time trends
- **Failure Patterns**: Which tests fail most frequently
- **Tenant Health Correlation**: Do SDK tests fail when smoke tests barely pass?

## Troubleshooting

### Common Issues

#### 1. Authentication Failure
```
❌ ERROR: Failed to get OAuth token
```

**Causes:**
- Invalid client credentials
- Keycloak service unavailable
- Network connectivity issues

**Fix:** Verify secrets are correct in GitHub Settings

#### 2. Tenant Not Reachable
```
❌ ERROR: Cannot reach Atlan tenant
```

**Causes:**
- Smoke test passed but tenant degraded afterward
- Network issues (VPN disconnected)
- Ingress/LB issues

**Fix:** Check smoke test logs, verify tenant is actually healthy

#### 3. Test Timeout
```
✗ GlossaryTest FAILED (600s)
```

**Causes:**
- Tenant performance degraded
- Test hanging on API call
- Network latency

**Fix:** Review test logs, check tenant resource utilization

#### 4. Clone Failure
```
❌ ERROR: Failed to clone atlan-java repository
```

**Causes:**
- GitHub rate limiting
- Network issues
- Repository access issues

**Fix:** Retry workflow, check GitHub status page

#### 5. User-Related Test Failures
```
✗ GlossaryTest FAILED
✗ ConnectionTest FAILED  
✗ LineageTest FAILED
(but CustomMetadataTest PASSED)
```

**Causes:**
- `ATLAN_FIXED_USER` secret not configured
- Configured user doesn't exist in tenant
- User exists but is not verified/active
- User lacks required permissions

**Fix:**
1. Check if `VCLUSTER_AWS_FIXED_USER` secret is set in GitHub
2. Verify the user exists in the AWS tenant
3. Ensure user is verified and active
4. Confirm user has permissions for asset ownership/assignment operations

**To configure:**
```bash
# Settings → Secrets and variables → Actions → New repository secret
# Name: VCLUSTER_AWS_FIXED_USER
# Value: <username> (e.g., "admin" or "serviceaccount")
```

## Expansion Plans

### Phase 1 (Current)
- ✅ Test against AWS tenant only
- ✅ 5 critical path tests
- ✅ Informational only (non-blocking)

### Phase 2 (Future)
- 🔜 Add more tests (10-15 total)
- 🔜 Test against multiple tenants (Azure, GCP)
- 🔜 Parallel test execution
- 🔜 Test result trending dashboard

### Phase 3 (Future)
- 🔮 Block releases on critical test failures
- 🔮 Auto-bisect to find breaking commits
- 🔮 Performance regression detection
- 🔮 SDK compatibility matrix

## Related Documentation

- **Architecture Analysis**: `docs/ATLAN_JAVA_SDK_COMPATIBILITY.md`
- **Test Compatibility Matrix**: `docs/ATLAN_JAVA_COMPATIBLE_TESTS.md`
- **Smoke Tests**: `scripts/multi-cloud-smoke-test.sh`
- **CI Workflow**: `.github/workflows/maven.yml`

## Maintenance

### Updating Tests

To add/remove tests, edit `scripts/atlan-java-sdk-test.sh`:

```bash
TESTS=(
  "GlossaryTest"
  "CustomMetadataTest"
  "AtlanTagTest"
  "LineageTest"
  "SearchTest"
  # Add new test here:
  # "YourNewTest"
)
```

### Updating Credentials

Credentials are stored in GitHub Secrets (Settings → Secrets → Actions):

1. `VCLUSTER_AWS_ATLAN_DOMAIN`
2. `VCLUSTER_AWS_CLIENT_ID`
3. `VCLUSTER_AWS_CLIENT_SECRET`
4. `VCLUSTER_AWS_FIXED_USER` (optional - defaults to `chris` if not set)

To update: Settings → Secrets and variables → Actions → Update

**Note on FIXED_USER:**
- The SDK tests require a verified, active user for ownership/assignment operations
- If not configured, tests will use hardcoded username `chris` (may not exist in all tenants)
- Configure this to match a real user in your AWS tenant for better test coverage

### Changing Failure Policy

To make SDK tests **block releases**, edit `.github/workflows/maven.yml`:

```yaml
- name: Run SDK tests against AWS tenant
  id: sdk_tests
  continue-on-error: false  # ← Change from true to false
```

Then update `helm-publish` condition to check `needs.sdk-test.result == 'success'`.

## Questions?

Contact: Platform Team  
Slack: #platform-support  
Docs: This file

---

*Last Updated: November 6, 2024*

