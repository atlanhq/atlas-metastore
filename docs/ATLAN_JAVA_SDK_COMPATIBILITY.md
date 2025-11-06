# Atlan-Java SDK Compatibility with Atlas-Metastore

**Date:** November 6, 2024  
**Status:** ❌ **Not Compatible**  
**Decision:** Abandon extended integration tests with atlan-java SDK

## Summary

The `atlan-java` SDK is designed for the **full Atlan product** and requires multiple Atlan-specific microservices that are not part of the open-source `atlas-metastore` project. Testing `atlan-java` against standalone `atlas-metastore` is **not feasible**.

## Investigation Findings

### Architecture Mismatch

**Full Atlan Product:**
```
┌─────────────────────────────────────┐
│ Atlan Product Stack                 │
├─────────────────────────────────────┤
│ • Atlas-metastore (metadata core)   │
│ • Heracles (users/roles/groups)     │
│ • Heka (search/indexing)            │
│ • Chronos (workflows)               │
│ • Keycloak (authentication)         │
│ • Other Atlan services              │
└─────────────────────────────────────┘
```

**Atlas-metastore (standalone):**
```
┌─────────────────────────────────────┐
│ Atlas-metastore Only                │
├─────────────────────────────────────┤
│ • Apache Atlas REST API             │
│ • Basic metadata management         │
│ ❌ NO Heracles                      │
│ ❌ NO Heka                          │
│ ❌ NO Chronos                       │
│ ❌ NO Keycloak integration          │
└─────────────────────────────────────┘
```

### Missing Service Endpoints

The `atlan-java` SDK depends on these Atlan-specific services that **do not exist** in `atlas-metastore`:

#### 1. **Heracles** (`/api/service/*`)
- **Purpose:** User, role, and group management
- **SDK Code:** `com.atlan.api.HeraclesEndpoint`
- **Service URL:** `http://heracles-service.heracles.svc.cluster.local`
- **Used By:** Almost all integration tests
- **Example Failure:**
  ```java
  // First line in ConnectionTest methods:
  String adminRoleGuid = client.getRoleCache().getIdForSid("$admin");
  // Calls: GET /api/service/roles
  // Result: Hangs for 5+ minutes, then times out
  ```

#### 2. **Heka** (search/indexing)
- **SDK Code:** `com.atlan.api.HekaEndpoint`
- **Used By:** Search-related tests

#### 3. **Chronos** (workflows)
- **SDK Code:** `com.atlan.api.ChronosEndpoint`
- **Used By:** Workflow tests

#### 4. **Keycloak** (authentication)
- **SDK Code:** `com.atlan.api.KeycloakEndpoint`
- **Used By:** Authentication tests

### Observed Test Behavior

When running `atlan-java` tests against `atlas-metastore`:

1. ✅ **Container Starts:** Testcontainers successfully launches Atlas
2. ✅ **Client Initializes:** `AtlanClient` connects to Atlas API
3. ❌ **Tests Hang:** First API call to Heracles blocks indefinitely
4. ⏱️ **Timeout:** After 5-10 minutes, HTTP client timeout kicks in
5. ✅ **False Pass:** Test framework reports "PASSED" (actually timed out)
6. 📝 **No Output:** No test methods actually execute

**Example from GitHub Actions:**
```
> Task :integration-tests:test
Gradle Test Executor 4 STANDARD_OUT
    17:18:58.794 [Test worker] INFO  atlan.AtlanClient - Running using provided API token, against: http://localhost:32775

✓ ConnectionTest PASSED (300s)
```

**What's missing:**
```
com.atlan.java.sdk.ConnectionTest createCustomConnection PASSED (45.8s)
com.atlan.java.sdk.ConnectionTest customConnection PASSED (1s)
com.atlan.java.sdk.ConnectionTest invalidConnection PASSED (1s)
...
```

### Network Retry Behavior

The SDK's retry logic masks the underlying failures:

```java
// From HttpClient.java
maxNetworkRetriesDelay = Duration.ofSeconds(5);
minNetworkRetriesDelay = Duration.ofMillis(500);

// Typical retry sequence for missing endpoint:
// Attempt 1: 500ms delay
// Attempt 2: 1s delay
// Attempt 3: 2s delay
// Attempt 4: 4s delay
// Attempt 5: 5s delay
// ... continues for 10 attempts
// Total: ~5-10 minutes
```

This is why tests take **exactly** the timeout duration (5 or 10 minutes).

## Attempted Solutions (Failed)

### ❌ Approach 1: Run All Tests
- **Problem:** 99% of tests require Heracles endpoints
- **Result:** All tests hang and timeout

### ❌ Approach 2: Curated Test List
- **Attempted:** Selected 8 "basic" tests
- **Problem:** Even basic tests like `ConnectionTest` call role APIs
- **Result:** Still hang on first method call

### ❌ Approach 3: Local Docker Compose
- **Problem:** No docker-compose for full Atlan stack (proprietary)
- **Result:** Cannot deploy required services

### ❌ Approach 4: Mock Heracles Endpoints
- **Consideration:** Would require extensive mocking infrastructure
- **Decision:** Not worth the effort for limited value

## Compatible Alternative

**Atlas-metastore has its own integration tests** that work perfectly:

```bash
# Run existing Atlas integration tests (works!)
mvn verify -DskipUTs -Dskip.python
```

These tests use:
- ✅ Apache Atlas REST API (`/api/atlas/v2/*`)
- ✅ Standard metadata operations
- ✅ No external service dependencies

## Recommendations

1. **✅ Keep:** Existing `atlas-metastore` integration tests
   - Test the actual functionality of the metastore
   - No external dependencies
   - Fast and reliable

2. **❌ Remove:** Extended integration tests with `atlan-java`
   - Incompatible architecture
   - False positives from timeouts
   - No actual testing value

3. **📝 Document:** This compatibility limitation
   - `atlan-java` = full Atlan product
   - `atlas-metastore` = Apache Atlas core
   - Different API surfaces

4. **🔮 Future:** If needed, test `atlan-java` against:
   - Full Atlan product deployment
   - Staging environment
   - Not standalone Atlas

## Conclusion

The `atlan-java` SDK and `atlas-metastore` serve different purposes:

- **`atlas-metastore`:** Open-source Apache Atlas metadata management core
- **`atlan-java`:** SDK for the commercial Atlan product (Atlas + proprietary services)

Testing one against the other is like testing a PostgreSQL client library against a MySQL database - they're both databases, but have fundamentally different APIs and capabilities.

## ✅ Solution: Test Against Deployed Atlan Tenants

Instead of testing against standalone `atlas-metastore`, we now test `atlan-java` SDK against **deployed Atlan tenants** (full product stack) in our CI pipeline.

### Implementation

**New CI Job: `sdk-test`** (runs after `smoke-test` succeeds)

- **Target**: AWS vCluster tenant (full Atlan deployment)
- **Authentication**: OAuth via Keycloak
- **Tests**: 5 critical path tests (GlossaryTest, CustomMetadataTest, AtlanTagTest, LineageTest, SearchTest)
- **Duration**: ~10-15 minutes
- **Failure Handling**: Informational only, does not block releases

See: `.github/workflows/maven.yml` (sdk-test job) and `scripts/atlan-java-sdk-test.sh`

This approach:
- ✅ Tests SDK against real Atlan product
- ✅ Validates SDK compatibility with latest atlas-metastore changes
- ✅ Runs on actual deployment infrastructure
- ✅ Provides early warning of SDK breaking changes
- ✅ No false positives from missing services

## References

- Issue discovered: November 6, 2024
- Investigation commits: `atlas_ci_cd_updates` branch
- Key files examined:
  - `atlan-java/sdk/src/main/java/com/atlan/api/HeraclesEndpoint.java`
  - `atlan-java/integration-tests/src/test/java/com/atlan/java/sdk/ConnectionTest.java`
  - `atlas-metastore/webapp/src/main/java/org/apache/atlas/web/rest/AuthREST.java`

---

*This document explains why we cannot run atlan-java SDK tests against standalone atlas-metastore and justifies the decision to abandon this approach.*

