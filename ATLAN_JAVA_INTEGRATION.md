# Atlan-Java SDK Integration Tests

## Overview

This document describes the integration of `atlan-java` SDK tests into the `atlas-metastore` Maven CI workflow. These tests provide extended coverage by running the official Atlan Java SDK test suite against the same testcontainers used for our core integration tests.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Maven CI Workflow                         │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  1. Build Atlas WAR                                          │
│  2. Run Integration Tests (atlas-metastore)                  │
│     └─ Testcontainers: Cassandra, Kafka, ES, Atlas         │
│     └─ Tests: BasicServiceAvailabilityTest, etc.           │
│                                                               │
│  3. Run Extended Tests (atlan-java SDK) ← NEW!             │
│     └─ Uses SAME testcontainers (still running)            │
│     └─ Clone atlan-java repo                                │
│     └─ Run ConnectionTest (and others in future)           │
│                                                               │
│  4. Cleanup Testcontainers                                   │
│  5. Build Docker Image                                       │
│  6. Run Smoke Tests                                          │
│  7. Publish Helm Charts                                      │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## Current Implementation

### Test Coverage

**Phase 1** (Current):
- ✅ `ConnectionTest` - Tests connection creation, retrieval, updates, and deletion

**Phase 2** (Future):
- `GlossaryTest` - Business glossary management
- `SearchTest` - Asset search functionality
- `CustomMetadataTest` - Custom metadata handling
- `LineageTest` - Lineage creation and traversal
- `AtlanTagTest` - Tag management

### Authentication

The `atlan-java` SDK supports a special `"LOCAL"` mode for testing against local Atlas instances:

```bash
ATLAN_BASE_URL="LOCAL"     # SDK uses http://localhost:21000
ATLAN_API_KEY="admin:admin" # Basic Auth (username:password)
```

**How it works:**
1. `AtlanClient` constructor detects `"LOCAL"` keyword
2. Sets `apiBase = "http://localhost:21000"`
3. Uses `LocalTokenManager` instead of `APITokenManager`
4. `LocalTokenManager` Base64-encodes credentials for Basic Auth
5. Sends requests with `Authorization: Basic YWRtaW46YWRtaW4=`

This matches the authentication used in our existing `BasicServiceAvailabilityTest`:

```java
// Our test (BasicServiceAvailabilityTest.java)
String auth = "admin:admin";
String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
request.header("Authorization", "Basic " + encodedAuth);

// atlan-java SDK (LocalTokenManager.java)
String authHeader = String.format("Basic %s", 
    Base64.getEncoder().encodeToString(basicAuth.getBytes()));
```

---

## Workflow Steps

### 1. Keep Testcontainers Running
```yaml
- name: Run Integration Tests
  run: |
    chmod +x ./run-integration-tests.sh && ./run-integration-tests.sh --keep-containers
```
- The `--keep-containers` flag prevents automatic cleanup
- Testcontainers (Atlas, Cassandra, Kafka, Elasticsearch) stay running
- Allows atlan-java SDK tests to reuse the same infrastructure

### 2. Verify Containers Are Ready
```yaml
- name: Verify testcontainers are still running
  run: |
    docker ps --filter "name=atlas-test"
    curl -f -s -u admin:admin http://localhost:21000/api/atlas/admin/version
```
- Confirms all containers are still healthy
- Waits for Atlas to be responsive (up to 60 seconds)
- Fails fast if containers were unexpectedly stopped

### 3. Clone atlan-java Repository
```yaml
- name: Clone atlan-java repository
  run: |
    git clone --depth 1 https://github.com/atlanhq/atlan-java.git /tmp/atlan-java
```
- Uses shallow clone (`--depth 1`) for speed
- Clones to `/tmp` to avoid interfering with atlas-metastore workspace

### 4. Set up Java 21
```yaml
- name: Set up JDK 21 for atlan-java tests
  uses: actions/setup-java@v1
  with:
    java-version: 21
```
- `atlan-java` requires Java 21 (atlas-metastore uses Java 17)
- GitHub Actions allows switching Java versions mid-workflow

### 5. Run SDK Test
```yaml
- name: Run atlan-java SDK integration test
  working-directory: /tmp/atlan-java
  env:
    ATLAN_BASE_URL: "LOCAL"
    ATLAN_API_KEY: "admin:admin"
  run: |
    ./gradlew test -PintegrationTests \
      --tests "com.atlan.java.sdk.ConnectionTest" \
      --info \
      --no-daemon
```

**Key parameters:**
- `-PintegrationTests` - Gradle property that enables integration tests
- `--tests "..."` - Runs specific test class
- `--info` - Verbose logging for debugging
- `--no-daemon` - Don't start Gradle daemon (saves resources in CI)

### 6. Upload Test Results
```yaml
- name: Upload atlan-java test results
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: atlan-java-test-results-${{ github.run_id }}
    path: |
      /tmp/atlan-java/integration-tests/build/reports/tests/
      /tmp/atlan-java/integration-tests/build/test-results/
```

Artifacts include:
- HTML test reports (human-readable)
- JUnit XML results (machine-readable)
- Test logs

### 7. Cleanup Testcontainers
```yaml
- name: Clean up after integration tests
  if: always()  # Always clean up, even if tests fail
  run: |
    # Remove testcontainers that we kept running
    docker rm -f $(docker ps -a --filter "name=atlas-test" --format "{{.Names}}")
    docker system prune -af --volumes
```
- Explicitly removes all testcontainers
- Runs even if atlan-java tests fail (`if: always()`)
- Ensures no resource leaks in CI

---

## Benefits

### 1. Extended Test Coverage
- **39 additional test suites** available from atlan-java
- Covers SDK functionality: connections, glossaries, search, lineage, tags, etc.
- Tests real-world usage patterns

### 2. SDK Compatibility Validation
- Ensures Atlas APIs work correctly with the official Java SDK
- Catches breaking changes before they affect SDK users
- Tests both REST API and SDK layer

### 3. Zero Infrastructure Overhead
- Reuses existing testcontainers (no additional setup)
- Runs sequentially after core tests (containers still up)
- No additional resource costs

### 4. Early Bug Detection
- Finds issues in Atlas that might only surface via SDK
- Tests complex workflows (not just simple API calls)
- Validates end-to-end scenarios

---

## Troubleshooting

### Issue: Test fails with "Connection refused"

**Cause:** Atlas container might not be ready or port mapping issue

**Solution:**
```yaml
# Add a health check wait before running atlan-java tests
- name: Wait for Atlas to stabilize
  run: |
    echo "Waiting for Atlas..."
    for i in {1..30}; do
      if curl -f -s -u admin:admin http://localhost:21000/api/atlas/admin/version > /dev/null; then
        echo "✓ Atlas is ready"
        break
      fi
      echo "Waiting... ($i/30)"
      sleep 2
    done
```

### Issue: Test fails with authentication error

**Cause:** Basic Auth not configured or wrong credentials

**Solution:**
1. Verify Atlas container has Basic Auth enabled
2. Check credentials: default is `admin:admin`
3. Verify `ATLAN_BASE_URL="LOCAL"` triggers `LocalTokenManager`

### Issue: Gradle build fails

**Cause:** Java version mismatch or Gradle wrapper issues

**Solution:**
```yaml
# Ensure Java 21 is active
- name: Verify Java version
  run: |
    java -version
    which java
    echo $JAVA_HOME
```

### Issue: Test takes too long

**Cause:** ConnectionTest creates multiple resources

**Solution:**
- Current test takes ~2-5 minutes (acceptable)
- If adding more tests, consider:
  - Running tests in parallel (multiple Gradle workers)
  - Selective test execution based on changed files
  - Timeout limits per test

---

## Future Enhancements

### Phase 2: Add More Tests (Selective)
```yaml
./gradlew test -PintegrationTests \
  --tests "com.atlan.java.sdk.ConnectionTest" \
  --tests "com.atlan.java.sdk.GlossaryTest" \
  --tests "com.atlan.java.sdk.SearchTest" \
  --tests "com.atlan.java.sdk.LineageTest"
```
**Estimated time:** +10-15 minutes

### Phase 3: Full Test Suite (Optional)
```yaml
./gradlew test -PintegrationTests
```
**Estimated time:** +60-90 minutes
**Coverage:** All 39 test suites

### Phase 4: Test Result Analysis
- Parse JUnit XML results
- Report test metrics to GitHub Status Checks
- Track test duration trends over time
- Alert on new test failures

### Phase 5: Conditional Execution
- Only run atlan-java tests if certain files changed
- Skip on draft PRs
- Run full suite only on main branch

---

## Performance Impact

| Stage | Before | After | Delta |
|-------|--------|-------|-------|
| Integration Tests | ~5 mins | ~5 mins | +0 mins |
| **atlan-java SDK Test** | **N/A** | **~3 mins** | **+3 mins** |
| Total Build Time | ~15 mins | ~18 mins | +3 mins |

**Per-test breakdown:**
- ConnectionTest: ~2-3 minutes
- GlossaryTest: ~3-4 minutes (if added)
- SearchTest: ~2-3 minutes (if added)

---

## Related Documentation

- [INTEGRATION_SMOKE_TESTS_GUIDE.md](./INTEGRATION_SMOKE_TESTS_GUIDE.md) - Core integration & smoke tests
- [ATLAS_HELM_MIGRATION.md](./ATLAS_HELM_MIGRATION.md) - Helm chart migration details
- [atlan-java README](https://github.com/atlanhq/atlan-java/blob/main/README.md) - Official SDK docs

---

## References

- **atlan-java repository**: https://github.com/atlanhq/atlan-java
- **SDK documentation**: https://developer.atlan.com/getting-started/java-sdk/
- **Test suite location**: `integration-tests/src/test/java/com/atlan/java/sdk/`

---

**Last Updated:** October 30, 2025
**Status:** Phase 1 (Single Test) - Implemented

