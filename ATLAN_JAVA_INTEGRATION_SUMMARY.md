# Atlan-Java Integration - Implementation Summary

## 🎯 Problems Solved

### Problem 1: Testcontainers Cleanup

**Issue:** Testcontainers were being cleaned up immediately after `run-integration-tests.sh` completed, preventing atlan-java SDK tests from reusing them.

**Root Cause:** The script has cleanup logic at line 154-156:
```bash
if [ "$KEEP_CONTAINERS" = false ]; then
    docker rm -f $(docker ps -a --filter "name=atlas-test" --format "{{.Names}}")
fi
```

**Solution:** Use `--keep-containers` flag to prevent automatic cleanup.

---

### Problem 2: Container Naming

**Issue:** Testcontainers generates **random container names** like:
- `testcontainers-kafka-a1b2c3d4`
- `testcontainers-cassandra-e5f6g7h8`  
- `testcontainers-atlas-i9j0k1l2`

The filter `--filter "name=atlas-test"` wouldn't match anything!

**Root Cause:** Testcontainers doesn't use predictable names - it generates random names for each container.

**Solution:** Use the `org.testcontainers` label that Testcontainers automatically adds to all containers:
```bash
# Find all testcontainers:
docker ps --filter "label=org.testcontainers"

# Remove all testcontainers:
docker rm -f $(docker ps -a --filter "label=org.testcontainers" -q)
```

---

## ✅ Solution Implemented

### 1. Keep Containers Running
**File:** `.github/workflows/maven.yml` line 174

```yaml
# BEFORE:
chmod +x ./run-integration-tests.sh && ./run-integration-tests.sh

# AFTER:
chmod +x ./run-integration-tests.sh && ./run-integration-tests.sh --keep-containers
```

**Effect:** Testcontainers persist after integration tests complete

---

### 2. Verify Containers Are Ready
**File:** `.github/workflows/maven.yml` lines 201-240

```yaml
- name: Verify testcontainers are still running
  run: |
    # List running testcontainers (they have org.testcontainers label)
    docker ps --filter "label=org.testcontainers"
    
    # Count testcontainers
    CONTAINER_COUNT=$(docker ps --filter "label=org.testcontainers" -q | wc -l)
    echo "Found $CONTAINER_COUNT testcontainers running"
    
    # Check Atlas accessibility
    for i in {1..30}; do
      if curl -f -s -u admin:admin http://localhost:21000/api/atlas/admin/version; then
        echo "✓ Atlas is accessible on port 21000"
        break
      fi
      sleep 2
    done
```

**Note:** Testcontainers automatically adds the `org.testcontainers` label to all containers it creates. Container names are random (e.g., `testcontainers-kafka-a1b2c3`).

**Effect:** Confirms containers are healthy before running atlan-java tests

---

### 3. Run Atlan-Java Tests
**File:** `.github/workflows/maven.yml` lines 229-262

```yaml
- name: Clone atlan-java repository
  run: git clone --depth 1 https://github.com/atlanhq/atlan-java.git /tmp/atlan-java

- name: Set up JDK 21
  uses: actions/setup-java@v1
  with:
    java-version: 21

- name: Run atlan-java SDK integration test
  env:
    ATLAN_BASE_URL: "LOCAL"      # SDK uses http://localhost:21000
    ATLAN_API_KEY: "admin:admin" # Basic Auth
  run: |
    ./gradlew test -PintegrationTests \
      --tests "com.atlan.java.sdk.ConnectionTest" \
      --no-daemon
```

**Effect:** Runs ConnectionTest against existing testcontainers

---

### 4. Explicit Cleanup
**File:** `.github/workflows/maven.yml` lines 306-328

```yaml
- name: Clean up after integration tests
  if: always()  # Run even if atlan-java tests fail
  run: |
    # Remove testcontainers using label filter
    TESTCONTAINER_IDS=$(docker ps -a --filter "label=org.testcontainers" -q)
    if [ -n "$TESTCONTAINER_IDS" ]; then
      docker rm -f $TESTCONTAINER_IDS
    fi
    docker system prune -af --volumes
```

**Effect:** Ensures containers are removed, even if tests fail. Uses `label=org.testcontainers` to find all testcontainers regardless of their random names.

---

## 🔄 Complete Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Build Atlas WAR                                              │
│    └─ mvn clean install                                         │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. Run Integration Tests (atlas-metastore)                      │
│    └─ ./run-integration-tests.sh --keep-containers  ← MODIFIED │
│    └─ Testcontainers START and STAY RUNNING ✅                 │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. Verify Testcontainers                            ← NEW       │
│    └─ docker ps --filter "name=atlas-test"                     │
│    └─ curl http://localhost:21000 (health check)               │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ 4. Clone atlan-java Repository                      ← NEW       │
│    └─ git clone --depth 1 atlan-java /tmp/atlan-java           │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ 5. Switch to Java 21                                ← NEW       │
│    └─ actions/setup-java@v1 with java-version: 21              │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ 6. Run atlan-java ConnectionTest                    ← NEW       │
│    └─ ATLAN_BASE_URL="LOCAL"                                   │
│    └─ ATLAN_API_KEY="admin:admin"                              │
│    └─ ./gradlew test -PintegrationTests                        │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ 7. Cleanup Testcontainers                           ← MODIFIED │
│    └─ docker rm -f atlas-test-*                                │
│    └─ docker system prune -af                                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ 8. Build Docker Image                                           │
│ 9. Run Smoke Tests                                              │
│ 10. Publish Helm Charts                                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 Timeline

| Time | Step | Duration |
|------|------|----------|
| 0:00 | Build Atlas WAR | ~3 mins |
| 0:03 | Integration Tests (atlas-metastore) | ~2 mins |
| 0:05 | **Verify Testcontainers** | **~10 secs** |
| 0:05 | **Clone atlan-java** | **~5 secs** |
| 0:05 | **Setup Java 21** | **~10 secs** |
| 0:06 | **Run ConnectionTest** | **~2-3 mins** |
| 0:09 | **Cleanup** | **~10 secs** |
| 0:09 | Build Docker Image | ~5 mins |
| 0:14 | Smoke Tests | ~3 mins |
| 0:17 | Publish Helm Charts | ~1 min |

**Total Build Time:** ~18 minutes (+3 mins from before)

---

## 🔍 Key Implementation Details

### Authentication Works Out of the Box

**Your Current Tests:**
```java
// BasicServiceAvailabilityTest.java
String auth = "admin:admin";
String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
request.header("Authorization", "Basic " + encodedAuth);
```

**atlan-java SDK (LOCAL mode):**
```java
// AtlanClient.java line 284-287
if (baseURL.equals("LOCAL")) {
    apiBase = "http://localhost:21000";
}

// LocalTokenManager.java line 17-24
public LocalTokenManager(String basicAuth) {
    super(Base64.getEncoder()
        .encodeToString(basicAuth.getBytes())
        .toCharArray());
}

// LocalTokenManager.java line 28-30
protected String getAuthHeader() {
    return String.format("Basic %s", new String(token));
}
```

**Result:** Both use identical `Authorization: Basic YWRtaW46YWRtaW4=` header! ✅

---

## ✅ Testing Checklist

### Local Testing (Before CI)

1. **Run integration tests with keep flag:**
   ```bash
   cd /Users/krishnanunni.m/Documents/atlas-metastore
   ./run-integration-tests.sh --keep-containers
   ```

2. **In another terminal, verify containers:**
   ```bash
   docker ps --filter "label=org.testcontainers"
   # Should show: kafka, cassandra, elasticsearch, redis, atlas, zookeeper
   # Names will be random like: testcontainers-kafka-abc123
   ```

3. **Test atlan-java manually:**
   ```bash
   cd /tmp
   git clone https://github.com/atlanhq/atlan-java.git
   cd atlan-java
   export ATLAN_BASE_URL="LOCAL"
   export ATLAN_API_KEY="admin:admin"
   ./gradlew test -PintegrationTests --tests "com.atlan.java.sdk.ConnectionTest"
   ```

4. **Cleanup:**
   ```bash
   docker rm -f $(docker ps -a --filter "label=org.testcontainers" -q)
   ```

### CI Testing

1. **Push changes:**
   ```bash
   git add .github/workflows/maven.yml ATLAN_JAVA_INTEGRATION*.md
   git commit -m "feat: integrate atlan-java ConnectionTest with testcontainers"
   git push origin <branch>
   ```

2. **Monitor CI logs for:**
   - ✅ "Testcontainers are ready for atlan-java SDK tests"
   - ✅ "✓ atlan-java SDK test completed"
   - ✅ Artifacts uploaded successfully

---

## 🐛 Troubleshooting

### Issue: Containers not found

**Symptom:**
```
docker ps --filter "label=org.testcontainers"
# Empty result
```

**Cause:** Containers were cleaned up despite `--keep-containers` flag

**Solution:** 
1. Check `run-integration-tests.sh` line 154 - ensure `KEEP_CONTAINERS=true`
2. Verify testcontainers were actually started:
   ```bash
   # List ALL containers (including stopped)
   docker ps -a
   ```

---

### Issue: Atlas not accessible

**Symptom:**
```
curl: (7) Failed to connect to localhost port 21000: Connection refused
```

**Cause:** Atlas container stopped or port mapping failed

**Solution:**
```bash
# Find the Atlas container
ATLAS_CONTAINER=$(docker ps --filter "label=org.testcontainers" --format "{{.Names}}" | grep atlas)

# Check container status
docker ps -a --filter "name=$ATLAS_CONTAINER"

# Check logs
docker logs $ATLAS_CONTAINER
```

---

### Issue: Authentication failed

**Symptom:**
```
com.atlan.exception.AuthenticationException: 401 Unauthorized
```

**Cause:** Basic Auth not working or wrong credentials

**Solution:**
1. Verify Atlas has Basic Auth enabled
2. Check credentials: `admin:admin`
3. Test manually: `curl -u admin:admin http://localhost:21000/api/atlas/admin/version`

---

### Issue: Java version conflict

**Symptom:**
```
Unsupported class file major version 65
```

**Cause:** atlan-java requires Java 21, but Java 17 is active

**Solution:**
Verify Java 21 setup step runs before atlan-java tests:
```yaml
- name: Set up JDK 21 for atlan-java tests
  uses: actions/setup-java@v1
  with:
    java-version: 21
```

---

## 📈 Future Enhancements

### Phase 2: Add More Tests
```yaml
./gradlew test -PintegrationTests \
  --tests "com.atlan.java.sdk.ConnectionTest" \
  --tests "com.atlan.java.sdk.GlossaryTest" \
  --tests "com.atlan.java.sdk.SearchTest"
```
**Impact:** +5-10 minutes CI time

### Phase 3: Parallel Test Execution
```yaml
./gradlew test -PintegrationTests \
  --parallel \
  --max-workers=4
```
**Impact:** Reduce test time by 30-50%

### Phase 4: Conditional Execution
Only run atlan-java tests if certain files changed:
```yaml
- name: Check for SDK-relevant changes
  id: check_changes
  run: |
    if git diff --name-only HEAD~1 | grep -E "(webapp/|intg/)"; then
      echo "run_sdk_tests=true" >> $GITHUB_OUTPUT
    fi

- name: Run atlan-java SDK integration test
  if: steps.check_changes.outputs.run_sdk_tests == 'true'
  # ...
```

---

## 📚 Related Files

- `.github/workflows/maven.yml` - Main CI workflow (modified)
- `run-integration-tests.sh` - Integration test runner (uses `--keep-containers` flag)
- `ATLAN_JAVA_INTEGRATION.md` - Detailed documentation
- `webapp/src/test/java/.../BasicServiceAvailabilityTest.java` - Existing integration tests

---

**Last Updated:** October 30, 2025
**Status:** ✅ Ready for testing

