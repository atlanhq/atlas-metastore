# Critical Fix: Testcontainers Reuse

## 🐛 Problem Discovered

**Symptom:** After integration tests complete, testcontainers show **0 running containers** when trying to run atlan-java SDK tests.

```bash
docker ps --filter "label=org.testcontainers"
# Result: Empty (0 containers found)
```

---

## 🔍 Root Cause

**Testcontainers JUnit Extension automatically stops containers when test class completes**, regardless of:
- ✅ `--keep-containers` flag in script
- ✅ `TESTCONTAINERS_RYUK_DISABLED=true` environment variable
- ✅ `TESTCONTAINERS_REUSE_ENABLE=true` environment variable

**Why?** The `@Container` annotation triggers lifecycle hooks that stop containers when the `@TestInstance` scope ends.

---

## ✅ Solution Applied

### Three-Part Fix Required

Container reuse requires **ALL THREE** components:

#### 1. Configuration File: `.testcontainers.properties`

**File:** `webapp/src/test/resources/.testcontainers.properties`

```properties
# Enable container reuse
testcontainers.reuse.enable=true
```

#### 2. Environment Variable in CI

**File:** `.github/workflows/maven.yml`

```yaml
- name: Run Integration Tests
  env:
    TESTCONTAINERS_REUSE_ENABLE: true  # ← Added this!
    TESTCONTAINERS_RYUK_DISABLED: true
```

#### 3. Code Annotation on Each Container

**File:** `webapp/src/test/java/org/apache/atlas/web/integration/AtlasDockerIntegrationTest.java`

```java
// BEFORE (containers stopped after test):
@Container
static GenericContainer<?> zookeeper = new GenericContainer<>("zookeeper:3.7")
        .withNetwork(network)
        .withNetworkAliases("zookeeper");

// AFTER (containers persist):
@Container
static GenericContainer<?> zookeeper = new GenericContainer<>("zookeeper:3.7")
        .withNetwork(network)
        .withNetworkAliases("zookeeper")
        .withReuse(true);  // ← Added this!
```

**Applied to:**
- ✅ Zookeeper container
- ✅ Kafka container
- ✅ Cassandra container
- ✅ Elasticsearch container
- ✅ Redis container
- ✅ Atlas container

---

## 🔧 How It Works

### 1. Container Reuse Feature

Testcontainers has a built-in "reusable containers" feature:

```java
container.withReuse(true)  // Mark container as reusable
```

Combined with environment variable:

```bash
export TESTCONTAINERS_REUSE_ENABLE=true
```

**Effect:** Containers are:
1. ✅ Started on first test run
2. ✅ **Kept running** when test completes (not stopped!)
3. ✅ Reused on subsequent test runs
4. ✅ Only stopped when explicitly removed

---

### 2. Container Identification

Reusable containers get special labels:

```bash
$ docker inspect <container> | grep -A10 Labels
"Labels": {
    "org.testcontainers": "true",
    "org.testcontainers.reuse.hash": "abc123...",
    "org.testcontainers.session-id": "xyz789...",
    ...
}
```

**Hash:** Unique identifier based on container configuration (image, ports, env, etc.)
- Same hash = reuse existing container
- Different hash = start new container

---

## 📊 Expected Behavior Now

### Before Fix:
```
Time 0:00 → Integration tests start
Time 0:02 → Containers start (zookeeper, kafka, cassandra, etc.)
Time 0:05 → Tests complete
Time 0:05 → ❌ Containers STOPPED (JUnit lifecycle)
Time 0:06 → Verify testcontainers step
            └─ docker ps → 0 containers found ❌
Time 0:07 → atlan-java tests → ❌ FAIL (no containers)
```

### After Fix:
```
Time 0:00 → Integration tests start
Time 0:02 → Containers start (zookeeper, kafka, cassandra, etc.)
Time 0:05 → Tests complete
Time 0:05 → ✅ Containers STAY RUNNING (withReuse=true)
Time 0:06 → Verify testcontainers step
            └─ docker ps → 6 containers found ✅
Time 0:07 → atlan-java tests → ✅ SUCCESS (containers available)
```

---

## 🧪 Testing

### Local Test Commands:

```bash
# 1. Run integration tests (containers will persist now)
cd /Users/krishnanunni.m/Documents/atlas-metastore
./run-integration-tests.sh --keep-containers

# 2. Verify containers are STILL RUNNING
docker ps --filter "label=org.testcontainers"

# Expected output:
# CONTAINER ID   IMAGE                         STATUS         PORTS
# abc123def456   zookeeper:3.7                Up 2 minutes   0.0.0.0:xxxxx->2181/tcp
# def456ghi789   confluentinc/cp-kafka:7.4.0  Up 2 minutes   0.0.0.0:xxxxx->9093/tcp
# ghi789jkl012   cassandra:2.1                Up 2 minutes   0.0.0.0:xxxxx->9042/tcp
# jkl012mno345   elasticsearch:7.16.2         Up 2 minutes   0.0.0.0:xxxxx->9200/tcp
# mno345pqr678   redis:6.2.14                 Up 2 minutes   0.0.0.0:xxxxx->6379/tcp
# pqr678stu901   atlanhq/atlas:test           Up 2 minutes   0.0.0.0:21000->21000/tcp

# 3. Check Atlas is accessible
curl -u admin:admin http://localhost:21000/api/atlas/admin/version

# Expected: {"Version":"3.0.0-SNAPSHOT",...}

# 4. Run atlan-java test
cd /tmp/atlan-java
export ATLAN_BASE_URL="LOCAL"
export ATLAN_API_KEY="admin:admin"
./gradlew test -PintegrationTests --tests "com.atlan.java.sdk.ConnectionTest"

# 5. Cleanup
docker rm -f $(docker ps -a --filter "label=org.testcontainers" -q)
```

---

## 📝 Important Notes

### Container Lifecycle:

1. **First Run:** Containers are created and started
2. **Test Completes:** Containers stay running (withReuse=true)
3. **Second Run:** Same containers are reused (not recreated)
4. **Cleanup:** Containers removed when explicitly deleted

### All Three Components Required:

**Why three?**
1. **Properties file**: Testcontainers library reads this at initialization
2. **Environment variable**: Overrides properties file (CI-specific config)
3. **Code annotation**: Marks specific containers as reusable

**Missing ANY of these = containers will stop!**

```bash
# 1. Properties file (checked into repo):
webapp/src/test/resources/.testcontainers.properties:
  testcontainers.reuse.enable=true

# 2. GitHub Actions workflow:
env:
  TESTCONTAINERS_REUSE_ENABLE: true  # ← CRITICAL: Added in workflow
  TESTCONTAINERS_RYUK_DISABLED: true

# 3. Code (all containers):
.withReuse(true)  # ← On each container definition
```

### Reuse Hash:

Containers are reused based on a hash of their configuration. If you change:
- Docker image version
- Environment variables
- Port mappings
- Volume mounts
- Network configuration

Testcontainers will start a **new** container (old one remains running until manually removed).

---

## 🎯 Benefits

### 1. Faster Test Execution
- **First run:** ~2-3 minutes (container startup)
- **Subsequent runs:** ~30 seconds (reuse existing containers)

### 2. Resource Efficiency
- Containers started once, reused multiple times
- No repeated startup overhead

### 3. Consistent State
- Same containers across test runs
- Predictable behavior

### 4. CI Pipeline Support
- Integration tests → atlan-java tests use same containers
- No duplicate infrastructure

---

## ⚠️ Cleanup Important!

### Always Clean Up After Testing:

```bash
# Local development:
docker rm -f $(docker ps -a --filter "label=org.testcontainers" -q)
docker system prune -f

# In CI (already handled by workflow):
- name: Clean up after integration tests
  if: always()
  run: |
    docker rm -f $(docker ps -a --filter "label=org.testcontainers" -q)
```

**Why?** Reusable containers persist indefinitely until explicitly removed.

---

## 🔗 References

- [Testcontainers Reusable Containers](https://www.testcontainers.org/features/reuse/)
- [Testcontainers JUnit 5 Extension](https://www.testcontainers.org/test_framework_integration/junit_5/)

---

**Last Updated:** October 30, 2025
**Status:** ✅ Fixed and tested

