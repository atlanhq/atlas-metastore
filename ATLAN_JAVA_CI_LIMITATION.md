# Atlan-Java SDK Tests - CI Limitation

## ❌ Problem: Testcontainers Don't Persist Across JVM Processes

### What We Tried

We attempted to run atlan-java SDK integration tests in CI by:
1. Running atlas-metastore integration tests (starts testcontainers)
2. Keeping containers alive with `.withReuse(true)`
3. Running atlan-java SDK tests against those same containers

### Why It Doesn't Work

**Testcontainers' `.withReuse(true)` is NOT designed for CI pipelines with separate processes.**

```
Maven Integration Tests (Process 1)
├─ JVM starts
├─ Testcontainers starts containers
├─ Tests run
├─ JVM exits → Testcontainers shutdown hooks run
└─ ❌ Containers stopped (despite .withReuse(true))

Gradle Atlan-Java Tests (Process 2)
├─ New JVM starts
├─ No containers running!
└─ ❌ Tests fail
```

**Root Cause:**
- `.withReuse(true)` only works for multiple test runs in the SAME JVM session
- When the JVM exits, Testcontainers' shutdown hooks **always** stop containers
- This is by design - Testcontainers manages container lifecycle tied to JVM lifecycle
- Separate CI steps = separate JVMs = containers can't persist

### What We Implemented

All three required components were correctly implemented:

1. ✅ **Code**: `.withReuse(true)` on all 6 containers
2. ✅ **Config**: `.testcontainers.properties` with `testcontainers.reuse.enable=true`
3. ✅ **Workflow**: `TESTCONTAINERS_REUSE_ENABLE=true` environment variable

But it still doesn't work because of the JVM process boundary.

---

## ✅ Alternative Solutions

### Option 1: Manual Docker Containers (Complex)
- Start containers with plain `docker run` commands
- Don't use Testcontainers lifecycle management
- Manually configure all networking, volumes, env vars
- **Downside**: Loses Testcontainers' benefits (auto-cleanup, wait strategies, etc.)

### Option 2: Single Test Process (Best for CI)
- Integrate atlan-java test classes into atlas-metastore Maven project
- Run all tests in single Maven execution
- Share same JVM = share same testcontainers
- **Downside**: Tightly couples two repositories

### Option 3: Dedicated Test Environment
- Deploy Atlas to a persistent test environment (vCluster, staging, etc.)
- Run atlan-java SDK tests against that environment
- **Downside**: Not true "integration test" - depends on external environment

### Option 4: Local Development Only
- Keep atlan-java integration for **local development workflow**
- Developers run: `./run-integration-tests.sh --keep-containers` + atlan-java tests
- Skip in CI, accept reduced coverage
- **Downside**: No automated SDK validation in CI

---

## 🎯 Recommended Approach: Option 4 (Local Only)

For now, **run atlan-java SDK tests locally only**:

```bash
# Terminal 1: Start integration tests and keep containers
cd /path/to/atlas-metastore
./run-integration-tests.sh --keep-containers

# Terminal 2: Run atlan-java SDK tests (while containers still running)
cd /tmp
git clone https://github.com/atlanhq/atlan-java.git
cd atlan-java
export ATLAN_BASE_URL="LOCAL"
export ATLAN_API_KEY="admin:admin"
./gradlew test -PintegrationTests --tests "com.atlan.java.sdk.ConnectionTest"

# Terminal 1: Cleanup when done
docker rm -f $(docker ps -a --filter "label=org.testcontainers" -q)
```

**Benefits:**
- ✅ Works reliably in local development
- ✅ Provides extended test coverage for developers
- ✅ No complex CI infrastructure needed
- ✅ Simple to understand and maintain

**Trade-off:**
- ⚠️ Not automated in CI (manual validation required)

---

## 📚 References

- [Testcontainers Reusable Containers](https://www.testcontainers.org/features/reuse/) - "Designed for local development"
- [GitHub Issue: Reuse across different processes](https://github.com/testcontainers/testcontainers-java/issues/1781)

---

**Status**: Atlan-java SDK integration **disabled in CI** (commented out in maven.yml)
**Local testing**: Still fully supported and recommended
