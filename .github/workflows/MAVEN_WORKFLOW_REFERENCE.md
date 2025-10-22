# Maven Workflow Technical Reference

**File:** `.github/workflows/maven.yml`  
**Purpose:** CI/CD pipeline with integration tests, Docker build, and multi-cloud smoke testing  
**Trigger:** Push to specific branches  
**Runner:** GitHub-hosted `ubuntu-latest`  

---

## Table of Contents

- [Workflow Overview](#workflow-overview)
- [Triggers](#triggers)
- [Jobs](#jobs)
  - [Job: build](#job-build)
  - [Job: smoke-test](#job-smoke-test)
- [Environment Variables](#environment-variables)
- [Secrets](#secrets)
- [Variables](#variables)
- [Outputs](#outputs)
- [Dependencies](#dependencies)
- [Artifacts](#artifacts)
- [Security](#security)
- [Troubleshooting](#troubleshooting)

---

## Workflow Overview

```yaml
name: Maven CI/CD with Multi-Cloud Smoke Tests
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    steps: [...]
  
  smoke-test:
    needs: build
    runs-on: ubuntu-latest
    steps: [...]
```

**Architecture:**
```
Developer Push
     ↓
Build Job (15-20 min)
  - Maven build
  - Integration tests
  - Docker build & push
     ↓
Smoke Test Job (10-12 min)
  - VPN connection
  - vCluster connections
  - Parallel cloud tests
```

### Detailed Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Developer Push to Branch                        │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  JOB 1: BUILD (ubuntu-latest)                          ~15-20 min   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. Setup                                                           │
│     ├─ Checkout code                                                │
│     ├─ Set up Docker + Buildx                                       │
│     ├─ Set up JDK 17                                                │
│     └─ Cache Maven dependencies                                     │
│                                                                     │
│  2. Build                                                           │
│     ├─ Create Maven settings (GitHub auth)                          │
│     └─ Build with Maven (./build.sh)                                │
│                                                                     │
│  3. Integration Tests                                               │
│     ├─ Check & cleanup disk space                                   │
│     ├─ Run Testcontainers tests                                     │
│     │  ├─ Zookeeper container                                       │
│     │  ├─ Kafka container                                           │
│     │  ├─ Cassandra container                                       │
│     │  ├─ Elasticsearch container                                   │
│     │  ├─ Redis container (simple, no Sentinel)                     │
│     │  └─ Atlas container (SPRING_PROFILES_ACTIVE=local)            │
│     ├─ On FAIL: Capture logs + tmate SSH                            │
│     └─ Cleanup containers                                           │
│                                                                     │
│  4. Docker Build & Push                                             │
│     ├─ Set up QEMU (multi-arch)                                     │
│     ├─ Login to GitHub Registry (ghcr.io)                           │
│     ├─ Build & push (linux/amd64, linux/arm64)                      │
│     │  Tag: ghcr.io/atlanhq/atlas-metastore-BRANCH:COMMIT_ID        │
│     ├─ Scan with Trivy                                              │
│     └─ Upload scan results                                          │
│                                                                     │
│  ✓ Job Complete                                                     │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  JOB 2: MULTI-CLOUD SMOKE TEST (ubuntu-latest)     ~10-12 min       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. Setup (Shared - 1 minute)                                       │
│     ├─ Checkout code                                                │
│     ├─ Calculate image name from build                              │
│     ├─ Install kubectl                                              │
│     ├─ Install vCluster CLI                                         │
│     └─ Install jq                                                   │
│                                                                     │
│  2. VPN Connection (Shared - 30 seconds)                            │
│     ├─ Install OpenConnect                                          │
│     ├─ Connect to GlobalProtect VPN (single connection)             │
│     │  Uses default DTLS/ESP (works for AWS)                        │
│     ├─ Configure routing (172.17.0.0/16 → tun0)                     │
│     └─ Verify connectivity to vCluster Platform                     │
│                                                                     │
│  3. vCluster Platform Login (Shared - 5 seconds)                    │
│     └─ Login with access key                                        │
│                                                                     │
│  4. Connect to All vClusters (Shared - 15 seconds)                  │
│     ├─ Connect to AWS (hkmeta02) → kubeconfig-aws.yaml              │
│     ├─ Connect to Azure (enpla1cp21) → kubeconfig-azure.yaml        │
│     ├─ [Future: Connect to GCP] → kubeconfig-gcp.yaml               │
│     ├─ Verify kubeconfigs created                                   │
│     └─ Test connections (kubectl cluster-info)                      │
│                                                                     │
│  5. Run Parallel Smoke Tests (~10 minutes)                          │
│     └─ Execute: ./scripts/multi-cloud-smoke-test.sh <image>         │
│        │                                                            │
│        ├─────────────────────┬─────────────────────┐                │
│        │                     │                     │                │
│        ▼                     ▼                     ▼                │
│   ┌─────────────┐       ┌─────────────┐      ┌─────────────┐        │
│   │ AWS Thread  │       │Azure Thread │      │ GCP Thread  │        │
│   │ (PID 3047)  │       │ (PID 3048)  │      │ (PID 3049)  │        │
│   ├─────────────┤       ├─────────────┤      ├─────────────┤        │
│   │KUBECONFIG=  │       │KUBECONFIG=  │      │KUBECONFIG=  │        │
│   │aws.yaml     │       │azure.yaml   │      │gcp.yaml     │        │
│   │PORT=21001   │       │PORT=21002   │      │PORT=21003   │        │
│   │             │       │             │      │             │        │
│   │1. Patch     │       │1. Patch     │      │1. Patch     │        │
│   │   deployment│       │   deployment│      │   deployment│        │
│   │   ✓         │       │   ✓         │      │   ✓         │        │
│   │             │       │             │      │             │        │
│   │2. Wait      │       │2. Wait      │      │2. Wait      │        │
│   │   rollout   │       │   rollout   │      │   rollout   │        │
│   │   ✓ 2m      │       │   ✗ TIMEOUT │      │   ✓ 2m      │        │
│   │             │       │   10m       │      │             │        │
│   │3. Port-fwd  │       │             │      │3. Port-fwd  │        │
│   │   :21001    │       │ [Not reach] │      │   :21003    │        │
│   │             │       │             │      │             │        │
│   │4. Status    │       │             │      │4. Status    │        │
│   │   ✓ ACTIVE  │       │             │      │   ✓ ACTIVE  │        │
│   │             │       │             │      │             │        │
│   │✅ PASS      │       │❌ FAIL      │      │✅ PASS      │        │
│   └─────────────┘       └─────────────┘      └─────────────┘        │
│        │                     │                     │                │
│        └─────────────────────┴─────────────────────┘                │
│                              │                                      │
│  6. Aggregate Results                                               │
│     ├─ Stop log tailing                                             │
│     ├─ Display summary (color-coded)                                │
│     │  ✅ AWS: PASSED                                               │
│     │  ❌ Azure: FAILED (Redis init timeout)                        │
│     │  ✅ GCP: PASSED                                               │
│     ├─ Upload logs as artifacts                                     │
│     └─ Exit 1 (Azure failed)                                        │
│                                                                     │
│  ✗ Job Failed (Azure timeout proves CI blind spot!)                 │
└─────────────────────────────────────────────────────────────────────┘
```

**Key Points:**
- ✅ **Build job** always passes (uses `local` profile with simple Redis)
- ✅ **AWS smoke test** passes (fast ConfigMap mounting)
- ❌ **Azure smoke test** fails (slow ConfigMap mounting → Redis init timeout)
- ✅ **GCP smoke test** passes (medium ConfigMap mounting)
- **Result:** CI blind spot proven! Integration tests pass, but real-world Azure deployment fails

---

## Triggers

### Push Events

```yaml
on:
  push:
    branches:
      - master
      - feature-*
      - tags_intg_test
      - nb_tags_intg_test
      - prove-ci-blind-fresh
```

**Behavior:**
- Runs on **every push** to listed branches
- No pull request trigger (to avoid double runs)
- No manual dispatch (fully automated)

**Branch Patterns:**
- `master` - Production releases
- `feature-*` - Feature development branches
- `tags_intg_test` - Tag-specific integration tests
- `nb_tags_intg_test` - Non-blocking tag tests
- `prove-ci-blind-fresh` - CI blind spot demonstration branch

---

## Jobs

### Job: build

**Purpose:** Build, test, package, and publish Atlas Docker image

**Runner:** `ubuntu-latest` (GitHub-hosted, 2-core, 7GB RAM)

**Duration:** ~15-20 minutes

**Dependencies:** None (runs immediately)

#### Step-by-Step Breakdown

##### 1. Checkout (`actions/checkout@v3`)

```yaml
- uses: actions/checkout@v3
```

**What it does:**
- Clones repository to `$GITHUB_WORKSPACE`
- Checks out the commit that triggered the workflow
- Sets up git configuration

**Default behavior:**
- Fetch depth: 1 (shallow clone)
- Submodules: false
- LFS: false

---

##### 2. Set up Docker (`docker/setup-buildx-action@v2`)

```yaml
- name: Set up Docker
  uses: docker/setup-buildx-action@v2
```

**What it does:**
- Installs Docker Buildx (multi-platform builder)
- Enables advanced Docker build features
- Required for multi-arch image builds

**Capabilities enabled:**
- Multi-architecture builds (amd64, arm64)
- Build caching
- Advanced Dockerfile syntax

---

##### 3. Set up JDK (`actions/setup-java@v3`)

```yaml
- name: Set up JDK 17
  uses: actions/setup-java@v3
  with:
    java-version: '17'
    distribution: 'temurin'
```

**Configuration:**
- **Version:** JDK 17 (LTS)
- **Distribution:** Eclipse Temurin (formerly AdoptOpenJDK)
- **Architecture:** Auto-detected (x64)

**Why Temurin:**
- Free, open-source, TCK-certified
- Reliable long-term support
- Recommended by Eclipse Foundation

---

##### 4. Cache Maven Dependencies (`actions/cache@v3`)

```yaml
- name: Cache Maven dependencies
  uses: actions/cache@v3
  with:
    path: ~/.m2/repository
    key: ${{ runner.os }}-maven-${{ hashFiles('**/pom.xml') }}
    restore-keys: |
      ${{ runner.os }}-maven-
```

**Cache Strategy:**
- **Primary key:** `Linux-maven-<pom.xml-hash>`
  - Invalidates when any `pom.xml` changes
- **Fallback keys:** `Linux-maven-*`
  - Restores most recent cache if pom.xml changed

**Performance Impact:**
- **First run:** Downloads all dependencies (~5 min)
- **Cached runs:** Restores from cache (~30 sec)
- **Storage:** ~500 MB

**Cache Location:** `~/.m2/repository`

---

##### 5. Create Maven Settings (`run`)

```yaml
- name: Create Maven settings
  run: |
    mkdir -p ~/.m2
    cat > ~/.m2/settings.xml <<EOF
    <settings>
      <servers>
        <server>
          <id>github</id>
          <username>${{ github.actor }}</username>
          <password>${{ secrets.GITHUB_TOKEN }}</password>
        </server>
      </servers>
    </settings>
    EOF
```

**Purpose:** Authenticate Maven to GitHub Packages

**Authentication:**
- **Username:** `${{ github.actor }}` (user who triggered workflow)
- **Password:** `${{ secrets.GITHUB_TOKEN }}` (auto-provided)

**Why needed:**
- Atlas depends on private packages in GitHub Packages
- Maven needs credentials to download dependencies
- `settings.xml` provides per-server authentication

**Security:**
- Token is automatically scoped to repository
- Expires when workflow completes
- Never logged or exposed

---

##### 6. Build with Maven (`run`)

```yaml
- name: Build with Maven
  run: ./build.sh
  env:
    GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

**Script:** `./build.sh`

**What it does:**
```bash
#!/bin/bash
mvn clean install -DskipTests
```

**Maven Goals:**
- `clean` - Remove target/ directories
- `install` - Compile, test, package, install to local repo

**Flags:**
- `-DskipTests` - Skip unit tests (run integration tests separately)

**Output:**
- JAR files in `target/` directories
- Installed artifacts in `~/.m2/repository`
- Build logs in Maven output

**Environment:**
- `GITHUB_TOKEN` available for private package downloads

---

##### 7. Check Disk Space (`run`)

```yaml
- name: Check disk space before tests
  run: df -h
```

**Purpose:** Monitor available disk space

**Why important:**
- Integration tests use multiple Docker containers
- Elasticsearch requires significant disk space
- Prevents "No space left on device" errors

**Thresholds:**
- ⚠️ Warning: < 10 GB free
- ❌ Critical: < 5 GB free

**Output Example:**
```
Filesystem      Size  Used Avail Use% Mounted on
/dev/root        84G   45G   39G  54% /
```

---

##### 8. Clean up Docker (`run`)

```yaml
- name: Clean up Docker to free space
  run: |
    docker system prune -af --volumes || true
    df -h
```

**Actions:**
- `docker system prune -af --volumes`
  - `-a` - Remove all unused images
  - `-f` - Force (no confirmation)
  - `--volumes` - Remove unused volumes
- `|| true` - Continue even if fails

**Space Freed:** ~5-10 GB typically

**When to skip:**
- First run (nothing to clean)
- Plenty of space available (>20 GB)

---

##### 9. Run Integration Tests (`run`)

```yaml
- name: Run integration tests
  run: |
    echo "=========================================="
    echo "Running integration tests..."
    echo "=========================================="
    
    cd webapp
    mvn test -Dtest=Basic* -DfailIfNoTests=false
    
    echo "=========================================="
    echo "Integration tests completed successfully!"
    echo "=========================================="
  env:
    GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
    TESTCONTAINERS_RYUK_DISABLED: false
```

**Maven Command:**
```bash
mvn test -Dtest=Basic* -DfailIfNoTests=false
```

**Flags:**
- `-Dtest=Basic*` - Run tests matching pattern (BasicServiceAvailabilityTest, BasicSanityForAttributesTypesTest)
- `-DfailIfNoTests=false` - Don't fail if no tests found

**Test Classes Run:**
1. `BasicServiceAvailabilityTest`
   - Health check
   - Get all types
   - Create table asset

2. `BasicSanityForAttributesTypesTest`
   - Attribute tests
   - Type sanity checks

**Testcontainers Setup:**
```java
// AtlasDockerIntegrationTest.java
@BeforeAll
static void setup() {
  // Start containers
  zookeeper.start();
  kafka.start();
  cassandra.start();
  elasticsearch.start();
  redis.start();
  atlas.start();
}
```

**Containers:**
| Container | Image | Port | Purpose |
|-----------|-------|------|---------|
| Zookeeper | `zookeeper:3.7` | 2181 | Coordination |
| Kafka | `confluentinc/cp-kafka:7.4.0` | 9093 | Messaging |
| Cassandra | `cassandra:2.1` | 9042 | Storage |
| Elasticsearch | `docker.elastic.co/elasticsearch/elasticsearch:7.17.10` | 9200 | Search |
| Redis | `redis:6.2.14` | 6379 | Caching |
| Atlas | `atlanhq/atlas:test` | 21000 | Application |

**Environment Variables:**
- `GITHUB_TOKEN` - For Maven dependencies
- `TESTCONTAINERS_RYUK_DISABLED=false` - Enable container cleanup

**Test Duration:** ~8-10 minutes

**Success Criteria:**
- All tests pass (exit code 0)
- No container startup failures
- Atlas API responds to health checks

---

##### 10. Capture Container Logs (on failure)

```yaml
- name: Capture container logs on failure
  if: failure()
  run: |
    echo "=========================================="
    echo "CAPTURING CONTAINER LOGS"
    echo "=========================================="
    
    echo "All containers:"
    docker ps -a
    
    mkdir -p container-logs
    
    for container in $(docker ps -a --format '{{.Names}}'); do
      echo "Capturing logs for: $container"
      docker logs $container > container-logs/$container.log 2>&1 || true
    done
    
    echo "Log files created:"
    ls -lh container-logs/
    
    # Preview Atlas logs
    echo "=========================================="
    echo "PREVIEW: Atlas Container Logs (last 100 lines)"
    echo "=========================================="
    find container-logs/ -name '*atlas*' -type f -exec tail -100 {} \; || echo "No Atlas container found"
```

**Condition:** `if: failure()` - Only runs if previous step failed

**Actions:**
1. List all containers (running and stopped)
2. Create `container-logs/` directory
3. Capture logs from each container
4. Preview Atlas container logs (last 100 lines)

**Log Files Created:**
- `container-logs/zookeeper.log`
- `container-logs/kafka.log`
- `container-logs/cassandra.log`
- `container-logs/elasticsearch.log`
- `container-logs/redis.log`
- `container-logs/atlas.log`

**Error Handling:**
- `|| true` prevents failure if container doesn't exist
- Continues even if some logs can't be captured

---

##### 11. Setup tmate session (on failure)

```yaml
- name: Setup tmate session for debugging
  if: failure()
  uses: mxschmitt/action-tmate@v3
  timeout-minutes: 30
```

**Condition:** Only runs if tests fail

**What it does:**
- Starts SSH server on GitHub runner
- Provides interactive shell access
- Allows real-time debugging

**Usage:**
1. Workflow fails
2. Step outputs SSH connection string:
   ```
   ssh <token>@nyc1.tmate.io
   ```
3. Connect to debug:
   ```bash
   ssh <token>@nyc1.tmate.io
   cd $GITHUB_WORKSPACE
   docker ps
   docker logs <container>
   ```

**Timeout:** 30 minutes (auto-terminates)

**Security:**
- Unique token per session
- Expires after workflow completes
- Read-only access to secrets

---

##### 12. Upload Container Logs (on failure)

```yaml
- name: Upload container logs
  if: failure()
  uses: actions/upload-artifact@v4
  with:
    name: container-logs-${{ github.run_id }}
    path: container-logs/
    retention-days: 5
```

**Condition:** Only runs if tests fail

**Artifact:**
- **Name:** `container-logs-<run-id>`
- **Contents:** All container log files
- **Retention:** 5 days
- **Size:** ~10-50 MB typically

**Access:**
- GitHub Actions UI → Run → Artifacts
- Download as ZIP file
- Useful for post-mortem analysis

---

##### 13. Set up QEMU (`docker/setup-qemu-action@v2`)

```yaml
- name: Set up QEMU
  uses: docker/setup-qemu-action@v2
```

**Purpose:** Enable multi-architecture Docker builds

**What it does:**
- Installs QEMU static binaries
- Registers binfmt_misc handlers
- Enables cross-platform emulation

**Architectures Enabled:**
- `linux/amd64` (x86_64)
- `linux/arm64` (aarch64)
- `linux/arm/v7` (armv7)
- And more...

**Why needed:**
- Build ARM images on x86 runners
- Support Apple Silicon (M1/M2)
- Multi-platform deployments

---

##### 14. Login to GitHub Container Registry (`docker/login-action@v2`)

```yaml
- name: Login to GitHub Container Registry
  uses: docker/login-action@v2
  with:
    registry: ghcr.io
    username: ${{ github.actor }}
    password: ${{ secrets.GITHUB_TOKEN }}
```

**Registry:** `ghcr.io` (GitHub Container Registry)

**Authentication:**
- **Username:** Workflow actor (e.g., `krishnanunni-m`)
- **Password:** Automatic GitHub token

**Permissions Required:**
- `contents: read` (read repository)
- `packages: write` (push to GHCR)

**Why GHCR:**
- Free for public repositories
- Integrated with GitHub
- Automatic cleanup policies
- No rate limits for authenticated users

---

##### 15. Get Branch Name (`run`)

```yaml
- name: Get branch name
  run: |
    echo "BRANCH_NAME=${GITHUB_REF#refs/heads/}" >> $GITHUB_ENV
    echo BRANCH_NAME=${GITHUB_REF#refs/heads/}
```

**Purpose:** Extract branch name for image tagging

**Logic:**
```bash
GITHUB_REF = "refs/heads/feature-redis-fix"
${GITHUB_REF#refs/heads/} = "feature-redis-fix"
```

**Shell Parameter Expansion:**
- `${var#pattern}` - Remove shortest match from beginning

**Output:**
- Sets environment variable: `BRANCH_NAME`
- Prints to console for visibility

**Example Values:**
- `master`
- `feature-redis-fix`
- `prove-ci-blind-fresh`

---

##### 16. Get Commit ID (`run`)

```yaml
- name: Get commit ID
  run: echo "COMMIT_ID=$(echo ${GITHUB_SHA} | cut -c1-7)abcd" >> $GITHUB_ENV
```

**Purpose:** Create short commit hash for image tagging

**Logic:**
```bash
GITHUB_SHA = "a1b2c3d4e5f6g7h8i9j0"
cut -c1-7 = "a1b2c3d"
+ "abcd" = "a1b2c3dabcd"
```

**Why `abcd` suffix:**
- Distinguishes CI builds from manual builds
- Makes it clear image came from automation
- Easier to identify in registries

**Output:**
- Sets environment variable: `COMMIT_ID`
- Format: `<7-char-hash>abcd`

**Example:** `064f482abcd`

---

##### 17. Build and Push Docker Image (`run`)

```yaml
- name: Build and push Docker image
  run: |
    IMAGE_NAME="ghcr.io/atlanhq/${{ github.event.repository.name }}-${{ env.BRANCH_NAME }}:${{ env.COMMIT_ID }}"
    echo "Building image: $IMAGE_NAME"
    
    docker buildx build \
      --platform linux/amd64,linux/arm64 \
      --file Dockerfile \
      --tag "$IMAGE_NAME" \
      --push \
      .
    
    echo "Image pushed successfully: $IMAGE_NAME"
```

**Image Name Format:**
```
ghcr.io/atlanhq/atlas-metastore-{BRANCH}:{COMMIT_ID}
```

**Example:**
```
ghcr.io/atlanhq/atlas-metastore-prove-ci-blind-fresh:064f482abcd
```

**Docker Buildx Command:**
```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \  # Multi-arch
  --file Dockerfile \                    # Dockerfile path
  --tag "$IMAGE_NAME" \                  # Tag
  --push \                               # Push after build
  .                                      # Build context
```

**Platforms:**
- `linux/amd64` - Intel/AMD x86_64 (most servers)
- `linux/arm64` - ARM64 (Apple Silicon, AWS Graviton)

**Build Context:** Current directory (`.`)

**Build Duration:** ~3-5 minutes

**Output:**
- Multi-platform manifest pushed to GHCR
- Automatically creates manifest list
- Pulls correct architecture at runtime

---

##### 18. Verify Image (`run`)

```yaml
- name: Verify image
  run: docker buildx imagetools inspect --raw ghcr.io/atlanhq/${{ github.event.repository.name }}-${{ env.BRANCH_NAME }}:${{ env.COMMIT_ID }}
```

**Purpose:** Confirm image was pushed successfully

**Command:** `docker buildx imagetools inspect --raw`

**What it checks:**
- Image exists in registry
- Manifest list is valid
- All architectures present

**Output Example:**
```json
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
  "manifests": [
    {
      "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
      "platform": {
        "architecture": "amd64",
        "os": "linux"
      }
    },
    {
      "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
      "platform": {
        "architecture": "arm64",
        "os": "linux"
      }
    }
  ]
}
```

---

##### 19. Scan Image (`aquasecurity/trivy-action@master`)

```yaml
- name: Scan Image
  uses: aquasecurity/trivy-action@master
  with:
    image-ref: 'ghcr.io/atlanhq/${{ github.event.repository.name }}-${{ env.BRANCH_NAME }}:${{ env.COMMIT_ID }}'
    vuln-type: 'os,library'
    format: 'sarif'
    output: 'trivy-image-results.sarif'
```

**Scanner:** Trivy (open-source vulnerability scanner)

**Configuration:**
- **Image:** Built image from GHCR
- **Vulnerability Types:** OS packages + library dependencies
- **Output Format:** SARIF (Static Analysis Results Interchange Format)
- **Output File:** `trivy-image-results.sarif`

**Vulnerabilities Detected:**
- CVEs in base image (Ubuntu/Alpine)
- CVEs in Java dependencies
- Misconfigurations
- Secrets in layers

**Severity Levels:**
- CRITICAL - Immediate action required
- HIGH - Important to fix
- MEDIUM - Should fix eventually
- LOW - Nice to fix
- UNKNOWN - Unassessed

**Action:** Scan only (doesn't fail build)

---

##### 20. Upload Trivy Results (`github/codeql-action/upload-sarif@v2.1.33`)

```yaml
- name: Upload Trivy scan results to GitHub Security tab
  uses: github/codeql-action/upload-sarif@v2.1.33
  with:
    sarif_file: 'trivy-image-results.sarif'
```

**Purpose:** Display vulnerabilities in GitHub Security tab

**Location:** Repository → Security → Code scanning alerts

**Features:**
- Grouped by severity
- Links to CVE databases
- Fix recommendations
- Trend tracking over time

**Visibility:**
- Repository admins
- Security team
- Developers with access

---

### Job: smoke-test

**Purpose:** Validate deployment across multiple cloud environments

**Runner:** `ubuntu-latest` (GitHub-hosted)

**Duration:** ~10-12 minutes

**Dependencies:** `needs: build` (waits for build job)

#### Step-by-Step Breakdown

##### 1. Checkout (`actions/checkout@v3`)

```yaml
- name: Checkout
  uses: actions/checkout@v3
```

**Why needed again:**
- Access to `scripts/multi-cloud-smoke-test.sh`
- Jobs run on separate runners (no shared filesystem)

---

##### 2. Get Branch Name (`run`)

```yaml
- name: Get branch name
  run: echo "BRANCH_NAME=${GITHUB_REF#refs/heads/}" >> $GITHUB_ENV
```

**Same as build job** - Extract branch name

---

##### 3. Get Commit ID (`run`)

```yaml
- name: Get commit ID
  run: echo "COMMIT_ID=$(echo ${GITHUB_SHA} | cut -c1-7)abcd" >> $GITHUB_ENV
```

**Same as build job** - Extract commit hash

---

##### 4. Set Test Image (`run`)

```yaml
- name: Set test image
  run: echo "TEST_IMAGE=ghcr.io/atlanhq/${{ github.event.repository.name }}-${{ env.BRANCH_NAME }}:${{ env.COMMIT_ID }}" >> $GITHUB_ENV
```

**Purpose:** Construct image name from build job

**Logic:**
- Uses same branch name and commit ID
- Points to image pushed by build job
- Ensures testing the exact build that passed CI

**Example:**
```
TEST_IMAGE=ghcr.io/atlanhq/atlas-metastore-prove-ci-blind-fresh:064f482abcd
```

---

##### 5. Install kubectl (`azure/setup-kubectl@v3`)

```yaml
- name: Install kubectl
  uses: azure/setup-kubectl@v3
```

**What it does:**
- Downloads kubectl binary
- Installs to PATH
- Verifies installation

**Version:** Latest stable

**Why needed:**
- Interact with Kubernetes clusters
- Deploy Atlas to vclusters
- Check pod status, logs, events

---

##### 6. Install vCluster CLI (`loft-sh/setup-vcluster@main`)

```yaml
- name: Install vCluster CLI
  uses: loft-sh/setup-vcluster@main
```

**What it does:**
- Downloads vCluster CLI
- Installs to PATH
- Authenticates with platform

**Why needed:**
- Connect to vCluster Platform
- Generate kubeconfigs for vclusters
- Manage vcluster lifecycle

**Commands Provided:**
- `vcluster platform login`
- `vcluster platform connect`
- `vcluster platform list`

---

##### 7. Install jq (`run`)

```yaml
- name: Install jq
  run: sudo apt-get install -y jq
```

**Purpose:** JSON parsing in smoke test script

**Why needed:**
- Parse Atlas API responses
- Extract status field
- Validate health check results

**Example Usage:**
```bash
STATUS=$(curl /api/atlas/admin/status | jq -r '.Status')
[ "$STATUS" = "ACTIVE" ]
```

---

##### 8. Connect to GlobalProtect VPN (`run`)

```yaml
- name: Connect to GlobalProtect VPN
  env:
    VCLUSTER_PLATFORM_URL: ${{ secrets.VCLUSTER_PLATFORM_URL }}
  run: |
    echo "=================================================="
    echo "CONNECTING TO VPN (Shared for all clouds)"
    echo "=================================================="
    
    # Install OpenConnect
    sudo apt-get update -qq
    sudo apt-get install -y openconnect
    
    # Connect to VPN (using default DTLS/ESP for AWS compatibility)
    echo "${{ secrets.GLOBALPROTECT_PASSWORD }}" | sudo openconnect \
      --protocol=gp \
      --user="${{ secrets.GLOBALPROTECT_USERNAME }}" \
      --passwd-on-stdin \
      --background \
      "${{ vars.GLOBALPROTECT_PORTAL_URL }}"
    
    # Wait for connection to establish
    echo "Waiting for VPN connection to stabilize..."
    sleep 20
    
    # Check if VPN is running
    if ! pgrep -x openconnect > /dev/null; then
      echo "ERROR: OpenConnect exited unexpectedly"
      exit 1
    fi
    echo "VPN process is running (PID: $(pgrep -x openconnect))"
    
    # Configure routing for vCluster Platform (172.17.0.0/16)
    VPN_INTERFACE=$(ip addr show | grep -E '^[0-9]+: tun' | head -1 | cut -d: -f2 | tr -d ' ' || echo "tun0")
    echo "Using VPN interface: $VPN_INTERFACE"
    
    sudo ip route del 172.17.0.0/16 dev docker0 2>/dev/null || true
    sudo ip route add 172.17.0.0/16 dev $VPN_INTERFACE
    
    # Verify connectivity
    if curl -k -sS $VCLUSTER_PLATFORM_URL -o /dev/null --max-time 30; then
      echo "✓ VPN connected successfully"
    else
      echo "ERROR: VPN connectivity test failed"
      exit 1
    fi
```

**VPN Protocol:** GlobalProtect (via OpenConnect)

**OpenConnect Configuration:**
- `--protocol=gp` - GlobalProtect protocol
- `--user=<username>` - From secret
- `--passwd-on-stdin` - Password from pipe
- `--background` - Daemonize after connection

**Connection Flow:**
1. Install OpenConnect
2. Connect to VPN gateway
3. Wait 20 seconds for stabilization
4. Verify process is running
5. Configure routing
6. Test connectivity

**Routing Configuration:**
```bash
# Remove Docker network (conflicts with VPN)
sudo ip route del 172.17.0.0/16 dev docker0

# Route vCluster Platform traffic through VPN
sudo ip route add 172.17.0.0/16 dev tun0
```

**Why This Works:**
- vCluster Platform at 172.17.x.x
- Single VPN tunnel for all vclusters
- No per-cloud VPN connections needed

**Connectivity Test:**
```bash
curl -k -sS $VCLUSTER_PLATFORM_URL --max-time 30
```

**Success Criteria:**
- OpenConnect process running
- Routes configured
- Platform accessible via curl

---

##### 9. Login to vCluster Platform (`run`)

```yaml
- name: Login to vCluster Platform
  env:
    VCLUSTER_PLATFORM_URL: ${{ secrets.VCLUSTER_PLATFORM_URL }}
    VCLUSTER_ACCESS_KEY: ${{ secrets.VCLUSTER_ACCESS_KEY }}
  run: |
    echo "=================================================="
    echo "LOGGING IN TO VCLUSTER PLATFORM (Shared)"
    echo "=================================================="
    vcluster platform login $VCLUSTER_PLATFORM_URL --access-key $VCLUSTER_ACCESS_KEY
    echo "✓ Login successful"
```

**Command:**
```bash
vcluster platform login <URL> --access-key <KEY>
```

**Authentication:**
- **Access Key:** Long-lived API token
- **Generated:** `vcluster platform create accesskey`
- **Scope:** Platform-wide access

**Session:**
- Stored in `~/.vcluster/config.json`
- Valid for all subsequent commands
- No need to re-authenticate per vcluster

**Output:**
```
Successfully logged into vcluster Platform instance https://onboarding-tenant.atlan.com
```

---

##### 10. Connect to All vClusters (`run`)

```yaml
- name: Connect to all vClusters
  run: |
    echo "=================================================="
    echo "CONNECTING TO ALL VCLUSTERS"
    echo "=================================================="
    
    # Connect to AWS vCluster
    echo "Connecting to AWS vCluster (hkmeta02)..."
    KUBECONFIG=kubeconfig-aws.yaml vcluster platform connect vcluster hkmeta02 --project default
    echo "✓ AWS kubeconfig saved to kubeconfig-aws.yaml"
    
    # Connect to Azure vCluster  
    echo "Connecting to Azure vCluster (enpla1cp21)..."
    KUBECONFIG=kubeconfig-azure.yaml vcluster platform connect vcluster enpla1cp21 --project default
    echo "✓ Azure kubeconfig saved to kubeconfig-azure.yaml"
    
    echo ""
    echo "Verifying kubeconfigs..."
    ls -lh kubeconfig-*.yaml
    
    echo ""
    echo "Testing AWS connection..."
    KUBECONFIG=kubeconfig-aws.yaml kubectl cluster-info | head -1
    
    echo ""
    echo "Testing Azure connection..."
    KUBECONFIG=kubeconfig-azure.yaml kubectl cluster-info | head -1
    
    echo ""
    echo "✓ All vCluster connections established"
```

**Key Innovation:** Separate kubeconfigs via `KUBECONFIG` env var

**AWS vCluster:**
```bash
KUBECONFIG=kubeconfig-aws.yaml \
  vcluster platform connect vcluster hkmeta02 --project default
```

**Azure vCluster:**
```bash
KUBECONFIG=kubeconfig-azure.yaml \
  vcluster platform connect vcluster enpla1cp21 --project default
```

**Output Files:**
- `kubeconfig-aws.yaml` - AWS cluster credentials
- `kubeconfig-azure.yaml` - Azure cluster credentials

**Verification:**
```bash
# Test each connection
KUBECONFIG=kubeconfig-aws.yaml kubectl cluster-info
# Kubernetes control plane is running at https://...

KUBECONFIG=kubeconfig-azure.yaml kubectl cluster-info
# Kubernetes control plane is running at https://...
```

**Why This Works:**
- Each kubeconfig contains cluster-specific credentials
- No conflicts between cloud environments
- Can be used concurrently in parallel tests

---

##### 11. Run Parallel Smoke Tests (`run`)

```yaml
- name: Run parallel smoke tests
  run: ./scripts/multi-cloud-smoke-test.sh ${{ env.TEST_IMAGE }}
```

**Script:** `scripts/multi-cloud-smoke-test.sh`

**Arguments:**
- `$1` (TEST_IMAGE) - Image to deploy and test

**Execution:**
```bash
./scripts/multi-cloud-smoke-test.sh \
  ghcr.io/atlanhq/atlas-metastore-prove-ci-blind-fresh:064f482abcd
```

**Script Responsibilities:**
1. Validate arguments
2. Define `test_cloud()` function
3. Launch parallel tests (AWS, Azure)
4. Stream logs with color coding
5. Aggregate results
6. Exit with proper code

**See:** [Script Documentation](#script-documentation) section below

**Duration:** ~10 minutes (limited by slowest test)

**Output:**
- Real-time interleaved logs
- Color-coded success/failure
- Final summary
- Log artifacts

---

##### 12. Upload Smoke Test Logs (`actions/upload-artifact@v4`)

```yaml
- name: Upload smoke test logs
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: smoke-test-logs-${{ github.run_id }}
    path: smoke-test-logs/
    retention-days: 5
```

**Condition:** `if: always()` - Runs even if tests fail

**Artifact:**
- **Name:** `smoke-test-logs-<run-id>`
- **Contents:** `smoke-test-logs/` directory
  - `AWS.log`
  - `Azure.log`
  - (Future: `GCP.log`)
- **Retention:** 5 days
- **Size:** ~100 KB - 1 MB per log

**Access:**
- GitHub UI → Actions → Run → Artifacts
- Download as ZIP
- View individual cloud logs

**Use Cases:**
- Post-mortem analysis
- Share with team
- Compare across runs
- Debug intermittent failures

---

## Script Documentation

### multi-cloud-smoke-test.sh

**Location:** `scripts/multi-cloud-smoke-test.sh`

**Purpose:** Execute parallel smoke tests across multiple clouds

**Usage:**
```bash
./scripts/multi-cloud-smoke-test.sh <test-image>
```

**Example:**
```bash
./scripts/multi-cloud-smoke-test.sh \
  ghcr.io/atlanhq/atlas-metastore-master:a1b2c3dabcd
```

#### Script Structure

**1. Argument Validation**
```bash
if [ $# -ne 1 ]; then
  echo "Error: Missing test image argument"
  echo "Usage: $0 <test-image>"
  exit 1
fi
TEST_IMAGE=$1
```

**2. Define test_cloud() Function**
```bash
test_cloud() {
  CLOUD=$1                # AWS, Azure, GCP
  KUBECONFIG_FILE=$2      # kubeconfig-{cloud}.yaml
  LOG_FILE="smoke-test-logs/${CLOUD}.log"
  
  # All operations logged to file
  {
    # 1. Verify kubeconfig
    [ -f "$KUBECONFIG_FILE" ] || exit 1
    
    # 2. Patch deployment
    KUBECONFIG=$KUBECONFIG_FILE kubectl set image ...
    
    # 3. Wait for rollout
    KUBECONFIG=$KUBECONFIG_FILE kubectl rollout status ...
    
    # 4. Port-forward
    KUBECONFIG=$KUBECONFIG_FILE kubectl port-forward ...
    
    # 5. Health check
    curl http://localhost:$LOCAL_PORT/api/atlas/admin/status
    
    # 6. Validate
    [ "$STATUS" = "ACTIVE" ]
    
  } > "$LOG_FILE" 2>&1
}
```

**3. Launch Parallel Tests**
```bash
bash -c "test_cloud AWS kubeconfig-aws.yaml" &
PID_AWS=$!

bash -c "test_cloud Azure kubeconfig-azure.yaml" &
PID_AZURE=$!
```

**4. Stream Logs with Colors**
```bash
tail -f smoke-test-logs/AWS.log | while read line; do
  if echo "$line" | grep -q "ERROR\|❌"; then
    echo -e "\033[0;31m[AWS] $line\033[0m"  # Red
  elif echo "$line" | grep -q "✓\|✅"; then
    echo -e "\033[0;32m[AWS] $line\033[0m"  # Green
  else
    echo "[AWS] $line"  # White
  fi
done &
```

**5. Wait for Completion**
```bash
wait $PID_AWS
AWS_EXIT=$?

wait $PID_AZURE
AZURE_EXIT=$?

FAILED=0
[ $AWS_EXIT -ne 0 ] && FAILED=1
[ $AZURE_EXIT -ne 0 ] && FAILED=1
```

**6. Display Summary**
```bash
echo "AWS Results:"
cat smoke-test-logs/AWS.log | tail -5

echo "Azure Results:"
cat smoke-test-logs/Azure.log | tail -5
```

**7. Exit with Aggregate Status**
```bash
[ $FAILED -eq 1 ] && exit 1
echo "✅ All smoke tests passed!"
```

#### Per-Cloud Test Flow

**Test Function Details:**

1. **Verify Kubeconfig**
   ```bash
   if [ ! -f "$KUBECONFIG_FILE" ]; then
     echo "❌ ERROR: Kubeconfig not found: $KUBECONFIG_FILE"
     exit 1
   fi
   ```

2. **Patch Deployment**
   ```bash
   KUBECONFIG=$KUBECONFIG_FILE kubectl set image deployment/atlas \
     atlas-main=$TEST_IMAGE \
     -n atlas
   ```
   
   **What it does:**
   - Updates Atlas deployment image
   - Triggers rolling update
   - Kubernetes starts new pods

3. **Wait for Rollout**
   ```bash
   KUBECONFIG=$KUBECONFIG_FILE kubectl rollout status \
     deployment/atlas -n atlas --timeout=10m
   ```
   
   **What it waits for:**
   - New pods scheduled
   - Containers started
   - Readiness probes passing
   - Old pods terminated
   
   **Timeout:** 10 minutes
   
   **Success:** All replicas updated and ready
   **Failure:** Timeout, probe failures, crashes

4. **Port-Forward**
   ```bash
   # AWS uses port 21001
   # Azure uses port 21002
   # GCP uses port 21003
   
   if [ "$CLOUD" = "AWS" ]; then
     LOCAL_PORT=21001
   elif [ "$CLOUD" = "Azure" ]; then
     LOCAL_PORT=21002
   else
     LOCAL_PORT=21003
   fi
   
   KUBECONFIG=$KUBECONFIG_FILE kubectl port-forward \
     -n atlas svc/atlas-service-atlas $LOCAL_PORT:80 > /dev/null 2>&1 &
   PF_PID=$!
   sleep 5
   ```
   
   **Why unique ports:**
   - Avoid conflicts in parallel execution
   - Each test has isolated network access
   - Easier to debug (know which cloud by port)

5. **Health Check**
   ```bash
   STATUS_RESPONSE=$(curl -f -s \
     "http://localhost:$LOCAL_PORT/api/atlas/admin/status")
   
   STATUS=$(echo "$STATUS_RESPONSE" | jq -r '.Status')
   ```
   
   **Endpoint:** `/api/atlas/admin/status`
   **Expected Response:**
   ```json
   {
     "Status": "ACTIVE"
   }
   ```
   
   **Validation:**
   ```bash
   if [ "$STATUS" = "ACTIVE" ]; then
     echo "✓ Atlas is ACTIVE"
   else
     echo "❌ ERROR: Status check failed - Status: $STATUS"
     exit 1
   fi
   ```

6. **Cleanup**
   ```bash
   kill $PF_PID 2>/dev/null || true
   ```

7. **Result**
   ```bash
   echo "✅✅✅ SMOKE TEST PASSED ✅✅✅"
   ```

#### Color Codes

```bash
RED='\033[0;31m'      # Errors, failures
GREEN='\033[0;32m'    # Success, passes
YELLOW='\033[1;33m'   # Status updates
BLUE='\033[0;34m'     # Section headers
NC='\033[0m'          # Reset
```

**Usage Patterns:**
- `ERROR|❌|failed` → Red
- `✓|✅|PASSED|successfully` → Green
- Cloud names, status → Yellow
- Section dividers → Blue

---

## Environment Variables

### Job-Level Variables

| Variable | Set By | Scope | Example | Purpose |
|----------|--------|-------|---------|---------|
| `GITHUB_TOKEN` | GitHub | All jobs | `ghs_xxx...` | Maven auth, GHCR push |
| `GITHUB_REF` | GitHub | All jobs | `refs/heads/master` | Branch reference |
| `GITHUB_SHA` | GitHub | All jobs | `a1b2c3d...` | Full commit hash |
| `GITHUB_ACTOR` | GitHub | All jobs | `krishnanunni-m` | User who triggered workflow |
| `GITHUB_WORKSPACE` | GitHub | All jobs | `/home/runner/work/atlas-metastore/atlas-metastore` | Workspace path |
| `GITHUB_RUN_ID` | GitHub | All jobs | `12345678` | Unique run ID |

### Custom Variables

| Variable | Set In Step | Used In Step | Example | Purpose |
|----------|-------------|--------------|---------|---------|
| `BRANCH_NAME` | Get branch name | Build & push, Set test image | `prove-ci-blind-fresh` | Image tagging |
| `COMMIT_ID` | Get commit ID | Build & push, Set test image | `064f482abcd` | Image tagging |
| `TEST_IMAGE` | Set test image | Run smoke tests | `ghcr.io/atlanhq/atlas-metastore-prove-ci-blind-fresh:064f482abcd` | Deploy target |
| `TESTCONTAINERS_RYUK_DISABLED` | Integration tests | Testcontainers | `false` | Enable cleanup |

### Script Variables (multi-cloud-smoke-test.sh)

| Variable | Set By | Scope | Example | Purpose |
|----------|--------|-------|---------|---------|
| `TEST_IMAGE` | Argument | Global | `ghcr.io/.../atlas:tag` | Deploy image |
| `CLOUD` | Function arg | Per-test | `AWS`, `Azure`, `GCP` | Cloud identifier |
| `KUBECONFIG_FILE` | Function arg | Per-test | `kubeconfig-aws.yaml` | Cluster credentials |
| `LOG_FILE` | Function | Per-test | `smoke-test-logs/AWS.log` | Output file |
| `LOCAL_PORT` | Calculated | Per-test | `21001`, `21002`, `21003` | Port-forward port |
| `PF_PID` | kubectl | Per-test | `3047` | Port-forward process ID |

---

## Secrets

### Required Secrets

| Secret | Description | Scope | Example | How to Generate |
|--------|-------------|-------|---------|-----------------|
| `GITHUB_TOKEN` | Auto-provided by GitHub | Per-workflow | `ghs_xxx...` | Automatic (no setup) |
| `GLOBALPROTECT_USERNAME` | VPN username | Organization | `user@company.com` | From IT/Security team |
| `GLOBALPROTECT_PASSWORD` | VPN password | Organization | `P@ssw0rd!` | From IT/Security team |
| `VCLUSTER_ACCESS_KEY` | vCluster Platform API key | Organization | `eyJhbGc...` | `vcluster platform create accesskey` |
| `VCLUSTER_PLATFORM_URL` | vCluster Platform URL | Organization | `https://onboarding-tenant.atlan.com` | From vCluster admin |

### Secret Configuration

**Add Secrets:**
1. Go to repository Settings
2. Navigate to Secrets and variables → Actions
3. Click "New repository secret"
4. Enter name and value
5. Click "Add secret"

**Secret Scopes:**
- **Repository:** Available to this repo only
- **Organization:** Shared across repos (recommended for VPN/vCluster)
- **Environment:** Scoped to specific environments (e.g., production)

**Best Practices:**
- ✅ Use organization secrets for shared credentials
- ✅ Rotate secrets regularly
- ✅ Use service accounts (not personal credentials)
- ✅ Limit secret access to required workflows
- ❌ Never log secret values
- ❌ Never echo secrets in workflow output

---

## Variables

### Repository Variables

| Variable | Description | Example | Required |
|----------|-------------|---------|----------|
| `GLOBALPROTECT_PORTAL_URL` | VPN gateway URL | `vpn.company.com` | Yes |

**Add Variables:**
1. Go to repository Settings
2. Navigate to Secrets and variables → Actions → Variables tab
3. Click "New repository variable"
4. Enter name and value
5. Click "Add variable"

**Variables vs Secrets:**
- **Variables:** Non-sensitive, can be logged
- **Secrets:** Sensitive, masked in logs

---

## Outputs

### Build Job Outputs

**None explicitly defined**, but produces:

1. **Docker Image:**
   - Registry: `ghcr.io`
   - Name: `atlanhq/atlas-metastore-{BRANCH}`
   - Tag: `{COMMIT_ID}`
   - Platforms: `linux/amd64`, `linux/arm64`

2. **Trivy SARIF:**
   - File: `trivy-image-results.sarif`
   - Uploaded to: GitHub Security tab

3. **Artifacts (on failure):**
   - `container-logs-{RUN_ID}.zip`
     - Contains: All container logs
     - Retention: 5 days

### Smoke Test Job Outputs

**Artifacts (always):**
- `smoke-test-logs-{RUN_ID}.zip`
  - Contains: `AWS.log`, `Azure.log`
  - Retention: 5 days
  - Size: ~100 KB - 1 MB

---

## Dependencies

### External Actions

| Action | Version | Purpose | Documentation |
|--------|---------|---------|---------------|
| `actions/checkout` | `@v3` | Clone repository | [GitHub](https://github.com/actions/checkout) |
| `actions/setup-java` | `@v3` | Install JDK | [GitHub](https://github.com/actions/setup-java) |
| `actions/cache` | `@v3` | Cache dependencies | [GitHub](https://github.com/actions/cache) |
| `docker/setup-buildx-action` | `@v2` | Multi-platform builds | [GitHub](https://github.com/docker/setup-buildx-action) |
| `docker/setup-qemu-action` | `@v2` | Cross-platform emulation | [GitHub](https://github.com/docker/setup-qemu-action) |
| `docker/login-action` | `@v2` | Registry authentication | [GitHub](https://github.com/docker/login-action) |
| `mxschmitt/action-tmate` | `@v3` | SSH debugging | [GitHub](https://github.com/mxschmitt/action-tmate) |
| `actions/upload-artifact` | `@v4` | Upload artifacts | [GitHub](https://github.com/actions/upload-artifact) |
| `aquasecurity/trivy-action` | `@master` | Security scanning | [GitHub](https://github.com/aquasecurity/trivy-action) |
| `github/codeql-action/upload-sarif` | `@v2.1.33` | Upload scan results | [GitHub](https://github.com/github/codeql-action) |
| `azure/setup-kubectl` | `@v3` | Install kubectl | [GitHub](https://github.com/Azure/setup-kubectl) |
| `loft-sh/setup-vcluster` | `@main` | Install vCluster CLI | [GitHub](https://github.com/loft-sh/setup-vcluster) |

### System Dependencies

**Installed via apt:**
- `openconnect` - VPN client (GlobalProtect support)
- `jq` - JSON processor

**Pre-installed on ubuntu-latest:**
- Docker
- Git
- curl
- bash
- coreutils (cut, tail, grep, etc.)

---

## Artifacts

### Container Logs (on test failure)

**Name:** `container-logs-{RUN_ID}`

**Contents:**
```
container-logs/
├── zookeeper.log
├── kafka.log
├── cassandra.log
├── elasticsearch.log
├── redis.log
└── atlas.log
```

**Size:** ~10-50 MB

**Retention:** 5 days

**Access:** GitHub Actions UI → Run → Artifacts

**Use Cases:**
- Debug test failures
- Identify container crashes
- Check startup logs
- Analyze error patterns

---

### Smoke Test Logs (always)

**Name:** `smoke-test-logs-{RUN_ID}`

**Contents:**
```
smoke-test-logs/
├── AWS.log
└── Azure.log
```

**Size:** ~100 KB - 1 MB per log

**Retention:** 5 days

**Access:** GitHub Actions UI → Run → Artifacts

**Use Cases:**
- Compare cloud behavior
- Identify Azure timeout cause
- Share with team
- Track deployment trends

**Log Format:**
```
==========================================
[AWS] Starting smoke test
==========================================
Image: ghcr.io/atlanhq/atlas-metastore-prove-ci-blind-fresh:064f482abcd
Kubeconfig: kubeconfig-aws.yaml

[AWS] Patching Atlas deployment...
deployment.apps/atlas image updated
[AWS] ✓ Deployment patched

[AWS] Waiting for rollout (10 min timeout)...
Waiting for deployment "atlas" rollout to finish: 1 out of 2 new replicas have been updated...
Waiting for deployment "atlas" rollout to finish: 1 of 2 updated replicas are available...
deployment "atlas" successfully rolled out
[AWS] ✓ Rollout completed successfully

[AWS] Setting up port-forward...
[AWS] Running status check...
[AWS] ✓ Atlas is ACTIVE

[AWS] ✅✅✅ SMOKE TEST PASSED ✅✅✅
```

---

## Security

### Token Permissions

**GITHUB_TOKEN Auto-Permissions:**
```yaml
permissions:
  contents: read      # Read repository
  packages: write     # Push to GHCR
  security-events: write  # Upload Trivy results
```

**Scope:**
- Limited to repository
- Expires after workflow completes
- Cannot access other repositories
- Cannot modify workflow files

### Secret Handling

**Best Practices:**
```yaml
# ✅ Good: Secret used directly in action
- name: Login to GHCR
  uses: docker/login-action@v2
  with:
    password: ${{ secrets.GITHUB_TOKEN }}

# ✅ Good: Secret passed as env var
- name: Connect to VPN
  env:
    PASSWORD: ${{ secrets.GLOBALPROTECT_PASSWORD }}
  run: |
    echo "$PASSWORD" | openconnect ...

# ❌ Bad: Secret echoed (masked but still bad practice)
- run: echo "Password is ${{ secrets.GLOBALPROTECT_PASSWORD }}"

# ❌ Bad: Secret stored in file (persists on runner)
- run: echo "${{ secrets.GLOBALPROTECT_PASSWORD }}" > password.txt
```

**Masking:**
- GitHub automatically masks secret values in logs
- Displays `***` instead of actual value
- Applies to all registered secrets

### Network Security

**VPN:**
- Required for vCluster access
- Encrypts all traffic to clusters
- Authenticates with username/password
- Creates isolated network tunnel

**GHCR:**
- TLS encryption for image push/pull
- Authentication required for private repos
- Rate limiting for unauthenticated pulls

**vCluster:**
- TLS for Platform API
- Access key authentication
- Per-vcluster RBAC
- Isolated namespaces

### Image Security

**Trivy Scanning:**
- Scans for CVEs in OS packages
- Scans for CVEs in libraries
- Checks for misconfigurations
- Detects exposed secrets

**Results:**
- Uploaded to GitHub Security tab
- Visible to repository admins
- Tracked over time
- Alerts for new vulnerabilities

**Action:**
- Scan only (doesn't block pipeline)
- Manual review required
- Fix critical vulnerabilities before production
- Update base images regularly

---

## Troubleshooting

### Build Job Failures

#### Integration Tests Fail

**Symptom:** `Tests run: 2, Failures: 0, Errors: 2`

**Common Causes:**
1. **Disk space exhausted**
   ```
   Error: No space left on device
   ```
   
   **Fix:**
   - Check "Check disk space" step output
   - Review "Clean up Docker" step
   - Increase cleanup aggressiveness
   
2. **Container startup timeout**
   ```
   org.testcontainers.containers.ContainerLaunchException: 
   Timed out waiting for container port to open
   ```
   
   **Fix:**
   - Check container logs in artifacts
   - Increase wait timeout
   - Verify image availability
   
3. **Elasticsearch disk watermark**
   ```
   cluster_block_exception: blocked by: [FORBIDDEN/12/index read-only / allow delete]
   ```
   
   **Fix:**
   - More aggressive disk cleanup
   - Disable disk watermark checks in tests

**Debug Steps:**
1. Download `container-logs-{RUN_ID}` artifact
2. Check Atlas logs for errors
3. Use tmate SSH session if available
4. Review Elasticsearch/Cassandra logs

---

#### Docker Build Fails

**Symptom:** `Error: buildx failed with: ERROR: failed to solve`

**Common Causes:**
1. **Dockerfile syntax error**
   - Review Dockerfile
   - Test build locally
   
2. **Base image not found**
   - Verify base image exists
   - Check registry authentication
   
3. **COPY source not found**
   - Ensure files exist in build context
   - Check .dockerignore

**Debug Steps:**
```bash
# Local test
docker buildx build --platform linux/amd64 .

# Verbose output
docker buildx build --progress=plain .
```

---

#### Docker Push Fails

**Symptom:** `Error: denied: permission denied`

**Fix:**
1. Verify GITHUB_TOKEN permissions
2. Check GHCR package settings
3. Ensure repository visibility matches registry

---

### Smoke Test Job Failures

#### VPN Connection Fails

**Symptom:** `ERROR: OpenConnect exited unexpectedly`

**Common Causes:**
1. **Incorrect credentials**
   - Verify GLOBALPROTECT_USERNAME secret
   - Verify GLOBALPROTECT_PASSWORD secret
   
2. **Wrong portal URL**
   - Verify GLOBALPROTECT_PORTAL_URL variable
   - Test URL from browser
   
3. **Network timeout**
   - VPN gateway unreachable
   - Firewall blocking connection

**Debug Steps:**
```bash
# Manual test (in tmate session)
echo "$PASSWORD" | sudo openconnect \
  --protocol=gp \
  --user="$USERNAME" \
  --passwd-on-stdin \
  vpn.company.com

# Check process
pgrep -x openconnect

# Check logs
sudo journalctl -u openconnect
```

---

#### vCluster Connection Fails

**Symptom:** `fatal unknown flag` or `not found`

**Common Causes:**
1. **Wrong vcluster name**
   - Verify vcluster exists: `vcluster platform list vclusters`
   
2. **Wrong project**
   - Verify project: `vcluster platform list projects`
   
3. **Access key expired**
   - Regenerate: `vcluster platform create accesskey`

**Debug Steps:**
```bash
# List vclusters
vcluster platform list vclusters --project default

# Test connection
KUBECONFIG=test.yaml vcluster platform connect vcluster hkmeta02 --project default
kubectl --kubeconfig=test.yaml get namespaces
```

---

#### Smoke Test Timeout

**AWS/GCP Pass, Azure Fails:**
✅ **Expected behavior** - This proves the CI blind spot!

**All Tests Fail:**

**Symptom:** `ERROR: Rollout failed or timed out`

**Debug Steps:**
1. Check pod status in logs
2. Review events (excluding Normal)
3. Check image pull errors
4. Verify Atlas configuration

**Common Causes:**
1. **Image not found**
   ```
   Failed to pull image: manifest not found
   ```
   
   **Fix:**
   - Verify image was pushed in build job
   - Check image name/tag
   
2. **Insufficient resources**
   ```
   0/56 nodes available: 5 Insufficient cpu, 3 Insufficient memory
   ```
   
   **Fix:**
   - Scale down other deployments
   - Request more resources
   
3. **ConfigMap not mounted**
   ```
   Redis Sentinel configuration not found
   ```
   
   **Fix:**
   - This is the bug we're testing!
   - Expected in Azure
   - Investigate if happens in AWS/GCP

---

#### Port-Forward Fails

**Symptom:** `Error from server: error upgrading connection`

**Debug Steps:**
```bash
# Check service exists
kubectl get svc -n atlas

# Check pods are running
kubectl get pods -n atlas -l app=atlas

# Test direct service connection
kubectl run -it --rm debug --image=curlimages/curl --restart=Never -- \
  curl http://atlas-service-atlas.atlas.svc.cluster.local/api/atlas/admin/status
```

---

## Performance Optimization

### Cache Optimization

**Maven Cache Hit Rate:**
- Target: >90%
- Measure: Check "Restore cache" step duration
- Optimize: Ensure pom.xml hash is stable

### Parallel Execution

**Current:**
- Build job: Sequential (no parallelism)
- Smoke test: Parallel (AWS + Azure simultaneously)

**Improvements:**
- Could parallelize Maven modules
- Could run Trivy scan in parallel with smoke tests

### Runner Selection

**Current:** `ubuntu-latest` (GitHub-hosted)

**Alternatives:**
- Self-hosted runners (faster builds, closer to services)
- Larger runners (more CPU/RAM for parallel builds)

### Docker Layer Caching

**Current:** Not enabled

**Improvement:**
```yaml
- name: Build and push
  uses: docker/build-push-action@v4
  with:
    cache-from: type=registry,ref=ghcr.io/atlanhq/atlas-metastore:buildcache
    cache-to: type=registry,ref=ghcr.io/atlanhq/atlas-metastore:buildcache,mode=max
```

**Benefit:** ~50% faster Docker builds

---

## Metrics & Monitoring

### Job Duration Tracking

**Build Job:**
- Target: <15 minutes
- Typical: 15-20 minutes
- Bottleneck: Integration tests (8-10 min)

**Smoke Test Job:**
- Target: <12 minutes
- Typical: 10-12 minutes
- Bottleneck: Azure rollout timeout (10 min)

### Success Rate

**Expected:**
- Build: 100% (should always pass)
- Smoke Test: 50% (AWS passes, Azure fails)

**Actual Success = Demonstrating the Bug:**
- ✅ Build passes
- ✅ AWS passes
- ❌ Azure fails (proves CI blind spot)

### Resource Usage

**Build Job:**
- CPU: ~50-70% of 2 cores
- Memory: ~4-5 GB of 7 GB
- Disk: ~30 GB of 84 GB (after cleanup)
- Duration: 15-20 min

**Smoke Test Job:**
- CPU: ~20-30% of 2 cores
- Memory: ~1-2 GB of 7 GB
- Disk: ~5 GB of 84 GB
- Duration: 10-12 min

**Cost:**
- GitHub-hosted: Free for public repos
- Private repos: Consumes Actions minutes

---

## Related Documentation

- **High-Level Guide:** `CI_CD_WORKFLOW_GUIDE.md`
- **Smoke Test Script:** `scripts/multi-cloud-smoke-test.sh`
- **Integration Tests:** `webapp/src/test/java/org/apache/atlas/web/integration/`
- **Redis Implementations:**
  - Production: `common/src/main/java/org/apache/atlas/service/redis/RedisServiceImpl.java`
  - Test: `common/src/main/java/org/apache/atlas/service/redis/RedisServiceLocalImpl.java`

---

## Version History

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0.0 | 2025-10-22 | Initial workflow with matrix strategy | Team |
| 2.0.0 | 2025-10-22 | Refactored to parallel script-based approach | Team |

---

## FAQ

### Why does Azure always fail?

**Answer:** This is intentional! Azure has slower ConfigMap mounting (~30-60s), which causes `RedisServiceImpl` to fail initialization. This demonstrates a CI blind spot where integration tests pass (using `local` profile with simple Redis) but production deployments fail due to environment-specific timing issues.

### Why not use GitHub Actions matrix for parallel testing?

**Answer:** Matrix creates separate jobs, each requiring its own VPN connection. This causes:
- VPN authentication conflicts (concurrent sessions)
- Slower execution (sequential VPN connections)
- More complex workflow

The script-based approach uses:
- Single VPN connection
- Single vCluster Platform login
- True parallel execution with background processes

### Can I run smoke tests locally?

**Answer:** Yes! See the high-level guide for instructions:
```bash
# 1. Connect to VPN (GUI or openconnect)
# 2. Login to vCluster Platform
vcluster platform login https://... --access-key ...

# 3. Generate kubeconfigs
KUBECONFIG=kubeconfig-aws.yaml vcluster platform connect ...
KUBECONFIG=kubeconfig-azure.yaml vcluster platform connect ...

# 4. Run script
./scripts/multi-cloud-smoke-test.sh ghcr.io/.../atlas:tag
```

### Why use Temurin JDK instead of others?

**Answer:** Eclipse Temurin (formerly AdoptOpenJDK) is:
- Free and open-source
- TCK-certified (Java compatibility guaranteed)
- Actively maintained by Eclipse Foundation
- Recommended by GitHub Actions

### Why scan with Trivy if it doesn't block the pipeline?

**Answer:** Security scanning is informational:
- Tracks vulnerabilities over time
- Alerts security team to critical CVEs
- Provides fix recommendations
- Helps prioritize security updates

Blocking the pipeline would:
- Prevent urgent hotfixes
- Require manual overrides
- Slow down development

### How do I add GCP to smoke tests?

**Answer:**
1. Get GCP vcluster name from admin
2. Update `Connect to all vClusters` step:
   ```yaml
   KUBECONFIG=kubeconfig-gcp.yaml vcluster platform connect vcluster <gcp-name> --project default
   ```
3. Update script to test GCP:
   ```bash
   bash -c "test_cloud GCP kubeconfig-gcp.yaml" &
   PID_GCP=$!
   ```

### Why do tests use profile="local" instead of fixing the bug?

**Answer:** The workflow's purpose is to **demonstrate** the bug, not fix it. Using profiles allows:
- Integration tests to pass (prove CI works)
- Production deployments to fail on Azure (prove bug exists)
- Clear demonstration of environment-specific issues

To fix the bug, you would need to:
- Add retry logic to `RedisServiceImpl`
- Wait for ConfigMap before Redis init
- Use init containers to ensure ConfigMap mounted

---

## Maintenance

### Regular Updates

**Monthly:**
- Update action versions (@v3 → @v4)
- Review Trivy scan results
- Rotate access keys

**Quarterly:**
- Audit secrets and variables
- Review workflow performance
- Optimize caching strategy

**Annually:**
- Upgrade JDK version
- Review security best practices
- Update documentation

### Breaking Changes

**Action Version Updates:**
1. Check action changelog
2. Test in feature branch
3. Update all usages
4. Update documentation

**JDK Version Updates:**
1. Test build locally
2. Update setup-java version
3. Update Dockerfile base image
4. Test integration tests

### Deprecation Handling

**GitHub Actions:**
- Monitor GitHub blog for deprecations
- Update before deprecation date
- Test thoroughly before merge

**Dependencies:**
- Monitor Dependabot alerts
- Update Maven dependencies
- Rebuild Docker images

---

## Support

### Getting Help

**Internal:**
- Team Slack channel
- DevOps team
- Security team

**External:**
- GitHub Actions docs
- vCluster community
- Stack Overflow

### Reporting Issues

**Workflow Issues:**
1. Capture workflow run URL
2. Download artifacts (logs)
3. Create GitHub issue with:
   - Workflow run link
   - Error messages
   - Expected vs actual behavior
   - Steps to reproduce

**Infrastructure Issues:**
- VPN: Contact IT/Security
- vCluster: Contact Platform team
- GHCR: Contact DevOps

---

**End of Technical Reference**

