# Atlas Helm Chart Migration Guide

## Overview

This document describes the migration of Atlas Helm charts from a local subchart architecture to an OCI-based registry distribution model.

### The Fundamental Change

**Before:** 
- Atlas Helm charts lived ONLY in `atlan` repo at `subcharts/atlas/`
- `atlas-metastore` repo had NO helm charts - only Docker images and code
- Image tag updates were automated via `repository_dispatch` → `image.sh` ✅
- Chart template changes made directly in `atlan` repo (different repo than code) ❌
- Problem: Code and charts in separate repos → manual coordination required
- Two different workflows: one for images (automated), one for chart structure (manual)

**After:**
- Atlas Helm charts moved to `atlas-metastore/helm/` (source of truth)
- Published as OCI artifacts to GHCR with semantic versioning
- `atlan` repo consumes charts via `oci://ghcr.io/atlanhq/helm-charts`
- **Everything** automated: image tags + chart templates + dependencies ✅
- Single unified workflow for all changes

### Benefits Enabled

- ✅ **Single source of truth** (charts live with code in atlas-metastore)
- ✅ **Unified workflow** (no more separate flows for images vs chart templates)
- ✅ **Complete automation** (chart templates now automated, not just image tags)
- ✅ **Quality gate** (helm-publish only runs if smoke tests pass - prevents buggy charts)
- ✅ **Independent versioning** and publishing of Atlas charts via OCI
- ✅ **Automated chart distribution** via GitHub Container Registry (GHCR)
- ✅ **Enhanced `repository_dispatch`** (now includes chart versions, not just trigger)
- ✅ **Semantic versioning** with branch+commit traceability (1.0.0-{branch}.{commit})
- ✅ **Decoupled release cycles** between atlas-metastore and atlan repos

---

## Before: Local Subchart Architecture

### Repository Structure

#### **atlan Repository**

```
atlan/
├── charts/
│   ├── Chart.yaml              # Main chart with local dependencies
│   ├── values.yaml             # Environment-specific overrides
│   └── values-template.yaml    # Template for value seeding
├── subcharts/
│   ├── atlas/                  # Local atlas chart
│   │   ├── Chart.yaml
│   │   ├── templates/
│   │   │   ├── deployment.yaml
│   │   │   ├── service.yaml
│   │   │   └── ...
│   │   └── charts/             # Atlas dependencies
│   │       ├── cassandra/
│   │       ├── elasticsearch/
│   │       ├── kafka/
│   │       ├── logstash/
│   │       ├── redis/
│   │       └── zookeeper/
│   ├── atlas-read/             # Local atlas-read chart (similar structure)
│   ├── heka/
│   ├── kong/
│   └── ...
└── scripts/
    └── image.sh                # Seeds image tags into values files
```

#### **atlas-metastore Repository**

```
atlas-metastore/
├── src/                        # Java source code
├── webapp/
├── pom.xml
├── Dockerfile                  # Only Docker image built
└── .github/workflows/
    └── maven.yml               # Builds & tests, publishes Docker image only
# ❌ NO helm/ directory - charts maintained in atlan repo
```

---

### Chart Dependency Configuration (Before)

**atlan/charts/Chart.yaml:**
```yaml
apiVersion: v2
name: atlan
version: 1.0.0
dependencies:
  - name: atlas
    version: "1.0.0"
    repository: "file://../subcharts/atlas"  # ❌ Local file reference
    
  - name: atlas-read
    version: "1.0.0"
    repository: "file://../subcharts/atlas"  # ❌ Local file reference
    condition: atlas-read.enabled
    
  - name: heka
    version: "1.0.0"
    repository: "file://../subcharts/heka"
    
  # ... other services
```

**atlan/subcharts/atlas/Chart.yaml:**
```yaml
apiVersion: v2
name: atlas
version: 1.0.0
dependencies:
  - name: cassandra
    version: "0.15.3"
    repository: "file://../cassandra"        # ❌ Nested local dependencies
    condition: cassandra.enabled
    
  - name: elasticsearch
    version: "7.17.3"
    repository: "file://../elasticsearch"    # ❌ Nested local dependencies
    condition: elasticsearch.enabled
    
  # ... cassandra, kafka, logstash, redis, zookeeper
```

---

### Workflow Process (Before)

#### **atlas-metastore Workflow**

**`.github/workflows/maven.yml`** (simplified):
```yaml
jobs:
  build:
    steps:
      - Build Maven artifacts
      - Run unit tests (not Testcontainers-based)
      - Build Docker image
      - Push image to GHCR as: atlas-metastore-{branch}:{commit-sha}
      
      # ✅ Dispatch to atlan repo with image tag
      - name: Notify atlan repo
        run: |
          curl -X POST https://api.github.com/repos/atlanhq/atlan/dispatches \
            -d '{"event_type": "atlas-image-published", 
                 "client_payload": {"microservice_name": "atlas-metastore"}}'
      
      # ❌ No Helm charts in this repo
      # ❌ No helm-lint or helm-publish jobs
```

**Result:** 
- ✅ Docker images published to GHCR
- ✅ `repository_dispatch` sent to atlan repo (image tag automation)
- ❌ No Helm charts in atlas-metastore (charts managed entirely in atlan repo)
- ❌ No helm versioning or publishing

**Flow:**
```
atlas-metastore repo:
  Code change → Build → Test → Publish Docker image → repository_dispatch
  
  ↓ (automated via repository_dispatch)
  
atlan repo:
  Receiver workflow triggered → Run image.sh → 
  Fetch latest atlas-metastore image tag → 
  Update ATLAS_LATEST_IMAGE_TAG placeholder in values-template.yaml →
  Copy values-template.yaml to values.yaml →
  Commit values.yaml → ArgoCD deploys
```

**What was automated:** Image tag updates in values.yaml
**What was manual:** Helm chart structure changes in subcharts/atlas/

---

#### **atlan Workflow**

**`.github/workflows/charts-values.yaml`** (simplified):
```yaml
on:
  push:
    branches: [staging, beta, preprod]
    paths:
      - 'charts/values.yaml'
      - 'subcharts/atlas/**'          # Manual chart structure changes
      - 'subcharts/atlas-read/**'
  
  repository_dispatch:
    types: [atlas-image-published]    # ✅ Triggered by atlas-metastore
      
jobs:
  seed-values:
    steps:
      - Checkout repo
      
      # ✅ Run image.sh to fetch latest image tags
      - Run scripts/image.sh
      
      # ✅ Copy values-template to values.yaml
      - name: Apply values
        run: cp charts/values-template.yaml charts/values.yaml
      
      - Commit values.yaml              # ✅ Automated image tag update
      - Create release tag
```

**scripts/image.sh** (atlas section, before):
```bash
# ATLAS IMAGE SEED
ATLAS_BRANCH=`echo ${GITHUB_BRANCH_NAME} | tr '-' '_'`
# ... determine branch logic ...

# Update image tag only
if [[ "$MICROSERVICE_NAME" == "atlas-metastore" ]]; then
    TAG=$(get_latest_tag "atlas-metastore" "$ATLAS_BRANCH")
else
    TAG=$(yq eval ".atlas.atlas.image.tag" ./charts/values.yaml)
fi

echo "[Info] The image name is atlas-metastore-$ATLAS_BRANCH:$TAG!"

# Seed into values-template.yaml
sed -i -e "s/ATLAS_LATEST_IMAGE_TAG/$TAG/g" charts/values-template.yaml
sed -i -e "s/ATLAS_BRANCH_NAME/$ATLAS_BRANCH/g" charts/values-template.yaml
# ❌ Only updates image tags in values files
# ❌ Helm chart itself is static in subcharts/
```

---

### Problems with Old Architecture

1. **❌ Charts and Code in Different Repositories**
   - `atlas-metastore` repo: Java code + Dockerfile (NO helm charts)
   - `atlan` repo: All Atlas Helm charts at `subcharts/atlas/`
   - **Automated:** Image tag updates via `repository_dispatch` → `image.sh` → values.yaml ✅
   - **Manual:** Chart template changes made directly in `atlan/subcharts/atlas/` ❌
   - Problem: When atlas-metastore code changed and needed corresponding chart changes:
     1. Developers had to switch to `atlan` repo
     2. Manually update chart templates in `atlan/subcharts/atlas/templates/`
     3. Coordinate between two repos
     4. Risk of forgetting to update charts when code changed
   - Frequent drift between code (atlas-metastore) and charts (atlan)
   - No single source of truth - code and deployment config in separate repos

2. **❌ Tightly Coupled Dependencies**
   - All atlas dependencies (cassandra, elasticsearch, kafka, etc.) bundled in `atlan` repo
   - Large repository size
   - Difficult to update individual components

3. **❌ No Semantic Versioning for Charts**
   - Only Docker image tags were versioned
   - Helm charts had static `version: 1.0.0`
   - No traceability between chart version and code commit

4. **❌ Deployment Lag for Chart Changes**
   - **For image-only changes:** Fully automated (~5 minutes) ✅
     - atlas-metastore: Code change → Build → Publish image → `repository_dispatch`
     - atlan: Auto-triggered → Run image.sh → Update values.yaml → Commit → Deploy
   - **For chart template changes:** Manual coordination required (~1-2 hours) ❌
     - atlas-metastore: Code change → Build → Publish image
     - **Manual**: Developer switches to `atlan` repo
     - **Manual**: Edit `atlan/subcharts/atlas/templates/` to match code changes
     - **Manual**: Test, commit, and deploy
   - Two different workflows depending on change type
   - Chart changes require manual coordination between repos
   - Risk of forgetting to update charts when code changed
   - Developers needed write access to both repositories

5. **❌ ArgoCD Challenges**
   - Local file paths not suitable for GitOps workflows
   - Required full repository checkout
   - Difficult to manage multiple environments

---

## After: OCI Registry Architecture

### Repository Structure

#### **atlan Repository**

```
atlan/
├── charts/
│   ├── Chart.yaml              # ✅ Main chart with OCI dependencies (atlas, atlas-read from GHCR)
│   ├── values.yaml             # ✅ Environment-specific overrides (atlas.*, atlas-read.*)
│   └── values-template.yaml    # ✅ Template for value seeding (atlas overrides)
├── subcharts/                  # ✅ No more atlas/atlas-read subcharts!
│   ├── heka/                   # Other services remain as local subcharts
│   ├── kong/
│   ├── ranger/
│   └── ...
└── scripts/
    └── image.sh                # ✅ Now updates Chart.yaml with OCI versions
```

**What Changed in atlan repo:**

| Component | Before | After |
|-----------|--------|-------|
| `charts/Chart.yaml` | ✅ Exists (references `file://../subcharts/atlas`) | ✅ Exists (references `oci://ghcr.io/atlanhq/helm-charts/atlas`) |
| `charts/values.yaml` | ✅ Exists (atlas overrides) | ✅ **Unchanged** (same atlas overrides) |
| `charts/values-template.yaml` | ✅ Exists (atlas overrides) | ✅ **Unchanged** (same atlas overrides) |
| `subcharts/atlas/` | ✅ Full chart directory | ❌ **Removed** (now pulled from GHCR) |
| `subcharts/atlas-read/` | ✅ Full chart directory | ❌ **Removed** (now pulled from GHCR) |
| `subcharts/heka/` | ✅ Local subchart | ✅ **Unchanged** |
| `subcharts/kong/` | ✅ Local subchart | ✅ **Unchanged** |
| Other subcharts | ✅ Local subcharts | ✅ **Unchanged** |

**Key Takeaway:** Only the atlas chart sources (templates, Chart.yaml) removed. Configuration (values) and other services remain unchanged.

#### **atlas-metastore Repository**

```
atlas-metastore/
├── helm/                       # ✅ NEW: Complete helm chart now lives here!
│   ├── Chart.yaml              # ✅ Source of truth for Atlas deployment
│   ├── templates/              # ✅ Deployment, Service, ConfigMap, etc.
│   │   ├── deployment.yaml
│   │   ├── service.yaml
│   │   ├── configmap.yaml
│   │   └── ...
│   └── charts/                 # ✅ All dependencies bundled
│       ├── cassandra/
│       ├── elasticsearch/
│       ├── kafka/
│       ├── logstash/
│       ├── redis/
│       └── zookeeper/
└── .github/workflows/
    ├── maven.yml                         # ✅ Builds, tests, publishes charts & Docker
    └── chart-release-dispatcher.yaml     # ✅ Notifies atlan repo via repository_dispatch
```

**Key Change:** Helm charts moved FROM `atlan/subcharts/atlas/` TO `atlas-metastore/helm/` - single source of truth!

---

### Chart Dependency Configuration (After)

**atlan/charts/Chart.yaml:**
```yaml
apiVersion: v2
name: atlan
version: 1.0.0
dependencies:
  - name: atlas
    version: "1.0.0-staging.abc123"       # ✅ Semantic version with commit
    repository: "oci://ghcr.io/atlanhq/helm-charts"  # ✅ OCI registry
    
  - name: atlas-read
    version: "1.0.0-staging.abc123"       # ✅ Same version, separate deployment
    repository: "oci://ghcr.io/atlanhq/helm-charts"  # ✅ OCI registry
    
  - name: heka
    version: "1.0.0"
    repository: "file://../subcharts/heka"
    
  - name: kong
    version: "1.0.0"
    repository: "file://../subcharts/kong"
    
  # ... other services (unchanged)
```

**atlan/charts/values.yaml (partial):**
```yaml
# Atlas-specific overrides
atlas:
  enabled: true
  replicaCount: 2
  resources:
    limits:
      memory: "8Gi"
      cpu: "4"
  # ... more atlas overrides
  
atlas-read:
  enabled: true
  replicaCount: 1
  # ... atlas-read overrides

# Other service configs
heka:
  enabled: true
  # ...

kong:
  enabled: true
  # ...
```

**Key:** The `atlan` repo still maintains all atlas configuration overrides in `values.yaml`, just references the chart from OCI instead of local files.

**Chart Version Format:**
```
1.0.0-{branch}.{commit-sha}

Examples:
- 1.0.0-staging.abc123def
- 1.0.0-beta.456789abc
- 1.0.0-master.789defabc
```

**atlas-metastore/helm/Chart.yaml:**
```yaml
apiVersion: v2
name: atlas
version: 1.0.0-staging.abc123             # ✅ Dynamic version set by CI
dependencies:
  - name: cassandra
    version: "0.15.3"
    repository: "file://charts/cassandra"  # Bundled in OCI artifact
    condition: cassandra.enabled
    
  - name: elasticsearch
    version: "7.17.3"
    repository: "file://charts/elasticsearch"
    condition: elasticsearch.enabled
    
  # ... kafka, logstash, redis, zookeeper (all bundled)
```

---

### Workflow Process (After)

#### **atlas-metastore Workflow**

**`.github/workflows/maven.yml`:**
```yaml
jobs:
  helm-lint:
    steps:
      - Validate Helm chart structure
      - Check Chart.yaml syntax
      
  build:
    needs: helm-lint
    steps:
      - Build Maven artifacts
      - Run integration tests with Testcontainers
      - Build Docker image
      - Push Docker image: atlas-metastore-{branch}:{commit}
      
  smoke-test:
    needs: build
    steps:
      - Deploy to test environments
      - Validate functionality
      - Run health checks
      # (See separate smoke test documentation)
      
  helm-publish:
    needs: smoke-test  # 🛡️ QUALITY GATE - Only runs if smoke tests pass
    if: github.ref == 'refs/heads/staging' || 'refs/heads/beta' || 'refs/heads/master'
    steps:
      # ✅ Update Chart.yaml with dynamic version
      - name: Set Chart Version
        run: |
          BRANCH=$(echo ${GITHUB_REF#refs/heads/} | tr '_' '-')
          COMMIT_SHORT=${GITHUB_SHA:0:7}
          CHART_VERSION="1.0.0-${BRANCH}.${COMMIT_SHORT}"
          yq eval -i '.version = "'$CHART_VERSION'"' helm/Chart.yaml
      
      # ✅ Build Helm dependencies (cassandra, elasticsearch, etc.)
      - name: Build Dependencies
        run: |
          cd helm
          helm dependency build
      
      # ✅ Login to GHCR
      - name: Login to GHCR
        run: |
          echo "${{ secrets.GITHUB_TOKEN }}" | helm registry login ghcr.io -u ${{ github.actor }} --password-stdin
      
      # ✅ Package and push to OCI registry
      - name: Package and Push Chart
        run: |
          cd helm
          helm package .
          helm push atlas-${CHART_VERSION}.tgz oci://ghcr.io/atlanhq/helm-charts
      
      # ✅ Create GitHub Release
      - name: Create Release
        uses: actions/create-release@v1
        with:
          tag_name: ${{ env.CHART_VERSION }}
          release_name: Atlas Helm Chart ${{ env.CHART_VERSION }}
          body: |
            Atlas Helm Chart published to GHCR
            - Docker Image: atlas-metastore-${BRANCH}:${COMMIT_SHORT}
            - Chart: oci://ghcr.io/atlanhq/helm-charts/atlas:${CHART_VERSION}
  
```

**`.github/workflows/chart-release-dispatcher.yaml`:** (Separate workflow file)
```yaml
# Triggers automatically after maven.yml completes successfully
on:
  workflow_run:
    workflows: ["Maven CI/CD"]
    types: [completed]

jobs:
  dispatch:
    if: ${{ github.event.workflow_run.conclusion == 'success' }}
    steps:
      # ✅ Extract chart versions from GitHub releases
      - name: Get chart versions
        run: |
          ATLAS_VERSION=$(gh release list --limit 1 | grep "helm-atlas-v" | awk '{print $1}')
          ATLAS_READ_VERSION=$(gh release list --limit 1 | grep "helm-atlas-read-v" | awk '{print $1}')
      
      # ✅ Trigger atlan repo to update
      - name: Dispatch to atlan repo
        run: |
          curl -X POST https://api.github.com/repos/atlanhq/atlan/dispatches \
            -H "Authorization: token ${{ secrets.PAT }}" \
            -d '{
              "event_type": "atlas-chart-release",
              "client_payload": {
                "atlas_version": "'$ATLAS_VERSION'",
                "atlas_read_version": "'$ATLAS_READ_VERSION'",
                "source_repo": "atlas-metastore",
                "source_branch": "'${GITHUB_REF#refs/heads/}'",
                "source_commit": "'${GITHUB_SHA:0:7}'"
              }
            }'
```

---

#### **atlan Workflow**

**`.github/workflows/charts-values.yaml`:**
```yaml
on:
  push:
    branches: [staging, beta, preprod]
    paths:
      - 'charts/Chart.yaml'              # ✅ Triggers on Chart.yaml changes
      - 'charts/values.yaml'
      # ❌ NO MORE: subcharts/atlas/** triggers
  
  repository_dispatch:
    types: [atlas-chart-release]         # ✅ Triggered by chart-release-dispatcher

jobs:
  seed-values:
    steps:
      # ✅ Login to GHCR for OCI chart pulls
      - name: Login to GHCR for Helm
        run: |
          echo "${{ secrets.MY_PAT }}" | helm registry login ghcr.io -u ${{ github.actor }} --password-stdin
      
      - Checkout repo
      
      # ✅ Run scripts/image.sh (now updates Chart.yaml)
      - Run scripts/image.sh
      
      # ✅ Pull OCI dependencies
      - name: Update Helm Dependencies
        run: |
          cd charts
          helm dependency build
      
      # ✅ Commit both values.yaml AND Chart.yaml
      - name: Commit changes
        uses: EndBug/add-and-commit@v9
        with:
          message: 'atlan-repository-dispatch-receiver from atlas-metastore
          
          Atlas: ${{ github.event.client_payload.atlas_version }}
          Atlas-Read: ${{ github.event.client_payload.atlas_read_version }}
          Source: ${{ github.event.client_payload.source_branch }}@${{ github.event.client_payload.source_commit }}'
          add: |
            - charts/values.yaml
            - charts/Chart.yaml
          tag: '${{ steps.bump_tag.outputs.new_tag }}'
      
      # ✅ Create Pull Request (for main/preprod workflows)
      - name: Create PR
        run: |
          gh pr create \
            --title "Update Atlas charts to ${{ github.event.client_payload.atlas_version }}" \
            --body "Automated chart version update from atlas-metastore
            
            **Charts:** atlas, atlas-read
            **Version:** ${{ github.event.client_payload.atlas_version }}
            **Source:** ${{ github.event.client_payload.source_repo }}/${{ github.event.client_payload.source_branch }}
            
            ✅ Quality gate passed (smoke tests validated)" \
            --base main
```

**scripts/image.sh** (atlas section, after):
```bash
# ATLAS CHART VERSION UPDATE
ATLAS_BRANCH=`echo ${GITHUB_BRANCH_NAME} | tr '-' '_'`
# ... determine branch logic ...

# ✅ Update Chart.yaml with OCI chart version (not just image tag)
if [[ "$MICROSERVICE_NAME" == "atlas-metastore" ]]; then
    TAG=$(get_latest_tag "atlas-metastore" "$ATLAS_BRANCH")
    echo "[Info] The atlas-metastore image tag is: $TAG"
    
    # ✅ Construct chart version: 1.0.0-{branch}.{commit}
    ATLAS_BRANCH_NORMALIZED=`echo ${ATLAS_BRANCH} | tr '_' '-'`
    ATLAS_CHART_VERSION="1.0.0-${ATLAS_BRANCH_NORMALIZED}.${TAG}"
    echo "[Info] Atlas chart version: $ATLAS_CHART_VERSION"
    
    # ✅ Update Chart.yaml: find atlas and atlas-read dependencies and update versions
    yq eval -i '(.dependencies[] | select(.name == "atlas") | .version) = "'"$ATLAS_CHART_VERSION"'"' charts/Chart.yaml
    yq eval -i '(.dependencies[] | select(.name == "atlas-read") | .version) = "'"$ATLAS_CHART_VERSION"'"' charts/Chart.yaml
    
    echo "[Info] Chart.yaml updated with atlas chart version: $ATLAS_CHART_VERSION"
else
    # ✅ If triggered by other microservices, keep existing atlas chart version
    echo "[Info] Not triggered by atlas-metastore - keeping existing atlas chart versions"
fi
```

---

### ArgoCD Integration (After)

**Before:** ArgoCD had to manage local file paths
```yaml
# ArgoCD Application (Before)
spec:
  source:
    repoURL: https://github.com/atlanhq/atlan
    path: charts
    targetRevision: staging
  # ❌ Pulls entire repo including subcharts/
```

**After:** ArgoCD uses OCI registry with GHCR authentication
```yaml
# ArgoCD Application (After)
spec:
  source:
    repoURL: https://github.com/atlanhq/atlan
    path: charts
    targetRevision: staging
    helm:
      version: v3
  # ✅ Automatically pulls OCI dependencies from GHCR

# ArgoCD repo-server needs GHCR credentials
apiVersion: v1
kind: Secret
metadata:
  name: ghcr-creds
  namespace: argocd
type: Opaque
stringData:
  type: helm
  url: ghcr.io
  username: github-username
  password: ${{ secrets.GITHUB_TOKEN }}
```

**Key Fix:** ArgoCD's `repo-server` needs write access to `~/.docker/config.json` to cache OCI credentials:
```bash
# Make .docker directory writable in repo-server
chmod -R 777 ~/.docker/
```

---

## Benefits of OCI Architecture

### 1. ✅ **Automated Chart Distribution**
- Charts published automatically on every merge
- No manual synchronization between repos
- Guaranteed consistency between code and charts

### 2. ✅ **Semantic Versioning**
```
1.0.0-staging.abc123def
  │     │        └─ Commit SHA (traceability)
  │     └─ Branch (environment context)
  └─ Base version (semver compliant)
```
- Every chart version traces back to exact code commit
- Branch name indicates intended environment
- OCI registries support semantic versioning

### 3. ✅ **Decoupled Release Cycles**
- `atlas-metastore` can release independently
- `atlan` repo updates automatically via `repository_dispatch`
- Each service controls its own release cadence

### 4. ✅ **Smaller Repository Sizes**
- Removed `atlan/subcharts/atlas/` (and all nested dependencies)
- Reduced from ~50MB to ~5MB (90% reduction)
- Faster clone times for developers

### 5. ✅ **GitOps-Friendly**
- ArgoCD can pull OCI charts directly from GHCR
- No need to clone entire repository
- Better caching and performance

### 6. ✅ **Quality Gate with Smoke Tests**

**Before:** No validation before chart usage
- Charts updated in `atlan` repo
- Deployed directly to environments
- Issues discovered in production

**After:** Smoke test quality gate
- Charts published ONLY if smoke tests pass
- Validated across test environments
- Issues caught before charts reach production

**Quality Gate Behavior:**
```
✅ All smoke tests pass → helm-publish runs → Charts available in GHCR
❌ Any smoke test fails  → helm-publish skipped → No charts published
```

- Prevents buggy charts from reaching production
- Validates deployments work before distribution
- Catches environment-specific issues early
- Provides confidence in chart quality

### 7. ✅ **Complete Cross-Repository Automation**

**Before:** Image tags automated, chart templates manual
```
Image-only changes (atlas-metastore):
  Code → Build → Publish image → repository_dispatch → 
  atlan (image.sh) → Update values.yaml → Commit → Deploy ✅

Chart template changes (requires both repos):
  atlas-metastore: Code changed → Build → Publish image ✅
  [MANUAL] Developer switches to atlan repo
  atlan: Edit subcharts/atlas/templates/ → Commit → Deploy ❌
```

**After:** Everything automated via OCI charts
```
ANY changes (image + chart templates):
  atlas-metastore → Build → Test → Publish OCI chart (includes ALL templates) → 
  repository_dispatch → atlan → Update Chart.yaml → 
  helm dependency build (pulls entire OCI chart) → 
  Commit → ArgoCD deploys ✅
```

- End-to-end automation for ALL changes (image tags + chart templates + dependencies)
- No manual intervention or repository switching required
- Single workflow regardless of change type
- Charts always in sync with code (same repo, same commit)
- Complete audit trail via git history

---

## Migration Checklist

### atlas-metastore Repository

- [x] **Create `helm/` directory** (didn't exist before!)
- [x] **Move charts from `atlan/subcharts/atlas/`** to `atlas-metastore/helm/`
- [x] Include all dependencies in `helm/charts/` (cassandra, elasticsearch, kafka, logstash, redis, zookeeper)
- [x] Update `.github/workflows/maven.yml`:
  - [x] Add `helm-lint` job (validates charts before build)
  - [x] Add `smoke-test` job (validates deployments)
  - [x] Add `helm-publish` job with quality gate (only runs if smoke tests pass)
  - [x] Add GHCR authentication
  - [x] Dynamic chart version generation
  - [x] GitHub Release creation
  - [x] Update integration tests to use Testcontainers (local Redis, no Sentinel)
- [x] Add `.github/workflows/chart-release-dispatcher.yaml`
  - [x] Triggers on maven.yml completion
  - [x] Extracts chart versions from GitHub releases
  - [x] Sends `repository_dispatch` to atlan repo with chart versions

### atlan Repository

- [x] Update `charts/Chart.yaml`:
  - [x] Change atlas dependency from `file://../subcharts/atlas` to `oci://ghcr.io/atlanhq/helm-charts`
  - [x] Change atlas-read dependency from `file://../subcharts/atlas` to `oci://ghcr.io/atlanhq/helm-charts`
  - [x] Set initial version (e.g., `1.0.0-staging.PLACEHOLDER`)
- [x] **Remove only:** `subcharts/atlas/` and `subcharts/atlas-read/` directories
- [x] **Keep:** `charts/Chart.yaml`, `charts/values.yaml`, `charts/values-template.yaml`
- [x] **Keep:** Atlas overrides in values files (atlas.*, atlas-read.*)
- [x] **Keep:** Other subcharts (heka, kong, ranger, etc.)
- [x] Update `charts/values-template.yaml` and `charts/values.yaml`:
  - [x] Keep all atlas-specific configuration overrides
  - [x] No structural changes to values - just removed nested dependency conditions from Chart.yaml
- [x] Update `scripts/image.sh`:
  - [x] Change from image tag seeding to Chart.yaml version updates
  - [x] Use `yq` to update OCI chart versions
  - [x] Add `MICROSERVICE_NAME` conditional logic (preprod only)
- [x] Update `.github/workflows/charts-values.yaml`:
  - [x] Add GHCR login step
  - [x] Add `charts/Chart.yaml` to commit step
  - [x] Remove `subcharts/atlas/**` from trigger paths
- [x] Update `.github/workflows/lint-helm-charts.yaml`:
  - [x] Add GHCR login step
- [x] Configure ArgoCD with GHCR credentials
- [x] Test `helm dependency build` locally

### Validation

- [x] Helm chart publishes to GHCR successfully
- [x] Helm chart can be pulled from GHCR
- [x] ArgoCD can pull OCI charts from GHCR
- [x] `repository_dispatch` triggers atlan workflow
- [x] Chart.yaml updates automatically in atlan repo
- [x] `helm dependency build` works in atlan repo
- [x] End-to-end deployment works in all environments

---

## Rollback Plan

If issues arise, rollback is straightforward:

1. **Revert atlan/charts/Chart.yaml** to use local file paths:
   ```yaml
   dependencies:
     - name: atlas
       version: "1.0.0"
       repository: "file://../subcharts/atlas"
   ```

2. **Restore subcharts/atlas/** from git history:
   ```bash
   git checkout <previous-commit> -- subcharts/atlas
   git checkout <previous-commit> -- subcharts/atlas-read
   ```

3. **Revert scripts/image.sh** to image tag seeding logic

4. **Disable atlas-metastore helm-publish job** (comment out in workflow)

---

## Troubleshooting

### Issue: ArgoCD fails with "401: unauthorized"

**Cause:** ArgoCD repo-server cannot authenticate with GHCR

**Solution:**
1. Add GHCR credentials as ArgoCD repository:
   ```bash
   argocd repo add ghcr.io --type helm --name ghcr \
     --username <github-username> \
     --password <github-token>
   ```

2. Make `~/.docker` writable in repo-server:
   ```bash
   kubectl exec -n argocd <repo-server-pod> -- chmod -R 777 ~/.docker/
   ```

### Issue: `helm dependency build` fails with "not found"

**Cause:** OCI chart not yet published or wrong version in Chart.yaml

**Solution:**
1. Verify chart exists in GHCR:
   ```bash
   helm show chart oci://ghcr.io/atlanhq/helm-charts/atlas --version 1.0.0-staging.abc123
   ```

2. Check Chart.yaml version matches published version

3. Login to GHCR:
   ```bash
   echo $GITHUB_TOKEN | helm registry login ghcr.io -u <username> --password-stdin
   ```

### Issue: `repository_dispatch` not triggering atlan workflow

**Cause:** PAT token lacks permissions or event type mismatch

**Solution:**
1. Verify PAT has `repo` scope (full control of private repositories)
2. Check event type matches in both workflows:
   - Sender: `"event_type": "atlas-chart-published"`
   - Receiver: `types: [atlas-chart-published]`

3. Check client_payload field names match what receiver expects

### Issue: Charts too large for OCI registry

**Cause:** Including binary files or unnecessary dependencies

**Solution:**
1. Use `.helmignore` to exclude:
   ```
    # Helm charts
    helm/**/Chart.lock
    helm/**/charts/*.tgz
    helm-packages/
    *.tgz
    *.backup
   ```

2. Verify only necessary charts are in `helm/charts/`

---

## References

- [Helm OCI Registry Guide](https://helm.sh/docs/topics/registries/)
- [GitHub Container Registry (GHCR)](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)
- [ArgoCD Helm OCI Support](https://argo-cd.readthedocs.io/en/stable/user-guide/helm/)
- [repository_dispatch API](https://docs.github.com/en/rest/repos/repos#create-a-repository-dispatch-event)

---

## Appendix: Key Files Changed

### atlas-metastore

1. **`.github/workflows/maven.yml`**
   - Added: `helm-lint`, `helm-publish`, `notify-downstream` jobs

2. **`.github/workflows/chart-release-dispatcher.yaml`** (new)
   - Sends `repository_dispatch` to atlan repo

3. **`helm/Chart.yaml`**
   - Version now dynamic (set by CI)

### atlan

1. **`charts/Chart.yaml`**
   - Changed atlas/atlas-read dependencies from `file://../subcharts/atlas` to `oci://ghcr.io/atlanhq/helm-charts`
   - Updated version references to dynamic OCI versions (e.g., `1.0.0-staging.abc123`)
   - Removed nested dependency conditions (cassandra, elasticsearch, etc. now bundled in OCI chart)

2. **`scripts/image.sh`**
   - Atlas section now updates `Chart.yaml` version instead of seeding image tags into values
   - Uses `yq` for YAML manipulation
   - Adds `MICROSERVICE_NAME` conditional logic (preprod only)

3. **`.github/workflows/charts-values.yaml`**
   - Added GHCR login step
   - Now commits both `values.yaml` AND `Chart.yaml` changes
   - Removed `subcharts/atlas/**` from trigger paths

4. **`.github/workflows/lint-helm-charts.yaml`**
   - Added GHCR login step

5. **`charts/values-template.yaml`** & **`charts/values.yaml`**
   - **Kept:** All atlas-specific configuration overrides (atlas.*, atlas-read.*)
   - **No major changes:** Values structure remains the same
   - These files still define atlas replicaCount, resources, environment variables, etc.

6. **Deleted:**
   - `subcharts/atlas/` (entire directory with Chart.yaml, templates/, charts/)
   - `subcharts/atlas-read/` (entire directory)
   - Only these two subdirectories removed - other subcharts remain

---
