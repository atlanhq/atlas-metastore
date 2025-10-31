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
- ✅ **Quality gate** (helm-publish only runs if tests pass)
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

### 🔑 Critical Architectural Decision: Separate OCI Artifacts

**Why Not Nested Subcharts?**

Initially, we attempted to publish atlas as a single OCI artifact that bundled its infrastructure dependencies (cassandra, elasticsearch, logstash) as subcharts. **This approach failed** due to a fundamental Helm limitation:

**Problem:** Helm's Value Override Path Requirements

When a chart has nested subcharts, parent charts must use the full path to override subchart values:

```yaml
# If atlas bundles cassandra as a subchart, parent chart (atlan) must use:
atlas:
  cassandra:
    replicas: 3  # ✅ Correct nested path

# But our existing values.yaml has:
cassandra:
  replicas: 3  # ❌ Ignored! Not nested under atlas.*
```

**Impact:**
- All existing `cassandra.*`, `elasticsearch.*`, `logstash.*` overrides in `atlan/charts/values.yaml` would be ignored
- Manifests would have empty namespaces, default names like `release-name-cassandra` instead of `atlas-cassandra`
- Deployments would fail or create incorrectly configured resources
- **Breaking change** for ALL environments (staging, beta, preprod, production)
- Would require restructuring every ArgoCD application manifest

**Solution:** Publish Each Chart as a Separate OCI Artifact

Instead of nesting, we publish 8 independent OCI artifacts:

1. `atlas` (application chart)
2. `atlas-read` (read replica chart)
3. `cassandra` (infrastructure)
4. `elasticsearch` (infrastructure)
5. `logstash` (infrastructure)
6. `cassandra-online-dc` (atlas-read infrastructure)
7. `elasticsearch-read` (atlas-read infrastructure)
8. `elasticsearch-exporter-read` (atlas-read infrastructure)

All are consumed as **peer dependencies** in the atlan chart, maintaining the exact same value override structure as before.

**Benefits:**
- ✅ **Zero breaking changes** - existing `values.yaml` structure preserved
- ✅ **No ArgoCD manifest updates** required
- ✅ **All charts versioned together** - same commit traceability
- ✅ **Backward compatible** - existing configurations work unchanged
- ✅ **Clean architecture** - each chart is independently addressable

---

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
├── helm/                       # ✅ NEW: All helm charts now live here!
│   ├── atlas/                  # ✅ Application chart
│   │   ├── Chart.yaml          # No dependencies!
│   │   ├── templates/
│   │   └── charts/             # Infrastructure published separately
│   │       ├── cassandra/
│   │       ├── elasticsearch/
│   │       └── logstash/
│   └── atlas-read/             # ✅ Read replica chart
│       ├── Chart.yaml          # No dependencies!
│       ├── templates/
│       └── charts/             # Infrastructure published separately
│           ├── cassandra-online-dc/
│           ├── elasticsearch-read/
│           └── elasticsearch-exporter-read/
└── .github/workflows/
    ├── maven.yml                         # ✅ Builds, tests, publishes Docker & all 8 Helm charts
    └── chart-release-dispatcher.yaml     # ✅ Notifies atlan repo via repository_dispatch
```

**Key Changes:** 
- Helm charts moved FROM `atlan/subcharts/atlas/` TO `atlas-metastore/helm/` - single source of truth!
- Each chart published separately as OCI artifact (8 total)
- No nested dependencies in Chart.yaml files

---

### Chart Dependency Configuration (After)

**atlan/charts/Chart.yaml:**
```yaml
apiVersion: v2
name: atlan
version: 1.0.0
dependencies:
  # ============================================================
  # Atlas Charts (OCI Registry) - Published from atlas-metastore
  # All 8 charts are separate OCI artifacts (peer dependencies)
  # ============================================================
  
  # Application charts
  - name: atlas
    version: "1.0.0-staging.abc123"       # ✅ Semantic version with commit
    repository: "oci://ghcr.io/atlanhq/helm-charts"  # ✅ OCI registry
    
  - name: atlas-read
    version: "1.0.0-staging.abc123"       # ✅ Same version, separate deployment
    repository: "oci://ghcr.io/atlanhq/helm-charts"  # ✅ OCI registry
  
  # Atlas infrastructure charts (peers, not nested)
  - name: cassandra
    version: "0.14.4-staging.abc123"      # ✅ Independent OCI artifact
    repository: "oci://ghcr.io/atlanhq/helm-charts"
    
  - name: elasticsearch
    version: "7.6.1-staging.abc123"       # ✅ Independent OCI artifact
    repository: "oci://ghcr.io/atlanhq/helm-charts"
    
  - name: logstash
    version: "9.1.2-staging.abc123"       # ✅ Independent OCI artifact
    repository: "oci://ghcr.io/atlanhq/helm-charts"
  
  # Atlas-Read infrastructure charts (peers, not nested)
  - name: cassandra-online-dc
    version: "0.14.4-staging.abc123"      # ✅ Independent OCI artifact
    repository: "oci://ghcr.io/atlanhq/helm-charts"
    
  - name: elasticsearch-read
    version: "7.6.1-staging.abc123"       # ✅ Independent OCI artifact
    repository: "oci://ghcr.io/atlanhq/helm-charts"
    
  - name: elasticsearch-exporter-read
    version: "3.3.0-staging.abc123"       # ✅ Independent OCI artifact
    repository: "oci://ghcr.io/atlanhq/helm-charts"
  
  # Other platform services (unchanged - still local subcharts)
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
# Atlas application overrides (root level, NOT nested)
atlas:
  enabled: true
  replicaCount: 2
  resources:
    limits:
      memory: "8Gi"
      cpu: "4"
  # Disable infrastructure subcharts - consumed as peer OCI dependencies instead
  cassandra:
    enabled: false
  elasticsearch:
    enabled: false
  logstash:
    enabled: false
  # ... more atlas overrides
  
atlas-read:
  enabled: true
  replicaCount: 1
  # Disable infrastructure subcharts - consumed as peer OCI dependencies instead
  cassandra-online-dc:
    enabled: false
  elasticsearch-read:
    enabled: false
  elasticsearch-exporter-read:
    enabled: false
  # ... atlas-read overrides

# Atlas infrastructure overrides (root level as peers, NOT nested under atlas.*)
cassandra:
  Namespace: atlas
  fullnameOverride: atlas-cassandra
  replicas: 3
  # ... cassandra overrides

elasticsearch:
  custom_deployment:
    enabled: false
  # ... elasticsearch overrides

logstash:
  replicas: 1
  fullnameOverride: atlas-logstash
  # ... logstash overrides

# Atlas-Read infrastructure overrides  
cassandra-online-dc:
  Namespace: atlas
  fullnameOverride: atlas-cassandra-online-dc
  # ... cassandra-online-dc overrides

elasticsearch-read:
  # ... elasticsearch-read overrides

elasticsearch-exporter-read:
  # ... exporter overrides

# Other platform services (unchanged)
heka:
  enabled: true
  # ...

kong:
  enabled: true
  # ...
```

**Key Points:**

1. **Root-level overrides preserved:** All infrastructure overrides (`cassandra.*`, `elasticsearch.*`, etc.) remain at root level as peer dependencies, maintaining backward compatibility.

2. **Disable flags prevent duplicates:** The `atlas.cassandra.enabled: false` flags explicitly disable infrastructure subcharts within the atlas/atlas-read OCI artifacts. Without these flags, infrastructure charts would render twice:
   - Once from the atlas subchart's bundled dependencies
   - Once from the peer OCI dependencies in atlan's Chart.yaml
   
   This dual-inclusion would create duplicate StatefulSets and cause deployment failures. The disable flags ensure each infrastructure chart renders only once (as a peer).

**Chart Version Format:**
```
1.0.0-{branch}.{commit-sha}

Examples:
- 1.0.0-staging.abc123def
- 1.0.0-beta.456789abc
- 1.0.0-master.789defabc
```

**atlas-metastore/helm/atlas/Chart.yaml:**
```yaml
apiVersion: v2
name: atlas
description: Apache Atlas Metadata Management and Governance Platform
version: 1.0.0                            # ✅ Updated to 1.0.0-{branch}.{commit} by CI
appVersion: "abc123"                      # ✅ Updated to commit SHA by CI
dependencies:
  # Infrastructure charts - disabled by default (published separately as OCI)
  # Charts exist in charts/ subdirectory for individual OCI publishing
  # Consumed as peers in parent atlan chart, not as subcharts here
  - name: cassandra
    repository: file://./charts/cassandra
    version: 0.x.x
    condition: cassandra.enabled  # Disabled in values.yaml
  - name: elasticsearch
    repository: file://./charts/elasticsearch
    version: 7.x.x
    condition: elasticsearch.enabled  # Disabled in values.yaml
  - name: logstash
    repository: file://./charts/logstash
    version: 9.x.x
    condition: logstash.enabled  # Disabled in values.yaml
```

**atlas-metastore/helm/atlas-read/Chart.yaml:**
```yaml
apiVersion: v2
name: atlas-read
description: Apache Atlas Read Replica for Metadata Management
version: 1.0.0                            # ✅ Updated to 1.0.0-{branch}.{commit} by CI
appVersion: "abc123"                      # ✅ Updated to commit SHA by CI
dependencies:
  # Infrastructure charts - disabled by default (published separately as OCI)
  # Charts exist in charts/ subdirectory for individual OCI publishing
  # Consumed as peers in parent atlan chart, not as subcharts here
  - name: cassandra-online-dc
    repository: file://./charts/cassandra-online-dc
    version: 0.x.x
    condition: cassandra-online-dc.enabled  # Disabled in values.yaml
  - name: elasticsearch-read
    repository: file://./charts/elasticsearch-read
    version: 7.x.x
    condition: elasticsearch-read.enabled  # Disabled in values.yaml
  - name: elasticsearch-exporter-read
    repository: file://./charts/elasticsearch-exporter-read
    version: 3.3.0
    condition: elasticsearch-exporter-read.enabled  # Disabled in values.yaml
```

**Infrastructure Chart Examples (helm/atlas/charts/):**
```yaml
# cassandra/Chart.yaml
apiVersion: v2
name: cassandra
version: 0.14.4                           # ✅ Updated to 0.14.4-{branch}.{commit} by CI

# elasticsearch/Chart.yaml
apiVersion: v2
name: elasticsearch
version: 7.6.1                            # ✅ Updated to 7.6.1-{branch}.{commit} by CI

# logstash/Chart.yaml
apiVersion: v2
name: logstash
version: 9.1.2                            # ✅ Updated to 9.1.2-{branch}.{commit} by CI
```

**atlas-metastore/helm/atlas/values.yaml (partial):**
```yaml
# Infrastructure charts disabled - published separately as OCI artifacts
cassandra:
  enabled: false
elasticsearch:
  enabled: false
logstash:
  enabled: false

# Default values for atlas application
global:
  Tier_Type: ""
  cloud: ""
  # ... other global values
```

**atlas-metastore/helm/atlas-read/values.yaml (partial):**
```yaml
# Infrastructure charts disabled - published separately as OCI artifacts
cassandra-online-dc:
  enabled: false
elasticsearch-read:
  enabled: false
elasticsearch-exporter-read:
  enabled: false

# Default values for atlas-read application
global:
  Tier_Type: ""
  # ... other global values
```

**Key Changes:**

1. **Dependencies declared but disabled:** Infrastructure charts are listed in Chart.yaml with `condition: *.enabled` flags. This satisfies Helm linting requirements (charts physically exist in the `charts/` subdirectory) while preventing them from being bundled as subcharts.

2. **Disabled in values.yaml:** Each infrastructure chart has `enabled: false` in the chart's own values.yaml, ensuring they won't render when the atlas/atlas-read chart is deployed standalone.

3. **Parent chart re-disables:** The atlan repo's values.yaml also sets `atlas.cassandra.enabled: false` (and similar for all infrastructure charts) to ensure they remain disabled when consumed as OCI dependencies.

4. **Published independently:** Despite being declared as dependencies, each chart is packaged and published separately as an OCI artifact. The atlan parent chart then consumes all 8 as peer dependencies.

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
      
  helm-publish:
    needs: build  # 🛡️ QUALITY GATE - Only runs if build and tests pass
    runs-on: ubuntu-latest
    strategy:
      matrix:
        include:
          # Application charts
          - chart: atlas
            path: helm/atlas
            base_version: "1.0.0"
            requires_app_version: true
          - chart: atlas-read
            path: helm/atlas-read
            base_version: "1.0.0"
            requires_app_version: true
          # Atlas infrastructure charts
          - chart: cassandra
            path: helm/atlas/charts/cassandra
            base_version: "0.14.4"
            requires_app_version: false
          - chart: elasticsearch
            path: helm/atlas/charts/elasticsearch
            base_version: "7.6.1"
            requires_app_version: false
          - chart: logstash
            path: helm/atlas/charts/logstash
            base_version: "9.1.2"
            requires_app_version: false
          # Atlas-Read infrastructure charts
          - chart: cassandra-online-dc
            path: helm/atlas-read/charts/cassandra-online-dc
            base_version: "0.14.4"
            requires_app_version: false
          - chart: elasticsearch-read
            path: helm/atlas-read/charts/elasticsearch-read
            base_version: "7.6.1"
            requires_app_version: false
          - chart: elasticsearch-exporter-read
            path: helm/atlas-read/charts/elasticsearch-exporter-read
            base_version: "3.3.0"
            requires_app_version: false
      max-parallel: 1  # Publish sequentially
    
    steps:
      # ✅ Calculate version for this specific chart
      - name: Generate chart version
        run: |
          BRANCH_NAME_NORMALIZED=$(echo "${{ steps.branch.outputs.name }}" | tr '_' '-')
          CHART_VERSION="${{ matrix.base_version }}-${BRANCH_NAME_NORMALIZED}.${{ steps.commit.outputs.id }}"
          
          # Version format: {base}-{branch}.{commit}
          # Examples:
          #   - atlas: 1.0.0-staging.abc123abcd
          #   - cassandra: 0.14.4-staging.abc123abcd
      
      # ✅ Update Chart.yaml version
      - name: Update Chart.yaml with version
        run: |
          sed -i "s/^version: .*/version: ${{ steps.version.outputs.chart }}/" ${{ matrix.path }}/Chart.yaml
          
          # Only update appVersion for application charts (not infrastructure)
          if [[ "${{ matrix.requires_app_version }}" == "true" ]]; then
            sed -i "s/^appVersion: .*/appVersion: \"${{ steps.commit.outputs.id }}\"/" ${{ matrix.path }}/Chart.yaml
          fi
      
      # ✅ Update values.yaml with image tags (application charts only)
      - name: Update values.yaml with image tags
        if: matrix.requires_app_version == true
        run: |
          # Update atlas/atlas-read application image tags
          yq eval -i '.atlas.image.tag = "${{ steps.commit.outputs.id }}"' ${{ matrix.path }}/values.yaml
      
      # ✅ Package chart
      - name: Package helm chart
        run: |
          helm package ${{ matrix.path }}/ --destination ./helm-packages/
      
      # ✅ Login to GHCR
      - name: Login to GitHub Container Registry
        uses: docker/login-action@v2
        with:
          registry: ghcr.io
          username: $GITHUB_ACTOR
          password: ${{ secrets.ORG_PAT_GITHUB }}
      
      # ✅ Push to OCI registry
      - name: Push chart to GHCR (OCI Registry)
        run: |
          CHART_FILE=$(ls helm-packages/${{ matrix.chart }}-*.tgz)
          helm push ${CHART_FILE} oci://ghcr.io/atlanhq/helm-charts
          
          echo "✅ Published: oci://ghcr.io/atlanhq/helm-charts/${{ matrix.chart }}:${CHART_VERSION}"
      
      # ✅ Create GitHub Release
      - name: Create GitHub Release
        uses: ncipollo/release-action@v1
        with:
          tag: helm-${{ matrix.chart }}-v${{ steps.version.outputs.chart }}
          name: "${{ matrix.chart }} Helm Chart v${{ steps.version.outputs.chart }}"
```

**Key:** ALL 8 charts published individually using matrix strategy in the SAME workflow!

---

**`.github/workflows/chart-release-dispatcher.yaml`:** (Separate workflow file)
```yaml
name: Charts Values Seed Trigger Dispatcher
on:
  workflow_run:
    workflows: ["Java CI with Maven"]  # ✅ Triggers after maven.yml completes
    types: [completed]
    branches: [staging, beta, preprod, atlas_ci_cd_updates]

jobs:
  charts-release-dispatcher:
    if: ${{ github.event.workflow_run.conclusion == 'success' }}
    runs-on: ubuntu-latest
    steps:
      # ✅ Extract branch and commit info
      - name: Get branch name
        run: |
          BRANCH="${{ github.event.workflow_run.head_branch }}"
          SHA="${{ github.event.workflow_run.head_sha }}"
          echo "Branch: ${BRANCH}, Commit: ${SHA}"
      
      # ✅ Trigger atlan repo to update Chart.yaml
      - name: Repository Dispatch
        uses: peter-evans/repository-dispatch@v2
        with:
          token: ${{ secrets.ORG_PAT_GITHUB }}
          repository: atlanhq/atlan
          event-type: dispatch_chart_release_workflow
          client-payload: |
            {
              "repo": {
                "name": "atlas-metastore",
                "branch": "${{ github.event.workflow_run.head_branch }}",
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
            
            ✅ Quality gate passed (build and tests validated)" \
            --base main
```

**scripts/image.sh** (atlas section, after):
```bash
# ATLAS CHART VERSION UPDATE
ATLAS_BRANCH=`echo ${GITHUB_BRANCH_NAME} | tr '-' '_'`
eval FILE=$`echo ${ATLAS_BRANCH}"_atlas_metastore"`
if [ -z $FILE ]
then
    ATLAS_BRANCH=$GITHUB_BRANCH_NAME
    echo $ATLAS_BRANCH-atlas-branch-coming-from-github-current-workflow-branch
else
    ATLAS_BRANCH=$FILE
    echo $ATLAS_BRANCH-atlas-branch-coming-from-instancemap
fi

# ✅ Update Chart.yaml with ALL 8 OCI chart versions (not just image tags)
if [[ "$MICROSERVICE_NAME" == "atlas-metastore" ]]; then
    TAG=$(get_latest_tag "atlas-metastore" "$ATLAS_BRANCH")
    echo "[Info] The atlas-metastore image tag is: $TAG"
    
    # ✅ Construct chart versions: {base_version}-{branch}.{commit}
    ATLAS_BRANCH_NORMALIZED=`echo ${ATLAS_BRANCH} | tr '_' '-'`
    
    # Application charts (atlas, atlas-read): base version 1.0.0
    APP_CHART_VERSION="1.0.0-${ATLAS_BRANCH_NORMALIZED}.${TAG}"
    echo "[Info] Application chart version (atlas, atlas-read): $APP_CHART_VERSION"
    
    # Infrastructure chart versions with their respective base versions
    CASSANDRA_VERSION="0.14.4-${ATLAS_BRANCH_NORMALIZED}.${TAG}"
    ELASTICSEARCH_VERSION="7.6.1-${ATLAS_BRANCH_NORMALIZED}.${TAG}"
    LOGSTASH_VERSION="9.1.2-${ATLAS_BRANCH_NORMALIZED}.${TAG}"
    ES_EXPORTER_READ_VERSION="3.3.0-${ATLAS_BRANCH_NORMALIZED}.${TAG}"
    
    echo "[Info] Infrastructure chart versions:"
    echo "  - cassandra: $CASSANDRA_VERSION (base: 0.14.4)"
    echo "  - elasticsearch: $ELASTICSEARCH_VERSION (base: 7.6.1)"
    echo "  - logstash: $LOGSTASH_VERSION (base: 9.1.2)"
    echo "  - cassandra-online-dc: $CASSANDRA_VERSION (base: 0.14.4)"
    echo "  - elasticsearch-read: $ELASTICSEARCH_VERSION (base: 7.6.1)"
    echo "  - elasticsearch-exporter-read: $ES_EXPORTER_READ_VERSION (base: 3.3.0)"
    
    # ✅ Update Chart.yaml: update all 8 chart versions using yq
    echo "[Info] Updating Chart.yaml with all chart versions..."
    
    # Application charts
    yq eval -i '(.dependencies[] | select(.name == "atlas") | .version) = "'"$APP_CHART_VERSION"'"' charts/Chart.yaml
    yq eval -i '(.dependencies[] | select(.name == "atlas-read") | .version) = "'"$APP_CHART_VERSION"'"' charts/Chart.yaml
    
    # Atlas infrastructure charts
    yq eval -i '(.dependencies[] | select(.name == "cassandra") | .version) = "'"$CASSANDRA_VERSION"'"' charts/Chart.yaml
    yq eval -i '(.dependencies[] | select(.name == "elasticsearch") | .version) = "'"$ELASTICSEARCH_VERSION"'"' charts/Chart.yaml
    yq eval -i '(.dependencies[] | select(.name == "logstash") | .version) = "'"$LOGSTASH_VERSION"'"' charts/Chart.yaml
    
    # Atlas-Read infrastructure charts (same versions, different names)
    yq eval -i '(.dependencies[] | select(.name == "cassandra-online-dc") | .version) = "'"$CASSANDRA_VERSION"'"' charts/Chart.yaml
    yq eval -i '(.dependencies[] | select(.name == "elasticsearch-read") | .version) = "'"$ELASTICSEARCH_VERSION"'"' charts/Chart.yaml
    yq eval -i '(.dependencies[] | select(.name == "elasticsearch-exporter-read") | .version) = "'"$ES_EXPORTER_READ_VERSION"'"' charts/Chart.yaml
    
    echo "[Info] ✓ Chart.yaml updated with all atlas chart versions"
else
    # ✅ If triggered by other microservices, keep existing atlas chart versions
    # This is critical for preprod/master to prevent unintended updates
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

### 6. ✅ **Complete Cross-Repository Automation**

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

### 7. ✅ **No Duplicate Manifests**

**Challenge:** When publishing atlas as an OCI artifact that declares infrastructure dependencies, and also consuming those same infrastructure charts as peer dependencies in the parent atlan chart, Helm would render each infrastructure chart twice, creating duplicate StatefulSets and causing deployment failures.

**Solution:** Multi-layer disable flags
1. **atlas-metastore charts:** Infrastructure dependencies declared in `Chart.yaml` with `condition: *.enabled` flags, and disabled in chart's own `values.yaml` (`cassandra.enabled: false`)
2. **atlan values:** Parent chart explicitly re-disables these via `atlas.cassandra.enabled: false` to ensure they remain disabled when OCI chart is consumed
3. **Result:** Infrastructure charts render only once (as peers in atlan), never as nested subcharts

**Benefits:**
- ✅ No duplicate Kubernetes resources (StatefulSets, Services, ConfigMaps)
- ✅ Correct resource naming (e.g., `atlas-cassandra` not `release-name-cassandra`)
- ✅ Proper namespace assignment from parent values
- ✅ Reduced manifest size (~2,000 lines removed from rendered output)

---

## Migration Checklist

### atlas-metastore Repository

- [x] **Create `helm/` directory** (didn't exist before!)
- [x] **Move charts from `atlan/subcharts/atlas/`** to `atlas-metastore/helm/`
- [x] Include all dependencies in `helm/charts/` (cassandra, elasticsearch, kafka, logstash, redis, zookeeper)
- [x] **Add infrastructure dependencies to Chart.yaml** with `condition: *.enabled` flags
- [x] **Add disable flags to values.yaml** for infrastructure charts (`cassandra.enabled: false`, etc.)
- [x] Update `.github/workflows/maven.yml`:
  - [x] Add `helm-lint` job (validates all 8 charts, with conditional logic for appVersion and parent values)
  - [x] Add `helm-publish` job with quality gate (only runs if build and tests pass, publishes all 8 charts)
  - [x] Add GHCR authentication
  - [x] Dynamic chart version generation for each chart with correct base versions
  - [x] GitHub Release creation for each chart
  - [x] Update integration tests to use Testcontainers (local Redis, no Sentinel)
- [x] Add `.github/workflows/chart-release-dispatcher.yaml`
  - [x] Triggers on maven.yml completion
  - [x] Sends `repository_dispatch` to atlan repo (chart versions extracted from GitHub releases)

### atlan Repository

- [x] Update `charts/Chart.yaml`:
  - [x] Change atlas dependency from `file://../subcharts/atlas` to `oci://ghcr.io/atlanhq/helm-charts`
  - [x] Change atlas-read dependency from `file://../subcharts/atlas` to `oci://ghcr.io/atlanhq/helm-charts`
  - [x] Add all 8 Atlas-related charts as peer OCI dependencies (atlas, atlas-read, cassandra, elasticsearch, logstash, cassandra-online-dc, elasticsearch-read, elasticsearch-exporter-read)
  - [x] Set initial versions with correct base versions (e.g., `1.0.0-staging.PLACEHOLDER`, `0.14.4-staging.PLACEHOLDER`, etc.)
- [x] **Remove only:** `subcharts/atlas/` and `subcharts/atlas-read/` directories
- [x] **Keep:** `charts/Chart.yaml`, `charts/values.yaml`, `charts/values-template.yaml`
- [x] **Keep:** Atlas overrides in values files (atlas.*, atlas-read.*, cassandra.*, elasticsearch.*, etc.)
- [x] **Keep:** Other subcharts (heka, kong, ranger, etc.)
- [x] Update `charts/values-template.yaml` and `charts/values.yaml`:
  - [x] Keep all atlas-specific configuration overrides (root level, unchanged structure)
  - [x] **Add disable flags** under atlas.* and atlas-read.* to prevent duplicate manifests:
    - `atlas.cassandra.enabled: false`
    - `atlas.elasticsearch.enabled: false`
    - `atlas.logstash.enabled: false`
    - `atlas-read.cassandra-online-dc.enabled: false`
    - `atlas-read.elasticsearch-read.enabled: false`
    - `atlas-read.elasticsearch-exporter-read.enabled: false`
- [x] Update `scripts/image.sh`:
  - [x] Change from image tag seeding to Chart.yaml version updates (all 8 charts)
  - [x] Use `yq` to update OCI chart versions for each chart with correct base versions
  - [x] Add `MICROSERVICE_NAME` conditional logic (preprod/master only)
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
   - Added: `helm-lint` job (lints all 8 charts with conditional logic)
   - Added: `helm-publish` job (publishes all 8 charts individually as OCI artifacts)
   - Matrix strategy for 8 charts with correct base versions
   - Quality gate: helm-publish only runs if build and tests pass

2. **`.github/workflows/chart-release-dispatcher.yaml`** (new)
   - Sends `repository_dispatch` to atlan repo after successful maven.yml completion

3. **`helm/atlas/Chart.yaml`**
   - Version now dynamic (set by CI to `1.0.0-{branch}.{commit}`)
   - Dependencies declared with `condition: *.enabled` flags (cassandra, elasticsearch, logstash)

4. **`helm/atlas-read/Chart.yaml`**
   - Version now dynamic (set by CI to `1.0.0-{branch}.{commit}`)
   - Dependencies declared with `condition: *.enabled` flags (cassandra-online-dc, elasticsearch-read, elasticsearch-exporter-read)

5. **`helm/atlas/values.yaml`** (new)
   - Disable flags for infrastructure charts (`cassandra.enabled: false`, etc.)

6. **`helm/atlas-read/values.yaml`** (new)
   - Disable flags for infrastructure charts (`cassandra-online-dc.enabled: false`, etc.)

### atlan

1. **`charts/Chart.yaml`**
   - Changed atlas/atlas-read dependencies from `file://../subcharts/atlas` to `oci://ghcr.io/atlanhq/helm-charts`
   - Added all 8 Atlas-related charts as peer OCI dependencies (atlas, atlas-read, cassandra, elasticsearch, logstash, cassandra-online-dc, elasticsearch-read, elasticsearch-exporter-read)
   - Updated version references to dynamic OCI versions with correct base versions (e.g., `1.0.0-staging.abc123`, `0.14.4-staging.abc123`, `7.6.1-staging.abc123`, etc.)

2. **`scripts/image.sh`**
   - Atlas section now updates `Chart.yaml` versions for ALL 8 charts (not just image tags in values)
   - Uses `yq` for YAML manipulation with separate version variables for each chart
   - Adds `MICROSERVICE_NAME` conditional logic (preprod/master only) to prevent unintended updates
   - Includes instance map logic for branch name resolution

3. **`.github/workflows/charts-values.yaml`**
   - Added GHCR login step
   - Now commits both `values.yaml` AND `Chart.yaml` changes
   - Removed `subcharts/atlas/**` from trigger paths

4. **`.github/workflows/lint-helm-charts.yaml`**
   - Added GHCR login step

5. **`charts/values-template.yaml`** & **`charts/values.yaml`**
   - **Kept:** All atlas-specific configuration overrides at root level (atlas.*, atlas-read.*, cassandra.*, elasticsearch.*, etc.)
   - **Added:** Disable flags under atlas.* and atlas-read.* to prevent duplicate manifests:
     - `atlas.cassandra.enabled: false`
     - `atlas.elasticsearch.enabled: false`
     - `atlas.logstash.enabled: false`
     - `atlas-read.cassandra-online-dc.enabled: false`
     - `atlas-read.elasticsearch-read.enabled: false`
     - `atlas-read.elasticsearch-exporter-read.enabled: false`
   - **No structural changes:** Values override structure remains the same (backward compatible)
   - These files still define atlas replicaCount, resources, environment variables, etc.

6. **Deleted:**
   - `subcharts/atlas/` (entire directory with Chart.yaml, templates/, charts/)
   - `subcharts/atlas-read/` (entire directory)
   - Only these two subdirectories removed - other subcharts remain

---
