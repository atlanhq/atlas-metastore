---
name: atlas-context
description: >
  Atlas Metastore codebase awareness, architectural constraints, and accumulated lessons
  from past code reviews, incidents, and performance investigations.
  Use when starting any new feature, reviewing a PR, estimating complexity, writing a
  feature spec, or debugging a performance regression.
  Triggers on: "how does X work", "estimate complexity", "what are the risks",
  "write a feature request", "what should I watch out for", "context on the codebase".
version: 2.0.0
---

# Atlas Metastore — Codebase Context, Constraints & Learned Intelligence

This skill is a **living document**. Every code review, incident, and performance
investigation produces lessons that get appended to the relevant section below.
When you finish a task that teaches something new, update this file.

---

## How to Use This Skill

**On a new feature task:**
Read sections: Architecture → Key Classes → Constraints → Feature Evaluation Framework

**On a code review:**
Read sections: Review Checklist → Lessons Learned → Anti-Patterns

**On a performance investigation:**
Read sections: Execution Paths → I/O Cost Model → Lessons Learned → Performance

**On a feature request / spec write-up:**
Read sections: Feature Evaluation Framework → Complexity Model → Past Patterns

**Self-update:** When you learn something new that isn't here, add it.
Run `/atlas-context update` and describe what to add — Claude will locate the right
section and append the new lesson in the standard format.

---

## Architecture Overview

```
HTTP Request
  └── webapp/REST Layer (AtlasEntityREST, SearchREST, …)
        └── AtlasEntityStoreV2.createOrUpdate()        ← main write path
              ├── EntityDiscoveryService               ← resolve existing entities
              ├── executePreProcessor(context)         ← transform before persist
              │     └── PreProcessor per type:
              │           AuthPolicyPreProcessor
              │           PersonaPreProcessor / PurposePreProcessor
              │           GlossaryPreProcessor / TermPreProcessor / CategoryPreProcessor
              │           DataDomainPreProcessor / DataProductPreProcessor
              │           QueryPreProcessor / …
              ├── EntityGraphMapper.mapAttributes()    ← write to JanusGraph
              ├── EntityGraphMapper.mapRelationships() ← write edges
              └── PostProcessor / ESAliasStore         ← side-effects (ES alias, tasks)
```

**Storage layer:**
- **JanusGraph** (vertex/edge store) backed by Cassandra + Elasticsearch index
- **Elasticsearch** for full-text + filtered search (`janusgraph_vertex_index`)
- **ES aliases** per Persona/Purpose — each alias contains a filter DSL that scopes
  which assets are visible to that access-control entity

**Async:**
- Background tasks via `AtlasTaskService` (stored in Cassandra, executed by task runners)
- Kafka notifications for entity mutations

---

## Key Classes & High-Impact Files

| File | Why it matters | Risk level |
|------|---------------|-----------|
| `AtlasEntityStoreV2.java` | All entity CRUD flows through here. `executePreProcessor` iterates N entities and calls `getPreProcessor()` — currently creates a new preprocessor instance per entity type per call | HIGH |
| `EntityGraphRetriever.java` | Converts JanusGraph vertices → `AtlasEntity`. `toAtlasEntity()` with `ignoreRelationshipAttr=false` triggers O(K×attrs) graph reads | HIGH |
| `EntityGraphMapper.java` | Writes vertices/edges to JanusGraph. `createVertex()` at line 240 assigns GUIDs. Changes here affect all entity types | HIGH |
| `AuthPolicyPreProcessor.java` | Creates/updates AuthPolicy entities. `getAccessControlEntity()` is the O(N×K) hotspot. `validateConnectionAdmin()` reads Connection from graph with no cache | MEDIUM |
| `ESAliasStore.java` | Rebuilds the ES filter alias for a Persona/Purpose after every policy change. `getIndexNameFromAliasIfExists()` makes a live ES HTTP GET every call to resolve a static index name | MEDIUM |
| `AccessControlUtils.java` | Shared helpers for Persona/Purpose/Policy. `objectToEntityList()` can NPE when `getReferredEntities()` returns null | MEDIUM |
| `PersonaPreProcessor.java` | Creates Personas. QN pattern: `{tenantId}/{nanoId}` | LOW |
| `GraphHelper.java` | Low-level graph traversal. `getActiveCollectionElementsUsingRelationship()` is the correct way to traverse a relationship edge collection without triggering full attribute mapping | LOW |
| `AtlasTypeRegistry.java` | In-memory type registry. `getEntityTypeByName()` returns null for unknown types — always null-check | LOW |

---

## Execution Paths with I/O Cost Model

### Bulk entity create: `POST /api/meta/entity/bulk` with N entities

```
For each of N entities in the batch:
  getPreProcessor(typeName)           → currently creates NEW instance (no sharing)
  preprocessor.processAttributes()   → varies by type (see per-type table below)
  EntityGraphMapper.mapAttributes()  → 1 vertex write per entity
  EntityGraphMapper.mapRelationships()→ 1 edge write per relationship
```

### AuthPolicy create cost (per policy, K = existing policies on parent Persona)

| Step | Operation | Cost (before fixes) | Cost (after Fix B) |
|------|-----------|--------------------|--------------------|
| getAccessControlEntity | Load Persona with all K policies | O(K × attrs) graph reads | O(1) scalar + O(K) edge reads |
| validateConnectionAdmin | Load Connection by QN | 1 graph read | 1 graph read (Fix C: → 0 after 1st) |
| aliasStore.updateAlias | Resolve ES index name | 1 ES HTTP GET | 1 ES GET (Fix A: → 0 after 1st) |
| aliasStore.updateAlias | Write ES alias filter | 1 ES HTTP PUT | 1 ES PUT (Fix E: → 1 total deferred) |

**N=1, K=500, no fixes:** ~25s
**N=1, K=500, Fix B only:** ~11s
**N=1, K=500, all fixes:** target <3s

### ES alias filter rebuild cost

`ESAliasStore.getFilterForPersona()` iterates all K policy entities to build a DSL.
Each call to `updateAlias()` does a full rebuild from scratch — no incremental update.
Calling it N times for N new policies in one request rebuilds the alias N times;
only the last write survives. **Fix E** (deferred post-batch alias update) eliminates this.

---

## Constraints & Non-Negotiables

### API compatibility
- Entity CRUD on `itau-scale.atlan.com` (and all Atlan tenants) uses `/api/meta/entity/bulk`
  NOT `/api/atlas/v2/entity/bulk`. The latter serves the frontend HTML on the main domain.
- `qualifiedName` is always required on create. Missing it causes silent failures downstream.
- `policyResourceCategory: "CUSTOM"` is required for `metadata` and `data` sub-category policies.
- `connectionQualifiedName` is required for metadata policies (validated in `validateConnectionAdmin`).
- Update requests must carry all existing attributes AND the `accessControl` relationship attribute,
  or `processUpdatePolicy()` will NPE when calling `getAccessControlEntity()`.

### JanusGraph / graph traversal
- **Never** use `entityRetriever.toAtlasEntity(vertex)` with `ignoreRelationshipAttr=false`
  inside a loop over K entities — this triggers O(K × attrs) graph reads.
- Use `noRelAttrRetriever` (constructed with `new EntityGraphRetriever(base, true)`) for
  scalar-only reads inside hot paths.
- Use `GraphHelper.getActiveCollectionElementsUsingRelationship()` to traverse a relationship
  edge collection without triggering full attribute mapping.
- **Edge direction:** use `AtlasAttribute.getRelationshipEdgeDirection()` to determine which
  vertex of an edge is the "referenced" entity. Never use `getIdForDisplay()` string comparison
  (fragile — vertex IDs differ between JanusGraph numeric longs and Cassandra UUID strings).
- **Relationship attribute map:** `AtlasEntityType.getRelationshipAttributes().get("policies")`
  returns `Map<String, AtlasAttribute>` keyed by **relationship type name**, not attribute name.
  Always look up by the specific relationship type key (e.g., `"access_control_policies"`),
  never use `iterator().next()` which picks an arbitrary entry.

### Type system
- `AtlasTypeRegistry.getEntityTypeByName()` returns null for unrecognised type names.
  Always null-check before accessing the result.
- The GitHub repo has minimal typedefs (base model only). Production typedefs include
  `Table`, `Column`, `Connection`, etc. — not available without `minimal.json` or a typedef export.
- `policySubCategory` controls which validation path runs: `domain` skips `validateConnectionAdmin()`,
  `metadata` and `data` require a valid Connection entity.

### ES alias
- ES alias names are the Persona's nanoId (the last path segment of its `qualifiedName`).
- Alias is always on `janusgraph_vertex_index` — this is a static name, not looked up at runtime
  (though current code does a live ES GET to verify it — see Fix A).
- Each policy maps to one filter clause in the alias DSL. A Persona with 500 policies has a
  500-clause filter. The alias is rebuilt from scratch on every `updateAlias()` call.

### Git / repo
- This repo (`atlanhq/atlas-metastore`) is a fork of `apache/atlas`.
  `gh` CLI defaults to the upstream `apache/atlas` parent.
  **Always** pass `--repo atlanhq/atlas-metastore` on every `gh` command.
- `.pre-commit-config.yaml` runs gitleaks, merge-conflict check, BOM fix, large-file check,
  case-conflict check. All hooks pass normally. Do NOT use `--no-verify`.
- Build with Java 17 (JAVA_HOME=/Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home).
  The `zulu-17.jdk` path referenced in CLAUDE.md may not exist; fall back to this path.

### Ring release
- Custom cohort JSON lives in `atlanhq/atlan-releases/cohorts/`.
- Cohort label format: `cohort:github:path:<filename-without-.json>` added to the PR.
- Temporal `ServiceReleaseWorkflow` is triggered by the label on the ring PR.
- Updating the cohort JSON (adding tenants) after the label is applied takes effect
  on the next Temporal trigger — no need to re-add the label.

---

## Code Review Checklist (Atlas-specific)

Run these checks on every PR before requesting review:

### Null safety
- [ ] Every `entity.getRelationshipAttribute()` return value is null-checked before use
- [ ] Every `context.getVertex(guid)` result is null-checked
- [ ] Every `typeRegistry.getEntityTypeByName()` result is null-checked
- [ ] `getReferredEntities()` on an `AtlasEntityWithExtInfo` can be null — always guard
- [ ] `objectToEntityList()` in `AccessControlUtils` handles null `referredEntities`

### Graph traversal
- [ ] No `toAtlasEntity()` with `ignoreRelationshipAttr=false` inside a K-entity loop
- [ ] Edge direction uses `getRelationshipEdgeDirection()`, not `getIdForDisplay()` comparison
- [ ] Relationship attribute map lookups use the specific relationship type name as key,
      not `iterator().next()`

### Performance instrumentation
- [ ] New preprocessor methods wrap with `AtlasPerfMetrics.MetricRecorder`
- [ ] REST endpoints have `@Timed` annotation
- [ ] Bulk paths avoid per-entity I/O that could be batched or cached

### Error handling
- [ ] `AtlasBaseException` with a specific `AtlasErrorCode`, not generic Exception
- [ ] No empty catch blocks
- [ ] Preprocessors propagate exceptions — do not suppress

### Test coverage
- [ ] New code paths are exercised by tests, not mocked away
  (e.g., if `typeRegistry.getEntityTypeByName()` is mocked to return null,
  the new traversal logic is never tested)

---

## Lessons Learned

Each entry: **date · source · lesson**

### Performance

**2026-04 · MS-752 · O(N×K) graph read anti-pattern**
When a preprocessor loads a parent entity via `entityRetriever.toAtlasEntity()` with
`ignoreRelationshipAttr=false`, it triggers `mapRelationshipAttributes()` which reads ALL
related entities. In a bulk create of N entities where the parent has K existing children,
this produces N×K graph reads. The fix: load parent scalar-only via `noRelAttrRetriever`,
then traverse edges directly via `GraphHelper.getActiveCollectionElementsUsingRelationship()`.
*See: `AuthPolicyPreProcessor.getAccessControlEntity()`, Fix B.*

**2026-04 · MS-752 · Preprocessor instance not reused across bulk batch**
`AtlasEntityStoreV2.executePreProcessor()` creates a new preprocessor instance per entity
via `getPreProcessor()`. Any per-instance cache (connection cache, ES index name cache)
is thrown away between entities in the same batch. Fix: build a `Map<String, List<PreProcessor>>`
once before the loop using `computeIfAbsent`. *See: Fix D (not yet implemented).*

**2026-04 · MS-752 · ES alias rebuilt N times for N new policies**
`updateAlias()` does a full alias rebuild (read all K policies, compute DSL, write to ES)
for each of the N new policies in a batch. Only the last write survives. Fix: defer alias
update to post-batch, once per Persona. *See: Fix E (not yet implemented).*

**2026-04 · MS-752 · ES index name resolved via live HTTP GET every call**
`ESAliasStore.getIndexNameFromAliasIfExists()` makes an ES HTTP GET to resolve a static
alias → index mapping on every call. The index name never changes at runtime. Fix: cache
it in a `volatile` field, resolve once. *See: Fix A (not yet implemented).*

### Correctness / Bugs

**2026-04 · MS-752 review · Edge direction: getIdForDisplay() is fragile**
Using `edge.getOutVertex().getIdForDisplay().equals(parentVertex.getIdForDisplay())` to
determine which end of an edge is the "referenced" vertex is fragile — vertex IDs differ
between JanusGraph numeric longs and Cassandra UUID strings. Always use
`AtlasAttribute.getRelationshipEdgeDirection()` (IN/OUT/BOTH) and match the pattern
in `EntityGraphRetriever.getReferencedEntityVertex()`. *Fixed in PR #6485.*

**2026-04 · MS-752 review · iterator().next() on relationship type map is brittle**
`AtlasEntityType.getRelationshipAttributes().get("policies")` returns
`Map<String, AtlasAttribute>` keyed by **relationship type name** (e.g., `"access_control_policies"`).
Taking `values().iterator().next()` picks an arbitrary entry if multiple relationship types
define the same attribute. Always use the specific type name as the key. *Fixed in PR #6485.*

**2026-04 · MS-752 · Update policy NPE when accessControl not in request**
`processUpdatePolicy()` calls `getAccessControlEntity(policy)` which returns null when the
`accessControl` relationship attribute is absent from the update request. Callers must
fetch the existing entity first, carry forward all attributes including `accessControl`, and
include it in the update payload. This is not enforced by the API — a missing attribute
causes a silent NPE.

**2026-04 · MS-752 · Metadata policy requires policyResourceCategory: CUSTOM**
Persona metadata policies require `policyResourceCategory: "CUSTOM"` and
`connectionQualifiedName` in attributes. Without these the API returns 400. Domain policies
skip `validateConnectionAdmin()` entirely — they do not need a Connection reference.

**2026-04 · MS-752 · objectToEntityList NPE on zero-policy Persona**
`AccessControlUtils.objectToEntityList()` called `extInfo.getReferredEntities().entrySet()`
without a null check. `getReferredEntities()` returns null when a Persona has no existing
policies. Added null-guard in PR #6450.

### Testing

**2026-04 · MS-752 · Mocking typeRegistry to null skips the new traversal path**
Unit tests that mock `typeRegistry.getEntityTypeByName()` to return null cause `policiesAttr`
to be null, which skips the `GraphHelper.getActiveCollectionElementsUsingRelationship()` loop
entirely. The new traversal logic (Steps 2-3 of Fix B) is never exercised. Acceptable for a
ring deployment (smoke tests on itau-scale cover it) but must be addressed before master merge
by providing a real `AtlasEntityType` mock that returns a valid `policiesAttrMap`.

### API / Tenant

**2026-04 · MS-752 · Entity CRUD endpoint differs from docs on Atlan tenants**
Production Atlan tenants expose entity CRUD at `/api/meta/entity/bulk`, not
`/api/atlas/v2/entity/bulk`. The `/api/atlas/v2/` prefix returns the frontend HTML
because the main domain serves the React SPA on that path. Always use `/api/meta/…` when
scripting against a live tenant.

**2026-04 · MS-752 · Keycloak client_credentials tokens expire in ~300s**
Load test scripts that run 20-30 requests at 10-15s each will hit token expiry mid-run.
Refresh the token every ~18 batches (or add an explicit expiry check + refresh in the script).

---

## Feature Request Evaluation Framework

Use this when writing or estimating a new feature for atlas-metastore.

### Step 1: Map to execution path
Which API endpoint does this feature touch? Trace the call from REST → Service → Store →
Preprocessor → Graph. Note every layer that needs to change.

### Step 2: Estimate I/O cost
| Entity count | Graph reads per entity | ES writes per request | Expected latency |
|---|---|---|---|
| N new, K existing | See cost model above | 1 alias PUT (or N if not deferred) | Scale with K |

Ask: *Does this feature introduce a new O(N×K) loop?*
If yes, design the cache or deferred-write strategy upfront, not as a follow-up.

### Step 3: Check constraint compatibility
- Does it require a new typedef? → Need `minimal.json` or typedef migration
- Does it add a new relationship? → Need a relationship type name constant for map lookups
- Does it change the ES alias DSL? → `ESAliasStore.getFilterForPersona()` needs updating
- Does it affect Persona/Purpose with K>100 existing policies? → Measure at itau-scale

### Step 4: Ring release plan
Who gets it first? List cohorts in order:
1. Dev/sandbox (no cohort file needed — deploy directly)
2. Single large tenant (custom cohort, e.g., `itau-scale.atlan.com`)
3. Ring 0–1 (small tenants, low blast radius)
4. Ring 2–5 (progressive rollout)

### Step 5: Validation checklist
- [ ] Smoke test covering create / read / update / delete lifecycle
- [ ] Bulk create test with N≥5 entities in one request
- [ ] Test against a Persona with K≥100 existing policies (verify no latency regression)
- [ ] ES alias validation: clause count before/after, scoped doc count correct
- [ ] 21/21 persona policy smoke test passes on target tenant

---

## Known Complexity Hotspots

These areas have caused bugs or regressions before — approach with extra care:

| Hotspot | Risk | Notes |
|---------|------|-------|
| `getAccessControlEntity()` | HIGH | O(N×K) before Fix B; edge direction & map key bugs in initial fix |
| `ESAliasStore.updateAlias()` | HIGH | Called N times for N policies; last write wins; rebuild is expensive |
| `executePreProcessor()` in AtlasEntityStoreV2 | MEDIUM | New preprocessor per entity — caches don't survive across entities |
| `EntityGraphRetriever.toAtlasEntity()` with relationships | HIGH | Every relationship attribute triggers additional graph reads |
| `AccessControlUtils.objectToEntityList()` | LOW-MEDIUM | NPE on null referredEntities (patched in #6450); watch for similar callers |
| `EntityGraphMapper.mapAttributes()` | HIGH | All entity types go through here; a bug here is a P0 |
| Keycloak token handling in scripts | LOW | Tokens expire ~300s; long-running scripts need refresh logic |

---

## Self-Update Protocol

When you encounter a new lesson (from a code review comment, incident, test failure, or
performance measurement) that isn't captured here:

1. **Identify the category:** Performance / Correctness / Testing / API / Feature Evaluation
2. **Write the entry:** `**YYYY-MM · source · short title**` followed by 2-5 sentences
   explaining what happened, why it matters, and how to avoid/fix it.
   Include the PR or commit reference if available.
3. **Append to the correct section** under "Lessons Learned" or the relevant reference table.
4. **Update "Known Complexity Hotspots"** if the lesson reveals a new risky area.
5. **Update the Constraints section** if the lesson is a hard rule (not just a learned pattern).

To trigger a self-update, the user can say:
> "Update atlas-context: [describe what was learned]"

Claude will then locate the correct section and append the entry, then commit the file.
