# MS-692: IndexSearch Rewrite — Optimised Rendering Pipeline

**Ticket:** MS-692 | **Date:** 2026-04-08 | **Author:** Pratham More
**Status:** Design | **Branch:** `ms-692-indexsearch-rewrite`
**Related:** [Confluence: CQL Amplification Analysis](https://atlanhq.atlassian.net/wiki/spaces/Metastore/pages/1788837894)

---

## Table of Contents

1. [Why Rewrite](#1-why-rewrite)
2. [Current Code Path — Every CQL Call Site](#2-current-code-path--every-cql-call-site)
3. [What Previous Optimisations Fixed vs What Remains](#3-what-previous-optimisations-fixed-vs-what-remains)
4. [Hotspot Traceability Matrix](#4-hotspot-traceability-matrix)
5. [New Architecture](#5-new-architecture)
6. [Design Principles & Patterns](#6-design-principles--patterns)
7. [Sequence Diagram — New Pipeline](#7-sequence-diagram--new-pipeline)
8. [Pseudo Code — Complete Implementation](#8-pseudo-code--complete-implementation)
9. [Implementation Phases](#9-implementation-phases)
10. [Migration & Rollout](#10-migration--rollout)
11. [Validation Strategy](#11-validation-strategy)
12. [Risk Analysis](#12-risk-analysis)
11. [Risk Analysis](#11-risk-analysis)

---

## 1. Why Rewrite

We attempted incremental optimisations in MS-821 and earlier MS-692 phases. Some helped, some didn't deliver expected gains. The gap between projected and actual improvement comes from:

- **Hotspots interact** — fixing one moves the bottleneck to another, and the improvement compounds less than expected
- **Hidden CQL call sites** — the docs captured the major ones, but code tracing reveals additional undocumented sources (e.g., `mapAssignedTerms()`)
- **Structural coupling** — `EntityGraphRetriever` is a 2,700+ line god class where every IndexSearch change risks breaking non-IndexSearch paths

A clean rewrite lets us:
1. Eliminate ALL CQL hotspots by design — not patching them one at a time
2. Build a pipeline architecture where each stage is independently testable and measurable
3. Keep the existing code untouched for non-IndexSearch paths (zero regression risk)
4. Feature-flag the new path for safe, per-tenant rollout

---

## 2. Current Code Path — Every CQL Call Site

**Traced from actual code on branch `ms-692-indexsearch-rewrite`, not from documentation projections.**

### 2.1 Complete Call Chain with CQL Annotations

```
DiscoveryREST.indexSearch() [DiscoveryREST.java:208]
  │
  ├─ RequestContext setup (isInvokedByIndexSearch=true)
  │
  └─ EntityDiscoveryService.directIndexSearch() [EntityDiscoveryService.java:320]
     │
     ├─ addPreFiltersToSearchQuery() [line 352]
     │  └─ ABAC auth DSL injection — 0 CQL (in-memory)
     │
     ├─ optimizeQueryIfApplicable() [line 356]
     │  └─ ES DSL optimization — 0 CQL (in-memory)
     │
     ├─ indexQuery.vertices(searchParams) [line 363]
     │  └─ CassandraIndexQuery → HTTP POST /{index}/_search?_source=false
     │  └─ Returns Iterator<Result> with ES doc IDs only
     │  └─ Cost: 1 HTTP call to Elasticsearch
     │
     ├─ prepareSearchResult() [line 368, method at line 558]
     │  │
     │  │  ┌─────────────────────────────────────────────────────────────────┐
     │  │  │  CQL SITE #1: Sequential Vertex Materialisation               │
     │  │  │  Location: EntityDiscoveryService.java:572-579                │
     │  │  │                                                               │
     │  │  │  results.stream().map(result -> {                             │
     │  │  │      AtlasVertex vertex = result.getVertex();  // <-- CQL     │
     │  │  │      return vertex.getId().toString();                        │
     │  │  │  })                                                           │
     │  │  │                                                               │
     │  │  │  CassandraIndexQuery.ResultImplDirect.getVertex() [line 843]: │
     │  │  │    → graph.getVertex(docId) [CassandraGraph.java:133]         │
     │  │  │    → vertexRepository.getVertex(vertexId, graph) [SYNC CQL]   │
     │  │  │    → SELECT * FROM atlas_graph.vertices WHERE vertex_id = ?   │
     │  │  │                                                               │
     │  │  │  Note: On ZeroGraph, ES _id IS the vertex_id. The CQL call   │
     │  │  │  is only to load the full vertex — the ID is already known.   │
     │  │  │  Subsequent getVertex() calls hit vertexCache (ThreadLocal).  │
     │  │  │                                                               │
     │  │  │  Cost: N SYNC SEQUENTIAL CQL reads (first call per vertex)    │
     │  │  │  BUT: vertexCache means B1 gets cache hits for free           │
     │  │  └─────────────────────────────────────────────────────────────────┘
     │  │
     │  ├─ enrichVertexPropertiesByVertexIds() [EntityGraphRetriever.java:1487]
     │  │  │
     │  │  │  ┌─────────────────────────────────────────────────────────────────┐
     │  │  │  │  CQL SITE #2: Bulk Vertex Property Load (Phase B1)             │
     │  │  │  │  Location: EntityGraphRetriever.java:1505                      │
     │  │  │  │  Method: getVertexPropertiesValueMap(vertexIds, batchSize=100)  │
     │  │  │  │  → getVertexPropertiesValueMapViaAtlasApi() [line 1297]        │
     │  │  │  │  → graph.getVertices(batchIds) [CassandraGraph.java:153]       │
     │  │  │  │  → vertexRepository.getVerticesAsync() for uncached IDs        │
     │  │  │  │                                                                │
     │  │  │  │  Cost: ~0 CQL (vertexCache HIT from Site #1)                   │
     │  │  │  │  BUT: iterates ALL property keys per vertex [line 1308]:       │
     │  │  │  │    for (String key : vertex.getPropertyKeys()) {               │
     │  │  │  │        vertex.getPropertyValues(key, Object.class) // in-mem   │
     │  │  │  │    }                                                           │
     │  │  │  │  This is CPU work, not CQL, but expensive for many properties  │
     │  │  │  └─────────────────────────────────────────────────────────────────┘
     │  │  │
     │  │  │  ┌─────────────────────────────────────────────────────────────────┐
     │  │  │  │  IN-MEMORY: Edge Label Resolution (Phase B2)                   │
     │  │  │  │  Location: EntityGraphRetriever.java:1517                      │
     │  │  │  │  Method: collectEdgeLabelsWithDirection() [line 1173]           │
     │  │  │  │                                                                │
     │  │  │  │  Collects unique typeNames from ALL result vertices [line 1180] │
     │  │  │  │  For EACH typeName × EACH requested attribute:                 │
     │  │  │  │    checks if relationship attribute via type registry           │
     │  │  │  │    unions into FLAT Map<edgeLabel, direction>                   │
     │  │  │  │                                                                │
     │  │  │  │  ⚠ PROBLEM: Returns ONE label set applied to ALL vertices.     │
     │  │  │  │  Table has ~8 labels, Column ~3, Connection 0.                 │
     │  │  │  │  But union = ~15-31 labels. Every vertex queried for all.      │
     │  │  │  │                                                                │
     │  │  │  │  Direction IS preserved (fixed in earlier PR).                 │
     │  │  │  │  But per-type scoping is NOT — the remaining big problem.      │
     │  │  │  │                                                                │
     │  │  │  │  Cost: 0 CQL (in-memory type registry lookup)                  │
     │  │  │  └─────────────────────────────────────────────────────────────────┘
     │  │  │
     │  │  │  ┌─────────────────────────────────────────────────────────────────┐
     │  │  │  │  CQL SITE #3: Edge Fetch — THE DOMINANT COST (Phase B3)        │
     │  │  │  │  Location: EntityGraphRetriever.java:1523-1524                 │
     │  │  │  │  Method: getConnectedRelationEdgesVertexBatching()             │
     │  │  │  │  → getEdgeInfoMapsViaAtlasApiBatched() [line 1378]             │
     │  │  │  │  → CassandraGraph.getEdgesForVerticesDirectionAware() [310]    │
     │  │  │  │  → EdgeRepository.getEdgesForVerticesByLabelsDirectionAware()  │
     │  │  │  │    [EdgeRepository.java:660]                                   │
     │  │  │  │                                                                │
     │  │  │  │  For EACH vertex × EACH label:                                 │
     │  │  │  │    if direction=OUT: CQL on edges_out only                     │
     │  │  │  │    if direction=IN:  CQL on edges_in only                      │
     │  │  │  │    if direction=BOTH: CQL on both tables                       │
     │  │  │  │                                                                │
     │  │  │  │  Formula: Σ(vertex × type_union_labels × direction_factor)     │
     │  │  │  │  Direction factor is 1 for most labels (already fixed).        │
     │  │  │  │  But type_union is still flat → every vertex gets union labels │
     │  │  │  │                                                                │
     │  │  │  │  Example (20 Table/View, 31 union labels, ~1.0 dir factor):    │
     │  │  │  │    20 × 31 × ~1.0 = ~620 async CQL                            │
     │  │  │  │    Of which: Connection vertices get 31 queries for 0 edges.   │
     │  │  │  │    Table vertices get 31 queries but only ~8 labels exist.     │
     │  │  │  │    Many empty results remain due to cross-type label union.    │
     │  │  │  │                                                                │
     │  │  │  │  Cost: N × L_union × ~1.0 async CQL queries (dominant cost)   │
     │  │  │  └─────────────────────────────────────────────────────────────────┘
     │  │  │
     │  │  │  ┌─────────────────────────────────────────────────────────────────┐
     │  │  │  │  CPU SITE: O(n²) Edge-to-Vertex Association (Phase B4)         │
     │  │  │  │  Location: EntityGraphRetriever.java:1530-1580                 │
     │  │  │  │                                                                │
     │  │  │  │  for(vertexId : vertexIds) {          // N vertices            │
     │  │  │  │      for(relationEdge : relationEdges) { // ALL E edges        │
     │  │  │  │          // check if edge belongs to vertex                    │
     │  │  │  │      }                                                         │
     │  │  │  │  }                                                             │
     │  │  │  │                                                                │
     │  │  │  │  Cost: O(N × E) CPU iterations. 0 CQL but significant CPU.    │
     │  │  │  │  100 results × 2000 edges = 200,000 iterations.               │
     │  │  │  └─────────────────────────────────────────────────────────────────┘
     │  │  │
     │  │  │  ┌─────────────────────────────────────────────────────────────────┐
     │  │  │  │  CQL SITE #4: Reference Vertex Property Load (Phase B5)        │
     │  │  │  │  Location: EntityGraphRetriever.java:1583                      │
     │  │  │  │  Method: getVertexPropertiesValueMap(vertexIdsToProcess, 1000)  │
     │  │  │  │  → Same path as Site #2 but for other-end vertices             │
     │  │  │  │                                                                │
     │  │  │  │  Note: MS-821 had added Caffeine cross-request cache here,       │
     │  │  │  │  but cross-request caching is dropped in rewrite — stale       │
     │  │  │  │  data is unacceptable. Fresh fetch every request.              │
     │  │  │  │                                                                │
     │  │  │  │  Cost: M async CQL (M = unique referenced vertices, ~30-60)    │
     │  │  │  └─────────────────────────────────────────────────────────────────┘
     │  │
     │  ├─ prefetchClassifications() [line 598]
     │  │  │
     │  │  │  ┌─────────────────────────────────────────────────────────────────┐
     │  │  │  │  CQL SITE #5: Batch Classification Fetch                       │
     │  │  │  │  Location: EntityGraphRetriever.java:2565-2579                 │
     │  │  │  │  → tagDAO.getAllClassificationsForVertices(vertexIds)           │
     │  │  │  │  → TagDAOCassandraImpl: per-vertex CQL to tags.tags_by_id     │
     │  │  │  │                                                                │
     │  │  │  │  Queries ALL N vertices regardless of whether they have tags.  │
     │  │  │  │  Typical: 5 of 20 have classifications → 75% wasted.          │
     │  │  │  │                                                                │
     │  │  │  │  Cost: N async CQL queries to tags.tags_by_id                  │
     │  │  │  └─────────────────────────────────────────────────────────────────┘
     │  │
     │  └─ for each result: toAtlasEntityHeader() [line 601-658]
     │     │
     │     ├─ mapVertexToAtlasEntityHeader() [EntityGraphRetriever.java:1916-2027]
     │     │  │ Core properties: from cache → 0 CQL ✓
     │     │  │ Timestamps, labels: from cache → 0 CQL ✓
     │     │  │
     │     │  │  ┌─────────────────────────────────────────────────────────────────┐
     │     │  │  │  CQL SITE #6: Classification Cache Miss Fallback               │
     │     │  │  │  Location: EntityGraphRetriever.java:1938-1939                 │
     │     │  │  │                                                                │
     │     │  │  │  if (tags == null) {  // cache miss!                           │
     │     │  │  │      tags = handleGetAllClassifications(entityVertex);          │
     │     │  │  │  }                                                             │
     │     │  │  │                                                                │
     │     │  │  │  → getAllClassifications_V2() [line 2423]                       │
     │     │  │  │  → tagDAO.getAllClassificationsForVertex(vertexId) — SYNC CQL   │
     │     │  │  │                                                                │
     │     │  │  │  This happens when:                                            │
     │     │  │  │  - classificationCache is null (prefetch disabled/failed)       │
     │     │  │  │  - vertex not in prefetch result (shouldn't happen, but does)   │
     │     │  │  │                                                                │
     │     │  │  │  Cost: 0-N sync CQL per entity (fallback path)                 │
     │     │  │  └─────────────────────────────────────────────────────────────────┘
     │     │  │
     │     │  │  ┌─────────────────────────────────────────────────────────────────┐
     │     │  │  │  CQL SITE #7: Term Assignment Query — UNDOCUMENTED HOTSPOT     │
     │     │  │  │  Location: EntityGraphRetriever.java:1962-1968                 │
     │     │  │  │  Method: mapAssignedTerms(entityVertex) [line 2582]            │
     │     │  │  │                                                                │
     │     │  │  │  // TODO: This should be optimized to use cache (line 1963)    │
     │     │  │  │  entityVertex.query()                                          │
     │     │  │  │      .direction(IN)                                            │
     │     │  │  │      .label(TERM_ASSIGNMENT_LABEL)                             │
     │     │  │  │      .edges()                                                  │
     │     │  │  │                                                                │
     │     │  │  │  → CassandraVertex.query() → CassandraVertexQuery              │
     │     │  │  │  → CQL: SELECT ... FROM edges_in                               │
     │     │  │  │         WHERE in_vertex_id=? AND edge_label=?                  │
     │     │  │  │                                                                │
     │     │  │  │  Then for EACH term edge found:                                │
     │     │  │  │    toTermAssignmentHeader(edge) [line 2597]:                   │
     │     │  │  │      edge.getOutVertex() → CQL to load term vertex             │
     │     │  │  │      GraphHelper.getGuid(termVertex) → property read           │
     │     │  │  │      getEncodedProperty(termVertex, displayName) → prop read   │
     │     │  │  │      edge.getProperty(...) × 5 → property reads               │
     │     │  │  │                                                                │
     │     │  │  │  Called PER ENTITY (inside per-result loop at line 1964).       │
     │     │  │  │  NOT BATCHED. NOT CACHED. NOT IN enrichVertexProperties.        │
     │     │  │  │                                                                │
     │     │  │  │  Cost: N CQL for edge query + N×T CQL for term vertex loads    │
     │     │  │  │  (T = avg term assignments per entity, typically 0-3)           │
     │     │  │  │  When entities have terms: this is a per-entity CQL cascade.   │
     │     │  │  └─────────────────────────────────────────────────────────────────┘
     │     │  │
     │     │  │  Header/Requested attributes: [line 1975-2017]
     │     │  │  → getVertexAttribute() [line 3254]
     │     │  │  → mapVertexToAttribute() [line 2685]
     │     │  │  │
     │     │  │  │  PRIMITIVE/ENUM: from cache → 0 CQL ✓
     │     │  │  │  OBJECT_ID_TYPE: from cache (mapVertexToObjectIdV2) → 0 CQL ✓
     │     │  │  │  ARRAY of OBJECT_ID: from cache → 0 CQL ✓
     │     │  │  │
     │     │  │  │  ┌─────────────────────────────────────────────────────┐
     │     │  │  │  │  CQL SITE #8: STRUCT Attribute Resolution           │
     │     │  │  │  │  Location: mapVertexToAttribute line 2714-2717     │
     │     │  │  │  │                                                    │
     │     │  │  │  │  case STRUCT:                                      │
     │     │  │  │  │    edgeLabel = getEdgeLabel(attribute.getName());  │
     │     │  │  │  │    ret = mapVertexToStruct(entityVertex, ...);     │
     │     │  │  │  │    → edge traversal → CQL per struct attribute     │
     │     │  │  │  │                                                    │
     │     │  │  │  │  Rare in typical indexsearch (structs uncommon     │
     │     │  │  │  │  in header/requested attrs). But when hit: CQL.   │
     │     │  │  │  │                                                    │
     │     │  │  │  │  Cost: 0-K CQL per entity (K = struct attrs)      │
     │     │  │  │  └─────────────────────────────────────────────────────┘
     │
     └─ scrubSearchResults() [line 666]
        └─ In-memory authorization post-filter — 0 CQL
```

### 2.2 Summary of ALL CQL Sites

| # | Location | What | Pattern | Cost | Batched? |
|---|----------|------|---------|------|----------|
| 1 | `prepareSearchResult:572` | Vertex materialisation from ES hits | SYNC, per-hit | N sequential CQL | No |
| 2 | `enrichVertex...:1505` | Bulk vertex property load | Async batch | ~0 (cache from #1) | Yes |
| 3 | `enrichVertex...:1523` | Edge fetch (dominant cost) | Async, all vertices × union labels | N × L_union × ~1 | Yes but over-fetches |
| 4 | `enrichVertex...:1583` | Reference vertex property load | Async batch | M async | Yes |
| 5 | `prefetchClassifications:2579` | Classification batch fetch | Async per-vertex | N async | Sort of (all vertices) |
| 6 | `mapVertexToAtlasEntityHeader:1939` | Classification cache miss fallback | SYNC per-entity | 0-N sync | No |
| 7 | `mapAssignedTerms:2582` | Term assignment edge + vertex load | SYNC per-entity | N + N×T sync | **No — major gap** |
| 8 | `mapVertexToAttribute:2714` | Struct attribute resolution | SYNC per-attr | 0-K sync | No |

### 2.3 The Three Remaining Architectural Problems

**Problem 1: Flat Label Union (Site #3)**
`collectEdgeLabelsWithDirection()` returns one `Map<label, direction>` for ALL vertices. Direction is correct (fixed earlier), but label scoping is not. A Connection vertex with 0 edges gets queried for 31 labels. A Table gets queried for glossary/domain/BI labels that can never exist on it.

**Problem 2: Unbatched Term Assignments (Site #7)**
`mapAssignedTerms()` is called inside the per-entity header construction loop. It does a per-entity edge query + per-edge vertex load. This bypasses all the bulk infrastructure built for enrichment. The code has a TODO acknowledging this: `// TODO: This should be optimized to use vertexEdgePropertiesCache`.

**Problem 3: Sequential Vertex Materialisation (Site #1)**
`result.getVertex()` does sync CQL per ES hit. On ZeroGraph, the ES `_id` field is the vertex_id — no CQL needed to extract the ID. The vertex load could be deferred to the async bulk Site #2.

---

## 3. What Previous Optimisations Fixed vs What Remains

| Optimisation | Status | What It Fixed | What It Didn't |
|-------------|--------|---------------|----------------|
| Direction-aware edge fetch | **Merged** | `edges_out` OR `edges_in` per label (not both) | Labels still flat union across types |
| Reference vertex cache (Caffeine) | **Merged** (but dropped in rewrite — stale data unacceptable) | Phase B5 CQL → 0 on warm cache | Served stale reference entity data within TTL window |
| Edge batch size 10→100 | **Merged** | Sequential RTTs reduced | Total CQL count unchanged |
| O(n²)→O(N+E) edge association | **Merged on some branches** | CPU waste in edge loop | May not be on this branch |
| Per-type label scoping | **NOT DONE** | — | Still the biggest remaining gap |
| Batch term assignments | **NOT DONE** | — | Per-entity CQL cascade |
| Smart classification pre-filter | **NOT DONE** | — | 75% wasted tag queries |
| Skip vertex materialisation | **NOT DONE** | — | N sequential sync CQL |

---

## 4. Hotspot Traceability Matrix

Every previously identified hotspot from all prior analysis documents, mapped to its resolution in the rewrite. Nothing is left unaddressed.

### 4.1 Confluence Doc Hotspots (5)

Source: [MS-692 CQL Amplification Analysis](https://atlanhq.atlassian.net/wiki/spaces/Metastore/pages/1788837894)

| Confluence Hotspot | Priority | Status | Rewrite Resolution | Stage |
|---|---|---|---|---|
| **H1: Flat label union across entity types** — `collectEdgeLabelsToProcess()` returns `Set<String>` union. Table has ~8 labels, union inflates to ~31. Connection gets 31 queries for 0 edges. | P0 | **Not fixed** | `TypeAwareEdgeLabelResolver.resolvePerVertex()` returns `Map<vertexId, Map<label, direction>>`. Each vertex gets ONLY its type's labels. Connection → 0 labels → 0 CQL. | Stage 2 |
| **H2: BOTH directions always queried** — `EdgeRepository` queries both `edges_out` AND `edges_in` for every label. Direction is deterministic per relationship. | P0 | **Already fixed** (earlier PR added `collectEdgeLabelsWithDirection()` + `getEdgesForVerticesByLabelsDirectionAware()`) | Rewrite preserves this — direction flows through `TypeAwareEdgeLabelResolver` to `getEdgesForVerticesPerType()`. | Stage 2 |
| **H3: Irrelevant edge-backed attributes** — 523 client attrs include `anchor`, `categories`, `parentDomain`, `explore`, etc. that never exist on Table/Column. | P1 | **Not fixed** (combined with H1) | Per-type resolution checks `entityType.getRelationshipAttributes().containsKey(attrName)`. Glossary/domain/BI attrs skipped for Table/Column because those types don't have them. Naturally eliminated by H1 fix. | Stage 2 |
| **H4: Sequential per-result vertex loading** — `result.getVertex()` → sync `CassandraGraph.getVertex()` → `SELECT * FROM vertices` per ES hit. On ZeroGraph, ES `_id` IS the vertex_id. | P1 | **Not fixed** | `DirectVertexIdExtractor` extracts vertex ID directly from ES hit `_id` field. 0 CQL. Vertex loading deferred to async bulk Stage 1. | Stage 0 |
| **H5: Blind classification queries** — `tagDAO.getAllClassificationsForVertices()` queries ALL N vertices. 75% have 0 classifications. | P2 | **Not fixed** | `SmartClassificationLoader` checks `__classificationNames` property from Stage 1 vertex data. Only queries vertices that actually have tags. Typical 20 → 5 CQL. | Stage 4 |

### 4.2 MS-821 Doc Hotspots (3)

Source: [MS-821 CQL Amplification Optimization](docs/ms-821-cql-amplification-optimization.md)

| MS-821 Hotspot | Status | Rewrite Resolution | Stage |
|---|---|---|---|
| **Zero cross-request caching for reference vertices** — Phase 3 re-fetches Connection/Database/Schema every request. | **Was fixed** (Caffeine cache, 60s TTL) but **dropped in rewrite** — stale data unacceptable. | Fresh async batch fetch every request. Optimisation comes from reduced CQL count (per-type labels), not caching. | Stage 3 |
| **O(n²) edge-to-vertex association** — nested loop `for(vertex) { for(allEdges) { ... } }` = 200K iterations for 100 results × 2000 edges. | **Fixed on some branches** | Rewrite uses HashMap pre-indexing in Stage 2. Edges returned per-vertex from `getEdgesForVerticesPerType()` — no post-hoc association needed. O(N+E). | Stage 2 |
| **Edge batch size default of 10** — 25 results = 3 sequential batches. | **Already fixed** (changed to 100) | Moot in rewrite — no vertex batching in edge fetch. All CQL queries fired in parallel in a single wave. | Stage 2 |

### 4.3 MS-692 Optimization Plan Phases (6)

Source: [MS-692 IndexSearch Optimization Plan](docs/ms-692-indexsearch-optimization-plan.md)

| MS-692 Phase | Status | Rewrite Resolution |
|---|---|---|
| **P1: Bulk tag/term cache** (80-120ms → 19ms prototype) | Stashed, not merged | **Subsumed** — Stage 4 (smart classification load) + Stage 5 (term assignments from cached edges). No separate tag/term cache needed. |
| **P2: CQL LIMIT push-down** — `CassandraVertexQuery` materialises ALL edges then truncates in Java. LIMIT-aware stmts exist but not wired. | Planned, not done | **Yes** — `getEdgesForVerticesPerType()` uses `selectEdgesOutByLabelLimitStmt` / `selectEdgesInByLabelLimitStmt` with `limitPerLabel`. LIMIT pushed into CQL. |
| **P3: Direct CQL for findEdge()** — O(n) scan for `getEdgeBetweenVertices()`. | Planned, not done | **Not in scope** — this is entity CRUD path (`EntityGraphMapper`), not IndexSearch rendering. Orthogonal improvement. |
| **P4: Split partition key** — `(out_vertex_id)` → `(out_vertex_id, edge_label)` compound key. | Design only | **Not in scope** — Cassandra schema migration, orthogonal to rendering pipeline rewrite. |
| **P5: State in clustering key** — Filter DELETED edges server-side. | Backlog | **Not in scope** — edge schema change. Current code filters in Java (adequate for rendering). |
| **P6: Edge count metadata table** — O(1) `getEdgesCount()`. | Backlog | **Not in scope** — IndexSearch doesn't need edge counts, only edge data. |

### 4.4 Newly Discovered Hotspots from Code Tracing (3)

These were NOT in any prior doc. Found by tracing every method in the IndexSearch call chain.

| New Hotspot | Location | Why It Was Missed | Rewrite Resolution | Stage |
|---|---|---|---|---|
| **Classification cache miss fallback** — When `classificationCache.get(vertexId)` returns null, falls back to `handleGetAllClassifications(entityVertex)` = per-entity sync CQL. | `EntityGraphRetriever.java:1938-1939` | Inside header construction loop, only triggers on cache miss. Looks benign but can fire N times if prefetch returns partial results. | **Eliminated** — `SmartClassificationLoader` batch result is authoritative. No fallback path. Empty list = no tags. | Stage 4 |
| **`mapAssignedTerms()` unbatched** — Per-entity edge query (`entityVertex.query().direction(IN).label(TERM_ASSIGNMENT).edges()`) + per-edge vertex load (`edge.getOutVertex()`) + property reads. Called inside per-entity header loop. Code has TODO: `// TODO: This should be optimized to use vertexEdgePropertiesCache` | `EntityGraphRetriever.java:1962-1968` calling `mapAssignedTerms()` at line 2582 | Hidden inside `mapVertexToAtlasEntityHeader()`. Not part of the `enrichVertexPropertiesByVertexIds()` pipeline. Looks like a simple method call but triggers per-entity CQL cascade. | **Eliminated** — `TERM_ASSIGNMENT_LABEL` included in Stage 2 edge labels. Term vertices loaded in Stage 3 reference cache. `TermAssignmentLoader` assembles headers from cached data. 0 CQL. | Stage 5 |
| **STRUCT attribute resolution** — `mapVertexToAttribute()` case STRUCT calls `mapVertexToStruct()` which traverses edges. Not cache-aware. | `EntityGraphRetriever.java:2714-2717` | Rare in IndexSearch (structs uncommon in requested attributes). But when triggered, causes per-attribute CQL. | **Partially addressed** — Most STRUCT attrs not requested in typical IndexSearch. Phase 4 handles edge cases. Can fall back to vertex property for non-edge-backed struct attrs. | Stage 6 (Phase 4) |

### 4.5 Summary

| Source | Total Hotspots | Already Fixed | Fixed in Rewrite | Not in Scope | Newly Found |
|--------|---------------|---------------|-----------------|--------------|-------------|
| Confluence doc | 5 | 1 (H2: direction) | **4** (H1, H3, H4, H5) | 0 | — |
| MS-821 doc | 3 | 3 (all merged) | Preserved in rewrite | 0 | — |
| MS-692 plan | 6 | 0 | **2** (P1 subsumed, P2 LIMIT) | 4 (P3-P6: CRUD/schema) | — |
| Code tracing | — | — | **3** | 0 | 3 (cls fallback, terms, struct) |
| **TOTAL** | **14 unique** | **4 already fixed** | **9 fixed in rewrite** | **4 not in scope** | **3 new** |

**Every hotspot that affects IndexSearch rendering is addressed. The 4 out-of-scope items (P3-P6) are entity CRUD or Cassandra schema changes — orthogonal to this rewrite.**

---

## 5. New Architecture

### 4.1 Design Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│               IndexSearchResultRenderer (NEW)                        │
│                                                                      │
│  Called FROM EntityDiscoveryService.directIndexSearch()               │
│  Replaces: prepareSearchResult() for IndexSearch path only           │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────┐      │
│  │              Enrichment Pipeline                           │      │
│  │                                                            │      │
│  │  Stage 0: VertexIdExtraction     → 0 CQL (ZeroGraph)      │      │
│  │  Stage 1: VertexBulkLoad         → N async CQL (1 RTT)    │      │
│  │  Stage 2: TypeAwareEdgeFetch     → N×L_type async (1 RTT) │      │
│  │  Stage 3: ReferenceVertexLoad    → M async (fresh)         │      │
│  │  Stage 4: ClassificationLoad     → K async (K ≤ N)         │      │
│  │  Stage 5: TermAssignmentLoad     → J async (J ≤ N) (NEW!) │      │
│  │  Stage 6: HeaderAssembly         → 0 CQL                  │      │
│  │                                                            │      │
│  │  All stages read/write SearchEnrichmentContext              │      │
│  └────────────────────────────────────────────────────────────┘      │
│                                                                      │
│  Post-pipeline: scrubSearchResults() (unchanged)                     │
└──────────────────────────────────────────────────────────────────────┘
```

### 4.2 Key Differences from Current Code

| Aspect | Current | New |
|--------|---------|-----|
| Vertex ID extraction | N sync CQL via `getVertex()` | 0 CQL — extract from ES `_id` |
| Edge label resolution | Flat union across all types | Per-vertex-type, only relevant labels |
| Edge fetch | N × L_union queries | N × L_type queries (L_type ≪ L_union) |
| Term assignments | Per-entity sync edge query + vertex loads | Batched as pipeline stage via edge cache |
| Classifications | Query all N vertices | Pre-filter via `__classificationNames` property |
| Edge-to-vertex association | O(N×E) nested loop | O(N+E) indexed HashMap |
| Classification fallback | Per-entity sync CQL on cache miss | Eliminated — batch result is authoritative |
| Architecture | 110-line method with interleaved concerns | Pipeline of single-responsibility stages |

---

## 6. Design Principles & Patterns

### 5.1 Patterns

| Pattern | Component | Rationale |
|---------|-----------|-----------|
| **Pipeline** | `EnrichmentStage` chain | Each stage has one job, reads context, writes context. Independently testable, replaceable, measurable. |
| **Strategy** | `VertexIdExtractor` interface | ZeroGraph: `DirectVertexIdExtractor` extracts ID from ES `_id` (0 CQL). JanusGraph path is not touched — uses existing `prepareSearchResult()` via feature flag. |
| **Context Object** | `SearchEnrichmentContext` | Single mutable state container flows through pipeline. Eliminates scattered state across `VertexEdgePropertiesCache`, `RequestContext` flags, method parameters. |
| **Template Method** | `EnrichmentStage.enrich(context)` | All stages share the same signature. Pipeline executor doesn't know stage internals. |
| **Registry Lookup** | `TypeAwareEdgeLabelResolver` | Pre-computes per-type edge labels using `AtlasTypeRegistry`. Cached in `ConcurrentHashMap` (type definitions don't change at runtime). |

### 5.2 SOLID Principles

| Principle | How Applied |
|-----------|-------------|
| **Single Responsibility** | Each stage does exactly one thing. `VertexBulkLoader` loads vertices. `TypeAwareEdgeFetcher` fetches edges. `SmartClassificationLoader` loads tags. No stage does two things. |
| **Open/Closed** | Adding a new enrichment (e.g., lineage preview) = add a new stage. No existing stage modified. |
| **Liskov Substitution** | All `EnrichmentStage` implementations are interchangeable in the pipeline. |
| **Interface Segregation** | `EnrichmentStage` has one method. `VertexIdExtractor` has one method. No fat interfaces. |
| **Dependency Inversion** | `IndexSearchResultRenderer` depends on `EnrichmentStage` interface, not on concrete loaders. Testable with mocks. |

### 5.3 What We Keep Unchanged

- **ES query path** — `_source=false`, ABAC pre-filtering, DSL optimisation — all in `EntityDiscoveryService`
- **`EntityGraphRetriever`** — non-IndexSearch paths (entity CRUD, lineage, export) use it. We don't modify it.
- **`CassandraGraph` / `EdgeRepository`** — low-level Cassandra access. We add one new method to EdgeRepository.
- **`scrubSearchResults()`** — authorisation post-filter. Called after our pipeline, unchanged.
- **`VertexEdgePropertiesCache`** — reused inside `SearchEnrichmentContext`.
- **REST layer** — `DiscoveryREST.java` unchanged. Same request/response contract.

---

## 7. Sequence Diagram — New Pipeline

```
Client               EntityDiscoveryService      ES            IndexSearchResultRenderer       CassandraGraph        EdgeRepository        TagDAO
  │                         │                     │                      │                          │                     │                   │
  │ POST /indexsearch       │                     │                      │                          │                     │                   │
  │ (20 entities)           │                     │                      │                          │                     │                   │
  │────────────────────────>│                     │                      │                          │                     │                   │
  │                         │                     │                      │                          │                     │                   │
  │                         │ addPreFilters (ABAC) │                      │                          │                     │                   │
  │                         │ optimizeQuery        │                      │                          │                     │                   │
  │                         │                     │                      │                          │                     │                   │
  │                         │ POST /_search       │                      │                          │                     │                   │
  │                         │ (_source=false)     │                      │                          │                     │                   │
  │                         │────────────────────>│                      │                          │                     │                   │
  │                         │ <── 20 doc IDs ─────│                      │                          │                     │                   │
  │                         │                     │                      │                          │                     │                   │
  │                         │ Feature flag ON?    │                      │                          │                     │                   │
  │                         │ ─── YES ────────────────────────────────>  │                          │                     │                   │
  │                         │                     │                      │                          │                     │                   │
  │                         │                     │  ┌─ STAGE 0: VertexIdExtraction ─────────────────────────────────────────────────────────────┐
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ ZeroGraph: ES _id = vertex_id. Extract directly. 0 CQL.                                  │
  │                         │                     │  │ Result: orderedVertexIds = ["41996504", "42094840", ...]                                  │
  │                         │                     │  │                                                                                           │
  │                         │                     │  └───────────────────────────────────────────────────────────────────────────────────────────────┘
  │                         │                     │                      │                          │                     │                   │
  │                         │                     │  ┌─ STAGE 1: VertexBulkLoad ─────────────────────────────────────────────────────────────────┐
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ graph.getVertices(20 IDs)                    │                     │                   │  │
  │                         │                     │  │ → vertexRepository.getVerticesAsync()         │                     │                   │  │
  │                         │                     │  │ → 20 async parallel CQL: SELECT * FROM vertices WHERE vertex_id=?  │                   │  │
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ Populate context.vertexProperties (all props) + context.vertexTypeMap                     │
  │                         │                     │  │ Cost: 20 async CQL, 1 RTT                                                                │
  │                         │                     │  │                                                                                           │
  │                         │                     │  └───────────────────────────────────────────────────────────────────────────────────────────────┘
  │                         │                     │                      │                          │                     │                   │
  │                         │                     │  ┌─ STAGE 2: TypeAwareEdgeFetch ──────────────────────────────────────────────────────────────┐
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ 2a. TypeAwareEdgeLabelResolver.resolve(vertexTypeMap, requestedAttrs)                     │
  │                         │                     │  │     Per-vertex-type label resolution:                                                     │
  │                         │                     │  │       vertex 41996504 (Table):  {__Table.columns→OUT, __Schema.tables→IN, ...} = 8 labels │
  │                         │                     │  │       vertex 42094840 (Table):  same 8 labels (cached for type)                           │
  │                         │                     │  │       vertex 20664 (Connection): {} = 0 labels → 0 CQL!                                  │
  │                         │                     │  │     0 CQL (in-memory type registry, ConcurrentHashMap cache)                              │
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ 2b. EdgeRepository.getEdgesForVerticesPerType(perVertexLabels, limit)                     │
  │                         │                     │  │     For vertex 41996504: 8 async CQL (8 labels × 1 direction each)                        │
  │                         │                     │  │     For vertex 42094840: 8 async CQL                                                     │
  │                         │                     │  │     For vertex 20664:   0 async CQL (no labels!)                                         │
  │                         │                     │  │     ...                                                                                   │
  │                         │                     │  │     Total: ~120 async CQL (vs ~620 with flat union)                                       │
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ 2c. O(N+E) indexed edge-to-vertex association (HashMap pre-index)                         │
  │                         │                     │  │     Collect referencedVertexIds. Remove already-loaded IDs.                               │
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ Cost: ~120 async CQL, 1 RTT (all parallel)                                               │
  │                         │                     │  │                                                                                           │
  │                         │                     │  └───────────────────────────────────────────────────────────────────────────────────────────────┘
  │                         │                     │                      │                          │                     │                   │
  │                         │                     │  ┌─ STAGE 3: ReferenceVertexLoad ────────────────────────────────────────────────────────────┐
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ Collect referenced vertex IDs from Stage 2 edges                                          │
  │                         │                     │  │ Remove IDs already loaded in Stage 1 (result vertices)                                    │
  │                         │                     │  │ Fresh async batch fetch via graph.getVertices() — no cross-request cache                  │
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ Cost: M async CQL (M = unique referenced vertices, ~30-60), 1 RTT                        │
  │                         │                     │  │                                                                                           │
  │                         │                     │  └───────────────────────────────────────────────────────────────────────────────────────────────┘
  │                         │                     │                      │                          │                     │                   │
  │                         │                     │  ┌─ STAGE 4: SmartClassificationLoad ────────────────────────────────────────────────────────┐
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ Check __classificationNames / __traitNames property from Stage 1 vertex data              │
  │                         │                     │  │ Only query vertices that actually have classifications                                    │
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ tagDAO.getAllClassificationsForVertices(filteredVertexIds)                                 │
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ Cost: K async CQL where K = vertices with tags (typically K ≈ 0.25 × N)                  │
  │                         │                     │  │ No fallback path — batch result is authoritative                                          │
  │                         │                     │  │                                                                                           │
  │                         │                     │  └───────────────────────────────────────────────────────────────────────────────────────────────┘
  │                         │                     │                      │                          │                     │                   │
  │                         │                     │  ┌─ STAGE 5: TermAssignmentLoad (NEW — fixes undocumented hotspot) ──────────────────────────┐
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ Option A: Use TERM_ASSIGNMENT edges already fetched in Stage 2                             │
  │                         │                     │  │   If TERM_ASSIGNMENT_LABEL was included in edge labels, edges are in context.              │
  │                         │                     │  │   Build AtlasTermAssignmentHeader from cached edge data + referenced vertex props.         │
  │                         │                     │  │   Cost: 0 CQL                                                                            │
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ Option B: Batch fetch term assignment edges for all vertices                               │
  │                         │                     │  │   If TERM_ASSIGNMENT not in Stage 2 (because includeMeanings resolved later):              │
  │                         │                     │  │   Single batch query: getEdgesForVertices(allVertexIds, {TERM_ASSIGNMENT→IN}, limit)       │
  │                         │                     │  │   Then batch load term vertices (already in reference vertex cache or Stage 3)             │
  │                         │                     │  │   Cost: N async CQL (1 per vertex, 1 RTT) + 0 for term vertices (cached)                  │
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ Current code: N sync per-entity edge queries + N×T sync vertex loads = major hotspot       │
  │                         │                     │  │                                                                                           │
  │                         │                     │  └───────────────────────────────────────────────────────────────────────────────────────────────┘
  │                         │                     │                      │                          │                     │                   │
  │                         │                     │  ┌─ STAGE 6: HeaderAssembly ──────────────────────────────────────────────────────────────────┐
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ For each vertex in order:                                                                 │
  │                         │                     │  │   Build AtlasEntityHeader from context data:                                              │
  │                         │                     │  │     - Core properties (typeName, guid, status, timestamps) → from context                 │
  │                         │                     │  │     - Classifications → from context (Stage 4)                                            │
  │                         │                     │  │     - Term assignments → from context (Stage 5)                                           │
  │                         │                     │  │     - Header attributes (name, qualifiedName, etc.) → from context                        │
  │                         │                     │  │     - Requested attributes → from context (primitive/enum from props, rel from edges)     │
  │                         │                     │  │     - Relationship attributes → from context (Stage 2 edges + Stage 3 ref props)          │
  │                         │                     │  │   Add score, highlights, sort, collapse from ES hit data                                  │
  │                         │                     │  │                                                                                           │
  │                         │                     │  │ Cost: 0 CQL — all data in context                                                        │
  │                         │                     │  │                                                                                           │
  │                         │                     │  └───────────────────────────────────────────────────────────────────────────────────────────────┘
  │                         │                     │                      │                          │                     │                   │
  │                         │  <── enriched result ──────────────────────│                          │                     │                   │
  │                         │                     │                      │                          │                     │                   │
  │                         │ scrubSearchResults() (unchanged, in-memory)│                          │                     │                   │
  │                         │                     │                      │                          │                     │                   │
  │ <── 200 OK ─────────────│                     │                      │                          │                     │                   │
```

---

## 8. Pseudo Code — Complete Implementation

### 7.1 Core Interfaces

```java
/**
 * Single stage in the enrichment pipeline. All stages share
 * the same SearchEnrichmentContext for reading inputs and writing outputs.
 */
public interface EnrichmentStage {
    void enrich(SearchEnrichmentContext context) throws AtlasBaseException;
    String name();  // for metrics: "indexSearch.stage.<name>"
}

/**
 * Strategy interface for extracting vertex IDs from ES results.
 * Only used for ZeroGraph (CassandraGraph) — JanusGraph path is unchanged.
 */
public interface VertexIdExtractor {
    List<String> extractVertexIds(List<ESHitResult> esHits);
}
```

### 7.2 SearchEnrichmentContext

```java
/**
 * Mutable context that flows through all pipeline stages.
 * Each stage populates its section. Header assembly reads all sections.
 */
public class SearchEnrichmentContext {

    // ──── Input (immutable after construction) ────
    private final List<ESHitResult> esHits;
    private final Set<String> requestedAttributes;
    private final Set<String> requestedRelationAttributes;
    private final SearchParams searchParams;
    private final boolean includeClassifications;
    private final boolean includeClassificationNames;
    private final boolean includeMeanings;

    // ──── Stage 0 output ────
    private List<String> orderedVertexIds;  // preserves ES result order

    // ──── Stage 1 output ────
    private final Map<String, Map<String, List<?>>> vertexProperties = new HashMap<>();
    private final Map<String, AtlasVertex> vertexObjects = new HashMap<>();
    private final Map<String, String> vertexTypeMap = new HashMap<>();  // vertexId → typeName

    // ──── Stage 2 output ────
    // vertexId → list of edges (each edge has: id, label, inVertexId, outVertexId, valueMap)
    private final Map<String, List<EdgeVertexReference>> vertexEdges = new HashMap<>();
    private final Set<String> referencedVertexIds = new HashSet<>();

    // ──── Stage 4 output ────
    private final Map<String, List<AtlasClassification>> classificationMap = new HashMap<>();

    // ──── Stage 5 output ────
    private final Map<String, List<AtlasTermAssignmentHeader>> termAssignmentMap = new HashMap<>();

    // ──── Accessors ────

    /**
     * Get a single property value from a vertex's cached properties.
     * Used by HeaderAssembler to read without CQL.
     */
    @SuppressWarnings("unchecked")
    public <T> T getVertexProperty(String vertexId, String propertyKey, Class<T> type) {
        Map<String, List<?>> props = vertexProperties.get(vertexId);
        if (props == null) return null;

        List<?> values = props.get(propertyKey);
        if (values == null || values.isEmpty()) return null;

        Object value = values.get(0);
        if (value == null) return null;

        if (type.isInstance(value)) return type.cast(value);
        // Handle String→Long, String→Boolean conversions for common cases
        if (type == Long.class && value instanceof Number) return type.cast(((Number) value).longValue());
        if (type == String.class) return type.cast(String.valueOf(value));
        return null;
    }

    /**
     * Get all edges for a vertex matching a specific label.
     */
    public List<EdgeVertexReference> getEdgesForLabel(String vertexId, String edgeLabel) {
        List<EdgeVertexReference> edges = vertexEdges.get(vertexId);
        if (edges == null) return Collections.emptyList();
        return edges.stream()
            .filter(e -> edgeLabel.equals(e.getEdgeLabel()))
            .collect(Collectors.toList());
    }

    // Stage population methods
    public void addVertexData(String vertexId, Map<String, List<?>> properties, AtlasVertex vertex) {
        vertexProperties.put(vertexId, properties);
        vertexObjects.put(vertexId, vertex);
        // Extract typeName from properties
        List<?> typeNames = properties.get(Constants.TYPE_NAME_PROPERTY_KEY);
        if (typeNames != null && !typeNames.isEmpty()) {
            vertexTypeMap.put(vertexId, String.valueOf(typeNames.get(0)));
        }
    }

    public void addEdges(String vertexId, List<EdgeVertexReference> edges) {
        vertexEdges.put(vertexId, edges);
    }

    // ... standard builder/getters omitted for brevity
}
```

### 7.3 ESHitResult — Lightweight ES Hit Wrapper

```java
/**
 * Wraps the raw ES hit LinkedHashMap with typed accessors.
 * Avoids coupling pipeline stages to ES response format.
 */
public class ESHitResult {
    private final LinkedHashMap<String, Object> hit;

    public ESHitResult(LinkedHashMap<String, Object> hit) {
        this.hit = hit;
    }

    public String getDocumentId() {
        return String.valueOf(hit.get("_id"));
    }

    public double getScore() {
        Object score = hit.get("_score");
        return score != null ? Double.parseDouble(String.valueOf(score)) : -1;
    }

    @SuppressWarnings("unchecked")
    public Map<String, List<String>> getHighlights() {
        Object highlight = hit.get("highlight");
        return highlight != null ? (Map<String, List<String>>) highlight : Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    public ArrayList<Object> getSort() {
        Object sort = hit.get("sort");
        return (sort instanceof List) ? (ArrayList<Object>) sort : new ArrayList<>();
    }

    @SuppressWarnings("unchecked")
    public Map<String, LinkedHashMap> getInnerHits() {
        Object innerHits = hit.get("inner_hits");
        return innerHits != null ? AtlasType.fromJson(AtlasType.toJson(innerHits), Map.class) : null;
    }
}
```

### 7.4 DirectVertexIdExtractor (Stage 0)

```java
/**
 * ZeroGraph strategy: ES _id IS the Cassandra vertex_id.
 * No CQL needed — extract directly from the ES hit.
 *
 * Current code does: result.getVertex() → CassandraGraph.getVertex(docId) → SYNC CQL
 * New code does: hit.get("_id") → String. Zero CQL.
 */
public class DirectVertexIdExtractor implements VertexIdExtractor {

    @Override
    public List<String> extractVertexIds(List<ESHitResult> esHits) {
        List<String> ids = new ArrayList<>(esHits.size());
        for (ESHitResult hit : esHits) {
            String docId = hit.getDocumentId();
            if (docId != null) {
                ids.add(docId);
            } else {
                LOG.warn("ES hit with null _id, skipping");
            }
        }
        return ids;
    }
}

// JanusGraph path: NOT touched. Uses existing prepareSearchResult() via feature flag.
```

### 7.5 VertexBulkLoader (Stage 1)

```java
/**
 * Stage 1: Bulk-load all result vertex properties in a single async batch.
 *
 * Current code: getVertexPropertiesValueMap() — same logic but this is a clean stage.
 * We reuse the same CassandraGraph.getVertices() + property iteration pattern.
 */
public class VertexBulkLoader implements EnrichmentStage {

    private final AtlasGraph graph;

    @Override
    public String name() { return "vertexBulkLoad"; }

    @Override
    public void enrich(SearchEnrichmentContext context) throws AtlasBaseException {
        List<String> vertexIds = context.getOrderedVertexIds();
        if (CollectionUtils.isEmpty(vertexIds)) return;

        // Async batch fetch — all 20 vertices in parallel, 1 RTT
        String[] ids = vertexIds.toArray(new String[0]);
        Set<AtlasVertex> vertices = graph.getVertices(ids);

        for (AtlasVertex vertex : vertices) {
            String vertexId = vertex.getIdForDisplay();
            Map<String, List<?>> properties = new HashMap<>();

            for (String key : vertex.getPropertyKeys()) {
                Collection<Object> values = vertex.getPropertyValues(key, Object.class);
                if (values != null && !values.isEmpty()) {
                    properties.put(key, new ArrayList<>(values));
                }
            }

            context.addVertexData(vertexId, properties, vertex);
        }
    }
}
```

### 7.6 TypeAwareEdgeLabelResolver

```java
/**
 * Per-vertex-type edge label resolution with correct direction.
 *
 * Current: collectEdgeLabelsWithDirection() returns flat Map<label, direction> (union of all types).
 * New: returns Map<vertexId, Map<label, direction>> — each vertex gets ONLY its type's labels.
 *
 * The direction-aware part already exists in current code.
 * The per-vertex-type scoping is what's new and delivers the main CQL reduction.
 */
public class TypeAwareEdgeLabelResolver {

    private final AtlasTypeRegistry typeRegistry;

    /**
     * Cache: cacheKey → { edgeLabel → direction }
     * Type defs don't change at runtime. Safe to cache indefinitely.
     * Key includes attribute set hash to handle different attribute requests.
     */
    private final ConcurrentHashMap<String, Map<String, AtlasEdgeDirection>> typeCache = new ConcurrentHashMap<>();

    /**
     * Resolve edge labels per-vertex-type.
     *
     * @return vertexId → { edgeLabel → direction }. Empty map for vertex = 0 CQL for that vertex.
     */
    public Map<String, Map<String, AtlasEdgeDirection>> resolvePerVertex(
            Map<String, String> vertexTypeMap,
            Set<String> requestedAttrs,
            Set<String> requestedRelAttrs,
            boolean includeMeanings) {

        Map<String, Map<String, AtlasEdgeDirection>> result = new HashMap<>();

        // Group vertices by type — vertices of the same type share the same label set
        Map<String, List<String>> typeToVertexIds = new HashMap<>();
        for (Map.Entry<String, String> entry : vertexTypeMap.entrySet()) {
            typeToVertexIds.computeIfAbsent(entry.getValue(), k -> new ArrayList<>())
                .add(entry.getKey());
        }

        for (Map.Entry<String, List<String>> typeEntry : typeToVertexIds.entrySet()) {
            String typeName = typeEntry.getKey();
            List<String> vertexIds = typeEntry.getValue();

            // Get or compute label set for this type (cached)
            String cacheKey = typeName + "|" + Objects.hash(requestedAttrs, requestedRelAttrs, includeMeanings);
            Map<String, AtlasEdgeDirection> labelsForType = typeCache.computeIfAbsent(
                cacheKey, k -> computeLabelsForType(typeName, requestedAttrs, requestedRelAttrs, includeMeanings)
            );

            // All vertices of this type get the same label set
            if (!labelsForType.isEmpty()) {
                for (String vertexId : vertexIds) {
                    result.put(vertexId, labelsForType);
                }
            }
            // Empty labelsForType → 0 CQL for these vertices (e.g., Connection)
        }

        return result;
    }

    private Map<String, AtlasEdgeDirection> computeLabelsForType(
            String typeName, Set<String> requestedAttrs,
            Set<String> requestedRelAttrs, boolean includeMeanings) {

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        if (entityType == null) return Collections.emptyMap();

        Map<String, AtlasEdgeDirection> labels = new HashMap<>();
        RequestContext ctx = RequestContext.get();

        // Check all requested attributes — which ones are relationship attrs for THIS type?
        Set<String> allAttrs = new HashSet<>();
        if (requestedAttrs != null) allAttrs.addAll(requestedAttrs);
        if (requestedRelAttrs != null) allAttrs.addAll(requestedRelAttrs);

        for (String attrName : allAttrs) {
            if (!entityType.getRelationshipAttributes().containsKey(attrName)) continue;

            AtlasAttribute attr = entityType.getRelationshipAttribute(attrName, null);
            if (attr == null || attr.getAttributeType() == null) continue;

            // Skip if product call with no relAttrsForSearch (matches existing behaviour)
            if (ctx.isInvokedByIndexSearch() && ctx.isInvokedByProduct()
                    && CollectionUtils.isEmpty(ctx.getRelationAttrsForSearch())
                    && !requestedAttrs.contains(attrName)) {
                continue;
            }

            String label = attr.getRelationshipEdgeLabel();
            AtlasEdgeDirection direction = toAtlasEdgeDirection(attr.getRelationshipEdgeDirection());
            if (label != null) {
                labels.merge(label, direction, (existing, incoming) ->
                    existing == incoming ? existing : AtlasEdgeDirection.BOTH);
            }
        }

        // Include header attributes that are edge-backed
        for (AtlasAttribute headerAttr : entityType.getHeaderAttributes().values()) {
            if (headerAttr.getRelationshipEdgeLabel() != null) {
                labels.merge(
                    headerAttr.getRelationshipEdgeLabel(),
                    toAtlasEdgeDirection(headerAttr.getRelationshipEdgeDirection()),
                    (existing, incoming) -> existing == incoming ? existing : AtlasEdgeDirection.BOTH
                );
            }
        }

        // Include TERM_ASSIGNMENT_LABEL if meanings requested
        if (includeMeanings) {
            labels.put(TERM_ASSIGNMENT_LABEL, AtlasEdgeDirection.IN);
        }

        return labels;
    }
}
```

### 7.7 TypeAwareEdgeFetcher (Stage 2)

```java
/**
 * Stage 2: Fetch edges using per-vertex-type labels and correct direction.
 *
 * Current: N × L_union × ~1 async CQL (L_union = flat union of all types)
 * New:     Σ(per vertex: |type_labels| × 1) async CQL
 *
 * Also fixes O(n²) edge association by using HashMap pre-indexing.
 */
public class TypeAwareEdgeFetcher implements EnrichmentStage {

    private final AtlasGraph graph;
    private final TypeAwareEdgeLabelResolver labelResolver;

    @Override
    public String name() { return "typeAwareEdgeFetch"; }

    @Override
    public void enrich(SearchEnrichmentContext context) throws AtlasBaseException {
        int limitPerLabel = RequestContext.get().isInvokedByProduct()
            ? MIN_EDGES_SUPER_VERTEX.getInt()
            : MAX_EDGES_SUPER_VERTEX.getInt();

        // Resolve per-vertex-type labels (0 CQL, in-memory)
        Map<String, Map<String, AtlasEdgeDirection>> perVertexLabels =
            labelResolver.resolvePerVertex(
                context.getVertexTypeMap(),
                context.getRequestedAttributes(),
                context.getRequestedRelationAttributes(),
                context.isIncludeMeanings()
            );

        if (perVertexLabels.isEmpty()) return;

        // Fetch edges — new method that accepts per-vertex label maps
        // This pipeline only runs on ZeroGraph (CassandraGraph)
        Map<String, List<Map<String, Object>>> edgesByVertex =
            ((CassandraGraph) graph).getEdgesForVerticesPerType(perVertexLabels, limitPerLabel);

        // Process edges into context with O(N+E) indexed association
        for (Map.Entry<String, List<Map<String, Object>>> entry : edgesByVertex.entrySet()) {
            String vertexId = entry.getKey();
            List<Map<String, Object>> rawEdges = entry.getValue();
            List<EdgeVertexReference> edgeRefs = new ArrayList<>(rawEdges.size());

            for (Map<String, Object> edgeData : rawEdges) {
                // Filter: only ACTIVE edges with relationship GUID
                String state = String.valueOf(edgeData.getOrDefault("state", ""));
                if (!ACTIVE.name().equals(state)) continue;
                if (edgeData.get("relationshipGuid") == null) continue;

                String edgeId = String.valueOf(edgeData.get("id"));
                String edgeLabel = String.valueOf(edgeData.get("label"));
                String outVertexId = String.valueOf(edgeData.get("outVertexId"));
                String inVertexId = String.valueOf(edgeData.get("inVertexId"));
                LinkedHashMap<Object, Object> valueMap =
                    (LinkedHashMap<Object, Object>) edgeData.get("valueMap");

                String referencedVertex = vertexId.equals(outVertexId) ? inVertexId : outVertexId;
                context.getReferencedVertexIds().add(referencedVertex);

                edgeRefs.add(new EdgeVertexReference(
                    referencedVertex, edgeId, edgeLabel, inVertexId, outVertexId, valueMap
                ));
            }

            context.addEdges(vertexId, edgeRefs);
        }

        // Remove result vertex IDs from reference set (already loaded in Stage 1)
        context.getReferencedVertexIds().removeAll(new HashSet<>(context.getOrderedVertexIds()));
    }
}
```

### 7.8 ReferenceVertexLoader (Stage 3)

```java
/**
 * Stage 3: Load properties for referenced vertices (other end of edges).
 * These are typically Connection, Database, Schema entities.
 *
 * No cross-request cache — stale data is unacceptable. Every request
 * fetches fresh data. The optimisation comes from reduced CQL count
 * (per-type labels eliminate most unnecessary edge queries, so fewer
 * referenced vertices are discovered) and async parallel fetch.
 */
public class ReferenceVertexLoader implements EnrichmentStage {

    private final AtlasGraph graph;

    @Override
    public String name() { return "referenceVertexLoad"; }

    @Override
    public void enrich(SearchEnrichmentContext context) throws AtlasBaseException {
        Set<String> refIds = context.getReferencedVertexIds();
        if (CollectionUtils.isEmpty(refIds)) return;

        // Async batch fetch — all referenced vertices in parallel, 1 RTT
        String[] ids = refIds.toArray(new String[0]);
        Set<AtlasVertex> vertices = graph.getVertices(ids);

        for (AtlasVertex vertex : vertices) {
            String vertexId = vertex.getIdForDisplay();
            Map<String, List<?>> properties = new HashMap<>();
            for (String key : vertex.getPropertyKeys()) {
                Collection<Object> values = vertex.getPropertyValues(key, Object.class);
                if (values != null && !values.isEmpty()) {
                    properties.put(key, new ArrayList<>(values));
                }
            }

            context.getVertexProperties().put(vertexId, properties);
            context.getVertexObjects().put(vertexId, vertex);
        }
    }
}
```

### 7.9 SmartClassificationLoader (Stage 4)

```java
/**
 * Stage 4: Load classifications only for vertices that have them.
 *
 * Current: tagDAO.getAllClassificationsForVertices(ALL N vertices)
 * New: pre-filter using __classificationNames from Stage 1 properties, query only K ≤ N
 *
 * Also eliminates the fallback path in mapVertexToAtlasEntityHeader:1938-1939
 * where cache miss triggers per-entity sync CQL. Here, the batch is authoritative.
 */
public class SmartClassificationLoader implements EnrichmentStage {

    private final TagDAO tagDAO;

    @Override
    public String name() { return "smartClassificationLoad"; }

    @Override
    public void enrich(SearchEnrichmentContext context) throws AtlasBaseException {
        if (!context.isIncludeClassifications()) return;
        if (!DynamicConfigStore.isTagV2Enabled()) return;

        // Pre-filter: check which vertices actually have classifications
        List<String> verticesWithTags = new ArrayList<>();
        for (String vertexId : context.getOrderedVertexIds()) {
            String clsNames = context.getVertexProperty(vertexId, CLASSIFICATION_NAMES_KEY, String.class);
            String traitNames = context.getVertexProperty(vertexId, TRAIT_NAMES_PROPERTY_KEY, String.class);

            if (StringUtils.isNotEmpty(clsNames) || StringUtils.isNotEmpty(traitNames)) {
                verticesWithTags.add(vertexId);
            }
        }

        if (verticesWithTags.isEmpty()) return;

        // Batch fetch only for vertices with tags
        Map<String, List<AtlasClassification>> clsMap =
            tagDAO.getAllClassificationsForVertices(verticesWithTags);

        if (clsMap != null) {
            context.getClassificationMap().putAll(clsMap);
        }
    }
}
```

### 7.10 TermAssignmentLoader (Stage 5)

```java
/**
 * Stage 5: Build term assignments from edge data already in context.
 *
 * Current code (mapAssignedTerms at line 2582):
 *   Per entity: entityVertex.query().direction(IN).label(TERM_ASSIGNMENT).edges()
 *   Per edge: edge.getOutVertex() → CQL load, multiple property reads
 *   = N sync edge queries + N×T sync vertex loads (UNBATCHED!)
 *
 * New code: TERM_ASSIGNMENT_LABEL was included in Stage 2 edge labels.
 * Term edges are already in context. Term vertices are in Stage 3 reference cache.
 * We just assemble AtlasTermAssignmentHeader from cached data. 0 CQL.
 */
public class TermAssignmentLoader implements EnrichmentStage {

    @Override
    public String name() { return "termAssignmentLoad"; }

    @Override
    public void enrich(SearchEnrichmentContext context) throws AtlasBaseException {
        if (!context.isIncludeMeanings()) return;

        for (String vertexId : context.getOrderedVertexIds()) {
            List<EdgeVertexReference> termEdges =
                context.getEdgesForLabel(vertexId, TERM_ASSIGNMENT_LABEL);

            if (termEdges.isEmpty()) {
                context.getTermAssignmentMap().put(vertexId, Collections.emptyList());
                continue;
            }

            List<AtlasTermAssignmentHeader> headers = new ArrayList<>();
            for (EdgeVertexReference edge : termEdges) {
                // Edge state already filtered in Stage 2
                AtlasTermAssignmentHeader header = new AtlasTermAssignmentHeader();

                String termVertexId = edge.getReferencedVertexId();

                // Term GUID — from reference vertex properties (Stage 3)
                String termGuid = context.getVertexProperty(termVertexId, GUID_PROPERTY_KEY, String.class);
                if (termGuid != null) header.setTermGuid(termGuid);

                // Display text — from reference vertex properties
                String displayName = context.getVertexProperty(
                    termVertexId, GLOSSARY_TERM_DISPLAY_NAME_ATTR, String.class);
                if (displayName != null) header.setDisplayText(displayName);

                // Relationship GUID — from edge valueMap
                Object relGuid = edge.getValueMap().get(RELATIONSHIP_GUID_PROPERTY_KEY);
                if (relGuid != null) header.setRelationGuid(String.valueOf(relGuid));

                // Edge properties — all from edge valueMap (already in memory)
                Object desc = edge.getValueMap().get(TERM_ASSIGNMENT_ATTR_DESCRIPTION);
                if (desc != null) header.setDescription(String.valueOf(desc));

                Object expr = edge.getValueMap().get(TERM_ASSIGNMENT_ATTR_EXPRESSION);
                if (expr != null) header.setExpression(String.valueOf(expr));

                Object status = edge.getValueMap().get(TERM_ASSIGNMENT_ATTR_STATUS);
                if (status != null) header.setStatus(AtlasTermAssignmentStatus.valueOf(String.valueOf(status)));

                Object confidence = edge.getValueMap().get(TERM_ASSIGNMENT_ATTR_CONFIDENCE);
                if (confidence instanceof Integer) header.setConfidence((Integer) confidence);

                headers.add(header);
            }

            context.getTermAssignmentMap().put(vertexId, headers);
        }
    }
}
```

### 7.11 HeaderAssembler (Stage 6)

```java
/**
 * Builds AtlasEntityHeader from enriched context. 0 CQL.
 *
 * Consolidates the 4 overloaded mapVertexToAtlasEntityHeader() methods
 * (lines 1883-2260) in EntityGraphRetriever into a single clean builder.
 */
public class HeaderAssembler {

    private final AtlasTypeRegistry typeRegistry;

    public AtlasEntityHeader assemble(String vertexId, SearchEnrichmentContext context)
            throws AtlasBaseException {

        AtlasEntityHeader header = new AtlasEntityHeader();

        // ── Core properties ──
        String typeName = context.getVertexProperty(vertexId, TYPE_NAME_PROPERTY_KEY, String.class);
        String guid = context.getVertexProperty(vertexId, GUID_PROPERTY_KEY, String.class);
        String state = context.getVertexProperty(vertexId, STATE_PROPERTY_KEY, String.class);

        header.setTypeName(typeName);
        header.setGuid(guid);
        header.setStatus(state != null ? AtlasEntity.Status.valueOf(state) : AtlasEntity.Status.ACTIVE);

        // ── Timestamps ──
        header.setCreatedBy(context.getVertexProperty(vertexId, CREATED_BY_KEY, String.class));
        header.setUpdatedBy(context.getVertexProperty(vertexId, MODIFIED_BY_KEY, String.class));

        Long createdTime = context.getVertexProperty(vertexId, TIMESTAMP_PROPERTY_KEY, Long.class);
        Long updatedTime = context.getVertexProperty(vertexId, MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
        if (createdTime != null) {
            header.setCreateTime(new Date(createdTime));
            header.setUpdateTime(new Date(Optional.ofNullable(updatedTime).orElse(createdTime)));
        }

        // ── Labels ──
        String labelsStr = context.getVertexProperty(vertexId, LABELS_PROPERTY_KEY, String.class);
        if (labelsStr != null) {
            header.setLabels(GraphHelper.parseLabelsString(labelsStr));
        }

        // ── Classifications (from Stage 4) ──
        if (context.isIncludeClassifications()) {
            List<AtlasClassification> tags = context.getClassificationMap().get(vertexId);
            // No fallback to per-entity CQL — batch result is authoritative
            header.setClassifications(tags != null ? tags : Collections.emptyList());
            header.setClassificationNames(getAllTagNames(tags));
        } else if (context.isIncludeClassificationNames()) {
            String clsNames = context.getVertexProperty(vertexId, CLASSIFICATION_NAMES_KEY, String.class);
            header.setClassificationNames(parseClassificationNames(clsNames));
        }

        // ── Term assignments (from Stage 5 — was per-entity CQL, now 0 CQL) ──
        if (context.isIncludeMeanings()) {
            List<AtlasTermAssignmentHeader> terms = context.getTermAssignmentMap()
                .getOrDefault(vertexId, Collections.emptyList());
            header.setMeanings(terms);
            header.setMeaningNames(terms.stream()
                .map(AtlasTermAssignmentHeader::getDisplayText)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        }

        // ── Type-specific attributes ──
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        if (entityType == null) {
            LOG.warn("Entity type not found: {} for vertex {}", typeName, vertexId);
            return header;
        }

        header.setSuperTypeNames(entityType.getAllSuperTypes());

        // Doc ID
        AtlasVertex vertex = context.getVertexObjects().get(vertexId);
        if (vertex != null) {
            header.setDocId(LongEncodingUtil.vertexIdToDocId(vertex.getIdForDisplay()));
        }

        // Header attributes (name, qualifiedName, description, owner, etc.)
        for (AtlasAttribute headerAttr : entityType.getHeaderAttributes().values()) {
            Object value = resolveAttribute(vertexId, headerAttr, context);
            if (value != null) header.setAttribute(headerAttr.getName(), value);
        }

        // Display text
        Object displayText = resolveDisplayText(vertexId, entityType, context);
        if (displayText != null) header.setDisplayText(displayText.toString());

        // Requested attributes (beyond headers)
        if (CollectionUtils.isNotEmpty(context.getRequestedAttributes())) {
            for (String attrName : context.getRequestedAttributes()) {
                if (header.hasAttribute(attrName)) continue;

                AtlasAttribute attr = entityType.getAttribute(attrName);
                if (attr == null) {
                    String cleanName = toNonQualifiedName(attrName);
                    if (header.hasAttribute(cleanName)) continue;
                    attr = entityType.getAttribute(cleanName);
                    if (attr == null) {
                        attr = entityType.getRelationshipAttribute(cleanName, null);
                        if (attr != null && RequestContext.get().isInvokedByProduct()
                                && CollectionUtils.isEmpty(context.getRequestedRelationAttributes())) {
                            continue;
                        }
                    }
                    attrName = cleanName;
                }

                Object value = resolveAttribute(vertexId, attr, context);
                if (value != null) header.setAttribute(attrName, value);
            }
        }

        return header;
    }

    /**
     * Resolve attribute value from context. Handles both primitive (vertex property)
     * and relationship (edge-backed) attributes.
     */
    private Object resolveAttribute(String vertexId, AtlasAttribute attr,
                                    SearchEnrichmentContext context) {
        if (attr == null) return null;

        String edgeLabel = attr.getRelationshipEdgeLabel();
        if (edgeLabel != null) {
            return resolveRelationshipAttribute(vertexId, attr, context);
        }

        // Primitive/enum: read from vertex properties
        return context.getVertexProperty(vertexId, attr.getVertexPropertyName(), Object.class);
    }

    /**
     * Build relationship attribute value from cached edge data.
     */
    private Object resolveRelationshipAttribute(String vertexId, AtlasAttribute attr,
                                                 SearchEnrichmentContext context) {
        String edgeLabel = attr.getRelationshipEdgeLabel();
        List<EdgeVertexReference> edges = context.getEdgesForLabel(vertexId, edgeLabel);

        if (edges.isEmpty()) return null;

        AtlasRelationshipEdgeDirection direction = attr.getRelationshipEdgeDirection();
        List<AtlasObjectId> refs = new ArrayList<>();

        for (EdgeVertexReference edge : edges) {
            String refVertexId = edge.getReferencedVertexId();
            String refGuid = context.getVertexProperty(refVertexId, GUID_PROPERTY_KEY, String.class);
            String refTypeName = context.getVertexProperty(refVertexId, TYPE_NAME_PROPERTY_KEY, String.class);

            if (refGuid == null) continue;

            AtlasObjectId objectId = new AtlasObjectId(refGuid, refTypeName);

            String refQN = context.getVertexProperty(refVertexId, QUALIFIED_NAME, String.class);
            if (refQN != null) {
                objectId.getUniqueAttributes().put(QUALIFIED_NAME, refQN);
            }

            // Include relationship attributes if requested
            if (CollectionUtils.isNotEmpty(context.getRequestedRelationAttributes())) {
                Map<String, Object> relAttrs = new HashMap<>();
                for (String relAttrName : context.getRequestedRelationAttributes()) {
                    Object val = edge.getValueMap().get(relAttrName);
                    if (val != null) relAttrs.put(relAttrName, val);
                }
                if (!relAttrs.isEmpty()) {
                    objectId.setRelationshipAttributes(relAttrs);
                }
            }

            refs.add(objectId);
        }

        if (attr.getAttributeDef().getCardinality() == AtlasAttributeDef.Cardinality.SINGLE) {
            return refs.isEmpty() ? null : refs.get(0);
        }
        return refs;
    }
}
```

### 7.12 IndexSearchResultRenderer — Orchestrator

```java
/**
 * Orchestrates the full enrichment pipeline.
 *
 * Called from EntityDiscoveryService.directIndexSearch() behind feature flag.
 * Replaces the IndexSearch-specific portion of prepareSearchResult().
 */
@Component
public class IndexSearchResultRenderer {

    private final VertexIdExtractor vertexIdExtractor;
    private final List<EnrichmentStage> pipeline;
    private final HeaderAssembler headerAssembler;

    @Inject
    public IndexSearchResultRenderer(AtlasGraph graph, AtlasTypeRegistry typeRegistry, TagDAO tagDAO) {

        // Only runs on ZeroGraph (CassandraGraph) — JanusGraph uses existing prepareSearchResult()
        this.vertexIdExtractor = new DirectVertexIdExtractor();

        TypeAwareEdgeLabelResolver labelResolver = new TypeAwareEdgeLabelResolver(typeRegistry);

        this.pipeline = List.of(
            new VertexBulkLoader(graph),
            new TypeAwareEdgeFetcher(graph, labelResolver),
            new ReferenceVertexLoader(graph),
            new SmartClassificationLoader(tagDAO),
            new TermAssignmentLoader()
        );

        this.headerAssembler = new HeaderAssembler(typeRegistry);
    }

    public void render(AtlasSearchResult result,
                       DirectIndexQueryResult indexQueryResult,
                       Set<String> resultAttributes,
                       SearchParams searchParams) throws AtlasBaseException {

        MetricRecorder overallMetric = RequestContext.get().startMetricRecord("indexSearchResultRenderer.render");
        try {
            // ── Stage 0: Extract vertex IDs (0 CQL on ZeroGraph) ──
            List<ESHitResult> esHits = toESHitResults(indexQueryResult);
            List<String> vertexIds = vertexIdExtractor.extractVertexIds(esHits);

            if (CollectionUtils.isEmpty(vertexIds)) return;

            // ── Build context ──
            RequestContext reqCtx = RequestContext.get();
            SearchEnrichmentContext context = new SearchEnrichmentContext(
                esHits, vertexIds, resultAttributes,
                reqCtx.getRelationAttrsForSearch(),
                searchParams,
                reqCtx.includeClassifications(),
                reqCtx.isIncludeClassificationNames(),
                reqCtx.includeMeanings()
            );

            // ── Execute pipeline ──
            for (EnrichmentStage stage : pipeline) {
                MetricRecorder stageMetric = reqCtx.startMetricRecord("indexSearch.stage." + stage.name());
                try {
                    stage.enrich(context);
                } finally {
                    reqCtx.endMetricRecord(stageMetric);
                }
            }

            // ── Assemble headers from enriched context ──
            for (int i = 0; i < vertexIds.size(); i++) {
                String vertexId = vertexIds.get(i);
                ESHitResult esHit = esHits.get(i);

                AtlasEntityHeader header = headerAssembler.assemble(vertexId, context);

                // Score
                if (searchParams.getShowSearchScore()) {
                    result.addEntityScore(header.getGuid(), esHit.getScore());
                }

                // Highlights & sort
                if (searchParams.getShowSearchMetadata()) {
                    result.addHighlights(header.getGuid(), esHit.getHighlights());
                    result.addSort(header.getGuid(), esHit.getSort());
                } else if (searchParams.getShowHighlights()) {
                    result.addHighlights(header.getGuid(), esHit.getHighlights());
                }

                // Collapse results
                handleCollapse(header, esHit, context, resultAttributes, searchParams, result);

                result.addEntity(header);
            }

        } finally {
            RequestContext.get().endMetricRecord(overallMetric);
        }
    }

    /** Convert DirectIndexQueryResult's iterator to List<ESHitResult>.
     *  Only handles CassandraIndexQuery.ResultImplDirect — this pipeline only runs on ZeroGraph. */
    private List<ESHitResult> toESHitResults(DirectIndexQueryResult indexQueryResult) {
        Iterator<Result> iterator = indexQueryResult.getIterator();
        List<ESHitResult> hits = new ArrayList<>();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            if (result instanceof CassandraIndexQuery.ResultImplDirect) {
                hits.add(new ESHitResult(((CassandraIndexQuery.ResultImplDirect) result).getRawHit()));
            }
        }
        return hits;
    }

    /** Handle collapse results (inner_hits). Recursive pipeline call for collapsed groups. */
    private void handleCollapse(AtlasEntityHeader header, ESHitResult esHit,
                                SearchEnrichmentContext parentContext,
                                Set<String> resultAttributes,
                                SearchParams searchParams,
                                AtlasSearchResult parentResult) throws AtlasBaseException {
        Map<String, LinkedHashMap> innerHitsMap = esHit.getInnerHits();
        if (innerHitsMap == null || innerHitsMap.isEmpty()) return;

        Map<String, AtlasSearchResult> collapse = new HashMap<>();
        for (String collapseKey : innerHitsMap.keySet()) {
            AtlasSearchResult collapseResult = new AtlasSearchResult();
            collapseResult.setSearchParameters(searchParams);

            // Parse inner hits into DirectIndexQueryResult and render recursively
            DirectIndexQueryResult collapseQueryResult = parseInnerHits(innerHitsMap.get(collapseKey));

            Set<String> collapseAttrs = searchParams.getCollapseAttributes() != null
                ? new HashSet<>(searchParams.getCollapseAttributes())
                : resultAttributes;

            render(collapseResult, collapseQueryResult, collapseAttrs, searchParams);

            collapseResult.setSearchParameters(null);
            collapse.put(collapseKey, collapseResult);
        }

        if (!collapse.isEmpty()) {
            header.setCollapse(collapse);
        }
    }
}
```

### 7.13 Integration Point — EntityDiscoveryService

```java
// In EntityDiscoveryService.directIndexSearch(), line 368:
// Replace:
//   prepareSearchResult(ret, indexQueryResult, resultAttributes, true, useVertexEdgeBulkFetching);
// With:

if (AtlasConfiguration.ATLAS_INDEXSEARCH_USE_OPTIMISED_PIPELINE.getBoolean()) {
    indexSearchResultRenderer.render(ret, indexQueryResult, resultAttributes, searchParams);
} else {
    prepareSearchResult(ret, indexQueryResult, resultAttributes, true, useVertexEdgeBulkFetching);
}
```

### 7.14 New EdgeRepository Method

```java
/**
 * NEW method: Per-vertex-type edge fetch.
 * Each vertex gets its own label set (not a flat union).
 *
 * @param perVertexLabels vertexId → { edgeLabel → direction }
 * @param limitPerLabel   max edges per (vertex, label, direction)
 * @return vertexId → list of edge data maps
 */
public Map<String, List<CassandraEdge>> getEdgesForVerticesPerType(
        Map<String, Map<String, AtlasEdgeDirection>> perVertexLabels,
        CassandraGraph graph, int limitPerLabel) {

    Map<String, List<CompletionStage<AsyncResultSet>>> outFutures = new LinkedHashMap<>();
    Map<String, List<CompletionStage<AsyncResultSet>>> inFutures = new LinkedHashMap<>();

    for (Map.Entry<String, Map<String, AtlasEdgeDirection>> vertexEntry : perVertexLabels.entrySet()) {
        String vertexId = vertexEntry.getKey();
        Map<String, AtlasEdgeDirection> labels = vertexEntry.getValue();

        List<CompletionStage<AsyncResultSet>> vOutFutures = new ArrayList<>();
        List<CompletionStage<AsyncResultSet>> vInFutures = new ArrayList<>();

        for (Map.Entry<String, AtlasEdgeDirection> labelEntry : labels.entrySet()) {
            String label = labelEntry.getKey();
            AtlasEdgeDirection dir = labelEntry.getValue();

            if (dir == AtlasEdgeDirection.OUT || dir == AtlasEdgeDirection.BOTH) {
                BoundStatement stmt = limitPerLabel > 0
                    ? selectEdgesOutByLabelLimitStmt.bind(vertexId, label, limitPerLabel)
                    : selectEdgesOutByLabelStmt.bind(vertexId, label);
                vOutFutures.add(session.executeAsync(stmt));
            }

            if (dir == AtlasEdgeDirection.IN || dir == AtlasEdgeDirection.BOTH) {
                BoundStatement stmt = limitPerLabel > 0
                    ? selectEdgesInByLabelLimitStmt.bind(vertexId, label, limitPerLabel)
                    : selectEdgesInByLabelStmt.bind(vertexId, label);
                vInFutures.add(session.executeAsync(stmt));
            }
        }

        outFutures.put(vertexId, vOutFutures);
        inFutures.put(vertexId, vInFutures);
    }

    // Collect results
    Map<String, List<CassandraEdge>> results = new LinkedHashMap<>();
    for (String vertexId : perVertexLabels.keySet()) {
        List<CassandraEdge> edges = new ArrayList<>();

        // OUT edges
        for (CompletionStage<AsyncResultSet> future : outFutures.getOrDefault(vertexId, Collections.emptyList())) {
            AsyncResultSet rs = future.toCompletableFuture().join();
            collectOutEdgePages(rs, vertexId, edges, graph);
        }

        // IN edges
        for (CompletionStage<AsyncResultSet> future : inFutures.getOrDefault(vertexId, Collections.emptyList())) {
            AsyncResultSet rs = future.toCompletableFuture().join();
            collectInEdgePages(rs, vertexId, edges, graph);
        }

        results.put(vertexId, edges);
    }

    return results;
}
```

---

## 9. Implementation Phases

### Phase 1: Foundation — Interfaces & Context (3-4 days, zero risk)

Additive only. No existing code changes.

- `SearchEnrichmentContext`, `EnrichmentStage`, `ESHitResult`
- `VertexIdExtractor` + `DirectVertexIdExtractor` (ZeroGraph only, no JanusGraph changes)
- Feature flag in `AtlasConfiguration`
- Unit tests for all new classes

### Phase 2: Core — TypeAwareEdgeLabelResolver + Per-Type Edge Fetch (4-5 days)

The main CQL reduction. One new method in EdgeRepository.

- `TypeAwareEdgeLabelResolver` — per-type, direction-aware label resolution
- `EdgeRepository.getEdgesForVerticesPerType()` — new method
- `CassandraGraph` — delegation method
- Comprehensive unit tests with mock type registry

### Phase 3: Full Pipeline Assembly (5-6 days)

All stages + orchestrator, behind feature flag.

- `VertexBulkLoader`, `TypeAwareEdgeFetcher`, `ReferenceVertexLoader`
- `SmartClassificationLoader`, `TermAssignmentLoader`, `HeaderAssembler`
- `IndexSearchResultRenderer` orchestrator
- Feature-flag integration in `EntityDiscoveryService`
- Need to expose raw ES hit from `ResultImplDirect` (add `getRawHit()` accessor)

### Phase 4: Collapse + Edge Cases (2-3 days)

- Collapse result handling (recursive pipeline for inner_hits)
- Null vertices, missing types, ABAC interaction
- `getClassificationNames` path (when only names requested, not full objects)
- STRUCT attribute handling (rare but must work)
- Metrics parity with existing code

### Phase 5: Validation + Rollout (3-4 days)

- A/B comparison mode (run both pipelines, diff responses, log CQL counts)
- HAR replay against staging
- Per-tenant production rollout via `DynamicConfigStore`
- Monitor: CQL per search, latency, response correctness

| Phase | Effort | What It Fixes | Risk |
|-------|--------|---------------|------|
| 1 | 3-4 days | Foundation setup | None |
| 2 | 4-5 days | Flat label union (main CQL reduction) | Medium |
| 3 | 5-6 days | All remaining hotspots (terms, cls, vertex load) | Medium |
| 4 | 2-3 days | Edge cases and completeness | Low |
| 5 | 3-4 days | Production validation | Low |
| **Total** | **~18-22 days** | | |

---

## 10. Migration & Rollout

**Feature flag:** `atlas.indexsearch.use.optimised.pipeline` (default: `false`)

**Rollout:**
1. Deploy with flag OFF
2. Enable on internal test tenant → validate
3. Enable on 1 low-traffic production tenant → monitor 24h
4. Enable on 5 medium-traffic tenants → monitor 48h
5. Enable globally
6. Remove old code path (future PR)

**Rollback:** Flag OFF → instant revert. No data migration, no schema changes.

---

## 11. Validation Strategy

**A/B comparison mode:** Run both old and new pipelines for a configurable % of requests. Compare response bodies (ignoring order). Log any differences + CQL counts for both paths. Return the old result (safe).

**Correctness checks:**
- Entity headers contain same attributes, same values
- Classifications match
- Term assignments match
- Relationship attributes match
- Collapse results match

**Performance checks:**
- Per-stage CQL count via MetricRecorder
- Overall latency p50/p99
- Empty result ratio (target: < 10%, currently ~90%)

---

## 12. Risk Analysis

| Risk | Impact | Mitigation |
|------|--------|------------|
| Response body differs from old pipeline | High | A/B comparison mode, HAR replay |
| Missing edge label for some type | Medium | TypeAwareEdgeLabelResolver uses same type registry as current code. Comprehensive unit tests. |
| STRUCT attributes need per-entity CQL | Low | Rare in IndexSearch. Can fall back to vertex property for struct attrs. |
| Collapse results break | Medium | Dedicated Phase 4 with recursive pipeline |
| getRawHit() accessor on ResultImplDirect | Low | Trivial change, no logic change |
| EntityGraphRetriever breaks | None | We don't modify it — extraction only |

---

## Appendix: File Inventory

### New Files (`repository/src/main/java/org/apache/atlas/discovery/searchpipeline/`)

| File | Description |
|------|-------------|
| `SearchEnrichmentContext.java` | Pipeline shared state |
| `EnrichmentStage.java` | Stage interface |
| `ESHitResult.java` | Lightweight ES hit wrapper |
| `VertexIdExtractor.java` | Strategy interface |
| `DirectVertexIdExtractor.java` | ZeroGraph: 0-CQL ID extraction |
| `TypeAwareEdgeLabelResolver.java` | Per-type, direction-aware label resolution |
| `IndexSearchResultRenderer.java` | Pipeline orchestrator |
| `stages/VertexBulkLoader.java` | Stage 1: async bulk vertex load |
| `stages/TypeAwareEdgeFetcher.java` | Stage 2: per-type edge fetch |
| `stages/ReferenceVertexLoader.java` | Stage 3: reference vertex load (fresh, no cross-request cache) |
| `stages/SmartClassificationLoader.java` | Stage 4: pre-filtered tag load |
| `stages/TermAssignmentLoader.java` | Stage 5: batch term assembly |
| `stages/HeaderAssembler.java` | Stage 6: entity header construction |

### Modified Files

| File | Change |
|------|--------|
| `EdgeRepository.java` | Add `getEdgesForVerticesPerType()` |
| `CassandraGraph.java` | Add delegation method |
| `CassandraIndexQuery.java` | Add `getRawHit()` on `ResultImplDirect` |
| `EntityDiscoveryService.java` | Feature-flag gate to new renderer |
| `AtlasConfiguration.java` | Add config key |

### Unchanged

| File | Why |
|------|-----|
| `EntityGraphRetriever.java` | Non-IndexSearch paths. We extract, not modify. |
| `DiscoveryREST.java` | REST layer unchanged |
| `TagDAOCassandraImpl.java` | API unchanged |
| `VertexEdgePropertiesCache.java` | Reused inside context |
