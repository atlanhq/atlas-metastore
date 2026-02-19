# Cassandra Schema Design, Query Patterns & IndexSearch Optimizations

## Table Design

### `vertices` — Entity/Vertex Storage

```sql
CREATE TABLE vertices (
  vertex_id    text PRIMARY KEY,  -- Random UUID (assigned at vertex creation)
  properties   text,              -- JSON blob of ALL vertex properties
  vertex_label text,              -- e.g., 'vertex' (not heavily used)
  type_name    text,              -- __typeName value (denormalized for quick filtering)
  state        text,              -- ACTIVE / DELETED
  created_at   timestamp,
  modified_at  timestamp
);
```

**Key point:** `properties` is a JSON blob containing every Atlas property on the vertex,
including `__guid`, `qualifiedName`, `__typeName`, `name`, `description`, `ownerUsers`,
`certificateStatus`, and any type-specific attributes. This is a schemaless design — no
Cassandra column per attribute.

**Query patterns:**
- Lookup by vertex_id: `SELECT * FROM vertices WHERE vertex_id = ?` (O(1))
- Bulk lookup: N async queries fired in parallel via `VertexRepository.getVerticesAsync()`

### `edges_out` / `edges_in` — Adjacency Lists

```sql
CREATE TABLE edges_out (
  out_vertex_id text,
  edge_label    text,    -- e.g., '__Table.columns', 'r:AtlasGlossarySemanticAssignment'
  edge_id       text,
  in_vertex_id  text,
  properties    text,    -- JSON blob of edge properties
  state         text,
  created_at    timestamp,
  modified_at   timestamp,
  PRIMARY KEY ((out_vertex_id), edge_label, edge_id)
) WITH CLUSTERING ORDER BY (edge_label ASC, edge_id ASC);

-- Mirror table for reverse traversals
CREATE TABLE edges_in (
  in_vertex_id  text,
  edge_label    text,
  edge_id       text,
  out_vertex_id text,
  properties    text,
  state         text,
  created_at    timestamp,
  modified_at   timestamp,
  PRIMARY KEY ((in_vertex_id), edge_label, edge_id)
) WITH CLUSTERING ORDER BY (edge_label ASC, edge_id ASC);
```

**Key design decision:** Partition key is the vertex_id, clustering key starts with edge_label.
This allows efficient queries by vertex + label (a single partition slice) without full partition scans.

**Query patterns:**
- All edges for a vertex: `SELECT * FROM edges_out WHERE out_vertex_id = ?`
- Edges by label: `SELECT * FROM edges_out WHERE out_vertex_id = ? AND edge_label = ?`
- Edges by label with LIMIT: `SELECT * FROM edges_out WHERE out_vertex_id = ? AND edge_label = ? LIMIT ?`

### `edges_by_id` — Edge Lookup by ID

```sql
CREATE TABLE edges_by_id (
  edge_id       text PRIMARY KEY,
  out_vertex_id text,
  in_vertex_id  text,
  edge_label    text,
  properties    text,
  state         text,
  created_at    timestamp,
  modified_at   timestamp
);
```

### `vertex_index` — 1:1 Unique Composite Indexes

```sql
CREATE TABLE vertex_index (
  index_name  text,   -- e.g., '__guid_idx', 'qn_type_idx'
  index_value text,   -- the lookup value
  vertex_id   text,   -- points to vertices table
  PRIMARY KEY ((index_name, index_value))
);
```

**Indexes maintained:**
| index_name | index_value | Purpose |
|------------|-------------|---------|
| `__guid_idx` | `<guid>` | Lookup entity by GUID |
| `qn_type_idx` | `<qualifiedName>:<typeName>` | Lookup entity by QN + type (uniqueness) |
| `type_typename_idx` | `<vertexType>:<typeDefName>` | TypeDef vertex lookup |

### `vertex_property_index` — 1:N Property Indexes

```sql
CREATE TABLE vertex_property_index (
  index_name  text,
  index_value text,
  vertex_id   text,
  PRIMARY KEY ((index_name, index_value), vertex_id)
);
```

Used for lookups that can return multiple vertices (e.g., all TypeDefs of a given category).

### `schema_registry` — Property Key Metadata

```sql
CREATE TABLE schema_registry (
  property_name  text PRIMARY KEY,
  property_class text,     -- Java class name (String, Long, etc.)
  cardinality    text,     -- SINGLE, SET, LIST
  created_at     timestamp
);
```

**Note:** This table stores property key metadata for AtlasGraphManagement. It does NOT
store Atlas TypeDef information. TypeDefs are stored as vertices in the `vertices` table
and cached in-memory by `AtlasTypeRegistry`.

---

## IndexSearch: Attributes vs. RelationAttributes

### How the UI Search Payload Works

The UI sends an indexsearch request with two attribute lists:

```json
{
  "dsl": { ... },
  "attributes": [
    "name", "qualifiedName", "description", "ownerUsers",
    "schemas", "table", "view", "meanings", "readme", "anchor",
    "columnCount", "rowCount", ...
    // 150+ attributes covering ALL possible entity types
  ],
  "relationAttributes": [
    "displayText", "name", "qualifiedName", "connectorName", ...
    // ~22 properties to return FOR EACH referenced entity
  ]
}
```

| Field | Meaning | Used For |
|-------|---------|----------|
| `attributes` | Names of attributes to include in search results. Can be regular (vertex property) or relationship (edge-based). Covers ALL possible entity types because the UI doesn't know result types beforehand. | Determines WHAT to return per entity |
| `relationAttributes` | Property names to include when serializing a REFERENCED entity (the entity on the other end of a relationship edge). | Determines what properties of the related entity to include |

### The Problem: Attribute-to-Edge-Label Explosion

When the `attributes` list includes a name like `meanings`, `readme`, `schemas`, `table`, `columns`,
etc., and that name is a **relationship attribute** on the returned entity type, Atlas needs to
fetch the corresponding edges from Cassandra to resolve the relationship.

For high-cardinality relationships this is catastrophic:
- A Table entity with 5,000 columns has 5,000 `__Table.columns` edges
- If `columns` is in the attributes list and Table entities are in the results, all 5,000 edges
  get fetched from Cassandra — even though the UI only needs a few

### Optimizations Applied

#### 1. Per-Label Cassandra Queries (instead of full partition scans)

**Before:** `SELECT * FROM edges_out WHERE out_vertex_id = ?` → returns ALL edges (all labels)
**After:** `SELECT * FROM edges_out WHERE out_vertex_id = ? AND edge_label = ?` → returns only edges for the specific relationship

This is a partition slice query using the clustering key. Multiple (vertex, label, direction)
queries are fired concurrently using `session.executeAsync()`.

#### 2. Per-Type Edge Label Grouping

**Before:** Collect ALL edge labels from ALL entity types, query ALL vertices for ALL labels.
A Table vertex would get queried for Column-specific edge labels like `__Table.columns` (returning 5,000 edges).

**After:** `collectEdgeLabelsPerType()` groups vertices by their `__typeName` and only collects
edge labels that are valid relationship attributes for THAT type:

```
Table (1 vertex)     → [__Asset.readme, __Schema.tables, __Asset.dataContractLatest, r:AtlasGlossarySemanticAssignment]
Column (18 vertices) → [__Table.columns, __View.columns, __Column.foreignKeyTo, ...]
```

Table vertices are NOT queried for `__Table.columns` because `columns` is not a Table relationship
attribute — it's a Table relationship from Column's perspective (Column has a `table` relationship
pointing to Table, which uses edge label `__Table.columns` with Column as IN-vertex).

#### 3. LIMIT Pushed to Cassandra

Even with per-type grouping, a Column vertex queried for `__Table.columns` IN edges would return
all sibling columns. The LIMIT is pushed down to Cassandra:

```sql
SELECT * FROM edges_out WHERE out_vertex_id = ? AND edge_label = ? LIMIT ?
SELECT * FROM edges_in  WHERE in_vertex_id = ? AND edge_label = ? LIMIT ?
```

The limit value is `MIN_EDGES_SUPER_VERTEX` (default: 100) for product indexsearch requests.
This prevents Cassandra from transferring thousands of rows when only ~100 are needed.

**Result:** Full UI payload dropped from **25+ seconds to ~130ms** (190x speedup).

### Attribute Validation via TypeDef Registry

The `collectEdgeLabelsPerType()` method validates each attribute name against the entity type's
relationship attributes using `AtlasTypeRegistry`:

```java
for (String attribute : attributes) {
    // Only proceeds if this attribute IS a relationship attribute on this entity type
    if (!entityType.getRelationshipAttributes().containsKey(attribute)) {
        return; // silently skipped — not a relationship attribute for this type
    }
    AtlasAttribute atlasAttribute = entityType.getRelationshipAttribute(attribute, null);
    edgeLabels.add(atlasAttribute.getRelationshipEdgeLabel());
}
```

If the UI sends an attribute like `xyz` that doesn't exist in ANY entity type's typedef:
- For **regular attributes**: `entityType.getAttribute("xyz")` returns null, then
  `entityType.getRelationshipAttribute("xyz", null)` also returns null → attribute is silently
  skipped in `mapVertexToAtlasEntityHeader` (no error, no edge fetch)
- For **edge label collection**: `entityType.getRelationshipAttributes().containsKey("xyz")`
  returns false → no edge label is added → no Cassandra query fired

**Current behavior is correct** — unknown attributes are safely ignored. However, the attributes
list is still iterated in full (150+ items per entity type per vertex). A future optimization
could pre-filter the attributes list to only include names that exist in at least one loaded typedef.

### Known Limitations

1. **No per-entity-type attribute pre-filtering at the API level**: The UI sends ALL possible
   attributes because it doesn't know result types beforehand. This means we iterate 150+ strings
   per entity type even though most are irrelevant. The typedef check is cheap (HashMap lookup)
   but the iteration is unnecessary.

2. **LIMIT is per-(vertex, label, direction)**: If there are 20 Column vertices each needing
   `__Table.columns` edges, that's 20 queries to Cassandra. Each returns at most LIMIT rows.
   For 20 vertices x 10 labels x 2 directions = 400 concurrent queries. This is fine because
   each is a tiny partition slice, but at extreme scale (1000+ vertices) it could become a
   problem.

3. **Edge properties are JSON blobs**: Each edge's properties are stored as a JSON string and
   deserialized on read. For edges with many properties, this adds CPU overhead during
   `parseProperties()`. Consider columnar edge properties if this becomes a bottleneck.

4. **Vertex properties are JSON blobs**: Similarly, all vertex properties are in a single JSON
   column. Updating a single property requires rewriting the entire JSON blob. This is fine for
   read-heavy workloads (indexsearch) but suboptimal for write-heavy mutation patterns.

5. **No server-side filtering of DELETED edges**: The `state` column is checked in Java after
   reading from Cassandra. A Cassandra materialized view or a separate table for active-only
   edges could avoid reading deleted rows.

---

## Metric Breakdown (Full UI Payload — 20 entities, 150+ attributes, 22 relation attributes)

| Metric | Before All Optimizations | After All Optimizations |
|--------|-------------------------|------------------------|
| Total request | 41,700ms | **134ms** |
| `getConnectedRelationEdgesVertexBatching` | 41,374ms | **16ms** |
| `enrichVertexPropertiesByVertexIds` | ~41,400ms | **18ms** |
| `getVertexPropertiesValueMap` | ~1ms | **1ms** |
| `prepareSearchResult` | ~41,500ms | **121ms** |
| `mapVertexToAtlasEntityHeader` (per vertex) | N/A | **~4ms** |
