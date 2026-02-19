Feasibility: Lineage with Tabular Cassandra Graph

The lineage traversal only needs 4 types of Cassandra queries, all of which are efficient partition/clustering-key lookups on our tables:

┌──────────────────────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────────┐
│                  Traversal Step                  │                                  Cassandra Query                                   │
├──────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────┤
│ Find processes connected to a dataset (incoming) │ SELECT * FROM edges_in WHERE in_vertex_id=? AND edge_label=?                       │
├──────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────┤
│ Find datasets connected to a process (outgoing)  │ SELECT * FROM edges_out WHERE out_vertex_id=? AND edge_label=?                     │
├──────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────┤
│ Get vertex properties (for entity headers)       │ SELECT * FROM vertices WHERE vertex_id=?                                           │
├──────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────┤
│ Resolve GUID → vertex_id                         │ SELECT vertex_id FROM vertex_index WHERE index_name='__guid_idx' AND index_value=? │
└──────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────┘

The edge labels are only two: __Process.inputs and __Process.outputs. The direction logic maps cleanly:
- Downstream: edges_in(dataset, __Process.inputs) → process → edges_out(process, __Process.outputs) → downstream dataset
- Upstream: edges_in(dataset, __Process.outputs) → process → edges_out(process, __Process.inputs) → upstream dataset

CassandraLineageService — Direct Cassandra Lineage Traversal

File: repository/src/main/java/org/apache/atlas/discovery/CassandraLineageService.java

How it works

The class bypasses the entire graph abstraction layer (JanusGraph / CassandraGraph / AtlasVertex / AtlasEdge) and talks to Cassandra tables directly via CqlSession with 4 prepared statements:

┌────────────────────────┬──────────────┬───────────────────────────────────────────────────────┐
│       Statement        │    Table     │                        Purpose                        │
├────────────────────────┼──────────────┼───────────────────────────────────────────────────────┤
│ findVertexIdByGuidStmt │ vertex_index │ Resolve entity GUID → vertex_id                       │
├────────────────────────┼──────────────┼───────────────────────────────────────────────────────┤
│ getVertexStmt          │ vertices     │ Load entity properties (guid, typeName, name, etc.)   │
├────────────────────────┼──────────────┼───────────────────────────────────────────────────────┤
│ getEdgesInByLabelStmt  │ edges_in     │ Find processes connected to a dataset (by edge label) │
├────────────────────────┼──────────────┼───────────────────────────────────────────────────────┤
│ getEdgesOutByLabelStmt │ edges_out    │ Find datasets connected to a process (by edge label)  │
└────────────────────────┴──────────────┴───────────────────────────────────────────────────────┘

Traversal algorithm

Same DFS as EntityLineageService.traverseEdgesOnDemand():

Downstream from dataset D:
1. edges_in WHERE in_vertex_id = D AND edge_label = '__Process.inputs' → finds Process P
2. edges_out WHERE out_vertex_id = P AND edge_label = '__Process.outputs' → finds downstream Dataset D2
3. Recurse on D2

Upstream from dataset D:
1. edges_in WHERE in_vertex_id = D AND edge_label = '__Process.outputs' → finds Process P
2. edges_out WHERE out_vertex_id = P AND edge_label = '__Process.inputs' → finds upstream Dataset D0
3. Recurse on D0

Same response format

Returns AtlasLineageOnDemandInfo with:
- guidEntityMap — entity headers built directly from vertex properties JSON
- relations — LineageRelation(fromGuid, toGuid, relationshipGuid) with same direction semantics
- relationsOnDemand — per-entity pagination info (hasMoreInputs, hasMoreOutputs, hasUpstream, hasDownstream)
- Depth/traversalOrder/finishTime on entity headers

Features implemented

- Per-entity relation limits (inputRelationsLimit, outputRelationsLimit)
- Depth limiting with configurable default (3)
- Cycle detection via visitedVertices set
- Offset pagination (from parameter)
- hasUpstream/hasDownstream leaf detection
- Global entity limit (LINEAGE_MAX_NODE_COUNT)
- Per-request vertex caching (avoids re-reading vertices during recursion)
- Visited edge deduplication (same key format as EntityLineageService: guid1->guid2)

What's deferred

- Predicate evaluation (entityTraversalFilters / relationshipTraversalFilters) — most lineage requests don't use these. Can be added by evaluating against vertex/edge properties maps.

To wire it in

The EntityLineageService can delegate to this class when the Cassandra backend is active. The CqlSession can be obtained from CassandraSessionProvider. I can show you the wiring code whenever you're ready to integrate it.