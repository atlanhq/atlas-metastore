package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletionStage;

public class EdgeRepository {

    private static final Logger LOG = LoggerFactory.getLogger(EdgeRepository.class);

    private final CqlSession session;
    private PreparedStatement insertEdgeOutStmt;
    private PreparedStatement insertEdgeInStmt;
    private PreparedStatement insertEdgeByIdStmt;
    private PreparedStatement selectEdgeByIdStmt;
    private PreparedStatement selectEdgesOutStmt;
    private PreparedStatement selectEdgesOutByLabelStmt;
    private PreparedStatement selectEdgesInStmt;
    private PreparedStatement selectEdgesInByLabelStmt;
    private PreparedStatement deleteEdgeOutStmt;
    private PreparedStatement deleteEdgeInStmt;
    private PreparedStatement deleteEdgeByIdStmt;

    public EdgeRepository(CqlSession session) {
        this.session = session;
        prepareStatements();
    }

    private void prepareStatements() {
        insertEdgeOutStmt = session.prepare(
            "INSERT INTO edges_out (out_vertex_id, edge_label, edge_id, in_vertex_id, properties, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        );

        insertEdgeInStmt = session.prepare(
            "INSERT INTO edges_in (in_vertex_id, edge_label, edge_id, out_vertex_id, properties, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        );

        insertEdgeByIdStmt = session.prepare(
            "INSERT INTO edges_by_id (edge_id, out_vertex_id, in_vertex_id, edge_label, properties, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        );

        selectEdgeByIdStmt = session.prepare(
            "SELECT edge_id, out_vertex_id, in_vertex_id, edge_label, properties, state " +
            "FROM edges_by_id WHERE edge_id = ?"
        );

        selectEdgesOutStmt = session.prepare(
            "SELECT edge_id, edge_label, in_vertex_id, properties, state " +
            "FROM edges_out WHERE out_vertex_id = ?"
        );

        selectEdgesOutByLabelStmt = session.prepare(
            "SELECT edge_id, edge_label, in_vertex_id, properties, state " +
            "FROM edges_out WHERE out_vertex_id = ? AND edge_label = ?"
        );

        selectEdgesInStmt = session.prepare(
            "SELECT edge_id, edge_label, out_vertex_id, properties, state " +
            "FROM edges_in WHERE in_vertex_id = ?"
        );

        selectEdgesInByLabelStmt = session.prepare(
            "SELECT edge_id, edge_label, out_vertex_id, properties, state " +
            "FROM edges_in WHERE in_vertex_id = ? AND edge_label = ?"
        );

        deleteEdgeOutStmt = session.prepare(
            "DELETE FROM edges_out WHERE out_vertex_id = ? AND edge_label = ? AND edge_id = ?"
        );

        deleteEdgeInStmt = session.prepare(
            "DELETE FROM edges_in WHERE in_vertex_id = ? AND edge_label = ? AND edge_id = ?"
        );

        deleteEdgeByIdStmt = session.prepare(
            "DELETE FROM edges_by_id WHERE edge_id = ?"
        );
    }

    public void insertEdge(CassandraEdge edge) {
        String propsJson = AtlasType.toJson(edge.getProperties());
        String state     = "ACTIVE";
        Instant now      = Instant.now();

        // Write to all three tables
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);

        batch.addStatement(insertEdgeOutStmt.bind(
            edge.getOutVertexId(), edge.getLabel(), edge.getIdString(),
            edge.getInVertexId(), propsJson, state, now, now
        ));

        batch.addStatement(insertEdgeInStmt.bind(
            edge.getInVertexId(), edge.getLabel(), edge.getIdString(),
            edge.getOutVertexId(), propsJson, state, now, now
        ));

        batch.addStatement(insertEdgeByIdStmt.bind(
            edge.getIdString(), edge.getOutVertexId(), edge.getInVertexId(),
            edge.getLabel(), propsJson, state, now, now
        ));

        session.execute(batch.build());
    }

    public CassandraEdge getEdge(String edgeId, CassandraGraph graph) {
        ResultSet rs = session.execute(selectEdgeByIdStmt.bind(edgeId));
        Row row = rs.one();

        if (row == null) {
            return null;
        }

        return rowToEdgeById(row, graph);
    }

    @SuppressWarnings("unchecked")
    public List<CassandraEdge> getEdgesForVertex(String vertexId, AtlasEdgeDirection direction,
                                                  String edgeLabel, CassandraGraph graph) {
        List<CassandraEdge> result = new ArrayList<>();

        if (direction == AtlasEdgeDirection.OUT || direction == AtlasEdgeDirection.BOTH) {
            ResultSet rs;
            if (edgeLabel != null) {
                rs = session.execute(selectEdgesOutByLabelStmt.bind(vertexId, edgeLabel));
            } else {
                rs = session.execute(selectEdgesOutStmt.bind(vertexId));
            }

            for (Row row : rs) {
                String state = row.getString("state");
                if (!"DELETED".equals(state)) {
                    String edgeId      = row.getString("edge_id");
                    String label       = row.getString("edge_label");
                    String inVertexId  = row.getString("in_vertex_id");
                    String propsJson   = row.getString("properties");
                    Map<String, Object> props = parseProperties(propsJson);
                    result.add(new CassandraEdge(edgeId, vertexId, inVertexId, label, props, graph));
                }
            }
        }

        if (direction == AtlasEdgeDirection.IN || direction == AtlasEdgeDirection.BOTH) {
            ResultSet rs;
            if (edgeLabel != null) {
                rs = session.execute(selectEdgesInByLabelStmt.bind(vertexId, edgeLabel));
            } else {
                rs = session.execute(selectEdgesInStmt.bind(vertexId));
            }

            for (Row row : rs) {
                String state = row.getString("state");
                if (!"DELETED".equals(state)) {
                    String edgeId       = row.getString("edge_id");
                    String label        = row.getString("edge_label");
                    String outVertexId  = row.getString("out_vertex_id");
                    String propsJson    = row.getString("properties");
                    Map<String, Object> props = parseProperties(propsJson);
                    result.add(new CassandraEdge(edgeId, outVertexId, vertexId, label, props, graph));
                }
            }
        }

        return result;
    }

    /**
     * Fetch all edges for multiple vertices concurrently using async Cassandra queries.
     * Queries both edges_out and edges_in for each vertex in parallel, reducing
     * 2N sequential round-trips to ~1 round-trip wall-clock time.
     *
     * @return Map of vertexId → list of all edges (both directions, all labels)
     */
    public Map<String, List<CassandraEdge>> getEdgesForVerticesAsync(
            Collection<String> vertexIds, AtlasEdgeDirection direction, CassandraGraph graph) {
        if (vertexIds == null || vertexIds.isEmpty()) {
            return Collections.emptyMap();
        }

        // Fire all queries concurrently
        Map<String, CompletionStage<AsyncResultSet>> outFutures = new LinkedHashMap<>();
        Map<String, CompletionStage<AsyncResultSet>> inFutures = new LinkedHashMap<>();

        for (String vertexId : vertexIds) {
            if (direction == AtlasEdgeDirection.OUT || direction == AtlasEdgeDirection.BOTH) {
                outFutures.put(vertexId, session.executeAsync(selectEdgesOutStmt.bind(vertexId)));
            }
            if (direction == AtlasEdgeDirection.IN || direction == AtlasEdgeDirection.BOTH) {
                inFutures.put(vertexId, session.executeAsync(selectEdgesInStmt.bind(vertexId)));
            }
        }

        // Collect results
        Map<String, List<CassandraEdge>> results = new LinkedHashMap<>();

        for (Map.Entry<String, CompletionStage<AsyncResultSet>> entry : outFutures.entrySet()) {
            String vertexId = entry.getKey();
            try {
                AsyncResultSet rs = entry.getValue().toCompletableFuture().join();
                List<CassandraEdge> edges = results.computeIfAbsent(vertexId, k -> new ArrayList<>());
                collectOutEdgePages(rs, vertexId, edges, graph);
            } catch (Exception e) {
                LOG.warn("Failed to fetch out-edges for vertex {}", vertexId, e);
            }
        }

        for (Map.Entry<String, CompletionStage<AsyncResultSet>> entry : inFutures.entrySet()) {
            String vertexId = entry.getKey();
            try {
                AsyncResultSet rs = entry.getValue().toCompletableFuture().join();
                List<CassandraEdge> edges = results.computeIfAbsent(vertexId, k -> new ArrayList<>());
                collectInEdgePages(rs, vertexId, edges, graph);
            } catch (Exception e) {
                LOG.warn("Failed to fetch in-edges for vertex {}", vertexId, e);
            }
        }

        return results;
    }

    private void collectOutEdgePages(AsyncResultSet rs, String vertexId,
                                      List<CassandraEdge> edges, CassandraGraph graph) {
        while (rs != null) {
            for (Row row : rs.currentPage()) {
                String state = row.getString("state");
                if (!"DELETED".equals(state)) {
                    String edgeId     = row.getString("edge_id");
                    String label      = row.getString("edge_label");
                    String inVertexId = row.getString("in_vertex_id");
                    String propsJson  = row.getString("properties");
                    Map<String, Object> props = parseProperties(propsJson);
                    edges.add(new CassandraEdge(edgeId, vertexId, inVertexId, label, props, graph));
                }
            }
            if (rs.hasMorePages()) {
                rs = rs.fetchNextPage().toCompletableFuture().join();
            } else {
                break;
            }
        }
    }

    private void collectInEdgePages(AsyncResultSet rs, String vertexId,
                                     List<CassandraEdge> edges, CassandraGraph graph) {
        while (rs != null) {
            for (Row row : rs.currentPage()) {
                String state = row.getString("state");
                if (!"DELETED".equals(state)) {
                    String edgeId      = row.getString("edge_id");
                    String label       = row.getString("edge_label");
                    String outVertexId = row.getString("out_vertex_id");
                    String propsJson   = row.getString("properties");
                    Map<String, Object> props = parseProperties(propsJson);
                    edges.add(new CassandraEdge(edgeId, outVertexId, vertexId, label, props, graph));
                }
            }
            if (rs.hasMorePages()) {
                rs = rs.fetchNextPage().toCompletableFuture().join();
            } else {
                break;
            }
        }
    }

    public void deleteEdge(CassandraEdge edge) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);

        batch.addStatement(deleteEdgeOutStmt.bind(
            edge.getOutVertexId(), edge.getLabel(), edge.getIdString()
        ));

        batch.addStatement(deleteEdgeInStmt.bind(
            edge.getInVertexId(), edge.getLabel(), edge.getIdString()
        ));

        batch.addStatement(deleteEdgeByIdStmt.bind(edge.getIdString()));

        session.execute(batch.build());
    }

    public void deleteEdgesForVertex(String vertexId, CassandraGraph graph) {
        // Get all edges and delete them
        List<CassandraEdge> allEdges = getEdgesForVertex(vertexId, AtlasEdgeDirection.BOTH, null, graph);
        for (CassandraEdge edge : allEdges) {
            deleteEdge(edge);
        }
    }

    public void batchInsertEdges(List<CassandraEdge> edges) {
        if (edges.isEmpty()) {
            return;
        }

        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
        Instant now = Instant.now();

        for (CassandraEdge edge : edges) {
            String propsJson = AtlasType.toJson(edge.getProperties());
            String state     = "ACTIVE";

            batch.addStatement(insertEdgeOutStmt.bind(
                edge.getOutVertexId(), edge.getLabel(), edge.getIdString(),
                edge.getInVertexId(), propsJson, state, now, now
            ));

            batch.addStatement(insertEdgeInStmt.bind(
                edge.getInVertexId(), edge.getLabel(), edge.getIdString(),
                edge.getOutVertexId(), propsJson, state, now, now
            ));

            batch.addStatement(insertEdgeByIdStmt.bind(
                edge.getIdString(), edge.getOutVertexId(), edge.getInVertexId(),
                edge.getLabel(), propsJson, state, now, now
            ));
        }

        session.execute(batch.build());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseProperties(String propsJson) {
        if (propsJson == null || propsJson.isEmpty()) {
            return new LinkedHashMap<>();
        }
        Map<String, Object> props = AtlasType.fromJson(propsJson, Map.class);
        return props != null ? props : new LinkedHashMap<>();
    }

    @SuppressWarnings("unchecked")
    private CassandraEdge rowToEdgeById(Row row, CassandraGraph graph) {
        String edgeId      = row.getString("edge_id");
        String outVertexId = row.getString("out_vertex_id");
        String inVertexId  = row.getString("in_vertex_id");
        String label       = row.getString("edge_label");
        String propsJson   = row.getString("properties");

        Map<String, Object> props = parseProperties(propsJson);
        return new CassandraEdge(edgeId, outVertexId, inVertexId, label, props, graph);
    }
}
