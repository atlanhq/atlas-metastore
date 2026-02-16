package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

public class VertexRepository {

    private static final Logger LOG = LoggerFactory.getLogger(VertexRepository.class);

    private final CqlSession session;
    private PreparedStatement insertVertexStmt;
    private PreparedStatement selectVertexStmt;
    private PreparedStatement deleteVertexStmt;
    private PreparedStatement selectVerticesByPropertyStmt;

    public VertexRepository(CqlSession session) {
        this.session = session;
        prepareStatements();
    }

    private void prepareStatements() {
        insertVertexStmt = session.prepare(
            "INSERT INTO vertices (vertex_id, properties, vertex_label, type_name, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        );

        selectVertexStmt = session.prepare(
            "SELECT vertex_id, properties, vertex_label, type_name, state, created_at, modified_at " +
            "FROM vertices WHERE vertex_id = ?"
        );

        deleteVertexStmt = session.prepare(
            "DELETE FROM vertices WHERE vertex_id = ?"
        );
    }

    public void insertVertex(CassandraVertex vertex) {
        Map<String, Object> props = vertex.getProperties();
        String typeName = props.containsKey("__typeName") ? String.valueOf(props.get("__typeName")) : null;
        String state    = props.containsKey("__state") ? String.valueOf(props.get("__state")) : "ACTIVE";
        Instant now     = Instant.now();

        session.execute(insertVertexStmt.bind(
            vertex.getIdString(),
            AtlasType.toJson(props),
            vertex.getVertexLabel(),
            typeName,
            state,
            now,
            now
        ));
    }

    public void updateVertex(CassandraVertex vertex) {
        // For simplicity, update = full overwrite
        insertVertex(vertex);
    }

    public CassandraVertex getVertex(String vertexId, CassandraGraph graph) {
        ResultSet rs = session.execute(selectVertexStmt.bind(vertexId));
        Row row = rs.one();

        if (row == null) {
            return null;
        }

        return rowToVertex(row, graph);
    }

    public List<CassandraVertex> getVertices(Collection<String> vertexIds, CassandraGraph graph) {
        List<CassandraVertex> results = new ArrayList<>();
        // Use async or batch for performance in production; simple loop for correctness
        for (String vertexId : vertexIds) {
            CassandraVertex v = getVertex(vertexId, graph);
            if (v != null) {
                results.add(v);
            }
        }
        return results;
    }

    public void deleteVertex(String vertexId) {
        session.execute(deleteVertexStmt.bind(vertexId));
    }

    public void batchInsertVertices(List<CassandraVertex> vertices) {
        if (vertices.isEmpty()) {
            return;
        }

        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.LOGGED);

        for (CassandraVertex vertex : vertices) {
            Map<String, Object> props = vertex.getProperties();
            String typeName = props.containsKey("__typeName") ? String.valueOf(props.get("__typeName")) : null;
            String state    = props.containsKey("__state") ? String.valueOf(props.get("__state")) : "ACTIVE";
            Instant now     = Instant.now();

            batchBuilder.addStatement(insertVertexStmt.bind(
                vertex.getIdString(),
                AtlasType.toJson(props),
                vertex.getVertexLabel(),
                typeName,
                state,
                now,
                now
            ));
        }

        session.execute(batchBuilder.build());
    }

    @SuppressWarnings("unchecked")
    private CassandraVertex rowToVertex(Row row, CassandraGraph graph) {
        String vertexId    = row.getString("vertex_id");
        String propsJson   = row.getString("properties");
        String vertexLabel = row.getString("vertex_label");

        Map<String, Object> props = new LinkedHashMap<>();
        if (propsJson != null && !propsJson.isEmpty()) {
            props = AtlasType.fromJson(propsJson, Map.class);
            if (props == null) {
                props = new LinkedHashMap<>();
            }
        }

        CassandraVertex vertex = new CassandraVertex(vertexId, vertexLabel, props, graph);
        return vertex;
    }
}
