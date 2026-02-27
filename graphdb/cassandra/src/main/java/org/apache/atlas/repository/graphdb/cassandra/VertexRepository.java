package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

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

    /**
     * Fetch multiple vertices concurrently using async Cassandra queries.
     * All queries are fired in parallel and results collected, reducing
     * wall-clock time from N sequential round-trips to ~1 round-trip.
     */
    public Map<String, CassandraVertex> getVerticesAsync(Collection<String> vertexIds, CassandraGraph graph) {
        if (vertexIds == null || vertexIds.isEmpty()) {
            return Collections.emptyMap();
        }

        // Fire all queries concurrently
        Map<String, CompletionStage<AsyncResultSet>> futures = new LinkedHashMap<>();
        for (String vertexId : vertexIds) {
            futures.put(vertexId, session.executeAsync(selectVertexStmt.bind(vertexId)));
        }

        // Collect results
        Map<String, CassandraVertex> results = new LinkedHashMap<>();
        int failedCount = 0;
        for (Map.Entry<String, CompletionStage<AsyncResultSet>> entry : futures.entrySet()) {
            try {
                AsyncResultSet rs = entry.getValue().toCompletableFuture().join();
                Row row = rs.one();
                if (row != null) {
                    CassandraVertex vertex = rowToVertex(row, graph);
                    results.put(vertex.getIdString(), vertex);
                }
            } catch (CompletionException e) {
                failedCount++;
                LOG.warn("Failed to fetch vertex {}", entry.getKey(), e);
            }
        }

        if (failedCount > 0) {
            LOG.warn("getVerticesAsync: {} of {} vertex fetches failed — results are partial",
                     failedCount, futures.size());
        }

        return results;
    }

    public List<CassandraVertex> getVertices(Collection<String> vertexIds, CassandraGraph graph) {
        return new ArrayList<>(getVerticesAsync(vertexIds, graph).values());
    }

    public void deleteVertex(String vertexId) {
        session.execute(deleteVertexStmt.bind(vertexId));
    }

    // 1 statement per vertex; keep batches under 100 to avoid Cassandra's
    // batch_size_fail_threshold (50KB default) for vertices with large property JSON.
    private static final int VERTEX_BATCH_LIMIT = 100;

    public void batchInsertVertices(List<CassandraVertex> vertices) {
        if (vertices.isEmpty()) {
            return;
        }

        for (int i = 0; i < vertices.size(); i += VERTEX_BATCH_LIMIT) {
            List<CassandraVertex> batch = vertices.subList(i, Math.min(i + VERTEX_BATCH_LIMIT, vertices.size()));
            BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.LOGGED);

            for (CassandraVertex vertex : batch) {
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
    }

    @SuppressWarnings("unchecked")
    private CassandraVertex rowToVertex(Row row, CassandraGraph graph) {
        String vertexId    = row.getString("vertex_id");
        String propsJson   = row.getString("properties");
        String vertexLabel = row.getString("vertex_label");

        Map<String, Object> props = new LinkedHashMap<>();
        if (propsJson != null && !propsJson.isEmpty()) {
            Map<String, Object> rawProps = AtlasType.fromJson(propsJson, Map.class);
            if (rawProps != null) {
                // Normalize property names: strip JanusGraph type-qualified prefixes.
                // Migrated data may have keys like "__type.Asset.certificateUpdatedAt"
                // or "Referenceable.qualifiedName" — Atlas expects just "certificateUpdatedAt"
                // and "qualifiedName".
                for (Map.Entry<String, Object> entry : rawProps.entrySet()) {
                    String key = entry.getKey();
                    if (key == null) {
                        LOG.warn("rowToVertex: null property key in vertex_id={}, skipping", vertexId);
                        continue;
                    }
                    props.put(normalizePropertyName(key), entry.getValue());
                }
            } else {
                LOG.warn("rowToVertex: AtlasType.fromJson returned null for vertex_id={}. propsJson length={}",
                         vertexId, propsJson.length());
            }
        } else {
            LOG.warn("rowToVertex: empty/null properties JSON for vertex_id={}", vertexId);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("rowToVertex: vertex_id={}, label={}, propCount={}, keys={}",
                      vertexId, vertexLabel, props.size(), props.keySet());
        }

        CassandraVertex vertex = new CassandraVertex(vertexId, vertexLabel, props, graph);
        return vertex;
    }

    /** Cache for normalized property names — same input always gives same output. */
    private static final ConcurrentHashMap<String, String> PROPERTY_NAME_CACHE = new ConcurrentHashMap<>();

    /**
     * Normalize JanusGraph property key names to Atlas attribute names.
     * JanusGraph stores some properties with type-qualified names:
     *   "Referenceable.qualifiedName" → "qualifiedName"
     *   "Asset.name" → "name"
     *
     * Properties starting with "__" are NEVER normalized — they are Atlas internal
     * properties (e.g., "__guid", "__typeName", "__type.atlas_operation").
     * The "__type." prefix is used by Atlas's TypeDef system (PROPERTY_PREFIX = "__type.")
     * and must be preserved.
     *
     * Results are cached because this pure function is called for every property on
     * every vertex read (100 vertices × 50 properties = 5000 calls with identical inputs).
     */
    static String normalizePropertyName(String name) {
        if (name == null) return null;
        return PROPERTY_NAME_CACHE.computeIfAbsent(name, VertexRepository::doNormalizePropertyName);
    }

    private static String doNormalizePropertyName(String name) {
        // Properties starting with "__" are Atlas internal — never normalize.
        if (name.startsWith("__")) {
            return name;
        }

        // For type-qualified names like "Asset.name" or "Referenceable.qualifiedName",
        // strip the type prefix to get just the attribute name.
        int dotIndex = name.indexOf('.');
        if (dotIndex > 0 && dotIndex < name.length() - 1) {
            return name.substring(dotIndex + 1);
        }

        return name;
    }
}
