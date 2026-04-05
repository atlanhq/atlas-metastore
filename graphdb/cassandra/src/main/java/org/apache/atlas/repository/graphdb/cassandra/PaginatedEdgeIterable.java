package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Lazy {@link Iterable} that wraps a Cassandra {@link ResultSet} and maps each
 * row to a {@link CassandraEdge} on demand. The Cassandra driver handles page
 * fetching automatically via the page size configured on the statement.
 *
 * <p>Memory usage is O(pageSize) instead of O(totalEdges).
 *
 * <p>This is a <b>single-use</b> iterable — calling {@link #iterator()} more
 * than once throws {@link IllegalStateException}. All audited callsites
 * consume the iterable exactly once (verified in Risk 1 analysis).
 */
public class PaginatedEdgeIterable implements Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> {

    private static final Logger LOG = LoggerFactory.getLogger(PaginatedEdgeIterable.class);

    private final ResultSet     resultSet;
    private final String        vertexId;
    private final boolean       isOut;
    private final CassandraGraph graph;
    private volatile boolean    consumed = false;

    /**
     * @param resultSet Cassandra result set with page size already configured
     * @param vertexId  the vertex ID that owns these edges
     * @param isOut     true if these are OUT edges (vertexId is the out-vertex),
     *                  false if IN edges (vertexId is the in-vertex)
     * @param graph     the graph instance for edge construction
     */
    public PaginatedEdgeIterable(ResultSet resultSet, String vertexId, boolean isOut, CassandraGraph graph) {
        this.resultSet = resultSet;
        this.vertexId  = vertexId;
        this.isOut     = isOut;
        this.graph     = graph;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> iterator() {
        if (consumed) {
            throw new IllegalStateException(
                "PaginatedEdgeIterable has already been consumed. " +
                "Each instance can only be iterated once.");
        }
        consumed = true;
        return (Iterator) new PaginatedEdgeIterator();
    }

    private class PaginatedEdgeIterator implements Iterator<CassandraEdge> {
        private final Iterator<Row> rowIterator = resultSet.iterator();

        @Override
        public boolean hasNext() {
            return rowIterator.hasNext();
        }

        @Override
        public CassandraEdge next() {
            if (!rowIterator.hasNext()) {
                throw new NoSuchElementException();
            }
            Row row = rowIterator.next();
            return mapRowToEdge(row);
        }

        private CassandraEdge mapRowToEdge(Row row) {
            String edgeId    = row.getString("edge_id");
            String label     = row.getString("edge_label");
            String propsJson = row.getString("properties");
            String state     = row.getString("state");

            Map<String, Object> props = parseProperties(propsJson);
            props.put("__state", state != null ? state : "ACTIVE");

            if (isOut) {
                String inVertexId = row.getString("in_vertex_id");
                return new CassandraEdge(edgeId, vertexId, inVertexId, label, props, graph);
            } else {
                String outVertexId = row.getString("out_vertex_id");
                return new CassandraEdge(edgeId, outVertexId, vertexId, label, props, graph);
            }
        }

        @SuppressWarnings("unchecked")
        private Map<String, Object> parseProperties(String propsJson) {
            if (propsJson == null || propsJson.isEmpty()) {
                return new LinkedHashMap<>();
            }
            Map<String, Object> props = AtlasType.fromJson(propsJson, Map.class);
            return props != null ? props : new LinkedHashMap<>();
        }
    }
}
