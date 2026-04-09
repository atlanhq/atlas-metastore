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
import java.util.Set;
import java.util.function.Supplier;

/**
 * Lazy {@link Iterable} that wraps a Cassandra query and maps each
 * row to a {@link CassandraEdge} on demand. The Cassandra driver handles page
 * fetching automatically via the page size configured on the statement.
 *
 * <p>Memory usage is O(pageSize) instead of O(totalEdges).
 *
 * <p>When a {@code labelFilter} is supplied, rows whose {@code edge_label} is
 * not in the filter set are skipped <b>before</b> JSON property parsing,
 * avoiding wasted deserialization on non-matching rows.
 *
 * <p>This iterable is <b>re-iterable</b> — each call to {@link #iterator()}
 * re-executes the CQL query via the supplied {@link Supplier}, producing a
 * fresh result set.
 */
public class PaginatedEdgeIterable implements Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> {

    private static final Logger LOG = LoggerFactory.getLogger(PaginatedEdgeIterable.class);

    private final Supplier<ResultSet> resultSetSupplier;
    private final String              vertexId;
    private final boolean             isOut;
    private final CassandraGraph      graph;
    private final Set<String>         labelFilter;  // null = no filtering

    /**
     * @param resultSetSupplier supplier that executes the CQL query and returns a fresh ResultSet
     * @param vertexId  the vertex ID that owns these edges
     * @param isOut     true if these are OUT edges (vertexId is the out-vertex),
     *                  false if IN edges (vertexId is the in-vertex)
     * @param graph     the graph instance for edge construction
     */
    public PaginatedEdgeIterable(Supplier<ResultSet> resultSetSupplier, String vertexId,
                                 boolean isOut, CassandraGraph graph) {
        this(resultSetSupplier, vertexId, isOut, graph, null);
    }

    /**
     * @param resultSetSupplier supplier that executes the CQL query and returns a fresh ResultSet
     * @param vertexId    the vertex ID that owns these edges
     * @param isOut       true if these are OUT edges, false if IN edges
     * @param graph       the graph instance for edge construction
     * @param labelFilter if non-null, only edges whose label is in this set are yielded;
     *                    rows with non-matching labels are skipped before JSON parsing
     */
    public PaginatedEdgeIterable(Supplier<ResultSet> resultSetSupplier, String vertexId,
                                 boolean isOut, CassandraGraph graph, Set<String> labelFilter) {
        this.resultSetSupplier = resultSetSupplier;
        this.vertexId    = vertexId;
        this.isOut       = isOut;
        this.graph       = graph;
        this.labelFilter = labelFilter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> iterator() {
        return (Iterator) new PaginatedEdgeIterator(resultSetSupplier.get());
    }

    private class PaginatedEdgeIterator implements Iterator<CassandraEdge> {
        private final Iterator<Row> rowIterator;
        private CassandraEdge nextEdge;

        PaginatedEdgeIterator(ResultSet resultSet) {
            this.rowIterator = resultSet.iterator();
        }

        @Override
        public boolean hasNext() {
            if (nextEdge != null) {
                return true;
            }
            nextEdge = advance();
            return nextEdge != null;
        }

        @Override
        public CassandraEdge next() {
            if (nextEdge == null) {
                nextEdge = advance();
            }
            if (nextEdge == null) {
                throw new NoSuchElementException();
            }
            CassandraEdge result = nextEdge;
            nextEdge = null;
            return result;
        }

        private CassandraEdge advance() {
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();

                // Check label filter BEFORE expensive JSON parsing
                if (labelFilter != null) {
                    String label = row.getString("edge_label");
                    if (!labelFilter.contains(label)) {
                        continue;
                    }
                }

                return mapRowToEdge(row);
            }
            return null;
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
