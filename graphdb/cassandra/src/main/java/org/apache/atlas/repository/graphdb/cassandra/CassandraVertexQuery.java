package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class CassandraVertexQuery implements AtlasVertexQuery<CassandraVertex, CassandraEdge> {

    private final CassandraGraph  graph;
    private final CassandraVertex vertex;
    private AtlasEdgeDirection    direction = AtlasEdgeDirection.BOTH;
    private String[]              labels;
    private final Map<String, Object> hasPredicates = new LinkedHashMap<>();

    public CassandraVertexQuery(CassandraGraph graph, CassandraVertex vertex) {
        this.graph  = graph;
        this.vertex = vertex;
    }

    @Override
    public AtlasVertexQuery<CassandraVertex, CassandraEdge> direction(AtlasEdgeDirection queryDirection) {
        this.direction = queryDirection;
        return this;
    }

    @Override
    public AtlasVertexQuery<CassandraVertex, CassandraEdge> label(String label) {
        this.labels = new String[]{label};
        return this;
    }

    @Override
    public AtlasVertexQuery<CassandraVertex, CassandraEdge> label(String... labels) {
        this.labels = labels;
        return this;
    }

    @Override
    public AtlasVertexQuery<CassandraVertex, CassandraEdge> has(String key, Object value) {
        hasPredicates.put(key, value);
        return this;
    }

    @Override
    public Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> vertices() {
        return getAdjacentVertices(-1);
    }

    @Override
    public Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> vertices(int limit) {
        return getAdjacentVertices(limit);
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges() {
        return getMatchingEdges(-1);
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges(int limit) {
        return getMatchingEdges(limit);
    }

    @Override
    public long count() {
        // When there are no has-predicates, use server-side CQL COUNT(*)
        // instead of materialising all edges into Java.
        if (hasPredicates.isEmpty()) {
            long total = 0;
            if (labels != null && labels.length > 0) {
                for (String label : labels) {
                    total += vertex.getEdgesCount(direction, label);
                }
            } else {
                total = vertex.getEdgesCount(direction, null);
            }
            return total;
        }

        // Has-predicates require property inspection — stream lazily and count
        long count = 0;
        for (AtlasEdge<CassandraVertex, CassandraEdge> ignored : getMatchingEdges(-1)) {
            count++;
        }
        return count;
    }

    private Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> getMatchingEdges(int limit) {
        // Source: already-lazy iterable from CassandraVertex (PaginatedEdgeIterable/MergedEdgeIterable)
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> rawEdges =
                (labels != null && labels.length > 0)
                        ? vertex.getEdges(direction, labels)
                        : vertex.getEdges(direction);

        // Filter: lazy predicate filter (common case: no predicates → no wrapping)
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> filtered =
                hasPredicates.isEmpty()
                        ? rawEdges
                        : new FilteredEdgeIterable(rawEdges, this::matchesHasPredicates);

        // Limit: lazy truncation (limit <= 0 means unlimited)
        return limit > 0
                ? new LimitedIterable<>(filtered, limit)
                : filtered;
    }

    @SuppressWarnings("unchecked")
    private Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> getAdjacentVertices(int limit) {
        // Edges: already-lazy from getMatchingEdges (no limit — let vertex limit handle it)
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges = getMatchingEdges(-1);

        // Map edge → adjacent vertex lazily; nulls are skipped by MappedIterable
        Function<AtlasEdge<CassandraVertex, CassandraEdge>, AtlasVertex<CassandraVertex, CassandraEdge>> edgeToVertex =
                edge -> {
                    if (direction == AtlasEdgeDirection.OUT) {
                        return edge.getInVertex();
                    } else if (direction == AtlasEdgeDirection.IN) {
                        return edge.getOutVertex();
                    } else {
                        // BOTH: return the other vertex
                        String vertexId = vertex.getIdString();
                        CassandraEdge ce = (CassandraEdge) edge;
                        if (ce.getOutVertexId().equals(vertexId)) {
                            return edge.getInVertex();
                        } else {
                            return edge.getOutVertex();
                        }
                    }
                };

        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> vertices =
                (Iterable) new MappedIterable<>(edges, edgeToVertex);

        return limit > 0
                ? new LimitedIterable<>(vertices, limit)
                : vertices;
    }

    private boolean matchesHasPredicates(AtlasEdge<CassandraVertex, CassandraEdge> edge) {
        for (Map.Entry<String, Object> entry : hasPredicates.entrySet()) {
            Object actual = edge.getProperty(entry.getKey(), Object.class);
            if (actual == null || !actual.equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }
}
