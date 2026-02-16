package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CassandraGraphQuery implements AtlasGraphQuery<CassandraVertex, CassandraEdge> {

    private final CassandraGraph graph;
    private final List<Predicate> predicates;
    private final List<AtlasGraphQuery<CassandraVertex, CassandraEdge>> orQueries;
    private String sortKey;
    private SortOrder sortOrder;
    private final boolean isChildQuery;

    public CassandraGraphQuery(CassandraGraph graph) {
        this(graph, false);
    }

    private CassandraGraphQuery(CassandraGraph graph, boolean isChildQuery) {
        this.graph        = graph;
        this.predicates   = new ArrayList<>();
        this.orQueries    = new ArrayList<>();
        this.isChildQuery = isChildQuery;
    }

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> has(String propertyKey, Object value) {
        predicates.add(new HasPredicate(propertyKey, value));
        return this;
    }

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> in(String propertyKey, Collection<?> values) {
        predicates.add(new InPredicate(propertyKey, values));
        return this;
    }

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> has(String propertyKey, QueryOperator op, Object values) {
        predicates.add(new OperatorPredicate(propertyKey, op, values));
        return this;
    }

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> orderBy(String propertyKey, SortOrder order) {
        this.sortKey   = propertyKey;
        this.sortOrder = order;
        return this;
    }

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> or(List<AtlasGraphQuery<CassandraVertex, CassandraEdge>> childQueries) {
        orQueries.addAll(childQueries);
        return this;
    }

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> createChildQuery() {
        return new CassandraGraphQuery(graph, true);
    }

    @Override
    public boolean isChildQuery() {
        return isChildQuery;
    }

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> addConditionsFrom(AtlasGraphQuery<CassandraVertex, CassandraEdge> otherQuery) {
        if (otherQuery instanceof CassandraGraphQuery) {
            CassandraGraphQuery other = (CassandraGraphQuery) otherQuery;
            this.predicates.addAll(other.predicates);
            this.orQueries.addAll(other.orQueries);
        }
        return this;
    }

    @Override
    public Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> vertices() {
        return executeVertexQuery(0, -1);
    }

    @Override
    public Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> vertices(int limit) {
        return executeVertexQuery(0, limit);
    }

    @Override
    public Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> vertices(int offset, int limit) {
        return executeVertexQuery(offset, limit);
    }

    @Override
    public Iterable<Object> vertexIds() {
        return executeVertexIdQuery(0, -1);
    }

    @Override
    public Iterable<Object> vertexIds(int limit) {
        return executeVertexIdQuery(0, limit);
    }

    @Override
    public Iterable<Object> vertexIds(int offset, int limit) {
        return executeVertexIdQuery(offset, limit);
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges() {
        return executeEdgeQuery(0, -1);
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges(int limit) {
        return executeEdgeQuery(0, limit);
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges(int offset, int limit) {
        return executeEdgeQuery(offset, limit);
    }

    // ---- Internal execution ----

    @SuppressWarnings("unchecked")
    private Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> executeVertexQuery(int offset, int limit) {
        // Try index-based lookup for simple has predicates
        if (predicates.size() >= 1 && orQueries.isEmpty()) {
            Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> indexed = tryIndexLookup();
            if (indexed != null) {
                List<AtlasVertex<CassandraVertex, CassandraEdge>> result = new ArrayList<>();
                for (AtlasVertex<CassandraVertex, CassandraEdge> v : indexed) {
                    if (matchesAllPredicates(v)) {
                        result.add(v);
                    }
                }
                return applyPaging(result, offset, limit);
            }
        }

        // Fallback: scan vertices that match via graph.getVertices for first predicate
        if (!predicates.isEmpty() && predicates.get(0) instanceof HasPredicate) {
            HasPredicate first = (HasPredicate) predicates.get(0);
            Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> candidates = graph.getVertices(first.key, first.value);

            List<AtlasVertex<CassandraVertex, CassandraEdge>> result = new ArrayList<>();
            for (AtlasVertex<CassandraVertex, CassandraEdge> v : candidates) {
                if (matchesAllPredicates(v)) {
                    result.add(v);
                }
            }
            return applyPaging(result, offset, limit);
        }

        return Collections.emptyList();
    }

    private Iterable<Object> executeVertexIdQuery(int offset, int limit) {
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> vertices = executeVertexQuery(offset, limit);
        List<Object> ids = new ArrayList<>();
        for (AtlasVertex<CassandraVertex, CassandraEdge> v : vertices) {
            ids.add(v.getId());
        }
        return ids;
    }

    @SuppressWarnings("unchecked")
    private Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> executeEdgeQuery(int offset, int limit) {
        // Edges are not typically queried by property via graph query
        // Return empty for now - edge queries go through vertex.getEdges()
        return Collections.emptyList();
    }

    private Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> tryIndexLookup() {
        // Check if we have a simple __guid lookup
        for (Predicate p : predicates) {
            if (p instanceof HasPredicate) {
                HasPredicate hp = (HasPredicate) p;
                if ("__guid".equals(hp.key) || "Property.__guid".equals(hp.key)) {
                    // Try Cassandra index first (works after commit)
                    String vertexId = graph.getIndexRepository().lookupVertex("__guid_idx", String.valueOf(hp.value));
                    if (vertexId != null) {
                        AtlasVertex<CassandraVertex, CassandraEdge> vertex = graph.getVertex(vertexId);
                        if (vertex != null) {
                            return Collections.singletonList(vertex);
                        }
                    }
                    // Index entry may not be committed yet - scan vertex cache (pre-commit case)
                    return scanVertexCache();
                }
            }
        }

        // Check for qualifiedName + __typeName composite lookup
        // qualifiedName may appear as "qualifiedName" or "__u_qualifiedName" (unique shade property)
        String qn = null, typeName = null;
        for (Predicate p : predicates) {
            if (p instanceof HasPredicate) {
                HasPredicate hp = (HasPredicate) p;
                if ("qualifiedName".equals(hp.key) || "Property.qualifiedName".equals(hp.key)
                        || "__u_qualifiedName".equals(hp.key) || "Property.__u_qualifiedName".equals(hp.key)) {
                    qn = String.valueOf(hp.value);
                } else if ("__typeName".equals(hp.key) || "Property.__typeName".equals(hp.key)) {
                    typeName = String.valueOf(hp.value);
                }
            }
        }
        if (qn != null && typeName != null) {
            // Try Cassandra index first (works after commit)
            String vertexId = graph.getIndexRepository().lookupVertex("qn_type_idx", qn + ":" + typeName);
            if (vertexId != null) {
                AtlasVertex<CassandraVertex, CassandraEdge> vertex = graph.getVertex(vertexId);
                if (vertex != null) {
                    return Collections.singletonList(vertex);
                }
            }
            // Index entry may not be committed yet - scan vertex cache (pre-commit case)
            return scanVertexCache();
        }

        return null; // No index available
    }

    /**
     * Scan the in-memory vertex cache for vertices matching all predicates.
     * Used as fallback when Cassandra index entries haven't been committed yet.
     */
    @SuppressWarnings("unchecked")
    private List<AtlasVertex<CassandraVertex, CassandraEdge>> scanVertexCache() {
        List<AtlasVertex<CassandraVertex, CassandraEdge>> result = new ArrayList<>();
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> allCachedVertices = (Iterable) graph.getVertices();
        for (AtlasVertex<CassandraVertex, CassandraEdge> v : allCachedVertices) {
            if (matchesAllPredicates(v)) {
                result.add(v);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private boolean matchesAllPredicates(AtlasVertex<CassandraVertex, CassandraEdge> vertex) {
        for (Predicate p : predicates) {
            if (!p.matches(vertex)) {
                return false;
            }
        }

        if (!orQueries.isEmpty()) {
            boolean anyMatch = false;
            for (AtlasGraphQuery<CassandraVertex, CassandraEdge> orQuery : orQueries) {
                CassandraGraphQuery cq = (CassandraGraphQuery) orQuery;
                boolean allMatch = true;
                for (Predicate p : cq.predicates) {
                    if (!p.matches(vertex)) {
                        allMatch = false;
                        break;
                    }
                }
                if (allMatch) {
                    anyMatch = true;
                    break;
                }
            }
            if (!anyMatch) {
                return false;
            }
        }

        return true;
    }

    private <T> List<T> applyPaging(List<T> items, int offset, int limit) {
        if (offset > 0 && offset < items.size()) {
            items = items.subList(offset, items.size());
        }
        if (limit > 0 && limit < items.size()) {
            items = items.subList(0, limit);
        }
        return items;
    }

    // ---- Predicate types ----

    private interface Predicate {
        boolean matches(AtlasVertex<CassandraVertex, CassandraEdge> vertex);
    }

    private static class HasPredicate implements Predicate {
        final String key;
        final Object value;

        HasPredicate(String key, Object value) {
            this.key   = key;
            this.value = value;
        }

        @Override
        public boolean matches(AtlasVertex<CassandraVertex, CassandraEdge> vertex) {
            Object actual = vertex.getProperty(key, Object.class);
            if (actual == null) {
                return value == null;
            }
            return actual.equals(value) || String.valueOf(actual).equals(String.valueOf(value));
        }
    }

    private static class InPredicate implements Predicate {
        final String key;
        final Collection<?> values;

        InPredicate(String key, Collection<?> values) {
            this.key    = key;
            this.values = values;
        }

        @Override
        public boolean matches(AtlasVertex<CassandraVertex, CassandraEdge> vertex) {
            Object actual = vertex.getProperty(key, Object.class);
            if (actual == null) {
                return false;
            }
            for (Object v : values) {
                if (actual.equals(v) || String.valueOf(actual).equals(String.valueOf(v))) {
                    return true;
                }
            }
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static class OperatorPredicate implements Predicate {
        final String key;
        final QueryOperator op;
        final Object value;

        OperatorPredicate(String key, QueryOperator op, Object value) {
            this.key   = key;
            this.op    = op;
            this.value = value;
        }

        @Override
        public boolean matches(AtlasVertex<CassandraVertex, CassandraEdge> vertex) {
            Object actual = vertex.getProperty(key, Object.class);
            if (actual == null) {
                return false;
            }

            if (op instanceof ComparisionOperator) {
                return matchComparison((ComparisionOperator) op, actual, value);
            }

            if (op instanceof MatchingOperator) {
                return matchString((MatchingOperator) op, String.valueOf(actual), String.valueOf(value));
            }

            return false;
        }

        private boolean matchComparison(ComparisionOperator cop, Object actual, Object expected) {
            if (actual instanceof Comparable && expected instanceof Comparable) {
                int cmp = ((Comparable) actual).compareTo(expected);
                switch (cop) {
                    case EQUAL:              return cmp == 0;
                    case NOT_EQUAL:          return cmp != 0;
                    case GREATER_THAN:       return cmp > 0;
                    case GREATER_THAN_EQUAL: return cmp >= 0;
                    case LESS_THAN:          return cmp < 0;
                    case LESS_THAN_EQUAL:    return cmp <= 0;
                }
            }
            return false;
        }

        private boolean matchString(MatchingOperator mop, String actual, String pattern) {
            switch (mop) {
                case CONTAINS: return actual.toLowerCase().contains(pattern.toLowerCase());
                case PREFIX:   return actual.startsWith(pattern);
                case SUFFIX:   return actual.endsWith(pattern);
                case REGEX:    return actual.matches(pattern);
            }
            return false;
        }
    }
}
