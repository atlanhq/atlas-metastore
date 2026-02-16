package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ESAliasRequestBuilder;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.model.discovery.SearchParams;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.type.AtlasType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CassandraGraph implements AtlasGraph<CassandraVertex, CassandraEdge> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraGraph.class);

    private final CqlSession        session;
    private final VertexRepository  vertexRepository;
    private final EdgeRepository    edgeRepository;
    private final IndexRepository   indexRepository;
    private final Set<String>       multiProperties;

    // Transaction buffer: accumulates changes in memory, flushed on commit
    private final ThreadLocal<TransactionBuffer> txBuffer = ThreadLocal.withInitial(TransactionBuffer::new);

    // Vertex cache for the current thread/transaction
    private final ThreadLocal<Map<String, CassandraVertex>> vertexCache = ThreadLocal.withInitial(ConcurrentHashMap::new);

    // Schema registry (in-memory)
    private final Map<String, CassandraPropertyKey> propertyKeys = new ConcurrentHashMap<>();
    private final Map<String, CassandraEdgeLabel>   edgeLabels   = new ConcurrentHashMap<>();
    private final Map<String, CassandraGraphIndex>  graphIndexes = new ConcurrentHashMap<>();

    public CassandraGraph(CqlSession session) {
        this.session          = session;
        this.vertexRepository = new VertexRepository(session);
        this.edgeRepository   = new EdgeRepository(session);
        this.indexRepository  = new IndexRepository(session);
        this.multiProperties  = ConcurrentHashMap.newKeySet();
    }

    // ---- Vertex operations ----

    @Override
    public AtlasVertex<CassandraVertex, CassandraEdge> addVertex() {
        String vertexId = UUID.randomUUID().toString();
        CassandraVertex vertex = new CassandraVertex(vertexId, this);
        txBuffer.get().addVertex(vertex);
        vertexCache.get().put(vertexId, vertex);
        return vertex;
    }

    @Override
    public AtlasVertex<CassandraVertex, CassandraEdge> getVertex(String vertexId) {
        if (vertexId == null) {
            return null;
        }

        // Check transaction buffer first
        CassandraVertex cached = vertexCache.get().get(vertexId);
        if (cached != null) {
            return cached;
        }

        // Then read from Cassandra
        CassandraVertex vertex = vertexRepository.getVertex(vertexId, this);
        if (vertex != null) {
            vertexCache.get().put(vertexId, vertex);
        }
        return vertex;
    }

    @Override
    public Set<AtlasVertex> getVertices(String... vertexIds) {
        Set<AtlasVertex> result = new LinkedHashSet<>();
        if (vertexIds != null) {
            for (String id : vertexIds) {
                AtlasVertex v = getVertex(id);
                if (v != null) {
                    result.add(v);
                }
            }
        }
        return result;
    }

    @Override
    public Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> getVertices(String key, Object value) {
        // Try composite index lookup first
        String indexValue = String.valueOf(value);

        // Check known composite indexes
        String vertexId = indexRepository.lookupVertex(key + "_idx", indexValue);
        if (vertexId != null) {
            AtlasVertex<CassandraVertex, CassandraEdge> vertex = getVertex(vertexId);
            if (vertex != null) {
                return Collections.singletonList(vertex);
            }
        }

        // Fallback: scan transaction buffer
        List<AtlasVertex<CassandraVertex, CassandraEdge>> result = new ArrayList<>();
        for (CassandraVertex v : vertexCache.get().values()) {
            Object propVal = v.getProperty(key, Object.class);
            if (propVal != null && propVal.equals(value)) {
                result.add(v);
            }
        }

        return result;
    }

    @Override
    public Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> getVertices() {
        // This is a potentially expensive full scan - used rarely
        LOG.warn("getVertices() called without parameters - this performs a full table scan");
        return new ArrayList<>(vertexCache.get().values());
    }

    @Override
    public void removeVertex(AtlasVertex<CassandraVertex, CassandraEdge> vertex) {
        if (vertex == null) {
            return;
        }
        CassandraVertex cv = (CassandraVertex) vertex;
        cv.markDeleted();
        txBuffer.get().removeVertex(cv);
        vertexCache.get().remove(cv.getIdString());
    }

    // ---- Edge operations ----

    @Override
    public AtlasEdge<CassandraVertex, CassandraEdge> addEdge(AtlasVertex<CassandraVertex, CassandraEdge> outVertex,
                                                               AtlasVertex<CassandraVertex, CassandraEdge> inVertex,
                                                               String label) {
        String edgeId = UUID.randomUUID().toString();
        CassandraEdge edge = new CassandraEdge(edgeId,
                ((CassandraVertex) outVertex).getIdString(),
                ((CassandraVertex) inVertex).getIdString(),
                label, this);
        txBuffer.get().addEdge(edge);
        return edge;
    }

    @Override
    public AtlasEdge<CassandraVertex, CassandraEdge> getEdgeBetweenVertices(AtlasVertex fromVertex,
                                                                             AtlasVertex toVertex,
                                                                             String relationshipLabel) {
        if (fromVertex == null || toVertex == null) {
            return null;
        }

        String fromId = ((CassandraVertex) fromVertex).getIdString();
        String toId   = ((CassandraVertex) toVertex).getIdString();

        List<CassandraEdge> outEdges = edgeRepository.getEdgesForVertex(fromId, AtlasEdgeDirection.OUT, relationshipLabel, this);
        for (CassandraEdge edge : outEdges) {
            if (edge.getInVertexId().equals(toId)) {
                return edge;
            }
        }
        return null;
    }

    @Override
    public AtlasEdge<CassandraVertex, CassandraEdge> getEdge(String edgeId) {
        if (edgeId == null) {
            return null;
        }
        return edgeRepository.getEdge(edgeId, this);
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> getEdges() {
        LOG.warn("getEdges() called without parameters - this is not efficient");
        return Collections.emptyList();
    }

    @Override
    public void removeEdge(AtlasEdge<CassandraVertex, CassandraEdge> edge) {
        if (edge == null) {
            return;
        }
        CassandraEdge ce = (CassandraEdge) edge;
        ce.markDeleted();
        txBuffer.get().removeEdge(ce);
    }

    @SuppressWarnings("unchecked")
    Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> getEdgesForVertex(String vertexId,
                                                                            AtlasEdgeDirection direction,
                                                                            String edgeLabel) {
        // Check buffer for new edges first
        List<CassandraEdge> bufferedEdges = txBuffer.get().getEdgesForVertex(vertexId, direction, edgeLabel);
        List<CassandraEdge> persistedEdges = edgeRepository.getEdgesForVertex(vertexId, direction, edgeLabel, this);

        // Merge, avoiding duplicates
        Map<String, CassandraEdge> merged = new LinkedHashMap<>();
        for (CassandraEdge e : persistedEdges) {
            if (!txBuffer.get().isEdgeRemoved(e.getIdString())) {
                merged.put(e.getIdString(), e);
            }
        }
        for (CassandraEdge e : bufferedEdges) {
            merged.put(e.getIdString(), e);
        }

        return (Iterable) new ArrayList<>(merged.values());
    }

    // ---- Query operations ----

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> query() {
        return new CassandraGraphQuery(this);
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex, AtlasEdge> V(Object... vertexIds) {
        return new CassandraGraphTraversal(this, vertexIds);
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex, AtlasEdge> E(Object... edgeIds) {
        return new CassandraGraphTraversal(this, true, edgeIds);
    }

    @Override
    public AtlasIndexQuery<CassandraVertex, CassandraEdge> indexQuery(String indexName, String queryString) {
        return new CassandraIndexQuery(this, indexName, queryString, 0);
    }

    @Override
    public AtlasIndexQuery<CassandraVertex, CassandraEdge> indexQuery(String indexName, String queryString, int offset) {
        return new CassandraIndexQuery(this, indexName, queryString, offset);
    }

    @Override
    public AtlasIndexQuery<CassandraVertex, CassandraEdge> indexQuery(GraphIndexQueryParameters indexQueryParameters) {
        return new CassandraIndexQuery(this, indexQueryParameters);
    }

    @Override
    public AtlasIndexQuery<CassandraVertex, CassandraEdge> elasticsearchQuery(String indexName, SearchSourceBuilder sourceBuilder) {
        return new CassandraIndexQuery(this, indexName, sourceBuilder);
    }

    @Override
    public AtlasIndexQuery<CassandraVertex, CassandraEdge> elasticsearchQuery(String indexName, SearchParams searchParams) throws AtlasBaseException {
        return new CassandraIndexQuery(this, indexName, searchParams);
    }

    @Override
    public void createOrUpdateESAlias(ESAliasRequestBuilder aliasRequestBuilder) throws AtlasBaseException {
        // TODO: implement ES alias management
        LOG.debug("createOrUpdateESAlias called - delegating to ES client");
    }

    @Override
    public void deleteESAlias(String indexName, String aliasName) throws AtlasBaseException {
        // TODO: implement ES alias deletion
        LOG.debug("deleteESAlias called for index={}, alias={}", indexName, aliasName);
    }

    @Override
    public AtlasIndexQuery elasticsearchQuery(String indexName) throws AtlasBaseException {
        return new CassandraIndexQuery(this, indexName, (SearchSourceBuilder) null);
    }

    // ---- Transaction operations ----

    @Override
    public void commit() {
        TransactionBuffer buffer = txBuffer.get();

        try {
            // Flush new/dirty vertices
            List<CassandraVertex> newVertices = buffer.getNewVertices();
            if (!newVertices.isEmpty()) {
                vertexRepository.batchInsertVertices(newVertices);
            }

            List<CassandraVertex> dirtyVertices = buffer.getDirtyVertices();
            for (CassandraVertex v : dirtyVertices) {
                vertexRepository.updateVertex(v);
            }

            // Flush new edges
            List<CassandraEdge> newEdges = buffer.getNewEdges();
            if (!newEdges.isEmpty()) {
                edgeRepository.batchInsertEdges(newEdges);
            }

            // Process removals
            for (CassandraEdge edge : buffer.getRemovedEdges()) {
                edgeRepository.deleteEdge(edge);
            }
            for (CassandraVertex vertex : buffer.getRemovedVertices()) {
                edgeRepository.deleteEdgesForVertex(vertex.getIdString(), this);
                vertexRepository.deleteVertex(vertex.getIdString());
            }

            // Update index entries for new vertices
            List<IndexRepository.IndexEntry> indexEntries = new ArrayList<>();
            for (CassandraVertex v : newVertices) {
                buildIndexEntries(v, indexEntries);
            }
            for (CassandraVertex v : dirtyVertices) {
                buildIndexEntries(v, indexEntries);
            }
            if (!indexEntries.isEmpty()) {
                indexRepository.batchAddIndexes(indexEntries);
            }

            // Mark all elements as persisted
            for (CassandraVertex v : newVertices) {
                v.markPersisted();
            }
            for (CassandraVertex v : dirtyVertices) {
                v.markPersisted();
            }
            for (CassandraEdge e : newEdges) {
                e.markPersisted();
            }

        } finally {
            buffer.clear();
            // Clear vertex cache so the next transaction on this thread reads fresh from Cassandra.
            // Without this, a thread reused from the pool could return stale cached vertices.
            vertexCache.get().clear();
        }
    }

    @Override
    public void rollback() {
        TransactionBuffer buffer = txBuffer.get();
        buffer.clear();
        // Clear vertex cache so the next transaction reads fresh from Cassandra
        vertexCache.get().clear();
    }

    private void buildIndexEntries(CassandraVertex vertex, List<IndexRepository.IndexEntry> entries) {
        // Index by __guid
        Object guid = vertex.getProperty("__guid", String.class);
        if (guid != null) {
            entries.add(new IndexRepository.IndexEntry("__guid_idx", String.valueOf(guid), vertex.getIdString()));
        }

        // Index by qualifiedName + __typeName
        Object qn = vertex.getProperty("qualifiedName", String.class);
        Object typeName = vertex.getProperty("__typeName", String.class);
        if (qn != null && typeName != null) {
            entries.add(new IndexRepository.IndexEntry("qn_type_idx", qn + ":" + typeName, vertex.getIdString()));
        }
    }

    // ---- Management operations ----

    @Override
    public AtlasGraphManagement getManagementSystem() {
        return new CassandraGraphManagement(this);
    }

    @Override
    public Set<String> getEdgeIndexKeys() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getVertexIndexKeys() {
        return Collections.emptySet();
    }

    // ---- Gremlin operations (deprecated/no-op) ----

    @Override
    public GroovyExpression generatePersisentToLogicalConversionExpression(GroovyExpression valueExpr, AtlasType type) {
        return valueExpr;
    }

    @Override
    public boolean isPropertyValueConversionNeeded(AtlasType type) {
        return false;
    }

    @Override
    public GremlinVersion getSupportedGremlinVersion() {
        return GremlinVersion.THREE;
    }

    @Override
    public boolean requiresInitialIndexedPredicate() {
        return false;
    }

    @Override
    public GroovyExpression getInitialIndexedPredicate(GroovyExpression parent) {
        return parent;
    }

    @Override
    public GroovyExpression addOutputTransformationPredicate(GroovyExpression expr, boolean isSelect, boolean isPath) {
        return expr;
    }

    @Override
    public ScriptEngine getGremlinScriptEngine() throws AtlasBaseException {
        throw new AtlasBaseException("Gremlin script engine is not supported in Cassandra graph backend");
    }

    @Override
    public void releaseGremlinScriptEngine(ScriptEngine scriptEngine) {
        // no-op
    }

    @Override
    public Object executeGremlinScript(String query, boolean isPath) throws AtlasBaseException {
        throw new AtlasBaseException("Gremlin script execution is not supported in Cassandra graph backend");
    }

    @Override
    public Object executeGremlinScript(ScriptEngine scriptEngine, Map<? extends String, ? extends Object> bindings,
                                       String query, boolean isPath) throws ScriptException {
        throw new ScriptException("Gremlin script execution is not supported in Cassandra graph backend");
    }

    // ---- Utility operations ----

    @Override
    public boolean isMultiProperty(String name) {
        return multiProperties.contains(name);
    }

    public void addMultiProperty(String name) {
        multiProperties.add(name);
    }

    @Override
    public AtlasIndexQueryParameter indexQueryParameter(String parameterName, String parameterValue) {
        return new CassandraIndexQueryParameter(parameterName, parameterValue);
    }

    @Override
    public AtlasGraphIndexClient getGraphIndexClient() throws AtlasException {
        return new CassandraGraphIndexClient(this);
    }

    @Override
    public void shutdown() {
        CassandraSessionProvider.shutdown();
    }

    @Override
    public void clear() {
        LOG.warn("clear() called - this will truncate all graph tables");
        session.execute("TRUNCATE vertices");
        session.execute("TRUNCATE edges_out");
        session.execute("TRUNCATE edges_in");
        session.execute("TRUNCATE edges_by_id");
        session.execute("TRUNCATE vertex_index");
        vertexCache.get().clear();
        txBuffer.get().clear();
    }

    @Override
    public Set getOpenTransactions() {
        return Collections.emptySet();
    }

    @Override
    public void exportToGson(OutputStream os) throws IOException {
        throw new UnsupportedOperationException("exportToGson is not supported in Cassandra graph backend");
    }

    // ---- Internal accessors ----

    public CqlSession getSession() {
        return session;
    }

    public VertexRepository getVertexRepository() {
        return vertexRepository;
    }

    public EdgeRepository getEdgeRepository() {
        return edgeRepository;
    }

    public IndexRepository getIndexRepository() {
        return indexRepository;
    }

    public Map<String, CassandraPropertyKey> getPropertyKeysMap() {
        return propertyKeys;
    }

    public Map<String, CassandraEdgeLabel> getEdgeLabelsMap() {
        return edgeLabels;
    }

    public Map<String, CassandraGraphIndex> getGraphIndexesMap() {
        return graphIndexes;
    }

    void notifyVertexDirty(CassandraVertex vertex) {
        txBuffer.get().markVertexDirty(vertex);
    }

    void clearVertexCache() {
        vertexCache.get().clear();
    }
}
