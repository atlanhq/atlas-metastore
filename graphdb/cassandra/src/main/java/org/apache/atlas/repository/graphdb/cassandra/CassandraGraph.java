package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ESAliasRequestBuilder;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.model.discovery.SearchParams;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.repository.graphdb.elasticsearch.AtlasElasticsearchDatabase;
import org.apache.atlas.type.AtlasType;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
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

    /** Tracks ES indexes that have been verified/created during this JVM session. */
    private static final Set<String> VERIFIED_ES_INDEXES = ConcurrentHashMap.newKeySet();

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
        if (vertexIds == null || vertexIds.length == 0) {
            return Collections.emptySet();
        }

        // Separate cached from uncached
        Set<AtlasVertex> result = new LinkedHashSet<>();
        List<String> uncachedIds = new ArrayList<>();
        for (String id : vertexIds) {
            CassandraVertex cached = vertexCache.get().get(id);
            if (cached != null) {
                result.add(cached);
            } else {
                uncachedIds.add(id);
            }
        }

        // Fetch uncached vertices in parallel using async queries
        if (!uncachedIds.isEmpty()) {
            Map<String, CassandraVertex> fetched = vertexRepository.getVerticesAsync(uncachedIds, this);
            for (CassandraVertex v : fetched.values()) {
                vertexCache.get().put(v.getIdString(), v);
                result.add(v);
            }
        }

        return result;
    }

    /**
     * Bulk fetch all edges for multiple vertices concurrently.
     * Returns a map of vertexId → list of all edges (both directions, all labels).
     * Edges from the transaction buffer are merged in; removed edges are excluded.
     */
    @SuppressWarnings("unchecked")
    public Map<String, List<CassandraEdge>> getAllEdgesForVertices(Collection<String> vertexIds) {
        // Fetch persisted edges for all vertices in parallel
        Map<String, List<CassandraEdge>> persisted =
            edgeRepository.getEdgesForVerticesAsync(vertexIds, AtlasEdgeDirection.BOTH, this);

        // Merge with transaction buffer
        Map<String, List<CassandraEdge>> result = new LinkedHashMap<>();
        for (String vertexId : vertexIds) {
            List<CassandraEdge> persistedEdges = persisted.getOrDefault(vertexId, Collections.emptyList());
            List<CassandraEdge> bufferedEdges = txBuffer.get().getEdgesForVertex(vertexId, AtlasEdgeDirection.BOTH, null);

            Map<String, CassandraEdge> merged = new LinkedHashMap<>();
            for (CassandraEdge e : persistedEdges) {
                if (!txBuffer.get().isEdgeRemoved(e.getIdString())) {
                    merged.put(e.getIdString(), e);
                }
            }
            for (CassandraEdge e : bufferedEdges) {
                merged.put(e.getIdString(), e);
            }
            result.put(vertexId, new ArrayList<>(merged.values()));
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, List<AtlasEdge<CassandraVertex, CassandraEdge>>> getEdgesForVertices(
            Collection<String> vertexIds) {
        Map<String, List<CassandraEdge>> raw = getAllEdgesForVertices(vertexIds);
        Map<String, List<AtlasEdge<CassandraVertex, CassandraEdge>>> result = new LinkedHashMap<>();
        for (Map.Entry<String, List<CassandraEdge>> entry : raw.entrySet()) {
            result.put(entry.getKey(), (List) entry.getValue());
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
        ensureESIndexExists(indexName);
        return new CassandraIndexQuery(this, indexName, sourceBuilder);
    }

    @Override
    public AtlasIndexQuery<CassandraVertex, CassandraEdge> elasticsearchQuery(String indexName, SearchParams searchParams) throws AtlasBaseException {
        ensureESIndexExists(indexName);
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
        ensureESIndexExists(indexName);
        return new CassandraIndexQuery(this, indexName, (SearchSourceBuilder) null);
    }

    /**
     * Ensures the given ES index exists, creating it if necessary.
     * Called lazily from elasticsearchQuery() methods, which guarantees the ES client
     * is available (unlike during startup when GraphBackedSearchIndexer runs before
     * AtlasElasticsearchDatabase is initialized).
     */
    static void ensureESIndexExists(String indexName) {
        if (indexName == null || VERIFIED_ES_INDEXES.contains(indexName)) {
            return;
        }
        try {
            RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
            if (client == null) {
                LOG.warn("ES client not available, cannot ensure index {} exists", indexName);
                return;
            }

            // HEAD check - does the index exist?
            Request headReq = new Request("HEAD", "/" + indexName);
            Response headResp = client.performRequest(headReq);
            if (headResp.getStatusLine().getStatusCode() == 200) {
                VERIFIED_ES_INDEXES.add(indexName);
                return;
            }
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                createESIndex(indexName);
            } else {
                LOG.warn("Failed to check ES index {}: {}", indexName, e.getMessage());
            }
        } catch (Exception e) {
            LOG.warn("Failed to check ES index {}: {}", indexName, e.getMessage());
        }
    }

    private static void createESIndex(String indexName) {
        try {
            RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
            if (client == null) {
                return;
            }

            String settings = "{\n" +
                "  \"settings\": {\n" +
                "    \"number_of_shards\": 1,\n" +
                "    \"number_of_replicas\": 0,\n" +
                "    \"index.mapping.total_fields.limit\": 2000\n" +
                "  },\n" +
                "  \"mappings\": {\n" +
                "    \"dynamic\": true\n" +
                "  }\n" +
                "}";

            Request req = new Request("PUT", "/" + indexName);
            req.setEntity(new StringEntity(settings, ContentType.APPLICATION_JSON));
            Response resp = client.performRequest(req);

            int status = resp.getStatusLine().getStatusCode();
            if (status >= 200 && status < 300) {
                LOG.info("Created ES index: {}", indexName);
                VERIFIED_ES_INDEXES.add(indexName);
            } else {
                LOG.warn("Failed to create ES index {}: status={}", indexName, status);
            }
        } catch (Exception e) {
            LOG.warn("Failed to create ES index {}: {}", indexName, e.getMessage());
        }
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

            // Update index entries for new and dirty vertices
            List<IndexRepository.IndexEntry> uniqueIndexEntries   = new ArrayList<>();
            List<IndexRepository.IndexEntry> propertyIndexEntries = new ArrayList<>();
            for (CassandraVertex v : newVertices) {
                buildIndexEntries(v, uniqueIndexEntries, propertyIndexEntries);
            }
            for (CassandraVertex v : dirtyVertices) {
                buildIndexEntries(v, uniqueIndexEntries, propertyIndexEntries);
            }
            LOG.info("commit: {} new vertices, {} dirty vertices, {} unique index entries, {} property index entries",
                    newVertices.size(), dirtyVertices.size(), uniqueIndexEntries.size(), propertyIndexEntries.size());
            if (!uniqueIndexEntries.isEmpty()) {
                indexRepository.batchAddIndexes(uniqueIndexEntries);
                LOG.info("commit: wrote {} unique index entries to Cassandra", uniqueIndexEntries.size());
            }
            if (!propertyIndexEntries.isEmpty()) {
                indexRepository.batchAddPropertyIndexes(propertyIndexEntries);
            }

            // Sync vertices to Elasticsearch (replaces JanusGraph's mixed index sync)
            syncVerticesToElasticsearch(newVertices, dirtyVertices, buffer.getRemovedVertices());

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

    private void buildIndexEntries(CassandraVertex vertex,
                                   List<IndexRepository.IndexEntry> uniqueEntries,
                                   List<IndexRepository.IndexEntry> propertyEntries) {
        // ---- 1:1 unique indexes (vertex_index table) ----

        // Index by __guid
        Object guid = vertex.getProperty("__guid", String.class);
        if (guid != null) {
            String guidStr = String.valueOf(guid);
            uniqueEntries.add(new IndexRepository.IndexEntry("__guid_idx", guidStr, vertex.getIdString()));
            LOG.info("buildIndexEntries: __guid_idx [{}] -> vertexId [{}]", guidStr, vertex.getIdString());
        }

        // Index by qualifiedName + __typeName (entity lookup)
        Object qn = vertex.getProperty("qualifiedName", String.class);
        Object entityTypeName = vertex.getProperty("__typeName", String.class);
        if (qn != null && entityTypeName != null) {
            String indexVal = qn + ":" + entityTypeName;
            uniqueEntries.add(new IndexRepository.IndexEntry("qn_type_idx", indexVal, vertex.getIdString()));
            LOG.info("buildIndexEntries: qn_type_idx [{}] -> vertexId [{}]", indexVal, vertex.getIdString());
        }

        // Index by VERTEX_TYPE + TYPENAME (TypeDef lookup: findTypeVertexByName)
        Object vertexType = vertex.getProperty(Constants.VERTEX_TYPE_PROPERTY_KEY, String.class);
        Object typeDefName = vertex.getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class);
        if (vertexType != null && typeDefName != null) {
            uniqueEntries.add(new IndexRepository.IndexEntry("type_typename_idx",
                    String.valueOf(vertexType) + ":" + String.valueOf(typeDefName), vertex.getIdString()));
        }

        // ---- 1:N property indexes (vertex_property_index table) ----

        // Index by VERTEX_TYPE + TYPE_CATEGORY (for findTypeVerticesByCategory / getAll)
        Object typeCategory = vertex.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY, String.class);
        if (vertexType != null && typeCategory != null) {
            propertyEntries.add(new IndexRepository.IndexEntry("type_category_idx",
                    String.valueOf(vertexType) + ":" + String.valueOf(typeCategory), vertex.getIdString()));
        }

        if (guid == null && qn == null && vertexType == null) {
            LOG.warn("buildIndexEntries: vertex [{}] has no indexable properties! Keys: {}",
                    vertex.getIdString(), vertex.getPropertyKeys());
        }
    }

    // ---- Elasticsearch sync (replaces JanusGraph's mixed index) ----

    /**
     * Syncs vertex properties to Elasticsearch after Cassandra commit.
     * This replaces JanusGraph's automatic mixed-index sync.
     * Uses the ES bulk API for efficiency.
     */
    private void syncVerticesToElasticsearch(List<CassandraVertex> newVertices,
                                             List<CassandraVertex> dirtyVertices,
                                             List<CassandraVertex> removedVertices) {
        try {
            RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
            if (client == null) {
                LOG.debug("ES client not available, skipping ES sync");
                return;
            }

            String indexName = Constants.VERTEX_INDEX_NAME;
            StringBuilder bulkBody = new StringBuilder();

            // Index new vertices
            for (CassandraVertex v : newVertices) {
                appendESIndexAction(bulkBody, indexName, v);
            }

            // Re-index dirty (updated) vertices
            for (CassandraVertex v : dirtyVertices) {
                appendESIndexAction(bulkBody, indexName, v);
            }

            // Delete removed vertices
            for (CassandraVertex v : removedVertices) {
                bulkBody.append("{\"delete\":{\"_index\":\"").append(indexName)
                        .append("\",\"_id\":\"").append(v.getIdString()).append("\"}}\n");
            }

            if (bulkBody.length() > 0) {
                Request bulkReq = new Request("POST", "/_bulk");
                bulkReq.setEntity(new StringEntity(bulkBody.toString(), ContentType.APPLICATION_JSON));
                Response resp = client.performRequest(bulkReq);
                int status = resp.getStatusLine().getStatusCode();
                if (status >= 200 && status < 300) {
                    LOG.debug("Synced {} new + {} updated vertices to ES index {}",
                            newVertices.size(), dirtyVertices.size(), indexName);
                } else {
                    LOG.warn("ES bulk sync returned status {}", status);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to sync vertices to ES: {}", e.getMessage());
        }
    }

    private void appendESIndexAction(StringBuilder bulkBody, String indexName, CassandraVertex v) {
        bulkBody.append("{\"index\":{\"_index\":\"").append(indexName)
                .append("\",\"_id\":\"").append(v.getIdString()).append("\"}}\n");
        bulkBody.append(AtlasType.toJson(v.getProperties())).append("\n");
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
        session.execute("TRUNCATE vertex_property_index");
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
