package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.repository.graphdb.elasticsearch.AtlasElasticsearchDatabase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CassandraGraphManagement implements AtlasGraphManagement {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraGraphManagement.class);

    // Cache of ES index -> set of field names already mapped, to avoid redundant PUT requests on restart
    private static final Map<String, Set<String>> esMappingCache = new ConcurrentHashMap<>();

    private final CassandraGraph graph;

    public CassandraGraphManagement(CassandraGraph graph) {
        this.graph = graph;
    }

    @Override
    public boolean containsPropertyKey(String key) {
        return graph.getPropertyKeysMap().containsKey(key);
    }

    @Override
    public void rollback() {
        // no-op for schema management
    }

    @Override
    public void commit() {
        // Schema changes are applied immediately in the in-memory registry
        // Persist to schema_registry table
        LOG.debug("CassandraGraphManagement.commit() - schema changes persisted");
    }

    @Override
    public AtlasPropertyKey makePropertyKey(String propertyName, Class propertyClass, AtlasCardinality cardinality) {
        CassandraPropertyKey key = new CassandraPropertyKey(propertyName, propertyClass, cardinality);
        graph.getPropertyKeysMap().put(propertyName, key);

        if (cardinality.isMany()) {
            graph.addMultiProperty(propertyName);
        }

        return key;
    }

    @Override
    public AtlasEdgeLabel makeEdgeLabel(String label) {
        CassandraEdgeLabel edgeLabel = new CassandraEdgeLabel(label);
        graph.getEdgeLabelsMap().put(label, edgeLabel);
        return edgeLabel;
    }

    @Override
    public void deletePropertyKey(String propertyKey) {
        graph.getPropertyKeysMap().remove(propertyKey);
    }

    @Override
    public AtlasPropertyKey getPropertyKey(String propertyName) {
        return graph.getPropertyKeysMap().get(propertyName);
    }

    @Override
    public AtlasEdgeLabel getEdgeLabel(String label) {
        return graph.getEdgeLabelsMap().get(label);
    }

    @Override
    public void createVertexCompositeIndex(String propertyName, boolean isUnique, List<AtlasPropertyKey> propertyKeys) {
        CassandraGraphIndex index = new CassandraGraphIndex(propertyName, false, true, false, true, isUnique);
        for (AtlasPropertyKey key : propertyKeys) {
            index.addFieldKey(key);
        }
        graph.getGraphIndexesMap().put(propertyName, index);
        LOG.info("Created vertex composite index: {}, unique={}", propertyName, isUnique);
    }

    @Override
    public void createEdgeCompositeIndex(String propertyName, boolean isUnique, List<AtlasPropertyKey> propertyKeys) {
        CassandraGraphIndex index = new CassandraGraphIndex(propertyName, false, true, true, false, isUnique);
        for (AtlasPropertyKey key : propertyKeys) {
            index.addFieldKey(key);
        }
        graph.getGraphIndexesMap().put(propertyName, index);
        LOG.info("Created edge composite index: {}", propertyName);
    }

    @Override
    public AtlasGraphIndex getGraphIndex(String indexName) {
        return graph.getGraphIndexesMap().get(indexName);
    }

    @Override
    public boolean edgeIndexExist(String label, String indexName) {
        return graph.getGraphIndexesMap().containsKey(indexName);
    }

    @Override
    public void createVertexMixedIndex(String name, String backingIndex, List<AtlasPropertyKey> propertyKeys) {
        CassandraGraphIndex index = new CassandraGraphIndex(name, true, false, false, true, false);
        for (AtlasPropertyKey key : propertyKeys) {
            index.addFieldKey(key);
        }
        graph.getGraphIndexesMap().put(name, index);
        // Use prefixed name (e.g., "janusgraph_vertex_index") matching what query callers expect
        String esIndexName = Constants.INDEX_PREFIX + name;
        ensureESIndexExists(esIndexName);
        LOG.info("Created vertex mixed index: {} (ES index: {}, backing: {})", name, esIndexName, backingIndex);
    }

    @Override
    public void createEdgeMixedIndex(String indexName, String backingIndex, List<AtlasPropertyKey> propertyKeys) {
        CassandraGraphIndex index = new CassandraGraphIndex(indexName, true, false, true, false, false);
        for (AtlasPropertyKey key : propertyKeys) {
            index.addFieldKey(key);
        }
        graph.getGraphIndexesMap().put(indexName, index);
        // Use prefixed name matching what query callers expect
        String esIndexName = Constants.INDEX_PREFIX + indexName;
        ensureESIndexExists(esIndexName);
        LOG.info("Created edge mixed index: {} (ES index: {}, backing: {})", indexName, esIndexName, backingIndex);
    }

    @Override
    public void createEdgeIndex(String label, String indexName, AtlasEdgeDirection edgeDirection,
                                List<AtlasPropertyKey> propertyKeys) {
        LOG.info("Created edge index: {} for label: {}", indexName, label);
    }

    @Override
    public void createFullTextMixedIndex(String index, String backingIndex, List<AtlasPropertyKey> propertyKeys) {
        CassandraGraphIndex graphIndex = new CassandraGraphIndex(index, true, false, false, true, false);
        for (AtlasPropertyKey key : propertyKeys) {
            graphIndex.addFieldKey(key);
        }
        graph.getGraphIndexesMap().put(index, graphIndex);
        LOG.info("Created full-text mixed index: {} (backing: {})", index, backingIndex);
    }

    @Override
    public String addMixedIndex(String vertexIndex, AtlasPropertyKey propertyKey, boolean isStringField) {
        CassandraGraphIndex index = graph.getGraphIndexesMap().get(vertexIndex);
        if (index != null) {
            index.addFieldKey(propertyKey);
        }
        // Update ES mapping so queries (sort, filter) work even before any documents are indexed
        String esIndexName = Constants.INDEX_PREFIX + vertexIndex;
        Class<?> propClass = (propertyKey instanceof CassandraPropertyKey)
                ? ((CassandraPropertyKey) propertyKey).getPropertyClass() : null;
        addESFieldMapping(esIndexName, propertyKey.getName(), propClass, isStringField);
        // Return the property name directly (no JanusGraph field encoding)
        return propertyKey.getName();
    }

    @Override
    public String addMixedIndex(String vertexIndex, AtlasPropertyKey propertyKey, boolean isStringField,
                                HashMap<String, Object> indexTypeESConfig,
                                HashMap<String, HashMap<String, Object>> indexTypeESFields) {
        return addMixedIndex(vertexIndex, propertyKey, isStringField);
    }

    @Override
    public String getIndexFieldName(String indexName, AtlasPropertyKey propertyKey, boolean isStringField) {
        // Return property name directly - no JanusGraph field encoding needed
        return propertyKey.getName();
    }

    @Override
    public void updateUniqueIndexesForConsistencyLock() {
        LOG.debug("updateUniqueIndexesForConsistencyLock - no-op in Cassandra backend");
    }

    @Override
    public void updateSchemaStatus() {
        LOG.debug("updateSchemaStatus - no-op in Cassandra backend");
    }

    @Override
    public void reindex(String indexName, List<AtlasElement> elements) throws Exception {
        LOG.info("Reindex request for index: {}, elements: {}", indexName, elements.size());
    }

    @Override
    public Object startIndexRecovery(long startTime) {
        LOG.info("startIndexRecovery from: {} - no-op in Cassandra backend", startTime);
        return null;
    }

    @Override
    public void stopIndexRecovery(Object txRecoveryObject) {
        // no-op
    }

    @Override
    public void printIndexRecoveryStats(Object txRecoveryObject) {
        // no-op
    }

    /**
     * Ensures the Elasticsearch index exists, creating it with dynamic mapping if not.
     * JanusGraph normally creates these indexes; in the Cassandra backend we manage them directly.
     */
    private void ensureESIndexExists(String indexName) {
        try {
            RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
            if (client == null) {
                LOG.warn("ES client not available, cannot ensure index {} exists", indexName);
                return;
            }

            // Check if index already exists
            Request headReq = new Request("HEAD", "/" + indexName);
            Response headResp = client.performRequest(headReq);
            if (headResp.getStatusLine().getStatusCode() == 200) {
                LOG.info("ES index {} already exists", indexName);
                preloadESMappingCache(client, indexName);
                return;
            }
        } catch (org.elasticsearch.client.ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                // Index doesn't exist, create it
                createESIndex(indexName);
            } else {
                LOG.warn("Failed to check ES index {}: {}", indexName, e.getMessage());
            }
        } catch (Exception e) {
            LOG.warn("Failed to check ES index {}: {}", indexName, e.getMessage());
        }
    }

    /**
     * Preloads the ES mapping cache with existing field names from an ES index.
     * This avoids redundant PUT _mapping requests for fields that already exist.
     */
    private void preloadESMappingCache(RestClient client, String indexName) {
        try {
            Request req = new Request("GET", "/" + indexName + "/_mapping");
            Response resp = client.performRequest(req);
            if (resp.getStatusLine().getStatusCode() == 200) {
                String body = EntityUtils.toString(resp.getEntity());
                // Parse field names from the mapping response JSON
                // Response format: {"indexName":{"mappings":{"properties":{"field1":{...},"field2":{...}}}}}
                Set<String> cachedFields = esMappingCache.computeIfAbsent(indexName, k -> ConcurrentHashMap.newKeySet());
                if (body != null && body.contains("\"properties\"")) {
                    int propsIdx = body.indexOf("\"properties\"");
                    if (propsIdx > 0) {
                        // Simple extraction: find all quoted strings that are field names in properties
                        String propsSection = body.substring(propsIdx);
                        int braceStart = propsSection.indexOf('{');
                        if (braceStart >= 0) {
                            int depth = 0;
                            int fieldStart = -1;
                            boolean inQuote = false;
                            for (int i = braceStart; i < propsSection.length(); i++) {
                                char c = propsSection.charAt(i);
                                if (c == '"' && (i == 0 || propsSection.charAt(i - 1) != '\\')) {
                                    inQuote = !inQuote;
                                    if (inQuote && depth == 1) {
                                        fieldStart = i + 1;
                                    } else if (!inQuote && depth == 1 && fieldStart > 0) {
                                        String fieldName = propsSection.substring(fieldStart, i);
                                        cachedFields.add(fieldName);
                                        fieldStart = -1;
                                    }
                                } else if (!inQuote) {
                                    if (c == '{') depth++;
                                    else if (c == '}') {
                                        depth--;
                                        if (depth == 0) break;
                                    }
                                }
                            }
                        }
                    }
                }
                LOG.info("Preloaded {} existing ES field mappings for index {}", cachedFields.size(), indexName);
            }
        } catch (Exception e) {
            LOG.warn("Failed to preload ES mapping cache for {}: {}", indexName, e.getMessage());
        }
    }

    /**
     * Adds a field mapping to an existing ES index so that sort/filter queries work
     * even before any documents containing the field are indexed.
     * Skips the PUT if the field is already known to exist (from cache or prior call).
     */
    private void addESFieldMapping(String indexName, String fieldName, Class<?> propertyClass, boolean isStringField) {
        // Check cache first — avoid redundant PUT requests on restart
        Set<String> cachedFields = esMappingCache.computeIfAbsent(indexName, k -> ConcurrentHashMap.newKeySet());
        if (cachedFields.contains(fieldName)) {
            return;
        }

        try {
            RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
            if (client == null) {
                return;
            }

            String esType = mapPropertyClassToESType(propertyClass, isStringField);
            String mappingBody;
            if ("keyword".equals(esType)) {
                mappingBody = String.format(
                    "{\"properties\":{\"%s\":{\"type\":\"keyword\",\"ignore_above\":5120}}}", fieldName);
            } else {
                mappingBody = String.format(
                    "{\"properties\":{\"%s\":{\"type\":\"%s\"}}}", fieldName, esType);
            }

            Request req = new Request("PUT", "/" + indexName + "/_mapping");
            req.setEntity(new StringEntity(mappingBody, ContentType.APPLICATION_JSON));
            Response resp = client.performRequest(req);

            if (resp.getStatusLine().getStatusCode() >= 200 && resp.getStatusLine().getStatusCode() < 300) {
                cachedFields.add(fieldName);
                LOG.debug("Added ES field mapping: index={}, field={}, type={}", indexName, fieldName, esType);
            }
        } catch (Exception e) {
            // If it fails because the field already exists with a different type, cache it to avoid retrying
            String msg = e.getMessage();
            if (msg != null && msg.contains("mapper_parsing_exception")) {
                cachedFields.add(fieldName);
                LOG.debug("ES field mapping already exists with different type for {}.{}, skipping", indexName, fieldName);
            } else {
                LOG.warn("Failed to add ES field mapping for {}.{}: {}", indexName, fieldName, msg);
            }
        }
    }

    private static String mapPropertyClassToESType(Class<?> clazz, boolean isStringField) {
        if (clazz == null || clazz == String.class) {
            return "keyword";
        } else if (clazz == Integer.class || clazz == int.class) {
            return "integer";
        } else if (clazz == Long.class || clazz == long.class) {
            return "long";
        } else if (clazz == Float.class || clazz == float.class) {
            return "float";
        } else if (clazz == Double.class || clazz == double.class) {
            return "double";
        } else if (clazz == Boolean.class || clazz == boolean.class) {
            return "boolean";
        } else {
            return "keyword";
        }
    }

    private void createESIndex(String indexName) {
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
            } else {
                LOG.warn("Failed to create ES index {}: status={}", indexName, status);
            }
        } catch (Exception e) {
            LOG.warn("Failed to create ES index {}: {}", indexName, e.getMessage());
        }
    }
}
