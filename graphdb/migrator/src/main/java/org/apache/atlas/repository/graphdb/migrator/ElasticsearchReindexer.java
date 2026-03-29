package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Re-indexes all migrated vertices into Elasticsearch.
 *
 * Uses the ES low-level REST client (no Lucene dependency) to avoid
 * version conflicts with JanusGraph's bundled Lucene in the fat jar.
 *
 * Reads from the target Cassandra vertices table and bulk-indexes into ES
 * using vertex_id as the ES document ID.
 */
public class ElasticsearchReindexer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchReindexer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MigratorConfig   config;
    private final MigrationMetrics metrics;
    private final CqlSession       targetSession;
    private final RestClient       esClient;

    public ElasticsearchReindexer(MigratorConfig config, MigrationMetrics metrics, CqlSession targetSession) {
        this.config        = config;
        this.metrics       = metrics;
        this.targetSession = targetSession;
        this.esClient      = createEsClient();
    }

    private RestClient createEsClient() {
        RestClientBuilder builder = RestClient.builder(
            new HttpHost(config.getTargetEsHostname(), config.getTargetEsPort(), config.getTargetEsProtocol()));

        String username = config.getTargetEsUsername();
        String password = config.getTargetEsPassword();
        if (username != null && !username.isEmpty()) {
            BasicCredentialsProvider creds = new BasicCredentialsProvider();
            creds.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(b -> b.setDefaultCredentialsProvider(creds));
        }

        // Increase timeouts for bulk operations
        builder.setRequestConfigCallback(b -> b
            .setConnectTimeout(30_000)
            .setSocketTimeout(120_000));

        return builder.build();
    }

    /**
     * Re-index all vertices from the target Cassandra vertices table into Elasticsearch.
     */
    public void reindexAll() throws IOException {
        reindexAll(Collections.emptySet());
    }

    /**
     * Re-index all vertices, normalizing rank_feature fields using the provided set.
     * If the set is empty, rank_feature fields are auto-discovered from the ES mapping.
     */
    public void reindexAll(Set<String> knownRankFeatureFields) throws IOException {
        String ks = config.getTargetCassandraKeyspace();
        String esIndex = config.getTargetEsIndex();
        int bulkSize = config.getEsBulkSize();

        LOG.info("Starting ES re-indexing from {}.vertices to ES index '{}'", ks, esIndex);

        // Ensure the index exists
        ensureIndexExists(esIndex);

        // Discover rank_feature fields if not provided
        Set<String> rfFields = (knownRankFeatureFields != null && !knownRankFeatureFields.isEmpty())
                ? knownRankFeatureFields
                : discoverRankFeatureFields(esIndex);
        long rankFeatureNormalized = 0;

        ResultSet rs = targetSession.execute(
            "SELECT vertex_id, properties, type_name, state FROM " + ks + ".vertices");

        StringBuilder bulkBody = new StringBuilder();
        int batchCount = 0;
        long totalDocs = 0;

        long skipped = 0;

        for (Row row : rs) {
            String vertexId = row.getString("vertex_id");
            String propsJson = row.getString("properties");
            String typeName = row.getString("type_name");
            String state = row.getString("state");

            if (propsJson == null || propsJson.equals("{}")) {
                continue;
            }

            // Skip non-entity vertices: system vertices (patches, index recovery, etc.)
            // that lack both __typeName and __type should NOT be indexed in ES.
            // They pollute search results — the UI can't render them as assets.
            if (typeName == null || typeName.isEmpty()) {
                // Check if it's a type definition vertex (__type = "typeSystem")
                try {
                    Map<String, Object> checkProps = MAPPER.readValue(propsJson, Map.class);
                    Object vertexType = checkProps.get("__type");
                    if (vertexType == null) {
                        skipped++;
                        continue;
                    }
                } catch (Exception e) {
                    skipped++;
                    continue;
                }
            }

            // Build ES document: merge properties with top-level vertex metadata
            try {
                Map<String, Object> rawDoc = MAPPER.readValue(propsJson, Map.class);

                // Sanitize field names: ES 7.x interprets dots in JSON keys as
                // nested object paths. Replace dots with underscores.
                // Also clamp rank_feature fields with value <= 0 to Float.MIN_NORMAL.
                Map<String, Object> doc = new java.util.LinkedHashMap<>(rawDoc.size());
                for (Map.Entry<String, Object> entry : rawDoc.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    if (key.contains(".")) {
                        key = key.replace('.', '_');
                    }
                    if (value instanceof Number && rfFields.contains(key)) {
                        float fv = ((Number) value).floatValue();
                        if (fv < ParallelEsIndexer.RANK_FEATURE_MIN) {
                            value = ParallelEsIndexer.RANK_FEATURE_MIN;
                            rankFeatureNormalized++;
                        }
                    }
                    doc.put(key, value);
                }

                // Ensure key fields are present at top level for ES queries
                if (typeName != null) doc.putIfAbsent("__typeName", typeName);
                if (state != null)    doc.putIfAbsent("__state", state);

                String docJson = MAPPER.writeValueAsString(doc);

                // NDJSON bulk format: action line + doc line
                bulkBody.append("{\"index\":{\"_index\":\"").append(esIndex)
                        .append("\",\"_id\":\"").append(escapeJson(vertexId)).append("\"}}\n");
                bulkBody.append(docJson).append("\n");

                batchCount++;
                totalDocs++;

                if (batchCount >= bulkSize) {
                    executeBulk(bulkBody.toString(), batchCount, totalDocs);
                    bulkBody.setLength(0);
                    batchCount = 0;
                }
            } catch (Exception e) {
                LOG.warn("Failed to index vertex {} to ES", vertexId, e);
            }
        }

        // Flush remaining
        if (batchCount > 0) {
            executeBulk(bulkBody.toString(), batchCount, totalDocs);
        }

        LOG.info("ES re-indexing complete: {} docs submitted, {} succeeded, {} failed, {} skipped, " +
                 "{} rank_feature values normalized",
                 String.format("%,d", totalDocs),
                 String.format("%,d", totalBulkItemSuccesses),
                 String.format("%,d", totalBulkItemFailures),
                 String.format("%,d", skipped),
                 String.format("%,d", rankFeatureNormalized));

        // Post-reindex verification: compare ES _count against Cassandra-sourced count
        if (totalBulkItemFailures > 0) {
            LOG.warn("ES REINDEX WARNING: {} item-level failures detected during bulk indexing. " +
                     "Some documents may not be searchable.", totalBulkItemFailures);
        }
        verifyEsDocCount(esIndex, totalBulkItemSuccesses);
    }

    void ensureIndexExists(String esIndex) {
        try {
            Response response = esClient.performRequest(new Request("HEAD", "/" + esIndex));
            if (response.getStatusLine().getStatusCode() == 200) {
                LOG.info("ES index '{}' already exists", esIndex);
                return;
            }
        } catch (IOException e) {
            // Index doesn't exist, create it below
        }

        // Try to copy mappings + settings from the source JanusGraph index (migrated tenant).
        // If the source index doesn't exist (fresh tenant), fall back to empty body
        // so the ES index template (atlan-template) applies its defaults.
        String sourceIndex = config.getSourceEsIndex();
        String createBody = getCreateBodyFromSourceIndex(sourceIndex);

        // Ensure field limit is set in the create body (use max of configured default and source index limit)
        createBody = ensureFieldLimit(createBody, resolveFieldLimit(config.getSourceEsIndex()));

        try {
            Request createReq = new Request("PUT", "/" + esIndex);
            createReq.setJsonEntity(createBody);
            esClient.performRequest(createReq);

            if ("{}".equals(createBody)) {
                LOG.info("Created ES index '{}' (fresh tenant — settings from ES index template)", esIndex);
            } else {
                LOG.info("Created ES index '{}' with mappings+settings copied from source index '{}'",
                         esIndex, sourceIndex);
            }
        } catch (IOException e) {
            LOG.warn("Failed to create ES index '{}' (may already exist): {}", esIndex, e.getMessage());
        }
    }

    /**
     * Default mappings applied when the source index doesn't exist (e.g., deleted during
     * remigration cleanup). Maps all strings to keyword to match the Atlas ES schema
     * (addons/elasticsearch/es-mappings.json). Without this, ES dynamic mapping creates
     * text fields which break sort/aggregation queries on __guid, __typeName, etc.
     */
    private static final String DEFAULT_MAPPINGS_JSON =
        "{\"properties\":{\"relationshipList\":{\"type\":\"nested\",\"properties\":" +
        "{\"typeName\":{\"type\":\"keyword\"},\"guid\":{\"type\":\"keyword\"}," +
        "\"provenanceType\":{\"type\":\"integer\"},\"endName\":{\"type\":\"keyword\"}," +
        "\"endGuid\":{\"type\":\"keyword\"},\"endTypeName\":{\"type\":\"keyword\"}," +
        "\"endQualifiedName\":{\"type\":\"keyword\"},\"label\":{\"type\":\"keyword\"}," +
        "\"propagateTags\":{\"type\":\"keyword\"},\"status\":{\"type\":\"keyword\"}," +
        "\"createdBy\":{\"type\":\"keyword\"},\"updatedBy\":{\"type\":\"keyword\"}," +
        "\"createTime\":{\"type\":\"long\"},\"updateTime\":{\"type\":\"long\"}," +
        "\"version\":{\"type\":\"long\"}}}}," +
        "\"dynamic_templates\":[{\"custom_metadata_strings\":" +
        "{\"match_mapping_type\":\"string\",\"mapping\":{\"type\":\"keyword\",\"ignore_above\":5120}}}]}";

    /**
     * Reads mappings and settings from the source JanusGraph ES index and returns
     * a JSON body suitable for PUT /{index} to create the target index with
     * identical field mappings and analyzer settings.
     *
     * Falls back to default Atlas mappings (keyword for strings) if the source
     * index doesn't exist (e.g., deleted during remigration cleanup).
     */
    private String getCreateBodyFromSourceIndex(String sourceIndex) {
        if (sourceIndex == null || sourceIndex.isEmpty()) {
            LOG.info("No source ES index configured, using default Atlas mappings");
            return "{\"mappings\":" + DEFAULT_MAPPINGS_JSON + "}";
        }

        try {
            // Check if source index exists
            Response headResp = esClient.performRequest(new Request("HEAD", "/" + sourceIndex));
            if (headResp.getStatusLine().getStatusCode() != 200) {
                LOG.info("Source ES index '{}' does not exist, using default Atlas mappings", sourceIndex);
                return "{\"mappings\":" + DEFAULT_MAPPINGS_JSON + "}";
            }
        } catch (IOException e) {
            LOG.info("Source ES index '{}' not found, using default Atlas mappings", sourceIndex);
            return "{\"mappings\":" + DEFAULT_MAPPINGS_JSON + "}";
        }

        try {
            // Read mappings from source index
            String mappingsJson = "{}";
            Response mappingResp = esClient.performRequest(new Request("GET", "/" + sourceIndex + "/_mapping"));
            if (mappingResp.getStatusLine().getStatusCode() == 200) {
                String body = EntityUtils.toString(mappingResp.getEntity());
                Map<String, Object> parsed = MAPPER.readValue(body, Map.class);
                // Response: { "indexName": { "mappings": { ... } } }
                Map<String, Object> indexData = (Map<String, Object>) parsed.values().iterator().next();
                Object mappings = indexData.get("mappings");
                if (mappings != null) {
                    mappingsJson = MAPPER.writeValueAsString(mappings);
                }
            }

            // Read settings from source index (only the user-defined settings, not ES internals)
            String settingsJson = null;
            Response settingsResp = esClient.performRequest(new Request("GET", "/" + sourceIndex + "/_settings"));
            if (settingsResp.getStatusLine().getStatusCode() == 200) {
                String body = EntityUtils.toString(settingsResp.getEntity());
                Map<String, Object> parsed = MAPPER.readValue(body, Map.class);
                // Response: { "indexName": { "settings": { "index": { ... } } } }
                Map<String, Object> indexData = (Map<String, Object>) parsed.values().iterator().next();
                Map<String, Object> settings = (Map<String, Object>) indexData.get("settings");
                if (settings != null) {
                    Map<String, Object> indexSettings = (Map<String, Object>) settings.get("index");
                    if (indexSettings != null) {
                        // Allowlist: only keep settings that are safe and meaningful for index creation.
                        // ES returns many read-only / auto-generated settings (creation_date, uuid, version,
                        // resize, routing, blocks, etc.) that cause 400 errors if included in PUT /{index}.
                        java.util.Set<String> ALLOWED_SETTINGS = java.util.Set.of(
                            "analysis", "number_of_shards", "number_of_replicas", "refresh_interval",
                            "max_result_window", "mapping", "similarity"
                        );
                        indexSettings.keySet().retainAll(ALLOWED_SETTINGS);
                        settingsJson = MAPPER.writeValueAsString(Map.of("index", indexSettings));
                    }
                }
            }

            // Build the create index body with both mappings and settings
            StringBuilder createBody = new StringBuilder("{");
            if (settingsJson != null) {
                createBody.append("\"settings\":").append(settingsJson);
            }
            if (!"{}".equals(mappingsJson)) {
                if (settingsJson != null) createBody.append(",");
                createBody.append("\"mappings\":").append(mappingsJson);
            }
            createBody.append("}");

            LOG.info("Copied mappings+settings from source index '{}' for target index creation", sourceIndex);
            return createBody.toString();

        } catch (Exception e) {
            LOG.warn("Failed to read mappings/settings from source index '{}', falling back to default Atlas mappings: {}",
                     sourceIndex, e.getMessage());
            return "{\"mappings\":" + DEFAULT_MAPPINGS_JSON + "}";
        }
    }

    /**
     * Queries the source ES index to discover its mapping.total_fields.limit setting.
     * Returns the limit as an int, or -1 if the index doesn't exist or the setting isn't found.
     */
    @SuppressWarnings("unchecked")
    int discoverFieldLimit(String indexName) {
        if (indexName == null || indexName.isEmpty()) {
            return -1;
        }
        try {
            Request req = new Request("GET", "/" + indexName + "/_settings/index.mapping.total_fields.limit");
            Response resp = esClient.performRequest(req);
            if (resp.getStatusLine().getStatusCode() != 200) {
                return -1;
            }
            String body = EntityUtils.toString(resp.getEntity());
            Map<String, Object> parsed = MAPPER.readValue(body, Map.class);
            // Response: { "indexName": { "settings": { "index": { "mapping": { "total_fields": { "limit": "5000" } } } } } }
            Map<String, Object> indexData = (Map<String, Object>) parsed.get(indexName);
            if (indexData == null) return -1;
            Map<String, Object> settings = (Map<String, Object>) indexData.get("settings");
            if (settings == null) return -1;
            Map<String, Object> indexSettings = (Map<String, Object>) settings.get("index");
            if (indexSettings == null) return -1;
            Map<String, Object> mapping = (Map<String, Object>) indexSettings.get("mapping");
            if (mapping == null) return -1;
            Map<String, Object> totalFields = (Map<String, Object>) mapping.get("total_fields");
            if (totalFields == null) return -1;
            Object limitVal = totalFields.get("limit");
            if (limitVal == null) return -1;
            return Integer.parseInt(limitVal.toString().trim());
        } catch (Exception e) {
            LOG.debug("Could not discover field limit from source index '{}': {}", indexName, e.getMessage());
            return -1;
        }
    }

    /**
     * Resolves the effective field limit by taking the max of the configured default
     * and the source index's actual limit. This prevents silent field-limit downgrades
     * when the source index has been expanded beyond the configured default.
     */
    int resolveFieldLimit(String sourceIndexName) {
        int configuredLimit = config.getEsFieldLimit();
        int sourceLimit = discoverFieldLimit(sourceIndexName);
        int effective = Math.max(configuredLimit, sourceLimit);
        LOG.info("Field limit: configured={}, source('{}')={}, effective={}",
                 configuredLimit, sourceIndexName, sourceLimit > 0 ? sourceLimit : "N/A", effective);
        return effective;
    }

    /**
     * Ensures the index creation body includes index.mapping.total_fields.limit.
     * Injects the setting into the "settings" block if not already present.
     */
    @SuppressWarnings("unchecked")
    private String ensureFieldLimit(String createBody, int fieldLimit) {
        if (fieldLimit <= 0) return createBody;

        try {
            Map<String, Object> body = MAPPER.readValue(createBody, Map.class);

            Map<String, Object> settings = (Map<String, Object>) body.get("settings");
            if (settings == null) {
                settings = new java.util.LinkedHashMap<>();
                body.put("settings", settings);
            }

            Map<String, Object> indexSettings = (Map<String, Object>) settings.get("index");
            if (indexSettings == null) {
                indexSettings = new java.util.LinkedHashMap<>();
                settings.put("index", indexSettings);
            }

            // Set mapping.total_fields.limit
            Map<String, Object> mapping = (Map<String, Object>) indexSettings.get("mapping");
            if (mapping == null) {
                mapping = new java.util.LinkedHashMap<>();
                indexSettings.put("mapping", mapping);
            }
            Map<String, Object> totalFields = (Map<String, Object>) mapping.get("total_fields");
            if (totalFields == null) {
                totalFields = new java.util.LinkedHashMap<>();
                mapping.put("total_fields", totalFields);
            }
            totalFields.put("limit", fieldLimit);

            LOG.info("ES index field limit set to: {}", fieldLimit);
            return MAPPER.writeValueAsString(body);
        } catch (Exception e) {
            LOG.warn("Failed to inject field limit into index settings: {}", e.getMessage());
            return createBody;
        }
    }

    // Tracks cumulative bulk indexing failures across all batches
    private long totalBulkItemFailures = 0;
    private long totalBulkItemSuccesses = 0;
    private long totalEdgeBulkItemFailures = 0;
    private long totalEdgeBulkItemSuccesses = 0;

    private void executeBulk(String bulkBody, int docCount, long totalSoFar) throws IOException {
        long bulkStart = System.currentTimeMillis();

        Request request = new Request("POST", "/_bulk");
        request.setJsonEntity(bulkBody);
        Response response = esClient.performRequest(request);
        long bulkMs = System.currentTimeMillis() - bulkStart;

        int statusCode = response.getStatusLine().getStatusCode();
        String body = EntityUtils.toString(response.getEntity());

        if (statusCode >= 300) {
            LOG.warn("ES bulk response status {}: {}", statusCode, body.substring(0, Math.min(500, body.length())));
            totalBulkItemFailures += docCount;
        } else {
            // Parse bulk response to count actual successes vs failures
            int[] counts = parseBulkResponseCounts(body);
            int succeeded = counts[0];
            int failed = counts[1];
            totalBulkItemSuccesses += succeeded;
            totalBulkItemFailures += failed;
            metrics.incrEsDocsIndexed(succeeded);

            if (failed > 0) {
                LOG.warn("ES bulk: {} succeeded, {} failed (batch total: {}, cumulative failures: {}): {}...",
                         succeeded, failed, String.format("%,d", totalSoFar),
                         totalBulkItemFailures,
                         body.substring(0, Math.min(500, body.length())));
            }
        }

        LOG.debug("ES bulk indexed {} docs in {}ms (total: {}, rate: {}/s)",
                  docCount, bulkMs, String.format("%,d", totalSoFar),
                  bulkMs > 0 ? (docCount * 1000L / bulkMs) : "N/A");
    }

    /**
     * Parse a bulk response JSON to count successful vs failed items.
     * Returns [succeeded, failed].
     */
    @SuppressWarnings("unchecked")
    private int[] parseBulkResponseCounts(String body) {
        int succeeded = 0;
        int failed = 0;
        try {
            Map<String, Object> parsed = MAPPER.readValue(body, Map.class);
            Object itemsObj = parsed.get("items");
            if (itemsObj instanceof List) {
                List<Map<String, Object>> items = (List<Map<String, Object>>) itemsObj;
                for (Map<String, Object> item : items) {
                    // Each item is {"index": {"_id": ..., "status": 200/201, ...}} or has "error" key
                    for (Object actionResult : item.values()) {
                        if (actionResult instanceof Map) {
                            Map<String, Object> result = (Map<String, Object>) actionResult;
                            if (result.containsKey("error")) {
                                failed++;
                            } else {
                                succeeded++;
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            // If parsing fails, check the simple "errors" flag
            if (body.contains("\"errors\":true")) {
                // Can't determine exact counts — assume all failed as worst case
                LOG.warn("Failed to parse bulk response items: {}", e.getMessage());
            }
        }
        return new int[]{succeeded, failed};
    }

    /**
     * Post-reindex verification: refresh the index and compare ES _count against
     * the expected count from bulk indexing.
     */
    @SuppressWarnings("unchecked")
    private void verifyEsDocCount(String indexName, long expectedCount) {
        try {
            // Force a refresh so _count reflects all indexed docs
            esClient.performRequest(new Request("POST", "/" + indexName + "/_refresh"));

            Request countReq = new Request("GET", "/" + indexName + "/_count");
            Response resp = esClient.performRequest(countReq);
            String body = EntityUtils.toString(resp.getEntity());
            Map<String, Object> parsed = MAPPER.readValue(body, Map.class);
            long esCount = ((Number) parsed.get("count")).longValue();

            if (esCount == expectedCount) {
                LOG.info("ES doc count verification PASSED for '{}': ES={}, expected={}",
                         indexName, String.format("%,d", esCount), String.format("%,d", expectedCount));
            } else {
                long diff = expectedCount - esCount;
                LOG.warn("ES doc count verification MISMATCH for '{}': ES={}, expected={}, diff={} " +
                         "(some documents may have been silently lost during indexing)",
                         indexName, String.format("%,d", esCount), String.format("%,d", expectedCount),
                         String.format("%,d", diff));
            }
        } catch (Exception e) {
            LOG.warn("ES doc count verification failed for '{}': {}", indexName, e.getMessage());
        }
    }

    /** Escape special JSON characters in a string value */
    private static String escapeJson(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Discover fields with rank_feature type or sub-field from the ES index mapping.
     * Shared logic — ParallelEsIndexer has its own copy using its own RestClient.
     */
    @SuppressWarnings("unchecked")
    private Set<String> discoverRankFeatureFields(String index) {
        Set<String> fields = new HashSet<>();
        try {
            Response resp = esClient.performRequest(new Request("GET", "/" + index + "/_mapping"));
            if (resp.getStatusLine().getStatusCode() != 200) return fields;

            String body = EntityUtils.toString(resp.getEntity());
            Map<String, Object> parsed = MAPPER.readValue(body, Map.class);
            Map<String, Object> indexData = (Map<String, Object>) parsed.values().iterator().next();
            Map<String, Object> mappings  = (Map<String, Object>) indexData.get("mappings");
            if (mappings == null) return fields;
            Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
            if (properties == null) return fields;

            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                if (!(entry.getValue() instanceof Map)) continue;
                Map<String, Object> fieldDef = (Map<String, Object>) entry.getValue();
                if ("rank_feature".equals(fieldDef.get("type"))) {
                    fields.add(entry.getKey());
                    continue;
                }
                Map<String, Object> subFields = (Map<String, Object>) fieldDef.get("fields");
                if (subFields != null) {
                    for (Object subFieldDef : subFields.values()) {
                        if (subFieldDef instanceof Map &&
                            "rank_feature".equals(((Map<String, Object>) subFieldDef).get("type"))) {
                            fields.add(entry.getKey());
                            break;
                        }
                    }
                }
            }
            if (!fields.isEmpty()) {
                LOG.info("Discovered {} rank_feature fields from ES mapping: {}", fields.size(), fields);
            }
        } catch (Exception e) {
            LOG.warn("Failed to discover rank_feature fields from ES mapping: {}", e.getMessage());
        }
        return Collections.unmodifiableSet(fields);
    }

    /**
     * Re-index all edges from the target Cassandra edges_by_id table into the ES edge index.
     * This populates atlas_graph_edge_index so that directRelationshipIndexSearch works post-cutover.
     */
    public void reindexEdges() throws IOException {
        String ks = config.getTargetCassandraKeyspace();
        String esEdgeIndex = config.getTargetEsEdgeIndex();
        int bulkSize = config.getEsBulkSize();

        LOG.info("Starting ES edge re-indexing from {}.edges_by_id to ES index '{}'", ks, esEdgeIndex);

        ensureEdgeIndexExists(esEdgeIndex);

        ResultSet rs = targetSession.execute(
            "SELECT edge_id, out_vertex_id, in_vertex_id, edge_label, properties, state FROM " + ks + ".edges_by_id");

        StringBuilder bulkBody = new StringBuilder();
        int batchCount = 0;
        long totalDocs = 0;

        for (Row row : rs) {
            String edgeId      = row.getString("edge_id");
            String outVertexId = row.getString("out_vertex_id");
            String inVertexId  = row.getString("in_vertex_id");
            String edgeLabel   = row.getString("edge_label");
            String propsJson   = row.getString("properties");
            String state       = row.getString("state");

            try {
                // Build ES document: edge properties + metadata
                Map<String, Object> doc;
                if (propsJson != null && !propsJson.equals("{}")) {
                    doc = MAPPER.readValue(propsJson, Map.class);
                    // Sanitize field names (dot→underscore)
                    Map<String, Object> sanitized = new java.util.LinkedHashMap<>(doc.size());
                    for (Map.Entry<String, Object> entry : doc.entrySet()) {
                        String key = entry.getKey();
                        if (key.contains(".")) {
                            key = key.replace('.', '_');
                        }
                        sanitized.put(key, entry.getValue());
                    }
                    doc = sanitized;
                } else {
                    doc = new java.util.LinkedHashMap<>();
                }

                // Add edge metadata fields
                doc.put("__edgeId", edgeId);
                doc.put("__edgeLabel", edgeLabel);
                doc.put("__outVertexId", outVertexId);
                doc.put("__inVertexId", inVertexId);
                doc.putIfAbsent("__state", state != null ? state : "ACTIVE");

                String docJson = MAPPER.writeValueAsString(doc);

                bulkBody.append("{\"index\":{\"_index\":\"").append(esEdgeIndex)
                        .append("\",\"_id\":\"").append(escapeJson(edgeId)).append("\"}}\n");
                bulkBody.append(docJson).append("\n");

                batchCount++;
                totalDocs++;

                if (batchCount >= bulkSize) {
                    executeEdgeBulk(bulkBody.toString(), batchCount, totalDocs);
                    bulkBody.setLength(0);
                    batchCount = 0;
                }
            } catch (Exception e) {
                LOG.warn("Failed to index edge {} to ES", edgeId, e);
            }
        }

        // Flush remaining
        if (batchCount > 0) {
            executeEdgeBulk(bulkBody.toString(), batchCount, totalDocs);
        }

        LOG.info("ES edge re-indexing complete: {} docs submitted, {} succeeded, {} failed into '{}'",
                 String.format("%,d", totalDocs),
                 String.format("%,d", totalEdgeBulkItemSuccesses),
                 String.format("%,d", totalEdgeBulkItemFailures),
                 esEdgeIndex);

        if (totalEdgeBulkItemFailures > 0) {
            LOG.warn("ES EDGE REINDEX WARNING: {} item-level failures detected during bulk indexing.",
                     totalEdgeBulkItemFailures);
        }
        verifyEsDocCount(esEdgeIndex, totalEdgeBulkItemSuccesses);
    }

    /**
     * Ensure the edge ES index exists. Uses default dynamic mappings (keyword for strings)
     * since the edge index schema is simpler than the vertex index.
     */
    void ensureEdgeIndexExists(String esEdgeIndex) {
        try {
            Response response = esClient.performRequest(new Request("HEAD", "/" + esEdgeIndex));
            if (response.getStatusLine().getStatusCode() == 200) {
                LOG.info("ES edge index '{}' already exists", esEdgeIndex);
                return;
            }
        } catch (IOException e) {
            // Index doesn't exist, create it below
        }

        // Edge index uses simple dynamic mappings (keyword for all strings)
        String sourceEdgeIndex = config.getSourceEsEdgeIndex();
        int edgeFieldLimit = (sourceEdgeIndex != null && !sourceEdgeIndex.isEmpty())
            ? resolveFieldLimit(sourceEdgeIndex)
            : config.getEsFieldLimit();
        String createBody = ensureFieldLimit(
            "{\"mappings\":{\"dynamic_templates\":[{\"strings_as_keywords\":" +
            "{\"match_mapping_type\":\"string\",\"mapping\":{\"type\":\"keyword\",\"ignore_above\":5120}}}]}}",
            edgeFieldLimit);

        try {
            Request createReq = new Request("PUT", "/" + esEdgeIndex);
            createReq.setJsonEntity(createBody);
            esClient.performRequest(createReq);
            LOG.info("Created ES edge index '{}'", esEdgeIndex);
        } catch (IOException e) {
            LOG.warn("Failed to create ES edge index '{}' (may already exist): {}", esEdgeIndex, e.getMessage());
        }
    }

    private void executeEdgeBulk(String bulkBody, int docCount, long totalSoFar) throws IOException {
        long bulkStart = System.currentTimeMillis();

        Request request = new Request("POST", "/_bulk");
        request.setJsonEntity(bulkBody);
        Response response = esClient.performRequest(request);
        long bulkMs = System.currentTimeMillis() - bulkStart;

        int statusCode = response.getStatusLine().getStatusCode();
        String body = EntityUtils.toString(response.getEntity());

        if (statusCode >= 300) {
            LOG.warn("ES edge bulk response status {}: {}", statusCode, body.substring(0, Math.min(500, body.length())));
            totalEdgeBulkItemFailures += docCount;
        } else {
            int[] counts = parseBulkResponseCounts(body);
            int succeeded = counts[0];
            int failed = counts[1];
            totalEdgeBulkItemSuccesses += succeeded;
            totalEdgeBulkItemFailures += failed;
            metrics.incrEsEdgeDocsIndexed(succeeded);

            if (failed > 0) {
                LOG.warn("ES edge bulk: {} succeeded, {} failed (batch total: {}, cumulative failures: {}): {}...",
                         succeeded, failed, String.format("%,d", totalSoFar),
                         totalEdgeBulkItemFailures,
                         body.substring(0, Math.min(500, body.length())));
            }
        }

        LOG.debug("ES edge bulk indexed {} docs in {}ms (total: {})",
                  docCount, bulkMs, String.format("%,d", totalSoFar));
    }

    /**
     * Delete the target ES index entirely.
     * Called during --fresh mode to ensure a clean slate before reindexing.
     * If the index doesn't exist, this is a no-op.
     */
    public void deleteIndex() {
        String esIndex = config.getTargetEsIndex();
        try {
            esClient.performRequest(new Request("DELETE", "/" + esIndex));
            LOG.info("Deleted ES index '{}'", esIndex);
        } catch (IOException e) {
            LOG.info("ES index '{}' did not exist or delete failed (non-fatal): {}", esIndex, e.getMessage());
        }
    }

    /**
     * Delete the ES edge index.
     * Called during --fresh mode.
     */
    public void deleteEdgeIndex() {
        String esEdgeIndex = config.getTargetEsEdgeIndex();
        try {
            esClient.performRequest(new Request("DELETE", "/" + esEdgeIndex));
            LOG.info("Deleted ES edge index '{}'", esEdgeIndex);
        } catch (IOException e) {
            LOG.info("ES edge index '{}' did not exist or delete failed (non-fatal): {}", esEdgeIndex, e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            esClient.close();
        } catch (IOException e) {
            LOG.warn("Error closing ES client", e);
        }
    }
}
