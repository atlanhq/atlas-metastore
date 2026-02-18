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
import java.util.Map;

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
        String ks = config.getTargetCassandraKeyspace();
        String esIndex = config.getTargetEsIndex();
        int bulkSize = config.getEsBulkSize();

        LOG.info("Starting ES re-indexing from {}.vertices to ES index '{}'", ks, esIndex);

        // Ensure the index exists
        ensureIndexExists(esIndex);

        ResultSet rs = targetSession.execute(
            "SELECT vertex_id, properties, type_name, state FROM " + ks + ".vertices");

        StringBuilder bulkBody = new StringBuilder();
        int batchCount = 0;
        long totalDocs = 0;

        for (Row row : rs) {
            String vertexId = row.getString("vertex_id");
            String propsJson = row.getString("properties");
            String typeName = row.getString("type_name");
            String state = row.getString("state");

            if (propsJson == null || propsJson.equals("{}")) {
                continue;
            }

            // Build ES document: merge properties with top-level vertex metadata
            try {
                Map<String, Object> rawDoc = MAPPER.readValue(propsJson, Map.class);

                // Sanitize field names: ES 7.x interprets dots in JSON keys as
                // nested object paths. TypeDef vertices have properties like
                // "__type.DbtTest.dbtTestStatus" alongside "__type": "typeSystem",
                // which causes a mapping conflict (text vs object).
                // Replace dots with underscores in field names to avoid this.
                Map<String, Object> doc = new java.util.LinkedHashMap<>(rawDoc.size());
                for (Map.Entry<String, Object> entry : rawDoc.entrySet()) {
                    String key = entry.getKey();
                    if (key.contains(".")) {
                        key = key.replace('.', '_');
                    }
                    doc.put(key, entry.getValue());
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

        LOG.info("ES re-indexing complete: {} documents indexed", String.format("%,d", totalDocs));
    }

    private void ensureIndexExists(String esIndex) {
        try {
            Response response = esClient.performRequest(new Request("HEAD", "/" + esIndex));
            if (response.getStatusLine().getStatusCode() == 200) {
                LOG.info("ES index '{}' already exists", esIndex);
                return;
            }
        } catch (IOException e) {
            // Index doesn't exist, create it
        }

        try {
            Request createReq = new Request("PUT", "/" + esIndex);
            createReq.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
            esClient.performRequest(createReq);
            LOG.info("Created ES index '{}'", esIndex);
        } catch (IOException e) {
            LOG.warn("Failed to create ES index '{}' (may already exist): {}", esIndex, e.getMessage());
        }
    }

    private void executeBulk(String bulkBody, int docCount, long totalSoFar) throws IOException {
        long bulkStart = System.currentTimeMillis();

        Request request = new Request("POST", "/_bulk");
        request.setJsonEntity(bulkBody);
        Response response = esClient.performRequest(request);
        long bulkMs = System.currentTimeMillis() - bulkStart;

        metrics.incrEsDocsIndexed(docCount);

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode >= 300) {
            String body = EntityUtils.toString(response.getEntity());
            LOG.warn("ES bulk response status {}: {}", statusCode, body.substring(0, Math.min(500, body.length())));
        } else {
            // Check for per-item errors in the response
            String body = EntityUtils.toString(response.getEntity());
            if (body.contains("\"errors\":true")) {
                LOG.warn("ES bulk had item-level errors (total: {}): {}...",
                         String.format("%,d", totalSoFar),
                         body.substring(0, Math.min(500, body.length())));
            }
        }

        LOG.info("ES bulk indexed {} docs in {}ms (total: {}, rate: {}/s)",
                 docCount, bulkMs, String.format("%,d", totalSoFar),
                 bulkMs > 0 ? (docCount * 1000L / bulkMs) : "N/A");
    }

    /** Escape special JSON characters in a string value */
    private static String escapeJson(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
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
