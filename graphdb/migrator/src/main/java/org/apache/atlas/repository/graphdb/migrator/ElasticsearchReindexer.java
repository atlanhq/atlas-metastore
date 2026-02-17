package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Re-indexes all migrated vertices into Elasticsearch.
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
    private final RestHighLevelClient esClient;

    public ElasticsearchReindexer(MigratorConfig config, MigrationMetrics metrics, CqlSession targetSession) {
        this.config        = config;
        this.metrics       = metrics;
        this.targetSession = targetSession;
        this.esClient      = createEsClient();
    }

    private RestHighLevelClient createEsClient() {
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

        return new RestHighLevelClient(builder);
    }

    /**
     * Re-index all vertices from the target Cassandra vertices table into Elasticsearch.
     */
    public void reindexAll() throws IOException {
        String ks = config.getTargetCassandraKeyspace();
        String esIndex = config.getTargetEsIndex();
        int bulkSize = config.getEsBulkSize();

        LOG.info("Starting ES re-indexing from {}.vertices to ES index '{}'", ks, esIndex);

        ResultSet rs = targetSession.execute(
            "SELECT vertex_id, properties, type_name, state FROM " + ks + ".vertices");

        BulkRequest bulkRequest = new BulkRequest();
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
                Map<String, Object> doc = MAPPER.readValue(propsJson, Map.class);

                // Ensure key fields are present at top level for ES queries
                if (typeName != null) doc.putIfAbsent("__typeName", typeName);
                if (state != null)    doc.putIfAbsent("__state", state);

                String docJson = MAPPER.writeValueAsString(doc);

                bulkRequest.add(new IndexRequest(esIndex)
                    .id(vertexId)
                    .source(docJson, XContentType.JSON));

                batchCount++;
                totalDocs++;

                if (batchCount >= bulkSize) {
                    executeBulk(bulkRequest, totalDocs);
                    bulkRequest = new BulkRequest();
                    batchCount = 0;
                }
            } catch (Exception e) {
                LOG.warn("Failed to index vertex {} to ES", vertexId, e);
            }
        }

        // Flush remaining
        if (batchCount > 0) {
            executeBulk(bulkRequest, totalDocs);
        }

        LOG.info("ES re-indexing complete: {} documents indexed", totalDocs);
    }

    private void executeBulk(BulkRequest request, long totalSoFar) throws IOException {
        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        int indexed = request.numberOfActions();
        metrics.incrEsDocsIndexed(indexed);

        if (response.hasFailures()) {
            LOG.warn("ES bulk had failures: {}", response.buildFailureMessage());
        }

        if (totalSoFar % 10000 == 0) {
            LOG.info("ES re-indexing progress: {} documents", totalSoFar);
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
