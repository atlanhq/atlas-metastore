package org.apache.atlas.repository.graphdb.migrator;

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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Indexes vertices into Elasticsearch in parallel with Cassandra writes during Phase 1.
 *
 * Instead of waiting for Phase 2 to re-read vertices from Cassandra and index them,
 * this class accepts ES documents directly from the writer pipeline and bulk-indexes
 * them on a dedicated background thread.
 *
 * Thread-safe: CassandraTargetWriter threads call {@link #enqueue} concurrently;
 * a single background thread drains the queue and fires bulk requests.
 */
public class ParallelEsIndexer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelEsIndexer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MigratorConfig config;
    private final MigrationMetrics metrics;
    private final RestClient esClient;
    private final String esIndex;
    private final int bulkSize;

    private final BlockingQueue<EsDoc> queue;
    private final Thread indexerThread;
    private volatile boolean producerDone = false;

    private final AtomicLong totalIndexed = new AtomicLong(0);
    private final AtomicLong totalSkipped = new AtomicLong(0);
    private final AtomicLong totalErrors  = new AtomicLong(0);

    /** Lightweight holder for an ES document to be indexed. */
    static class EsDoc {
        final String vertexId;
        final String propsJson;
        final String typeName;
        final String state;

        EsDoc(String vertexId, String propsJson, String typeName, String state) {
            this.vertexId  = vertexId;
            this.propsJson = propsJson;
            this.typeName  = typeName;
            this.state     = state;
        }
    }

    public ParallelEsIndexer(MigratorConfig config, MigrationMetrics metrics) {
        this.config   = config;
        this.metrics  = metrics;
        this.esClient = createEsClient();
        this.esIndex  = config.getTargetEsIndex();
        this.bulkSize = config.getEsBulkSize();
        this.queue    = new LinkedBlockingQueue<>(config.getQueueCapacity());

        this.indexerThread = new Thread(this::indexerLoop, "es-parallel-indexer");
        this.indexerThread.setDaemon(true);
    }

    /**
     * Ensure the target ES index exists (create with source mappings if needed).
     * Must be called before {@link #start()}.
     * Delegates to ElasticsearchReindexer's static-friendly logic.
     */
    public void ensureIndex(ElasticsearchReindexer reindexer) {
        // Reuse the existing ensureIndexExists logic from ElasticsearchReindexer
        reindexer.ensureIndexExists(esIndex);
    }

    public void start() {
        LOG.info("Starting parallel ES indexer (queue capacity: {}, bulk size: {})", config.getQueueCapacity(), bulkSize);
        indexerThread.start();
    }

    /**
     * Called by writer threads to enqueue a vertex for ES indexing.
     * Drops the document if the queue is full (ES indexing should not block Cassandra writes).
     */
    public void enqueue(String vertexId, String propsJson, String typeName, String state) {
        if (propsJson == null || "{}".equals(propsJson)) return;

        // Skip non-entity vertices (same filter as ElasticsearchReindexer.reindexAll)
        if (typeName == null || typeName.isEmpty()) {
            try {
                Map<String, Object> checkProps = MAPPER.readValue(propsJson, Map.class);
                if (checkProps.get("__type") == null) {
                    totalSkipped.incrementAndGet();
                    return;
                }
            } catch (Exception e) {
                totalSkipped.incrementAndGet();
                return;
            }
        }

        boolean offered = queue.offer(new EsDoc(vertexId, propsJson, typeName, state));
        if (!offered) {
            // Queue full — drop rather than block the Cassandra write path
            long dropped = totalErrors.incrementAndGet();
            if (dropped <= 10 || dropped % 10000 == 0) {
                LOG.warn("ES parallel indexer queue full, dropped doc for vertex {} (total dropped: {})", vertexId, dropped);
            }
        }
    }

    /** Signal that no more documents will be produced. */
    public void signalComplete() {
        this.producerDone = true;
    }

    /** Wait for the indexer thread to drain the queue and finish. */
    public void awaitCompletion() throws InterruptedException {
        indexerThread.join(TimeUnit.HOURS.toMillis(24));
        LOG.info("Parallel ES indexer complete: indexed={}, skipped={}, dropped/errors={}",
                 String.format("%,d", totalIndexed.get()),
                 String.format("%,d", totalSkipped.get()),
                 String.format("%,d", totalErrors.get()));
    }

    private void indexerLoop() {
        StringBuilder bulkBody = new StringBuilder();
        int batchCount = 0;

        while (true) {
            EsDoc doc;
            try {
                if (producerDone && queue.isEmpty()) break;
                doc = queue.poll(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            if (doc == null) {
                // Flush partial batch on idle
                if (batchCount > 0) {
                    flushBulk(bulkBody, batchCount);
                    bulkBody.setLength(0);
                    batchCount = 0;
                }
                if (producerDone && queue.isEmpty()) break;
                continue;
            }

            try {
                String docJson = buildEsDocJson(doc);
                if (docJson == null) {
                    totalSkipped.incrementAndGet();
                    continue;
                }

                bulkBody.append("{\"index\":{\"_index\":\"").append(esIndex)
                        .append("\",\"_id\":\"").append(escapeJson(doc.vertexId)).append("\"}}\n");
                bulkBody.append(docJson).append("\n");
                batchCount++;

                if (batchCount >= bulkSize) {
                    flushBulk(bulkBody, batchCount);
                    bulkBody.setLength(0);
                    batchCount = 0;
                }
            } catch (Exception e) {
                LOG.warn("Failed to build ES doc for vertex {}", doc.vertexId, e);
                totalErrors.incrementAndGet();
            }
        }

        // Final flush
        if (batchCount > 0) {
            flushBulk(bulkBody, batchCount);
        }
    }

    /**
     * Build the ES document JSON from a decoded vertex's properties.
     * Applies the same sanitization as ElasticsearchReindexer:
     * - Replaces dots in field names with underscores (ES 7.x nested path conflict)
     * - Ensures __typeName and __state are present at top level
     */
    @SuppressWarnings("unchecked")
    private String buildEsDocJson(EsDoc doc) {
        try {
            Map<String, Object> rawDoc = MAPPER.readValue(doc.propsJson, Map.class);

            Map<String, Object> sanitized = new LinkedHashMap<>(rawDoc.size());
            for (Map.Entry<String, Object> entry : rawDoc.entrySet()) {
                String key = entry.getKey();
                if (key.contains(".")) {
                    key = key.replace('.', '_');
                }
                sanitized.put(key, entry.getValue());
            }

            if (doc.typeName != null) sanitized.putIfAbsent("__typeName", doc.typeName);
            if (doc.state != null)    sanitized.putIfAbsent("__state", doc.state);

            return MAPPER.writeValueAsString(sanitized);
        } catch (Exception e) {
            return null;
        }
    }

    private void flushBulk(StringBuilder bulkBody, int docCount) {
        try {
            long start = System.currentTimeMillis();
            Request request = new Request("POST", "/_bulk");
            request.setJsonEntity(bulkBody.toString());
            Response response = esClient.performRequest(request);
            long ms = System.currentTimeMillis() - start;

            totalIndexed.addAndGet(docCount);
            metrics.incrEsDocsIndexed(docCount);

            int statusCode = response.getStatusLine().getStatusCode();
            String body = EntityUtils.toString(response.getEntity());
            if (statusCode >= 300 || body.contains("\"errors\":true")) {
                LOG.warn("ES parallel bulk had errors ({} docs, {}ms): {}...",
                         docCount, ms, body.substring(0, Math.min(500, body.length())));
            } else {
                LOG.debug("ES parallel bulk: {} docs in {}ms (total: {})",
                          docCount, ms, String.format("%,d", totalIndexed.get()));
            }
        } catch (IOException e) {
            totalErrors.addAndGet(docCount);
            LOG.error("ES parallel bulk request failed ({} docs): {}", docCount, e.getMessage());
        }
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

        builder.setRequestConfigCallback(b -> b
            .setConnectTimeout(30_000)
            .setSocketTimeout(120_000));

        return builder.build();
    }

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
