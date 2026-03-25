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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
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

    /**
     * Minimum positive normal float required by ES rank_feature fields.
     * Values <= 0 cause indexing failures; we clamp them to this floor.
     */
    static final float RANK_FEATURE_MIN = Float.MIN_NORMAL; // 1.17549435E-38

    private final MigratorConfig config;
    private final MigrationMetrics metrics;
    private final RestClient esClient;
    private final String esIndex;
    private final int bulkSize;

    /** Field names that have a rank_feature sub-field in the ES mapping. Discovered once at startup. */
    private Set<String> rankFeatureFields = Collections.emptySet();

    private final boolean isEdgeIndexer;
    private final BlockingQueue<EsDoc> queue;
    private final Thread indexerThread;
    private volatile boolean producerDone = false;

    private final AtomicLong totalIndexed = new AtomicLong(0);
    private final AtomicLong totalSkipped = new AtomicLong(0);
    private final AtomicLong totalErrors  = new AtomicLong(0);
    private final AtomicLong totalRankFeatureNormalized = new AtomicLong(0);

    /** Lightweight holder for an ES document to be indexed. */
    static class EsDoc {
        final String docId;
        final String propsJson;
        final String typeName;
        final String state;
        final boolean isEdge;

        EsDoc(String docId, String propsJson, String typeName, String state) {
            this.docId     = docId;
            this.propsJson = propsJson;
            this.typeName  = typeName;
            this.state     = state;
            this.isEdge    = false;
        }

        /** Edge doc constructor — propsJson is already the complete ES doc JSON. */
        EsDoc(String docId, String propsJson, boolean isEdge) {
            this.docId     = docId;
            this.propsJson = propsJson;
            this.typeName  = null;
            this.state     = null;
            this.isEdge    = isEdge;
        }
    }

    public ParallelEsIndexer(MigratorConfig config, MigrationMetrics metrics) {
        this(config, metrics, config.getTargetEsIndex(), "es-parallel-indexer", false);
    }

    /**
     * Constructor with custom ES index name and thread name.
     * Used for the edge ES indexer which targets a different index.
     */
    public ParallelEsIndexer(MigratorConfig config, MigrationMetrics metrics,
                              String esIndexName, String threadName, boolean isEdgeIndexer) {
        this.config   = config;
        this.metrics  = metrics;
        this.esClient = createEsClient();
        this.esIndex  = esIndexName;
        this.bulkSize = config.getEsBulkSize();
        this.isEdgeIndexer = isEdgeIndexer;
        this.queue    = new LinkedBlockingQueue<>(config.getQueueCapacity());

        this.indexerThread = new Thread(this::indexerLoop, threadName);
        this.indexerThread.setDaemon(true);
    }

    /**
     * Ensure the target ES index exists (create with source mappings if needed),
     * then discover rank_feature fields from the mapping.
     * Must be called before {@link #start()}.
     */
    public void ensureIndex(ElasticsearchReindexer reindexer) {
        reindexer.ensureIndexExists(esIndex);
        this.rankFeatureFields = discoverRankFeatureFields(esIndex);
    }

    public void start() {
        LOG.info("Starting parallel ES indexer (queue capacity: {}, bulk size: {}, rank_feature fields: {})",
                 config.getQueueCapacity(), bulkSize, rankFeatureFields);
        indexerThread.start();
    }

    /** Expose discovered rank_feature fields (for ElasticsearchReindexer Phase 2 path). */
    Set<String> getRankFeatureFields() {
        return rankFeatureFields;
    }

    /**
     * Called by writer threads to enqueue a vertex for ES indexing.
     * Blocks if the queue is full (backpressure — ensures zero doc drops).
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

        try {
            queue.put(new EsDoc(vertexId, propsJson, typeName, state));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            totalErrors.incrementAndGet();
            LOG.warn("Interrupted while enqueuing ES doc for vertex {}", vertexId);
        }
    }

    /**
     * Called by writer threads to enqueue an edge for ES edge indexing.
     * The edgeDocJson is already a complete ES document JSON (built by CassandraTargetWriter).
     */
    public void enqueueEdge(String edgeId, String edgeDocJson) {
        if (edgeDocJson == null || edgeDocJson.isEmpty()) return;

        try {
            queue.put(new EsDoc(edgeId, edgeDocJson, true));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            totalErrors.incrementAndGet();
            LOG.warn("Interrupted while enqueuing ES edge doc for edge {}", edgeId);
        }
    }

    /** Signal that no more documents will be produced. */
    public void signalComplete() {
        this.producerDone = true;
    }

    /** Wait for the indexer thread to drain the queue and finish. */
    public void awaitCompletion() throws InterruptedException {
        indexerThread.join(TimeUnit.HOURS.toMillis(24));
        LOG.info("Parallel ES indexer complete: indexed={}, skipped={}, dropped/errors={}, rank_feature_normalized={}",
                 String.format("%,d", totalIndexed.get()),
                 String.format("%,d", totalSkipped.get()),
                 String.format("%,d", totalErrors.get()),
                 String.format("%,d", totalRankFeatureNormalized.get()));
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
                String docJson;
                if (doc.isEdge) {
                    // Edge doc — already has complete JSON
                    docJson = doc.propsJson;
                } else {
                    docJson = buildEsDocJson(doc);
                }
                if (docJson == null) {
                    totalSkipped.incrementAndGet();
                    continue;
                }

                bulkBody.append("{\"index\":{\"_index\":\"").append(esIndex)
                        .append("\",\"_id\":\"").append(escapeJson(doc.docId)).append("\"}}\n");
                bulkBody.append(docJson).append("\n");
                batchCount++;

                if (batchCount >= bulkSize) {
                    flushBulk(bulkBody, batchCount);
                    bulkBody.setLength(0);
                    batchCount = 0;
                }
            } catch (Exception e) {
                LOG.warn("Failed to build ES doc for {}", doc.docId, e);
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
     * Applies sanitization:
     * - Replaces dots in field names with underscores (ES 7.x nested path conflict)
     * - Clamps rank_feature fields with value <= 0 to Float.MIN_NORMAL
     * - Ensures __typeName and __state are present at top level
     */
    @SuppressWarnings("unchecked")
    private String buildEsDocJson(EsDoc doc) {
        try {
            Map<String, Object> rawDoc = MAPPER.readValue(doc.propsJson, Map.class);

            Map<String, Object> sanitized = new LinkedHashMap<>(rawDoc.size());
            for (Map.Entry<String, Object> entry : rawDoc.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (key.contains(".")) {
                    key = key.replace('.', '_');
                }
                // Clamp rank_feature fields: ES rejects values <= 0
                if (value instanceof Number && rankFeatureFields.contains(key)) {
                    float fv = ((Number) value).floatValue();
                    if (fv < RANK_FEATURE_MIN) {
                        value = RANK_FEATURE_MIN;
                        totalRankFeatureNormalized.incrementAndGet();
                    }
                }
                sanitized.put(key, value);
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
            if (isEdgeIndexer) {
                metrics.incrEsEdgeDocsIndexed(docCount);
            } else {
                metrics.incrEsDocsIndexed(docCount);
            }

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

    /**
     * Query the ES index mapping and discover fields that have a rank_feature type or sub-field.
     * These are multi-field mappings like:
     * <pre>
     *   "viewScore": { "type": "float", "fields": { "rank_feature": { "type": "rank_feature" } } }
     * </pre>
     * Returns field names (e.g. "viewScore") whose rank_feature sub-field rejects values <= 0.
     */
    @SuppressWarnings("unchecked")
    private Set<String> discoverRankFeatureFields(String index) {
        Set<String> fields = new HashSet<>();
        try {
            Response resp = esClient.performRequest(new Request("GET", "/" + index + "/_mapping"));
            if (resp.getStatusLine().getStatusCode() != 200) return fields;

            String body = EntityUtils.toString(resp.getEntity());
            Map<String, Object> parsed = MAPPER.readValue(body, Map.class);
            // { "indexName": { "mappings": { "properties": { ... } } } }
            Map<String, Object> indexData = (Map<String, Object>) parsed.values().iterator().next();
            Map<String, Object> mappings  = (Map<String, Object>) indexData.get("mappings");
            if (mappings == null) return fields;
            Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
            if (properties == null) return fields;

            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                if (!(entry.getValue() instanceof Map)) continue;
                Map<String, Object> fieldDef = (Map<String, Object>) entry.getValue();

                // Check top-level type
                if ("rank_feature".equals(fieldDef.get("type"))) {
                    fields.add(entry.getKey());
                    continue;
                }
                // Check sub-fields (multi-field mapping)
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
            LOG.warn("Failed to discover rank_feature fields from ES mapping (non-fatal): {}", e.getMessage());
        }
        return Collections.unmodifiableSet(fields);
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
