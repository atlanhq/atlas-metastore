package org.apache.atlas.repository.store.graph.v2;
import io.micrometer.core.instrument.Timer;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.MapUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.janusgraph.util.StringUtils;
import org.apache.atlas.repository.store.graph.v2.LongEncodingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX_NAME;
import static org.apache.atlas.repository.audit.ESBasedAuditRepository.getHttpHosts;

public class ESConnector implements Closeable {
    private static final Logger LOG      = LoggerFactory.getLogger(ESConnector.class);

    private static RestClient lowLevelClient;

    private static Set<String> DENORM_ATTRS;
    private static String GET_DOCS_BY_ID = VERTEX_INDEX_NAME + "/_mget";

    static {
        try {
            lowLevelClient = initializeClient();
            DENORM_ATTRS = initializeDenormAttributes();
        } catch (AtlasException e) {
            throw new RuntimeException("Failed to initialize ESConnector", e);
        }
    }

    private static RestClient initializeClient() throws AtlasException {
        try {
            List<HttpHost> httpHosts = getHttpHosts();
            RestClientBuilder builder = RestClient.builder(httpHosts.get(0))
                    .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                            .setConnectTimeout(AtlasConfiguration.INDEX_CLIENT_CONNECTION_TIMEOUT.getInt())
                            .setSocketTimeout(AtlasConfiguration.INDEX_CLIENT_SOCKET_TIMEOUT.getInt()));

            return builder.build();
        } catch (Exception e) {
            throw new AtlasException("Failed to initialize Elasticsearch client", e);
        }
    }

    private static Set<String> initializeDenormAttributes() {
        Set<String> attrs = new HashSet<>();
        attrs.add(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY);
        attrs.add(PROPAGATED_CLASSIFICATION_NAMES_KEY);
        attrs.add(CLASSIFICATION_TEXT_KEY);
        attrs.add(TRAIT_NAMES_PROPERTY_KEY);
        attrs.add(CLASSIFICATION_NAMES_KEY);
        return Collections.unmodifiableSet(attrs);
    }

    public static void writeTagProperties(Map<String, Map<String, Object>> entitiesMap) {
        writeTagProperties(entitiesMap, false);
    }

    /**
     * Updates and writes tag properties for multiple entities to Elasticsearch index.
     *
     * This method processes the provided entities map to prepare an Elasticsearch bulk
     * request for updating tag properties and denormalized attributes. The modifications
     * include attributes specified in the {@code DENORM_ATTRS} field and a modification
     * timestamp. The bulk request is then executed using a low-level client.
     *
     * @param entitiesMap A map where the keys represent the entity vertex IDs (as strings),
     *                    and the values are maps containing the attributes to be updated
     *                    for each entity.
     * @param upsert A boolean flag that indicates whether the update operation should upsert
     *               (create new doc if not found) the document in the Elasticsearch index.
     */
    public static void writeTagProperties(Map<String, Map<String, Object>> entitiesMap, boolean upsert) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("writeTagPropertiesES");
        Timer.Sample latencySample = ESConnectorMetrics.startLatencyTimer();

        try {
            if (MapUtils.isEmpty(entitiesMap))
                return;

            ESConnectorMetrics.recordAttempt();
            ESConnectorMetrics.recordBulkDocCount(entitiesMap.size());

            if (!ESCircuitBreaker.allowRequest()) {
                ESConnectorMetrics.recordCircuitBreakerShortCircuit();
                ESConnectorMetrics.recordFailure("circuit_open");
                throw new ESWriteCircuitOpenException(
                        "ES circuit breaker is OPEN — short-circuiting bulk write for " + entitiesMap.size() + " entities");
            }

            // Track docId → vertexId mapping for failure reporting
            Map<String, String> docIdToVertexId = new LinkedHashMap<>();

            StringBuilder bulkRequestBody = new StringBuilder();

            for (String assetVertexId : entitiesMap.keySet()) {
                Map<String, Object> entry = entitiesMap.get(assetVertexId);
                Map<String, Object> toUpdate = new HashMap<>();

                DENORM_ATTRS.stream().filter(entry::containsKey).forEach(x -> toUpdate.put(x, entry.get(x)));

                String docId = LongEncodingUtil.vertexIdToDocId(assetVertexId);
                docIdToVertexId.put(docId, assetVertexId);
                bulkRequestBody.append("{\"update\":{\"_index\":\"" + VERTEX_INDEX_NAME + "\",\"_id\":\"").append(docId).append("\" }}\n");

                bulkRequestBody.append("{");
                String attrsToUpdate = AtlasType.toJson(toUpdate);
                bulkRequestBody.append("\"doc\":").append(attrsToUpdate);

                if (upsert) {
                    bulkRequestBody.append(",\"upsert\":").append(attrsToUpdate);
                }

                bulkRequestBody.append("}\n");
            }

            int maxRetries = AtlasConfiguration.ES_MAX_RETRIES.getInt();

            // Track which doc IDs still need to be retried
            Set<String> pendingDocIds = new LinkedHashSet<>(docIdToVertexId.keySet());
            int totalPermanentlyFailed = 0;

            for (int retryCount = 0; retryCount < maxRetries && !pendingDocIds.isEmpty(); retryCount++) {
                if (retryCount > 0) {
                    ESConnectorMetrics.recordRetry();
                    try {
                        Thread.sleep(computeBackoffMs(retryCount));
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                        ESConnectorMetrics.recordFailure("interrupted");
                        throw new RuntimeException("ES update interrupted during retry delay", interruptedException);
                    }
                }

                // Rebuild bulk body for pending items only (on retry)
                StringBuilder currentBody;
                if (retryCount == 0) {
                    currentBody = bulkRequestBody;
                } else {
                    currentBody = new StringBuilder();
                    for (String assetVertexId : entitiesMap.keySet()) {
                        String docId = LongEncodingUtil.vertexIdToDocId(assetVertexId);
                        if (!pendingDocIds.contains(docId)) continue;

                        Map<String, Object> entry = entitiesMap.get(assetVertexId);
                        Map<String, Object> toUpdate = new HashMap<>();
                        DENORM_ATTRS.stream().filter(entry::containsKey).forEach(x -> toUpdate.put(x, entry.get(x)));

                        currentBody.append("{\"update\":{\"_index\":\"" + VERTEX_INDEX_NAME + "\",\"_id\":\"").append(docId).append("\" }}\n");
                        currentBody.append("{");
                        String attrsToUpdate = AtlasType.toJson(toUpdate);
                        currentBody.append("\"doc\":").append(attrsToUpdate);
                        if (upsert) {
                            currentBody.append(",\"upsert\":").append(attrsToUpdate);
                        }
                        currentBody.append("}\n");
                    }
                }

                Request request = new Request("POST", "/_bulk");
                request.setEntity(new StringEntity(currentBody.toString(), ContentType.APPLICATION_JSON));

                try {
                    Response response = lowLevelClient.performRequest(request);
                    int statusCode = response.getStatusLine().getStatusCode();

                    if (statusCode >= 200 && statusCode < 300) {
                        String responseBody = EntityUtils.toString(response.getEntity());

                        if (responseBody != null && responseBody.contains("\"errors\":true")) {
                            // Parse per-item results to detect partial failures
                            Set<String> retryableDocIds = new LinkedHashSet<>();
                            Set<String> permanentlyFailed = new LinkedHashSet<>();
                            parseBulkResponse(responseBody, retryableDocIds, permanentlyFailed);

                            ESConnectorMetrics.recordPartialFailure(permanentlyFailed.size() + retryableDocIds.size());
                            totalPermanentlyFailed += permanentlyFailed.size();
                            if (!permanentlyFailed.isEmpty()) {
                                ESConnectorMetrics.recordFailure("non_retryable_status");
                                LOG.error("writeTagProperties: {} items permanently failed (4xx): {}",
                                        permanentlyFailed.size(), permanentlyFailed);
                            }

                            // Only retry items with transient failures (5xx/429)
                            pendingDocIds.retainAll(retryableDocIds);

                            if (pendingDocIds.isEmpty()) {
                                if (totalPermanentlyFailed == 0) {
                                    ESCircuitBreaker.recordSuccess();
                                    ESConnectorMetrics.recordSuccess();
                                }
                                return; // All retryable items resolved (some may have permanently failed)
                            }

                            LOG.warn("writeTagProperties: {} items have retryable failures, will retry ({}/{})",
                                    pendingDocIds.size(), retryCount + 1, maxRetries);
                        } else {
                            if (totalPermanentlyFailed == 0) {
                                ESCircuitBreaker.recordSuccess();
                                ESConnectorMetrics.recordSuccess();
                            }
                            return; // All items succeeded
                        }
                    } else if (statusCode >= 500) {
                        ESConnectorMetrics.recordFailure("server_error_5xx");
                        LOG.warn("Failed to update ES doc due to server error ({}). Retrying... ({}/{})",
                                statusCode, retryCount + 1, maxRetries);
                    } else {
                        String responseBody = EntityUtils.toString(response.getEntity());
                        ESConnectorMetrics.recordFailure("non_retryable_status");
                        ESCircuitBreaker.recordFailure();
                        throw new RuntimeException("Failed to update ES doc. Status: " + statusCode + ", Body: " + responseBody);
                    }
                } catch (IOException e) {
                    ESConnectorMetrics.recordFailure("io_exception");
                    LOG.warn("Failed to update ES doc for denorm attributes. Retrying... ({}/{})", retryCount + 1, maxRetries, e);
                }
            }

            if (!pendingDocIds.isEmpty()) {
                ESConnectorMetrics.recordFailure("retries_exhausted");
                ESCircuitBreaker.recordFailure();
                throw new RuntimeException("Failed to update ES doc for denorm attributes after " + maxRetries +
                        " retries. " + pendingDocIds.size() + " items still pending.");
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
            ESConnectorMetrics.stopLatencyTimer(latencySample);
        }
    }

    /**
     * Compute the retry delay for a given retry attempt: exponential backoff
     * (initial * 2^(retry-1)), capped at the configured maximum, and optionally
     * with equal jitter applied to spread retries across pods (avoids thundering
     * herd when many pods fail at the same time).
     *
     * @param retryCount the 1-based retry index (1 = first retry, 2 = second, ...)
     */
    static long computeBackoffMs(int retryCount) {
        long initial = AtlasConfiguration.ES_RETRY_DELAY_MS.getLong();
        long max     = AtlasConfiguration.ES_RETRY_MAX_DELAY_MS.getLong();
        long base    = initial * (long) Math.pow(2, Math.max(0, retryCount - 1));
        long capped  = Math.min(Math.max(initial, base), max);

        if (!AtlasConfiguration.ES_RETRY_JITTER_ENABLED.getBoolean()) {
            return capped;
        }
        // Equal jitter: half of capped is fixed, the other half is random in [0, capped/2)
        long half = capped / 2;
        if (half <= 0) {
            return capped;
        }
        return half + ThreadLocalRandom.current().nextLong(half);
    }

    /**
     * Thrown when an ES write is short-circuited because the per-pod circuit
     * breaker is OPEN. Distinct from a generic RuntimeException so callers
     * (and the outbox in MS-1010) can detect this case explicitly.
     */
    public static class ESWriteCircuitOpenException extends RuntimeException {
        public ESWriteCircuitOpenException(String message) { super(message); }
    }

    /**
     * Parses an ES bulk response to separate retryable (5xx/429) from permanently
     * failed (4xx) items.
     */
    @SuppressWarnings("unchecked")
    private static void parseBulkResponse(String respBody, Set<String> retryableIds, Set<String> permanentlyFailed) {
        try {
            Map<String, Object> bulkResp = AtlasType.fromJson(respBody, Map.class);
            List<Map<String, Object>> items = (List<Map<String, Object>>) bulkResp.get("items");
            if (items == null) return;

            for (Map<String, Object> item : items) {
                Map<String, Object> action = (Map<String, Object>) item.values().iterator().next();
                if (action == null) continue;

                String docId = String.valueOf(action.get("_id"));
                Object statusObj = action.get("status");
                int itemStatus = (statusObj instanceof Number) ? ((Number) statusObj).intValue() : 0;

                if (action.containsKey("error")) {
                    if (itemStatus >= 500 || itemStatus == 429) {
                        retryableIds.add(docId);
                        LOG.warn("writeTagProperties: bulk item retryable failure: _id='{}', status={}", docId, itemStatus);
                    } else {
                        permanentlyFailed.add(docId);
                        LOG.error("writeTagProperties: bulk item FAILED (non-retryable): _id='{}', status={}, error={}",
                                docId, itemStatus, AtlasType.toJson(action.get("error")));
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("writeTagProperties: failed to parse bulk response: {}. Raw (truncated): {}",
                    e.getMessage(), respBody.substring(0, Math.min(4000, respBody.length())));
        }
    }

    public static void ensureNestedMappings(Map<String, Map<String, Object>> nestedMappings) {
        if (nestedMappings == null || nestedMappings.isEmpty()) {
            return;
        }
        try {
            Map<String, Object> properties = new LinkedHashMap<>();
            for (Map.Entry<String, Map<String, Object>> entry : nestedMappings.entrySet()) {
                Map<String, Object> fieldMapping = new LinkedHashMap<>();
                fieldMapping.put("type", "nested");
                fieldMapping.put("properties", entry.getValue());
                properties.put(entry.getKey(), fieldMapping);
            }
            Map<String, Object> body = Collections.singletonMap("properties", properties);
            String jsonBody = AtlasType.toJson(body);
            Request request = new Request("PUT", "/" + VERTEX_INDEX_NAME + "/_mapping");
            request.setEntity(new org.apache.http.entity.StringEntity(jsonBody, ContentType.APPLICATION_JSON));
            Response response = lowLevelClient.performRequest(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= 200 && statusCode < 300) {
                LOG.info("ESConnector: applied {} nested mapping(s) to ES: {}", nestedMappings.size(), nestedMappings.keySet());
            } else {
                String responseBody = EntityUtils.toString(response.getEntity());
                LOG.warn("ESConnector: failed to apply nested mappings (HTTP {}): {}", statusCode, responseBody);
            }
        } catch (Exception e) {
            LOG.warn("ESConnector: failed to apply nested mappings to ES — non-fatal, existing mappings may suffice", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (lowLevelClient != null) {
            lowLevelClient.close();
        }
    }
}