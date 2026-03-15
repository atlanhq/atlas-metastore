package org.apache.atlas.repository.store.graph.v2;
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
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

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
        writeTagPropertiesWithResult(entitiesMap, false);
    }

    public static void writeTagProperties(Map<String, Map<String, Object>> entitiesMap, boolean upsert) {
        writeTagPropertiesWithResult(entitiesMap, upsert);
    }

    /**
     * Updates tag properties in ES and returns detailed result with success/failure per doc.
     * Parses the ES bulk response to detect partial failures.
     */
    public static TagDenormESWriteResult writeTagPropertiesWithResult(Map<String, Map<String, Object>> entitiesMap, boolean upsert) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("writeTagPropertiesES");

        try {
            if (MapUtils.isEmpty(entitiesMap))
                return TagDenormESWriteResult.allSuccess(0);

            StringBuilder bulkRequestBody = new StringBuilder();
            // Track docId → vertexId mapping for failure reporting
            Map<String, String> docIdToVertexId = new LinkedHashMap<>();

            for (String assetVertexId : entitiesMap.keySet()) {
                Map<String, Object> entry = entitiesMap.get(assetVertexId);
                Map<String, Object> toUpdate = new HashMap<>();

                DENORM_ATTRS.stream().filter(entry::containsKey).forEach(x -> toUpdate.put(x, entry.get(x)));

                long vertexId = Long.parseLong(assetVertexId);
                String docId = LongEncoding.encode(vertexId);
                docIdToVertexId.put(docId, assetVertexId);

                bulkRequestBody.append("{\"update\":{\"_index\":\"janusgraph_vertex_index\",\"_id\":\"").append(docId).append("\" }}\n");

                bulkRequestBody.append("{");
                String attrsToUpdate = AtlasType.toJson(toUpdate);
                bulkRequestBody.append("\"doc\":").append(attrsToUpdate);

                if (upsert) {
                    bulkRequestBody.append(",\"upsert\":").append(attrsToUpdate);
                }

                bulkRequestBody.append("}\n");
            }

            Request request = new Request("POST", "/_bulk");
            request.setEntity(new StringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON));

            int maxRetries = AtlasConfiguration.ES_MAX_RETRIES.getInt();
            long initialRetryDelay = AtlasConfiguration.ES_RETRY_DELAY_MS.getLong();

            for (int retryCount = 0; retryCount < maxRetries; retryCount++) {
                try {
                    Response response = lowLevelClient.performRequest(request);
                    int statusCode = response.getStatusLine().getStatusCode();

                    if (statusCode >= 200 && statusCode < 300) {
                        return parseBulkResponse(response, docIdToVertexId, entitiesMap.size());
                    }

                    if (statusCode >= 500) {
                        LOG.warn("Failed to update ES doc due to server error ({}). Retrying...", statusCode);
                    } else {
                        String responseBody = EntityUtils.toString(response.getEntity());
                        throw new RuntimeException("Failed to update ES doc. Status: " + statusCode + ", Body: " + responseBody);
                    }
                } catch (IOException e) {
                    LOG.warn("Failed to update ES doc for denorm attributes. Retrying... ({}/{})", retryCount + 1, maxRetries, e);
                }

                if (retryCount < maxRetries - 1) {
                    try {
                        long exponentialBackoffDelay = initialRetryDelay * (long) Math.pow(2, retryCount);
                        Thread.sleep(exponentialBackoffDelay);
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("ES update interrupted during retry delay", interruptedException);
                    }
                }
            }

            // All retries exhausted — all docs failed
            LOG.error("Failed to update ES docs for denorm attributes after {} retries. Marking all {} docs as failed.", maxRetries, entitiesMap.size());
            return TagDenormESWriteResult.allFailed(entitiesMap.keySet());
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    /**
     * Parses ES bulk response to detect partial failures.
     * Returns a result indicating which docs succeeded and which failed.
     */
    @SuppressWarnings("unchecked")
    private static TagDenormESWriteResult parseBulkResponse(Response response, Map<String, String> docIdToVertexId, int totalDocs) {
        try {
            String responseBody = EntityUtils.toString(response.getEntity());
            Map<String, Object> bulkResponse = AtlasType.fromJson(responseBody, Map.class);

            Boolean hasErrors = (Boolean) bulkResponse.get("errors");
            if (hasErrors == null || !hasErrors) {
                return TagDenormESWriteResult.allSuccess(totalDocs);
            }

            // Parse individual item results to find failures
            List<Map<String, Object>> items = (List<Map<String, Object>>) bulkResponse.get("items");
            if (items == null) {
                LOG.warn("ES bulk response has errors=true but no items array");
                return TagDenormESWriteResult.allSuccess(totalDocs);
            }

            List<String> failedVertexIds = new ArrayList<>();
            for (Map<String, Object> item : items) {
                Map<String, Object> updateResult = (Map<String, Object>) item.get("update");
                if (updateResult == null) continue;

                Object statusObj = updateResult.get("status");
                int itemStatus = statusObj instanceof Number ? ((Number) statusObj).intValue() : 500;

                if (itemStatus >= 400) {
                    String docId = (String) updateResult.get("_id");
                    String vertexId = docIdToVertexId.get(docId);
                    if (vertexId != null) {
                        failedVertexIds.add(vertexId);
                    }
                    LOG.warn("ES bulk update failed for docId={}, vertexId={}, status={}, error={}",
                            docId, vertexId, itemStatus, updateResult.get("error"));
                }
            }

            int successCount = totalDocs - failedVertexIds.size();
            if (!failedVertexIds.isEmpty()) {
                LOG.warn("ES bulk write: {}/{} docs succeeded, {} failed", successCount, totalDocs, failedVertexIds.size());
            }
            return new TagDenormESWriteResult(successCount, failedVertexIds);
        } catch (Exception e) {
            LOG.warn("Failed to parse ES bulk response, treating as all failed for safety", e);
            return TagDenormESWriteResult.allFailed(docIdToVertexId.values());
        }
    }

    @Override
    public void close() throws IOException {
        if (lowLevelClient != null) {
            lowLevelClient.close();
        }
    }

    /**
     * Result of a bulk ES write operation for tag denorm attributes.
     */
    public static class TagDenormESWriteResult {
        private final int successCount;
        private final List<String> failedVertexIds;

        public TagDenormESWriteResult(int successCount, List<String> failedVertexIds) {
            this.successCount = successCount;
            this.failedVertexIds = failedVertexIds;
        }

        public static TagDenormESWriteResult allSuccess(int count) {
            return new TagDenormESWriteResult(count, Collections.emptyList());
        }

        public static TagDenormESWriteResult allFailed(Collection<String> vertexIds) {
            return new TagDenormESWriteResult(0, new ArrayList<>(vertexIds));
        }

        public int getSuccessCount()          { return successCount; }
        public List<String> getFailedVertexIds() { return failedVertexIds; }
        public boolean hasFailures()           { return !failedVertexIds.isEmpty(); }
    }
}