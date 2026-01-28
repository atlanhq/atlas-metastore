package org.apache.atlas.repository.graphdb.janus.cassandra;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX_NAME;


public class ESConnector {
    // TODO: Check another ESConnector in repository module & dedup file & code
    private static final Logger LOG      = LoggerFactory.getLogger(ESConnector.class);

    private static RestClient lowLevelClient;

    private static Set<String> DENORM_ATTRS;
    private static String GET_DOCS_BY_ID = VERTEX_INDEX_NAME + "/_mget";
    public static final String JG_ES_DOC_ID_PREFIX = "S"; // S fot string type custom vertex ID

    // Painless script that sets non-null values and removes null values from ES doc.
    // This preserves fields not in the update (like tag attributes) while allowing field removal.
    private static final String UPSERT_SCRIPT = "for (entry in params.updates.entrySet()) { " +
            "if (entry.getValue() == null) { ctx._source.remove(entry.getKey()); } " +
            "else { ctx._source[entry.getKey()] = entry.getValue(); } }";



    public static final String INDEX_BACKEND_CONF = "atlas.graph.index.search.hostname";

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
        attrs.add(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY); //List
        attrs.add(PROPAGATED_CLASSIFICATION_NAMES_KEY); //String
        attrs.add(CLASSIFICATION_TEXT_KEY); //String
        attrs.add(TRAIT_NAMES_PROPERTY_KEY); //List
        attrs.add(CLASSIFICATION_NAMES_KEY); //String
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
        syncToEs(entitiesMap, null, upsert, false);
    }

    /**
     * Updates attributes on ES docs. Fields with null values will be REMOVED from ES doc.
     * @param entitiesMapForUpdate map to update on ES. Null values indicate field removal.
     * @param docIdsToDelete Doc Ids to delete from ES
     * @param upsert if true, helps to avoid document_missing_exception if doc is not present while creating/updating asset
     * @param useScript if true, use painless script to support field removal (null values remove field from ES doc)
     */
    public static void syncToEs(Map<String, Map<String, Object>> entitiesMapForUpdate, List<String> docIdsToDelete, boolean upsert, boolean useScript) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("writeTagPropertiesES");

        if (MapUtils.isEmpty(entitiesMapForUpdate) && CollectionUtils.isEmpty(docIdsToDelete)) {
            return;
        }
        try {
            StringBuilder bulkRequestBody = new StringBuilder();

            if (!MapUtils.isEmpty(entitiesMapForUpdate)) {
                for (String assetDocId : entitiesMapForUpdate.keySet()) {
                    Map<String, Object> toUpdate = new HashMap<>(entitiesMapForUpdate.get(assetDocId));

                    // Check if there are any null values that need to be removed
                    boolean hasNullValues = useScript && toUpdate.values().stream().anyMatch(v -> v == null);

                    bulkRequestBody.append("{\"update\":{\"_index\":\"");
                    bulkRequestBody.append(VERTEX_INDEX_NAME).append("\",\"_id\":\"").append(assetDocId).append("\"}}\n");

                    if (hasNullValues) {
                        // Use script to support field removal: null values remove field, non-null values set field
                        // This preserves fields not in the update (like tag attributes)
                        Map<String, Object> scriptBody = new HashMap<>();
                        Map<String, Object> script = new HashMap<>();
                        script.put("source", UPSERT_SCRIPT);
                        script.put("lang", "painless");
                        script.put("params", Collections.singletonMap("updates", toUpdate));
                        scriptBody.put("script", script);

                        if (upsert) {
                            // For upsert, filter out null values since the doc doesn't exist yet
                            Map<String, Object> upsertDoc = new HashMap<>();
                            for (Map.Entry<String, Object> entry : toUpdate.entrySet()) {
                                if (entry.getValue() != null) {
                                    upsertDoc.put(entry.getKey(), entry.getValue());
                                }
                            }
                            scriptBody.put("upsert", upsertDoc);
                        }
                        bulkRequestBody.append(AtlasType.toJson(scriptBody)).append("\n");
                    } else {
                        // Simple doc merge (no null values, just adds/updates)
                        String attrsToUpdate = AtlasType.toJson(toUpdate);
                        bulkRequestBody.append("{\"doc\":").append(attrsToUpdate);
                        if (upsert) {
                            bulkRequestBody.append(",\"upsert\":").append(attrsToUpdate);
                        }
                        bulkRequestBody.append("}\n");
                    }
                }
            }

            if (!CollectionUtils.isEmpty(docIdsToDelete)) {
                for (String docId: docIdsToDelete) {
                    bulkRequestBody.append("{\"delete\":{\"_index\":\"").append(VERTEX_INDEX_NAME).append("\",");
                    bulkRequestBody.append("\"_id\":\"").append(docId).append("\"}}");
                    bulkRequestBody.append("}\n");
                }
            }

            Request request = new Request("POST", "/_bulk");
            request.setEntity(new StringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON));

            int maxRetries = AtlasConfiguration.ES_MAX_RETRIES.getInt();
            long initialRetryDelay = AtlasConfiguration.ES_RETRY_DELAY_MS.getLong();

            for (int retryCount = 0; retryCount < maxRetries; retryCount++) {
                try {
                    Response response = lowLevelClient.performRequest(request); // Capture the response
                    int statusCode = response.getStatusLine().getStatusCode();

                    if (statusCode >= 200 && statusCode < 300) {
                        // Check response body for partial failures if necessary
                        return; // Success
                    }

                    // Add logic to retry on 5xx or throw on 4xx
                    if (statusCode >= 500) {
                        LOG.warn("Failed to update ES doc due to server error ({}). Retrying...", statusCode);
                    } else {
                        // Not a retryable error
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
            // If the loop completes, all retries have failed. Throw an exception.
            throw new RuntimeException("Failed to update ES doc for denorm attributes after " + maxRetries + " retries");
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private static List<HttpHost> getHttpHosts() throws AtlasException {
        List<HttpHost> httpHosts = new ArrayList<>();
        Configuration configuration = ApplicationProperties.get();
        String indexConf = configuration.getString(INDEX_BACKEND_CONF);
        String[] hosts = indexConf.split(",");
        for (String host : hosts) {
            host = host.trim();
            String[] hostAndPort = host.split(":");
            if (hostAndPort.length == 1) {
                httpHosts.add(new HttpHost(hostAndPort[0]));
            } else if (hostAndPort.length == 2) {
                httpHosts.add(new HttpHost(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
            } else {
                throw new AtlasException("Invalid config");
            }
        }
        return httpHosts;
    }
}