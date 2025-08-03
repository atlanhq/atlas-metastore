/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.EntityAuditEvent;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.audit.EntityAuditSearchResult;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.*;

import static java.nio.charset.Charset.defaultCharset;
import static org.apache.atlas.repository.Constants.DOMAIN_GUIDS;
import static org.springframework.util.StreamUtils.copyToString;

/**
 * This class provides cassandra support as the backend for audit storage support.
 */
@Singleton
@Component
@Order(8)
@ConditionalOnAtlasProperty(property = "atlas.EntityAuditRepositorySearch.impl")
public class ESBasedAuditRepository extends AbstractStorageBasedAuditRepository {
    private static final Logger LOG = LoggerFactory.getLogger(ESBasedAuditRepository.class);
    public static final String INDEX_BACKEND_CONF = "atlas.graph.index.search.hostname";
    private static final String TOTAL_FIELD_LIMIT = "atlas.index.audit.elasticsearch.total_field_limit";
    public static final String INDEX_NAME = "entity_audits";
    private static final String ENTITYID = "entityId";
    private static final String TYPE_NAME = "typeName";
    private static final String ENTITY_QUALIFIED_NAME = "entityQualifiedName";
    private static final String CREATED = "created";
    private static final String TIMESTAMP = "timestamp";
    private static final String EVENT_KEY = "eventKey";
    private static final String ACTION = "action";
    private static final String USER = "user";
    private static final String DETAIL = "detail";
    private static final String ENTITY = "entity";
    private static final String bulkMetadata = String.format("{ \"index\" : { \"_index\" : \"%s\" } }%n", INDEX_NAME);
    private static final Set<String> ALLOWED_LINKED_ATTRIBUTES = new HashSet<>(Arrays.asList(DOMAIN_GUIDS));
    private static final String ENTITY_AUDITS_INDEX = "entity_audits";

    // Static ObjectMapper to prevent memory allocations on each request (memory leak fix)
    private static final ObjectMapper STATIC_OBJECT_MAPPER = new ObjectMapper();

    /*
    *    created   → event creation time
         timestamp → entity modified timestamp
         eventKey  → entityId:timestamp
    * */

    private final Configuration configuration;
    private EntityGraphRetriever entityGraphRetriever;

    @Inject
    public ESBasedAuditRepository(Configuration configuration, EntityGraphRetriever entityGraphRetriever) {
        this.configuration = configuration;
        this.entityGraphRetriever = entityGraphRetriever;
    }

    @Override
    public void putEventsV1(List<EntityAuditEvent> events) throws AtlasException {

    }

    @Override
    public List<EntityAuditEvent> listEventsV1(String entityId, String startKey, short n) throws AtlasException {
        return null;
    }

    @Override
    public void putEventsV2(List<EntityAuditEventV2> events) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("pushInES");
        try {
            if (events != null && events.size() > 0) {

                Map<String, String> requestContextHeaders = RequestContext.get().getRequestContextHeaders();
                String entityPayloadTemplate = getQueryTemplate(requestContextHeaders);

                StringBuilder bulkRequestBody = new StringBuilder();
                for (EntityAuditEventV2 event : events) {
                    String created = String.format("%s", event.getTimestamp());
                    String auditDetailPrefix = EntityAuditListenerV2.getV2AuditPrefix(event.getAction());
                    String details = event.getDetails().substring(auditDetailPrefix.length());

                    String bulkItem = MessageFormat.format(entityPayloadTemplate,
                            event.getEntityId(),
                            event.getAction(),
                            details,
                            event.getUser(),
                            event.getEntityId() + ":" + event.getEntity().getUpdateTime().getTime(),
                            event.getEntityQualifiedName(),
                            event.getEntity().getTypeName(),
                            created,
                            "" + event.getEntity().getUpdateTime().getTime());

                    bulkRequestBody.append(bulkMetadata);
                    bulkRequestBody.append(bulkItem);
                    bulkRequestBody.append("\n");
                }
                String endpoint = INDEX_NAME + "/_bulk";
                HttpEntity entity = new NStringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON);
                Request request = new Request("POST", endpoint);
                request.setEntity(entity);
                Response response = AtlasElasticsearchDatabase.getLowLevelClient().performRequest(request);
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    throw new AtlasException("Unable to push entity audits to ES");
                }
                String responseString = EntityUtils.toString(response.getEntity());
                Map<String, Object> responseMap = AtlasType.fromJson(responseString, Map.class);
                if ((boolean) responseMap.get("errors")) {
                    List<String> errors = new ArrayList<>();
                    List<Map<String, Object>> resultItems = (List<Map<String, Object>>) responseMap.get("items");
                    for (Map<String, Object> resultItem : resultItems) {
                        if (resultItem.get("index") != null) {
                            Map<String, Object> resultIndex = (Map<String, Object>) resultItem.get("index");
                            if (resultIndex.get("error") != null) {
                                errors.add(resultIndex.get("error").toString());
                            }
                        }
                    }
                    throw new AtlasException(errors.toString());
                }
            }
        } catch (Exception e) {
            throw new AtlasBaseException("Unable to push entity audits to ES", e);
        }finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    private String getQueryTemplate(Map<String, String> requestContextHeaders) {
        StringBuilder template = new StringBuilder();

        template.append("'{'\"entityId\":\"{0}\",\"action\":\"{1}\",\"detail\":{2},\"user\":\"{3}\", \"eventKey\":\"{4}\", " +
                        "\"entityQualifiedName\": {5}, \"typeName\": \"{6}\",\"created\":{7}, \"timestamp\":{8}");

        if (MapUtils.isNotEmpty(requestContextHeaders)) {
            template.append(",")
                    .append("\"").append("headers").append("\"")
                    .append(":")
                    .append(AtlasType.toJson(requestContextHeaders).replaceAll("\\{", "'{").replaceAll("\\}", "'}"));

        }

        template.append("'}'");

        return template.toString();
    }

    @Override
    public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String startKey, short maxResultCount) throws AtlasBaseException {
        List<EntityAuditEventV2> ret;
        String queryTemplate = "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"entityId\":\"%s\"}},{\"term\":{\"action\":\"%s\"}}]}}}";
        String queryWithEntityFilter = String.format(queryTemplate, entityId, auditAction);
        ret = searchEvents(queryWithEntityFilter).getEntityAudits();

        return ret;
    }

    @Override
    public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String sortByColumn, boolean sortOrderDesc, int offset, short limit) throws AtlasBaseException {
        return null;
    }

    @Override
    public EntityAuditSearchResult searchEvents(String queryString) throws AtlasBaseException {
        try {
            String response = performSearchOnIndex(queryString);
            return getResultFromResponse(response);
        } catch (IOException e) {
            throw new AtlasBaseException(e);
        }
    }

    private EntityAuditSearchResult getResultFromResponse(String responseString) throws AtlasBaseException {
        List<EntityAuditEventV2> entityAudits = new ArrayList<>();
        EntityAuditSearchResult searchResult = new EntityAuditSearchResult();
        Map<String, Object> responseMap = AtlasType.fromJson(responseString, Map.class);
        Map<String, Object> hits_0 = (Map<String, Object>) responseMap.get("hits");
        List<LinkedHashMap> hits_1 = (List<LinkedHashMap>) hits_0.get("hits");
        Map<String, AtlasEntityHeader> existingLinkedEntities = searchResult.getLinkedEntities();

        for (LinkedHashMap hit : hits_1) {
            Map source = (Map) hit.get("_source");
            String entityGuid = (String) source.get(ENTITYID);
            EntityAuditEventV2 event = new EntityAuditEventV2();
            event.setEntityId(entityGuid);
            event.setAction(EntityAuditEventV2.EntityAuditActionV2.fromString((String) source.get(ACTION)));
            event.setDetail((Map<String, Object>) source.get(DETAIL));
            event.setUser((String) source.get(USER));
            event.setCreated((long) source.get(CREATED));
            if (source.get(TIMESTAMP) != null) {
                event.setTimestamp((long) source.get(TIMESTAMP));
            }
            if (source.get(TYPE_NAME) != null) {
                event.setTypeName((String) source.get(TYPE_NAME));
            }

            event.setEntityQualifiedName((String) source.get(ENTITY_QUALIFIED_NAME));

            String eventKey = (String) source.get(EVENT_KEY);
            if (StringUtils.isEmpty(eventKey)) {
                eventKey = event.getEntityId() + ":" + event.getTimestamp();
            }

            Map<String, Object> detail = event.getDetail();
            if (detail != null && detail.containsKey("attributes")) {
                Map<String, Object> attributes = (Map<String, Object>) detail.get("attributes");

                for (Map.Entry<String, Object> entry: attributes.entrySet()) {
                    if (ALLOWED_LINKED_ATTRIBUTES.contains(entry.getKey())) {
                        List<String> guids = (List<String>) entry.getValue();

                        if (guids != null && !guids.isEmpty()){
                            for (String guid: guids){
                                if(!existingLinkedEntities.containsKey(guid)){
                                    try {
                                        AtlasEntityHeader entityHeader = fetchAtlasEntityHeader(guid);
                                        if (entityHeader != null) {
                                            existingLinkedEntities.put(guid, entityHeader);
                                        }
                                    } catch (AtlasBaseException e) {
                                        LOG.error("Error while fetching entity header for guid: {}", guid, e);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            event.setHeaders((Map<String, String>) source.get("headers"));

            event.setEventKey(eventKey);
            entityAudits.add(event);
        }
        Map<String, Object> aggregationsMap = (Map<String, Object>) responseMap.get("aggregations");
        Map<String, Object> countObject = (Map<String, Object>) hits_0.get("total");
        int totalCount = (int) countObject.get("value");
        searchResult.setEntityAudits(entityAudits);
        searchResult.setLinkedEntities(existingLinkedEntities);
        searchResult.setAggregations(aggregationsMap);
        searchResult.setTotalCount(totalCount);
        searchResult.setCount(entityAudits.size());
        return searchResult;
    }

    private AtlasEntityHeader fetchAtlasEntityHeader(String domainGUID) throws AtlasBaseException {
        try {
            AtlasEntityHeader entityHeader = entityGraphRetriever.toAtlasEntityHeader(domainGUID);
            return entityHeader;
        } catch (AtlasBaseException e) {
            throw new AtlasBaseException(e);
        }
    }

    private String performSearchOnIndex(String queryString) throws IOException {
        HttpEntity entity = new NStringEntity(queryString, ContentType.APPLICATION_JSON);
        String endPoint = INDEX_NAME + "/_search";
        Request request = new Request("GET", endPoint);
        request.setEntity(entity);
        Response response = AtlasElasticsearchDatabase.getLowLevelClient().performRequest(request);
        return EntityUtils.toString(response.getEntity());
    }

    @Override
    public Set<String> getEntitiesWithTagChanges(long fromTimestamp, long toTimestamp) throws AtlasBaseException {
        throw new NotImplementedException();
    }

    @Override
    public void start() throws AtlasException {
        LOG.info("ESBasedAuditRepo - start!");
        initApplicationProperties();
        startInternal();
    }

    @VisibleForTesting
    void startInternal() throws AtlasException {
        createSession();
    }

    void createSession() throws AtlasException {
        LOG.info("Create ES Session in ES Based Audit Repo");
        // Using centralized ES client from AtlasElasticsearchDatabase
        try {
            boolean indexExists = checkIfIndexExists();
            if (!indexExists) {
                LOG.info("Create ES index for entity audits in ES Based Audit Repo");
                createAuditIndex();
            }
            if (shouldUpdateFieldLimitSetting()) {
                LOG.info("Updating ES total field limit");
                updateFieldLimit();
            }
            updateMappingsIfChanged();
        } catch (IOException e) {
            LOG.error("error", e);
            throw new AtlasException(e);
        }

    }

    private boolean checkIfIndexExists() throws IOException {
        Request request = new Request("HEAD", INDEX_NAME);
        Response response = AtlasElasticsearchDatabase.getLowLevelClient().performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200) {
            LOG.info("Entity audits index exists!");
            return true;
        }
        LOG.info("Entity audits index does not exist!");
        return false;
    }

    private boolean createAuditIndex() throws IOException {
        LOG.info("ESBasedAuditRepo - createAuditIndex!");
        String esMappingsString = getAuditIndexMappings();
        HttpEntity entity = new NStringEntity(esMappingsString, ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", INDEX_NAME);
        request.setEntity(entity);
        Response response = AtlasElasticsearchDatabase.getLowLevelClient().performRequest(request);
        return isSuccess(response);
    }

    private boolean shouldUpdateFieldLimitSetting() {
        JsonNode currentFieldLimit;
        try {
            currentFieldLimit = getIndexFieldLimit();
        } catch (IOException e) {
            LOG.error("Problem while retrieving the index field limit!", e);
            return false;
        }
        Integer fieldLimitFromConfigurationFile = configuration.getInt(TOTAL_FIELD_LIMIT, 0);
        return currentFieldLimit == null || fieldLimitFromConfigurationFile > currentFieldLimit.asInt();
    }

    private JsonNode getIndexFieldLimit() throws IOException {
        Request request = new Request("GET", INDEX_NAME + "/_settings");
        Response response = AtlasElasticsearchDatabase.getLowLevelClient().performRequest(request);
        // Use static ObjectMapper to prevent memory allocation (memory leak fix)
        String fieldPath = String.format("/%s/settings/index/mapping/total_fields/limit", INDEX_NAME);

        return STATIC_OBJECT_MAPPER.readTree(copyToString(response.getEntity().getContent(), Charset.defaultCharset())).at(fieldPath);
    }

    // Memory optimization note: For very large responses, consider streaming parsing instead of copyToString
    private void updateFieldLimit() {
        Request request = new Request("PUT", INDEX_NAME + "/_settings");
        String requestBody = String.format("{\"index.mapping.total_fields.limit\": %d}", configuration.getInt(TOTAL_FIELD_LIMIT));
        HttpEntity entity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
        request.setEntity(entity);
        Response response;
        try {
            response = AtlasElasticsearchDatabase.getLowLevelClient().performRequest(request);
            if (response.getStatusLine().getStatusCode() != 200) {
                LOG.error("Error while updating the Elasticsearch total field limits! Error: " + copyToString(response.getEntity().getContent(), defaultCharset()));
            } else {
                LOG.info("ES total field limit has been updated");
            }
        } catch (IOException e) {
            LOG.error("Error while updating the field limit", e);
        }
    }

    private void updateMappingsIfChanged() throws IOException, AtlasException {
        LOG.info("ESBasedAuditRepo - updateMappings!");
        // Use static ObjectMapper to prevent memory allocation (memory leak fix)
        Map<String, JsonNode> activeIndexMappings = getActiveIndexMappings(STATIC_OBJECT_MAPPER);
        JsonNode indexInformationFromConfigurationFile = STATIC_OBJECT_MAPPER.readTree(getAuditIndexMappings());
        for (String activeAuditIndex : activeIndexMappings.keySet()) {
            if (!areConfigurationsSame(activeIndexMappings.get(activeAuditIndex), indexInformationFromConfigurationFile)) {
                Response response = updateMappings(indexInformationFromConfigurationFile);
                if (isSuccess(response)) {
                    LOG.info("ESBasedAuditRepo - Elasticsearch mappings have been updated for index: {}", activeAuditIndex);
                } else {
                    LOG.error("Error while updating the Elasticsearch indexes for index: {}", activeAuditIndex);
                    throw new AtlasException(copyToString(response.getEntity().getContent(), Charset.defaultCharset()));
                }
            }
        }
    }

    private Map<String, JsonNode> getActiveIndexMappings(ObjectMapper mapper) throws IOException {
        Request request = new Request("GET", INDEX_NAME);
        Response response = AtlasElasticsearchDatabase.getLowLevelClient().performRequest(request);
        String responseString = copyToString(response.getEntity().getContent(), Charset.defaultCharset());
        Map<String, JsonNode> indexMappings = new TreeMap<>();
        JsonNode rootNode = mapper.readTree(responseString);

        // Iterate over the index names and get the mappings
        for (Iterator<String> it = rootNode.fieldNames(); it.hasNext(); ) {
            String indexName = it.next();
            if (indexName.startsWith(ENTITY_AUDITS_INDEX)) {
                indexMappings.put(indexName, rootNode.get(indexName).get("mappings"));
            }
        }
        return indexMappings;
    }

    private boolean areConfigurationsSame(JsonNode activeIndexInformation, JsonNode indexInformationFromConfigurationFile) {
        return indexInformationFromConfigurationFile.get("mappings").equals(activeIndexInformation);
    }

    private Response updateMappings(JsonNode indexInformationFromConfigurationFile) throws IOException {
        Request request = new Request("PUT", INDEX_NAME + "/_mapping");
        HttpEntity entity = new NStringEntity(indexInformationFromConfigurationFile.get("mappings").toString(), ContentType.APPLICATION_JSON);
        request.setEntity(entity);
        return AtlasElasticsearchDatabase.getLowLevelClient().performRequest(request);
    }

    private String getAuditIndexMappings() throws IOException {
        String atlasHomeDir = System.getProperty("atlas.home");
        String atlasHome = StringUtils.isEmpty(atlasHomeDir) ? "." : atlasHomeDir;
        File elasticsearchSettingsFile = Paths.get(atlasHome, "elasticsearch", "es-audit-mappings.json").toFile();
        return new String(Files.readAllBytes(elasticsearchSettingsFile.toPath()), StandardCharsets.UTF_8);
    }

    @Override
    public void stop() throws AtlasException {
        LOG.info("ESBasedAuditRepo - stop! (Using shared ES client - no cleanup needed)");
        // No client cleanup needed - using shared AtlasElasticsearchDatabase client
    }

    private boolean isSuccess(Response response) {
        return response.getStatusLine().getStatusCode() == 200;
    }
}


