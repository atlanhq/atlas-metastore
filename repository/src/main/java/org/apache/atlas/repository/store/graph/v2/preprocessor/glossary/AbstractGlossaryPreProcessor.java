/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2.preprocessor.glossary;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.repository.store.graph.v2.tasks.MeaningsTask;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.ATLAS_GLOSSARY_TERM_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.ELASTICSEARCH_PAGINATION_SIZE;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.UNIQUE_QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.indexSearchPaginated;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;
import static org.apache.atlas.type.Constants.MEANINGS_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.MEANINGS_TEXT_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.MEANING_NAMES_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.PENDING_TASKS_PROPERTY_KEY;

public abstract class AbstractGlossaryPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractGlossaryPreProcessor.class);

    static final boolean DEFERRED_ACTION_ENABLED = AtlasConfiguration.TASKS_USE_ENABLED.getBoolean();

    protected static final String ATTR_MEANINGS   = "meanings";
    protected static final String ATTR_CATEGORIES = "categories";

    protected final AtlasTypeRegistry typeRegistry;
    protected final EntityGraphRetriever entityRetriever;
    protected final TaskManagement taskManagement;

    protected EntityDiscoveryService discovery;

    AbstractGlossaryPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph, TaskManagement taskManagement) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.taskManagement = taskManagement;

        try {
            this.discovery = new EntityDiscoveryService(typeRegistry, graph, null, null, null, null, entityRetriever);
        } catch (AtlasException e) {
            e.printStackTrace();
        }
    }

    public void termExists(String termName, String glossaryQName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("termExists");
        boolean ret = false;

        try {
            LOG.info("[TERM_DUP_DEBUG] Checking if term exists: termName='{}', glossaryQName='{}'", termName, glossaryQName);
            
            List mustClauseList = new ArrayList();
            mustClauseList.add(mapOf("term", mapOf("__glossary", glossaryQName)));
            mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", ATLAS_GLOSSARY_TERM_ENTITY_TYPE)));
            mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
            mustClauseList.add(mapOf("term", mapOf("name.keyword", termName)));

            Map<String, Object> dsl = mapOf("query", mapOf("bool", mapOf("must", mustClauseList)));
            
            LOG.info("[TERM_DUP_DEBUG] ElasticSearch query: __glossary='{}', name='{}', __typeName='AtlasGlossaryTerm', __state='ACTIVE'", glossaryQName, termName);

            List<AtlasEntityHeader> terms = indexSearchPaginated(dsl, null, this.discovery);
            
            LOG.info("[TERM_DUP_DEBUG] ES query returned {} term(s)", terms != null ? terms.size() : 0);

            if (CollectionUtils.isNotEmpty(terms)) {
                LOG.info("[TERM_DUP_DEBUG] Analyzing {} term(s) returned from ElasticSearch to check for exact name match", terms.size());
                
                for (AtlasEntityHeader term : terms) {
                    String foundName = (String) term.getAttribute(NAME);
                    String foundGuid = term.getGuid();
                    String foundQualifiedName = (String) term.getAttribute(QUALIFIED_NAME);
                    Object foundGlossary = term.getAttribute("__glossary");
                    
                    LOG.info("[TERM_DUP_DEBUG] Found term via ES: guid='{}', name='{}', qualifiedName='{}', __glossary='{}'", 
                            foundGuid, foundName, foundQualifiedName, foundGlossary);
                    
                    // Try to get __u_qualifiedName from vertex to detect data inconsistency
                    try {
                        AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(foundGuid);
                        if (vertex != null) {
                            String vertexQualifiedName = vertex.getProperty(QUALIFIED_NAME, String.class);
                            String vertexUniqueQualifiedName = vertex.getProperty(UNIQUE_QUALIFIED_NAME, String.class);
                            
                            if (vertexQualifiedName != null && vertexUniqueQualifiedName != null && 
                                !vertexQualifiedName.equals(vertexUniqueQualifiedName)) {
                                LOG.info("[TERM_DUP_DEBUG] ⚠️  DATA INCONSISTENCY in found term guid='{}': " +
                                        "vertex.qualifiedName='{}' != vertex.__u_qualifiedName='{}'. " +
                                        "This term likely was moved between glossaries and __u_qualifiedName was not updated. " +
                                        "This data inconsistency may be causing false duplicate detection!",
                                        foundGuid, vertexQualifiedName, vertexUniqueQualifiedName);
                            } else {
                                LOG.info("[TERM_DUP_DEBUG] Found term guid='{}' has consistent data: qualifiedName matches __u_qualifiedName", foundGuid);
                            }
                        }
                    } catch (Exception e) {
                        LOG.debug("[TERM_DUP_DEBUG] Could not fetch vertex details for guid='{}': {}", foundGuid, e.getMessage());
                    }
                }
                ret = terms.stream().map(term -> (String) term.getAttribute(NAME)).anyMatch(name -> termName.equals(name));
            }

            if (ret) {
                StringBuilder dupDetails = new StringBuilder();
                dupDetails.append(String.format("Term '%s' already exists in glossary '%s'. ", termName, glossaryQName));
                dupDetails.append(String.format("ElasticSearch found %d term(s) in this glossary with matching criteria. ", terms.size()));
                dupDetails.append("Duplicate detection grounds: ");
                dupDetails.append("(1) ES indexed term(s) with __glossary='").append(glossaryQName).append("' AND ");
                dupDetails.append("(2) name.keyword='").append(termName).append("' AND ");
                dupDetails.append("(3) __typeName='AtlasGlossaryTerm' AND ");
                dupDetails.append("(4) __state='ACTIVE'. ");
                
                // List all matching terms
                if (terms != null && !terms.isEmpty()) {
                    dupDetails.append("Conflicting term GUIDs: [");
                    for (int i = 0; i < terms.size(); i++) {
                        if (i > 0) dupDetails.append(", ");
                        dupDetails.append(terms.get(i).getGuid());
                    }
                    dupDetails.append("]. ");
                }
                
                dupDetails.append("Check logs above for data inconsistency warnings (__u_qualifiedName mismatch) which may indicate false positive.");
                
                LOG.info("[TERM_DUP_DEBUG] DUPLICATE DETECTED - {} Throwing ATLAS-409.", dupDetails.toString());
                throw new AtlasBaseException(AtlasErrorCode.GLOSSARY_TERM_ALREADY_EXISTS, termName);
            } else {
                LOG.info("[TERM_DUP_DEBUG] No duplicate found - Term '{}' does not exist in glossary '{}'. " +
                        "ES returned {} term(s) but none had exact name match after filtering.", 
                        termName, glossaryQName, terms != null ? terms.size() : 0);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public void createAndQueueTask(String taskType,
                                   String currentTermName, String updatedTermName,
                                   String termQName, String updatedTermQualifiedName,
                                   AtlasVertex termVertex) {
        String termGuid = GraphHelper.getGuid(termVertex);
        String currentUser = RequestContext.getCurrentUser();
        Map<String, Object> taskParams = MeaningsTask.toParameters(currentTermName, updatedTermName, termQName, updatedTermQualifiedName, termGuid);
        AtlasTask task = taskManagement.createTask(taskType, currentUser, taskParams);

        AtlasGraphUtilsV2.addEncodedProperty(termVertex, PENDING_TASKS_PROPERTY_KEY, task.getGuid());

        RequestContext.get().queueTask(task);
    }

    public boolean checkEntityTermAssociation(String termQName) throws AtlasBaseException {
        List<AtlasEntityHeader> entityHeader;
        entityHeader = discovery.searchUsingTermQualifiedName(0,1,termQName,null,null);
        return entityHeader != null;
    }

    public void updateMeaningsAttributesInEntitiesOnTermUpdate(String currentTermName, String updatedTermName,
                                                               String termQName, String updatedTermQName,
                                                               String termGuid) throws AtlasBaseException {
        Set<String> attributes = new HashSet<String>(){{
            add(ATTR_MEANINGS);
        }};

        Set<String> relationAttributes = new HashSet<String>(){{
            add(STATE_PROPERTY_KEY);
            add(NAME);
        }};

        int from = 0;
        while (true) {
            List<AtlasEntityHeader> entityHeaders = discovery.searchUsingTermQualifiedName(from, ELASTICSEARCH_PAGINATION_SIZE,
                    termQName, attributes, relationAttributes);

            if (entityHeaders == null)
                break;

            for (AtlasEntityHeader entityHeader : entityHeaders) {
                AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(entityHeader.getGuid());

                if (!currentTermName.equals(updatedTermName)) {
                    List<AtlasObjectId> meanings = (List<AtlasObjectId>) entityHeader.getAttribute(ATTR_MEANINGS);

                    String updatedMeaningsText = meanings
                            .stream()
                            .filter(x -> AtlasEntity.Status.ACTIVE.name().equals(x.getAttributes().get(STATE_PROPERTY_KEY)))
                            .map(x -> x.getGuid().equals(termGuid) ? updatedTermName : x.getAttributes().get(NAME).toString())
                            .collect(Collectors.joining(","));

                    AtlasGraphUtilsV2.setEncodedProperty(entityVertex, MEANINGS_TEXT_PROPERTY_KEY, updatedMeaningsText);
                    List<String> meaningsNames = entityVertex.getMultiValuedProperty(MEANING_NAMES_PROPERTY_KEY, String.class);

                    if (meaningsNames.contains(currentTermName)) {
                        AtlasGraphUtilsV2.removeItemFromListPropertyValue(entityVertex, MEANING_NAMES_PROPERTY_KEY, currentTermName);
                        AtlasGraphUtilsV2.addListProperty(entityVertex, MEANING_NAMES_PROPERTY_KEY, updatedTermName, true);
                    }
                }

                if (StringUtils.isNotEmpty(updatedTermQName) && !termQName.equals(updatedTermQName)) {
                    AtlasGraphUtilsV2.removeItemFromListPropertyValue(entityVertex, MEANINGS_PROPERTY_KEY, termQName);
                    AtlasGraphUtilsV2.addEncodedProperty(entityVertex, MEANINGS_PROPERTY_KEY, updatedTermQName);
                }
            }

            from += ELASTICSEARCH_PAGINATION_SIZE;

            if (entityHeaders.size() < ELASTICSEARCH_PAGINATION_SIZE) {
                break;
            }
        }
    }

    protected void isAuthorized(AtlasEntityHeader sourceGlossary, AtlasEntityHeader targetGlossary) throws AtlasBaseException {

        // source -> CREATE + UPDATE + DELETE
        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, sourceGlossary),
                "create on source Glossary: ", sourceGlossary.getAttribute(NAME));

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, sourceGlossary),
                "update on source Glossary: ", sourceGlossary.getAttribute(NAME));

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_DELETE, sourceGlossary),
                "delete on source Glossary: ", sourceGlossary.getAttribute(NAME));


        // target -> CREATE + UPDATE + DELETE
        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, targetGlossary),
                "create on source Glossary: ", targetGlossary.getAttribute(NAME));

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, targetGlossary),
                "update on source Glossary: ", targetGlossary.getAttribute(NAME));

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_DELETE, targetGlossary),
                "delete on source Glossary: ", targetGlossary.getAttribute(NAME));
    }

    /**
     * Record the updated child entities, it will be used to send notification and store audit logs
     * @param entityVertex Child entity vertex
     * @param updatedAttributes Updated attributes while updating required attributes on updating collection
     */
    protected void recordUpdatedChildEntities(AtlasVertex entityVertex, Map<String, Object> updatedAttributes) {
        RequestContext requestContext = RequestContext.get();
        AtlasPerfMetrics.MetricRecorder metricRecorder = requestContext.startMetricRecord("recordUpdatedChildEntities");
        AtlasEntity entity = new AtlasEntity();
        entity = entityRetriever.mapSystemAttributes(entityVertex, entity);
        entity.setAttributes(updatedAttributes);
        requestContext.cacheDifferentialEntity(new AtlasEntity(entity));

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

        //Add the min info attributes to entity header to be sent as part of notification
        if(entityType != null) {
            AtlasEntity finalEntity = entity;
            entityType.getMinInfoAttributes().values().stream().filter(attribute -> !updatedAttributes.containsKey(attribute.getName())).forEach(attribute -> {
                Object attrValue = null;
                try {
                    attrValue = entityRetriever.getVertexAttribute(entityVertex, attribute);
                } catch (AtlasBaseException e) {
                    LOG.error("Error while getting vertex attribute", e);
                }
                if(attrValue != null) {
                    finalEntity.setAttribute(attribute.getName(), attrValue);
                }
            });
            requestContext.recordEntityUpdate(new AtlasEntityHeader(finalEntity));
        }

        requestContext.endMetricRecord(metricRecorder);
    }
}
