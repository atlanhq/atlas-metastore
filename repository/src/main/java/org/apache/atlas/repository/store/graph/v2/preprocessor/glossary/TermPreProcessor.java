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


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.util.Iterator;
import java.util.List;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.getActiveParentVertices;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.store.graph.v2.tasks.MeaningsTaskFactory.UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE;
import static org.apache.atlas.type.Constants.LEXICOGRAPHICAL_SORT_ORDER;

@Component
public class TermPreProcessor extends AbstractGlossaryPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(TermPreProcessor.class);

    private AtlasEntityHeader anchor;
    public TermPreProcessor( AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph, TaskManagement taskManagement) {
        super(typeRegistry, entityRetriever, graph, taskManagement);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("TermPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        setAnchor(entity, context);

        switch (operation) {
            case CREATE:
                processCreateTerm(entity, vertex);
                break;
            case UPDATE:
                processUpdateTerm(entity, vertex);
                break;
        }
    }

    private void processCreateTerm(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateTerm");
        String termName = (String) entity.getAttribute(NAME);
        String termQName = vertex.getProperty(QUALIFIED_NAME, String.class);

        if (StringUtils.isEmpty(termName) || isNameInvalid(termName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        String glossaryQName = (String) anchor.getAttribute(QUALIFIED_NAME);

        termExists(termName, glossaryQName);

        String parentQname = validateAndGetCategory(entity);

        String lexicographicalSortOrder = (String) entity.getAttribute(LEXICOGRAPHICAL_SORT_ORDER);
        if(StringUtils.isEmpty(lexicographicalSortOrder)){
            assignNewLexicographicalSortOrder(entity, glossaryQName, parentQname, this.discovery);
        } else {
            isValidLexoRank(lexicographicalSortOrder, glossaryQName, parentQname, this.discovery);
        }

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName());
        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, new AtlasEntityHeader(entity)),
                "create entity: type=", entity.getTypeName());

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateTerm(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateTerm");
        String termName = (String) entity.getAttribute(NAME);
        String vertexName = vertex.getProperty(NAME, String.class);
        String termGuid = entity.getGuid();
        String vertexQualifiedName = vertex.getProperty(QUALIFIED_NAME, String.class);
        String vertexUniqueQualifiedName = vertex.getProperty(UNIQUE_QUALIFIED_NAME, String.class);

        LOG.info("[TERM_DUP_DEBUG] processUpdateTerm START: termGuid='{}', termName='{}', vertexName='{}', " +
                "vertex.qualifiedName='{}', vertex.__u_qualifiedName='{}'",
                termGuid, termName, vertexName, vertexQualifiedName, vertexUniqueQualifiedName);

        if (StringUtils.isEmpty(termName) || isNameInvalid(termName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        String parentQname = validateAndGetCategory(entity);

        AtlasEntity storedTerm = entityRetriever.toAtlasEntity(vertex);
        AtlasRelatedObjectId currentGlossary = (AtlasRelatedObjectId) storedTerm.getRelationshipAttribute(ANCHOR);
        AtlasEntityHeader currentGlossaryHeader = entityRetriever.toAtlasEntityHeader(currentGlossary.getGuid());
        String currentGlossaryQualifiedName = (String) currentGlossaryHeader.getAttribute(QUALIFIED_NAME);

        String termQualifiedName = vertex.getProperty(QUALIFIED_NAME, String.class);

        String newGlossaryQualifiedName = (String) anchor.getAttribute(QUALIFIED_NAME);

        String lexicographicalSortOrder = (String) entity.getAttribute(LEXICOGRAPHICAL_SORT_ORDER);
        if(StringUtils.isNotEmpty(lexicographicalSortOrder)) {
            isValidLexoRank(lexicographicalSortOrder, newGlossaryQualifiedName, parentQname, this.discovery);
        } else {
            entity.removeAttribute(LEXICOGRAPHICAL_SORT_ORDER);
        }

        if (!currentGlossaryQualifiedName.equals(newGlossaryQualifiedName)){
            LOG.info("[TERM_DUP_DEBUG] GLOSSARY CHANGE DETECTED - Moving term between glossaries: " +
                    "termGuid='{}', termName='{}', currentGlossary='{}', newGlossary='{}'",
                    termGuid, termName, currentGlossaryQualifiedName, newGlossaryQualifiedName);
            
            //Auth check
            isAuthorized(currentGlossaryHeader, anchor);

            String updatedTermQualifiedName = moveTermToAnotherGlossary(entity, vertex, currentGlossaryQualifiedName, newGlossaryQualifiedName, termQualifiedName);

            if (checkEntityTermAssociation(termQualifiedName)) {
                if (taskManagement != null && DEFERRED_ACTION_ENABLED) {
                    createAndQueueTask(UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE, vertexName, termName, termQualifiedName, updatedTermQualifiedName, vertex);
                } else {
                    updateMeaningsAttributesInEntitiesOnTermUpdate(vertexName, termName, termQualifiedName, updatedTermQualifiedName, termGuid);
                }
            }

        } else {
            LOG.info("[TERM_DUP_DEBUG] SAME GLOSSARY - Term update within same glossary: " +
                    "termGuid='{}', oldName='{}', newName='{}', glossary='{}'",
                    termGuid, vertexName, termName, newGlossaryQualifiedName);

            if (!vertexName.equals(termName)) {
                LOG.info("[TERM_DUP_DEBUG] NAME CHANGE DETECTED - Checking if new name '{}' already exists in glossary '{}'", 
                        termName, newGlossaryQualifiedName);
                termExists(termName, newGlossaryQualifiedName);
            } else {
                LOG.info("[TERM_DUP_DEBUG] NO NAME CHANGE - Term name stays '{}', no duplicate check needed", termName);
            }

            entity.setAttribute(QUALIFIED_NAME, termQualifiedName);

            if (!termName.equals(vertexName) && checkEntityTermAssociation(termQualifiedName)) {
                if (taskManagement != null && DEFERRED_ACTION_ENABLED) {
                    createAndQueueTask(UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE, vertexName, termName, termQualifiedName, null, vertex);
                } else {
                    updateMeaningsAttributesInEntitiesOnTermUpdate(vertexName, termName, termQualifiedName, null, termGuid);
                }
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private String validateAndGetCategory(AtlasEntity entity) throws AtlasBaseException {
        String glossaryQualifiedName = (String) anchor.getAttribute(QUALIFIED_NAME);
        String categoryQualifiedName = null;

        if (entity.hasRelationshipAttribute(ATTR_CATEGORIES) && entity.getRelationshipAttribute(ATTR_CATEGORIES) != null) {
            List<AtlasObjectId> categories = (List<AtlasObjectId>) entity.getRelationshipAttribute(ATTR_CATEGORIES);

            if (CollectionUtils.isNotEmpty(categories)) {
                AtlasObjectId category = categories.get(0);

                if (category.getUniqueAttributes() != null && category.getUniqueAttributes().containsKey(QUALIFIED_NAME)) {
                    categoryQualifiedName = (String) category.getUniqueAttributes().get(QUALIFIED_NAME);
                } else {
                    AtlasVertex categoryVertex = entityRetriever.getEntityVertex(category.getGuid());
                    categoryQualifiedName = categoryVertex.getProperty(QUALIFIED_NAME, String.class);
                }

                if (!categoryQualifiedName.endsWith(glossaryQualifiedName)) {
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Passed category doesn't belongs to Passed Glossary");
                }
            }
        }
        return categoryQualifiedName;
    }

    public String moveTermToAnotherGlossary(AtlasEntity entity, AtlasVertex vertex,
                                           String sourceGlossaryQualifiedName,
                                           String targetGlossaryQualifiedName,
                                           String currentTermQualifiedName) throws AtlasBaseException {

        String termGuid = vertex.getProperty(GUID_PROPERTY_KEY, String.class);
        String termName = (String) entity.getAttribute(NAME);
        String vertexQualifiedName = vertex.getProperty(QUALIFIED_NAME, String.class);
        String vertexUniqueQualifiedName = vertex.getProperty(UNIQUE_QUALIFIED_NAME, String.class);
        
        LOG.info("[TERM_DUP_DEBUG] BEFORE moveTermToAnotherGlossary: termGuid='{}', termName='{}', " +
                "sourceGlossary='{}', targetGlossary='{}', " +
                "currentTermQualifiedName='{}', " +
                "vertex.qualifiedName='{}', vertex.__u_qualifiedName='{}'",
                termGuid, termName, sourceGlossaryQualifiedName, targetGlossaryQualifiedName,
                currentTermQualifiedName, vertexQualifiedName, vertexUniqueQualifiedName);
        
        //check duplicate term name
        termExists((String) entity.getAttribute(NAME), targetGlossaryQualifiedName);


        String updatedQualifiedName = currentTermQualifiedName.replace(sourceGlossaryQualifiedName, targetGlossaryQualifiedName);
        
        LOG.info("[TERM_DUP_DEBUG] Calculated updatedQualifiedName='{}' (replacing source with target)", updatedQualifiedName);

        //qualifiedName
        entity.setAttribute(QUALIFIED_NAME, updatedQualifiedName);
        
        LOG.info("[TERM_DUP_DEBUG] AFTER setAttribute: entity.qualifiedName updated to '{}'. " +
                "NOTE: vertex.__u_qualifiedName is NOT updated here - this may cause data inconsistency! " +
                "Current vertex.__u_qualifiedName still='{}', should be='{}'",
                updatedQualifiedName, vertexUniqueQualifiedName, updatedQualifiedName);

        // __categories
        /*  if category is not passed in relationshipAttributes, check
            whether category belongs to target glossary, if not throw an exception
         */
        if (!entity.hasRelationshipAttribute(ATTR_CATEGORIES)) {
            Iterator<AtlasVertex> categoriesItr = getActiveParentVertices(vertex, CATEGORY_TERMS_EDGE_LABEL);

            if (categoriesItr.hasNext()) {
                AtlasVertex categoryVertex = categoriesItr.next();

                String categoryQualifiedName = categoryVertex.getProperty(QUALIFIED_NAME, String.class);

                if (!categoryQualifiedName.endsWith(targetGlossaryQualifiedName)) {
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Passed category doesn't belongs to Passed Glossary");
                }
            }
        }

        return updatedQualifiedName;
    }

    private String createQualifiedName() {
        return getUUID() + "@" + anchor.getAttribute(QUALIFIED_NAME);
    }

    private void setAnchor(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("TermPreProcessor.setAnchor");
        if (anchor == null) {
            AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(ANCHOR);

            if (StringUtils.isNotEmpty(objectId.getGuid())) {
                AtlasVertex vertex = context.getVertex(objectId.getGuid());

                if (vertex == null) {
                    anchor = entityRetriever.toAtlasEntityHeader(objectId.getGuid());
                } else {
                    anchor = entityRetriever.toAtlasEntityHeader(vertex);
                }

            } else if (MapUtils.isNotEmpty(objectId.getUniqueAttributes()) &&
                    StringUtils.isNotEmpty( (String) objectId.getUniqueAttributes().get(QUALIFIED_NAME))) {
                anchor = new AtlasEntityHeader(objectId.getTypeName(), objectId.getUniqueAttributes());

            }
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }
}
