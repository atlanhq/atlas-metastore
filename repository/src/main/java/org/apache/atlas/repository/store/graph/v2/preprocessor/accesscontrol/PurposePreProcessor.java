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
package org.apache.atlas.repository.store.graph.v2.preprocessor.accesscontrol;


import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.store.aliasstore.ESAliasStore;
import org.apache.atlas.repository.store.aliasstore.IndexAliasStore;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PURPOSE_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_ACCESS_CONTROL_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_PURPOSE_CLASSIFICATIONS;
import static org.apache.atlas.repository.util.AccessControlUtils.REL_ATTR_POLICIES;
import static org.apache.atlas.repository.util.AccessControlUtils.getESAliasName;
import static org.apache.atlas.repository.util.AccessControlUtils.getEntityName;
import static org.apache.atlas.repository.util.AccessControlUtils.getIsEnabled;
import static org.apache.atlas.repository.util.AccessControlUtils.getPurposeTags;
import static org.apache.atlas.repository.util.AccessControlUtils.getTenantId;
import static org.apache.atlas.repository.util.AccessControlUtils.getUUID;
import static org.apache.atlas.repository.util.AccessControlUtils.validateUniquenessByName;
import static org.apache.atlas.repository.util.AccessControlUtils.validateUniquenessByTags;

public class PurposePreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PurposePreProcessor.class);

    private final AtlasGraph graph;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private IndexAliasStore aliasStore;
    private AtlasEntityStore entityStore;

    public PurposePreProcessor(AtlasGraph graph,
                               AtlasTypeRegistry typeRegistry,
                               EntityGraphRetriever entityRetriever,
                               AtlasEntityStore entityStore) {
        this.graph = graph;
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
        this.entityStore = entityStore;

        aliasStore = new ESAliasStore(graph, entityRetriever);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("PurposePreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreatePurpose(entity);
                break;
            case UPDATE:
                processUpdatePurpose(context, entity, context.getVertex(entity.getGuid()));
                break;
        }
    }

    private void processCreatePurpose(AtlasStruct entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreatePurpose");

        String tenantId = getTenantId(entity);

        entity.setAttribute(QUALIFIED_NAME, String.format("%s/%s", tenantId, getUUID()));
        entity.setAttribute(ATTR_ACCESS_CONTROL_ENABLED, true);

        //create ES alias
        aliasStore.createAlias((AtlasEntity) entity);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdatePurpose(EntityMutationContext context,
                                      AtlasStruct entity,
                                      AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdatePurpose");

        AtlasEntity purpose = (AtlasEntity) entity;
        AtlasEntity.AtlasEntityWithExtInfo existingPurposeExtInfo = entityRetriever.toAtlasEntityWithExtInfo(vertex);
        AtlasEntity existingPurposeEntity = existingPurposeExtInfo.getEntity();

        String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);
        purpose.setAttribute(QUALIFIED_NAME, vertexQName);

        if (!AtlasEntity.Status.ACTIVE.equals(existingPurposeEntity.getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Purpose not Active");
        }

        boolean isEnabled = getIsEnabled(purpose);
        if (getIsEnabled(existingPurposeEntity) != isEnabled) {
            if (isEnabled) {
                //TODO
                //enablePurpose(existingPurposeWithExtInfo);
            } else {
                //TODO
                //disablePurpose(existingPurposeWithExtInfo);
            }
        }

        String newName = getEntityName(purpose);

        if (!newName.equals(getEntityName(existingPurposeEntity))) {
            validateUniquenessByName(graph, newName, PURPOSE_ENTITY_TYPE);
        }

        List<String> newTags = getPurposeTags(purpose);

        if (!CollectionUtils.isEmpty(newTags) && !CollectionUtils.isEqualCollection(newTags, getPurposeTags(existingPurposeEntity))) {
            validateUniquenessByTags(graph, newTags, PURPOSE_ENTITY_TYPE);

            List<AtlasObjectId> policies = (List<AtlasObjectId>) existingPurposeEntity.getRelationshipAttribute(REL_ATTR_POLICIES);

            if (CollectionUtils.isNotEmpty(policies)) {
                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(POLICY_ENTITY_TYPE);
                List<String> newTagsResources = newTags.stream().map(x -> "tag:" + x).collect(Collectors.toList());

                for (AtlasObjectId policy : policies) {
                    AtlasVertex policyVertex = entityRetriever.getEntityVertex(policy.getGuid());

                    policyVertex.removeProperty(ATTR_POLICY_RESOURCES);
                    newTagsResources.forEach(x -> policyVertex.setProperty(ATTR_POLICY_RESOURCES, x));

                    AtlasEntity policyToBeUpdated = entityRetriever.toAtlasEntity(policyVertex);

                    context.addUpdated(policyToBeUpdated.getGuid(), policyToBeUpdated, entityType, policyVertex);

                    existingPurposeExtInfo.addReferredEntity(policyToBeUpdated);
                }
            }

            existingPurposeExtInfo.getEntity().setAttribute(ATTR_PURPOSE_CLASSIFICATIONS, newTags);
            aliasStore.updateAlias(existingPurposeExtInfo, null);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(vertex);
        AtlasEntity purpose = entityWithExtInfo.getEntity();

        if(!purpose.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
            LOG.info("Purpose with guid {} is already deleted/purged", purpose.getGuid());
            return;
        }

        //delete policies
        List<AtlasObjectId> policies = (List<AtlasObjectId>) purpose.getRelationshipAttribute(REL_ATTR_POLICIES);

        for (AtlasObjectId policyObjectId : policies) {
            entityStore.deleteById(policyObjectId.getGuid());
        }

        //delete ES alias
        aliasStore.deleteAlias(getESAliasName(purpose));
    }

    public static String createQualifiedName() {
        return getUUID();
    }
}
