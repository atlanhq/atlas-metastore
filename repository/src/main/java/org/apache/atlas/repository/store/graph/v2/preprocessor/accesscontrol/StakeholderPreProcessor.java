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


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.STAKEHOLDER_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.STAKEHOLDER_TITLE_ENTITY_TYPE;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.indexSearchPaginated;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.verifyDuplicateAssetByName;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh.StakeholderTitlePreProcessor.ATTR_DOMAIN_QUALIFIED_NAMES;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh.StakeholderTitlePreProcessor.STAR;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_ACCESS_CONTROL_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_PERSONA_ROLE_ID;
import static org.apache.atlas.repository.util.AccessControlUtils.REL_ATTR_POLICIES;
import static org.apache.atlas.repository.util.AccessControlUtils.getESAliasName;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaRoleId;
import static org.apache.atlas.repository.util.AccessControlUtils.getUUID;
import static org.apache.atlas.repository.util.AccessControlUtils.validateNoPoliciesAttached;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class StakeholderPreProcessor extends PersonaPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(StakeholderPreProcessor.class);

    public static final String ATTR_DOMAIN_QUALIFIED_NAME  = "domainQualifiedName";
    public static final String ATTR_STAKEHOLDER_TITLE_GUID = "stakeholderTitleGuid";

    public static final String REL_ATTR_STAKEHOLDER_TITLE = "stakeholderTitle";
    public static final String REL_ATTR_STAKEHOLDER_DOMAIN = "dataDomain";

    protected EntityDiscoveryService discovery;

    public StakeholderPreProcessor(AtlasGraph graph,
                                   AtlasTypeRegistry typeRegistry,
                                   EntityGraphRetriever entityRetriever,
                                   AtlasEntityStore entityStore) {
        super(graph, typeRegistry, entityRetriever, entityStore);

        try {
            this.discovery = new EntityDiscoveryService(typeRegistry, graph, null, null, null, null);
        } catch (AtlasException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("StakeholderPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreateStakeholder(entity);
                break;
            case UPDATE:
                processUpdateStakeholder(context, entity);
                break;
        }
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(vertex);
        AtlasEntity persona = entityWithExtInfo.getEntity();

        if(!persona.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
            LOG.info("Persona with guid {} is already deleted/purged", persona.getGuid());
            return;
        }

        //delete policies
        List<AtlasObjectId> policies = (List<AtlasObjectId>) persona.getRelationshipAttribute(REL_ATTR_POLICIES);
        if (CollectionUtils.isNotEmpty(policies)) {
            for (AtlasObjectId policyObjectId : policies) {
                //AtlasVertex policyVertex = entityRetriever.getEntityVertex(policyObjectId.getGuid());
                entityStore.deleteById(policyObjectId.getGuid());
            }
        }

        //remove role
        keycloakStore.removeRole(getPersonaRoleId(persona));

        //delete ES alias
        aliasStore.deleteAlias(getESAliasName(persona));
    }

    private void processCreateStakeholder(AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateStakeholder");

        validateNoPoliciesAttached(entity);

        if (!entity.hasRelationshipAttribute(REL_ATTR_STAKEHOLDER_TITLE) || !entity.hasRelationshipAttribute(REL_ATTR_STAKEHOLDER_DOMAIN)) {
            throw new AtlasBaseException(BAD_REQUEST, "Relations stakeholderTitle and dataDomain are mandatory");
        }

        String domainQualifiedName = getQualifiedNameFromRelationAttribute(entity, REL_ATTR_STAKEHOLDER_DOMAIN);
        String stakeholderTitleGuid = getGuidFromRelationAttribute(entity, REL_ATTR_STAKEHOLDER_TITLE);

        ensureTitleAvailableForDomain(domainQualifiedName, stakeholderTitleGuid);

        //validate Stakeholder & StakeholderTitle pair is unique for this domain
        verifyDuplicateByDomainAndTitle(domainQualifiedName, stakeholderTitleGuid);

        //validate Name uniqueness for Stakeholders across this domain
        String name = (String) entity.getAttribute(NAME);
        verifyDuplicateAssetByName(STAKEHOLDER_ENTITY_TYPE, name, discovery,
                String.format("Stakeholder with name %s already exists for current domain", name));

        entity.setAttribute(ATTR_DOMAIN_QUALIFIED_NAME, domainQualifiedName);
        entity.setAttribute(ATTR_STAKEHOLDER_TITLE_GUID, stakeholderTitleGuid);

        String personaQualifiedName = String.format("default/%s/%s",
                getUUID(),
                domainQualifiedName);

        entity.setAttribute(QUALIFIED_NAME, personaQualifiedName);


        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, new AtlasEntityHeader(entity)),
                "create Stakeholder: ", entity.getAttribute(NAME));

        entity.setAttribute(ATTR_ACCESS_CONTROL_ENABLED, entity.getAttributes().getOrDefault(ATTR_ACCESS_CONTROL_ENABLED, true));

        //create keycloak role
        String roleId = createKeycloakRole(entity);

        entity.setAttribute(ATTR_PERSONA_ROLE_ID, roleId);

        //create ES alias
        aliasStore.createAlias(entity);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateStakeholder(EntityMutationContext context, AtlasEntity stakeholder) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateStakeholder");

        validateNoPoliciesAttached(stakeholder);

        validateNoPoliciesAttached(stakeholder);
        AtlasVertex vertex = context.getVertex(stakeholder.getGuid());

        AtlasEntity existingStakeholderEntity = entityRetriever.toAtlasEntityWithExtInfo(vertex).getEntity();

        if (!AtlasEntity.Status.ACTIVE.equals(existingStakeholderEntity.getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Stakeholder not Active");
        }

        stakeholder.removeAttribute(ATTR_DOMAIN_QUALIFIED_NAME);
        stakeholder.removeAttribute(ATTR_STAKEHOLDER_TITLE_GUID);
        stakeholder.getRelationshipAttributes().remove(REL_ATTR_STAKEHOLDER_DOMAIN);
        stakeholder.getRelationshipAttributes().remove(REL_ATTR_STAKEHOLDER_TITLE);


        String currentName = vertex.getProperty(NAME, String.class);
        String newName = (String) stakeholder.getAttribute(NAME);

        if (!currentName.equals(newName)) {
            verifyDuplicateAssetByName(STAKEHOLDER_ENTITY_TYPE, newName, discovery,
                    String.format("Stakeholder with name %s already exists for current domain", newName));
        }

        String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);
        stakeholder.setAttribute(QUALIFIED_NAME, vertexQName);
        stakeholder.setAttribute(ATTR_PERSONA_ROLE_ID, getPersonaRoleId(existingStakeholderEntity));

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, new AtlasEntityHeader(stakeholder)),
                "update Stakeholder: ", stakeholder.getAttribute(NAME));

        updateKeycloakRole(stakeholder, existingStakeholderEntity);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private String getGuidFromRelationAttribute(AtlasEntity entity, String relationshipAttributeName) throws AtlasBaseException {
        AtlasObjectId relationObjectId = (AtlasObjectId) entity.getRelationshipAttribute(relationshipAttributeName);

        String guid = relationObjectId.getGuid();
        if (StringUtils.isEmpty(guid)) {
            AtlasVertex vertex = entityRetriever.getEntityVertex(relationObjectId);
            guid = vertex.getProperty("__guid", String.class);
        }

        return guid;
    }

    private String getQualifiedNameFromRelationAttribute(AtlasEntity entity, String relationshipAttributeName) throws AtlasBaseException {
        AtlasObjectId relationObjectId = (AtlasObjectId) entity.getRelationshipAttribute(relationshipAttributeName);
        String qualifiedName = null;

        if (relationObjectId.getUniqueAttributes() != null) {
            qualifiedName = (String) relationObjectId.getUniqueAttributes().get(QUALIFIED_NAME);
        }

        if (StringUtils.isEmpty(qualifiedName)) {
            AtlasVertex vertex = entityRetriever.getEntityVertex(relationObjectId);
            qualifiedName = vertex.getProperty(QUALIFIED_NAME, String.class);
        }

        return qualifiedName;
    }

    protected void verifyDuplicateByDomainAndTitle(String domainQualifiedName, String stakeholderTitleGuid) throws AtlasBaseException {

        List<Map<String, Object>> mustClauseList = new ArrayList();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", STAKEHOLDER_ENTITY_TYPE)));
        mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
        mustClauseList.add(mapOf("term", mapOf(ATTR_DOMAIN_QUALIFIED_NAME, domainQualifiedName)));
        mustClauseList.add(mapOf("term", mapOf(ATTR_STAKEHOLDER_TITLE_GUID, stakeholderTitleGuid)));


        Map<String, Object> bool = mapOf("must", mustClauseList);

        Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

        List<AtlasEntityHeader> assets = indexSearchPaginated(dsl, null, this.discovery);

        if (CollectionUtils.isNotEmpty(assets)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                    String.format("Stakeholder for provided title & domain combination already exists"));
        }
    }

    protected void ensureTitleAvailableForDomain(String domainQualifiedName, String stakeholderTitleGuid) throws AtlasBaseException {

        List<Map<String, Object>> mustClauseList = new ArrayList();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", STAKEHOLDER_TITLE_ENTITY_TYPE)));
        mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
        mustClauseList.add(mapOf("term", mapOf("__guid", stakeholderTitleGuid)));

        /*List<Map<String, Object>> shouldClauseList = new ArrayList();
        shouldClauseList.add(mapOf("term", mapOf("domainQualifiedNames", domainQualifiedName)));
        shouldClauseList.add(mapOf("term", mapOf("domainQualifiedNames", "*")));
        mustClauseList.add(mapOf("bool", mapOf("should", shouldClauseList)));
        */

        Map<String, Object> bool = mapOf("must", mustClauseList);

        Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

        List<AtlasEntityHeader> assets = indexSearchPaginated(dsl, Collections.singleton(ATTR_DOMAIN_QUALIFIED_NAMES), this.discovery);

        if (CollectionUtils.isNotEmpty(assets)) {
            AtlasEntityHeader stakeholderTitleHeader = assets.get(0);

            List<String> domainQualifiedNames = (List<String>) stakeholderTitleHeader.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);

            if (!domainQualifiedNames.contains(STAR)) {
                Optional parentDomain = domainQualifiedNames.stream().filter(x -> domainQualifiedName.startsWith(x)).findFirst();

                if (!parentDomain.isPresent()) {
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                            String.format("Provided StakeholderTitle is not applicable to the domain"));
                }
            }
        }
    }
}
