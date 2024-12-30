package org.apache.atlas.repository.store.graph.v2.preprocessor.model;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.isNameInvalid;

public class DMAttributePreprocessor extends AbstractModelPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DMAttributePreprocessor.class);

    public DMAttributePreprocessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, EntityGraphMapper entityGraphMapper, AtlasRelationshipStore atlasRelationshipStore) {
        super(typeRegistry, entityRetriever, entityGraphMapper, atlasRelationshipStore);
    }


    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        //Handle name & qualifiedName
        if (LOG.isDebugEnabled()) {
            LOG.debug("ModelPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                createDMAttribute(entity, vertex, context);
                break;
            case UPDATE:
                updateDMAttributes(entity, vertex, context);
        }
    }

    private void createDMAttribute(AtlasEntity entityAttribute, AtlasVertex vertexAttribute, EntityMutationContext context) throws AtlasBaseException {
        if (!entityAttribute.getTypeName().equals(ATLAS_DM_ATTRIBUTE_TYPE)) {
            return;
        }
        if (CollectionUtils.isEmpty(context.getCreatedEntities())) {
            return;
        }

        String entityName = (String) entityAttribute.getAttribute(NAME);

        if (StringUtils.isEmpty(entityName) || isNameInvalid(entityName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }
        long now = RequestContext.get().getRequestTime();


        // get entity qualifiedName with qualifiedNamePrefix
        String attributeQualifiedNamePrefix = (String) entityAttribute.getAttributes().get(ATLAS_DM_QUALIFIED_NAME_PREFIX);
        int lastIndex = attributeQualifiedNamePrefix.lastIndexOf("/");
        String entityQualifiedNamePrefix = attributeQualifiedNamePrefix.substring(0, lastIndex);
        String namespace = (String) entityAttribute.getAttributes().get(ATLAS_DM_NAMESPACE);
        String modelVersion = "v1";

        ModelResponse modelENtityResponse = null;
        AtlasVertex latestEntityVertex = AtlasGraphUtilsV2.findLatestEntityAttributeVerticesByType(ATLAS_DM_ENTITY_TYPE, entityQualifiedNamePrefix);
        lastIndex = entityQualifiedNamePrefix.lastIndexOf("/");

        String modelQualifiedName = entityQualifiedNamePrefix.substring(0, lastIndex);

        Map<String, Object> attrValues = new HashMap<>();
        attrValues.put(QUALIFIED_NAME, modelQualifiedName);
        String modelGuid = AtlasGraphUtilsV2.getGuidByUniqueAttributes(
                typeRegistry.getEntityTypeByName(ATLAS_DM_DATA_MODEL),
                attrValues);

        List<AtlasRelatedObjectId> existingAttributes = null;

        //
        if (latestEntityVertex != null) {
            modelENtityResponse = replicateModelEntity(
                    entityRetriever.toAtlasEntity(latestEntityVertex),
                    latestEntityVertex,
                    entityQualifiedNamePrefix,
                    now
            );
            modelVersion = "v2";
            if (modelENtityResponse.getExistingEntity() != null && modelENtityResponse.getExistingEntity().getRelationshipAttributes() != null) {
                existingAttributes = (List<AtlasRelatedObjectId>) modelENtityResponse.getExistingEntity().getRelationshipAttributes().get("dMAttributes");
            }
        } else {
            int lastSlashIndex = entityQualifiedNamePrefix.lastIndexOf("/");

            // Extract the substring after the last "/"
            String name = entityQualifiedNamePrefix.substring(lastSlashIndex + 1);
            modelENtityResponse = createEntity(
                    entityQualifiedNamePrefix + "_" + now,
                    name,
                    ATLAS_DM_ENTITY_TYPE,
                    namespace,
                    context
            );
        }

        List<AtlasRelatedObjectId> existingEntities = null;
        ModelResponse modelVersionResponse = replicateModelVersion(modelGuid, modelQualifiedName, now);
        if (modelVersionResponse.getReplicaEntity() == null) {
            modelVersionResponse = createEntity(
                    (modelQualifiedName + "/" + modelVersion),
                    modelVersion,
                    ATLAS_DM_VERSION_TYPE,
                    namespace,
                    context);
        }
        AtlasEntity latestModelVersionEntity = modelVersionResponse.getReplicaEntity();
        AtlasVertex latestModelVersionVertex = modelVersionResponse.getReplicaVertex();


        // model --- modelVersion relation
        createModelModelVersionRelation(modelGuid, latestModelVersionEntity.getGuid());

        // modelVersion --- entity relation
        createModelVersionModelEntityRelationship(latestModelVersionVertex, modelENtityResponse.getReplicaVertex());

        // modelVersion --- entitiesOfExistingModelVersion
        if (modelVersionResponse.getExistingEntity() != null && modelVersionResponse.getExistingEntity().getRelationshipAttributes() != null) {
            existingEntities = (List<AtlasRelatedObjectId>) modelVersionResponse.getExistingEntity().getRelationshipAttributes().get("dMEntities");
            createModelVersionModelEntityRelationship(latestModelVersionVertex, existingEntities);
        }

        // entity --- attributes of existingEntity relation
        createModelEntityModelAttributeRelation(modelENtityResponse.getReplicaVertex(), existingAttributes);

        // latest entity ---- new attribute relation
        createModelEntityModelAttributeRelation(modelENtityResponse.getReplicaVertex(), vertexAttribute);

        context.addCreated(latestModelVersionEntity.getGuid(), latestModelVersionEntity,
                typeRegistry.getEntityTypeByName(ATLAS_DM_VERSION_TYPE), latestModelVersionVertex);

        context.addCreated(modelENtityResponse.getReplicaEntity().getGuid(), modelENtityResponse.getReplicaEntity(),
                typeRegistry.getEntityTypeByName(ATLAS_DM_ENTITY_TYPE), modelENtityResponse.getReplicaVertex());

        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        modelGuid,
                        entityRetriever.getEntityVertex(modelGuid));

        entityAttribute.setRelationshipAttributes(processRelationshipAttributesForAttribute(entityAttribute, entityAttribute.getRelationshipAttributes(), context));
    }

    private void updateDMAttributes(AtlasEntity entityAttribute, AtlasVertex vertexAttribute, EntityMutationContext context) throws AtlasBaseException {
        ModelResponse modelResponseParentEntity = updateDMAttribute(entityAttribute, vertexAttribute, context);
        // case when a mapping is added
        if (entityAttribute.getAppendRelationshipAttributes() != null) {
            Map<String, Object> appendRelationshipAttributes = processRelationshipAttributesForAttribute(entityAttribute, entityAttribute.getAppendRelationshipAttributes(), context);
            modelResponseParentEntity.getReplicaEntity().setAppendRelationshipAttributes(new HashMap<>(appendRelationshipAttributes));
            context.removeUpdatedWithRelationshipAttributes(entityAttribute);
        }

        if (entityAttribute.getRemoveRelationshipAttributes() != null) {
            Map<String, Object> appendRelationshipAttributes = processRelationshipAttributesForAttribute(entityAttribute, entityAttribute.getRemoveRelationshipAttributes(), context);
            modelResponseParentEntity.getReplicaEntity().setRemoveRelationshipAttributes(appendRelationshipAttributes);
            context.removeUpdatedWithDeleteRelationshipAttributes(entityAttribute);
        }
    }

}
