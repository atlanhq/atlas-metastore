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
import org.apache.atlas.type.AtlasEntityType;
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
                break;
        }
    }

    private void createDMAttribute(AtlasEntity entityAttribute, AtlasVertex vertexAttribute, EntityMutationContext context) throws AtlasBaseException {
        if (!entityAttribute.getTypeName().equals(MODEL_ATTRIBUTE)) {
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
        String attributeQualifiedNamePrefix = (String) entityAttribute.getAttributes().get(MODEL_QUALIFIED_NAME_PATTERN);
        int lastIndex = attributeQualifiedNamePrefix.lastIndexOf("/");
        String entityQualifiedNamePrefix = attributeQualifiedNamePrefix.substring(0, lastIndex);
        String namespace = (String) entityAttribute.getAttributes().get(MODEL_NAMESPACE);
        String modelVersion = "v2";

        lastIndex = entityQualifiedNamePrefix.lastIndexOf("/");
        String modelQualifiedName = entityQualifiedNamePrefix.substring(0, lastIndex);
        String modelGuid = context.getModel(modelQualifiedName);
        LOG.info("model retrieved from cache: " + StringUtils.isNotEmpty(modelGuid));

        if (StringUtils.isEmpty(modelGuid)){
            Map<String, Object> attrValues = new HashMap<>();
            attrValues.put(QUALIFIED_NAME, modelQualifiedName);
             modelGuid = AtlasGraphUtilsV2.getGuidByUniqueAttributes(
                    typeRegistry.getEntityTypeByName(MODEL_DATA_MODEL),
                    attrValues);

            if (StringUtils.isEmpty(modelGuid)){
                throw new AtlasBaseException(AtlasErrorCode.DATA_MODEL_NOT_EXIST);
            }
            context.cacheModel(modelQualifiedName, modelGuid);
        }

        ModelResponse modelENtityResponse = context.getModelEntity(entityQualifiedNamePrefix);

        if (modelENtityResponse == null) {
            AtlasVertex latestEntityVertex = AtlasGraphUtilsV2.findLatestEntityAttributeVerticesByType(MODEL_ENTITY, entityQualifiedNamePrefix);
            if (latestEntityVertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.DATA_ENTITY_NOT_EXIST);
            }
            modelENtityResponse = replicateModelEntity(
                    entityRetriever.toAtlasEntity(latestEntityVertex),
                    latestEntityVertex,
                    entityQualifiedNamePrefix,
                    now
            );
        }

        ModelResponse modelVersionResponse = context.getModelVersion(modelQualifiedName);
        LOG.info("model version retrieved from cache: " + (modelVersionResponse != null));

        if (modelVersionResponse == null) {
            modelVersionResponse = replicateModelVersion(modelGuid, modelQualifiedName, now);
            if (modelVersionResponse.getReplicaEntity() == null) {
                modelVersionResponse = createEntity(
                        (modelQualifiedName + "/" + modelVersion),
                        modelVersion,
                        MODEL_VERSION,
                        namespace,
                        context);
            }
            createModelModelVersionRelation(modelGuid, modelVersionResponse.getReplicaEntity().getGuid());
            context.cacheModelVersion(modelQualifiedName, modelVersionResponse);
        }

        AtlasEntity latestModelVersionEntity = modelVersionResponse.getReplicaEntity();
        AtlasVertex latestModelVersionVertex = modelVersionResponse.getReplicaVertex();


        // model --- modelVersion relation
        createModelModelVersionRelation(modelGuid, latestModelVersionEntity.getGuid());

        // modelVersion --- entity relation
        createModelVersionModelEntityRelationship(latestModelVersionVertex, modelENtityResponse.getReplicaVertex());

        // modelVersion --- entitiesOfExistingModelVersion
        if (modelVersionResponse.getExistingEntity() != null && modelVersionResponse.getExistingEntity().getRelationshipAttributes() != null) {
            List<AtlasRelatedObjectId> existingEntities = (List<AtlasRelatedObjectId>) modelVersionResponse.getExistingEntity().getRelationshipAttributes().get(MODEL_VERSION_ENTITIES);
            createModelVersionModelEntityRelationship(latestModelVersionVertex, modelENtityResponse.getExistingEntity(), existingEntities, context);
        }

        // entity --- attributes of existingEntity relation
        if (modelENtityResponse.getExistingEntity() != null && modelENtityResponse.getExistingEntity().getRelationshipAttributes() != null) {
            List<AtlasRelatedObjectId>  existingAttributes = (List<AtlasRelatedObjectId>) modelENtityResponse.getExistingEntity().getRelationshipAttributes().get(MODEL_ENTITY_ATTRIBUTES);
            createModelEntityModelAttributeRelation(modelENtityResponse.getReplicaVertex(), null, existingAttributes, context);
        }

        // latest entity ---- new attribute relation
        createModelEntityModelAttributeRelation(modelENtityResponse.getReplicaVertex(), vertexAttribute);

        AtlasEntityType entityType= typeRegistry.getEntityTypeByName(MODEL_ENTITY);
        AtlasEntityType versionType= typeRegistry.getEntityTypeByName(MODEL_VERSION);

        context.addCreated(latestModelVersionEntity.getGuid(), latestModelVersionEntity,
                versionType, latestModelVersionVertex);

        context.addCreated(modelENtityResponse.getReplicaEntity().getGuid(), modelENtityResponse.getReplicaEntity(),
                entityType, modelENtityResponse.getReplicaVertex());

        if (modelENtityResponse.getExistingEntity() != null) {
            context.removeUpdated(modelENtityResponse.getExistingEntity().getGuid(),
                    modelENtityResponse.getExistingEntity(), entityType, modelENtityResponse.getExistingVertex());
            context.addUpdated(modelENtityResponse.getExistingEntity().getGuid(),
                    modelENtityResponse.getExistingEntity(), entityType, modelENtityResponse.getExistingVertex());
        }
        if (modelVersionResponse.getExistingEntity() != null) {
            context.removeUpdated(modelVersionResponse.getExistingEntity().getGuid(), modelVersionResponse.getExistingEntity(),
                    versionType, modelVersionResponse.getExistingVertex());
            context.addUpdated(modelVersionResponse.getExistingEntity().getGuid(), modelVersionResponse.getExistingEntity(),
                    versionType, modelVersionResponse.getExistingVertex());
        }

        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        modelGuid,
                        entityRetriever.getEntityVertex(modelGuid));

        entityAttribute.setRelationshipAttributes(processRelationshipAttributesForAttribute(entityAttribute, entityAttribute.getRelationshipAttributes(), context));
    }

    private void updateDMAttributes(AtlasEntity entityAttribute, AtlasVertex vertexAttribute, EntityMutationContext context) throws AtlasBaseException {
        ModelResponse modelResponseParentEntity = updateDMAttribute(entityAttribute, vertexAttribute, context);

        if (entityAttribute.getRelationshipAttributes() != null) {
            Map<String, Object> appendRelationshipAttributes = processRelationshipAttributesForEntity(entityAttribute, entityAttribute.getRelationshipAttributes(), context);
            modelResponseParentEntity.getReplicaEntity().setRelationshipAttributes(appendRelationshipAttributes);
        }

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
