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
import org.springframework.stereotype.Component;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.isNameInvalid;

@Component
public class DMEntityPreProcessor extends AbstractModelPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DMEntityPreProcessor.class);


    public DMEntityPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, EntityGraphMapper entityGraphMapper, AtlasRelationshipStore atlasRelationshipStore) {
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
                createDMEntity(entity, vertex, context);
                break;
            case UPDATE:
                updateDMEntities(entity, vertex, context);
                break;
        }
    }

    private void createDMEntity(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        if (!entity.getTypeName().equals(ATLAS_DM_ENTITY_TYPE)) {
            return;
        }
        if (CollectionUtils.isEmpty(context.getCreatedEntities())) {
            return;
        }

        String entityName = (String) entity.getAttribute(NAME);

        if (StringUtils.isEmpty(entityName) || isNameInvalid(entityName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }
        long now = RequestContext.get().getRequestTime();

        // get model qualifiedName with qualifiedNamePrefix
        String qualifiedNamePrefix = (String) entity.getAttributes().get(ATLAS_DM_QUALIFIED_NAME_PREFIX);
        int lastIndex = qualifiedNamePrefix.lastIndexOf("/");
        String modelQualifiedName = qualifiedNamePrefix.substring(0, lastIndex);
        Map<String, Object> attrValues = new HashMap<>();
        attrValues.put(QUALIFIED_NAME, modelQualifiedName);

        AtlasVertex modelVertex = AtlasGraphUtilsV2.findByUniqueAttributes(
                typeRegistry.getEntityTypeByName(ATLAS_DM_DATA_MODEL), attrValues);

        if (modelVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.DATA_MODEL_NOT_EXIST);
        }

        String modelGuid = AtlasGraphUtilsV2.getIdFromVertex(modelVertex);

        ModelResponse modelVersionResponse = replicateModelVersion(modelGuid, modelQualifiedName, now);

        // create modelVersion
        if (modelVersionResponse.getReplicaEntity() == null) {
            String namespace = (String) entity.getAttributes().get(ATLAS_DM_NAMESPACE);
            modelVersionResponse = createEntity(
                    (modelQualifiedName + "/" + "v1"),
                    "v1",
                    ATLAS_DM_VERSION_TYPE,
                    namespace,
                    context);
        }

        AtlasEntity latestModelVersionEntity = modelVersionResponse.getReplicaEntity();
        AtlasVertex latestModelVersionVertex = modelVersionResponse.getReplicaVertex();


        // model --- modelVersion relation
        createModelModelVersionRelation(modelGuid, latestModelVersionEntity.getGuid());

        // modelVersion --- entity relation
        createModelVersionModelEntityRelationship(latestModelVersionVertex, vertex);

        if (modelVersionResponse.getExistingEntity() != null) {
            List<AtlasRelatedObjectId> existingEntities = (List<AtlasRelatedObjectId>) modelVersionResponse.getExistingEntity()
                    .getRelationshipAttributes()
                    .get("dMEntities");
            // modelVersion --- entitiesOfExistingModelVersion
            createModelVersionModelEntityRelationship(latestModelVersionVertex, existingEntities);

        }
        context.addCreated(latestModelVersionEntity.getGuid(), latestModelVersionEntity,
                typeRegistry.getEntityTypeByName(ATLAS_DM_VERSION_TYPE), latestModelVersionVertex);
        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        modelGuid,
                        entityRetriever.getEntityVertex(modelGuid));

        entity.setRelationshipAttributes(
                processRelationshipAttributesForEntity(entity, entity.getRelationshipAttributes(), context));
    }

    private void updateDMEntities(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        ModelResponse modelResponseParentEntity = updateDMEntity(entity, vertex, context);

        // case when a mapping is added
        if (entity.getAppendRelationshipAttributes() != null) {
            Map<String, Object> appendRelationshipAttributes = processRelationshipAttributesForEntity(entity, entity.getAppendRelationshipAttributes(), context);
            modelResponseParentEntity.getReplicaEntity().setAppendRelationshipAttributes(appendRelationshipAttributes);
            context.removeUpdatedWithRelationshipAttributes(entity);
            context.setUpdatedWithRelationshipAttributes(modelResponseParentEntity.getReplicaEntity());
        }

        if (entity.getRemoveRelationshipAttributes() != null) {
            Map<String, Object> appendRelationshipAttributes = processRelationshipAttributesForEntity(entity, entity.getRemoveRelationshipAttributes(), context);
            modelResponseParentEntity.getReplicaEntity().setRemoveRelationshipAttributes(appendRelationshipAttributes);
            context.removeUpdatedWithDeleteRelationshipAttributes(entity);
            context.setUpdatedWithRemoveRelationshipAttributes(modelResponseParentEntity.getReplicaEntity());
        }
    }
}



