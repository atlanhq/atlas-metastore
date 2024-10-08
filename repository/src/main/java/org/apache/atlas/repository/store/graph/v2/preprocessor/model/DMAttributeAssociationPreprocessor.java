package org.apache.atlas.repository.store.graph.v2.preprocessor.model;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.isNameInvalid;

public class DMAttributeAssociationPreprocessor extends AbstractModelPreProcessor{
    private static final Logger LOG = LoggerFactory.getLogger(DMAttributeAssociationPreprocessor.class);

    public DMAttributeAssociationPreprocessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, EntityGraphMapper entityGraphMapper, AtlasRelationshipStore atlasRelationshipStore) {
        super(typeRegistry, entityRetriever, entityGraphMapper, atlasRelationshipStore);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("ModelPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                createDMAttributeAssociation(entity, vertex, context);
                break;
            case UPDATE:
                updateDMAttributeAssociation(entity, vertex, context);
        }
    }

    private void createDMAttributeAssociation(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        if (!entity.getTypeName().equals(MODEL_ATTRIBUTE)) {
            return;
        }

        String entityName = (String) entity.getAttribute(NAME);

        if (StringUtils.isEmpty(entityName) || isNameInvalid(entityName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        entity.setRelationshipAttributes(
                processRelationshipAttributesForAttribute(entity, entity.getRelationshipAttributes(), context));
    }
    private void updateDMAttributeAssociation(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        if (!entity.getTypeName().equals(MODEL_ENTITY)) {
            return;
        }

        String entityName = (String) entity.getAttribute(NAME);

        if (StringUtils.isEmpty(entityName) || isNameInvalid(entityName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }


        long now = RequestContext.get().getRequestTime();

        ModelResponse modelResponse = replicateDMAssociation(entity, vertex, now);
        AtlasEntity copyEntity = modelResponse.getReplicaEntity();
        AtlasVertex copyVertex = modelResponse.getReplicaVertex();
        applyDiffs(entity, copyEntity, MODEL_ATTRIBUTE_ASSOCIATION);
        unsetExpiredDates(copyEntity, copyVertex);

        // case when a mapping is added
        if (entity.getRelationshipAttributes() != null) {
            Map<String, Object> appendRelationshipAttributes = processRelationshipAttributesForAttribute(entity, entity.getRemoveRelationshipAttributes(), context);
            modelResponse.getReplicaEntity().setRelationshipAttributes(appendRelationshipAttributes);
        }

        // case when a mapping is added
        if (entity.getAppendRelationshipAttributes() != null) {
            Map<String, Object> appendRelationshipAttributes = processRelationshipAttributesForAttribute(entity, entity.getAppendRelationshipAttributes(), context);
            modelResponse.getReplicaEntity().setAppendRelationshipAttributes(appendRelationshipAttributes);
            context.removeUpdatedWithRelationshipAttributes(entity);
            context.setUpdatedWithRelationshipAttributes(modelResponse.getReplicaEntity());
        }

        if (entity.getRemoveRelationshipAttributes() != null) {
            Map<String, Object> appendRelationshipAttributes = processRelationshipAttributesForAttribute(entity, entity.getRemoveRelationshipAttributes(), context);
            modelResponse.getReplicaEntity().setRemoveRelationshipAttributes(appendRelationshipAttributes);
            context.removeUpdatedWithDeleteRelationshipAttributes(entity);
            context.setUpdatedWithRemoveRelationshipAttributes(modelResponse.getReplicaEntity());
        }
    }
}
