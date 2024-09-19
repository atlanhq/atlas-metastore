package org.apache.atlas.repository.store.graph.v2.preprocessor.model;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

public abstract class AbstractModelPreProcessor implements PreProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractModelPreProcessor.class);
    private static final String ATTRIBUTE_TYPE = "DMAttribute";

    protected final AtlasTypeRegistry typeRegistry;

    protected final EntityGraphRetriever entityRetriever;

    protected EntityGraphMapper entityGraphMapper;
    protected AtlasRelationshipStore atlasRelationshipStore;

    public AbstractModelPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, EntityGraphMapper entityGraphMapper, AtlasRelationshipStore atlasRelationshipStore) {
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
        this.entityGraphMapper = entityGraphMapper;
        this.atlasRelationshipStore = atlasRelationshipStore;
    }

    protected static class ModelResponse {

        private AtlasEntity existingEntity;
        private AtlasEntity copyEntity;
        private AtlasVertex existingVertex;
        private AtlasVertex copyVertex;

        protected ModelResponse(AtlasEntity existingEntity, AtlasEntity copyEntity,
                                AtlasVertex existingVertex, AtlasVertex copyVertex) {
            this.existingEntity = existingEntity;
            this.copyEntity = copyEntity;
            this.existingVertex = existingVertex;
            this.copyVertex = copyVertex;
        }

        protected ModelResponse(AtlasEntity copyEntity, AtlasVertex copyVertex) {
            this.copyEntity = copyEntity;
            this.copyVertex = copyVertex;
        }

        public AtlasEntity getExistingEntity() {
            return existingEntity;
        }

        public AtlasEntity getCopyEntity() {
            return copyEntity;
        }

        public AtlasVertex getExistingVertex() {
            return existingVertex;
        }

        public AtlasVertex getCopyVertex() {
            return copyVertex;
        }
    }

    protected void setModelDates(AtlasEntity newEntity, AtlasVertex newVertex, Object value) {
        newEntity.setAttribute(ATLAS_DM_SYSTEM_DATE, value);
        newEntity.setAttribute(ATLAS_DM_BUSINESS_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, ATLAS_DM_SYSTEM_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, ATLAS_DM_BUSINESS_DATE, value);
    }

    protected void setModelExpiredAtDates(AtlasEntity oldEntity, AtlasVertex oldVertex, Object value) {
        oldEntity.setAttribute(ATLAS_DM_EXPIRED_AT_SYSTEM_DATE, value);
        oldEntity.setAttribute(ATLAS_DM_EXPIRED_AT_BUSINESS_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(oldVertex, ATLAS_DM_EXPIRED_AT_SYSTEM_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(oldVertex, ATLAS_DM_EXPIRED_AT_BUSINESS_DATE, value);
    }

    protected void setQualifiedName(AtlasEntity newEntity, AtlasVertex newVertex, Object value) {
        newEntity.setAttribute(QUALIFIED_NAME, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, QUALIFIED_NAME, value);
    }

    protected void setName(AtlasEntity newEntity, AtlasVertex newVertex, Object value) {
        newEntity.setAttribute(NAME, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, NAME, value);
    }


    protected ModelResponse createModelVersion(String modelQualifiedName, String modelVersion, String namespace, EntityMutationContext context) throws AtlasBaseException {
        String guid = UUID.randomUUID().toString();
        AtlasEntity modelVersionEntity = new AtlasEntity(ATLAS_DM_VERSION_TYPE);
        modelVersionEntity.setAttribute(VERSION_PROPERTY_KEY, 0);
        modelVersionEntity.setAttribute(QUALIFIED_NAME, modelQualifiedName + "/" + modelVersion);
        modelVersionEntity.setAttribute(ATLAS_DM_NAMESPACE, namespace);
        modelVersionEntity.setAttribute(ATLAS_DM_BUSINESS_DATE, RequestContext.get().getRequestTime());
        modelVersionEntity.setAttribute(ATLAS_DM_SYSTEM_DATE, RequestContext.get().getRequestTime());
        AtlasVertex versionVertex = entityGraphMapper.createVertexWithGuid(modelVersionEntity, guid);
        context.getDiscoveryContext().addResolvedGuid(guid, versionVertex);
        modelVersionEntity.setGuid(guid);
        context.addCreated(guid, modelVersionEntity, typeRegistry.getEntityTypeByName(ATLAS_DM_VERSION_TYPE), versionVertex);
        return new ModelResponse(modelVersionEntity, versionVertex);
    }
    protected ModelResponse replicateModelVersion(AtlasRelatedObjectId relatedObjectId, long epoch) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo existingModelVersionEntityWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(relatedObjectId.getGuid());
        AtlasVertex existingModelVersionVertex = entityRetriever.getEntityVertex(relatedObjectId.getGuid());
        AtlasVertex copyModelVertex = entityGraphMapper.createVertex(existingModelVersionEntityWithExtInfo.getEntity());
        AtlasEntity copyModelVersion = entityRetriever.toAtlasEntity(copyModelVertex);
        copyAllAttributes(existingModelVersionEntityWithExtInfo.getEntity(), copyModelVersion, epoch);
        setModelDates(copyModelVersion, copyModelVertex, epoch);
        // ex :  "default/dm/1234/modelName/v1";
        String existingModelVersionQualifiedName = ((String) existingModelVersionEntityWithExtInfo.getEntity().getAttribute(QUALIFIED_NAME));
        int lastIndex = existingModelVersionQualifiedName.lastIndexOf("/");

        // v1
        String existingVersion = existingModelVersionQualifiedName.substring(lastIndex + 1);
        int existingVersionNumber = Integer.parseInt(existingVersion.substring(1));

        // default/dm/1234/modelName
        String modelName = existingModelVersionQualifiedName.substring(0, lastIndex);

        String modelVersion = "v" + (++existingVersionNumber);

        String modelVersionQualifiedName = modelName + "/" + modelVersion;

        setName(copyModelVersion, copyModelVertex, modelVersion);
        setQualifiedName(copyModelVersion, copyModelVertex, modelVersionQualifiedName);
        setModelExpiredAtDates(existingModelVersionEntityWithExtInfo.getEntity(), existingModelVersionVertex, epoch);
        return new ModelResponse(existingModelVersionEntityWithExtInfo.getEntity(), copyModelVersion, existingModelVersionVertex, copyModelVertex);
    }

    protected ModelResponse replicateModelEntity(AtlasEntity existingEntity, AtlasVertex existingEntityVertex, String entityQualifiedNamePrefix, long epoch) throws AtlasBaseException {
        AtlasVertex copyEntityVertex = entityGraphMapper.createVertex(existingEntity);
        AtlasEntity copyEntity = entityRetriever.toAtlasEntity(copyEntityVertex);
        copyAllAttributes(existingEntity, copyEntity, epoch);
        // copyEntity.setRelationshipAttributes(entity.getRelationshipAttributes());
        setModelDates(copyEntity, copyEntityVertex, epoch);
        String entityQualifiedName = entityQualifiedNamePrefix + "_" + epoch;
        setQualifiedName(copyEntity, copyEntityVertex, entityQualifiedName);
        setModelDates(copyEntity, copyEntityVertex, epoch);
        setModelExpiredAtDates(existingEntity, existingEntityVertex, epoch);
        return new ModelResponse(existingEntity, copyEntity, existingEntityVertex, copyEntityVertex);
    }

    protected ModelResponse replicateModelAttribute(AtlasEntity existingAttribute, AtlasVertex existingAttributeVertex, String attributeQualifiedNamePrefix, long epoch) throws AtlasBaseException {
        AtlasVertex copyAttributeVertex = entityGraphMapper.createVertex(existingAttribute);
        AtlasEntity copyAttributeEntity = entityRetriever.toAtlasEntity(copyAttributeVertex);
        copyAllAttributes(existingAttribute, copyAttributeEntity, epoch);
        // copyEntity.setRelationshipAttributes(entity.getRelationshipAttributes());
        setModelDates(copyAttributeEntity, copyAttributeVertex, epoch);
        String attributeQualifiedName = attributeQualifiedNamePrefix + "_"  + epoch;
        setQualifiedName(copyAttributeEntity, copyAttributeVertex, attributeQualifiedName);
        setModelDates(copyAttributeEntity, copyAttributeVertex, epoch);
        setModelExpiredAtDates(existingAttribute, existingAttributeVertex, epoch);
        return new ModelResponse(existingAttribute, copyAttributeEntity, existingAttributeVertex, copyAttributeVertex);
    }

    protected void createModelVersionModelEntityRelationship(AtlasVertex modelVersionVertex,
                                                             AtlasVertex modelEntityVertex) throws AtlasBaseException {
        AtlasRelationship modelVersionEntityRelation = new AtlasRelationship("d_m_version_d_m_entities");
        modelVersionEntityRelation.setStatus(AtlasRelationship.Status.ACTIVE);
        modelVersionEntityRelation.setEnd1(new AtlasObjectId(
                GraphHelper.getGuid(modelVersionVertex),
                GraphHelper.getTypeName(modelVersionVertex)));
        modelVersionEntityRelation.setEnd2(new AtlasObjectId(
                GraphHelper.getGuid(modelEntityVertex),
                GraphHelper.getTypeName(modelEntityVertex)));
        atlasRelationshipStore.create(modelVersionEntityRelation);
    }

    protected void createModelVersionModelEntityRelationship(AtlasVertex modelVersionVertex,
                                                             List<AtlasRelatedObjectId> existingEntities) {
        if (CollectionUtils.isEmpty(existingEntities)){
            return;
        }
        AtlasRelationship modelVersionEntityRelation = new AtlasRelationship("d_m_version_d_m_entities");
        modelVersionEntityRelation.setStatus(AtlasRelationship.Status.ACTIVE);
        modelVersionEntityRelation.setEnd1(new AtlasObjectId(
                GraphHelper.getGuid(modelVersionVertex),
                GraphHelper.getTypeName(modelVersionVertex)));
        for (AtlasRelatedObjectId existingEntity : existingEntities) {
            if (
                    ((int) (existingEntity.getAttributes().get(ATLAS_DM_BUSINESS_DATE)) > 0) ||
                            ((int) (existingEntity.getAttributes().get(ATLAS_DM_SYSTEM_DATE)) > 0)
            ) {
                continue;
            }
            modelVersionEntityRelation.setEnd2(new AtlasObjectId(
                    existingEntity.getGuid(),
                    existingEntity.getTypeName()
            ));
        }
    }

    protected void createModelEntityModelAttributeRelation(AtlasVertex entity, List<AtlasRelatedObjectId> existingEntityAttributes) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(existingEntityAttributes)){
            return;
        }
        AtlasRelationship modelEntityAttributeRelation = new AtlasRelationship("d_m_entity_d_m_attributes");
        modelEntityAttributeRelation.setStatus(AtlasRelationship.Status.ACTIVE);
        modelEntityAttributeRelation.setEnd1(
                new AtlasObjectId(
                        GraphHelper.getGuid(entity),
                        GraphHelper.getTypeName(entity)));
        for (AtlasRelatedObjectId existingEntityAttribute : existingEntityAttributes) {
            if (
                    ((int) (existingEntityAttribute.getAttributes().get(ATLAS_DM_BUSINESS_DATE)) > 0) ||
                            ((int) (existingEntityAttribute.getAttributes().get(ATLAS_DM_SYSTEM_DATE)) > 0)
            ) {
                continue;
            }
            modelEntityAttributeRelation.setEnd2(
                    new AtlasObjectId(
                            existingEntityAttribute.getGuid(),
                            existingEntityAttribute.getTypeName()));
            atlasRelationshipStore.create(modelEntityAttributeRelation);
        }
    }

    protected void createModelEntityModelAttributeRelation(AtlasVertex entity, AtlasVertex attribute) throws AtlasBaseException {
        AtlasRelationship modelEntityAttributeRelation = new AtlasRelationship("d_m_entity_d_m_attributes");
        modelEntityAttributeRelation.setStatus(AtlasRelationship.Status.ACTIVE);
        modelEntityAttributeRelation.setEnd1(
                new AtlasObjectId(
                        GraphHelper.getGuid(entity),
                        GraphHelper.getTypeName(entity)));
        modelEntityAttributeRelation.setEnd2(
                new AtlasObjectId(
                        GraphHelper.getGuid(attribute),
                        GraphHelper.getTypeName(attribute)));
        atlasRelationshipStore.create(modelEntityAttributeRelation);
    }


    protected void createModelModelVersionRelation(String modelGuid, String latestModelVersionGuid) throws AtlasBaseException {
        AtlasRelationship modelVersionModelRelation = new AtlasRelationship("d_m_data_model_d_m_versions");
        modelVersionModelRelation.setStatus(AtlasRelationship.Status.ACTIVE);
        modelVersionModelRelation.setEnd1(
                new AtlasObjectId(
                        modelGuid, ATLAS_DM_DATA_MODEL));
        modelVersionModelRelation.setEnd2(
                new AtlasObjectId( latestModelVersionGuid, ATLAS_DM_VERSION_TYPE));
        atlasRelationshipStore.create(modelVersionModelRelation);
    }
    protected AtlasRelatedObjectId createModelModelVersionRelation(AtlasVertex existingModelVersionVertex, AtlasVertex latestModelVersionVertex) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo existingModelVersionExtInfo = entityRetriever.toAtlasEntityWithExtInfo(existingModelVersionVertex, false);
        AtlasRelatedObjectId existingModel = (AtlasRelatedObjectId) existingModelVersionExtInfo.getEntity().getRelationshipAttributes().get("dMDataModel");
        if (existingModel == null) {
            throw new AtlasBaseException(AtlasErrorCode.DATA_MODEL_NOT_EXIST);
        }
        AtlasRelationship modelVersionModelRelation = new AtlasRelationship("d_m_data_model_d_m_versions");
        modelVersionModelRelation.setStatus(AtlasRelationship.Status.ACTIVE);
        modelVersionModelRelation.setEnd1(
                new AtlasObjectId(
                        existingModel.getGuid(),
                        existingModel.getTypeName()));
        modelVersionModelRelation.setEnd2(
                new AtlasObjectId(
                        GraphHelper.getGuid(latestModelVersionVertex),
                        GraphHelper.getTypeName(latestModelVersionVertex)));
        atlasRelationshipStore.create(modelVersionModelRelation);
        return existingModel;
    }

    protected void copyAllAttributes(AtlasEntity source, AtlasEntity destination, long epochNow) {
        destination.setAttributes(source.getAttributes());
        destination.setMeanings(source.getMeanings());
        destination.setCreateTime(new Date(epochNow));
        destination.setUpdateTime(new Date(epochNow));
        //    destination.setRelationshipAttributes(source.getRelationshipAttributes());
        destination.setCustomAttributes(source.getCustomAttributes());
        destination.setClassifications(source.getClassifications());
        destination.setAppendRelationshipAttributes(source.getAppendRelationshipAttributes());
        destination.setRemoveRelationshipAttributes(source.getRemoveRelationshipAttributes());
    }

    public static void replaceAttributes(Map<String, Object> existingAttributes, Map<String, Object> diffAttributes) {
        if (MapUtils.isEmpty(diffAttributes)) {
            return;
        }
        // Temporary map to hold new key-value pairs during replacement
        Map<String, Object> tempMap = new HashMap<>();

        // Iterate over the original map
        for (Map.Entry<String, Object> entry : existingAttributes.entrySet()) {
            String originalKey = entry.getKey();
            Object value = entry.getValue();

            // Check if the second map contains a key for replacement
            if (diffAttributes.containsKey(originalKey)) {
                Object newValue = diffAttributes.get(originalKey);  // Get the new key from second map
                tempMap.put(originalKey, newValue);  // Put the new key in the temp map
            } else {
                tempMap.put(originalKey, value);  // No replacement, keep the original key
            }
        }

        // Clear the original map and put all the updated entries
        existingAttributes.clear();
        existingAttributes.putAll(tempMap);
    }

    protected void applyDiffs(AtlasEntity sourceEntity, AtlasEntity destinationEntity, String typeName) {
        RequestContext reqContext = RequestContext.get();
        AtlasEntity diffEntity = reqContext.getDifferentialEntity(sourceEntity.getGuid());
        boolean diffExistsForSameType = diffEntity.getTypeName().equals(typeName);
        if (!diffExistsForSameType) {
            return;
        }
        replaceAttributes(destinationEntity.getAttributes(), diffEntity.getAttributes());
        replaceAttributes(destinationEntity.getRelationshipAttributes(), diffEntity.getRelationshipAttributes());
        replaceAttributes(destinationEntity.getAppendRelationshipAttributes(), diffEntity.getAppendRelationshipAttributes());
        replaceAttributes(destinationEntity.getRemoveRelationshipAttributes(), diffEntity.getRemoveRelationshipAttributes());
    }

    protected void unsetExpiredDates(AtlasEntity latestEntity, AtlasVertex latestVertex) {
        latestEntity.setAttribute(ATLAS_DM_EXPIRED_AT_SYSTEM_DATE, 0);
        latestEntity.setAttribute(ATLAS_DM_EXPIRED_AT_BUSINESS_DATE, 0);
        AtlasGraphUtilsV2.setEncodedProperty(latestVertex, ATLAS_DM_EXPIRED_AT_SYSTEM_DATE, 0);
        AtlasGraphUtilsV2.setEncodedProperty(latestVertex, ATLAS_DM_SYSTEM_DATE, 0);
    }
}
