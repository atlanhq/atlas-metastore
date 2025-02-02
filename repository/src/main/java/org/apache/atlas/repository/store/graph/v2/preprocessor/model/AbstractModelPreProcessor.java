package org.apache.atlas.repository.store.graph.v2.preprocessor.model;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.isNameInvalid;

public abstract class AbstractModelPreProcessor implements PreProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractModelPreProcessor.class);

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

    protected void setModelDates(AtlasEntity newEntity, AtlasVertex newVertex, Object value) {
        newEntity.setAttribute(MODEL_SYSTEM_DATE, value);
        newEntity.setAttribute(MODEL_BUSINESS_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, MODEL_SYSTEM_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, MODEL_BUSINESS_DATE, value);
    }

    protected void setModelExpiredAtDates(AtlasEntity oldEntity, AtlasVertex oldVertex, Object value) {
        oldEntity.setAttribute(MODEL_EXPIRED_AT_SYSTEM_DATE, value);
        oldEntity.setAttribute(MODEL_EXPIRED_AT_BUSINESS_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(oldVertex, MODEL_EXPIRED_AT_SYSTEM_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(oldVertex, MODEL_EXPIRED_AT_BUSINESS_DATE, value);
    }

    protected void setQualifiedName(AtlasEntity newEntity, AtlasVertex newVertex, Object value) {
        newEntity.setAttribute(QUALIFIED_NAME, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, QUALIFIED_NAME, value);
    }

    protected void setName(AtlasEntity newEntity, AtlasVertex newVertex, Object value) {
        newEntity.setAttribute(NAME, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, NAME, value);
    }

    protected void setQualifiedNamePrefix(AtlasEntity newEntity, AtlasVertex newVertex, Object value) {
        newEntity.setAttribute(MODEL_QUALIFIED_NAME_PATTERN, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, MODEL_QUALIFIED_NAME_PATTERN, value);
    }

    protected void setNamespace(AtlasEntity newEntity, AtlasVertex newVertex, Object value) {
        newEntity.setAttribute(MODEL_NAMESPACE, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, MODEL_NAMESPACE, value);
    }

    protected ModelResponse createEntity(String qualifiedName, String name, String entityType, String namespace, EntityMutationContext context) throws AtlasBaseException {
        String guid = UUID.randomUUID().toString();
        AtlasEntity entity = new AtlasEntity(entityType);
        entity.setAttribute(NAME, name);
        entity.setAttribute(VERSION_PROPERTY_KEY, 0);
        entity.setAttribute(QUALIFIED_NAME, qualifiedName);
        entity.setAttribute(MODEL_NAMESPACE, namespace);
        entity.setAttribute(MODEL_BUSINESS_DATE, RequestContext.get().getRequestTime());
        entity.setAttribute(MODEL_SYSTEM_DATE, RequestContext.get().getRequestTime());
        if (entityType.equals(MODEL_ENTITY) || entityType.equals(MODEL_ATTRIBUTE)) {
            String prefix = qualifiedName.substring(0, qualifiedName.indexOf("_"));
            entity.setAttribute(MODEL_QUALIFIED_NAME_PATTERN, prefix);
        }
        AtlasVertex versionVertex = entityGraphMapper.createVertexWithGuid(entity, guid);
        context.getDiscoveryContext().addResolvedGuid(guid, versionVertex);
        entity.setGuid(guid);
        return new ModelResponse(entity, versionVertex);
    }

    public ModelResponse replicateModelVersion(String modelGuid, String modelQualifiedName, long now) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo dataModel = entityRetriever.toAtlasEntityWithExtInfo(modelGuid, false);
        List<AtlasRelatedObjectId> existingModelVersions = (List<AtlasRelatedObjectId>) dataModel.getEntity().getRelationshipAttribute(RELATED_MODEL_VERSIONS);

        String modelVersion = "v1";
        AtlasRelatedObjectId existingModelVersionObj = null;

        if (CollectionUtils.isEmpty(existingModelVersions)) {
            return new ModelResponse(null, null);
        }

        int existingVersionNumber = existingModelVersions.size();
        modelVersion = "v" + (++existingVersionNumber);

        // get active model version
        for (AtlasRelatedObjectId modelVersionObj : existingModelVersions) {
            AtlasEntity modelVersionEntity = entityRetriever.toAtlasEntity(modelVersionObj.getGuid());
            Date expiredAtBusinessDate = (Date) modelVersionEntity.getAttributes().get(MODEL_EXPIRED_AT_BUSINESS_DATE);
            Date expiredAtSystemDate = (Date) modelVersionEntity.getAttributes().get(MODEL_EXPIRED_AT_SYSTEM_DATE);

            if (expiredAtBusinessDate != null && expiredAtBusinessDate.getTime() > 0 || expiredAtSystemDate != null && expiredAtSystemDate.getTime() > 0) {
                continue;
            }
            existingModelVersionObj = modelVersionObj;
        }

        if (existingModelVersionObj == null) {
            throw new AtlasBaseException(AtlasErrorCode.DATA_MODEL_VERSION_NOT_EXIST);
        }

        AtlasEntity existingModelVersionEntity = entityRetriever.toAtlasEntityWithExtInfo(existingModelVersionObj.getGuid()).getEntity();
        AtlasVertex existingModelVersionVertex = entityRetriever.getEntityVertex(existingModelVersionObj.getGuid());


        AtlasVertex copyModelVertex = entityGraphMapper.createVertex(existingModelVersionEntity);
        AtlasEntity copyModelVersion = entityRetriever.toAtlasEntity(copyModelVertex);
        copyAllAttributes(existingModelVersionEntity, copyModelVersion, now);
        setModelDates(copyModelVersion, copyModelVertex, now);
        setQualifiedName(copyModelVersion, copyModelVertex, modelQualifiedName + "/" + modelVersion);
        setName(copyModelVersion, copyModelVertex, modelVersion);
        setNamespace(copyModelVersion, copyModelVertex, dataModel.getEntity().getAttribute(MODEL_NAMESPACE));
        setModelExpiredAtDates(existingModelVersionEntity, existingModelVersionVertex, now);
        return new ModelResponse(existingModelVersionEntity, copyModelVersion, existingModelVersionVertex, copyModelVertex);
    }

    public ModelResponse replicateModelEntity(AtlasEntity existingEntity, AtlasVertex existingEntityVertex, String entityQualifiedNamePrefix, long epoch) throws AtlasBaseException {
        AtlasVertex copyEntityVertex = entityGraphMapper.createVertex(existingEntity);
        AtlasEntity copyEntity = entityRetriever.toAtlasEntity(copyEntityVertex);
        copyAllAttributes(existingEntity, copyEntity, epoch);
        String entityQualifiedName = entityQualifiedNamePrefix + "_" + epoch;
        setQualifiedName(copyEntity, copyEntityVertex, entityQualifiedName);
        setModelDates(copyEntity, copyEntityVertex, epoch);
        setName(copyEntity, copyEntityVertex, existingEntity.getAttribute(NAME));
        setNamespace(copyEntity, copyEntityVertex, existingEntity.getAttribute(MODEL_NAMESPACE));
        setQualifiedNamePrefix(copyEntity, copyEntityVertex, existingEntity.getAttribute(MODEL_QUALIFIED_NAME_PATTERN));
        setModelExpiredAtDates(existingEntity, existingEntityVertex, epoch);
        return new ModelResponse(existingEntity, copyEntity, existingEntityVertex, copyEntityVertex);
    }

    protected ModelResponse replicateModelAttribute(AtlasEntity existingAttribute, AtlasVertex existingAttributeVertex, String attributeQualifiedNamePrefix, long epoch) throws AtlasBaseException {
        AtlasVertex copyAttributeVertex = entityGraphMapper.createVertex(existingAttribute);
        AtlasEntity copyAttributeEntity = entityRetriever.toAtlasEntity(copyAttributeVertex);
        copyAllAttributes(existingAttribute, copyAttributeEntity, epoch);
        String attributeQualifiedName = attributeQualifiedNamePrefix + "_" + epoch;
        setQualifiedName(copyAttributeEntity, copyAttributeVertex, attributeQualifiedName);
        setModelDates(copyAttributeEntity, copyAttributeVertex, epoch);
        setName(copyAttributeEntity, copyAttributeVertex, existingAttribute.getAttribute(NAME));
        setNamespace(copyAttributeEntity, copyAttributeVertex, existingAttribute.getAttribute(MODEL_NAMESPACE));
        setQualifiedNamePrefix(copyAttributeEntity, copyAttributeVertex, existingAttribute.getAttribute(MODEL_QUALIFIED_NAME_PATTERN));
        setModelExpiredAtDates(existingAttribute, existingAttributeVertex, epoch);
        return new ModelResponse(existingAttribute, copyAttributeEntity, existingAttributeVertex, copyAttributeVertex);
    }

    public void createModelVersionModelEntityRelationship(AtlasVertex modelVersionVertex,
                                                          AtlasVertex modelEntityVertex) throws AtlasBaseException {
        AtlasRelationship modelVersionEntityRelation = new AtlasRelationship(MODEL_VERSIONS_MODEL_VERSION_ENTITY_RELATION);
        modelVersionEntityRelation.setStatus(AtlasRelationship.Status.ACTIVE);
        modelVersionEntityRelation.setEnd1(new AtlasObjectId(
                GraphHelper.getGuid(modelVersionVertex),
                GraphHelper.getTypeName(modelVersionVertex)));
        modelVersionEntityRelation.setEnd2(new AtlasObjectId(
                GraphHelper.getGuid(modelEntityVertex),
                GraphHelper.getTypeName(modelEntityVertex)));
        atlasRelationshipStore.getOrCreate(modelVersionEntityRelation);
    }

    protected void createModelVersionModelEntityRelationship(AtlasVertex modelVersionVertex, AtlasEntity currentEntity,
                                                             List<AtlasRelatedObjectId> existingEntities, EntityMutationContext context) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(existingEntities)) {
            return;
        }
        AtlasRelationship modelVersionEntityRelation = new AtlasRelationship(MODEL_VERSIONS_MODEL_VERSION_ENTITY_RELATION);
        modelVersionEntityRelation.setStatus(AtlasRelationship.Status.ACTIVE);
        modelVersionEntityRelation.setEnd1(new AtlasObjectId(
                GraphHelper.getGuid(modelVersionVertex),
                GraphHelper.getTypeName(modelVersionVertex)));
        for (AtlasRelatedObjectId existingEntity : existingEntities) {
            AtlasEntity entity = entityRetriever.toAtlasEntity(existingEntity.getGuid());

            if (currentEntity != null && entity.getGuid().equals(currentEntity.getGuid())) {
                continue;
            }
            if (context.getModelEntitiesToBeUpdated().contains((String) entity.getAttribute(MODEL_QUALIFIED_NAME_PATTERN))){
                continue;
            }
            Date expiredAtBusinessDate = (Date) entity.getAttributes().get(MODEL_EXPIRED_AT_SYSTEM_DATE);
            Date expiredAtSystemDate = (Date) entity.getAttributes().get(MODEL_EXPIRED_AT_BUSINESS_DATE);
            if (expiredAtBusinessDate != null && expiredAtBusinessDate.getTime() > 0 || expiredAtSystemDate != null && expiredAtSystemDate.getTime() > 0) {
                continue;
            }
            modelVersionEntityRelation.setEnd2(new AtlasObjectId(
                    existingEntity.getGuid(),
                    existingEntity.getTypeName()
            ));
            atlasRelationshipStore.getOrCreate(modelVersionEntityRelation);
        }
    }

    protected void createModelEntityModelAttributeRelation(AtlasVertex entity, AtlasEntity currentAttribute, List<AtlasRelatedObjectId> existingEntityAttributes, EntityMutationContext context) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(existingEntityAttributes)) {
            return;
        }
        AtlasRelationship modelEntityAttributeRelation = new AtlasRelationship(MODEL_ENTITY_MODEL_ATTRIBUTES_RELATION );
        modelEntityAttributeRelation.setStatus(AtlasRelationship.Status.ACTIVE);
        modelEntityAttributeRelation.setEnd1(
                new AtlasObjectId(
                        GraphHelper.getGuid(entity),
                        GraphHelper.getTypeName(entity)));
        for (AtlasRelatedObjectId existingEntityAttribute : existingEntityAttributes) {
            AtlasEntity entityAttribute = entityRetriever.toAtlasEntity(existingEntityAttribute.getGuid());

            if (currentAttribute != null && entityAttribute.getGuid().equals(currentAttribute.getGuid())) {
                continue;
            }

            if (context.getModelAttributesToBeUpdated().contains((String) entityAttribute.getAttribute(MODEL_QUALIFIED_NAME_PATTERN))){
                continue;
            }
            Date expiredAtBusinessDate = (Date) entityAttribute.getAttributes().get(MODEL_EXPIRED_AT_SYSTEM_DATE);
            Date expiredAtSystemDate = (Date) entityAttribute.getAttributes().get(MODEL_EXPIRED_AT_BUSINESS_DATE);
            if (expiredAtBusinessDate != null && expiredAtBusinessDate.getTime() > 0 || expiredAtSystemDate != null && expiredAtSystemDate.getTime() > 0) {
                continue;
            }
            modelEntityAttributeRelation.setEnd2(
                    new AtlasObjectId(
                            existingEntityAttribute.getGuid(),
                            existingEntityAttribute.getTypeName()));
            atlasRelationshipStore.getOrCreate(modelEntityAttributeRelation);
        }
    }

    protected void createModelEntityModelAttributeRelation(AtlasVertex entity, AtlasVertex attribute) throws AtlasBaseException {
        AtlasRelationship modelEntityAttributeRelation = new AtlasRelationship(MODEL_ENTITY_MODEL_ATTRIBUTES_RELATION );
        modelEntityAttributeRelation.setStatus(AtlasRelationship.Status.ACTIVE);
        modelEntityAttributeRelation.setEnd1(
                new AtlasObjectId(
                        GraphHelper.getGuid(entity),
                        GraphHelper.getTypeName(entity)));
        modelEntityAttributeRelation.setEnd2(
                new AtlasObjectId(
                        GraphHelper.getGuid(attribute),
                        GraphHelper.getTypeName(attribute)));
        atlasRelationshipStore.getOrCreate(modelEntityAttributeRelation);
    }


    public void createModelModelVersionRelation(String modelGuid, String latestModelVersionGuid) throws AtlasBaseException {
        AtlasRelationship modelVersionModelRelation = new AtlasRelationship(MODEL_MODEL_VERSION_RELATION);
        modelVersionModelRelation.setStatus(AtlasRelationship.Status.ACTIVE);
        modelVersionModelRelation.setEnd1(
                new AtlasObjectId(
                        modelGuid, MODEL_DATA_MODEL));
        modelVersionModelRelation.setEnd2(
                new AtlasObjectId(latestModelVersionGuid, MODEL_VERSION));
        atlasRelationshipStore.getOrCreate(modelVersionModelRelation);
    }

    protected void copyAllAttributes(AtlasEntity source, AtlasEntity destination, long epochNow) {
        if (source == null || destination == null) {
            throw new IllegalArgumentException("Source and destination entities must not be null.");
        }

        if (source.getAttributes() != null) {
            destination.setAttributes(new HashMap<>(source.getAttributes()));
        } else {
            destination.setAttributes(new HashMap<>());
        }


        if (CollectionUtils.isNotEmpty(source.getMeanings())) {
            destination.setMeanings(new ArrayList<>(source.getMeanings()));
        } else {
            destination.setMeanings(new ArrayList<>());
        }

        long requestTime = RequestContext.get().getRequestTime();
        destination.setCreateTime(new Date(requestTime));
        destination.setUpdateTime(new Date(requestTime));


        if (source.getCustomAttributes() != null) {
            destination.setCustomAttributes(new HashMap<>(source.getCustomAttributes()));
        } else {
            destination.setCustomAttributes(new HashMap<>()); // Set empty map if source custom attributes are null
        }

        if (CollectionUtils.isNotEmpty(source.getClassifications())) {
            destination.setClassifications(new ArrayList<>(source.getClassifications()));
        } else {
            destination.setClassifications(new ArrayList<>()); // Set empty list if source classifications are null or empty
        }

        String entityType = source.getTypeName();

        if (MapUtils.isNotEmpty(source.getRelationshipAttributes())) {
            Map<String, Object> relationAttributes = copyRelationshipAttributes(source.getRelationshipAttributes(), destination, entityType);
            destination.setRelationshipAttributes(relationAttributes);
        }
        if (MapUtils.isNotEmpty(source.getAppendRelationshipAttributes())) {
            Map<String, Object> relationAttributes = copyRelationshipAttributes(source.getAppendRelationshipAttributes(), destination, entityType);
            destination.setAppendRelationshipAttributes(relationAttributes);
        }
        if (MapUtils.isNotEmpty(source.getRemoveRelationshipAttributes())) {
            Map<String, Object> relationAttributes = copyRelationshipAttributes(source.getRemoveRelationshipAttributes(), destination, entityType);
            destination.setRemoveRelationshipAttributes(relationAttributes);
        }

    }

    private Map<String, Object> copyRelationshipAttributes(Map<String, Object> sourceAttributes, AtlasEntity destination, String entityType) {
        Map<String, Object> destinationAttributes = new HashMap<>();

        if (MapUtils.isEmpty(sourceAttributes)) {
            return destinationAttributes;
        }

        Set<String> allowedRelations = allowedRelationshipsForEntityType(entityType);

        for (String attribute : sourceAttributes.keySet()) {
            if (allowedRelations.contains(attribute)) {
                destinationAttributes.put(attribute, sourceAttributes.get(attribute));
            }
        }

        return destinationAttributes;
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
        if (diffEntity == null) {
            return;
        }
        boolean diffExistsForSameType = diffEntity.getTypeName().equals(typeName);
        if (!diffExistsForSameType) {
            return;
        }
        replaceAttributes(destinationEntity.getAttributes(), diffEntity.getAttributes());
    }

    protected void unsetExpiredDates(AtlasEntity latestEntity, AtlasVertex latestVertex) {
        latestEntity.setAttribute(MODEL_EXPIRED_AT_SYSTEM_DATE, 0);
        latestEntity.setAttribute(MODEL_EXPIRED_AT_BUSINESS_DATE, 0);
        AtlasGraphUtilsV2.setEncodedProperty(latestVertex, MODEL_EXPIRED_AT_SYSTEM_DATE, 0);
        AtlasGraphUtilsV2.setEncodedProperty(latestVertex, MODEL_EXPIRED_AT_BUSINESS_DATE, 0);
    }

    protected Set<String> allowedRelationshipsForEntityType(String entityType) {
        Set<String> allowedRelationships = new HashSet<>();
        switch (entityType) {
            case MODEL_ENTITY:
                allowedRelationships.add(MODEL_ENTITY_MAPPED_TO_ENTITIES);
                allowedRelationships.add(MODEL_ENTITY_MAPPED_FROM_ENTITIES);
                allowedRelationships.add(MODEL_ENTITY_RELATED_FROM_ENTITIES);
                allowedRelationships.add(MODEL_ENTITY_RELATED_TO_ENTITIES);
                break;
            case MODEL_ATTRIBUTE:
                allowedRelationships.add(MODEL_ATTRIBUTE_MAPPED_FROM_ATTRIBUTES );
                allowedRelationships.add(MODEL_ATTRIBUTE_MAPPED_TO_ATTRIBUTES);
                allowedRelationships.add(MODEL_ATTRIBUTE_RELATED_FROM_ATTRIBUTES);
                allowedRelationships.add(MODEL_ATTRIBUTE_RELATED_TO_ATTRIBUTES);
                break;
            case MODEL_ENTITY_ASSOCIATION:
                allowedRelationships.add(MODEL_ENTITY_ASSOCIATION_TO);
                allowedRelationships.add(MODEL_ENTITY_ASSOCIATION_FROM);
                break;
            case MODEL_ATTRIBUTE_ASSOCIATION:
                allowedRelationships.add(MODEL_ATTRIBUTE_ASSOCIATION_FROM );
                allowedRelationships.add(MODEL_ATTRIBUTE_ASSOCIATION_TO);
                break;
        }
        return allowedRelationships;
    }

    protected ModelResponse updateDMEntity(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        if (!entity.getTypeName().equals(MODEL_ENTITY)) {
            return new ModelResponse(entity, vertex);
        }

        String entityName = (String) entity.getAttribute(NAME);

        if (StringUtils.isEmpty(entityName) || isNameInvalid(entityName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }


        long now = RequestContext.get().getRequestTime();
        AtlasEntity.AtlasEntityWithExtInfo existingEntity = entityRetriever.toAtlasEntityWithExtInfo(vertex, false);


        // get model qualifiedName with qualifiedNamePrefix
        String qualifiedNamePrefix = (String) entity.getAttributes().get(MODEL_QUALIFIED_NAME_PATTERN);
        int lastIndex = qualifiedNamePrefix.lastIndexOf("/");
        String modelQualifiedName = qualifiedNamePrefix.substring(0, lastIndex);
        String modelVersion = "v1";

        String modelGuid = context.getModel(modelQualifiedName);

        if (StringUtils.isEmpty(modelGuid)) {
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


        ModelResponse modelVersionResponse = context.getModelVersion(modelQualifiedName);

        if (modelVersionResponse == null) {

            modelVersionResponse = replicateModelVersion(modelGuid, modelQualifiedName, now);

            // model is not replicated successfully
            if (modelVersionResponse.getReplicaEntity() == null) {
                String namespace = (String) entity.getAttributes().get(MODEL_NAMESPACE);
                modelVersionResponse = createEntity(
                        (modelQualifiedName + "/" + modelVersion),
                        modelVersion,
                        MODEL_VERSION,
                        namespace,
                        context);
            }
            context.cacheModelVersion(modelQualifiedName, modelVersionResponse);
        }

        AtlasEntity latestModelVersionEntity = modelVersionResponse.getReplicaEntity();
        AtlasVertex latestModelVersionVertex = modelVersionResponse.getReplicaVertex();
        AtlasEntity existingVersion = modelVersionResponse.getExistingEntity();

        // create entity e1 ---> e1'
        ModelResponse modelEntityResponse = replicateModelEntity(existingEntity.getEntity(), vertex, qualifiedNamePrefix, now);
        AtlasVertex copyEntityVertex = modelEntityResponse.getReplicaVertex();
        AtlasEntity copyEntity = modelEntityResponse.getReplicaEntity();
        applyDiffs(entity, copyEntity, MODEL_ENTITY);
        unsetExpiredDates(copyEntity, copyEntityVertex);

        // create model-modelVersion relation
        createModelModelVersionRelation(modelGuid, latestModelVersionEntity.getGuid());

        // create modelVersion-modelEntity relationship with new entity
        createModelVersionModelEntityRelationship(latestModelVersionVertex, copyEntityVertex);

        // create modelVersion-modelEntity relation with old entities which are not expired
        if (existingVersion != null && MapUtils.isNotEmpty(existingVersion.getRelationshipAttributes())) {
            List<AtlasRelatedObjectId> existingEntities = (List<AtlasRelatedObjectId>) existingVersion.getRelationshipAttributes().get(MODEL_VERSION_ENTITIES);
            createModelVersionModelEntityRelationship(latestModelVersionVertex, entity, existingEntities, context);
        }

        //  create modelEntity - existing modelAttributeRelationship
        if (MapUtils.isNotEmpty(existingEntity.getEntity().getRelationshipAttributes())) {
            List<AtlasRelatedObjectId> existingEntityAttributes = (List<AtlasRelatedObjectId>) existingEntity.getEntity().getRelationshipAttributes().get(MODEL_ENTITY_ATTRIBUTES);
            createModelEntityModelAttributeRelation(copyEntityVertex, null, existingEntityAttributes, context);
        }


        /**
         * update context
         */
        // previousEntity and previousModelVersion
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());
        AtlasEntityType modelVersionType = typeRegistry.getEntityTypeByName(modelVersionResponse.getReplicaEntity().getTypeName());

        context.addCreated(copyEntity.getGuid(), copyEntity, entityType, copyEntityVertex);
        context.addCreated(latestModelVersionEntity.getGuid(), latestModelVersionEntity, modelVersionType, latestModelVersionVertex);


        context.removeUpdated(entity.getGuid(), entity, entityType, vertex);
        context.removeUpdated(existingVersion.getGuid(), existingVersion, modelVersionType, modelVersionResponse.getExistingVertex());

        context.addUpdated(entity.getGuid(), existingEntity.getEntity(), entityType, vertex);
        context.addUpdated(existingVersion.getGuid(), existingVersion, modelVersionType, modelVersionResponse.getExistingVertex());

        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        modelGuid,
                        entityRetriever.getEntityVertex(modelGuid));

        return new ModelResponse(copyEntity, copyEntityVertex);
    }

    protected Map<String, Object> processRelationshipAttributesForEntity(AtlasEntity entity, Map<String, Object> relationshipAttributes, EntityMutationContext context) throws AtlasBaseException {
        Map<String, Object> appendAttributesDestination = new HashMap<>();
        if (relationshipAttributes != null) {
            Map<String, Object> appendAttributesSource = (Map<String, Object>) relationshipAttributes;
            ;
            ModelResponse modelResponseRelatedEntity = null;
            String guid = "";
            Set<String> allowedRelations = allowedRelationshipsForEntityType(entity.getTypeName());

            for (String attribute : appendAttributesSource.keySet()) {

                if (appendAttributesSource.get(attribute) instanceof List) {

                    if (!allowedRelations.contains(attribute)) {
                        continue;
                    }
                    List<Map<String, Object>> destList = new ArrayList<>();
                    Map<String, Object> destMap = null;

                    List<Map<String, Object>> attributeList = (List<Map<String, Object>>) appendAttributesSource.get(attribute);

                    for (Map<String, Object> relationAttribute : attributeList) {
                        guid = (String) relationAttribute.get("guid");

                        // update end2
                        modelResponseRelatedEntity = updateDMEntity(
                                entityRetriever.toAtlasEntity(guid),
                                entityRetriever.getEntityVertex(guid),
                                context);
                        //relationAttribute.put("guid", modelResponseRelatedEntity.getCopyEntity());
                        destMap = new HashMap<>(relationAttribute);
                        guid = modelResponseRelatedEntity.getReplicaEntity().getGuid();
                        destMap.put("guid", guid);
                        //destMap.put(QUALIFIED_NAME, )
                        context.getDiscoveryContext().addResolvedGuid(guid, modelResponseRelatedEntity.getReplicaVertex());
                        destList.add(destMap);
                    }
                    appendAttributesDestination.put(attribute, destList);
                } else {
                    if (appendAttributesSource.get(attribute) instanceof Map) {
                        LinkedHashMap<String, Object> attributeList = (LinkedHashMap<String, Object>) appendAttributesSource.get(attribute);
                        guid = (String) attributeList.get("guid");

                        // update end2
                        modelResponseRelatedEntity = updateDMEntity(
                                entityRetriever.toAtlasEntity(guid),
                                entityRetriever.getEntityVertex(guid),
                                context);

                        Map<String, Object> destMap = new HashMap<>(attributeList);
                        destMap.put("guid", guid);
                        guid = modelResponseRelatedEntity.getReplicaEntity().getGuid();
                        context.getDiscoveryContext().addResolvedGuid(guid, modelResponseRelatedEntity.getReplicaVertex());
                        appendAttributesDestination.put(attribute, destMap);
                    }
                }
            }
        }
        return appendAttributesDestination;
    }

    protected ModelResponse replicateDMAssociation(AtlasEntity existingEntity, AtlasVertex existingEntityVertex, long epoch) throws AtlasBaseException {
        AtlasVertex copyEntityVertex = entityGraphMapper.createVertex(existingEntity);
        AtlasEntity copyEntity = entityRetriever.toAtlasEntity(copyEntityVertex);
        copyAllAttributes(existingEntity, copyEntity, epoch);
        setModelDates(copyEntity, copyEntityVertex, epoch);
        setModelDates(copyEntity, copyEntityVertex, epoch);
        setModelExpiredAtDates(existingEntity, existingEntityVertex, epoch);
        return new ModelResponse(existingEntity, copyEntity, existingEntityVertex, copyEntityVertex);
    }

    protected Map<String, Object> processRelationshipAttributesForAttribute(AtlasEntity entity, Map<String, Object> relationshipAttributes, EntityMutationContext context) throws AtlasBaseException {
        Map<String, Object> appendAttributesDestination = new HashMap<>();
        if (relationshipAttributes != null) {
            Map<String, Object> appendAttributesSource = (Map<String, Object>) relationshipAttributes;
            ;
            ModelResponse modelResponseRelatedEntity = null;
            String guid = "";
            Set<String> allowedRelations = allowedRelationshipsForEntityType(entity.getTypeName());

            for (String attribute : appendAttributesSource.keySet()) {

                if (appendAttributesSource.get(attribute) instanceof List) {

                    if (!allowedRelations.contains(attribute)) {
                        continue;
                    }
                    List<Map<String, Object>> destList = new ArrayList<>();
                    Map<String, Object> destMap = null;

                    List<Map<String, Object>> attributeList = (List<Map<String, Object>>) appendAttributesSource.get(attribute);

                    for (Map<String, Object> relationAttribute : attributeList) {
                        guid = (String) relationAttribute.get("guid");

                        // update end2
                        modelResponseRelatedEntity = updateDMAttribute(
                                entityRetriever.toAtlasEntity(guid),
                                entityRetriever.getEntityVertex(guid),
                                context);
                        //relationAttribute.put("guid", modelResponseRelatedEntity.getCopyEntity());
                        destMap = new HashMap<>(relationAttribute);
                        guid = modelResponseRelatedEntity.getReplicaEntity().getGuid();
                        destMap.put("guid", guid);
                        //destMap.put(QUALIFIED_NAME, )
                        context.getDiscoveryContext().addResolvedGuid(guid, modelResponseRelatedEntity.getReplicaVertex());
                        destList.add(destMap);
                    }
                    appendAttributesDestination.put(attribute, destList);
                } else {
                    if (appendAttributesSource.get(attribute) instanceof Map) {
                        LinkedHashMap<String, Object> attributeList = (LinkedHashMap<String, Object>) appendAttributesSource.get(attribute);
                        guid = (String) attributeList.get("guid");

                        // update end2
                        modelResponseRelatedEntity = updateDMAttribute(
                                entityRetriever.toAtlasEntity(guid),
                                entityRetriever.getEntityVertex(guid),
                                context);

                        Map<String, Object> destMap = new HashMap<>(attributeList);
                        destMap.put("guid", guid);
                        guid = modelResponseRelatedEntity.getReplicaEntity().getGuid();
                        context.getDiscoveryContext().addResolvedGuid(guid, modelResponseRelatedEntity.getReplicaVertex());
                        appendAttributesDestination.put(attribute, destMap);
                    }
                }
            }
        }
        return appendAttributesDestination;
    }

    protected ModelResponse updateDMAttribute(AtlasEntity entityAttribute, AtlasVertex vertexAttribute, EntityMutationContext context) throws AtlasBaseException {
        if (!entityAttribute.getTypeName().equals(MODEL_ATTRIBUTE)) {
            return new ModelResponse(entityAttribute, vertexAttribute);
        }

        String attributeName = (String) entityAttribute.getAttribute(NAME);

        if (StringUtils.isEmpty(attributeName) || isNameInvalid(attributeName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME, attributeName);
        }

        long now = RequestContext.get().getRequestTime();


        // get entity qualifiedName with qualifiedNamePrefix
        String attributeQualifiedNamePrefix = (String) entityAttribute.getAttributes().get(MODEL_QUALIFIED_NAME_PATTERN);
        int lastIndex = attributeQualifiedNamePrefix.lastIndexOf("/");
        String entityQualifiedNamePrefix = attributeQualifiedNamePrefix.substring(0, lastIndex);
        String namespace = (String) entityAttribute.getAttributes().get(MODEL_NAMESPACE);
        String modelVersion = "v2";
        // get model qualifiedName with qualifiedNamePrefix
        lastIndex = entityQualifiedNamePrefix.lastIndexOf("/");
        String modelQualifiedName = entityQualifiedNamePrefix.substring(0, lastIndex);

        String modelGuid = context.getModel(modelQualifiedName);

        if (StringUtils.isEmpty(modelGuid)) {
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
            AtlasVertex latestEntityVertex  = AtlasGraphUtilsV2.findLatestEntityAttributeVerticesByType(MODEL_ENTITY, entityQualifiedNamePrefix);

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

            // create model-modelVersion relationship
            createModelModelVersionRelation(modelGuid, modelVersionResponse.getReplicaEntity().getGuid());
            context.cacheModelVersion(modelQualifiedName, modelVersionResponse);
        }


        AtlasEntity latestModelVersionEntity = modelVersionResponse.getReplicaEntity();
        AtlasVertex latestModelVersionVertex = modelVersionResponse.getReplicaVertex();

        AtlasEntity existingEntityAttributeWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(entityAttribute.getGuid(), false).getEntity();
        AtlasVertex existingAttributeVertex= entityRetriever.getEntityVertex(entityAttribute.getGuid());

        // create attribute a1 ---> a1'
        ModelResponse modelAttributeResponse = replicateModelAttribute(
                existingEntityAttributeWithExtInfo,
                existingAttributeVertex,
                attributeQualifiedNamePrefix,
                now);

        AtlasVertex copyAttributeVertex = modelAttributeResponse.getReplicaVertex();
        AtlasEntity copyAttribute = modelAttributeResponse.getReplicaEntity();
        applyDiffs(entityAttribute, copyAttribute, MODEL_ATTRIBUTE);
        unsetExpiredDates(copyAttribute, copyAttributeVertex);

        // create modelVersion-entity relationship [with new entity]
        createModelVersionModelEntityRelationship(latestModelVersionVertex, modelENtityResponse.getReplicaVertex());

        // create modelVersion-entity relationship [with existing entities]
        if (modelVersionResponse.getExistingEntity() != null && modelVersionResponse.getExistingEntity().getRelationshipAttributes() != null) {
            List<AtlasRelatedObjectId> existingEntities = (List<AtlasRelatedObjectId>) modelVersionResponse.getExistingEntity().getRelationshipAttributes().get(MODEL_VERSION_ENTITIES);
            createModelVersionModelEntityRelationship(latestModelVersionVertex, modelENtityResponse.getExistingEntity() , existingEntities, context);
        }


        // create entity - attribute relation [with new attribute]
        createModelEntityModelAttributeRelation(modelENtityResponse.getReplicaVertex(), copyAttributeVertex);


        // create entity - attribute relation [with existing attributes]
        if (modelENtityResponse.getExistingEntity() != null && modelENtityResponse.getExistingEntity().getRelationshipAttributes() != null) {
            List<AtlasRelatedObjectId>  existingAttributes = (List<AtlasRelatedObjectId>) modelENtityResponse.getExistingEntity().getRelationshipAttributes().get(MODEL_ENTITY_ATTRIBUTES);
            createModelEntityModelAttributeRelation(modelENtityResponse.getReplicaVertex(), existingEntityAttributeWithExtInfo, existingAttributes, context);
        }

        AtlasEntityType attributeType = typeRegistry.getEntityTypeByName(entityAttribute.getTypeName());
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(modelENtityResponse.getReplicaEntity().getTypeName());
        AtlasEntityType modelVersionType = typeRegistry.getEntityTypeByName(latestModelVersionEntity.getTypeName());

        context.addCreated(copyAttribute.getGuid(), copyAttribute, attributeType, copyAttributeVertex);
        context.addCreated(modelENtityResponse.getReplicaEntity().getGuid(), modelENtityResponse.getReplicaEntity(),
                entityType, modelENtityResponse.getReplicaVertex());
        context.addCreated(latestModelVersionEntity.getGuid(),
                latestModelVersionEntity, modelVersionType, latestModelVersionVertex);

        context.removeUpdated(entityAttribute.getGuid(), entityAttribute,
                attributeType, existingAttributeVertex);
        context.addUpdated(entityAttribute.getGuid(), existingEntityAttributeWithExtInfo,
                attributeType, existingAttributeVertex);

        context.removeUpdated(modelENtityResponse.getExistingEntity().getGuid(),
                modelENtityResponse.getExistingEntity(), entityType, modelENtityResponse.getExistingVertex());
        context.addUpdated(modelENtityResponse.getExistingEntity().getGuid(),
                modelENtityResponse.getExistingEntity(), entityType, modelENtityResponse.getExistingVertex());

       context.removeUpdated(modelVersionResponse.getExistingEntity().getGuid(),
               modelVersionResponse.getExistingEntity(), modelVersionType,
               modelVersionResponse.getExistingVertex());
        context.addUpdated(modelVersionResponse.getExistingEntity().getGuid(),
                modelVersionResponse.getExistingEntity(), modelVersionType,
                modelVersionResponse.getExistingVertex());

        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        modelGuid,
                        entityRetriever.getEntityVertex(modelGuid));

        return new ModelResponse(copyAttribute, copyAttributeVertex);
    }

}
