/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.model.instance.AtlasEntity;

import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.model.ModelResponse;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.commons.lang.StringUtils;

import java.util.*;

public class EntityMutationContext {
    private final EntityGraphDiscoveryContext context;
    private final List<AtlasEntity> entitiesCreated = new ArrayList<>();
    private final List<AtlasEntity> entitiesUpdated = new ArrayList<>();
    private List<AtlasEntity> entitiesUpdatedWithAppendRelationshipAttribute = new ArrayList<>();
    private List<AtlasEntity> entitiesUpdatedWithRemoveRelationshipAttribute = new ArrayList<>();
    private final Map<String, AtlasEntityType> entityVsType = new HashMap<>();
    private final Map<String, AtlasVertex> entityVsVertex = new HashMap<>();
    private final Map<String, String> guidAssignments = new HashMap<>();
    private List<AtlasVertex> entitiesToDelete = null;
    private List<AtlasVertex> entitiesToRestore = null;

    private Map<String, ModelResponse> modelVersionCache = new HashMap<>();

    private Map<String, String> modelCache = new HashMap<>();

    private Map<String, ModelResponse> modelEntityCache = new HashMap<>();
    private Set<String> modelEntitiesToBeUpdated= new HashSet<>();
    private Set<String> modelAttributesToBeUpdated= new HashSet<>();

    private Set<String> removedLineageRelations = new HashSet<>();

    public EntityMutationContext(final EntityGraphDiscoveryContext context) {
        this.context = context;
    }

    public EntityMutationContext() {
        this.context = null;
    }

    public void addCreated(String internalGuid, AtlasEntity entity, AtlasEntityType type, AtlasVertex atlasVertex) {
        entitiesCreated.add(entity);
        entityVsType.put(entity.getGuid(), type);
        entityVsVertex.put(entity.getGuid(), atlasVertex);

        if (!StringUtils.equals(internalGuid, entity.getGuid())) {
            guidAssignments.put(internalGuid, entity.getGuid());
            entityVsVertex.put(internalGuid, atlasVertex);
        }
    }

    public void setUpdatedWithRelationshipAttributes(AtlasEntity entity) {
        entitiesUpdatedWithAppendRelationshipAttribute.add(entity);
    }

    public void setUpdatedWithRemoveRelationshipAttributes(AtlasEntity entity) {
        entitiesUpdatedWithRemoveRelationshipAttribute.add(entity);
    }

    public Collection<AtlasEntity> getEntitiesUpdatedWithRemoveRelationshipAttribute() {
        return entitiesUpdatedWithRemoveRelationshipAttribute;
    }

    public void addUpdated(String internalGuid, AtlasEntity entity, AtlasEntityType type, AtlasVertex atlasVertex) {
        if (!entityVsVertex.containsKey(internalGuid)) { // if the entity was already created/updated
            entitiesUpdated.add(entity);
            entityVsType.put(entity.getGuid(), type);
            entityVsVertex.put(entity.getGuid(), atlasVertex);

            if (!StringUtils.equals(internalGuid, entity.getGuid())) {
                guidAssignments.put(internalGuid, entity.getGuid());
                entityVsVertex.put(internalGuid, atlasVertex);
            }
        }
    }

    public void removeUpdated(String internalGuid, AtlasEntity entity, AtlasEntityType type, AtlasVertex atlasVertex) {
        if (entityVsVertex.containsKey(internalGuid)) { // if the entity was already created/updated
            entitiesUpdated.remove(entity);
            entityVsType.remove(entity.getGuid(), type);
            entityVsVertex.remove(entity.getGuid(), atlasVertex);

            if (StringUtils.equals(internalGuid, entity.getGuid())) {
                guidAssignments.remove(internalGuid, entity.getGuid());
                entityVsVertex.remove(internalGuid, atlasVertex);
            }
        }
    }

    public void removeUpdatedWithRelationshipAttributes(AtlasEntity entity) {
        Iterator<AtlasEntity> entities = entitiesUpdatedWithAppendRelationshipAttribute.iterator();
        while (entities.hasNext()) {
            String guid = entities.next().getGuid();
            if (guid.equals(entity.getGuid())) {
                entities.remove();
            }
        }
    }

    public void removeUpdatedWithDeleteRelationshipAttributes(AtlasEntity entity) {
        Iterator<AtlasEntity> entities = entitiesUpdatedWithRemoveRelationshipAttribute.iterator();
        while (entities.hasNext()) {
            String guid = entities.next().getGuid();
            if (guid.equals(entity.getGuid())) {
                entities.remove();
            }
        }
    }

    public void addEntityToRestore(AtlasVertex vertex) {
        if (entitiesToRestore == null) {
            entitiesToRestore = new ArrayList<>();
        }

        entitiesToRestore.add(vertex);
    }

    public void addEntityToDelete(AtlasVertex vertex) {
        if (entitiesToDelete == null) {
            entitiesToDelete = new ArrayList<>();
        }

        entitiesToDelete.add(vertex);
    }

    public void cacheEntity(String guid, AtlasVertex vertex, AtlasEntityType entityType) {
        entityVsType.put(guid, entityType);
        entityVsVertex.put(guid, vertex);
    }

    public void cacheModelVersion(String modelQualifiedName, ModelResponse modelVersionResponse) {
        modelVersionCache.putIfAbsent(modelQualifiedName, modelVersionResponse);
    }

    public ModelResponse getModelVersion(String modelQualifiedName) {
        return modelVersionCache.get(modelQualifiedName);
    }

    public void cacheModel(String modelQualifiedName, String modelGuid) {
        modelCache.putIfAbsent(modelQualifiedName, modelGuid);
    }

    public String getModel(String modelQualifiedName) {
        return modelCache.get(modelQualifiedName);
    }

    public EntityGraphDiscoveryContext getDiscoveryContext() {
        return this.context;
    }

    public Collection<AtlasEntity> getCreatedEntities() {
        return entitiesCreated;
    }

    public Collection<AtlasEntity> getUpdatedEntities() {
        return entitiesUpdated;
    }

    public Collection<AtlasEntity> getUpdatedEntitiesForAppendRelationshipAttribute() {
        return entitiesUpdatedWithAppendRelationshipAttribute;
    }

    public Map<String, String> getGuidAssignments() {
        return guidAssignments;
    }

    public List<AtlasVertex> getEntitiesToDelete() {
        return entitiesToDelete;
    }

    public List<AtlasVertex> getEntitiesToRestore() {
        return entitiesToRestore;
    }

    public AtlasEntityType getType(String guid) {
        return entityVsType.get(guid);
    }

    public AtlasVertex getVertex(String guid) {
        return entityVsVertex.get(guid);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final EntityMutationContext that = (EntityMutationContext) o;

        return Objects.equals(context, that.context) &&
                Objects.equals(entitiesCreated, that.entitiesCreated) &&
                Objects.equals(entitiesUpdated, that.entitiesUpdated) &&
                Objects.equals(entityVsType, that.entityVsType) &&
                Objects.equals(entityVsVertex, that.entityVsVertex);
    }

    @Override
    public int hashCode() {
        int result = (context != null ? context.hashCode() : 0);
        result = 31 * result + entitiesCreated.hashCode();
        result = 31 * result + entitiesUpdated.hashCode();
        result = 31 * result + entityVsType.hashCode();
        result = 31 * result + entityVsVertex.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "EntityMutationContext{" +
                "context=" + context +
                ", entitiesCreated=" + entitiesCreated +
                ", entitiesUpdated=" + entitiesUpdated +
                ", entityVsType=" + entityVsType +
                ", entityVsVertex=" + entityVsVertex +
                '}';
    }

    public AtlasEntity getCreatedEntity(String parentGuid) {
        return getFromCollection(parentGuid, getCreatedEntities());
    }

    public AtlasEntity getUpdatedEntity(String parentGuid) {
        return getFromCollection(parentGuid, getUpdatedEntities());
    }

    public boolean isDeletedEntity(AtlasVertex vertex) {
        return entitiesToDelete != null && entitiesToDelete.contains(vertex);
    }

    public boolean isRestoredEntity(AtlasVertex vertex) {
        return entitiesToRestore != null && entitiesToRestore.contains(vertex);
    }

    private AtlasEntity getFromCollection(String parentGuid, Collection<AtlasEntity> coll) {
        for (AtlasEntity e : coll) {
            if (e.getGuid().equalsIgnoreCase(parentGuid)) {
                return e;
            }
        }

        return null;
    }

    public AtlasEntity getCreatedOrUpdatedEntity(String parentGuid) {
        AtlasEntity e = getCreatedEntity(parentGuid);
        if (e == null) {
            return getUpdatedEntity(parentGuid);
        }

        return e;
    }

    public Set<String> getRemovedLineageRelations() {
        return removedLineageRelations;
    }

    public void addRemovedLineageRelations(Set<String> removedLineageRelations) {
        this.removedLineageRelations.addAll(removedLineageRelations);
    }

    public ModelResponse getModelEntity(String entityQualifiedNamePrefix) {
        return modelEntityCache.get(entityQualifiedNamePrefix);
    }

    public void cacheModelEntity(String qualifiedNamePrefix, ModelResponse modelEntity) {
        modelEntityCache.putIfAbsent(qualifiedNamePrefix, modelEntity);
    }

    public Set<String> getModelEntitiesToBeUpdated() {
        return modelEntitiesToBeUpdated;
    }

    public void setModelEntitiesToBeUpdated(Set<String> modelEntitiesToBeUpdated) {
        this.modelEntitiesToBeUpdated = modelEntitiesToBeUpdated;
    }

    public Set<String> getModelAttributesToBeUpdated() {
        return modelAttributesToBeUpdated;
    }

    public void setModelAttributesToBeUpdated(Set<String> modelAttributesToBeUpdated) {
        this.modelAttributesToBeUpdated = modelAttributesToBeUpdated;
    }

    public void updateModelEntitiesSet(String qualifiedNamePrefix){
        modelEntitiesToBeUpdated.add(qualifiedNamePrefix);
    }

    public void updateModelAttributesSet(String qualifiedNamePrefix){
        modelAttributesToBeUpdated.add(qualifiedNamePrefix);
    }
}
