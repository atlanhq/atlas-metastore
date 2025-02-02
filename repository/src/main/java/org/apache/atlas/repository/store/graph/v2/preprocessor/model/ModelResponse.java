package org.apache.atlas.repository.store.graph.v2.preprocessor.model;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasVertex;

public class ModelResponse {

    private AtlasEntity existingEntity;
    private AtlasEntity replicaEntity;
    private AtlasVertex existingVertex;
    private AtlasVertex replicaVertex;

    public ModelResponse(AtlasEntity existingEntity, AtlasEntity replicaEntity,
                         AtlasVertex existingVertex, AtlasVertex replicaVertex) {
        this.existingEntity = existingEntity;
        this.replicaEntity = replicaEntity;
        this.existingVertex = existingVertex;
        this.replicaVertex = replicaVertex;
    }

    public ModelResponse(AtlasEntity replicaEntity, AtlasVertex replicaVertex) {
        this.replicaEntity = replicaEntity;
        this.replicaVertex = replicaVertex;
    }

    public AtlasEntity getExistingEntity() {
        return existingEntity;
    }

    public AtlasEntity getReplicaEntity() {
        return replicaEntity;
    }

    public AtlasVertex getExistingVertex() {
        return existingVertex;
    }

    public AtlasVertex getReplicaVertex() {
        return replicaVertex;
    }
}
