package org.apache.atlas.repository.store.graph.v2;

import com.google.inject.Inject;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.store.graph.ObjectPropagationExecutorV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagPropagate implements ObjectPropagationExecutorV2 {

    private static final Logger LOG      = LoggerFactory.getLogger(EntityGraphMapper.class);
    private final EntityGraphMapper entityGraphMapper;

    @Inject
    public TagPropagate(EntityGraphMapper entityGraphMapper) {
        this.entityGraphMapper = entityGraphMapper;
    }

    public void attach(String entityGuid, String classificationVertexId) throws AtlasBaseException {
        entityGraphMapper.processClassificationPropagationAddition(entityGuid, classificationVertexId);
    }

    public void detach(String entityGuid, String classificationVertexId) throws AtlasBaseException {
        entityGraphMapper.deleteSingleClassificationPropagation(entityGuid, classificationVertexId);
    }

    public void updateText(String entityGuid, String classificationVertexId) throws AtlasBaseException{
        entityGraphMapper.updateClassificationTextPropagation(entityGuid, classificationVertexId);
    }

}
