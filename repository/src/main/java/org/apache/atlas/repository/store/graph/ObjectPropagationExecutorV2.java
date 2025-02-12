package org.apache.atlas.repository.store.graph;

import org.apache.atlas.exception.AtlasBaseException;

public interface ObjectPropagationExecutorV2 {

    void attach(String entityVertexGuid1, String entityVertexGuid2) throws AtlasBaseException;
    void detach(String entityVertexGuid1, String entityVertexGuid2) throws AtlasBaseException;

    public void updateText(String entityGuid, String classificationVertexId) throws AtlasBaseException;

}
