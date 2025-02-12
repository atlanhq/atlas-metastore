package org.apache.atlas.repository.store.graph.v2;

import com.google.inject.Inject;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.exception.EntityNotFoundException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.TaskV2Request;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.ObjectPropagationExecutorV2;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_LABEL;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;

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
        entityGraphMapper.deleteClassificationPropagation_(entityGuid, classificationVertexId);
    }

    public void updateText(String entityGuid, String classificationVertexId) throws AtlasBaseException{
        entityGraphMapper.updateClassificationTextPropagation(entityGuid, classificationVertexId);
    }

}
