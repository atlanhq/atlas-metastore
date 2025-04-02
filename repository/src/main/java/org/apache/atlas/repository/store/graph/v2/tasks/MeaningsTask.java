package org.apache.atlas.repository.store.graph.v2.tasks;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.exception.EntityNotFoundException;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.preprocessor.glossary.TermPreProcessor;
import org.apache.atlas.tasks.AbstractTask;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.model.tasks.AtlasTask.Status.*;
import static org.apache.atlas.repository.store.graph.v2.tasks.MeaningsTaskFactory.UPDATE_ENTITY_MEANINGS_ON_TERM_HARD_DELETE;

public abstract class MeaningsTask extends AbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(MeaningsTask.class);
    protected static final String PARAM_ENTITY_GUID = "entityGuid";
    protected static final String PARAM_ENTITY_QUALIFIED_NAME = "entityQName";
    protected static final String PARAM_ENTITY_UPDATED_QUALIFIED_NAME = "updatedEntityQName";
    protected static final String PARAM_UPDATED_TERM_NAME = "updatedTermName";
    protected static final String PARAM_CURRENT_TERM_NAME = "currentTermName";


    protected final EntityGraphMapper entityGraphMapper;
    protected final AtlasGraph graph;
    protected final TermPreProcessor preprocessor;
    protected final AtlasEntityStoreV2 entityStoreV2;


    public MeaningsTask(AtlasTask task, EntityGraphMapper entityGraphMapper,
                        AtlasGraph graph, TermPreProcessor preprocessor, AtlasEntityStoreV2 entityStoreV2) {
        super(task);
        this.entityGraphMapper = entityGraphMapper;
        this.graph = graph;
        this.preprocessor = preprocessor;
        this.entityStoreV2 = entityStoreV2;
    }

    @Override
    public AtlasTask.Status perform() throws Exception {
        Map<String, Object> params;
        params = getTaskDef().getParameters();
        if (!MapUtils.isEmpty(params)) {
            String userName = getTaskDef().getCreatedBy();

            if (StringUtils.isEmpty(userName)) {
                LOG.warn("Task: {}: Unable to process task as user name is empty!", getTaskGuid());

                return FAILED;
            }

            RequestContext.get().setUser(userName, null);
            try {
                setStatus(IN_PROGRESS);

                run(params);

                setStatus(COMPLETE);
            } catch (Exception e) {
                LOG.error("Task: {}: Error performing task!", getTaskGuid(), e);

                setStatus(FAILED);

                throw e;
            } finally {
                graph.commit();
            }
            return getStatus();
        } else {
            LOG.warn("Task: {}: Unable to process task: Parameters is not readable!", getTaskGuid());

            return FAILED;
        }
    }

    public static Map<String, Object> toParameters(String currentTerm, String updateTerm, String termQName, String updatedTermQName, String termGuid) {
        return new HashMap<String, Object>() {{
            put(PARAM_ENTITY_GUID, termGuid);
            put(PARAM_ENTITY_QUALIFIED_NAME, termQName);
            put(PARAM_ENTITY_UPDATED_QUALIFIED_NAME, updatedTermQName);
            put(PARAM_CURRENT_TERM_NAME, currentTerm);
            put(PARAM_UPDATED_TERM_NAME, updateTerm);
        }};
    }
    public static  Map<String,Object> toParameters(String termName, String termQName, String termGuid){
        return new HashMap<String,Object>(){{
            put(PARAM_ENTITY_QUALIFIED_NAME, termQName);
            put(PARAM_ENTITY_GUID, termGuid);
            put(PARAM_CURRENT_TERM_NAME, termName);
        }};
    }

    protected void setStatus(AtlasTask.Status status) {
        super.setStatus(status);
        try {
            if(UPDATE_ENTITY_MEANINGS_ON_TERM_HARD_DELETE.equals(getTaskType())){
                LOG.info("Entity Vertex Deleted, No Need to remove pending task for: {} ",getTaskGuid());
            }else {
                entityGraphMapper.removePendingTaskFromEntity((String) getTaskDef().getParameters().get(PARAM_ENTITY_GUID), getTaskGuid());
            }
        } catch (EntityNotFoundException  e) {
            LOG.error("Error updating associated element for: {}", getTaskGuid(), e);
        }

    }

    protected abstract void run(Map<String, Object> parameters) throws AtlasBaseException;
}
