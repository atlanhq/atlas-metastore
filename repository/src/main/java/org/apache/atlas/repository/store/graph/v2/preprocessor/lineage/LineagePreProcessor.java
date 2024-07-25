/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2.preprocessor.lineage;


import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.Constants.NAME;

public class LineagePreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(LineagePreProcessor.class);

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private AtlasEntityStore entityStore;


    public LineagePreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph, AtlasEntityStore entityStore) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.entityStore = entityStore;
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("LineageProcessPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                processCreateLineageProcess(entity);
                break;
            case UPDATE:
                processUpdateLineageProcess(entity, vertex, context);
                break;
        }
    }

    private void processCreateLineageProcess(AtlasEntity entity) {
        // check if connection lineage exists
        Map<String, Object> relAttrValues = entity.getRelationshipAttributes();

        relAttrValues.get("outputs");
        relAttrValues.get("inputs");


        // if not exist create lineage process
    }

    private void processUpdateLineageProcess(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) {
        // check if connection lineage exists

        // if not exist update lineage process

    }

    private void createConnectionProcessEntity(AtlasEntity entity, String connectionProcessName, String connectionProcessQn, Map<String, Object> relAttrValues) throws AtlasBaseException {
        AtlasEntity processEntity = new AtlasEntity();
        processEntity.setTypeName(PROCESS_ENTITY_TYPE);
        processEntity.setAttribute(NAME, connectionProcessName);
        processEntity.setAttribute(QUALIFIED_NAME,  connectionProcessQn + "/process");
        processEntity.setRelationshipAttributes(relAttrValues);

        try {
            RequestContext.get().setSkipAuthorizationCheck(true);
            AtlasEntity.AtlasEntitiesWithExtInfo processExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
            processExtInfo.addEntity(processEntity);
            EntityStream entityStream = new AtlasEntityStream(processExtInfo);
            entityStore.createOrUpdate(entityStream, false); // adding new process
        } finally {
            RequestContext.get().setSkipAuthorizationCheck(false);
        }
    }

    private void updateConnectionProcessEntity(AtlasEntity processEntity) throws AtlasBaseException{

    }
}
