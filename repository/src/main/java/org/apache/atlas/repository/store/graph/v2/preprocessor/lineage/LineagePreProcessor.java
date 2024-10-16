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


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.DeleteType;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.repository.util.AtlasEntityUtils;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.graph.GraphHelper.fetchAttributes;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.indexSearchPaginated;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class LineagePreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(LineagePreProcessor.class);
    private static final List<String> FETCH_ENTITY_ATTRIBUTES = Arrays.asList(CONNECTION_QUALIFIED_NAME);
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private AtlasEntityStore entityStore;
    protected EntityDiscoveryService discovery;

    public LineagePreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph, AtlasEntityStore entityStore) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.entityStore = entityStore;
        try {
            this.discovery = new EntityDiscoveryService(typeRegistry, graph, null, null, null, null);
        } catch (AtlasException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {

        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processAttributesForLineagePreprocessor");

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("LineageProcessPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
            }

            AtlasEntity entity = (AtlasEntity) entityStruct;
            AtlasVertex vertex = context.getVertex(entity.getGuid());
            ArrayList<String> connectionProcessQNs = getConnectionProcessQNsForTheGivenInputOutputs(entity);

            switch (operation) {
                case CREATE:
                    processCreateLineageProcess(entity, connectionProcessQNs);
                    break;
                case UPDATE:
                    processUpdateLineageProcess(entity, vertex, context, connectionProcessQNs);
                    break;
            }
        }catch(Exception exp){
            if (LOG.isDebugEnabled()) {
                LOG.debug("Lineage preprocessor: " + exp);
            }
        }finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

    }

    private void processCreateLineageProcess(AtlasEntity entity, ArrayList connectionProcessList) {
        // if not exist create lineage process
        // add owner connection process
        if(!connectionProcessList.isEmpty()){
            entity.setAttribute(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME, connectionProcessList);
        }
    }

    private void processUpdateLineageProcess(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context, ArrayList connectionProcessList) {
        // check if connection lineage exists
        // add owner connection process
        if(!connectionProcessList.isEmpty()){
            entity.setAttribute(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME, connectionProcessList);
        }
    }

    private AtlasEntity createConnectionProcessEntity(Map<String, Object> connectionProcessInfo) throws AtlasBaseException {
        AtlasEntity processEntity = new AtlasEntity();
        processEntity.setTypeName(CONNECTION_PROCESS_ENTITY_TYPE);
        processEntity.setAttribute(NAME, connectionProcessInfo.get("connectionProcessName"));
        processEntity.setAttribute(QUALIFIED_NAME,connectionProcessInfo.get("connectionProcessQualifiedName"));


        // fetch connection and add as relationship attributes
        AtlasObjectId inputConnection = new AtlasObjectId();
        inputConnection.setTypeName(CONNECTION_ENTITY_TYPE);
        inputConnection.setUniqueAttributes(mapOf(QUALIFIED_NAME, connectionProcessInfo.get("input")));

        AtlasObjectId outputConnection = new AtlasObjectId();
        outputConnection.setTypeName(CONNECTION_ENTITY_TYPE);
        outputConnection.setUniqueAttributes(mapOf(QUALIFIED_NAME, connectionProcessInfo.get("output")));


        Map<String, Object> connectionProcessMap = new HashMap<>();
        connectionProcessMap.put("input", inputConnection);
        connectionProcessMap.put("output", outputConnection);

        processEntity.setRelationshipAttributes(connectionProcessMap);

        try {
            RequestContext.get().setSkipAuthorizationCheck(true);
            AtlasEntity.AtlasEntitiesWithExtInfo processExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
            processExtInfo.addEntity(processEntity);
            EntityStream entityStream = new AtlasEntityStream(processExtInfo);
            entityStore.createOrUpdate(entityStream, false); // adding new process
        } finally {
            RequestContext.get().setSkipAuthorizationCheck(false);
        }

        return processEntity;
    }

    private ArrayList<String> getConnectionProcessQNsForTheGivenInputOutputs(AtlasEntity processEntity) throws  AtlasBaseException{

        // check connection lineage exists or not
        // check if connection lineage exists
        Map<String, Object> entityAttrValues = processEntity.getRelationshipAttributes();

        ArrayList<AtlasObjectId> inputsAssets = (ArrayList<AtlasObjectId>) entityAttrValues.get("inputs");
        ArrayList<AtlasObjectId> outputsAssets  = (ArrayList<AtlasObjectId>) entityAttrValues.get("outputs");

        // get connection process
        Set<Map<String,Object>> uniquesSetOfConnectionProcess = new HashSet<>();

        for (AtlasObjectId input : inputsAssets){
            AtlasVertex inputVertex = entityRetriever.getEntityVertex(input);
            Map<String,String> inputVertexConnectionQualifiedName = fetchAttributes(inputVertex, FETCH_ENTITY_ATTRIBUTES);
            for (AtlasObjectId output : outputsAssets){
                AtlasVertex outputVertex = entityRetriever.getEntityVertex(output);
                Map<String,String> outputVertexConnectionQualifiedName = fetchAttributes(outputVertex, FETCH_ENTITY_ATTRIBUTES);

                if(inputVertexConnectionQualifiedName.get(CONNECTION_QUALIFIED_NAME) == outputVertexConnectionQualifiedName.get(CONNECTION_QUALIFIED_NAME)){
                    continue;
                }

                String connectionProcessName = "(" + inputVertexConnectionQualifiedName.get(CONNECTION_QUALIFIED_NAME) + ")->(" + outputVertexConnectionQualifiedName.get(CONNECTION_QUALIFIED_NAME) + ")";
                String connectionProcessQualifiedName = outputVertexConnectionQualifiedName.get(CONNECTION_QUALIFIED_NAME) + "/" + connectionProcessName;
                // Create a map to store both connectionProcessName and connectionProcessQualifiedName
                Map<String, Object> connectionProcessMap = new HashMap<>();
                connectionProcessMap.put("input", inputVertexConnectionQualifiedName.get(CONNECTION_QUALIFIED_NAME));
                connectionProcessMap.put("output", outputVertexConnectionQualifiedName.get(CONNECTION_QUALIFIED_NAME));
                connectionProcessMap.put("connectionProcessName", connectionProcessName);
                connectionProcessMap.put("connectionProcessQualifiedName", connectionProcessQualifiedName);

                // Add the map to the set
                uniquesSetOfConnectionProcess.add(connectionProcessMap);
            }
        }

        ArrayList connectionProcessList = new ArrayList<>();

        // check if connection process exists
        for (Map<String, Object> connectionProcessInfo  : uniquesSetOfConnectionProcess){
            AtlasObjectId atlasObjectId = new AtlasObjectId();
            atlasObjectId.setTypeName(CONNECTION_PROCESS_ENTITY_TYPE);
            atlasObjectId.setUniqueAttributes(mapOf(QUALIFIED_NAME, connectionProcessInfo.get("connectionProcessQualifiedName")));
            AtlasVertex connectionProcessVertex = null;
            try {
                // TODO add caching here
                connectionProcessVertex = entityRetriever.getEntityVertex(atlasObjectId);
            }
            catch(AtlasBaseException exp){
                if(!exp.getAtlasErrorCode().equals(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)){
                    throw exp;
                }
            }

            AtlasEntity connectionProcess;
            if (connectionProcessVertex == null) {
                connectionProcess = createConnectionProcessEntity(connectionProcessInfo);
            } else {
                // exist so retrieve and perform any update so below statement to retrieve
                // TODO add caching here
                connectionProcess = entityRetriever.toAtlasEntity(connectionProcessVertex);
            }
            // only add in list if created
            connectionProcessList.add(connectionProcess.getAttribute(QUALIFIED_NAME));
        }

        return connectionProcessList;
    }

    public boolean checkIfMoreChildProcessExistForConnectionProcess(String connectionProcessQn) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("checkIfMoreChileProcessExistForConnectionProcess");
        boolean ret = false;

        try {
            List mustClauseList = new ArrayList();
            mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", PROCESS_ENTITY_TYPE)));
            mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
            mustClauseList.add(mapOf("term", mapOf(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME, connectionProcessQn)));

            Map<String, Object> dsl = mapOf("query", mapOf("bool", mapOf("must", mustClauseList)));

            List<AtlasEntityHeader> process = indexSearchPaginated(dsl, new HashSet<>(Arrays.asList(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME)) , this.discovery);

            if (CollectionUtils.isNotEmpty(process) && process.size()>1) {
               ret = true;
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
        return ret;
    }

    // handle process delete logic
    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processDeleteLineageProcess");

        try{
            if(RequestContext.get().getDeleteType() != DeleteType.SOFT) {
                List connectionProcessQNs = vertex.getMultiValuedProperty(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME,List.class);
                if (connectionProcessQNs==null || connectionProcessQNs.isEmpty()) {
                    return;
                }

                // check for each connectionProcessQn if more process exist having same QN then keep connection process else delete
                for (String connectionProcessQn : (ArrayList<String>)connectionProcessQNs) {
                    if (!checkIfMoreChildProcessExistForConnectionProcess(connectionProcessQn)) {
                        AtlasObjectId atlasObjectId = new AtlasObjectId();
                        atlasObjectId.setTypeName(CONNECTION_PROCESS_ENTITY_TYPE);
                        atlasObjectId.setUniqueAttributes(AtlasEntityUtils.mapOf(QUALIFIED_NAME, connectionProcessQn));
                        AtlasVertex connectionProcessVertex;
                        try {
                            connectionProcessVertex = entityRetriever.getEntityVertex(atlasObjectId);
                            entityStore.deleteById(connectionProcessVertex.getProperty("__guid", String.class));
                        } catch (AtlasBaseException exp) {
                            if (!exp.getAtlasErrorCode().equals(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)) {
                                throw exp;
                            }
                        }
                    }
                }
            }
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }
}
