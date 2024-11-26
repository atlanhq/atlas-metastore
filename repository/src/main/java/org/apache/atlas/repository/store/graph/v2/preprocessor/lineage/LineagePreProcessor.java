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
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.graph.GraphHelper.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.indexSearchPaginated;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class LineagePreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(LineagePreProcessor.class);
    private static final List<String> FETCH_ENTITY_ATTRIBUTES = Arrays.asList(CONNECTION_QUALIFIED_NAME);
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private AtlasEntityStore entityStore;
    protected EntityDiscoveryService discovery;
    private static final String HAS_LINEAGE = "__hasLineage";

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
        processEntity.setAttribute(QUALIFIED_NAME, connectionProcessInfo.get("connectionProcessQualifiedName"));

        // Set up relationship attributes for input and output connections
        AtlasObjectId inputConnection = new AtlasObjectId();
        inputConnection.setTypeName(CONNECTION_ENTITY_TYPE);
        inputConnection.setUniqueAttributes(mapOf(QUALIFIED_NAME, connectionProcessInfo.get("input")));

        AtlasObjectId outputConnection = new AtlasObjectId();
        outputConnection.setTypeName(CONNECTION_ENTITY_TYPE);
        outputConnection.setUniqueAttributes(mapOf(QUALIFIED_NAME, connectionProcessInfo.get("output")));

        Map<String, Object> relationshipAttributes = new HashMap<>();
        relationshipAttributes.put("inputs", Collections.singletonList(inputConnection));
        relationshipAttributes.put("outputs", Collections.singletonList(outputConnection));
        processEntity.setRelationshipAttributes(relationshipAttributes);

        try {
            RequestContext.get().setSkipAuthorizationCheck(true);
            AtlasEntity.AtlasEntitiesWithExtInfo processExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
            processExtInfo.addEntity(processEntity);
            EntityStream entityStream = new AtlasEntityStream(processExtInfo);
            entityStore.createOrUpdate(entityStream, false);

            // Update hasLineage for both connections
            updateConnectionLineageFlag((String) connectionProcessInfo.get("input"), true);
            updateConnectionLineageFlag((String) connectionProcessInfo.get("output"), true);
        } finally {
            RequestContext.get().setSkipAuthorizationCheck(false);
        }

        return processEntity;
    }

    private void updateConnectionLineageFlag(String connectionQualifiedName, boolean hasLineage) throws AtlasBaseException {
        AtlasObjectId connectionId = new AtlasObjectId();
        connectionId.setTypeName(CONNECTION_ENTITY_TYPE);
        connectionId.setUniqueAttributes(mapOf(QUALIFIED_NAME, connectionQualifiedName));

        try {
            AtlasVertex connectionVertex = entityRetriever.getEntityVertex(connectionId);
            AtlasEntity connection = entityRetriever.toAtlasEntity(connectionVertex);
            connection.setAttribute(HAS_LINEAGE, hasLineage);

            AtlasEntity.AtlasEntitiesWithExtInfo connectionExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
            connectionExtInfo.addEntity(connection);
            EntityStream entityStream = new AtlasEntityStream(connectionExtInfo);
            
            RequestContext.get().setSkipAuthorizationCheck(true);
            try {
                entityStore.createOrUpdate(entityStream, false);
            } finally {
                RequestContext.get().setSkipAuthorizationCheck(false);
            }
        } catch (AtlasBaseException e) {
            if (!e.getAtlasErrorCode().equals(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)) {
                throw e;
            }
        }
    }

    private void checkAndUpdateConnectionLineage(String connectionQualifiedName) throws AtlasBaseException {
        AtlasObjectId connectionId = new AtlasObjectId();
        connectionId.setTypeName(CONNECTION_ENTITY_TYPE);
        connectionId.setUniqueAttributes(mapOf(QUALIFIED_NAME, connectionQualifiedName));

        try {
            AtlasVertex connectionVertex = entityRetriever.getEntityVertex(connectionId);
            
            // Check both input and output edges
            boolean hasActiveConnectionProcess = hasActiveConnectionProcesses(connectionVertex);

            // Only update if the hasLineage status needs to change
            boolean currentHasLineage = getEntityHasLineage(connectionVertex);
            if (currentHasLineage != hasActiveConnectionProcess) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Updating hasLineage for connection {} from {} to {}", 
                        connectionQualifiedName, currentHasLineage, hasActiveConnectionProcess);
                }

                AtlasEntity connection = entityRetriever.toAtlasEntity(connectionVertex);
                connection.setAttribute(HAS_LINEAGE, hasActiveConnectionProcess);

                AtlasEntity.AtlasEntitiesWithExtInfo connectionExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
                connectionExtInfo.addEntity(connection);
                EntityStream entityStream = new AtlasEntityStream(connectionExtInfo);
                
                RequestContext.get().setSkipAuthorizationCheck(true);
                try {
                    entityStore.createOrUpdate(entityStream, false);
                } finally {
                    RequestContext.get().setSkipAuthorizationCheck(false);
                }
            }
        } catch (AtlasBaseException e) {
            if (!e.getAtlasErrorCode().equals(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)) {
                throw e;
            }
        }
    }

    private boolean hasActiveConnectionProcesses(AtlasVertex connectionVertex) {
        // Check both input and output edges
        Iterator<AtlasEdge> edges = connectionVertex.getEdges(AtlasEdgeDirection.BOTH, 
            new String[]{"__ConnectionProcess.inputs", "__ConnectionProcess.outputs"}).iterator();
        
        while (edges.hasNext()) {
            AtlasEdge edge = edges.next();
            
            if (getStatus(edge) == ACTIVE) {
                AtlasVertex processVertex = edge.getLabel().equals("__ConnectionProcess.inputs") ? 
                    edge.getOutVertex() : edge.getInVertex();
                    
                // If this is an active connection process
                if (getStatus(processVertex) == ACTIVE && 
                    getTypeName(processVertex).equals(CONNECTION_PROCESS_ENTITY_TYPE)) {
                    
                    // Get the other connection in this process
                    AtlasVertex otherConnectionVertex = null;
                    Iterator<AtlasEdge> processEdges = processVertex.getEdges(AtlasEdgeDirection.BOTH, 
                        new String[]{"__ConnectionProcess.inputs", "__ConnectionProcess.outputs"}).iterator();
                    
                    while (processEdges.hasNext()) {
                        AtlasEdge processEdge = processEdges.next();
                        if (getStatus(processEdge) == ACTIVE) {
                            AtlasVertex connVertex = processEdge.getInVertex();
                            if (!connVertex.getId().equals(connectionVertex.getId())) {
                                otherConnectionVertex = connVertex;
                                break;
                            }
                        }
                    }
                    
                    // If the other connection is active, this connection process is valid
                    if (otherConnectionVertex != null && getStatus(otherConnectionVertex) == ACTIVE) {
                        return true;
                    }
                }
            }
        }
        
        return false;
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

        try {
            // handle both soft and hard deletes
            // Collect all connections involved in the process being deleted
            AtlasEntity processEntity = entityRetriever.toAtlasEntity(vertex);

            Set<String> involvedConnections = new HashSet<>();

            // Retrieve inputs and outputs from the process
            List<AtlasObjectId> inputs = (List<AtlasObjectId>) processEntity.getRelationshipAttribute("inputs");
            List<AtlasObjectId> outputs = (List<AtlasObjectId>) processEntity.getRelationshipAttribute("outputs");

            if (inputs == null) inputs = Collections.emptyList();
            if (outputs == null) outputs = Collections.emptyList();

            List<AtlasObjectId> allAssets = new ArrayList<>();
            allAssets.addAll(inputs);
            allAssets.addAll(outputs);

            // For each asset, get its connection and add to involvedConnections
            for (AtlasObjectId assetId : allAssets) {
                try {
                    AtlasVertex assetVertex = entityRetriever.getEntityVertex(assetId);
                    Map<String, String> assetConnectionAttributes = fetchAttributes(assetVertex, FETCH_ENTITY_ATTRIBUTES);
                    if (assetConnectionAttributes != null) {
                        String connectionQN = assetConnectionAttributes.get(CONNECTION_QUALIFIED_NAME);
                        if (StringUtils.isNotEmpty(connectionQN)) {
                            involvedConnections.add(connectionQN);
                        }
                    }
                } catch (AtlasBaseException e) {
                    LOG.warn("Failed to retrieve connection for asset {}: {}", assetId.getGuid(), e.getMessage());
                }
            }

            // Collect affected connections from connection processes to be deleted
            Set<String> connectionProcessQNs = new HashSet<>();

            Object rawProperty = vertex.getProperty(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME, Object.class);

            if (rawProperty instanceof List) {
                // If the property is a List, cast and add all elements
                List<String> propertyList = (List<String>) rawProperty;
                connectionProcessQNs.addAll(propertyList);
            } else if (rawProperty instanceof String) {
                // If it's a single String, add it to the set
                connectionProcessQNs.add((String) rawProperty);
            } else if (rawProperty != null) {
                // Handle other object types if necessary
                connectionProcessQNs.add(rawProperty.toString());
            }

            if (connectionProcessQNs.isEmpty()) {
                return;
            }

            Set<String> affectedConnections = new HashSet<>();

            // Process each connection process
            for (String connectionProcessQn : connectionProcessQNs) {
                if (!checkIfMoreChildProcessExistForConnectionProcess(connectionProcessQn)) {
                    AtlasObjectId atlasObjectId = new AtlasObjectId();
                    atlasObjectId.setTypeName(CONNECTION_PROCESS_ENTITY_TYPE);
                    atlasObjectId.setUniqueAttributes(AtlasEntityUtils.mapOf(QUALIFIED_NAME, connectionProcessQn));

                    try {
                        // Get connection process before deletion to track affected connections
                        AtlasVertex connectionProcessVertex = entityRetriever.getEntityVertex(atlasObjectId);
                        AtlasEntity connectionProcess = entityRetriever.toAtlasEntity(connectionProcessVertex);

                        // Safely get connection qualified names
                        String inputConnQN = getConnectionQualifiedName(connectionProcess, "input");
                        String outputConnQN = getConnectionQualifiedName(connectionProcess, "output");

                        // Add non-null qualified names to affected connections
                        if (StringUtils.isNotEmpty(inputConnQN)) {
                            affectedConnections.add(inputConnQN);
                        }
                        if (StringUtils.isNotEmpty(outputConnQN)) {
                            affectedConnections.add(outputConnQN);
                        }

                        // Delete the connection process
                        entityStore.deleteById(connectionProcessVertex.getProperty("__guid", String.class));
                    } catch (AtlasBaseException exp) {
                        if (!exp.getAtlasErrorCode().equals(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)) {
                            throw exp;
                        }
                    }
                }
            }

            // Combine involved and affected connections
            Set<String> connectionsToCheck = new HashSet<>();
            connectionsToCheck.addAll(involvedConnections);
            connectionsToCheck.addAll(affectedConnections);

            // Check and update hasLineage for all connections involved
            for (String connectionQN : connectionsToCheck) {
                checkAndUpdateConnectionLineage(connectionQN);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    // Helper method to safely get connection qualified name
    private String getConnectionQualifiedName(AtlasEntity connectionProcess, String attributeName) {
        try {
            Object relationshipAttr = connectionProcess.getRelationshipAttribute(attributeName);
            if (relationshipAttr instanceof AtlasObjectId) {
                AtlasObjectId connObjectId = (AtlasObjectId) relationshipAttr;
                Map<String, Object> uniqueAttributes = connObjectId.getUniqueAttributes();
                if (uniqueAttributes != null) {
                    return (String) uniqueAttributes.get(QUALIFIED_NAME);
                }
            }
        } catch (Exception e) {
            LOG.warn("Error getting {} qualified name for connection process {}: {}", 
                    attributeName, connectionProcess.getGuid(), e.getMessage());
        }
        return null;
    }
}
