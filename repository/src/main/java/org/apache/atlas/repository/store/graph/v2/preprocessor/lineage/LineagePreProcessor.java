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

    private void processUpdateLineageProcess(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context, ArrayList<String> newConnectionProcessList) throws AtlasBaseException {
        // Get the old parentConnectionProcessQualifiedName from the existing vertex
        List<String> oldConnectionProcessList = null;
        try {
            Object propertyValue = vertex.getProperty(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME, Object.class);
            if (propertyValue instanceof String) {
                oldConnectionProcessList = Arrays.asList((String) propertyValue);
            } else if (propertyValue instanceof List) {
                oldConnectionProcessList = (List<String>) propertyValue;
            } else if (propertyValue != null) {
                oldConnectionProcessList = Collections.singletonList(propertyValue.toString());
            } else {
                oldConnectionProcessList = Collections.emptyList();
            }
        } catch (Exception e) {
            oldConnectionProcessList = Collections.emptyList();
        }

        // Identify ConnectionProcesses to remove (present in old list but not in new list)
        Set<String> connectionProcessesToRemove = new HashSet<>(oldConnectionProcessList);
        connectionProcessesToRemove.removeAll(newConnectionProcessList);

        // Identify ConnectionProcesses to add (present in new list but not in old list)
        Set<String> connectionProcessesToAdd = new HashSet<>(newConnectionProcessList);
        connectionProcessesToAdd.removeAll(oldConnectionProcessList);

        // For each ConnectionProcess to remove
        for (String connectionProcessQn : connectionProcessesToRemove) {
            // Check if more child Processes exist for this ConnectionProcess
            if (!checkIfMoreChildProcessExistForConnectionProcess(connectionProcessQn)) {
                // Delete the ConnectionProcess
                deleteConnectionProcess(connectionProcessQn);
            }
            // Update __hasLineage for involved Connections
            updateConnectionsHasLineageForConnectionProcess(connectionProcessQn);
        }

        // For new ConnectionProcesses, we've already created or retrieved them in getConnectionProcessQNsForTheGivenInputOutputs

        // Update the Process entity's parentConnectionProcessQualifiedName attribute
        entity.setAttribute(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME, newConnectionProcessList);
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

            // Check if this connection has any active connection processes
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
        // Iterate over both input and output edges connected to this connection
        Iterator<AtlasEdge> edges = connectionVertex.getEdges(AtlasEdgeDirection.BOTH,
                new String[]{"__ConnectionProcess.inputs", "__ConnectionProcess.outputs"}).iterator();

        while (edges.hasNext()) {
            AtlasEdge edge = edges.next();

            // Check if the edge is ACTIVE
            if (getStatus(edge) == ACTIVE) {
                // Get the connected process vertex (the other vertex of the edge)
                AtlasVertex processVertex = edge.getOutVertex().equals(connectionVertex) ?
                        edge.getInVertex() : edge.getOutVertex();

                // Check if the connected vertex is an ACTIVE ConnectionProcess
                if (getStatus(processVertex) == ACTIVE &&
                        getTypeName(processVertex).equals(CONNECTION_PROCESS_ENTITY_TYPE)) {
                    return true;
                }
            }
        }
        return false;
    }

    private ArrayList<String> getConnectionProcessQNsForTheGivenInputOutputs(AtlasEntity processEntity) throws  AtlasBaseException{

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
            AtlasEntity processEntity = entityRetriever.toAtlasEntity(vertex);
            Set<String> connectionsToCheck = new HashSet<>();
            
            // Collect connections from inputs and outputs
            collectConnectionsFromAssets(processEntity.getRelationshipAttribute("inputs"), connectionsToCheck);
            collectConnectionsFromAssets(processEntity.getRelationshipAttribute("outputs"), connectionsToCheck);

            // Handle connection processes
            Object rawConnectionProcessQNs = vertex.getProperty(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME, Object.class);
            if (rawConnectionProcessQNs == null) {
                return;
            }

            Set<String> connectionProcessQNs = new HashSet<>();
            if (rawConnectionProcessQNs instanceof List) {
                connectionProcessQNs.addAll((List<String>) rawConnectionProcessQNs);
            } else {
                connectionProcessQNs.add(rawConnectionProcessQNs.toString());
            }

            // Process each connection process
            for (String connectionProcessQn : connectionProcessQNs) {
                if (!checkIfMoreChildProcessExistForConnectionProcess(connectionProcessQn)) {
                    deleteConnectionProcess(connectionProcessQn, connectionsToCheck);
                }
            }

            // Update hasLineage flags
            for (String connectionQN : connectionsToCheck) {
                checkAndUpdateConnectionLineage(connectionQN);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void collectConnectionsFromAssets(Object assets, Set<String> connections) {
        if (assets instanceof List) {
            for (AtlasObjectId assetId : (List<AtlasObjectId>) assets) {
                try {
                    AtlasVertex assetVertex = entityRetriever.getEntityVertex(assetId);
                    Map<String, String> connectionAttr = fetchAttributes(assetVertex, FETCH_ENTITY_ATTRIBUTES);
                    String connectionQN = connectionAttr.get(CONNECTION_QUALIFIED_NAME);
                    if (StringUtils.isNotEmpty(connectionQN)) {
                        connections.add(connectionQN);
                    }
                } catch (AtlasBaseException e) {
                    LOG.warn("Failed to retrieve connection for asset {}: {}", assetId.getGuid(), e.getMessage());
                }
            }
        }
    }

    private void deleteConnectionProcess(String connectionProcessQn, Set<String> affectedConnections) throws AtlasBaseException {
        AtlasObjectId atlasObjectId = new AtlasObjectId();
        atlasObjectId.setTypeName(CONNECTION_PROCESS_ENTITY_TYPE);
        atlasObjectId.setUniqueAttributes(AtlasEntityUtils.mapOf(QUALIFIED_NAME, connectionProcessQn));

        try {
            AtlasVertex connectionProcessVertex = entityRetriever.getEntityVertex(atlasObjectId);
            AtlasEntity connectionProcess = entityRetriever.toAtlasEntity(connectionProcessVertex);
            
            // Add connection QNs to affected connections
            addConnectionToSet(connectionProcess.getRelationshipAttribute("input"), affectedConnections);
            addConnectionToSet(connectionProcess.getRelationshipAttribute("output"), affectedConnections);
            
            entityStore.deleteById(connectionProcessVertex.getProperty("__guid", String.class));
        } catch (AtlasBaseException e) {
            if (!e.getAtlasErrorCode().equals(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)) {
                throw e;
            }
        }
    }

    private void addConnectionToSet(Object connectionAttr, Set<String> connections) {
        if (connectionAttr instanceof AtlasObjectId) {
            AtlasObjectId connObjectId = (AtlasObjectId) connectionAttr;
            Map<String, Object> uniqueAttributes = connObjectId.getUniqueAttributes();
            if (uniqueAttributes != null) {
                String qn = (String) uniqueAttributes.get(QUALIFIED_NAME);
                if (StringUtils.isNotEmpty(qn)) {
                    connections.add(qn);
                }
            }
        }
    }

    private List<String> getConnectionQualifiedNames(AtlasEntity connectionProcess, String attributeName) {
        List<String> connectionQualifiedNames = new ArrayList<>();
        try {
            Object relationshipAttr = connectionProcess.getRelationshipAttribute(attributeName);
            if (relationshipAttr instanceof List) {
                List<AtlasObjectId> connObjectIds = (List<AtlasObjectId>) relationshipAttr;
                for (AtlasObjectId connObjectId : connObjectIds) {
                    Map<String, Object> uniqueAttributes = connObjectId.getUniqueAttributes();
                    if (uniqueAttributes != null) {
                        String qualifiedName = (String) uniqueAttributes.get(QUALIFIED_NAME);
                        if (StringUtils.isNotEmpty(qualifiedName)) {
                            connectionQualifiedNames.add(qualifiedName);
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Error getting {} qualified name for connection process {}: {}",
                    attributeName, connectionProcess.getGuid(), e.getMessage());
        }
        return connectionQualifiedNames;
    }

    private void deleteConnectionProcess(String connectionProcessQn) throws AtlasBaseException {
        AtlasObjectId atlasObjectId = new AtlasObjectId();
        atlasObjectId.setTypeName(CONNECTION_PROCESS_ENTITY_TYPE);
        atlasObjectId.setUniqueAttributes(AtlasEntityUtils.mapOf(QUALIFIED_NAME, connectionProcessQn));

        try {
            AtlasVertex connectionProcessVertex = entityRetriever.getEntityVertex(atlasObjectId);
            entityStore.deleteById(connectionProcessVertex.getProperty("__guid", String.class));
        } catch (AtlasBaseException exp) {
            if (!exp.getAtlasErrorCode().equals(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)) {
                throw exp;
            }
        }
    }

    private void updateConnectionsHasLineageForConnectionProcess(String connectionProcessQn) throws AtlasBaseException {
        // Get the ConnectionProcess entity
        AtlasObjectId atlasObjectId = new AtlasObjectId();
        atlasObjectId.setTypeName(CONNECTION_PROCESS_ENTITY_TYPE);
        atlasObjectId.setUniqueAttributes(AtlasEntityUtils.mapOf(QUALIFIED_NAME, connectionProcessQn));

        try {
            AtlasVertex connectionProcessVertex = entityRetriever.getEntityVertex(atlasObjectId);
            AtlasEntity connectionProcess = entityRetriever.toAtlasEntity(connectionProcessVertex);

            // Get input and output connections
            List<String> inputConnQNs = getConnectionQualifiedNames(connectionProcess, "inputs");
            List<String> outputConnQNs = getConnectionQualifiedNames(connectionProcess, "outputs");

            // For each connection, check and update __hasLineage
            Set<String> connectionsToUpdate = new HashSet<>();
            connectionsToUpdate.addAll(inputConnQNs);
            connectionsToUpdate.addAll(outputConnQNs);

            for (String connectionQN : connectionsToUpdate) {
                checkAndUpdateConnectionLineage(connectionQN);
            }
        } catch (AtlasBaseException exp) {
            if (!exp.getAtlasErrorCode().equals(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)) {
                throw exp;
            }
        }
    }
}
