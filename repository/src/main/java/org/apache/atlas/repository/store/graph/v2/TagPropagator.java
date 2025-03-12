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
package org.apache.atlas.repository.store.graph.v2;


import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graph.IFullTextMapper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.repository.store.graph.v1.RestoreHandlerV1;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.*;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfMetrics.MetricRecorder;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasConfiguration.LABEL_MAX_LENGTH;
import static org.apache.atlas.AtlasConfiguration.STORE_DIFFERENTIAL_AUDITS;
import static org.apache.atlas.model.TypeCategory.ARRAY;
import static org.apache.atlas.model.TypeCategory.CLASSIFICATION;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.model.instance.AtlasRelatedObjectId.KEY_RELATIONSHIP_ATTRIBUTES;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.*;
import static org.apache.atlas.model.tasks.AtlasTask.Status.IN_PROGRESS;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality.SET;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.CREATED_BY_KEY;
import static org.apache.atlas.repository.Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.GUID_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.IS_INCOMPLETE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.LABELS_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.MODIFIED_BY_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TYPE_NAME_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.getTypeName;
import static org.apache.atlas.repository.graph.GraphHelper.*;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.getIdFromVertex;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.*;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.INPUT_PORT_GUIDS_ATTR;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.OUTPUT_PORT_GUIDS_ATTR;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.*;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.IN;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.OUT;
import static org.apache.atlas.type.Constants.*;

@Component
public class TagPropagator {
    private static final Logger LOG      = LoggerFactory.getLogger(TagPropagator.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("entityGraphMapper");

    private static final String  SOFT_REF_FORMAT                   = "%s:%s";
    private static final int     INDEXED_STR_SAFE_LEN              = AtlasConfiguration.GRAPHSTORE_INDEXED_STRING_SAFE_LENGTH.getInt();
    private static final boolean WARN_ON_NO_RELATIONSHIP           = AtlasConfiguration.RELATIONSHIP_WARN_NO_RELATIONSHIPS.getBoolean();
    private static final String  CUSTOM_ATTRIBUTE_KEY_SPECIAL_PREFIX = AtlasConfiguration.CUSTOM_ATTRIBUTE_KEY_SPECIAL_PREFIX.getString();

    private static final String  CLASSIFICATION_NAME_DELIMITER     = "|";
    private static final Pattern CUSTOM_ATTRIBUTE_KEY_REGEX        = Pattern.compile("^[a-zA-Z0-9_-]*$");
    private static final Pattern LABEL_REGEX                       = Pattern.compile("^[a-zA-Z0-9_-]*$");
    private static final int     CUSTOM_ATTRIBUTE_KEY_MAX_LENGTH   = AtlasConfiguration.CUSTOM_ATTRIBUTE_KEY_MAX_LENGTH.getInt();
    private static final int     CUSTOM_ATTRIBUTE_VALUE_MAX_LENGTH = AtlasConfiguration.CUSTOM_ATTRIBUTE_VALUE_MAX_LENGTH.getInt();

    private static final String TYPE_GLOSSARY= "AtlasGlossary";
    private static final String TYPE_CATEGORY= "AtlasGlossaryCategory";
    private static final String TYPE_TERM = "AtlasGlossaryTerm";
    private static final String TYPE_PRODUCT = "DataProduct";
    private static final String TYPE_DOMAIN = "DataDomain";
    private static final String TYPE_PROCESS = "Process";
    private static final String ATTR_MEANINGS = "meanings";
    private static final String ATTR_ANCHOR = "anchor";
    private static final String ATTR_CATEGORIES = "categories";
    private static final List<String> ALLOWED_DATATYPES_FOR_DEFAULT_NULL = new ArrayList() {
        {
            add("int");
            add("long");
            add("float");
        }
    };

    private static final boolean ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES = AtlasConfiguration.ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES.getBoolean();
    private static final boolean CLASSIFICATION_PROPAGATION_DEFAULT                  = AtlasConfiguration.CLASSIFICATION_PROPAGATION_DEFAULT.getBoolean();
    private static final boolean RESTRICT_PROPAGATION_THROUGH_LINEAGE_DEFAULT        = false;

    private static final boolean RESTRICT_PROPAGATION_THROUGH_HIERARCHY_DEFAULT        = false;
    public static final int CLEANUP_BATCH_SIZE = 200000;
    private              boolean DEFERRED_ACTION_ENABLED                             = AtlasConfiguration.TASKS_USE_ENABLED.getBoolean();
    private              boolean DIFFERENTIAL_AUDITS                                 = STORE_DIFFERENTIAL_AUDITS.getBoolean();

    private static final int MAX_NUMBER_OF_RETRIES = AtlasConfiguration.MAX_NUMBER_OF_RETRIES.getInt();
    private static final int CHUNK_SIZE            = AtlasConfiguration.TASKS_GRAPH_COMMIT_CHUNK_SIZE.getInt();
    private static final int UD_REL_THRESHOLD = AtlasConfiguration.ATLAS_UD_RELATIONSHIPS_MAX_COUNT.getInt();

    private final GraphHelper               graphHelper;
    private final AtlasGraph                graph;
    private final DeleteHandlerDelegate     deleteDelegate;
    private final RestoreHandlerV1          restoreHandlerV1;
    private final AtlasTypeRegistry         typeRegistry;
    private final AtlasRelationshipStore    relationshipStore;
    private final IAtlasEntityChangeNotifier entityChangeNotifier;
    private final AtlasInstanceConverter    instanceConverter;
    private final EntityGraphRetriever      entityRetriever;
    private final IFullTextMapper           fullTextMapperV2;
    private final TaskManagement            taskManagement;
    private final TransactionInterceptHelper   transactionInterceptHelper;
    private final EntityGraphRetriever       retrieverNoRelation;

    private static final Set<String> excludedTypes = new HashSet<>(Arrays.asList(TYPE_GLOSSARY, TYPE_CATEGORY, TYPE_TERM, TYPE_PRODUCT, TYPE_DOMAIN));

    private final KafkaNotification kafkaNotification;

    @Inject
    public TagPropagator(DeleteHandlerDelegate deleteDelegate, RestoreHandlerV1 restoreHandlerV1, AtlasTypeRegistry typeRegistry, AtlasGraph graph,
                         AtlasRelationshipStore relationshipStore, IAtlasEntityChangeNotifier entityChangeNotifier,
                         AtlasInstanceConverter instanceConverter, IFullTextMapper fullTextMapperV2,
                         TaskManagement taskManagement, TransactionInterceptHelper transactionInterceptHelper, KafkaNotification kafkaNotification) {
        this.restoreHandlerV1 = restoreHandlerV1;
        this.graphHelper          = new GraphHelper(graph);
        this.deleteDelegate       = deleteDelegate;
        this.typeRegistry         = typeRegistry;
        this.graph                = graph;
        this.relationshipStore    = relationshipStore;
        this.entityChangeNotifier = entityChangeNotifier;
        this.instanceConverter    = instanceConverter;
        this.entityRetriever      = new EntityGraphRetriever(graph, typeRegistry);
        this.retrieverNoRelation  = new EntityGraphRetriever(graph, typeRegistry, true);
        this.fullTextMapperV2     = fullTextMapperV2;
        this.taskManagement       = taskManagement;
        this.transactionInterceptHelper = transactionInterceptHelper;
        this.kafkaNotification = kafkaNotification;
    }

    public void cleanUpClassificationPropagation(String classificationName, int batchLimit) {
        int CLEANUP_MAX = batchLimit <= 0 ? CLEANUP_BATCH_SIZE : batchLimit * CLEANUP_BATCH_SIZE;
        int cleanedUpCount = 0;
        long classificationEdgeCount = 0;
        long classificationEdgeInMemoryCount = 0;
        Iterator<AtlasVertex> tagVertices = GraphHelper.getClassificationVertices(graph, classificationName, CLEANUP_BATCH_SIZE);

        List<AtlasVertex> tagVerticesProcessed = new ArrayList<>(0);
        List<AtlasVertex> currentAssetVerticesBatch = new ArrayList<>(0);
        int totalCount = 0;
        while (tagVertices != null && tagVertices.hasNext()) {
            if (cleanedUpCount >= CLEANUP_MAX){
                return;
            }

            while (tagVertices.hasNext() && currentAssetVerticesBatch.size() < CLEANUP_BATCH_SIZE) {
                AtlasVertex tagVertex = tagVertices.next();

                int availableSlots = CLEANUP_BATCH_SIZE - currentAssetVerticesBatch.size();
                long assetCountForCurrentTagVertex = GraphHelper.getAssetsCountOfClassificationVertex(tagVertex);
                currentAssetVerticesBatch.addAll(GraphHelper.getAllAssetsWithClassificationVertex(tagVertex, availableSlots));
                LOG.info("Available slots : {}, assetCountForCurrentTagVertex : {}, queueSize : {}",availableSlots, assetCountForCurrentTagVertex, currentAssetVerticesBatch.size());
                if (assetCountForCurrentTagVertex <= availableSlots) {
                    tagVerticesProcessed.add(tagVertex);
                }
            }

            int currentAssetsBatchSize = currentAssetVerticesBatch.size();
            totalCount += currentAssetsBatchSize;

            if (currentAssetsBatchSize > 0) {
                LOG.info("To clean up tag {} from {} entities", classificationName, currentAssetsBatchSize);
                int offset = 0;
                do {
                    try {
                        int toIndex = Math.min((offset + CHUNK_SIZE), currentAssetsBatchSize);
                        List<AtlasVertex> entityVertices = currentAssetVerticesBatch.subList(offset, toIndex);
                        for (AtlasVertex vertex : entityVertices) {
                            List<String> kafkaMessage = kafkaNotification.createObjectPropKafkaMessage(vertex, graph, CLEANUP_CLASSIFICATION_PROPAGATION, vertex.getIdForDisplay());
                            kafkaNotification.sendInternal(NotificationInterface.NotificationType.EMIT_SUB_TASKS, kafkaMessage);
                            LOG.debug("OBJECT_PROP_EVENTS => {}", kafkaMessage);
                            List<AtlasClassification> deletedClassifications = new ArrayList<>();
                            GraphTransactionInterceptor.lockObjectAndReleasePostCommit(graphHelper.getGuid(vertex));
                            List<AtlasEdge> classificationEdges = GraphHelper.getClassificationEdges(vertex, null, classificationName);
                            classificationEdgeCount += classificationEdges.size();
                            int batchSize = CHUNK_SIZE;
                            for (int i = 0; i < classificationEdges.size(); i += batchSize) {
                                int end = Math.min(i + batchSize, classificationEdges.size());
                                List<AtlasEdge> batch = classificationEdges.subList(i, end);
                                for (AtlasEdge edge : batch) {
                                    try {
                                        AtlasClassification classification = entityRetriever.toAtlasClassification(edge.getInVertex());
                                        deletedClassifications.add(classification);
                                        deleteDelegate.getHandler().deleteEdgeReference(edge, TypeCategory.CLASSIFICATION, false, true, null, vertex);
                                        classificationEdgeInMemoryCount++;
                                    } catch (IllegalStateException | AtlasBaseException e) {
                                        e.printStackTrace();
                                    }
                                }
                                if(classificationEdgeInMemoryCount >= CHUNK_SIZE){
                                    transactionInterceptHelper.intercept();
                                    classificationEdgeInMemoryCount = 0;
                                }
                            }

                            try {
                                AtlasEntity entity = repairClassificationMappings(vertex);
                                entityChangeNotifier.onClassificationDeletedFromEntity(entity, deletedClassifications);
                            } catch (IllegalStateException | AtlasBaseException e) {
                                e.printStackTrace();
                            }

                        }

                        transactionInterceptHelper.intercept();
                        offset += CHUNK_SIZE;


                    } catch (NotificationException e) {
                        throw new RuntimeException(e);
                    } finally {
                        LOG.info("For offset {} , classificationEdge were : {}", offset, classificationEdgeCount);
                        classificationEdgeCount = 0;
                        LOG.info("Cleaned up {} entities for classification {}", offset, classificationName);
                    }

                } while (offset < currentAssetsBatchSize);

                for (AtlasVertex classificationVertex : tagVerticesProcessed) {
                    try {
                        deleteDelegate.getHandler().deleteClassificationVertex(classificationVertex, true);
                    } catch (IllegalStateException e) {
                        e.printStackTrace();
                    }
                }

                cleanedUpCount += currentAssetsBatchSize;
                currentAssetVerticesBatch.clear();
                tagVerticesProcessed.clear();
            }
            tagVertices = GraphHelper.getClassificationVertices(graph, classificationName, CLEANUP_BATCH_SIZE);
        }

        taskManagement.updateTaskVertexProperty(TASK_ASSET_COUNT_TO_PROPAGATE, graph, totalCount);
        taskManagement.updateTaskVertexProperty(TASK_ASSET_COUNT_PROPAGATED, graph, totalCount);

        transactionInterceptHelper.intercept();
        LOG.info("Completed cleaning up classification {}", classificationName);
    }

    public AtlasEntity repairClassificationMappings(AtlasVertex entityVertex) throws AtlasBaseException {
        String guid = GraphHelper.getGuid(entityVertex);
        AtlasEntity entity = instanceConverter.getEntity(guid, ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES);

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE_CLASSIFICATION, new AtlasEntityHeader(entity)), "repair classification mappings: guid=", guid);
        List<String> classificationNames = new ArrayList<>();
        List<String> propagatedClassificationNames = new ArrayList<>();

        if (entity.getClassifications() != null) {
            List<AtlasClassification> classifications = entity.getClassifications();
            for (AtlasClassification classification : classifications) {
                if (isPropagatedClassification(classification, guid)) {
                    propagatedClassificationNames.add(classification.getTypeName());
                } else {
                    classificationNames.add(classification.getTypeName());
                }
            }
        }
        //Delete array/set properties first
        entityVertex.removeProperty(TRAIT_NAMES_PROPERTY_KEY);
        entityVertex.removeProperty(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY);


        //Update classificationNames and propagatedClassificationNames in entityVertex
        entityVertex.setProperty(CLASSIFICATION_NAMES_KEY, getDelimitedClassificationNames(classificationNames));
        entityVertex.setProperty(PROPAGATED_CLASSIFICATION_NAMES_KEY, getDelimitedClassificationNames(propagatedClassificationNames));
        entityVertex.setProperty(CLASSIFICATION_TEXT_KEY, fullTextMapperV2.getClassificationTextForEntity(entity));
        // Make classificationNames unique list as it is of type SET
        classificationNames = classificationNames.stream().distinct().collect(Collectors.toList());
        //Update classificationNames and propagatedClassificationNames in entityHeader
        for(String classificationName : classificationNames) {
            AtlasGraphUtilsV2.addEncodedProperty(entityVertex, TRAIT_NAMES_PROPERTY_KEY, classificationName);
        }
        for (String classificationName : propagatedClassificationNames) {
            entityVertex.addListProperty(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, classificationName);
        }

        return entity;
    }

    private boolean isPropagatedClassification(AtlasClassification classification, String guid) {
        String classificationEntityGuid = classification.getEntityGuid();
        return StringUtils.isNotEmpty(classificationEntityGuid) && !StringUtils.equals(classificationEntityGuid, guid);
    }

    public void processClassificationPropagationAddition(List<AtlasVertex> verticesToPropagate, AtlasVertex classificationVertex) throws AtlasBaseException{
        MetricRecorder classificationPropagationMetricRecorder = RequestContext.get().startMetricRecord("processClassificationPropagationAddition");
        int impactedVerticesSize = verticesToPropagate.size();
        LOG.info(String.format("Total number of vertices to propagate: %d", impactedVerticesSize));

        try {
            for (AtlasVertex vertex: verticesToPropagate) {
                List<String> kafkaMessage = kafkaNotification.createObjectPropKafkaMessage(vertex, graph, CLASSIFICATION_PROPAGATION_ADD, classificationVertex.getIdForDisplay());
                kafkaNotification.sendInternal(NotificationInterface.NotificationType.EMIT_SUB_TASKS, kafkaMessage);
            }
        } catch (NotificationException e) {
            throw new RuntimeException(e);
        } finally {
            RequestContext.get().endMetricRecord(classificationPropagationMetricRecorder);
        }

    }


    public void updateClassificationTextPropagation(String classificationVertexId) throws AtlasBaseException {
        if (StringUtils.isEmpty(classificationVertexId)) {
            LOG.warn("updateClassificationTextPropagation(classificationVertexId={}): classification vertex id is empty", classificationVertexId);
            return;
        }
        AtlasVertex classificationVertex = graph.getVertex(classificationVertexId);
        AtlasClassification classification = entityRetriever.toAtlasClassification(classificationVertex);
        LOG.info("Fetched classification : {} ", classification.toString());
        List<AtlasVertex> impactedVertices = graphHelper.getAllPropagatedEntityVertices(classificationVertex);

        taskManagement.updateTaskVertexProperty(TASK_ASSET_COUNT_TO_PROPAGATE, graph, impactedVertices.size());

        LOG.info("impactedVertices : {}", impactedVertices.size());
        int batchSize = 100;
        for (int i = 0; i < impactedVertices.size(); i += batchSize) {
            int end = Math.min(i + batchSize, impactedVertices.size());
            List<AtlasVertex> batch = impactedVertices.subList(i, end);
            for (AtlasVertex vertex : batch) {
                List<String> kafkaMessage = kafkaNotification.createObjectPropKafkaMessage(vertex, graph, CLASSIFICATION_PROPAGATION_TEXT_UPDATE, classificationVertexId);
                try {
                    kafkaNotification.sendInternal(NotificationInterface.NotificationType.EMIT_SUB_TASKS, kafkaMessage);
                } catch (NotificationException e) {
                    throw new RuntimeException(e);
                }
                String entityGuid = graphHelper.getGuid(vertex);
                AtlasEntity entity = instanceConverter.getAndCacheEntity(entityGuid, true);

//                if (entity != null) {
//                    vertex.setProperty(CLASSIFICATION_TEXT_KEY, fullTextMapperV2.getClassificationTextForEntity(entity));
//                    entityChangeNotifier.onClassificationUpdatedToEntity(entity, Collections.singletonList(classification));
//                }
            }

//            taskManagement.updateTaskVertexProperty(TASK_ASSET_COUNT_PROPAGATED, graph, end);

            transactionInterceptHelper.intercept();
            LOG.info("Updated classificationText from {} for {}", i, batchSize);
        }
    }

    public void deleteClassificationPropagation(String entityGuid, String classificationVertexId) throws AtlasBaseException {
        try {
            if (StringUtils.isEmpty(classificationVertexId)) {
                LOG.warn("deleteClassificationPropagation(classificationVertexId={}): classification vertex id is empty", classificationVertexId);

                return;
            }

            AtlasVertex classificationVertex = graph.getVertex(classificationVertexId);
            if (classificationVertex == null) {
                LOG.warn("deleteClassificationPropagation(classificationVertexId={}): classification vertex not found", classificationVertexId);

                return;
            }

            AtlasClassification classification = entityRetriever.toAtlasClassification(classificationVertex);

            List<AtlasEdge> propagatedEdges = getPropagatedEdges(classificationVertex);
            if (propagatedEdges.isEmpty()) {
                LOG.warn("deleteClassificationPropagation(classificationVertexId={}): classification edges empty", classificationVertexId);

                return;
            }

            int propagatedEdgesSize = propagatedEdges.size();

            LOG.info(String.format("Number of edges to be deleted : %s for classification vertex with id : %s", propagatedEdgesSize, classificationVertexId));
            for (AtlasEdge edge : propagatedEdges) {
                List<String> kafkaMessage = kafkaNotification.createObjectPropKafkaMessage(edge.getOutVertex(), graph, CLASSIFICATION_PROPAGATION_DELETE, edge.getIdForDisplay());
                try {
                    kafkaNotification.sendInternal(NotificationInterface.NotificationType.EMIT_SUB_TASKS, kafkaMessage);
                } catch (NotificationException e) {
                    throw new RuntimeException(e);
                }
            }
//            List<String> deletedPropagationsGuid = processClassificationEdgeDeletionInChunk(classification, propagatedEdges);

//            deleteDelegate.getHandler().deleteClassificationVertex(classificationVertex, true);

            transactionInterceptHelper.intercept();
            //            return deletedPropagationsGuid;
        } catch (Exception e) {
            LOG.error("Error while removing classification id {} with error {} ", classificationVertexId, e.getMessage());
            throw new AtlasBaseException(e);
        }
    }

    public void deleteClassificationOnlyPropagation(Set<String> deletedEdgeIds) throws AtlasBaseException {
        RequestContext.get().getDeletedEdgesIds().clear();
        RequestContext.get().getDeletedEdgesIds().addAll(deletedEdgeIds);

        for (AtlasEdge edge : deletedEdgeIds.stream().map(x -> graph.getEdge(x)).collect(Collectors.toList())) {

            boolean isRelationshipEdge = deleteDelegate.getHandler().isRelationshipEdge(edge);
            String  relationshipGuid   = GraphHelper.getRelationshipGuid(edge);

            if (edge == null || !isRelationshipEdge) {
                continue;
            }

            List<AtlasVertex> currentClassificationVertices = getPropagatableClassifications(edge);

            for (AtlasVertex currentClassificationVertex : currentClassificationVertices) {
                LOG.info("Starting Classification {} Removal for deletion of edge {}",currentClassificationVertex.getIdForDisplay(), edge.getIdForDisplay());
                boolean isTermEntityEdge = isTermEntityEdge(edge);
                boolean removePropagationOnEntityDelete = getRemovePropagations(currentClassificationVertex);

                if (!(isTermEntityEdge || removePropagationOnEntityDelete)) {
                    LOG.debug("This edge is not term edge or remove propagation isn't enabled");
                    continue;
                }

                processClassificationDeleteOnlyPropagation(currentClassificationVertex, relationshipGuid);
                LOG.info("Finished Classification {} Removal for deletion of edge {}",currentClassificationVertex.getIdForDisplay(), edge.getIdForDisplay());
            }
        }
    }

    public void deleteClassificationOnlyPropagation(String deletedEdgeId, String classificationVertexId) throws AtlasBaseException {
        RequestContext.get().getDeletedEdgesIds().clear();
        RequestContext.get().getDeletedEdgesIds().add(deletedEdgeId);

        AtlasEdge edge = graph.getEdge(deletedEdgeId);

        boolean isRelationshipEdge = deleteDelegate.getHandler().isRelationshipEdge(edge);
        String  relationshipGuid   = GraphHelper.getRelationshipGuid(edge);

        if (edge == null || !isRelationshipEdge) {
            return;
        }

        AtlasVertex currentClassificationVertex = graph.getVertex(classificationVertexId);
        if (currentClassificationVertex == null) {
            LOG.warn("Classification Vertex with ID {} is not present or Deleted", classificationVertexId);
            return;
        }

        List<AtlasVertex> currentClassificationVertices = getPropagatableClassifications(edge);
        if (! currentClassificationVertices.contains(currentClassificationVertex)) {
            return;
        }

        boolean isTermEntityEdge = isTermEntityEdge(edge);
        boolean removePropagationOnEntityDelete = getRemovePropagations(currentClassificationVertex);

        if (!(isTermEntityEdge || removePropagationOnEntityDelete)) {
            LOG.debug("This edge is not term edge or remove propagation isn't enabled");
            return;
        }

        processClassificationDeleteOnlyPropagation(currentClassificationVertex, relationshipGuid);

        LOG.info("Finished Classification {} Removal for deletion of edge {}",currentClassificationVertex.getIdForDisplay(), edge.getIdForDisplay());
    }

    public void deleteClassificationOnlyPropagation(String classificationId, String referenceVertexId, boolean isTermEntityEdge) throws AtlasBaseException {
        AtlasVertex classificationVertex = graph.getVertex(classificationId);
        AtlasVertex referenceVertex = graph.getVertex(referenceVertexId);

        if (classificationVertex == null) {
            LOG.warn("Classification Vertex with ID {} is not present or Deleted", classificationId);
            return;
        }
        /*
            If reference vertex is deleted, we can consider that as this connected vertex was deleted
             some other task was created before it to remove propagations. No need to execute this task.
         */
        if (referenceVertex == null) {
            LOG.warn("Reference Vertex {} is deleted", referenceVertexId);
            return;
        }

        if (!GraphHelper.propagatedClassificationAttachedToVertex(classificationVertex, referenceVertex)) {
            LOG.warn("No Classification is attached to the reference vertex {} for classification {}", referenceVertexId, classificationId);
            return;
        }

        boolean removePropagationOnEntityDelete = getRemovePropagations(classificationVertex);

        if (!(isTermEntityEdge || removePropagationOnEntityDelete)) {
            LOG.debug("This edge is not term edge or remove propagation isn't enabled");
            return;
        }

        processClassificationDeleteOnlyPropagation(classificationVertex, null);

        LOG.info("Completed propagation removal via edge for classification {}", classificationId);
    }

    public void classificationRefreshPropagation(String classificationId) throws AtlasBaseException {
        MetricRecorder classificationRefreshPropagationMetricRecorder = RequestContext.get().startMetricRecord("classificationRefreshPropagation");

        AtlasVertex currentClassificationVertex             = graph.getVertex(classificationId);
        if (currentClassificationVertex == null) {
            LOG.warn("Classification vertex with ID {} is deleted", classificationId);
            return;
        }

        String              sourceEntityId                  = getClassificationEntityGuid(currentClassificationVertex);
        AtlasVertex         sourceEntityVertex              = AtlasGraphUtilsV2.findByGuid(this.graph, sourceEntityId);
        AtlasClassification classification                  = entityRetriever.toAtlasClassification(currentClassificationVertex);

        String propagationMode;

        Boolean restrictPropagationThroughLineage = AtlasGraphUtilsV2.getProperty(currentClassificationVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE, Boolean.class);
        Boolean restrictPropagationThroughHierarchy = AtlasGraphUtilsV2.getProperty(currentClassificationVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY, Boolean.class);

        propagationMode = entityRetriever.determinePropagationMode(restrictPropagationThroughLineage,restrictPropagationThroughHierarchy);
        Boolean toExclude = propagationMode == CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE ? true:false;

        List<String> propagatedVerticesIds = GraphHelper.getPropagatedVerticesIds(currentClassificationVertex);
        LOG.info("{} entity vertices have classification with id {} attached", propagatedVerticesIds.size(), classificationId);

        List<String> verticesIdsToAddClassification =  new ArrayList<>();
        List<String> propagatedVerticesIdWithoutEdge = entityRetriever.getImpactedVerticesIdsClassificationAttached(sourceEntityVertex , classificationId,
                CLASSIFICATION_PROPAGATION_MODE_LABELS_MAP.get(propagationMode),toExclude, verticesIdsToAddClassification);

        LOG.info("To add classification with id {} to {} vertices", classificationId, verticesIdsToAddClassification.size());

        List<String> verticesIdsToRemove = (List<String>)CollectionUtils.subtract(propagatedVerticesIds, propagatedVerticesIdWithoutEdge);

        List<AtlasVertex> verticesToRemove = verticesIdsToRemove.stream()
                .map(x -> graph.getVertex(x))
                .filter(vertex -> vertex != null)
                .collect(Collectors.toList());

        List<AtlasVertex> verticesToAddClassification  = verticesIdsToAddClassification.stream()
                .map(x -> graph.getVertex(x))
                .filter(vertex -> vertex != null)
                .collect(Collectors.toList());

        taskManagement.updateTaskVertexProperty(TASK_ASSET_COUNT_TO_PROPAGATE, graph, verticesToRemove.size() + verticesToAddClassification.size());

        //Remove classifications from unreachable vertices
        processPropagatedClassificationDeletionFromVertices(verticesToRemove, currentClassificationVertex, classification);

        //Add classification to the reachable vertices
        if (CollectionUtils.isEmpty(verticesToAddClassification)) {
            LOG.debug("propagateClassification(entityGuid={}, classificationVertexId={}): found no entities to propagate the classification", sourceEntityId, classificationId);
            return;
        }
        processClassificationPropagationAddition(verticesToAddClassification, currentClassificationVertex);

        LOG.info("Completed refreshing propagation for classification with vertex id {} with classification name {} and source entity {}",classificationId,
                classification.getTypeName(), classification.getEntityGuid());

        RequestContext.get().endMetricRecord(classificationRefreshPropagationMetricRecorder);
    }

    private void processClassificationDeleteOnlyPropagation(AtlasVertex currentClassificationVertex, String relationshipGuid) throws AtlasBaseException {
        String              classificationId                = currentClassificationVertex.getIdForDisplay();
        String              sourceEntityId                  = getClassificationEntityGuid(currentClassificationVertex);
        AtlasVertex         sourceEntityVertex              = AtlasGraphUtilsV2.findByGuid(this.graph, sourceEntityId);
        AtlasClassification classification                  = entityRetriever.toAtlasClassification(currentClassificationVertex);

        String propagationMode;

        Boolean restrictPropagationThroughLineage = AtlasGraphUtilsV2.getProperty(currentClassificationVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE, Boolean.class);
        Boolean restrictPropagationThroughHierarchy = AtlasGraphUtilsV2.getProperty(currentClassificationVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY, Boolean.class);
        propagationMode = entityRetriever.determinePropagationMode(restrictPropagationThroughLineage,restrictPropagationThroughHierarchy);
        Boolean toExclude = propagationMode == CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE ? true : false;
        List<String> propagatedVerticesIds = GraphHelper.getPropagatedVerticesIds(currentClassificationVertex);
        LOG.info("Traversed {} vertices including edge with relationship GUID {} for classification vertex {}", propagatedVerticesIds.size(), relationshipGuid, classificationId);

        List<String> propagatedVerticesIdWithoutEdge = entityRetriever.getImpactedVerticesIds(sourceEntityVertex, relationshipGuid , classificationId,
                CLASSIFICATION_PROPAGATION_MODE_LABELS_MAP.get(propagationMode),toExclude);

        LOG.info("Traversed {} vertices except edge with relationship GUID {} for classification vertex {}", propagatedVerticesIdWithoutEdge.size(), relationshipGuid, classificationId);

        List<String> verticesIdsToRemove = (List<String>)CollectionUtils.subtract(propagatedVerticesIds, propagatedVerticesIdWithoutEdge);

        List<AtlasVertex> verticesToRemove = verticesIdsToRemove.stream()
                .map(x -> graph.getVertex(x))
                .filter(vertex -> vertex != null)
                .collect(Collectors.toList());

        propagatedVerticesIdWithoutEdge.clear();
        propagatedVerticesIds.clear();

        LOG.info("To delete classification from {} vertices for deletion of edge with relationship GUID {} and classification {}", verticesToRemove.size(), relationshipGuid, classificationId);

        processPropagatedClassificationDeletionFromVertices(verticesToRemove, currentClassificationVertex, classification);

        LOG.info("Completed remove propagation for edge with relationship GUID {} and classification vertex {} with classification name {} and source entity {}", relationshipGuid,
                classificationId, classification.getTypeName(), classification.getEntityGuid());
    }

    private void processPropagatedClassificationDeletionFromVertices(List<AtlasVertex> VerticesToRemoveTag, AtlasVertex classificationVertex, AtlasClassification classification) throws AtlasBaseException {
        MetricRecorder propagatedClassificationDeletionMetricRecorder = RequestContext.get().startMetricRecord("processPropagatedClassificationDeletionFromVertices");

        int propagatedVerticesSize = VerticesToRemoveTag.size();
        int toIndex;
        int offset = 0;

        LOG.info("To delete classification of vertex id {} from {} entity vertices", classificationVertex.getIdForDisplay(), propagatedVerticesSize);

        try {
            do {
                toIndex = ((offset + CHUNK_SIZE > propagatedVerticesSize) ? propagatedVerticesSize : (offset + CHUNK_SIZE));
                List<AtlasVertex> verticesChunkToRemoveTag = VerticesToRemoveTag.subList(offset, toIndex);

                for (AtlasVertex vertex: verticesChunkToRemoveTag) {
                    List<String> kafkaMessage = kafkaNotification.createObjectPropKafkaMessage(vertex, graph, CLASSIFICATION_PROPAGATION_DELETE, classificationVertex.getIdForDisplay());
                    kafkaNotification.sendInternal(NotificationInterface.NotificationType.EMIT_SUB_TASKS, kafkaMessage);
                }
                List<String> impactedGuids = verticesChunkToRemoveTag.stream()
                        .map(entityVertex -> GraphHelper.getGuid(entityVertex))
                        .collect(Collectors.toList());
                GraphTransactionInterceptor.lockObjectAndReleasePostCommit(impactedGuids);

//                List<AtlasVertex> updatedVertices = deleteDelegate.getHandler().removeTagPropagation(classificationVertex, verticesChunkToRemoveTag);
//                List<AtlasEntity> updatedEntities = updateClassificationText(classification, updatedVertices);
//                entityChangeNotifier.onClassificationsDeletedFromEntities(updatedEntities, Collections.singletonList(classification));

                int propagatedAssetsCount = toIndex - offset;
                offset += CHUNK_SIZE;
                taskManagement.updateTaskVertexProperty(TASK_ASSET_COUNT_PROPAGATED, graph, propagatedAssetsCount);
                transactionInterceptHelper.intercept();

            } while (offset < propagatedVerticesSize);
        } catch (NotificationException e) {
            throw new RuntimeException(e);
        } finally {
            RequestContext.get().endMetricRecord(propagatedClassificationDeletionMetricRecorder);
        }
    }

    List<String> processClassificationEdgeDeletionInChunk(AtlasClassification classification, List<AtlasEdge> propagatedEdges) throws AtlasBaseException {
        List<String> deletedPropagationsGuid = new ArrayList<>();
        int propagatedEdgesSize = propagatedEdges.size();
        int toIndex;
        int offset = 0;

        taskManagement.updateTaskVertexProperty(TASK_ASSET_COUNT_TO_PROPAGATE, graph, propagatedEdgesSize);

        do {
            toIndex = ((offset + CHUNK_SIZE > propagatedEdgesSize) ? propagatedEdgesSize : (offset + CHUNK_SIZE));
            List<AtlasEdge> chunk = propagatedEdges.subList(offset, toIndex);
            for (AtlasEdge edge : chunk) {
                List<String> kafkaMessage = kafkaNotification.createObjectPropKafkaMessage(edge.getOutVertex(), graph, CLASSIFICATION_PROPAGATION_DELETE, edge.getIdForDisplay());
                try {
                    kafkaNotification.sendInternal(NotificationInterface.NotificationType.EMIT_SUB_TASKS, kafkaMessage);
                } catch (NotificationException e) {
                    throw new RuntimeException(e);
                }
            }
            List<AtlasVertex> entityVertices = deleteDelegate.getHandler().removeTagPropagation(classification, chunk);
            List<String> impactedGuids = entityVertices.stream().map(x -> GraphHelper.getGuid(x)).collect(Collectors.toList());

            GraphTransactionInterceptor.lockObjectAndReleasePostCommit(impactedGuids);

            List<AtlasEntity>  propagatedEntities = updateClassificationText(classification, entityVertices);

            entityChangeNotifier.onClassificationsDeletedFromEntities(propagatedEntities, Collections.singletonList(classification));
            if(! propagatedEntities.isEmpty()) {
                deletedPropagationsGuid.addAll(propagatedEntities.stream().map(x -> x.getGuid()).collect(Collectors.toList()));
            }

            int propagatedAssetsCount = toIndex - offset;

            offset += CHUNK_SIZE;

            taskManagement.updateTaskVertexProperty(TASK_ASSET_COUNT_PROPAGATED, graph, propagatedAssetsCount);
            transactionInterceptHelper.intercept();

        } while (offset < propagatedEdgesSize);

        return deletedPropagationsGuid;
    }

    List<AtlasEntity> updateClassificationText(AtlasClassification classification, Collection<AtlasVertex> propagatedVertices) throws AtlasBaseException {
        List<AtlasEntity> propagatedEntities = new ArrayList<>();
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updateClassificationText");

        if(CollectionUtils.isNotEmpty(propagatedVertices)) {
            for(AtlasVertex vertex : propagatedVertices) {
                AtlasEntity entity = null;
                for (int i = 1; i <= MAX_NUMBER_OF_RETRIES; i++) {
                    try {
                        entity = instanceConverter.getAndCacheEntity(graphHelper.getGuid(vertex), ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES);
                        break; //do not retry on success
                    } catch (AtlasBaseException ex) {
                        if (i == MAX_NUMBER_OF_RETRIES) {
                            LOG.error(String.format("Maximum retries reached for fetching vertex with id %s from graph. Retried %s times. Skipping...", vertex.getId(), i));
                            continue;
                        }
                        LOG.warn(String.format("Vertex with id %s could not be fetched from graph. Retrying for %s time", vertex.getId(), i));
                    }
                }

                if (entity != null) {
                    String classificationTextForEntity = fullTextMapperV2.getClassificationTextForEntity(entity);
                    vertex.setProperty(CLASSIFICATION_TEXT_KEY, classificationTextForEntity);
                    propagatedEntities.add(entity);
                }
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
        return propagatedEntities;
    }

}
