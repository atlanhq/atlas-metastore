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
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.glossary.enums.AtlasTermAssignmentStatus;
import org.apache.atlas.model.glossary.relations.AtlasTermAssignmentHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.repository.EdgeVertexReference;
import org.apache.atlas.repository.VertexEdgePropertiesCache;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusVertex;
import org.apache.atlas.repository.graphdb.janus.cassandra.DynamicVertex;
import org.apache.atlas.repository.graphdb.janus.cassandra.DynamicVertexService;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasConfiguration.MAX_EDGES_SUPER_VERTEX;
import static org.apache.atlas.AtlasConfiguration.MIN_EDGES_SUPER_VERTEX;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_CONFIDENCE;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_CREATED_BY;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_DESCRIPTION;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_EXPRESSION;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_SOURCE;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_STATUS;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_STEWARD;
import static org.apache.atlas.model.instance.AtlasRelationship.Status.ACTIVE;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_BIGDECIMAL;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_BIGINTEGER;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_BOOLEAN;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_BYTE;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_DATE;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_DOUBLE;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_FLOAT;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_INT;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_LONG;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_SHORT;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_STRING;
import static org.apache.atlas.repository.Constants.ENTITY_TYPE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.GUID_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.LEAN_GRAPH_ENABLED;
import static org.apache.atlas.repository.Constants.RELATIONSHIP_GUID_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TERM_ASSIGNMENT_LABEL;
import static org.apache.atlas.repository.Constants.TYPE_NAME_PROPERTY_KEY;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.isReference;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.IN;

final class EntityGraphBulkFetchHelper {
    private static final Logger LOG = LoggerFactory.getLogger(EntityGraphBulkFetchHelper.class);
    private static final String GLOSSARY_TERM_DISPLAY_NAME_ATTR = "name";

    interface CachedAttributeMappingContext {
        AtlasStruct mapStringToStruct(AtlasType atlasType, String value);

        AtlasObjectId mapVertexToObjectId(String sourceVertexId,
                                          Pair<String, EdgeVertexReference.EdgeInfo> referencedElementPair,
                                          AtlasRelationshipEdgeDirection edgeDirection,
                                          VertexEdgePropertiesCache cache) throws AtlasBaseException;

        boolean isInactiveEdge(Object element, boolean ignoreInactive);
    }

    private final AtlasGraph graph;
    private final AtlasTypeRegistry typeRegistry;
    private final DynamicVertexService dynamicVertexService;
    private final Map<String, Map<String, AtlasType>> vertexPropertyTypesByTypeName = new ConcurrentHashMap<>();

    EntityGraphBulkFetchHelper(AtlasGraph graph, AtlasTypeRegistry typeRegistry) {
        this.graph = graph;
        this.typeRegistry = typeRegistry;
        this.dynamicVertexService = graph instanceof AtlasJanusGraph ? ((AtlasJanusGraph) graph).getDynamicVertexRetrievalService() : null;
    }

    public VertexEdgePropertiesCache enrichVertexPropertiesByVertexIds(Set<String> vertexIds, Set<String> attributes) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("enrichVertexPropertiesByVertexIds");
        try {
            RequestContext context = RequestContext.get();
            int relationAttrsSize = MAX_EDGES_SUPER_VERTEX.getInt();
            if (context.isInvokedByIndexSearch() && context.isInvokedByProduct()) {
                relationAttrsSize = MIN_EDGES_SUPER_VERTEX.getInt();
            }

            VertexEdgePropertiesCache vertexEdgePropertyCache = new VertexEdgePropertiesCache();
            if (CollectionUtils.isEmpty(vertexIds)) {
                return null;
            }

            Pair<Map<String, Map<String, List<?>>>, Map<String, AtlasVertex>> vertexCache = getVertexPropertiesValueMap(vertexIds, 100);
            for (Map.Entry<String, Map<String, List<?>>> entry : vertexCache.getValue0().entrySet()) {
                String vertexId = entry.getKey();
                Map<String, List<?>> properties = normalizeVertexPropertyTypes(entry.getValue());

                if (MapUtils.isNotEmpty(properties)) {
                    vertexEdgePropertyCache.addVertexProperties(vertexId, properties);
                }
            }
            vertexEdgePropertyCache.addVertices(vertexCache.getValue1());

            Set<String> edgeLabelsToProcess = collectEdgeLabelsToProcess(vertexEdgePropertyCache, vertexIds, attributes);
            Set<String> vertexIdsToProcess = new HashSet<>();

            if (!CollectionUtils.isEmpty(edgeLabelsToProcess)) {
                List<Map<String, Object>> relationEdges;
                if (AtlasConfiguration.ATLAS_INDEXSEARCH_EDGE_BULK_FETCH_ENABLE.getBoolean()) {
                    relationEdges = getConnectedRelationEdgesVertexBatching(vertexIds, edgeLabelsToProcess, relationAttrsSize);
                } else {
                    relationEdges = getConnectedRelationEdges(vertexIds, edgeLabelsToProcess, relationAttrsSize);
                }

                for (String vertexId : vertexIds) {
                    for (Map<String, Object> relationEdge : relationEdges) {
                        if (!(relationEdge.containsKey("id") && relationEdge.containsKey("valueMap"))) {
                            continue;
                        }

                        @SuppressWarnings("unchecked")
                        LinkedHashMap<Object, Object> valueMap = (LinkedHashMap<Object, Object>) relationEdge.get("valueMap");
                        String edgeId = relationEdge.get("id").toString();
                        String edgeLabel = relationEdge.get("label").toString();
                        String outVertexId = relationEdge.get("outVertexId").toString();
                        String inVertexId = relationEdge.get("inVertexId").toString();

                        if (!edgeLabelsToProcess.contains(edgeLabel)) {
                            continue;
                        }

                        boolean isSelfLoop = vertexId.equals(outVertexId) && vertexId.equals(inVertexId);
                        boolean isSourceVertex = vertexId.equals(outVertexId);
                        boolean isTargetVertex = vertexId.equals(inVertexId);

                        if (!isSelfLoop && !isSourceVertex && !isTargetVertex) {
                            continue;
                        }

                        String referencedVertex;
                        if (isSelfLoop) {
                            referencedVertex = outVertexId;
                        } else if (isSourceVertex) {
                            referencedVertex = inVertexId;
                        } else {
                            referencedVertex = outVertexId;
                        }

                        EdgeVertexReference edgeRef = new EdgeVertexReference(
                                referencedVertex, edgeId, edgeLabel, inVertexId, outVertexId, valueMap
                        );

                        boolean wasAdded = vertexEdgePropertyCache.addEdgeLabelToVertexIds(
                                vertexId, edgeLabel, edgeRef, relationAttrsSize
                        );

                        if (wasAdded && !isSelfLoop) {
                            vertexIdsToProcess.add(referencedVertex);
                        }
                    }
                }
            }

            Pair<Map<String, Map<String, List<?>>>, Map<String, AtlasVertex>> referenceVertices =
                    getVertexPropertiesValueMap(vertexIdsToProcess, 1000);
            for (Map.Entry<String, Map<String, List<?>>> entry : referenceVertices.getValue0().entrySet()) {
                String vertexId = entry.getKey();
                Map<String, List<?>> properties = normalizeVertexPropertyTypes(entry.getValue());

                if (MapUtils.isNotEmpty(properties)) {
                    vertexEdgePropertyCache.addVertexProperties(vertexId, properties);
                }
            }
            vertexEdgePropertyCache.addVertices(referenceVertices.getValue1());

            return vertexEdgePropertyCache;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public List<Map<String, Object>> getConnectedRelationEdges(Set<String> vertexIds, Set<String> edgeLabels, int relationAttrsSize) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getConnectedRelationEdges");
        try {
            if (CollectionUtils.isEmpty(vertexIds)) {
                return Collections.emptyList();
            }

            GraphTraversal<Edge, Map<String, Object>> edgeTraversal = ((AtlasJanusGraph) graph).V(vertexIds).bothE();

            if (!CollectionUtils.isEmpty(edgeLabels)) {
                edgeTraversal = edgeTraversal.hasLabel(P.within(edgeLabels));
            }

            edgeTraversal = edgeTraversal
                    .has(STATE_PROPERTY_KEY, ACTIVE.name())
                    .has(RELATIONSHIP_GUID_PROPERTY_KEY)
                    .project("id", "valueMap", "label", "inVertexId", "outVertexId")
                    .by(__.id())
                    .by(__.valueMap(true))
                    .by(__.label())
                    .by(__.inV().id())
                    .by(__.outV().id());

            return edgeTraversal.toList();
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public List<Map<String, Object>> getConnectedRelationEdgesVertexBatching(Set<String> vertexIds, Set<String> edgeLabels, int relationAttrsSize) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getConnectedRelationEdgesVertexBatching");
        try {
            if (CollectionUtils.isEmpty(vertexIds)) {
                return Collections.emptyList();
            }

            List<Map<String, Object>> allResults = new ArrayList<>();
            List<String> vertexIdList = new ArrayList<>(vertexIds);
            int vertexBatchSize = AtlasConfiguration.ATLAS_INDEXSEARCH_EDGE_BULK_FETCH_BATCH_SIZE.getInt();

            for (int i = 0; i < vertexIdList.size(); i += vertexBatchSize) {
                int end = Math.min(i + vertexBatchSize, vertexIdList.size());
                List<String> vertexBatch = vertexIdList.subList(i, end);

                GraphTraversal<Edge, Map<String, Object>> edgeTraversal = ((AtlasJanusGraph) graph).V(vertexBatch).bothE();
                if (!CollectionUtils.isEmpty(edgeLabels)) {
                    edgeTraversal = edgeTraversal.hasLabel(P.within(edgeLabels));
                }

                List<Map<String, Object>> batchResults = edgeTraversal
                        .has(STATE_PROPERTY_KEY, ACTIVE.name())
                        .has(RELATIONSHIP_GUID_PROPERTY_KEY)
                        .dedup()
                        .project("id", "valueMap", "label", "inVertexId", "outVertexId")
                        .by(__.id())
                        .by(__.valueMap(true))
                        .by(__.label())
                        .by(__.inV().id())
                        .by(__.outV().id())
                        .toList();

                allResults.addAll(batchResults);
                LOG.debug("Processed vertex batch {}-{} of {}, found {} edges", i, end, vertexIdList.size(), batchResults.size());
            }

            return allResults;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    Object getVertexAttributeFromCache(String vertexId, AtlasAttribute attribute,
                                       final boolean includeReferences, boolean ignoreInactive,
                                       VertexEdgePropertiesCache vertexEdgePropertiesCache,
                                       CachedAttributeMappingContext context) throws AtlasBaseException {
        if (StringUtils.isEmpty(vertexId) || attribute == null || vertexEdgePropertiesCache == null || context == null) {
            return null;
        }

        Object ret = null;
        AtlasType attrType = attribute.getAttributeType();
        String edgeLabel = attribute.getRelationshipEdgeLabel();
        AtlasRelationshipEdgeDirection edgeDirection = attribute.getRelationshipEdgeDirection();

        switch (attrType.getTypeCategory()) {
            case PRIMITIVE:
                ret = EntityGraphRetriever.mapVertexToPrimitiveV2(vertexId, attribute.getVertexPropertyName(), attribute.getAttributeDef(), vertexEdgePropertiesCache);
                break;
            case ENUM:
                ret = vertexEdgePropertiesCache.getPropertyValueWithFallback(vertexId, attribute.getVertexPropertyName(), Object.class);
                break;
            case STRUCT:
                Object val = vertexEdgePropertiesCache.getPropertyValueWithFallback(vertexId, attribute.getVertexPropertyName(), Object.class);
                if (val instanceof String) {
                    ret = context.mapStringToStruct(attrType, (String) val);
                } else {
                    ret = val;
                }
                break;
            case OBJECT_ID_TYPE:
                if (includeReferences) {
                    if (TypeCategory.STRUCT == attribute.getDefinedInType().getTypeCategory()) {
                        edgeLabel = AtlasGraphUtilsV2.getEdgeLabel(attribute.getName());
                    }
                    ret = attribute.getAttributeDef().isSoftReferenced()
                            ? null
                            : context.mapVertexToObjectId(vertexId,
                            vertexEdgePropertiesCache.getRelationShipElement(vertexId, edgeLabel, edgeDirection),
                            edgeDirection, vertexEdgePropertiesCache);
                }
                break;
            case ARRAY: {
                AtlasArrayType arrayType = (AtlasArrayType) attrType;
                AtlasType arrayElementType = arrayType.getElementType();
                List<Object> arrayElements = getArrayElementsFromCache(arrayElementType, vertexId, attribute, vertexEdgePropertiesCache);

                if (CollectionUtils.isEmpty(arrayElements)) {
                    ret = arrayElements;
                } else {
                    List<Object> arrValues = new ArrayList<>(arrayElements.size());

                    for (Object element : arrayElements) {
                        if (element == null || context.isInactiveEdge(element, ignoreInactive)) {
                            continue;
                        }

                        Object arrValue = mapVertexToCollectionEntry(arrayElementType, vertexId, arrayElementType, element,
                                edgeLabel, edgeDirection, includeReferences, vertexEdgePropertiesCache, context);
                        if (arrValue != null) {
                            arrValues.add(arrValue);
                        }
                    }

                    ret = arrValues;
                }
                break;
            }
            case MAP:
                ret = mapVertexToMap(vertexId, attribute, includeReferences, vertexEdgePropertiesCache, context);
                break;
            case CLASSIFICATION:
            default:
                break;
        }

        return ret;
    }

    public Pair<List<AtlasTermAssignmentHeader>, List<String>> getMeaningHeadersFromCachedEdges(String sourceVertexId, VertexEdgePropertiesCache cache) {
        if (StringUtils.isEmpty(sourceVertexId) || cache == null) {
            return null;
        }

        List<EdgeVertexReference> meaningEdges = cache.getVertexEdgeReferencesByEdgeLabel(sourceVertexId, TERM_ASSIGNMENT_LABEL, IN);
        if (CollectionUtils.isEmpty(meaningEdges)) {
            return null;
        }

        List<AtlasTermAssignmentHeader> meaningHeaders = new ArrayList<>();
        List<String> meaningNames = new ArrayList<>();

        for (EdgeVertexReference meaningEdge : meaningEdges) {
            AtlasTermAssignmentHeader meaningHeader = toTermAssignmentHeader(meaningEdge, cache);
            if (meaningHeader == null) {
                continue;
            }

            meaningHeaders.add(meaningHeader);
            if (StringUtils.isNotEmpty(meaningHeader.getDisplayText())) {
                meaningNames.add(meaningHeader.getDisplayText());
            }
        }

        if (CollectionUtils.isEmpty(meaningHeaders)) {
            return null;
        }

        return Pair.with(meaningHeaders, meaningNames);
    }

    private AtlasTermAssignmentHeader toTermAssignmentHeader(EdgeVertexReference edgeReference, VertexEdgePropertiesCache cache) {
        if (edgeReference == null || cache == null) {
            return null;
        }

        AtlasTermAssignmentHeader ret = new AtlasTermAssignmentHeader();
        String termVertexId = edgeReference.getReferenceVertexId();

        ret.setTermGuid(getVertexPropertyValue(cache, termVertexId, GUID_PROPERTY_KEY, String.class));
        ret.setRelationGuid(getEdgePropertyValue(edgeReference, RELATIONSHIP_GUID_PROPERTY_KEY, String.class));
        ret.setDisplayText(getVertexPropertyValue(cache, termVertexId, GLOSSARY_TERM_DISPLAY_NAME_ATTR, String.class));
        ret.setDescription(getEdgePropertyValue(edgeReference, TERM_ASSIGNMENT_ATTR_DESCRIPTION, String.class));
        ret.setExpression(getEdgePropertyValue(edgeReference, TERM_ASSIGNMENT_ATTR_EXPRESSION, String.class));
        ret.setConfidence(getEdgePropertyValue(edgeReference, TERM_ASSIGNMENT_ATTR_CONFIDENCE, Integer.class));
        ret.setCreatedBy(getEdgePropertyValue(edgeReference, TERM_ASSIGNMENT_ATTR_CREATED_BY, String.class));
        ret.setSteward(getEdgePropertyValue(edgeReference, TERM_ASSIGNMENT_ATTR_STEWARD, String.class));
        ret.setSource(getEdgePropertyValue(edgeReference, TERM_ASSIGNMENT_ATTR_SOURCE, String.class));

        String status = getEdgePropertyValue(edgeReference, TERM_ASSIGNMENT_ATTR_STATUS, String.class);
        if (StringUtils.isNotEmpty(status)) {
            try {
                ret.setStatus(AtlasTermAssignmentStatus.valueOf(status));
            } catch (IllegalArgumentException ex) {
                LOG.debug("Invalid term-assignment status {} for edge {}", status, edgeReference.getEdgeId());
            }
        }

        return ret;
    }

    private <T> T getEdgePropertyValue(EdgeVertexReference edgeReference, String propertyName, Class<T> clazz) {
        if (edgeReference == null) {
            return null;
        }

        Object value = edgeReference.getProperty(propertyName);
        return castValue(getFirstValue(value), clazz);
    }

    private <T> T getVertexPropertyValue(VertexEdgePropertiesCache cache, String vertexId, String propertyName, Class<T> clazz) {
        if (cache == null || StringUtils.isEmpty(vertexId)) {
            return null;
        }

        Object value = cache.getPropertyValueWithFallback(vertexId, propertyName, Object.class);
        return castValue(getFirstValue(value), clazz);
    }

    private Object getFirstValue(Object value) {
        if (value instanceof List) {
            List<?> values = (List<?>) value;
            return CollectionUtils.isEmpty(values) ? null : values.get(0);
        }

        return value;
    }

    @SuppressWarnings("unchecked")
    private <T> T castValue(Object value, Class<T> clazz) {
        if (value == null) {
            return null;
        }

        if (clazz.isInstance(value)) {
            return (T) value;
        }

        if (clazz == String.class) {
            return (T) value.toString();
        }

        try {
            if (clazz == Integer.class) {
                return (T) Integer.valueOf(value.toString());
            }

            if (clazz == Long.class) {
                return (T) Long.valueOf(value.toString());
            }

            if (clazz == Float.class) {
                return (T) Float.valueOf(value.toString());
            }

            if (clazz == Double.class) {
                return (T) Double.valueOf(value.toString());
            }

            if (clazz == Boolean.class) {
                return (T) Boolean.valueOf(value.toString());
            }
        } catch (NumberFormatException e) {
            LOG.debug("Failed to cast {} to {}", value, clazz.getSimpleName());
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> mapVertexToMap(String vertexId, AtlasAttribute attribute,
                                               boolean includeReferences, VertexEdgePropertiesCache vertexEdgePropertiesCache,
                                               CachedAttributeMappingContext context) throws AtlasBaseException {
        Map<String, Object> ret = null;
        AtlasMapType mapType = (AtlasMapType) attribute.getAttributeType();
        AtlasType mapValueType = mapType.getValueType();

        if (isReference(mapValueType)) {
            Object cachedValue = vertexEdgePropertiesCache.getPropertyValueWithFallback(vertexId, attribute.getVertexPropertyName(), Object.class);

            if (cachedValue instanceof Map) {
                Map<?, ?> currentMap = (Map<?, ?>) cachedValue;
                ret = new HashMap<>();

                for (Map.Entry<?, ?> entry : currentMap.entrySet()) {
                    Object mapValue = mapVertexToCollectionEntry(mapValueType, vertexId, mapValueType, entry.getValue(),
                            attribute.getRelationshipEdgeLabel(), attribute.getRelationshipEdgeDirection(),
                            includeReferences, vertexEdgePropertiesCache, context);
                    if (mapValue != null) {
                        ret.put(String.valueOf(entry.getKey()), mapValue);
                    }
                }
            }
        } else {
            Object cachedValue = vertexEdgePropertiesCache.getPropertyValueWithFallback(vertexId, attribute.getVertexPropertyName(), Object.class);

            if (cachedValue instanceof String && LEAN_GRAPH_ENABLED) {
                ret = AtlasType.fromJson((String) cachedValue, Map.class);
            } else if (cachedValue instanceof Map) {
                ret = (Map<String, Object>) cachedValue;
            }
        }

        return ret;
    }

    @SuppressWarnings("unchecked")
    private Object mapVertexToCollectionEntry(AtlasType atlasType,
                                              String sourceVertexId, AtlasType arrayElement, Object value,
                                              String edgeLabel, AtlasRelationshipEdgeDirection edgeDirection,
                                              boolean includeReferences, VertexEdgePropertiesCache vertexEdgePropertiesCache,
                                              CachedAttributeMappingContext context) throws AtlasBaseException {
        Object ret = null;

        switch (arrayElement.getTypeCategory()) {
            case PRIMITIVE:
            case ENUM:
            case ARRAY:
                ret = value;
                break;
            case MAP:
                if (LEAN_GRAPH_ENABLED && value instanceof String) {
                    ret = AtlasType.fromJson((String) value, Map.class);
                } else {
                    ret = value;
                }
                break;
            case CLASSIFICATION:
                break;
            case STRUCT:
                ret = value instanceof String ? context.mapStringToStruct(atlasType, (String) value) : value;
                break;
            case OBJECT_ID_TYPE:
                if (vertexEdgePropertiesCache != null && value instanceof Pair<?, ?>) {
                    ret = includeReferences
                            ? context.mapVertexToObjectId(sourceVertexId, (Pair<String, EdgeVertexReference.EdgeInfo>) value,
                            edgeDirection, vertexEdgePropertiesCache)
                            : null;
                }
                break;
            default:
                break;
        }

        return ret;
    }

    private List<Object> getArrayElementsFromCache(AtlasType elementType, String vertexId, AtlasAttribute attribute,
                                                   VertexEdgePropertiesCache vertexEdgePropertiesCache) {
        String propertyName = attribute.getVertexPropertyName();
        boolean isArrayOfPrimitiveType = elementType.getTypeCategory().equals(TypeCategory.PRIMITIVE);
        boolean isArrayOfEnum = elementType.getTypeCategory().equals(TypeCategory.ENUM);

        if (isReference(elementType)) {
            boolean isStruct = TypeCategory.STRUCT == attribute.getDefinedInType().getTypeCategory()
                    || TypeCategory.STRUCT == elementType.getTypeCategory();

            if (isStruct) {
                String edgeLabel = AtlasGraphUtilsV2.getEdgeLabel(attribute.getName());
                List<EdgeVertexReference> references = vertexEdgePropertiesCache.getVertexEdgeReferencesByEdgeLabel(
                        vertexId, edgeLabel, attribute.getRelationshipEdgeDirection());
                List<Object> ret = new ArrayList<>(references.size());
                for (EdgeVertexReference reference : references) {
                    ret.add(Pair.with(reference.getReferenceVertexId(), reference.getEdgeInfo()));
                }
                return ret;
            }

            List<Pair<String, EdgeVertexReference.EdgeInfo>> values =
                    vertexEdgePropertiesCache.getCollectionElementsUsingRelationship(vertexId, attribute);
            return values == null ? Collections.emptyList() : new ArrayList<>(values);
        } else if (isArrayOfPrimitiveType || isArrayOfEnum) {
            List<?> values = vertexEdgePropertiesCache.getMultiValuedPropertiesWithFallback(vertexId, propertyName);
            return values == null ? Collections.emptyList() : new ArrayList<>(values);
        } else {
            List<?> values = vertexEdgePropertiesCache.getMultiValuedPropertiesWithFallback(vertexId, propertyName);
            return values == null ? Collections.emptyList() : new ArrayList<>(values);
        }
    }

    private Set<String> collectEdgeLabelsToProcess(VertexEdgePropertiesCache cache, Set<String> vertexIds, Set<String> attributes) {
        RequestContext context = RequestContext.get();
        Set<String> edgeLabels = new HashSet<>();
        if (LEAN_GRAPH_ENABLED && context.includeMeanings()) {
            edgeLabels.add(TERM_ASSIGNMENT_LABEL);
        }

        if (attributes == null || attributes.isEmpty()) {
            return edgeLabels;
        }

        Set<String> typeNames = vertexIds.stream()
                .map(cache::getTypeName)
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toSet());

        for (String typeName : typeNames) {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
            if (entityType == null) {
                continue;
            }

            for (String attribute : attributes) {
                processRelationshipAttribute(entityType, attribute, edgeLabels);
            }
        }

        return edgeLabels;
    }

    private void processRelationshipAttribute(AtlasEntityType entityType, String attribute, Set<String> edgeLabels) {
        RequestContext context = RequestContext.get();
        if (!entityType.getRelationshipAttributes().containsKey(attribute)) {
            return;
        }

        AtlasAttribute atlasAttribute = entityType.getRelationshipAttribute(attribute, null);
        if (atlasAttribute != null && atlasAttribute.getAttributeType() != null) {
            if (context.isInvokedByIndexSearch() && context.isInvokedByProduct() &&
                    CollectionUtils.isEmpty(context.getRelationAttrsForSearch())) {
                return;
            }
            edgeLabels.add(atlasAttribute.getRelationshipEdgeLabel());
        } else {
            LOG.debug("Ignoring non-relationship type attribute: {}", attribute);
        }
    }

    private Pair<Map<String, Map<String, List<?>>>, Map<String, AtlasVertex>> getVertexPropertiesValueMap(Set<String> vertexIds, int batchSize) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getVertexPropertiesValueMap");
        try {
            if (CollectionUtils.isEmpty(vertexIds)) {
                return Pair.with(Collections.emptyMap(), Collections.emptyMap());
            }

            if (LEAN_GRAPH_ENABLED) {
                return getVertexPropertiesValueMapLeanGraph(vertexIds, batchSize);
            }

            return getVertexPropertiesValueMapGraph(vertexIds, batchSize);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private Pair<Map<String, Map<String, List<?>>>, Map<String, AtlasVertex>> getVertexPropertiesValueMapGraph(Set<String> vertexIds, int batchSize) {
        Map<String, Map<String, List<?>>> vertexPropertyMap = new HashMap<>();
        Map<String, AtlasVertex> vertexMap = new HashMap<>();

        ListUtils.partition(new ArrayList<>(vertexIds), batchSize)
                .forEach(batch -> populateVertexPropertiesFromGraph(batch, vertexPropertyMap, vertexMap));

        return Pair.with(vertexPropertyMap, vertexMap);
    }

    private Pair<Map<String, Map<String, List<?>>>, Map<String, AtlasVertex>> getVertexPropertiesValueMapLeanGraph(Set<String> vertexIds, int batchSize) {
        Map<String, Map<String, List<?>>> vertexPropertyMap = new HashMap<>();
        Map<String, AtlasVertex> vertexMap = new HashMap<>();

        ListUtils.partition(new ArrayList<>(vertexIds), batchSize).forEach(batch -> {
            Map<String, DynamicVertex> dynamicVertices = Collections.emptyMap();
            if (dynamicVertexService != null) {
                try {
                    dynamicVertices = dynamicVertexService.retrieveVertices(batch);
                } catch (AtlasBaseException e) {
                    LOG.warn("Failed dynamic vertex fetch for lean-graph batch {}", batch, e);
                }
            }

            if (MapUtils.isNotEmpty(dynamicVertices)) {
                for (String vertexId : batch) {
                    DynamicVertex dynamicVertex = dynamicVertices.get(vertexId);
                    if (dynamicVertex == null || MapUtils.isEmpty(dynamicVertex.getAllProperties())) {
                        continue;
                    }

                    Map<String, List<?>> vertexProperties = toVertexPropertiesMap(dynamicVertex.getAllProperties());
                    if (MapUtils.isNotEmpty(vertexProperties)) {
                        vertexPropertyMap.put(vertexId, vertexProperties);
                    }
                }
            }
        });

        return Pair.with(vertexPropertyMap, vertexMap);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void populateVertexPropertiesFromGraph(List<String> vertexIds, Map<String, Map<String, List<?>>> vertexPropertyMap, Map<String, AtlasVertex> vertexMap) {
        if (CollectionUtils.isEmpty(vertexIds)) {
            return;
        }

        GraphTraversal vertices = graph.V(vertexIds)
                .project("vertex", "properties")
                .by()
                .by(__.valueMap(true));

        List<Map<String, Object>> results = vertices.toList();
        results.forEach(vertexInfo -> {
            AtlasVertex vertex = new AtlasJanusVertex((AtlasJanusGraph) graph, (Vertex) vertexInfo.get("vertex"));
            vertexMap.put(vertex.getIdForDisplay(), vertex);

            Map<Object, Object> properties = (Map<Object, Object>) vertexInfo.get("properties");
            if (MapUtils.isEmpty(properties) || !properties.containsKey(T.id)) {
                return;
            }

            String vertexId;
            if (LEAN_GRAPH_ENABLED) {
                vertexId = (String) properties.get(T.id);
            } else {
                Long id = (Long) properties.get(T.id);
                vertexId = id.toString();
            }

            properties.remove(T.id);
            properties.remove(T.label);
            vertexPropertyMap.put(vertexId, getStringArrayListMap(properties));
        });
    }

    private Map<String, List<?>> toVertexPropertiesMap(Map<String, Object> properties) {
        Map<String, List<?>> vertexProperties = new HashMap<>();
        if (MapUtils.isEmpty(properties)) {
            return vertexProperties;
        }

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }

            List<?> values;
            if (value instanceof Collection) {
                values = new ArrayList<>((Collection<?>) value);
            } else {
                values = Collections.singletonList(value);
            }

            vertexProperties.put(entry.getKey(), values);
        }

        return vertexProperties;
    }

    private Map<String, List<?>> normalizeVertexPropertyTypes(Map<String, List<?>> properties) {
        if (!LEAN_GRAPH_ENABLED || MapUtils.isEmpty(properties)) {
            return properties;
        }

        String typeName = getFirstPropertyStringValue(properties, ENTITY_TYPE_PROPERTY_KEY);
        if (StringUtils.isEmpty(typeName)) {
            typeName = getFirstPropertyStringValue(properties, TYPE_NAME_PROPERTY_KEY);
        }
        if (StringUtils.isEmpty(typeName)) {
            return properties;
        }

        Map<String, AtlasType> propertyTypes = getVertexPropertyTypes(typeName);
        if (MapUtils.isEmpty(propertyTypes)) {
            return properties;
        }

        for (Map.Entry<String, List<?>> entry : properties.entrySet()) {
            AtlasType attributeType = propertyTypes.get(entry.getKey());
            if (attributeType == null || CollectionUtils.isEmpty(entry.getValue())) {
                continue;
            }

            entry.setValue(convertPropertyValues(entry.getValue(), attributeType));
        }

        return properties;
    }

    private Map<String, AtlasType> getVertexPropertyTypes(String typeName) {
        return vertexPropertyTypesByTypeName.computeIfAbsent(typeName, this::buildVertexPropertyTypes);
    }

    private Map<String, AtlasType> buildVertexPropertyTypes(String typeName) {
        AtlasType atlasType;
        try {
            atlasType = typeRegistry.getType(typeName);
        } catch (AtlasBaseException e) {
            LOG.warn("Failed to resolve type '{}' while building vertex property type cache", typeName, e);
            return Collections.emptyMap();
        }

        if (!(atlasType instanceof AtlasStructType)) {
            return Collections.emptyMap();
        }

        AtlasStructType structType = (AtlasStructType) atlasType;
        Map<String, AtlasType> propertyTypes = new HashMap<>();

        addAttributeTypes(propertyTypes, structType.getAllAttributes().values());

        if (structType instanceof AtlasEntityType) {
            addAttributeTypes(propertyTypes, AtlasEntityType.getEntityRoot().getAllAttributes().values());
        } else if (structType instanceof AtlasClassificationType) {
            addAttributeTypes(propertyTypes, AtlasClassificationType.getClassificationRoot().getAllAttributes().values());
        }

        return propertyTypes;
    }

    private void addAttributeTypes(Map<String, AtlasType> propertyTypes, Collection<AtlasAttribute> attributes) {
        if (propertyTypes == null || CollectionUtils.isEmpty(attributes)) {
            return;
        }

        for (AtlasAttribute attribute : attributes) {
            AtlasType attributeType = attribute.getAttributeType();
            if (attributeType == null) {
                continue;
            }

            propertyTypes.put(attribute.getVertexPropertyName(), attributeType);
            propertyTypes.putIfAbsent(attribute.getName(), attributeType);
        }
    }

    private List<?> convertPropertyValues(List<?> values, AtlasType attributeType) {
        AtlasType valueType = getValueType(attributeType);
        if (CollectionUtils.isEmpty(values) || valueType == null) {
            return values;
        }

        TypeCategory valueTypeCategory = valueType.getTypeCategory();
        if (valueTypeCategory != TypeCategory.PRIMITIVE && valueTypeCategory != TypeCategory.ENUM) {
            return values;
        }

        List<Object> convertedValues = new ArrayList<>(values.size());
        boolean changed = false;

        for (Object value : values) {
            Object convertedValue = convertPropertyValue(value, valueType);
            convertedValues.add(convertedValue);
            changed = changed || convertedValue != value;
        }

        return changed ? convertedValues : values;
    }

    private AtlasType getValueType(AtlasType attributeType) {
        if (attributeType == null) {
            return null;
        }

        if (attributeType.getTypeCategory() == TypeCategory.ARRAY) {
            return ((AtlasArrayType) attributeType).getElementType();
        }

        return attributeType;
    }

    private Object convertPropertyValue(Object value, AtlasType valueType) {
        if (value == null || valueType == null) {
            return value;
        }

        TypeCategory valueTypeCategory = valueType.getTypeCategory();
        if (valueTypeCategory == TypeCategory.ENUM) {
            return value instanceof String ? value : String.valueOf(value);
        }
        if (valueTypeCategory != TypeCategory.PRIMITIVE) {
            return value;
        }

        try {
            switch (valueType.getTypeName()) {
                case ATLAS_TYPE_STRING:
                    return value instanceof String ? value : String.valueOf(value);
                case ATLAS_TYPE_SHORT:
                    if (value instanceof Short) {
                        return value;
                    } else if (value instanceof Number) {
                        return ((Number) value).shortValue();
                    } else if (value instanceof String && StringUtils.isNotBlank((String) value)) {
                        return Short.parseShort((String) value);
                    }
                    break;
                case ATLAS_TYPE_INT:
                    if (value instanceof Integer) {
                        return value;
                    } else if (value instanceof Number) {
                        return ((Number) value).intValue();
                    } else if (value instanceof String && StringUtils.isNotBlank((String) value)) {
                        return Integer.parseInt((String) value);
                    }
                    break;
                case ATLAS_TYPE_BIGINTEGER:
                    if (value instanceof BigInteger) {
                        return value;
                    } else if (value instanceof Number) {
                        return BigInteger.valueOf(((Number) value).longValue());
                    } else if (value instanceof String && StringUtils.isNotBlank((String) value)) {
                        return new BigInteger((String) value);
                    }
                    break;
                case ATLAS_TYPE_BOOLEAN:
                    if (value instanceof Boolean) {
                        return value;
                    } else if (value instanceof Number) {
                        return ((Number) value).intValue() != 0;
                    } else if (value instanceof String) {
                        return Boolean.parseBoolean((String) value);
                    }
                    break;
                case ATLAS_TYPE_BYTE:
                    if (value instanceof Byte) {
                        return value;
                    } else if (value instanceof Number) {
                        return ((Number) value).byteValue();
                    } else if (value instanceof String && StringUtils.isNotBlank((String) value)) {
                        return Byte.parseByte((String) value);
                    }
                    break;
                case ATLAS_TYPE_LONG:
                case ATLAS_TYPE_DATE:
                    if (value instanceof Long) {
                        return value;
                    } else if (value instanceof Date) {
                        return ((Date) value).getTime();
                    } else if (value instanceof Number) {
                        return ((Number) value).longValue();
                    } else if (value instanceof String && StringUtils.isNotBlank((String) value)) {
                        return Long.parseLong((String) value);
                    }
                    break;
                case ATLAS_TYPE_FLOAT:
                    if (value instanceof Float) {
                        return value;
                    } else if (value instanceof Number) {
                        return ((Number) value).floatValue();
                    } else if (value instanceof String && StringUtils.isNotBlank((String) value)) {
                        return Float.parseFloat((String) value);
                    }
                    break;
                case ATLAS_TYPE_DOUBLE:
                    if (value instanceof Double) {
                        return value;
                    } else if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    } else if (value instanceof String && StringUtils.isNotBlank((String) value)) {
                        return Double.parseDouble((String) value);
                    }
                    break;
                case ATLAS_TYPE_BIGDECIMAL:
                    if (value instanceof BigDecimal) {
                        return value;
                    } else if (value instanceof Number) {
                        return new BigDecimal(value.toString());
                    } else if (value instanceof String && StringUtils.isNotBlank((String) value)) {
                        return new BigDecimal((String) value);
                    }
                    break;
                default:
                    break;
            }
        } catch (NumberFormatException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to convert property value '{}' to type '{}'", value, valueType.getTypeName(), e);
            }
        }

        return value;
    }

    private String getFirstPropertyStringValue(Map<String, List<?>> properties, String propertyName) {
        if (MapUtils.isEmpty(properties) || StringUtils.isEmpty(propertyName)) {
            return null;
        }

        List<?> values = properties.get(propertyName);
        if (CollectionUtils.isEmpty(values) || values.get(0) == null) {
            return null;
        }

        Object value = values.get(0);
        return value instanceof String ? (String) value : String.valueOf(value);
    }

    private static Map<String, List<?>> getStringArrayListMap(Map<Object, Object> properties) {
        Map<String, List<?>> vertexProperties = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String attributeName = entry.getKey().toString();
            Object attributeValue = entry.getValue();
            if (attributeValue instanceof List) {
                vertexProperties.put(attributeName, new ArrayList<>((List<?>) attributeValue));
            }
        }
        return vertexProperties;
    }
}
