/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.discovery.searchpipeline;

import org.apache.atlas.model.discovery.SearchParams;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.glossary.relations.AtlasTermAssignmentHeader;
import org.apache.atlas.repository.EdgeVertexReference;
import org.apache.atlas.repository.graphdb.AtlasVertex;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Mutable context that flows through all enrichment pipeline stages.
 *
 * Each stage populates its section. The {@code HeaderAssembler} reads all
 * sections to build the final {@code AtlasEntityHeader} response objects.
 *
 * No cross-request caching — all data is fresh per request.
 */
public class SearchEnrichmentContext {

    // ── Input (immutable after construction) ──────────────────────

    private final List<ESHitResult>  esHits;
    private final Set<String>        requestedAttributes;
    private final Set<String>        requestedRelationAttributes;
    private final SearchParams       searchParams;
    private final boolean            includeClassifications;
    private final boolean            includeClassificationNames;
    private final boolean            includeMeanings;

    // ── Stage 0 output ───────────────────────────────────────────

    private List<String> orderedVertexIds;

    // ── Stage 1 output (VertexBulkLoader) ────────────────────────

    private final Map<String, Map<String, List<?>>> vertexProperties = new HashMap<>();
    private final Map<String, AtlasVertex>          vertexObjects    = new HashMap<>();
    private final Map<String, String>               vertexTypeMap    = new HashMap<>();

    // ── Stage 2 output (TypeAwareEdgeFetcher) ────────────────────

    private final Map<String, List<EdgeVertexReference>> vertexEdges        = new HashMap<>();
    private final Set<String>                            referencedVertexIds = new HashSet<>();

    // ── Stage 4 output (SmartClassificationLoader) ───────────────

    private final Map<String, List<AtlasClassification>> classificationMap = new HashMap<>();

    // ── Stage 5 output (TermAssignmentLoader) ────────────────────

    private final Map<String, List<AtlasTermAssignmentHeader>> termAssignmentMap = new HashMap<>();

    // ── Constructor ──────────────────────────────────────────────

    public SearchEnrichmentContext(List<ESHitResult> esHits,
                                   List<String> orderedVertexIds,
                                   Set<String> requestedAttributes,
                                   Set<String> requestedRelationAttributes,
                                   SearchParams searchParams,
                                   boolean includeClassifications,
                                   boolean includeClassificationNames,
                                   boolean includeMeanings) {
        this.esHits                      = esHits;
        this.orderedVertexIds            = orderedVertexIds;
        this.requestedAttributes         = requestedAttributes != null ? requestedAttributes : Collections.emptySet();
        this.requestedRelationAttributes = requestedRelationAttributes != null ? requestedRelationAttributes : Collections.emptySet();
        this.searchParams                = searchParams;
        this.includeClassifications      = includeClassifications;
        this.includeClassificationNames  = includeClassificationNames;
        this.includeMeanings             = includeMeanings;
    }

    // ── Vertex property accessors ────────────────────────────────

    /**
     * Read a single property value from a vertex's cached properties.
     * Returns null if the vertex or property is not found.
     *
     * Used by HeaderAssembler and other stages to read vertex data without CQL.
     */
    @SuppressWarnings("unchecked")
    public <T> T getVertexProperty(String vertexId, String propertyKey, Class<T> type) {
        Map<String, List<?>> props = vertexProperties.get(vertexId);
        if (props == null) {
            return null;
        }

        List<?> values = props.get(propertyKey);
        if (values == null || values.isEmpty()) {
            return null;
        }

        Object value = values.get(0);
        if (value == null) {
            return null;
        }

        if (type.isInstance(value)) {
            return type.cast(value);
        }

        // Common type conversions
        if (type == Long.class && value instanceof Number) {
            return type.cast(((Number) value).longValue());
        }
        if (type == Integer.class && value instanceof Number) {
            return type.cast(((Number) value).intValue());
        }
        if (type == Boolean.class && value instanceof String) {
            return type.cast(Boolean.valueOf((String) value));
        }
        if (type == String.class) {
            return type.cast(String.valueOf(value));
        }

        return null;
    }

    /**
     * Get multi-valued property (e.g., list attributes).
     */
    public List<?> getVertexPropertyValues(String vertexId, String propertyKey) {
        Map<String, List<?>> props = vertexProperties.get(vertexId);
        if (props == null) {
            return null;
        }
        return props.get(propertyKey);
    }

    // ── Edge accessors ───────────────────────────────────────────

    /**
     * Get all edges for a vertex matching a specific edge label.
     * Used by TermAssignmentLoader and HeaderAssembler to resolve
     * relationship attributes without CQL.
     */
    public List<EdgeVertexReference> getEdgesForLabel(String vertexId, String edgeLabel) {
        List<EdgeVertexReference> edges = vertexEdges.get(vertexId);
        if (edges == null || edges.isEmpty()) {
            return Collections.emptyList();
        }
        return edges.stream()
                .filter(e -> edgeLabel.equals(e.getEdgeLabel()))
                .collect(Collectors.toList());
    }

    /**
     * Get all edges for a vertex (all labels).
     */
    public List<EdgeVertexReference> getAllEdges(String vertexId) {
        return vertexEdges.getOrDefault(vertexId, Collections.emptyList());
    }

    // ── Stage population methods ─────────────────────────────────

    /**
     * Add vertex data from Stage 1 (VertexBulkLoader) or Stage 3 (ReferenceVertexLoader).
     * Extracts typeName from properties and populates vertexTypeMap.
     */
    public void addVertexData(String vertexId, Map<String, List<?>> properties, AtlasVertex vertex) {
        vertexProperties.put(vertexId, properties);
        if (vertex != null) {
            vertexObjects.put(vertexId, vertex);
        }

        // Extract typeName from properties for vertexTypeMap
        List<?> typeNames = properties.get("__typeName");
        if (typeNames != null && !typeNames.isEmpty()) {
            vertexTypeMap.put(vertexId, String.valueOf(typeNames.get(0)));
        }
    }

    public void setEdges(String vertexId, List<EdgeVertexReference> edges) {
        vertexEdges.put(vertexId, edges);
    }

    // ── Getters ──────────────────────────────────────────────────

    public List<ESHitResult> getEsHits()                                  { return esHits; }
    public List<String> getOrderedVertexIds()                             { return orderedVertexIds; }
    public void setOrderedVertexIds(List<String> ids)                     { this.orderedVertexIds = ids; }
    public Set<String> getRequestedAttributes()                           { return requestedAttributes; }
    public Set<String> getRequestedRelationAttributes()                   { return requestedRelationAttributes; }
    public SearchParams getSearchParams()                                 { return searchParams; }
    public boolean isIncludeClassifications()                             { return includeClassifications; }
    public boolean isIncludeClassificationNames()                         { return includeClassificationNames; }
    public boolean isIncludeMeanings()                                    { return includeMeanings; }

    public Map<String, Map<String, List<?>>> getVertexProperties()        { return vertexProperties; }
    public Map<String, AtlasVertex> getVertexObjects()                    { return vertexObjects; }
    public AtlasVertex getVertexObject(String vertexId)                   { return vertexObjects.get(vertexId); }
    public Map<String, String> getVertexTypeMap()                         { return vertexTypeMap; }

    public Map<String, List<EdgeVertexReference>> getVertexEdges()        { return vertexEdges; }
    public Set<String> getReferencedVertexIds()                           { return referencedVertexIds; }

    public Map<String, List<AtlasClassification>> getClassificationMap()  { return classificationMap; }
    public Map<String, List<AtlasTermAssignmentHeader>> getTermAssignmentMap() { return termAssignmentMap; }
}
