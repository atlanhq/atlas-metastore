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

import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.atlas.repository.Constants.TERM_ASSIGNMENT_LABEL;

/**
 * Per-vertex-type edge label resolution with correct direction.
 *
 * <p>This is the core CQL amplification fix. Current code at
 * {@code collectEdgeLabelsWithDirection()} returns a flat {@code Map<label, direction>}
 * that is the union of all edge labels across all entity types in the result set.
 * Every vertex is queried for every label — a Table with 8 labels gets queried
 * for 31 union labels, and a Connection with 0 edges gets 31 queries.</p>
 *
 * <p>This resolver returns {@code Map<vertexId, Map<label, direction>>} — each vertex
 * gets ONLY its own type's relevant edge labels with correct direction.
 * Connection → 0 labels → 0 CQL. Table → 8 labels → 8 CQL.</p>
 *
 * <p>Per-type label sets are cached in a {@link ConcurrentHashMap}. Type definitions
 * are immutable at runtime, so this is safe to cache indefinitely (it's metadata,
 * not CQL data).</p>
 */
public class TypeAwareEdgeLabelResolver {

    private static final Logger LOG = LoggerFactory.getLogger(TypeAwareEdgeLabelResolver.class);

    private final AtlasTypeRegistry typeRegistry;

    /**
     * Cache: cacheKey → { edgeLabel → direction }.
     * Key includes typeName + hash of requested attributes to handle different attribute sets.
     * Type defs don't change at runtime — safe to cache indefinitely.
     */
    private final ConcurrentHashMap<String, Map<String, AtlasEdgeDirection>> typeCache = new ConcurrentHashMap<>();

    public TypeAwareEdgeLabelResolver(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    /**
     * Resolve edge labels per-vertex-type.
     *
     * @param vertexTypeMap         vertexId → typeName (from Stage 1)
     * @param requestedAttrs        client-requested attributes
     * @param requestedRelAttrs     client-requested relation attributes
     * @param includeMeanings       whether to include TERM_ASSIGNMENT_LABEL for term assignments
     * @return vertexId → { edgeLabel → direction }. Empty map for a vertex = 0 CQL for that vertex.
     */
    public Map<String, Map<String, AtlasEdgeDirection>> resolvePerVertex(
            Map<String, String> vertexTypeMap,
            Set<String> requestedAttrs,
            Set<String> requestedRelAttrs,
            boolean includeMeanings) {

        if (vertexTypeMap == null || vertexTypeMap.isEmpty()) {
            return Collections.emptyMap();
        }

        // Group vertices by type — vertices of the same type share the same label set
        Map<String, List<String>> typeToVertexIds = new HashMap<>();
        for (Map.Entry<String, String> entry : vertexTypeMap.entrySet()) {
            if (entry.getValue() != null) {
                typeToVertexIds.computeIfAbsent(entry.getValue(), k -> new ArrayList<>())
                        .add(entry.getKey());
            }
        }

        Map<String, Map<String, AtlasEdgeDirection>> result = new HashMap<>();

        for (Map.Entry<String, List<String>> typeEntry : typeToVertexIds.entrySet()) {
            String typeName = typeEntry.getKey();
            List<String> vertexIds = typeEntry.getValue();

            // Get or compute label set for this type (cached across vertices of same type)
            String cacheKey = typeName + "|" + Objects.hash(requestedAttrs, requestedRelAttrs, includeMeanings);
            Map<String, AtlasEdgeDirection> labelsForType = typeCache.computeIfAbsent(
                    cacheKey, k -> computeLabelsForType(typeName, requestedAttrs, requestedRelAttrs, includeMeanings)
            );

            // All vertices of this type get the same label set
            if (!labelsForType.isEmpty()) {
                for (String vertexId : vertexIds) {
                    result.put(vertexId, labelsForType);
                }
            }
            // Empty labelsForType → 0 CQL for these vertices (e.g., Connection with no edges)
        }

        if (LOG.isDebugEnabled()) {
            int totalLabels = result.values().stream().mapToInt(Map::size).sum();
            LOG.debug("TypeAwareEdgeLabelResolver: {} vertices, {} unique types, {} total label queries",
                    vertexTypeMap.size(), typeToVertexIds.size(), totalLabels);
        }

        return result;
    }

    /**
     * Compute edge labels for a single entity type — scoped to THIS type only,
     * with correct direction per label.
     *
     * <p>Mirrors the logic in {@code EntityGraphRetriever.processRelationshipAttribute()}
     * but scoped to one type instead of union across all types.</p>
     */
    private Map<String, AtlasEdgeDirection> computeLabelsForType(
            String typeName,
            Set<String> requestedAttrs,
            Set<String> requestedRelAttrs,
            boolean includeMeanings) {

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        if (entityType == null) {
            LOG.debug("Entity type not found in registry: {}", typeName);
            return Collections.emptyMap();
        }

        RequestContext context = RequestContext.get();
        Map<String, AtlasEdgeDirection> labelDirections = new HashMap<>();

        // Combine all attributes to check
        Set<String> allAttrs = new HashSet<>();
        if (CollectionUtils.isNotEmpty(requestedAttrs)) {
            allAttrs.addAll(requestedAttrs);
        }
        if (CollectionUtils.isNotEmpty(requestedRelAttrs)) {
            allAttrs.addAll(requestedRelAttrs);
        }

        for (String attrName : allAttrs) {
            // Only check if THIS type has this relationship attribute
            if (!entityType.getRelationshipAttributes().containsKey(attrName)) {
                continue;
            }

            AtlasAttribute atlasAttribute = entityType.getRelationshipAttribute(attrName, null);
            if (atlasAttribute == null || atlasAttribute.getAttributeType() == null) {
                continue;
            }

            // Match existing behaviour: skip ALL relationship attrs for product calls with no relAttrsForSearch
            // (existing code at processRelationshipAttribute line 1212-1214)
            if (context.isInvokedByIndexSearch() && context.isInvokedByProduct()
                    && CollectionUtils.isEmpty(context.getRelationAttrsForSearch())) {
                continue;
            }

            String label = atlasAttribute.getRelationshipEdgeLabel();
            AtlasEdgeDirection direction = toAtlasEdgeDirection(atlasAttribute.getRelationshipEdgeDirection());

            if (label != null) {
                // If same label seen with different direction (rare), use BOTH
                labelDirections.merge(label, direction, (existing, incoming) ->
                        existing == incoming ? existing : AtlasEdgeDirection.BOTH);
            }
        }

        // Note: header attributes that are edge-backed are NOT included here,
        // matching existing collectEdgeLabelsWithDirection() behaviour. Header attrs
        // are resolved from cache in toAtlasEntityHeader — if the edge wasn't fetched
        // because the attr wasn't in the requested set, it returns null (same as current code).

        // Include TERM_ASSIGNMENT_LABEL if meanings requested
        if (includeMeanings) {
            labelDirections.put(TERM_ASSIGNMENT_LABEL, AtlasEdgeDirection.IN);
        }

        return labelDirections;
    }

    private static AtlasEdgeDirection toAtlasEdgeDirection(AtlasRelationshipEdgeDirection relDirection) {
        if (relDirection == null) {
            return AtlasEdgeDirection.BOTH;
        }
        switch (relDirection) {
            case IN:  return AtlasEdgeDirection.IN;
            case OUT: return AtlasEdgeDirection.OUT;
            default:  return AtlasEdgeDirection.BOTH;
        }
    }
}
