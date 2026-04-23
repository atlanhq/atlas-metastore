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
package org.apache.atlas.discovery.searchpipeline.stages;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.searchpipeline.EnrichmentStage;
import org.apache.atlas.discovery.searchpipeline.SearchEnrichmentContext;
import org.apache.atlas.discovery.searchpipeline.TypeAwareEdgeLabelResolver;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.EdgeVertexReference;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.cassandra.CassandraGraph;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.repository.Constants.*;

/**
 * Stage 2: Fetch edges using per-vertex-type labels and correct direction.
 *
 * <p>Uses {@link TypeAwareEdgeLabelResolver} to get per-vertex label sets, then
 * calls {@code CassandraGraph.getEdgesForVerticesPerType()} to fetch edges in
 * a single async wave. Only queries labels that exist on each vertex's type.</p>
 *
 * <p>Filters edges to ACTIVE state + has RELATIONSHIP_GUID (matching existing behaviour
 * in {@code EntityGraphRetriever.getEdgeInfoMapsViaAtlasApi}).</p>
 *
 * <p>Short-circuits if no relationship attributes requested and includeMeanings is false.</p>
 */
public class TypeAwareEdgeFetcher implements EnrichmentStage {

    private static final Logger LOG = LoggerFactory.getLogger(TypeAwareEdgeFetcher.class);

    private final AtlasGraph graph;
    private final TypeAwareEdgeLabelResolver labelResolver;

    public TypeAwareEdgeFetcher(AtlasGraph graph, TypeAwareEdgeLabelResolver labelResolver) {
        this.graph = graph;
        this.labelResolver = labelResolver;
    }

    @Override
    public String name() {
        return "typeAwareEdgeFetch";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void enrich(SearchEnrichmentContext context) throws AtlasBaseException {
        // Short-circuit: if no relationship attributes and no meanings, skip entirely
        if (CollectionUtils.isEmpty(context.getRequestedAttributes())
                && CollectionUtils.isEmpty(context.getRequestedRelationAttributes())
                && !context.isIncludeMeanings()) {
            return;
        }

        RequestContext reqCtx = RequestContext.get();
        int limitPerLabel = reqCtx.isInvokedByProduct()
                ? AtlasConfiguration.MIN_EDGES_SUPER_VERTEX.getInt()
                : AtlasConfiguration.MAX_EDGES_SUPER_VERTEX.getInt();

        // Resolve per-vertex-type labels with direction (0 CQL, in-memory)
        Map<String, Map<String, AtlasEdgeDirection>> perVertexLabels =
                labelResolver.resolvePerVertex(
                        context.getVertexTypeMap(),
                        context.getRequestedAttributes(),
                        context.getRequestedRelationAttributes(),
                        context.isIncludeMeanings()
                );

        if (MapUtils.isEmpty(perVertexLabels)) {
            return;
        }

        // Fetch edges — per-vertex labels, single async wave
        Map<String, List<AtlasEdge>> allEdgesMap = (Map) ((CassandraGraph) graph)
                .getEdgesForVerticesPerType(perVertexLabels, limitPerLabel);

        // Process edges into context
        for (Map.Entry<String, List<AtlasEdge>> entry : allEdgesMap.entrySet()) {
            String vertexId = entry.getKey();
            List<AtlasEdge> edges = entry.getValue();
            List<EdgeVertexReference> edgeRefs = new ArrayList<>();

            for (AtlasEdge edge : edges) {
                // Filter: match existing behaviour from getEdgeInfoMapsViaAtlasApi
                String state = edge.getProperty(STATE_PROPERTY_KEY, String.class);
                if (!ACTIVE.name().equals(state)) {
                    continue;
                }

                String relGuid = edge.getProperty(RELATIONSHIP_GUID_PROPERTY_KEY, String.class);
                if (relGuid == null) {
                    continue;
                }

                String edgeId = edge.getIdForDisplay();
                String edgeLabel = edge.getLabel();
                String outVertexId = edge.getOutVertex().getIdForDisplay();
                String inVertexId = edge.getInVertex().getIdForDisplay();

                // Build properties map from edge
                LinkedHashMap<Object, Object> valueMap = new LinkedHashMap<>();
                for (String key : edge.getPropertyKeys()) {
                    Object val = edge.getProperty(key, Object.class);
                    if (val != null) {
                        valueMap.put(key, val);
                    }
                }

                // Determine referenced vertex (the other end)
                String referencedVertex;
                boolean isSelfLoop = vertexId.equals(outVertexId) && vertexId.equals(inVertexId);
                if (isSelfLoop) {
                    referencedVertex = outVertexId;
                } else if (vertexId.equals(outVertexId)) {
                    referencedVertex = inVertexId;
                } else {
                    referencedVertex = outVertexId;
                }

                edgeRefs.add(new EdgeVertexReference(
                        referencedVertex, edgeId, edgeLabel, inVertexId, outVertexId, valueMap
                ));

                context.getReferencedVertexIds().add(referencedVertex);
            }

            context.setEdges(vertexId, edgeRefs);
        }

        // Remove result vertex IDs from reference set (already loaded in Stage 1)
        context.getReferencedVertexIds().removeAll(new HashSet<>(context.getOrderedVertexIds()));

        if (LOG.isDebugEnabled()) {
            int totalEdges = context.getVertexEdges().values().stream().mapToInt(List::size).sum();
            LOG.debug("TypeAwareEdgeFetcher: {} vertices, {} total edges, {} referenced vertices to load",
                    allEdgesMap.size(), totalEdges, context.getReferencedVertexIds().size());
        }
    }
}
