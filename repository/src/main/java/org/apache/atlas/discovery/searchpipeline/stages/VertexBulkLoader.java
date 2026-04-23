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

import org.apache.atlas.discovery.searchpipeline.EnrichmentStage;
import org.apache.atlas.discovery.searchpipeline.SearchEnrichmentContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Stage 1: Bulk-load all result vertex properties in a single async batch.
 *
 * <p>Calls {@code graph.getVertices(vertexIds)} which uses
 * {@code VertexRepository.getVerticesAsync()} — all CQL queries fired in parallel,
 * same existing concurrency pattern.</p>
 *
 * <p>Populates context with vertex properties, AtlasVertex objects, and vertexTypeMap
 * (vertexId → typeName) which is used by subsequent stages.</p>
 */
public class VertexBulkLoader implements EnrichmentStage {

    private static final Logger LOG = LoggerFactory.getLogger(VertexBulkLoader.class);

    private final AtlasGraph graph;

    public VertexBulkLoader(AtlasGraph graph) {
        this.graph = graph;
    }

    @Override
    public String name() {
        return "vertexBulkLoad";
    }

    @Override
    public void enrich(SearchEnrichmentContext context) throws AtlasBaseException {
        List<String> vertexIds = context.getOrderedVertexIds();
        if (CollectionUtils.isEmpty(vertexIds)) {
            return;
        }

        String[] ids = vertexIds.toArray(new String[0]);
        Set<AtlasVertex> vertices = graph.getVertices(ids);

        for (AtlasVertex vertex : vertices) {
            String vertexId = vertex.getIdForDisplay();
            Map<String, List<?>> properties = new HashMap<>();

            for (String key : vertex.getPropertyKeys()) {
                Collection<Object> values = vertex.getPropertyValues(key, Object.class);
                if (values != null && !values.isEmpty()) {
                    properties.put(key, new ArrayList<>(values));
                }
            }

            context.addVertexData(vertexId, properties, vertex);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("VertexBulkLoader: loaded {} of {} requested vertices",
                    vertices.size(), vertexIds.size());
        }
    }
}
