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
 * Stage 3: Load properties for referenced vertices (other end of edges).
 *
 * <p>These are typically Connection, Database, Schema, Term entities referenced
 * through relationship edges found in Stage 2.</p>
 *
 * <p>Fresh fetch every request — no cross-request cache (stale data unacceptable).
 * Uses the same {@code graph.getVertices()} async bulk method as Stage 1.</p>
 */
public class ReferenceVertexLoader implements EnrichmentStage {

    private static final Logger LOG = LoggerFactory.getLogger(ReferenceVertexLoader.class);

    private final AtlasGraph graph;

    public ReferenceVertexLoader(AtlasGraph graph) {
        this.graph = graph;
    }

    @Override
    public String name() {
        return "referenceVertexLoad";
    }

    @Override
    public void enrich(SearchEnrichmentContext context) throws AtlasBaseException {
        Set<String> refIds = context.getReferencedVertexIds();
        if (CollectionUtils.isEmpty(refIds)) {
            return;
        }

        String[] ids = refIds.toArray(new String[0]);
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
            LOG.debug("ReferenceVertexLoader: loaded {} of {} referenced vertices",
                    vertices.size(), refIds.size());
        }
    }
}
