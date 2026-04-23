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
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;

/**
 * Stage 4: Load classifications only for vertices that actually have them.
 *
 * <p>Pre-filters using {@code __classificationNames} / {@code __traitNames} vertex
 * property from Stage 1 data. Only queries TagDAO for vertices with non-empty
 * classification names. Typically reduces classification CQL by ~75%.</p>
 *
 * <p>Populates {@code Collections.emptyList()} for vertices WITHOUT tags in the
 * classificationMap. This prevents the fallback at EntityGraphRetriever line 1938
 * where {@code classificationCache.get(vertexId)} returning null triggers per-entity
 * sync CQL via {@code handleGetAllClassifications()}.</p>
 *
 * <p>Delegates actual fetch to {@code entityRetriever.prefetchClassifications()},
 * reusing existing TagDAO integration and TagV2/auth checks.</p>
 */
public class SmartClassificationLoader implements EnrichmentStage {

    private static final Logger LOG = LoggerFactory.getLogger(SmartClassificationLoader.class);

    private final EntityGraphRetriever entityRetriever;

    public SmartClassificationLoader(EntityGraphRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
    }

    @Override
    public String name() {
        return "smartClassificationLoad";
    }

    @Override
    public void enrich(SearchEnrichmentContext context) throws AtlasBaseException {
        if (!context.isIncludeClassifications()) {
            return;
        }

        // Pre-filter: find vertices with tags from Stage 1 vertex properties
        List<AtlasVertex> verticesWithTags = new ArrayList<>();
        for (String vertexId : context.getOrderedVertexIds()) {
            String clsNames = context.getVertexProperty(vertexId, CLASSIFICATION_NAMES_KEY, String.class);
            String traitNames = context.getVertexProperty(vertexId, TRAIT_NAMES_PROPERTY_KEY, String.class);

            if (StringUtils.isNotEmpty(clsNames) || StringUtils.isNotEmpty(traitNames)) {
                AtlasVertex vertex = context.getVertexObject(vertexId);
                if (vertex != null) {
                    verticesWithTags.add(vertex);
                }
            } else {
                // Populate EMPTY LIST for vertices without tags
                // Prevents fallback CQL at EntityGraphRetriever:1938
                context.getClassificationMap().put(vertexId, Collections.emptyList());
            }
        }

        if (verticesWithTags.isEmpty()) {
            LOG.debug("SmartClassificationLoader: 0 of {} vertices have classifications, skipping TagDAO call",
                    context.getOrderedVertexIds().size());
            return;
        }

        // Delegate to existing prefetchClassifications (handles TagV2/auth checks)
        Map<String, List<AtlasClassification>> result = entityRetriever.prefetchClassifications(verticesWithTags);
        if (result != null) {
            context.getClassificationMap().putAll(result);
        }

        // Ensure ALL result vertices have an entry (even if prefetch returned partial)
        for (String vertexId : context.getOrderedVertexIds()) {
            context.getClassificationMap().putIfAbsent(vertexId, Collections.emptyList());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("SmartClassificationLoader: {}/{} vertices have classifications",
                    verticesWithTags.size(), context.getOrderedVertexIds().size());
        }
    }
}
