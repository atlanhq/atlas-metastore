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

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.searchpipeline.stages.*;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParams;
import org.apache.atlas.model.glossary.relations.AtlasTermAssignmentHeader;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.EdgeVertexReference;
import org.apache.atlas.repository.VertexEdgePropertiesCache;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery.Result;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.DirectIndexQueryResult;
import org.apache.atlas.repository.graphdb.cassandra.CassandraIndexQuery;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Orchestrates the optimised IndexSearch rendering pipeline for ZeroGraph (CassandraGraph).
 *
 * <p>Replaces {@code EntityDiscoveryService.prepareSearchResult()} when the feature flag
 * {@code atlas.indexsearch.use.optimised.pipeline} is ON. Produces functionally identical
 * output with significantly fewer CQL queries by:</p>
 * <ul>
 *   <li>Extracting vertex IDs from ES _id directly (0 CQL vs N sync CQL)</li>
 *   <li>Fetching edges per-vertex-type (not flat union across all types)</li>
 *   <li>Pre-filtering classification queries (only vertices with tags)</li>
 *   <li>Batch term assignment from cached edges (0 CQL vs N per-entity CQL)</li>
 * </ul>
 *
 * <p>Header assembly delegates to the existing {@code entityRetriever.toAtlasEntityHeader()}
 * by building a {@link VertexEdgePropertiesCache} from pipeline context data. This ensures
 * 100% functional parity for all attribute types (PRIMITIVE, ENUM, OBJECT_ID_TYPE, ARRAY,
 * MAP, STRUCT) without reimplementing complex attribute resolution logic.</p>
 */
@Component
public class IndexSearchResultRenderer {

    private static final Logger LOG = LoggerFactory.getLogger(IndexSearchResultRenderer.class);

    private final VertexIdExtractor     vertexIdExtractor;
    private final List<EnrichmentStage> stages;
    private final StageExecutor         stageExecutor;
    private final EntityGraphRetriever  entityRetriever;

    @Inject
    public IndexSearchResultRenderer(AtlasGraph graph,
                                      AtlasTypeRegistry typeRegistry,
                                      EntityGraphRetriever entityRetriever) {
        this.vertexIdExtractor = new DirectVertexIdExtractor();
        this.stageExecutor     = new StageExecutor();
        this.entityRetriever   = entityRetriever;

        TypeAwareEdgeLabelResolver labelResolver = new TypeAwareEdgeLabelResolver(typeRegistry);

        this.stages = List.of(
                new VertexBulkLoader(graph),
                new TypeAwareEdgeFetcher(graph, labelResolver),
                new ReferenceVertexLoader(graph),
                new SmartClassificationLoader(entityRetriever),
                new TermAssignmentLoader()
        );
    }

    /**
     * Render search results through the optimised pipeline.
     *
     * <p>This is a complete replacement for {@code prepareSearchResult()} on ZeroGraph.
     * Caller must still invoke {@code scrubSearchResults()} after this method returns.</p>
     *
     * @param result          the AtlasSearchResult to populate with entity headers
     * @param queryResult     ES query result containing doc IDs
     * @param resultAttributes client-requested attributes
     * @param searchParams    original search parameters
     */
    public void render(AtlasSearchResult result,
                       DirectIndexQueryResult queryResult,
                       Set<String> resultAttributes,
                       SearchParams searchParams) throws AtlasBaseException {

        AtlasPerfMetrics.MetricRecorder overallMetric =
                RequestContext.get().startMetricRecord("indexSearchResultRenderer.render");
        try {
            List<ESHitResult> esHits = toESHitResults(queryResult);
            renderInternal(result, esHits, resultAttributes, searchParams, true);
        } finally {
            RequestContext.get().endMetricRecord(overallMetric);
        }
    }

    /**
     * Internal rendering logic shared by top-level render and collapse recursion.
     *
     * @param fetchCollapsedResults if true, handle inner_hits collapse. False for recursive calls.
     */
    private void renderInternal(AtlasSearchResult result,
                                List<ESHitResult> esHits,
                                Set<String> resultAttributes,
                                SearchParams searchParams,
                                boolean fetchCollapsedResults) throws AtlasBaseException {

        // Stage 0: Extract vertex IDs from ES _id (0 CQL on ZeroGraph)
        List<String> vertexIds = vertexIdExtractor.extractVertexIds(esHits);

        if (CollectionUtils.isEmpty(vertexIds)) {
            return;
        }

        // Build context
        RequestContext reqCtx = RequestContext.get();
        SearchEnrichmentContext context = new SearchEnrichmentContext(
                esHits,
                vertexIds,
                resultAttributes,
                reqCtx.getRelationAttrsForSearch(),
                searchParams,
                reqCtx.includeClassifications(),
                reqCtx.isIncludeClassificationNames(),
                reqCtx.includeMeanings()
        );

        // Execute stages sequentially with timeout + adaptive retry
        for (EnrichmentStage stage : stages) {
            stageExecutor.execute(stage, context);
        }

        // Build VertexEdgePropertiesCache from context for delegation
        VertexEdgePropertiesCache cache = buildCacheFromContext(context);

        // Classification map (includes empty lists for no-tag vertices)
        Map<String, List<AtlasClassification>> classificationMap = context.getClassificationMap();

        // Save and override includeMeanings — we handle terms ourselves from Stage 5
        boolean originalIncludeMeanings = reqCtx.includeMeanings();
        reqCtx.setIncludeMeanings(false);

        try {
            for (int i = 0; i < vertexIds.size(); i++) {
                String vertexId = vertexIds.get(i);
                AtlasVertex vertex = context.getVertexObject(vertexId);

                if (vertex == null) {
                    LOG.warn("Vertex not found in context for vertexId={}, skipping", vertexId);
                    continue;
                }

                // Delegate to EXISTING toAtlasEntityHeader — 100% functional parity
                AtlasEntityHeader header = entityRetriever.toAtlasEntityHeader(
                        vertex, resultAttributes, cache, classificationMap);

                if (header == null) {
                    continue;
                }

                // Add terms from Stage 5 (was skipped via includeMeanings=false)
                if (originalIncludeMeanings) {
                    List<AtlasTermAssignmentHeader> terms = context.getTermAssignmentMap()
                            .getOrDefault(vertexId, Collections.emptyList());
                    header.setMeanings(terms);
                    header.setMeaningNames(terms.stream()
                            .map(AtlasTermAssignmentHeader::getDisplayText)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList()));
                }

                // Score
                ESHitResult esHit = esHits.get(i);
                if (searchParams.getShowSearchScore()) {
                    result.addEntityScore(header.getGuid(), esHit.getScore());
                }

                // Collapse handling — matches existing prepareSearchResult behaviour
                if (fetchCollapsedResults && esHit.hasInnerHits()) {
                    handleCollapse(header, esHit, resultAttributes, searchParams, result);
                }

                // Highlights, sort
                if (searchParams.getShowSearchMetadata()) {
                    result.addHighlights(header.getGuid(), esHit.getHighlights());
                    result.addSort(header.getGuid(), esHit.getSort());
                } else if (searchParams.getShowHighlights()) {
                    result.addHighlights(header.getGuid(), esHit.getHighlights());
                }

                result.addEntity(header);
            }
        } finally {
            reqCtx.setIncludeMeanings(originalIncludeMeanings);
        }
    }

    /**
     * Handle collapse (inner_hits) for a single entity header.
     * Matches existing behaviour in prepareSearchResult lines 631-661.
     */
    @SuppressWarnings("unchecked")
    private void handleCollapse(AtlasEntityHeader header,
                                ESHitResult esHit,
                                Set<String> resultAttributes,
                                SearchParams searchParams,
                                AtlasSearchResult parentResult) throws AtlasBaseException {

        Map<String, LinkedHashMap> innerHitsMap = esHit.getInnerHits();
        if (innerHitsMap == null || innerHitsMap.isEmpty()) {
            return;
        }

        RequestContext reqCtx = RequestContext.get();
        // Copy — getRelationAttrsForSearch() returns a mutable set, clear() would clear our reference
        Set<String> savedRelationAttrs = new HashSet<>(reqCtx.getRelationAttrsForSearch());

        Map<String, AtlasSearchResult> collapse = new HashMap<>();

        for (String collapseKey : innerHitsMap.keySet()) {
            AtlasSearchResult collapseResult = new AtlasSearchResult();
            collapseResult.setSearchParameters(parentResult.getSearchParameters());

            // Determine collapse attributes (from searchParams or use parent's)
            Set<String> collapseAttrs;
            if (searchParams.getCollapseAttributes() != null) {
                collapseAttrs = new HashSet<>(searchParams.getCollapseAttributes());
            } else {
                collapseAttrs = resultAttributes;
            }

            // Handle collapse relation attributes
            if (searchParams.getCollapseRelationAttributes() != null) {
                reqCtx.getRelationAttrsForSearch().clear();
                reqCtx.setRelationAttrsForSearch(searchParams.getCollapseRelationAttributes());
            }

            // Parse inner hits into ESHitResult list
            List<ESHitResult> collapseHits = parseInnerHits(innerHitsMap.get(collapseKey));

            if (collapseHits != null && !collapseHits.isEmpty()) {
                // Recursive render — fetchCollapsedResults=false (no nested collapse)
                renderInternal(collapseResult, collapseHits, collapseAttrs, searchParams, false);
            }

            // Extract approximate count from inner hits
            try {
                LinkedHashMap innerHitResponse = innerHitsMap.get(collapseKey);
                Map<String, LinkedHashMap> hitsWrapper = AtlasType.fromJson(
                        AtlasType.toJson(innerHitResponse.get("hits")), Map.class);
                if (hitsWrapper != null && hitsWrapper.get("total") != null) {
                    Integer approximateCount = (Integer) hitsWrapper.get("total").get("value");
                    collapseResult.setApproximateCount(approximateCount);
                }
            } catch (Exception e) {
                LOG.debug("Failed to extract approximate count for collapse key {}", collapseKey, e);
            }

            collapseResult.setSearchParameters(null);
            collapse.put(collapseKey, collapseResult);
        }

        // Restore relation attributes
        if (searchParams.getCollapseRelationAttributes() != null) {
            reqCtx.getRelationAttrsForSearch().clear();
            reqCtx.setRelationAttrsForSearch(savedRelationAttrs);
        }

        if (!collapse.isEmpty()) {
            header.setCollapse(collapse);
        }
    }

    /**
     * Parse inner hits response into a list of ESHitResult.
     * Inner hits have the same structure as top-level ES hits.
     */
    @SuppressWarnings("unchecked")
    private List<ESHitResult> parseInnerHits(LinkedHashMap innerHitResponse) {
        if (innerHitResponse == null) {
            return Collections.emptyList();
        }

        try {
            Map<String, LinkedHashMap> hitsWrapper = AtlasType.fromJson(
                    AtlasType.toJson(innerHitResponse.get("hits")), Map.class);
            if (hitsWrapper == null) {
                return Collections.emptyList();
            }

            List<LinkedHashMap> hits = AtlasType.fromJson(
                    AtlasType.toJson(hitsWrapper.get("hits")), List.class);
            if (hits == null) {
                return Collections.emptyList();
            }

            List<ESHitResult> results = new ArrayList<>(hits.size());
            for (LinkedHashMap hit : hits) {
                results.add(new ESHitResult(hit));
            }
            return results;
        } catch (Exception e) {
            LOG.warn("Failed to parse inner hits for collapse", e);
            return Collections.emptyList();
        }
    }

    /**
     * Build {@link VertexEdgePropertiesCache} from pipeline context data for delegation
     * to existing {@code toAtlasEntityHeader()}.
     */
    private VertexEdgePropertiesCache buildCacheFromContext(SearchEnrichmentContext context) {
        VertexEdgePropertiesCache cache = new VertexEdgePropertiesCache();

        // Add all vertex properties (result + referenced)
        for (Map.Entry<String, Map<String, List<?>>> entry : context.getVertexProperties().entrySet()) {
            cache.addVertexProperties(entry.getKey(), entry.getValue());
        }

        // Add AtlasVertex objects
        cache.addVertices(context.getVertexObjects());

        // Add edges per vertex
        int edgeLimit = RequestContext.get().isInvokedByProduct()
                ? AtlasConfiguration.MIN_EDGES_SUPER_VERTEX.getInt()
                : AtlasConfiguration.MAX_EDGES_SUPER_VERTEX.getInt();

        for (Map.Entry<String, List<EdgeVertexReference>> entry : context.getVertexEdges().entrySet()) {
            String vertexId = entry.getKey();
            for (EdgeVertexReference edgeRef : entry.getValue()) {
                cache.addEdgeLabelToVertexIds(vertexId, edgeRef.getEdgeLabel(), edgeRef, edgeLimit);
            }
        }

        return cache;
    }

    /**
     * Convert DirectIndexQueryResult's iterator to List of ESHitResult.
     * Only handles CassandraIndexQuery.ResultImplDirect — this pipeline only runs on ZeroGraph.
     */
    @SuppressWarnings("unchecked")
    private List<ESHitResult> toESHitResults(DirectIndexQueryResult queryResult) {
        Iterator<Result> iterator = queryResult.getIterator();
        if (iterator == null) {
            return Collections.emptyList();
        }

        List<ESHitResult> hits = new ArrayList<>();
        while (iterator.hasNext()) {
            Result r = iterator.next();
            if (r instanceof CassandraIndexQuery.ResultImplDirect) {
                LinkedHashMap<String, Object> rawHit =
                        ((CassandraIndexQuery.ResultImplDirect) r).getRawHit();
                hits.add(new ESHitResult(rawHit));
            } else {
                LOG.warn("Unexpected Result type in optimised pipeline: {}", r.getClass().getName());
            }
        }
        return hits;
    }
}
