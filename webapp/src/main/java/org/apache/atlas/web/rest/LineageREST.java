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

package org.apache.atlas.web.rest;


import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.async.AsyncExecutorService;
import org.apache.atlas.discovery.AtlasLineageService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.lineage.*;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.service.FeatureFlag;
import org.apache.atlas.service.FeatureFlagStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.rest.validator.LineageListRequestValidator;
import org.apache.atlas.web.rest.validator.RequestValidator;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

/**
 * REST interface for an entity's lineage information
 */
@Path("lineage")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class LineageREST {
    private static final Logger LOG = LoggerFactory.getLogger(LineageREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.LineageREST");
    private static final String PREFIX_ATTR = "attr:";

    private final AtlasTypeRegistry typeRegistry;
    private final AtlasLineageService atlasLineageService;
    private final RequestValidator lineageListRequestValidator;
    private final AsyncExecutorService asyncExecutorService;
    private static final String DEFAULT_DIRECTION = "BOTH";
    private static final String DEFAULT_DEPTH = "3";
    private static final String DEFAULT_PAGE = "-1";
    private static final String DEFAULT_RECORD_PER_PAGE = "-1";

    @Context
    private HttpServletRequest httpServletRequest;

    @Inject
    public LineageREST(AtlasTypeRegistry typeRegistry, AtlasLineageService atlasLineageService, LineageListRequestValidator lineageListRequestValidator, AsyncExecutorService asyncExecutorService) {
        this.typeRegistry = typeRegistry;
        this.atlasLineageService = atlasLineageService;
        this.lineageListRequestValidator = lineageListRequestValidator;
        this.asyncExecutorService = asyncExecutorService;
    }

    /**
     * Returns lineage info about entity.
     * @return AtlasLineageInfo
     * @throws AtlasBaseException
     * @HTTP 200 If Lineage exists for the given entity
     * @HTTP 400 Bad query parameters
     * @HTTP 404 If no lineage is found for the given entity
     */
    @POST
    @Path("/{guid}")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public AtlasLineageOnDemandInfo getLineageGraph(@PathParam("guid") String guid,
                                                    LineageOnDemandRequest lineageOnDemandRequest) throws AtlasBaseException {
        if (!AtlasConfiguration.LINEAGE_ON_DEMAND_ENABLED.getBoolean()) {
            LOG.warn("LineageREST: "+ AtlasErrorCode.LINEAGE_ON_DEMAND_NOT_ENABLED.getFormattedErrorMessage(AtlasConfiguration.LINEAGE_ON_DEMAND_ENABLED.getPropertyName()));

            throw new AtlasBaseException(AtlasErrorCode.LINEAGE_ON_DEMAND_NOT_ENABLED, AtlasConfiguration.LINEAGE_ON_DEMAND_ENABLED.getPropertyName());
        }

        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer  perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "LineageREST.getOnDemandLineageGraph(" + guid + "," + lineageOnDemandRequest + ")");
            }

            return atlasLineageService.getAtlasLineageInfo(guid, lineageOnDemandRequest);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Returns lineage list info about entity.
     * @return AtlasLineageListInfo
     * @throws AtlasBaseException
     * @HTTP 200 If Lineage exists for the given entity
     * @HTTP 400 Bad query parameters
     */
    @POST
    @Path("/list")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public AtlasLineageListInfo getLineageList(LineageListRequest lineageListRequest) throws AtlasBaseException {
        lineageListRequestValidator.validate(lineageListRequest);

        String guid = lineageListRequest.getGuid();
        Servlets.validateQueryParamLength("guid", guid);
        AtlasPerfTracer  perf = null;

        RequestContext.get().setIncludeMeanings(!lineageListRequest.isExcludeMeanings());
        RequestContext.get().setIncludeClassifications(!lineageListRequest.isExcludeClassifications());
        RequestContext.get().setIsInvokedByLineage(true);
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG))
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "LineageREST.getLineageList(" + guid + "," + lineageListRequest + ")");

            return atlasLineageService.getLineageListInfoOnDemand(guid, lineageListRequest);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Returns lineage info about entity.
     *
     * @param guid      - unique entity id
     * @param direction - input, output or both
     * @param depth     - number of hops for lineage
     * @return AtlasLineageInfo
     * @throws AtlasBaseException
     * @HTTP 200 If Lineage exists for the given entity
     * @HTTP 400 Bad query parameters
     * @HTTP 404 If no lineage is found for the given entity
     */
    @GET
    @Path("/{guid}")
    @Timed
    public AtlasLineageInfo getLineageGraph(@PathParam("guid") String guid,
                                            @QueryParam("direction") @DefaultValue(DEFAULT_DIRECTION) LineageDirection direction,
                                            @QueryParam("depth") @DefaultValue(DEFAULT_DEPTH) int depth,
                                            @QueryParam("hideProcess") @DefaultValue("false") boolean hideProcess,
                                            @QueryParam("offset") @DefaultValue(DEFAULT_PAGE) int offset,
                                            @QueryParam("limit") @DefaultValue(DEFAULT_RECORD_PER_PAGE) int limit,
                                            @QueryParam("calculateRemainingVertexCounts") @DefaultValue("false") boolean calculateRemainingVertexCounts) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "LineageREST.getLineageGraph(" + guid + "," + direction +
                        "," + depth + ")");
            }

            // Only use async for BOTH direction where parallel INPUT/OUTPUT provides real benefit
            // Single direction async adds thread overhead without performance improvement
            if (isAsyncExecutionEnabled() && direction == LineageDirection.BOTH) {
                return executeLineageAsyncBoth(guid, depth, hideProcess, offset, limit, calculateRemainingVertexCounts);
            }
            return atlasLineageService.getAtlasLineageInfo(guid, direction, depth, hideProcess, offset, limit, calculateRemainingVertexCounts);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Execute lineage query for BOTH direction asynchronously.
     * Runs INPUT and OUTPUT queries in parallel and merges results.
     * This provides real performance benefit as two independent queries run concurrently.
     */
    private AtlasLineageInfo executeLineageAsyncBoth(String guid, int depth,
                                                      boolean hideProcess, int offset, int limit,
                                                      boolean calculateRemainingVertexCounts) throws AtlasBaseException {
        long timeoutMs = asyncExecutorService.getDefaultTimeoutMs();

        // Capture RequestContext for propagation to async threads
        RequestContext parentContext = RequestContext.get();
        RequestContext asyncContext = parentContext.copyForAsync();

        // Parallelize INPUT and OUTPUT queries
        CompletableFuture<AtlasLineageInfo> inputFuture = asyncExecutorService.supplyAsync(
            () -> {
                try {
                    // Propagate RequestContext
                    RequestContext.setCurrentContext(asyncContext.copyForAsync());
                    return atlasLineageService.getAtlasLineageInfo(guid, LineageDirection.INPUT, depth, hideProcess, offset, limit, calculateRemainingVertexCounts);
                } catch (AtlasBaseException e) {
                    throw new CompletionException(e);
                } finally {
                    RequestContext.clear();
                }
            },
            "lineage-input"
        );

        CompletableFuture<AtlasLineageInfo> outputFuture = asyncExecutorService.supplyAsync(
            () -> {
                try {
                    // Propagate RequestContext
                    RequestContext.setCurrentContext(asyncContext.copyForAsync());
                    return atlasLineageService.getAtlasLineageInfo(guid, LineageDirection.OUTPUT, depth, hideProcess, offset, limit, calculateRemainingVertexCounts);
                } catch (AtlasBaseException e) {
                    throw new CompletionException(e);
                } finally {
                    RequestContext.clear();
                }
            },
            "lineage-output"
        );

        // Combine results
        CompletableFuture<AtlasLineageInfo> combinedFuture = inputFuture.thenCombine(outputFuture, this::mergeLineageResults);
        combinedFuture = asyncExecutorService.withTimeout(combinedFuture, Duration.ofMillis(timeoutMs), "lineage-both");

        try {
            return combinedFuture.join();
        } catch (CompletionException ce) {
            return handleLineageException(ce, timeoutMs);
        }
    }

    /**
     * Merge INPUT and OUTPUT lineage results into a BOTH result.
     * Ensures output is identical to sync BOTH direction call.
     */
    private AtlasLineageInfo mergeLineageResults(AtlasLineageInfo inputLineage, AtlasLineageInfo outputLineage) {
        if (inputLineage == null) {
            return outputLineage;
        }
        if (outputLineage == null) {
            return inputLineage;
        }

        // Create merged result
        AtlasLineageInfo merged = new AtlasLineageInfo();
        merged.setBaseEntityGuid(inputLineage.getBaseEntityGuid());
        merged.setLineageDirection(LineageDirection.BOTH);
        merged.setLineageDepth(Math.max(inputLineage.getLineageDepth(), outputLineage.getLineageDepth()));

        // Merge limit/offset (should be same, use input's values)
        merged.setLimit(inputLineage.getLimit());
        merged.setOffset(inputLineage.getOffset());

        // Merge guidEntityMap
        Map<String, AtlasEntityHeader> guidEntityMap = new HashMap<>();
        if (MapUtils.isNotEmpty(inputLineage.getGuidEntityMap())) {
            guidEntityMap.putAll(inputLineage.getGuidEntityMap());
        }
        if (MapUtils.isNotEmpty(outputLineage.getGuidEntityMap())) {
            guidEntityMap.putAll(outputLineage.getGuidEntityMap());
        }
        merged.setGuidEntityMap(guidEntityMap);

        // Merge relations
        Set<AtlasLineageInfo.LineageRelation> relations = new HashSet<>();
        if (inputLineage.getRelations() != null) {
            relations.addAll(inputLineage.getRelations());
        }
        if (outputLineage.getRelations() != null) {
            relations.addAll(outputLineage.getRelations());
        }
        merged.setRelations(relations);

        // Merge upstream/downstream counts and flags
        // INPUT lineage has upstream info, OUTPUT lineage has downstream info
        if (inputLineage.getRemainingUpstreamVertexCount() != null) {
            merged.setRemainingUpstreamVertexCount(inputLineage.getRemainingUpstreamVertexCount());
        }
        merged.setHasMoreUpstreamVertices(inputLineage.getHasMoreUpstreamVertices());

        if (outputLineage.getRemainingDownstreamVertexCount() != null) {
            merged.setRemainingDownstreamVertexCount(outputLineage.getRemainingDownstreamVertexCount());
        }
        merged.setHasMoreDownstreamVertices(outputLineage.getHasMoreDownstreamVertices());

        // Merge vertexChildrenInfo from both
        // INPUT lineage has children info for INPUT direction
        // OUTPUT lineage has children info for OUTPUT direction
        mergeVertexChildrenInfo(merged, inputLineage);
        mergeVertexChildrenInfo(merged, outputLineage);

        return merged;
    }

    /**
     * Merge vertex children info from source into target.
     */
    private void mergeVertexChildrenInfo(AtlasLineageInfo target, AtlasLineageInfo source) {
        if (source == null) {
            return;
        }
        // Copy vertexChildrenInfo from source to target
        Map<String, Map<LineageDirection, Boolean>> sourceChildrenInfo = source.getVertexChildrenInfo();
        if (MapUtils.isNotEmpty(sourceChildrenInfo)) {
            for (Map.Entry<String, Map<LineageDirection, Boolean>> entry : sourceChildrenInfo.entrySet()) {
                String guid = entry.getKey();
                Map<LineageDirection, Boolean> directionMap = entry.getValue();
                if (MapUtils.isNotEmpty(directionMap)) {
                    for (Map.Entry<LineageDirection, Boolean> dirEntry : directionMap.entrySet()) {
                        target.setHasChildrenForDirection(guid,
                            new LineageChildrenInfo(dirEntry.getKey(), dirEntry.getValue()));
                    }
                }
            }
        }
    }

    /**
     * Handle exceptions from async lineage execution.
     */
    private AtlasLineageInfo handleLineageException(CompletionException ce, long timeoutMs) throws AtlasBaseException {
        Throwable cause = ce.getCause();
        if (cause instanceof TimeoutException) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR,
                "Lineage query timed out after " + (timeoutMs / 1000) + "s");
        }
        if (cause instanceof AtlasBaseException) {
            throw (AtlasBaseException) cause;
        }
        throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, cause.getMessage());
    }

    /**
     * Check if async execution is enabled via feature flag.
     */
    private boolean isAsyncExecutionEnabled() {
        try {
            return FeatureFlagStore.evaluate(FeatureFlag.ENABLE_ASYNC_EXECUTION.getKey(), "true");
        } catch (Exception e) {
            LOG.debug("Failed to evaluate async execution feature flag, defaulting to false", e);
            return false;
        }
    }


    /**
     * Returns lineage info about entity.
     *
     * @param request - AtlasLineageRequest
     * @return AtlasLineageInfo
     * @throws AtlasBaseException
     * @HTTP 200 If Lineage exists for the given entity
     * @HTTP 400 Bad query parameters
     * @HTTP 404 If no lineage is found for the given entity
     */
    @POST
    @Path("/getlineage")
    @Timed
    public AtlasLineageInfo getLineageGraph(AtlasLineageRequest request) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        request.performValidation();

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "LineageREST.getLineageGraph(" + request + ")");
            }

            return atlasLineageService.getAtlasLineageInfo(request);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Returns lineage info about entity.
     * <p>
     * In addition to the typeName path parameter, attribute key-value pair(s) can be provided in the following format
     * <p>
     * attr:<attrName>=<attrValue>
     * <p>
     * NOTE: The attrName and attrValue should be unique across entities, eg. qualifiedName
     *
     * @param typeName  - typeName of entity
     * @param direction - input, output or both
     * @param depth     - number of hops for lineage
     * @return AtlasLineageInfo
     * @throws AtlasBaseException
     * @HTTP 200 If Lineage exists for the given entity
     * @HTTP 400 Bad query parameters
     * @HTTP 404 If no lineage is found for the given entity
     */
    @GET
    @Path("/uniqueAttribute/type/{typeName}")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public AtlasLineageInfo getLineageByUniqueAttribute(@PathParam("typeName") String typeName, @QueryParam("direction") @DefaultValue(DEFAULT_DIRECTION) LineageDirection direction,
                                                        @QueryParam("depth") @DefaultValue(DEFAULT_DEPTH) int depth, @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);
        AtlasPerfTracer perf = null;

        try {
            AtlasEntityType entityType = ensureEntityType(typeName);
            Map<String, Object> attributes = getAttributes(servletRequest);
            String guid = AtlasGraphUtilsV2.getGuidByUniqueAttributes(entityType, attributes);

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "LineageREST.getLineageByUniqueAttribute(" + typeName + "," + attributes + "," + direction +
                        "," + depth + ")");
            }

            return atlasLineageService.getAtlasLineageInfo(guid, direction, depth);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private Map<String, Object> getAttributes(HttpServletRequest request) {
        Map<String, Object> attributes = new HashMap<>();

        if (MapUtils.isNotEmpty(request.getParameterMap())) {
            for (Map.Entry<String, String[]> e : request.getParameterMap().entrySet()) {
                String key = e.getKey();

                if (key != null && key.startsWith(PREFIX_ATTR)) {
                    String[] values = e.getValue();
                    String value = values != null && values.length > 0 ? values[0] : null;

                    attributes.put(key.substring(PREFIX_ATTR.length()), value);
                }
            }
        }

        return attributes;
    }

    private AtlasEntityType ensureEntityType(String typeName) throws AtlasBaseException {
        AtlasEntityType ret = typeRegistry.getEntityTypeByName(typeName);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), typeName);
        }

        return ret;
    }

}