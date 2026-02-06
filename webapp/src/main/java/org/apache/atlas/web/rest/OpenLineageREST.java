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
package org.apache.atlas.web.rest;

// Claude code reference
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.lineage.OpenLineageEvent;
import org.apache.atlas.repository.store.graph.v2.lineage.OpenLineageEventService;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST API for OpenLineage event ingestion and retrieval.
 *
 * This API follows the OpenLineage HTTP transport specification for receiving
 * lineage events from data producers and storing them in Cassandra.
 *
 * Endpoints:
 * - POST /api/atlas/v2/openlineage - Ingest a single OpenLineage event
 * - POST /api/atlas/v2/openlineage/batch - Ingest multiple OpenLineage events
 * - GET /api/atlas/v2/openlineage/runs/{runId} - Retrieve events for a specific run
 * - GET /api/atlas/v2/openlineage/health - Health check
 */
@Path("openlineage")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class OpenLineageREST {
    private static final Logger LOG = LoggerFactory.getLogger(OpenLineageREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.OpenLineageREST");

    private final OpenLineageEventService eventService;

    @Inject
    public OpenLineageREST(OpenLineageEventService eventService) {
        this.eventService = eventService;
    }

    /**
     * Ingest a single OpenLineage event.
     *
     * This endpoint receives OpenLineage events as JSON payloads and stores them
     * in Cassandra for lineage tracking and analysis.
     *
     * @param eventJson The OpenLineage event as a JSON string
     * @return HTTP 201 if successful, 400 for validation errors, 500 for server errors
     * @throws AtlasBaseException if event processing fails
     */
    @POST
    @Timed
    public Response ingestEvent(String eventJson) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "OpenLineageREST.ingestEvent()");
            }

            if (StringUtils.isEmpty(eventJson)) {
                LOG.warn("Received empty event payload");
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(createErrorResponse("Event payload cannot be empty"))
                        .build();
            }

            eventService.processEvent(eventJson);

            LOG.info("Successfully ingested OpenLineage event");
            return Response.status(Response.Status.CREATED)
                    .entity(createSuccessResponse("Event ingested successfully"))
                    .build();

        } catch (AtlasBaseException e) {
            LOG.error("Failed to ingest OpenLineage event", e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(createErrorResponse(e.getMessage()))
                    .build();
        } catch (Exception e) {
            LOG.error("Unexpected error ingesting OpenLineage event", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(createErrorResponse("Internal server error: " + e.getMessage()))
                    .build();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Ingest multiple OpenLineage events in a batch.
     *
     * This endpoint allows batch ingestion of OpenLineage events for improved throughput.
     * Individual event failures do not prevent other events from being processed.
     *
     * @param eventsJson List of OpenLineage events as JSON strings
     * @return HTTP 200 with processing results, 400 for validation errors
     * @throws AtlasBaseException if batch processing fails
     */
    @POST
    @Path("/batch")
    @Timed
    public Response ingestBatchEvents(List<String> eventsJson) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "OpenLineageREST.ingestBatchEvents(count=" +
                        (eventsJson != null ? eventsJson.size() : 0) + ")");
            }

            if (eventsJson == null || eventsJson.isEmpty()) {
                LOG.warn("Received empty batch payload");
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(createErrorResponse("Event batch cannot be empty"))
                        .build();
            }

            Map<Integer, String> errors = eventService.processBatchEvents(eventsJson);

            Map<String, Object> response = new HashMap<>();
            response.put("totalEvents", eventsJson.size());
            response.put("successfulEvents", eventsJson.size() - errors.size());
            response.put("failedEvents", errors.size());

            if (!errors.isEmpty()) {
                response.put("errors", errors);
                LOG.warn("Batch ingestion completed with {} errors out of {} events",
                        errors.size(), eventsJson.size());
            } else {
                LOG.info("Successfully ingested batch of {} OpenLineage events", eventsJson.size());
            }

            return Response.ok(response).build();

        } catch (AtlasBaseException e) {
            LOG.error("Failed to process OpenLineage event batch", e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(createErrorResponse(e.getMessage()))
                    .build();
        } catch (Exception e) {
            LOG.error("Unexpected error processing OpenLineage event batch", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(createErrorResponse("Internal server error: " + e.getMessage()))
                    .build();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Retrieve all events for a specific run.
     *
     * @param runId The run ID to query events for
     * @return List of OpenLineage events for the given run
     * @throws AtlasBaseException if query fails
     */
    @GET
    @Path("/runs/{runId}")
    @Timed
    public Response getEventsByRunId(@PathParam("runId") String runId) throws AtlasBaseException {
        Servlets.validateQueryParamLength("runId", runId);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "OpenLineageREST.getEventsByRunId(" + runId + ")");
            }

            if (StringUtils.isEmpty(runId)) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(createErrorResponse("runId cannot be empty"))
                        .build();
            }

            List<OpenLineageEvent> events = eventService.getEventsByRunId(runId);

            Map<String, Object> response = new HashMap<>();
            response.put("runId", runId);
            response.put("eventCount", events.size());
            response.put("events", events);

            LOG.debug("Retrieved {} events for runId={}", events.size(), runId);
            return Response.ok(response).build();

        } catch (AtlasBaseException e) {
            LOG.error("Failed to retrieve events for runId={}", runId, e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(createErrorResponse(e.getMessage()))
                    .build();
        } catch (Exception e) {
            LOG.error("Unexpected error retrieving events for runId={}", runId, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(createErrorResponse("Internal server error: " + e.getMessage()))
                    .build();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Health check endpoint for OpenLineage event storage.
     *
     * @return HTTP 200 if healthy, 503 if unhealthy
     */
    @GET
    @Path("/health")
    @Timed
    public Response healthCheck() {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "OpenLineageREST.healthCheck()");
            }

            boolean healthy = eventService.isHealthy();

            if (healthy) {
                return Response.ok(createSuccessResponse("OpenLineage event storage is healthy")).build();
            } else {
                return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                        .entity(createErrorResponse("OpenLineage event storage is unhealthy"))
                        .build();
            }
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private Map<String, Object> createSuccessResponse(String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", message);
        return response;
    }

    private Map<String, Object> createErrorResponse(String errorMessage) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("error", errorMessage);
        return response;
    }
}
