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
import org.apache.atlas.repository.store.graph.v2.lineage.OpenLineageEventPage;
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
import java.util.List;

/**
 * REST API for OpenLineage event ingestion and retrieval.
 *
 * This API follows the OpenLineage HTTP transport specification for receiving
 * lineage events from data producers and storing them in Cassandra.
 *
 * Endpoints:
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
     * Retrieve all events for a specific run.
     *
     * @param runId The run ID to query events for
     * @return List of OpenLineage events for the given run
     * @throws AtlasBaseException if query fails
     */
    @GET
    @Path("/runs/{runId}")
    @Timed
    public OpenLineageEventsResponse getEventsByRunId(@PathParam("runId") String runId,
                                                      @QueryParam("pageSize") Integer pageSize,
                                                      @QueryParam("pagingState") String pagingState) throws AtlasBaseException {
        Servlets.validateQueryParamLength("runId", runId);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "OpenLineageREST.getEventsByRunId(" + runId + ")");
            }

            if (StringUtils.isEmpty(runId)) {
                return OpenLineageEventsResponse.error("runId cannot be empty");
            }
            int effectivePageSize = pageSize != null ? pageSize : 25;
            if (effectivePageSize <= 0) {
                return OpenLineageEventsResponse.error("pageSize must be greater than 0");
            }

            OpenLineageEventPage page = eventService.getEventsByRunId(runId, effectivePageSize, pagingState);
            List<OpenLineageEvent> events = page.getEvents();

            LOG.debug("Retrieved events for runId={}", runId);
            return OpenLineageEventsResponse.success(runId, events, page.getPageSize(), page.getPagingState());

        } catch (AtlasBaseException e) {
            LOG.error("Failed to retrieve events for runId={}", runId, e);
            return OpenLineageEventsResponse.error(e.getMessage());
        } catch (Exception e) {
            LOG.error("Unexpected error retrieving events for runId={}", runId, e);
            return OpenLineageEventsResponse.error("Internal server error: " + e.getMessage());
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Retrieve a specific event by runId and eventId.
     *
     * @param runId   The run ID to query events for
     * @param eventId The event ID to query
     * @return OpenLineage event for the given runId and eventId
     * @throws AtlasBaseException if query fails
     */
    @GET
    @Path("/runs/{runId}/events/{eventId}")
    @Timed
    public OpenLineageEventsResponse getEventByRunIdAndEventId(@PathParam("runId") String runId,
                                                               @PathParam("eventId") String eventId) throws AtlasBaseException {
        Servlets.validateQueryParamLength("runId", runId);
        Servlets.validateQueryParamLength("eventId", eventId);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "OpenLineageREST.getEventByRunIdAndEventId(" + runId + "," + eventId + ")");
            }

            if (StringUtils.isEmpty(runId)) {
                return OpenLineageEventsResponse.error("runId cannot be empty");
            }
            if (StringUtils.isEmpty(eventId)) {
                return OpenLineageEventsResponse.error("eventId cannot be empty");
            }

            OpenLineageEvent event = eventService.getEventByRunIdAndEventId(runId, eventId);
            if (event == null) {
                return OpenLineageEventsResponse.error("Event not found");
            }

            return OpenLineageEventsResponse.success(runId, List.of(event), 1, null);
        } catch (AtlasBaseException e) {
            LOG.error("Failed to retrieve event for runId={}, eventId={}", runId, eventId, e);
            return OpenLineageEventsResponse.error(e.getMessage());
        } catch (Exception e) {
            LOG.error("Unexpected error retrieving event for runId={}, eventId={}", runId, eventId, e);
            return OpenLineageEventsResponse.error("Internal server error: " + e.getMessage());
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
    public OpenLineageEventsResponse healthCheck() {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "OpenLineageREST.healthCheck()");
            }

            boolean healthy = eventService.isHealthy();

            if (healthy) {
                return OpenLineageEventsResponse.successMessage("OpenLineage event storage is healthy");
            } else {
                return OpenLineageEventsResponse.error("OpenLineage event storage is unhealthy");
            }
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    public static class OpenLineageEventsResponse {
        private final boolean success;
        private final String message;
        private final String error;
        private final String runId;
        private final int eventCount;
        private final List<OpenLineageEvent> events;
        private final Integer pageSize;
        private final String nextPagingState;

        private OpenLineageEventsResponse(boolean success,
                                          String message,
                                          String error,
                                          String runId,
                                          int eventCount,
                                          List<OpenLineageEvent> events,
                                          Integer pageSize,
                                          String nextPagingState) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.runId = runId;
            this.eventCount = eventCount;
            this.events = events;
            this.pageSize = pageSize;
            this.nextPagingState = nextPagingState;
        }

        public static OpenLineageEventsResponse success(String runId,
                                                        List<OpenLineageEvent> events,
                                                        Integer pageSize,
                                                        String nextPagingState) {
            List<OpenLineageEvent> safeEvents = events != null ? events : List.of();
            return new OpenLineageEventsResponse(true, null, null, runId, safeEvents.size(), safeEvents, pageSize, nextPagingState);
        }

        public static OpenLineageEventsResponse successMessage(String message) {
            return new OpenLineageEventsResponse(true, message, null, null, 0, List.of(), null, null);
        }

        public static OpenLineageEventsResponse error(String errorMessage) {
            return new OpenLineageEventsResponse(false, null, errorMessage, null, 0, List.of(), null, null);
        }

        public boolean isSuccess() {
            return success;
        }

        public String getMessage() {
            return message;
        }

        public String getError() {
            return error;
        }

        public String getRunId() {
            return runId;
        }

        public int getEventCount() {
            return eventCount;
        }

        public List<OpenLineageEvent> getEvents() {
            return events;
        }

        public Integer getPageSize() {
            return pageSize;
        }

        public String getNextPagingState() {
            return nextPagingState;
        }
    }
}
