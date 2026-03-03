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
package org.apache.atlas.repository.store.graph.v2.lineage;

// Claude code reference
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.lineage.OpenLineageEvent;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Service layer for OpenLineage event operations.
 *
 * This service provides business logic for processing OpenLineage events,
 * including validation, parsing, and persistence.
 */
@Service
public class OpenLineageEventService {
    private static final Logger LOG = LoggerFactory.getLogger(OpenLineageEventService.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final SimpleDateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    static {
        ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private final OpenLineageEventDAO eventDAO;

    @Inject
    public OpenLineageEventService() {
        this.eventDAO = OpenLineageEventDAOCassandraImpl.getInstance();
    }

    /**
     * Process and store an OpenLineage event from a JSON payload.
     *
     * @param eventJson The OpenLineage event as JSON string
     * @throws AtlasBaseException if validation or storage fails
     */
    public void processEvent(String eventJson) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("processOpenLineageEvent");

        try {
            // Validate JSON
            if (StringUtils.isEmpty(eventJson)) {
                throw new AtlasBaseException("Event payload cannot be empty");
            }

            // Parse and validate OpenLineage event
            JsonNode rootNode = objectMapper.readTree(eventJson);

            // Unwrap envelope if event is wrapped in metadata/payload structure
            if (rootNode.has("payload") && rootNode.has("metadata")) {
                LOG.debug("Detected envelope format, unwrapping payload");
                rootNode = rootNode.get("payload");
            }

            // Extract required fields according to OpenLineage spec
            String eventType = getFieldWithDefault(rootNode, "eventType", "GENERIC_OL");
            String eventTime = getRequiredField(rootNode, "eventTime");

            JsonNode runNode = rootNode.get("run");
            if (runNode == null) {
                throw new AtlasBaseException("Missing required field: run");
            }
            String runId = getRequiredField(runNode, "runId");

            JsonNode jobNode = rootNode.get("job");
            String jobName = null;
            String producer = null;

            if (jobNode != null) {
                jobName = getField(jobNode, "name");
            }

            if (rootNode.has("producer")) {
                producer = rootNode.get("producer").asText();
            }

            Date parsedEventTime = parseEventTime(eventTime);
            UUID eventId = buildTimeUuidFromEventTime(parsedEventTime);

            // Create OpenLineageEvent object
            OpenLineageEvent event = new OpenLineageEvent();
            event.setEventId(eventId);
            event.setSource(producer);
            event.setJobName(jobName);
            event.setRunId(runId);
            event.setEventTime(parsedEventTime);
            event.setEvent(rootNode.toString());
            event.setStatus(eventType);

            // Store event
            eventDAO.storeEvent(event);

            LOG.info("Successfully processed OpenLineage event: runId={}, eventType={}, jobName={}",
                    runId, eventType, jobName);
        } catch (AtlasBaseException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Error processing OpenLineage event", e);
            throw new AtlasBaseException("Error processing OpenLineage event: " + e.getMessage(), e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public OpenLineageEventPage getEventsByRunId(String runId, int pageSize, String pagingState) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getOpenLineageEventsByRunIdPaged");
        try {
            if (StringUtils.isEmpty(runId)) {
                throw new AtlasBaseException("runId cannot be empty");
            }
            if (pageSize <= 0) {
                throw new AtlasBaseException("pageSize must be greater than 0");
            }

            return eventDAO.getEventsByRunId(runId, pageSize, pagingState);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public OpenLineageEvent getEventByRunIdAndEventId(String runId, String eventId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getOpenLineageEventByRunIdAndEventId");
        try {
            if (StringUtils.isEmpty(runId)) {
                throw new AtlasBaseException("runId cannot be empty");
            }
            if (StringUtils.isEmpty(eventId)) {
                throw new AtlasBaseException("eventId cannot be empty");
            }

            return eventDAO.getEventByRunIdAndEventId(runId, eventId);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public Iterator<OpenLineageEvent> getEventsById(String runId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getOpenLineageEventsByIdIterator");
        try {
            if (StringUtils.isEmpty(runId)) {
                throw new AtlasBaseException("runId cannot be empty");
            }
            return eventDAO.getEventsById(runId);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    /**
     * Check if the OpenLineage event storage is healthy.
     *
     * @return true if healthy, false otherwise
     */
    public boolean isHealthy() {
        return eventDAO.isHealthy();
    }

    private String getRequiredField(JsonNode node, String fieldName) throws AtlasBaseException {
        JsonNode fieldNode = node.get(fieldName);
        if (fieldNode == null || fieldNode.isNull()) {
            throw new AtlasBaseException("Missing required field: " + fieldName);
        }
        String value = fieldNode.asText();
        if (StringUtils.isEmpty(value)) {
            throw new AtlasBaseException("Required field cannot be empty: " + fieldName);
        }
        return value;
    }

    private String getFieldWithDefault(JsonNode node, String fieldName, String defaultValue) {
        JsonNode fieldNode = node.get(fieldName);
        if (fieldNode == null || fieldNode.isNull()) {
            return defaultValue;
        }
        String value = fieldNode.asText();
        if (StringUtils.isEmpty(value)) {
            return defaultValue;
        }
        return value;
    }

    private String getField(JsonNode node, String fieldName) {
        JsonNode fieldNode = node.get(fieldName);
        if (fieldNode == null || fieldNode.isNull()) {
            return null;
        }
        return fieldNode.asText();
    }

    protected Date parseEventTime(String eventTimeStr) throws AtlasBaseException {
        try {
            // Normalize: replace timezone offset (+00:00) with Z and truncate microseconds to milliseconds
            String normalized = eventTimeStr;

            // Handle timezone offset format: +00:00 or -05:00 -> convert to Z (UTC only) or parse with offset
            if (normalized.matches(".*[+-]\\d{2}:\\d{2}$")) {
                if (normalized.endsWith("+00:00")) {
                    normalized = normalized.substring(0, normalized.length() - 6) + "Z";
                } else {
                    // For non-UTC offsets, use java.time to parse and convert
                    java.time.OffsetDateTime odt = java.time.OffsetDateTime.parse(normalized);
                    return Date.from(odt.toInstant());
                }
            }

            // Truncate microseconds (6 digits) to milliseconds (3 digits) before Z
            if (normalized.matches(".*\\.\\d{4,}Z$")) {
                int dotIdx = normalized.lastIndexOf('.');
                String fractional = normalized.substring(dotIdx + 1, normalized.length() - 1);
                String millis = fractional.length() >= 3 ? fractional.substring(0, 3) : fractional;
                normalized = normalized.substring(0, dotIdx + 1) + millis + "Z";
            }

            return ISO_DATE_FORMAT.parse(normalized);
        } catch (ParseException e) {
            // Try alternative format without milliseconds
            try {
                SimpleDateFormat altFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                altFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                return altFormat.parse(eventTimeStr);
            } catch (ParseException e2) {
                LOG.error("Failed to parse eventTime: {}", eventTimeStr, e2);
                throw new AtlasBaseException("Invalid eventTime format: " + eventTimeStr +
                        ". Expected ISO 8601 format (e.g., 2024-01-15T10:30:00.000Z)", e2);
            }
        }
    }

    protected UUID buildTimeUuidFromEventTime(Date eventTime) throws AtlasBaseException {
        if (eventTime == null) {
            throw new AtlasBaseException("eventTime is required to generate eventId");
        }

        return UUIDs.startOf(eventTime.getTime());
    }
}
