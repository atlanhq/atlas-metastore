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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

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

            // Extract required fields according to OpenLineage spec
            String eventType = getRequiredField(rootNode, "eventType");
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
            UUID eventId = buildTimeUuidFromRunAndTime(runId, parsedEventTime);

            // Create OpenLineageEvent object
            OpenLineageEvent event = new OpenLineageEvent();
            event.setEventId(eventId);
            event.setSource(producer);
            event.setJobName(jobName);
            event.setRunId(runId);
            event.setEventTime(parsedEventTime);
            event.setEvent(eventJson);
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

    /**
     * Retrieve all events for a specific run.
     *
     * @param runId The run ID to query
     * @return List of OpenLineage events
     * @throws AtlasBaseException if query fails
     */
    public List<OpenLineageEvent> getEventsByRunId(String runId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getOpenLineageEventsByRunId");
        try {
            if (StringUtils.isEmpty(runId)) {
                throw new AtlasBaseException("runId cannot be empty");
            }

            return eventDAO.getEventsByRunId(runId);
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

    private String getField(JsonNode node, String fieldName) {
        JsonNode fieldNode = node.get(fieldName);
        if (fieldNode == null || fieldNode.isNull()) {
            return null;
        }
        return fieldNode.asText();
    }

    protected Date parseEventTime(String eventTimeStr) throws AtlasBaseException {
        try {
            // Try ISO 8601 format
            return ISO_DATE_FORMAT.parse(eventTimeStr);
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

    protected UUID buildTimeUuidFromRunAndTime(String runId, Date eventTime) throws AtlasBaseException {
        if (StringUtils.isEmpty(runId) || eventTime == null) {
            throw new AtlasBaseException("runId and eventTime are required to generate eventId");
        }

        UUID baseUuid = UUIDs.startOf(eventTime.getTime());
        long lsb = buildLsbFromRunId(runId);
        return new UUID(baseUuid.getMostSignificantBits(), lsb);
    }

    protected long buildLsbFromRunId(String runId) throws AtlasBaseException {
        byte[] hash = sha256Bytes(runId);
        long lsb = ByteBuffer.wrap(hash).getLong();
        // Set IETF variant (10xx...)
        lsb &= 0x3fffffffffffffffL;
        lsb |= 0x8000000000000000L;
        return lsb;
    }

    protected byte[] sha256Bytes(String value) throws AtlasBaseException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new AtlasBaseException("SHA-256 is not available for eventId generation", e);
        }
    }
}
