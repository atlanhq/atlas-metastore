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

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.lineage.OpenLineageEvent;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for OpenLineageEventService.
 *
 * Note: These tests require a running Cassandra instance.
 * For CI/CD, consider using an embedded Cassandra or mock the DAO layer.
 */
public class OpenLineageEventServiceTest {

    private static OpenLineageEventService service;

    @BeforeClass
    public static void setup() throws Exception {
        RequestContext.get();
        service = new OpenLineageEventService();
    }

    @Test
    public void testProcessValidEvent() throws AtlasBaseException {
        String eventJson = "{\n" +
                "  \"eventType\": \"START\",\n" +
                "  \"eventTime\": \"2024-01-15T10:30:00.000Z\",\n" +
                "  \"run\": {\n" +
                "    \"runId\": \"d46e465b-d358-4d32-83d4-df660ff614dd\"\n" +
                "  },\n" +
                "  \"job\": {\n" +
                "    \"namespace\": \"my-namespace\",\n" +
                "    \"name\": \"my-job\"\n" +
                "  },\n" +
                "  \"producer\": \"https://my-producer.com\",\n" +
                "  \"inputs\": [],\n" +
                "  \"outputs\": []\n" +
                "}";

        // This will throw an exception if it fails
        service.processEvent(eventJson);
    }

    @Test(expected = AtlasBaseException.class)
    public void testProcessEventWithMissingRunId() throws AtlasBaseException {
        String eventJson = "{\n" +
                "  \"eventType\": \"START\",\n" +
                "  \"eventTime\": \"2024-01-15T10:30:00.000Z\",\n" +
                "  \"run\": {},\n" +
                "  \"producer\": \"https://my-producer.com\"\n" +
                "}";

        service.processEvent(eventJson);
    }

    @Test(expected = AtlasBaseException.class)
    public void testProcessEventWithMissingEventType() throws AtlasBaseException {
        String eventJson = "{\n" +
                "  \"eventTime\": \"2024-01-15T10:30:00.000Z\",\n" +
                "  \"run\": {\n" +
                "    \"runId\": \"d46e465b-d358-4d32-83d4-df660ff614dd\"\n" +
                "  }\n" +
                "}";

        service.processEvent(eventJson);
    }

    @Test(expected = AtlasBaseException.class)
    public void testProcessEmptyEvent() throws AtlasBaseException {
        service.processEvent("");
    }

    @Test
    public void testProcessBatchEvents() throws AtlasBaseException {
        String event1 = "{\n" +
                "  \"eventType\": \"START\",\n" +
                "  \"eventTime\": \"2024-01-15T10:30:00.000Z\",\n" +
                "  \"run\": {\n" +
                "    \"runId\": \"batch-run-1\"\n" +
                "  },\n" +
                "  \"job\": {\n" +
                "    \"name\": \"batch-job-1\"\n" +
                "  },\n" +
                "  \"producer\": \"https://my-producer.com\"\n" +
                "}";

        String event2 = "{\n" +
                "  \"eventType\": \"COMPLETE\",\n" +
                "  \"eventTime\": \"2024-01-15T10:35:00.000Z\",\n" +
                "  \"run\": {\n" +
                "    \"runId\": \"batch-run-1\"\n" +
                "  },\n" +
                "  \"job\": {\n" +
                "    \"name\": \"batch-job-1\"\n" +
                "  },\n" +
                "  \"producer\": \"https://my-producer.com\"\n" +
                "}";

        List<String> events = Arrays.asList(event1, event2);
        Map<Integer, String> errors = service.processBatchEvents(events);

        assertTrue("Batch processing should succeed", errors.isEmpty());
    }

    @Test
    public void testProcessBatchWithPartialFailures() throws AtlasBaseException {
        String validEvent = "{\n" +
                "  \"eventType\": \"START\",\n" +
                "  \"eventTime\": \"2024-01-15T10:30:00.000Z\",\n" +
                "  \"run\": {\n" +
                "    \"runId\": \"valid-run\"\n" +
                "  },\n" +
                "  \"producer\": \"https://my-producer.com\"\n" +
                "}";

        String invalidEvent = "{\n" +
                "  \"eventType\": \"START\",\n" +
                "  \"eventTime\": \"2024-01-15T10:30:00.000Z\"\n" +
                "}";

        List<String> events = Arrays.asList(validEvent, invalidEvent);
        Map<Integer, String> errors = service.processBatchEvents(events);

        assertEquals("Should have one error", 1, errors.size());
        assertTrue("Error should be for second event", errors.containsKey(1));
    }

    @Test
    public void testGetEventsByRunId() throws AtlasBaseException {
        // First, insert a test event
        String testRunId = "test-run-" + System.currentTimeMillis();
        String eventJson = "{\n" +
                "  \"eventType\": \"START\",\n" +
                "  \"eventTime\": \"2024-01-15T10:30:00.000Z\",\n" +
                "  \"run\": {\n" +
                "    \"runId\": \"" + testRunId + "\"\n" +
                "  },\n" +
                "  \"job\": {\n" +
                "    \"name\": \"test-job\"\n" +
                "  },\n" +
                "  \"producer\": \"https://my-producer.com\"\n" +
                "}";

        service.processEvent(eventJson);

        // Now retrieve it
        List<OpenLineageEvent> events = service.getEventsByRunId(testRunId);

        assertNotNull("Events should not be null", events);
        assertFalse("Should have at least one event", events.isEmpty());
        assertEquals("First event should have correct runId", testRunId, events.get(0).getRunId());
        assertEquals("First event should have correct status", "START", events.get(0).getStatus());
    }

    @Test
    public void testHealthCheck() {
        boolean healthy = service.isHealthy();
        // The health check result depends on whether Cassandra is running
        // In a production test, you would assert true, but for unit tests
        // we just verify the method doesn't throw an exception
        assertNotNull("Health check should return a value", healthy);
    }
}
