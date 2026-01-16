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
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.lineage.OpenLineageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Cassandra implementation of OpenLineageEventDAO.
 *
 * This DAO stores OpenLineage events in Cassandra using the events_by_run table.
 * Schema:
 * - Keyspace: openlineage
 * - Table: events_by_run
 * - Primary Key: ((runID), eventTime DESC, event_id)
 *
 * Following patterns from TagDAOCassandraImpl with singleton pattern,
 * retry logic, and connection pooling.
 */
public class OpenLineageEventDAOCassandraImpl implements OpenLineageEventDAO, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(OpenLineageEventDAOCassandraImpl.class);

    // Configuration constants
    private static final String KEYSPACE = "openlineage";
    private static final String TABLE_NAME = "events_by_run";
    private static final int CASSANDRA_PORT = 9042;
    private static final String DEFAULT_HOST = "localhost";
    private static final String DATACENTER = "datacenter1";
    private static final String CASSANDRA_HOSTNAME_PROPERTY = "atlas.cassandra.hostname";
    private static final String CASSANDRA_REPLICATION_FACTOR_PROPERTY = "atlas.cassandra.replication.factor";

    // Retry and timeout configuration
    private static final int MAX_RETRIES = 3;
    private static final Duration INITIAL_BACKOFF = Duration.ofMillis(100);
    private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(30);

    private static final OpenLineageEventDAOCassandraImpl INSTANCE;

    /**
     * Static initializer block to create the singleton instance.
     */
    static {
        try {
            INSTANCE = new OpenLineageEventDAOCassandraImpl();
        } catch (AtlasBaseException e) {
            LOG.error("FATAL: Failed to initialize OpenLineageEventDAOCassandraImpl singleton", e);
            throw new RuntimeException("Could not initialize OpenLineageEventDAO Cassandra implementation", e);
        }
    }

    /**
     * Provides the global point of access to the OpenLineageEventDAOCassandraImpl instance.
     *
     * @return The single instance of OpenLineageEventDAOCassandraImpl.
     */
    public static OpenLineageEventDAOCassandraImpl getInstance() {
        return INSTANCE;
    }

    private final CqlSession cassSession;

    // Prepared Statements
    private final PreparedStatement insertEventStmt;
    private final PreparedStatement getEventsByRunIdStmt;
    private final PreparedStatement getEventByRunIdAndEventIdStmt;
    private final PreparedStatement healthCheckStmt;

    private OpenLineageEventDAOCassandraImpl() throws AtlasBaseException {
        try {
            String hostname = ApplicationProperties.get().getString(CASSANDRA_HOSTNAME_PROPERTY, DEFAULT_HOST);
            Map<String, String> replicationConfig = Map.of(
                    "class", "SimpleStrategy",
                    "replication_factor", ApplicationProperties.get().getString(CASSANDRA_REPLICATION_FACTOR_PROPERTY, "3")
            );

            DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, CONNECTION_TIMEOUT)
                    .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, CONNECTION_TIMEOUT)
                    .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, CONNECTION_TIMEOUT)
                    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, calculateOptimalLocalPoolSize())
                    .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, calculateOptimalRemotePoolSize())
                    .withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL)
                    .build();

            cassSession = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(hostname, CASSANDRA_PORT))
                    .withConfigLoader(configLoader)
                    .withLocalDatacenter(DATACENTER)
                    .build();

            initializeSchema(replicationConfig);

            // Prepare statements
            insertEventStmt = prepare(String.format(
                    "INSERT INTO %s.%s (event_id, source, jobName, runID, eventTime, event, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    KEYSPACE, TABLE_NAME));

            getEventsByRunIdStmt = prepare(String.format(
                    "SELECT event_id, source, jobName, runID, eventTime, event, status FROM %s.%s WHERE runID = ?",
                    KEYSPACE, TABLE_NAME));

            getEventByRunIdAndEventIdStmt = prepare(String.format(
                    "SELECT event_id, source, jobName, runID, eventTime, event, status FROM %s.%s WHERE runID = ? AND eventTime = ? AND event_id = ?",
                    KEYSPACE, TABLE_NAME));

            healthCheckStmt = prepare("SELECT release_version FROM system.local");

            LOG.info("OpenLineageEventDAO initialized successfully");
        } catch (Exception e) {
            LOG.error("Failed to initialize OpenLineageEventDAO", e);
            throw new AtlasBaseException("Failed to initialize OpenLineageEventDAO", e);
        }
    }

    private PreparedStatement prepare(String cql) {
        return cassSession.prepare(SimpleStatement.builder(cql)
                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                .build());
    }

    private void initializeSchema(Map<String, String> replicationConfig) throws AtlasBaseException {
        String replicationConfigString = replicationConfig.entrySet().stream()
                .map(entry -> String.format("'%s': '%s'", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(", "));

        // Create keyspace
        String createKeyspaceQuery = String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {%s} AND durable_writes = true;",
                KEYSPACE, replicationConfigString);
        executeWithRetry(SimpleStatement.builder(createKeyspaceQuery)
                .setConsistencyLevel(DefaultConsistencyLevel.ALL)
                .build());
        LOG.info("Ensured keyspace {} exists", KEYSPACE);

        // Create table
        String createTableQuery = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                        "event_id uuid, " +
                        "source text, " +
                        "jobName text, " +
                        "runID text, " +
                        "eventTime timestamp, " +
                        "event text, " +
                        "status text, " +
                        "PRIMARY KEY ((runID), eventTime, event_id)" +
                        ") WITH CLUSTERING ORDER BY (eventTime DESC);",
                KEYSPACE, TABLE_NAME);
        executeWithRetry(SimpleStatement.builder(createTableQuery)
                .setConsistencyLevel(DefaultConsistencyLevel.ALL)
                .build());
        LOG.info("Ensured table {}.{} exists", KEYSPACE, TABLE_NAME);
    }

    @Override
    public void storeEvent(OpenLineageEvent event) throws AtlasBaseException {
        if (event == null) {
            throw new AtlasBaseException("OpenLineageEvent cannot be null");
        }

        // Validate required fields
        if (event.getRunId() == null || event.getRunId().isEmpty()) {
            throw new AtlasBaseException("runId is required");
        }
        if (event.getEventTime() == null) {
            throw new AtlasBaseException("eventTime is required");
        }

        try {
            UUID eventId = event.getEventId() != null ? UUID.fromString(event.getEventId()) : UUID.randomUUID();

            BoundStatement bound = insertEventStmt.bind()
                    .setUuid("event_id", eventId)
                    .setString("source", event.getSource())
                    .setString("jobName", event.getJobName())
                    .setString("runID", event.getRunId())
                    .setInstant("eventTime", event.getEventTime().toInstant())
                    .setString("event", event.getEvent())
                    .setString("status", event.getStatus());

            executeWithRetry(bound);
            LOG.info("Successfully stored OpenLineage event: runId={}, eventId={}, status={}",
                    event.getRunId(), eventId, event.getStatus());
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid event_id format: {}", event.getEventId(), e);
            throw new AtlasBaseException("Invalid event_id format: " + event.getEventId(), e);
        } catch (Exception e) {
            LOG.error("Error storing OpenLineage event: runId={}", event.getRunId(), e);
            throw new AtlasBaseException("Error storing OpenLineage event", e);
        }
    }

    @Override
    public List<OpenLineageEvent> getEventsByRunId(String runId) throws AtlasBaseException {
        if (runId == null || runId.isEmpty()) {
            throw new AtlasBaseException("runId is required");
        }

        List<OpenLineageEvent> events = new ArrayList<>();
        try {
            BoundStatement bound = getEventsByRunIdStmt.bind(runId);
            ResultSet rs = executeWithRetry(bound);

            for (Row row : rs) {
                events.add(mapRowToEvent(row));
            }

            LOG.debug("Retrieved {} events for runId={}", events.size(), runId);
            return events;
        } catch (Exception e) {
            LOG.error("Error retrieving events for runId={}", runId, e);
            throw new AtlasBaseException("Error retrieving events for runId: " + runId, e);
        }
    }

    @Override
    public OpenLineageEvent getEventByRunIdAndEventId(String runId, String eventId) throws AtlasBaseException {
        if (runId == null || runId.isEmpty()) {
            throw new AtlasBaseException("runId is required");
        }
        if (eventId == null || eventId.isEmpty()) {
            throw new AtlasBaseException("eventId is required");
        }

        try {
            // Note: This query requires knowing the eventTime, which is part of the clustering key
            // For a production system, you might want to add a secondary index or
            // use a different query pattern
            LOG.warn("getEventByRunIdAndEventId requires eventTime for efficient query. " +
                    "Consider using getEventsByRunId and filtering in application layer.");

            // For now, we'll get all events for the run and filter
            List<OpenLineageEvent> events = getEventsByRunId(runId);
            return events.stream()
                    .filter(e -> eventId.equals(e.getEventId()))
                    .findFirst()
                    .orElse(null);
        } catch (Exception e) {
            LOG.error("Error retrieving event: runId={}, eventId={}", runId, eventId, e);
            throw new AtlasBaseException("Error retrieving event", e);
        }
    }

    @Override
    public boolean isHealthy() {
        try {
            Instant start = Instant.now();

            // Execute a lightweight query against system.local
            ResultSet rs = cassSession.execute(healthCheckStmt.bind());

            // Verify we get at least one row back
            boolean hasResults = rs.iterator().hasNext();

            Duration duration = Duration.between(start, Instant.now());

            if (hasResults) {
                LOG.debug("Cassandra health check successful in {}ms", duration.toMillis());
                return true;
            } else {
                LOG.warn("Cassandra health check failed - no results returned from system.local");
                return false;
            }

        } catch (DriverTimeoutException e) {
            LOG.warn("Cassandra health check failed due to timeout: {}", e.getMessage());
            return false;
        } catch (NoHostAvailableException e) {
            LOG.warn("Cassandra health check failed - no hosts available: {}", e.getMessage());
            return false;
        } catch (Exception e) {
            LOG.warn("Cassandra health check failed due to unexpected error: {}", e.getMessage(), e);
            return false;
        }
    }

    private OpenLineageEvent mapRowToEvent(Row row) {
        OpenLineageEvent event = new OpenLineageEvent();
        event.setEventId(row.getUuid("event_id").toString());
        event.setSource(row.getString("source"));
        event.setJobName(row.getString("jobName"));
        event.setRunId(row.getString("runID"));
        event.setEventTime(Date.from(row.getInstant("eventTime")));
        event.setEvent(row.getString("event"));
        event.setStatus(row.getString("status"));
        return event;
    }

    private <T extends Statement<T>> ResultSet executeWithRetry(Statement<T> statement) throws AtlasBaseException {
        int retryCount = 0;
        Exception lastException;

        while (true) {
            try {
                return cassSession.execute(statement);
            } catch (DriverTimeoutException | WriteTimeoutException | NoHostAvailableException e) {
                lastException = e;
                retryCount++;
                LOG.warn("Retry attempt {} for statement execution due to exception: {}", retryCount, e.toString());
                if (retryCount >= MAX_RETRIES) {
                    break;
                }
                try {
                    long backoff = INITIAL_BACKOFF.toMillis() * (long) Math.pow(2, retryCount - 1);
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new AtlasBaseException("Interrupted during retry backoff", ie);
                }
            }
        }
        LOG.error("Failed to execute statement after {} retries", MAX_RETRIES, lastException);
        throw new AtlasBaseException("Failed to execute statement after " + MAX_RETRIES + " retries", lastException);
    }

    private int calculateOptimalLocalPoolSize() {
        return Math.min(Math.max((int) Math.ceil(Runtime.getRuntime().availableProcessors() * 0.75), 4), 8);
    }

    private int calculateOptimalRemotePoolSize() {
        return Math.max(calculateOptimalLocalPoolSize() / 2, 2);
    }

    @Override
    public void close() {
        if (cassSession != null && !cassSession.isClosed()) {
            cassSession.close();
            LOG.info("OpenLineageEventDAO Cassandra session closed");
        }
    }
}
