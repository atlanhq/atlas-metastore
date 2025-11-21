package org.apache.atlas.repository.store.graph.v2.bulk;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.store.graph.v2.tags.CassandraTagConfig.CASSANDRA_HOSTNAME_PROPERTY;
import static org.apache.atlas.repository.store.graph.v2.tags.CassandraTagConfig.CASSANDRA_PORT;
import static org.apache.atlas.repository.store.graph.v2.tags.CassandraTagConfig.CASSANDRA_REPLICATION_FACTOR_PROPERTY;
import org.apache.atlas.AtlasConfiguration;


@Repository
public class IngestionDAOCassandraImpl implements IngestionDAO, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(IngestionDAOCassandraImpl.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("IngestionDAOCassandraImpl");

    private static final String RAW_INGEST_TABLE = "raw_ingest_payloads";
    private static final String STATUS_TABLE = "ingest_status";
    public static final String DEFAULT_HOST = "localhost";
    public static final String DATACENTER = "datacenter1";

    private CqlSession cassSession;
    private PreparedStatement insertRawStmt;
    private PreparedStatement insertStatusStmt;
    private PreparedStatement updateStatusStmt;
    private PreparedStatement updateStatusResultStmt;
    private PreparedStatement getStatusStmt;
    private PreparedStatement getPayloadStmt;

    private String keyspace;

    @PostConstruct
    public void init() throws AtlasBaseException {
        try {
            String hostname = ApplicationProperties.get().getString(CASSANDRA_HOSTNAME_PROPERTY, DEFAULT_HOST);
            
            DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(5))
                    .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(5))
                    .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(5))
                    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 4)
                    .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, 2)
                    .withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL, Duration.ofSeconds(30))
                    .build();

            cassSession = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(hostname, CASSANDRA_PORT))
                    .withConfigLoader(configLoader)
                    .withLocalDatacenter(DATACENTER)
                    .build();

            this.keyspace = AtlasConfiguration.INGESTION_DUMP_KEYSPACE.getString();
            initializeSchema();
            prepareStatements();
            
            LOG.info("IngestionDAOCassandraImpl initialized successfully");
        } catch (Exception e) {
            LOG.error("Failed to initialize IngestionDAOCassandraImpl", e);
            throw new AtlasBaseException("Failed to initialize IngestionDAOCassandraImpl", e);
        }
    }

    private void initializeSchema() throws AtlasBaseException {
        try {
            Map<String, String> replicationConfig = Map.of("class", "SimpleStrategy", "replication_factor", ApplicationProperties.get().getString(CASSANDRA_REPLICATION_FACTOR_PROPERTY, "1"));
            String replicationConfigString = replicationConfig.entrySet().stream()
                    .map(entry -> String.format("'%s': '%s'", entry.getKey(), entry.getValue()))
                    .collect(Collectors.joining(", "));

            String createKeyspaceQuery = String.format(
                    "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {%s} AND durable_writes = true;",
                    this.keyspace, replicationConfigString);
            cassSession.execute(SimpleStatement.builder(createKeyspaceQuery).setConsistencyLevel(DefaultConsistencyLevel.ALL).build());
            LOG.info("Ensured keyspace {} exists", this.keyspace);
        } catch (Exception e) {
            throw new AtlasBaseException("Failed to create keyspace", e);
        }

        String createRawTable = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                        "ingest_id uuid PRIMARY KEY, " +
                        "payload blob, " +
                        "request_options blob, " +
                        "request_context blob, " +
                        "created_at timestamp" +
                        ") WITH compaction = {'class': 'SizeTieredCompactionStrategy'};",
                this.keyspace, RAW_INGEST_TABLE);
        
        cassSession.execute(SimpleStatement.builder(createRawTable).setConsistencyLevel(DefaultConsistencyLevel.ALL).build());
        LOG.info("Ensured table {}.{} exists", keyspace, RAW_INGEST_TABLE);

        String createStatusTable = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                        "ingest_id uuid PRIMARY KEY, " +
                        "status text, " +
                        "result_payload blob, " +
                        "error_msg text, " +
                        "updated_at timestamp" +
                        ") WITH compaction = {'class': 'SizeTieredCompactionStrategy'};",
                this.keyspace, STATUS_TABLE);

        cassSession.execute(SimpleStatement.builder(createStatusTable).setConsistencyLevel(DefaultConsistencyLevel.ALL).build());
        LOG.info("Ensured table {}.{} exists", keyspace, STATUS_TABLE);
    }

    private void prepareStatements() {
        insertRawStmt = cassSession.prepare(String.format(
                "INSERT INTO %s.%s (ingest_id, payload, request_options, request_context, created_at) VALUES (?, ?, ?, ?, ?)",
                this.keyspace, RAW_INGEST_TABLE));

        insertStatusStmt = cassSession.prepare(String.format(
                "INSERT INTO %s.%s (ingest_id, status, updated_at) VALUES (?, ?, ?)",
                this.keyspace, STATUS_TABLE));

        updateStatusStmt = cassSession.prepare(String.format(
                "UPDATE %s.%s SET status = ?, updated_at = ?, error_msg = ? WHERE ingest_id = ?",
                this.keyspace, STATUS_TABLE));

        updateStatusResultStmt = cassSession.prepare(String.format(
                "UPDATE %s.%s SET status = ?, updated_at = ?, result_payload = ?, error_msg = ? WHERE ingest_id = ?",
                this.keyspace, STATUS_TABLE));

        getStatusStmt = cassSession.prepare(String.format(
                "SELECT ingest_id, status, result_payload, error_msg, updated_at FROM %s.%s WHERE ingest_id = ?",
                this.keyspace, STATUS_TABLE));

        getPayloadStmt = cassSession.prepare(String.format(
                "SELECT payload, request_options, request_context FROM %s.%s WHERE ingest_id = ?",
                this.keyspace, RAW_INGEST_TABLE));
    }

    @Override
    public void save(String requestId, byte[] payload, byte[] requestOptions, byte[] requestContext) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("IngestionDAO.save");
        try {
            Instant now = Instant.now();
            UUID uuid = UUID.fromString(requestId);
            
            // Insert into Raw Table
            BoundStatement rawBound = insertRawStmt.bind()
                    .setUuid("ingest_id", uuid)
                    .setByteBuffer("payload", ByteBuffer.wrap(payload))
                    .setByteBuffer("request_options", ByteBuffer.wrap(requestOptions))
                    .setByteBuffer("request_context", ByteBuffer.wrap(requestContext))
                    .setInstant("created_at", now);
            cassSession.execute(rawBound);

            // Insert into Status Table
            BoundStatement statusBound = insertStatusStmt.bind()
                    .setUuid("ingest_id", uuid)
                    .setString("status", "PENDING")
                    .setInstant("updated_at", now);
            cassSession.execute(statusBound);

        } catch (Exception e) {
            LOG.error("Error saving ingestion request: {}", requestId, e);
            throw new AtlasBaseException("Error saving ingestion request", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public void updateStatus(String requestId, String status, String errorMessage) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("IngestionDAO.updateStatus");
        try {
            BoundStatement bound = updateStatusStmt.bind()
                    .setString("status", status)
                    .setInstant("updated_at", Instant.now())
                    .setString("error_msg", errorMessage)
                    .setUuid("ingest_id", UUID.fromString(requestId));

            cassSession.execute(bound);
        } catch (Exception e) {
            LOG.error("Error updating status for request: {}", requestId, e);
            throw new AtlasBaseException("Error updating status", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public void updateStatus(String requestId, String status, byte[] resultSummary, String errorMessage) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("IngestionDAO.updateStatusWithResult");
        try {
            ByteBuffer resultBuffer = resultSummary != null ? ByteBuffer.wrap(resultSummary) : null;
            BoundStatement bound = updateStatusResultStmt.bind()
                    .setString("status", status)
                    .setInstant("updated_at", Instant.now())
                    .setByteBuffer("result_payload", resultBuffer)
                    .setString("error_msg", errorMessage)
                    .setUuid("ingest_id", UUID.fromString(requestId));

            cassSession.execute(bound);
        } catch (Exception e) {
            LOG.error("Error updating status for request: {}", requestId, e);
            throw new AtlasBaseException("Error updating status", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public IngestionRequest getStatus(String requestId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("IngestionDAO.getStatus");
        try {
            BoundStatement bound = getStatusStmt.bind().setUuid("ingest_id", UUID.fromString(requestId));
            ResultSet rs = cassSession.execute(bound);
            Row row = rs.one();

            if (row == null) {
                return null;
            }

            IngestionRequest request = new IngestionRequest();
            request.setRequestId(row.getUuid("ingest_id").toString());
            request.setStatus(row.getString("status"));
            
            ByteBuffer resultBuffer = row.getByteBuffer("result_payload");
            if (resultBuffer != null) {
                byte[] resultBytes = new byte[resultBuffer.remaining()];
                resultBuffer.get(resultBytes);
                request.setResultSummary(resultBytes);
            }
            
            request.setErrorMessage(row.getString("error_msg"));
            Instant updatedAt = row.getInstant("updated_at");
            if (updatedAt != null) {
                request.setUpdatedAt(updatedAt.toEpochMilli());
            }

            return request;
        } catch (Exception e) {
            LOG.error("Error fetching ingestion status: {}", requestId, e);
            throw new AtlasBaseException("Error fetching ingestion status", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public IngestionPayloadAndContext getPayloadAndContext(String requestId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("IngestionDAO.getPayloadAndContext");
        try {
            BoundStatement bound = getPayloadStmt.bind().setUuid("ingest_id", UUID.fromString(requestId));
            ResultSet rs = cassSession.execute(bound);
            Row row = rs.one();

            if (row == null) {
                return null;
            }

            IngestionPayloadAndContext result = new IngestionPayloadAndContext();
            
            ByteBuffer payloadBuffer = row.getByteBuffer("payload");
            if (payloadBuffer != null) {
                byte[] payloadBytes = new byte[payloadBuffer.remaining()];
                payloadBuffer.get(payloadBytes);
                result.setPayload(payloadBytes);
            }

            ByteBuffer optionsBuffer = row.getByteBuffer("request_options");
            if (optionsBuffer != null) {
                byte[] optionsBytes = new byte[optionsBuffer.remaining()];
                optionsBuffer.get(optionsBytes);
                result.setRequestOptions(optionsBytes);
            }

            ByteBuffer contextBuffer = row.getByteBuffer("request_context");
            if (contextBuffer != null) {
                byte[] contextBytes = new byte[contextBuffer.remaining()];
                contextBuffer.get(contextBytes);
                result.setRequestContext(contextBytes);
            }
            
            return result;
        } catch (Exception e) {
            LOG.error("Error fetching payload and context: {}", requestId, e);
            throw new AtlasBaseException("Error fetching payload and context", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public byte[] getPayload(String requestId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("IngestionDAO.getPayload");
        try {
            BoundStatement bound = getPayloadStmt.bind().setUuid("ingest_id", UUID.fromString(requestId));
            ResultSet rs = cassSession.execute(bound);
            Row row = rs.one();

            if (row == null) {
                return null;
            }

            ByteBuffer payloadBuffer = row.getByteBuffer("payload");
            if (payloadBuffer != null) {
                byte[] payloadBytes = new byte[payloadBuffer.remaining()];
                payloadBuffer.get(payloadBytes);
                return payloadBytes;
            }
            return null;
        } catch (Exception e) {
            LOG.error("Error fetching payload: {}", requestId, e);
            throw new AtlasBaseException("Error fetching payload", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }
    
    @Override
    public String getRequestOptions(String requestId) throws AtlasBaseException {
        throw new UnsupportedOperationException("This method is deprecated, use getPayloadAndContext instead");
    }

    @Override
    @PreDestroy
    public void close() {
        if (cassSession != null && !cassSession.isClosed()) {
            LOG.info("Closing Cassandra session for IngestionDAO");
            cassSession.close();
        }
    }
}
