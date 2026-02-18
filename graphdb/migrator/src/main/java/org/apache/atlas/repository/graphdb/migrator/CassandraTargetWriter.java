package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/**
 * Writes decoded JanusGraph vertices and edges to the new Cassandra schema (atlas_graph).
 *
 * Thread-safe: accepts vertices from the scanner pipeline via a BlockingQueue,
 * processes them with a configurable writer thread pool.
 *
 * Writes:
 *   - Vertex → vertices table
 *   - Edges → edges_out + edges_in + edges_by_id (logged batch per edge)
 *   - Indexes → vertex_index (GUID, QN+Type) + vertex_property_index (typeName, category)
 */
public class CassandraTargetWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraTargetWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MigratorConfig   config;
    private final MigrationMetrics metrics;
    private final CqlSession       targetSession;
    private final String           ks;

    private PreparedStatement insertVertexStmt;
    private PreparedStatement insertEdgeOutStmt;
    private PreparedStatement insertEdgeInStmt;
    private PreparedStatement insertEdgeByIdStmt;
    private PreparedStatement insertIndexStmt;
    private PreparedStatement insertPropertyIndexStmt;

    // Pipeline: scanner threads enqueue, writer threads dequeue
    private final BlockingQueue<DecodedVertex> queue;
    private final ExecutorService writerPool;
    private volatile boolean scanningComplete = false;

    public CassandraTargetWriter(MigratorConfig config, MigrationMetrics metrics, CqlSession targetSession) {
        this.config        = config;
        this.metrics       = metrics;
        this.targetSession = targetSession;
        this.ks            = config.getTargetCassandraKeyspace();
        this.queue         = new LinkedBlockingQueue<>(config.getQueueCapacity());
        this.writerPool    = Executors.newFixedThreadPool(config.getWriterThreads(), r -> {
            Thread t = new Thread(r);
            t.setName("writer-" + t.getId());
            t.setDaemon(true);
            return t;
        });
    }

    public void init() {
        createSchema();
        prepareStatements();
    }

    private void createSchema() {
        // Create keyspace if not exists
        String strategy = config.getTargetReplicationStrategy();
        int rf = config.getTargetReplicationFactor();
        String dc = config.getTargetCassandraDatacenter();

        String replication;
        if ("SimpleStrategy".equals(strategy)) {
            replication = "{'class': 'SimpleStrategy', 'replication_factor': " + rf + "}";
        } else {
            // NetworkTopologyStrategy: replicate to the configured datacenter
            replication = "{'class': 'NetworkTopologyStrategy', '" + dc + "': " + rf + "}";
        }

        LOG.info("Creating keyspace '{}' with replication: {}", ks, replication);
        targetSession.execute(
            "CREATE KEYSPACE IF NOT EXISTS " + ks +
            " WITH replication = " + replication);

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".vertices (" +
            "  vertex_id text PRIMARY KEY," +
            "  properties text," +
            "  vertex_label text," +
            "  type_name text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp)");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".edges_out (" +
            "  out_vertex_id text," +
            "  edge_label text," +
            "  edge_id text," +
            "  in_vertex_id text," +
            "  properties text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp," +
            "  PRIMARY KEY ((out_vertex_id), edge_label, edge_id)" +
            ") WITH CLUSTERING ORDER BY (edge_label ASC, edge_id ASC)");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".edges_in (" +
            "  in_vertex_id text," +
            "  edge_label text," +
            "  edge_id text," +
            "  out_vertex_id text," +
            "  properties text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp," +
            "  PRIMARY KEY ((in_vertex_id), edge_label, edge_id)" +
            ") WITH CLUSTERING ORDER BY (edge_label ASC, edge_id ASC)");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".edges_by_id (" +
            "  edge_id text PRIMARY KEY," +
            "  out_vertex_id text," +
            "  in_vertex_id text," +
            "  edge_label text," +
            "  properties text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp)");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".vertex_index (" +
            "  index_name text," +
            "  index_value text," +
            "  vertex_id text," +
            "  PRIMARY KEY ((index_name, index_value)))");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".vertex_property_index (" +
            "  index_name text," +
            "  index_value text," +
            "  vertex_id text," +
            "  PRIMARY KEY ((index_name, index_value), vertex_id))");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".schema_registry (" +
            "  property_name text PRIMARY KEY," +
            "  property_class text," +
            "  cardinality text," +
            "  created_at timestamp)");

        LOG.info("Target schema created/verified in keyspace '{}'", ks);
    }

    private void prepareStatements() {
        insertVertexStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".vertices " +
            "(vertex_id, properties, vertex_label, type_name, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)");

        insertEdgeOutStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".edges_out " +
            "(out_vertex_id, edge_label, edge_id, in_vertex_id, properties, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

        insertEdgeInStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".edges_in " +
            "(in_vertex_id, edge_label, edge_id, out_vertex_id, properties, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

        insertEdgeByIdStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".edges_by_id " +
            "(edge_id, out_vertex_id, in_vertex_id, edge_label, properties, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

        insertIndexStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".vertex_index (index_name, index_value, vertex_id) VALUES (?, ?, ?)");

        insertPropertyIndexStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".vertex_property_index (index_name, index_value, vertex_id) VALUES (?, ?, ?)");
    }

    /**
     * Called by scanner threads to enqueue a decoded vertex for writing.
     * Blocks if the queue is full (backpressure).
     */
    public void enqueue(DecodedVertex vertex) {
        try {
            queue.put(vertex);
            // Update queue depth in metrics for progress reporting
            metrics.setQueueDepth(queue.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while enqueuing vertex", e);
        }
    }

    /**
     * Start the writer threads that drain the queue and write to Cassandra.
     */
    public void startWriters() {
        metrics.setQueueCapacity(config.getQueueCapacity());
        for (int i = 0; i < config.getWriterThreads(); i++) {
            writerPool.submit(this::writerLoop);
        }
        LOG.info("Started {} writer threads (queue capacity: {})", config.getWriterThreads(), config.getQueueCapacity());
    }

    /**
     * Signal that scanning is complete. Writers will drain the remaining queue and stop.
     */
    public void signalScanComplete() {
        this.scanningComplete = true;
    }

    /**
     * Wait for all writer threads to finish processing the queue.
     */
    public void awaitCompletion() throws InterruptedException {
        writerPool.shutdown();
        writerPool.awaitTermination(24, TimeUnit.HOURS);
        LOG.info("All writer threads completed");
        LOG.info("DIAG WRITER SUMMARY: edge write attempts: {}, edge write errors: {}",
                 edgeWriteAttempts.get(), edgeWriteErrors.get());
    }

    private void writerLoop() {
        List<DecodedVertex> batch = new ArrayList<>(config.getWriterBatchSize());

        while (true) {
            batch.clear();

            // Drain up to batch size from the queue
            DecodedVertex first = null;
            try {
                if (scanningComplete && queue.isEmpty()) {
                    break;
                }
                first = queue.poll(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            if (first == null) {
                if (scanningComplete && queue.isEmpty()) {
                    break;
                }
                continue;
            }

            batch.add(first);
            queue.drainTo(batch, config.getWriterBatchSize() - 1);

            // Write the batch
            for (DecodedVertex vertex : batch) {
                try {
                    writeVertex(vertex);
                } catch (Exception e) {
                    metrics.incrWriteErrors();
                    LOG.error("Failed to write vertex {}", vertex.getVertexId(), e);
                }
            }
        }
    }

    // Diagnostic counters
    private final java.util.concurrent.atomic.AtomicLong edgeWriteAttempts = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong edgeWriteErrors = new java.util.concurrent.atomic.AtomicLong(0);
    private static final int WRITE_SAMPLE_LOG_LIMIT = 10;

    /**
     * Write a single decoded vertex: vertex row + edges + indexes.
     */
    private void writeVertex(DecodedVertex vertex) {
        String vertexId = vertex.getVertexId();
        Instant now = Instant.now();

        // Serialize properties to JSON
        String propsJson;
        try {
            propsJson = MAPPER.writeValueAsString(vertex.getProperties());
        } catch (Exception e) {
            propsJson = "{}";
            LOG.warn("Failed to serialize properties for vertex {}", vertexId, e);
        }

        // Write vertex
        targetSession.execute(insertVertexStmt.bind(
            vertexId,
            propsJson,
            vertex.getVertexLabel(),
            vertex.getTypeName(),
            vertex.getState(),
            now, now));
        metrics.incrVerticesWritten();

        // Write edges
        int edgeCount = vertex.getOutEdges().size();
        if (edgeCount > 0) {
            long totalAttempts = edgeWriteAttempts.addAndGet(edgeCount);
            if (totalAttempts <= WRITE_SAMPLE_LOG_LIMIT) {
                LOG.info("DIAG: Writing {} edges for vertex {} (typeName={}, guid={})",
                         edgeCount, vertexId, vertex.getTypeName(), vertex.getGuid());
            }
        }
        for (DecodedEdge edge : vertex.getOutEdges()) {
            try {
                writeEdge(edge, now);
            } catch (Exception e) {
                long errCount = edgeWriteErrors.incrementAndGet();
                if (errCount <= WRITE_SAMPLE_LOG_LIMIT) {
                    LOG.error("DIAG: Failed to write edge {} -[{}]-> {}: {}",
                              edge.getOutVertexId(), edge.getLabel(), edge.getInVertexId(), e.toString());
                }
            }
        }
        metrics.incrEdgesWritten(edgeCount);

        // Write indexes
        writeIndexes(vertex);
    }

    private void writeEdge(DecodedEdge edge, Instant now) {
        String edgePropsJson;
        try {
            edgePropsJson = edge.getProperties().isEmpty() ? "{}" :
                MAPPER.writeValueAsString(edge.getProperties());
        } catch (Exception e) {
            edgePropsJson = "{}";
        }

        // Get edge state from edge properties, default to ACTIVE
        Object edgeState = edge.getProperties().get("__state");
        String state = edgeState != null ? edgeState.toString() : "ACTIVE";

        // Write to all 3 edge tables in a logged batch for consistency
        BatchStatement batch = BatchStatement.newInstance(BatchType.LOGGED,
            insertEdgeOutStmt.bind(
                edge.getOutVertexId(), edge.getLabel(), edge.getEdgeId(),
                edge.getInVertexId(), edgePropsJson, state, now, now),
            insertEdgeInStmt.bind(
                edge.getInVertexId(), edge.getLabel(), edge.getEdgeId(),
                edge.getOutVertexId(), edgePropsJson, state, now, now),
            insertEdgeByIdStmt.bind(
                edge.getEdgeId(), edge.getOutVertexId(), edge.getInVertexId(),
                edge.getLabel(), edgePropsJson, state, now, now)
        );

        targetSession.execute(batch);
    }

    /**
     * Build and write composite index entries for a vertex.
     * These are the same indexes that CassandraGraph.buildIndexEntries() creates.
     */
    private void writeIndexes(DecodedVertex vertex) {
        String vertexId = vertex.getVertexId();
        Map<String, Object> props = vertex.getProperties();
        int indexCount = 0;

        // 1:1 unique index: __guid → vertex_id
        String guid = vertex.getGuid();
        if (guid != null) {
            targetSession.execute(insertIndexStmt.bind("__guid_idx", guid, vertexId));
            indexCount++;
        }

        // 1:1 composite index: qualifiedName + typeName → vertex_id
        // After property name normalization, qualifiedName is the canonical key.
        // Fall back to legacy type-qualified name for safety.
        Object qn = props.get("qualifiedName");
        if (qn == null) qn = props.get("Referenceable.qualifiedName");
        String typeName = vertex.getTypeName();
        if (qn != null && typeName != null) {
            targetSession.execute(insertIndexStmt.bind("qn_type_idx", qn + ":" + typeName, vertexId));
            indexCount++;
        }

        // 1:N index: __typeName → vertex_id (for getVertices("__typeName", "Table"))
        if (typeName != null) {
            targetSession.execute(insertPropertyIndexStmt.bind("type_typename_idx", typeName, vertexId));
            indexCount++;
        }

        // 1:N index: TypeDef category lookup (__type:__type_category)
        Object typeVertexType = props.get("__type");
        Object typeCategory = props.get("__type_category");
        if (typeVertexType != null && typeCategory != null) {
            targetSession.execute(insertPropertyIndexStmt.bind(
                "type_category_idx", typeVertexType + ":" + typeCategory, vertexId));
            indexCount++;
        }

        // 1:1 index: TypeDef name lookup (__type:__type_name → vertex_id)
        // Used by AtlasTypeDefGraphStoreV2.findTypeVertexByName():
        //   graph.query().has("__type", "typeSystem").has("__type_name", name).vertices()
        // Must match CassandraGraph.buildIndexEntries() which writes to vertex_index
        if (typeVertexType != null) {
            Object typeDefName = props.get("__type_name");
            if (typeDefName == null) typeDefName = props.get("__type.name");
            if (typeDefName != null) {
                targetSession.execute(insertIndexStmt.bind(
                    "type_typename_idx", typeVertexType + ":" + typeDefName, vertexId));
                indexCount++;
            }
        }

        metrics.incrIndexesWritten(indexCount);
    }

    @Override
    public void close() {
        if (!writerPool.isShutdown()) {
            writerPool.shutdownNow();
        }
    }
}
