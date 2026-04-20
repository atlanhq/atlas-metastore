package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writes decoded JanusGraph vertices and edges to the new Cassandra schema (atlas_graph).
 *
 * Thread-safe: accepts vertices from the scanner pipeline via a BlockingQueue,
 * processes them with a configurable writer thread pool.
 *
 * Write strategy:
 *   - All mutations (vertex row, indexes, edges) are fired as individual
 *     async statements — no cross-partition batching.
 *   - Each edge writes to 3 tables (edges_out, edges_in, edges_by_id) with
 *     different partition keys, making batches counterproductive (Cassandra
 *     warns on unlogged batches spanning >10 partitions, and logged batches
 *     add batchlog overhead for no benefit).
 *   - Statements are fired asynchronously with a Semaphore cap per thread,
 *     allowing pipelining without overwhelming the coordinator.
 *   - Atomicity is ensured by idempotent writes + token-range resume on failure.
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
    private PreparedStatement insertClaimStmt;
    private PreparedStatement selectClaimStmt;
    private PreparedStatement insertTypeDefStmt;
    private PreparedStatement insertTypeDefByCategoryStmt;
    private PreparedStatement insertEdgeIndexStmt;

    // Pipeline: scanner threads enqueue, writer threads dequeue
    private final BlockingQueue<QueueItem> queue;
    private final ExecutorService writerPool;
    private volatile boolean scanningComplete = false;

    // Diagnostic counters
    private final AtomicLong writeAttempts = new AtomicLong(0);
    private final AtomicLong writeErrors   = new AtomicLong(0);
    private static final int WRITE_SAMPLE_LOG_LIMIT = 10;
    private final AtomicLong claimRedirectCount = new AtomicLong(0);
    private final AtomicLong edgeFallbackCount  = new AtomicLong(0);
    private final ConcurrentMap<Long, String> vertexIdOverrides = new ConcurrentHashMap<>();

    // Optional parallel ES indexers — when set, vertices/edges are indexed into ES during Phase 1
    private volatile ParallelEsIndexer parallelEsIndexer;
    private volatile ParallelEsIndexer parallelEsEdgeIndexer;

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

    /** Set the parallel ES vertex indexer to pipeline ES indexing during Phase 1. */
    public void setParallelEsIndexer(ParallelEsIndexer indexer) {
        this.parallelEsIndexer = indexer;
    }

    /** Set the parallel ES edge indexer to pipeline ES edge indexing during Phase 1. */
    public void setParallelEsEdgeIndexer(ParallelEsIndexer indexer) {
        this.parallelEsEdgeIndexer = indexer;
    }

    private void createSchema() {
        String strategy = config.getTargetReplicationStrategy();
        int rf = config.getTargetReplicationFactor();
        String dc = config.getTargetCassandraDatacenter();

        String replication;
        if ("SimpleStrategy".equals(strategy)) {
            replication = "{'class': 'SimpleStrategy', 'replication_factor': " + rf + "}";
        } else {
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

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".entity_claims (" +
            "  identity_key text PRIMARY KEY," +
            "  vertex_id text," +
            "  claimed_at timestamp," +
            "  source text)");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".type_definitions (" +
            "  type_name text PRIMARY KEY," +
            "  type_category text," +
            "  vertex_id text," +
            "  created_at timestamp," +
            "  modified_at timestamp)");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".type_definitions_by_category (" +
            "  type_category text," +
            "  type_name text," +
            "  vertex_id text," +
            "  PRIMARY KEY ((type_category), type_name)" +
            ") WITH CLUSTERING ORDER BY (type_name ASC)");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".edge_index (" +
            "  index_name text," +
            "  index_value text," +
            "  edge_id text," +
            "  PRIMARY KEY ((index_name, index_value)))");

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

        insertClaimStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".entity_claims (identity_key, vertex_id, claimed_at, source) VALUES (?, ?, ?, ?) IF NOT EXISTS");

        selectClaimStmt = targetSession.prepare(
            "SELECT vertex_id FROM " + ks + ".entity_claims WHERE identity_key = ?");

        insertTypeDefStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".type_definitions " +
            "(type_name, type_category, vertex_id, created_at, modified_at) VALUES (?, ?, ?, ?, ?)");

        insertTypeDefByCategoryStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".type_definitions_by_category " +
            "(type_category, type_name, vertex_id) VALUES (?, ?, ?)");

        insertEdgeIndexStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".edge_index (index_name, index_value, edge_id) VALUES (?, ?, ?)");
    }

    /**
     * Called by scanner threads to enqueue a decoded vertex or edge chunk for writing.
     * Blocks if the queue is full (backpressure).
     */
    public void enqueue(QueueItem item) {
        try {
            queue.put(item);
            metrics.setQueueDepth(queue.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while enqueuing item", e);
        }
    }

    public void startWriters() {
        metrics.setQueueCapacity(config.getQueueCapacity());
        for (int i = 0; i < config.getWriterThreads(); i++) {
            writerPool.submit(this::writerLoop);
        }
        LOG.info("Started {} writer threads (queue capacity: {}, maxInflight/thread: {})",
                 config.getWriterThreads(), config.getQueueCapacity(),
                 config.getMaxInflightPerThread());
    }

    public void signalScanComplete() {
        this.scanningComplete = true;
    }

    public void awaitCompletion() throws InterruptedException {
        writerPool.shutdown();
        writerPool.awaitTermination(24, TimeUnit.HOURS);
        LOG.info("All writer threads completed");
        LOG.debug("DIAG WRITER SUMMARY: write attempts: {}, write errors: {}",
                  writeAttempts.get(), writeErrors.get());
    }

    /**
     * Writer loop: drains items from the queue, builds UNLOGGED batches,
     * and fires them asynchronously with a Semaphore cap on in-flight requests.
     *
     * Dispatches based on item type:
     *   - DecodedVertex → full vertex + indexes + edges
     *   - EdgeChunk → only edge table INSERTs (for super vertex continuation)
     */
    private void writerLoop() {
        Semaphore inflight = new Semaphore(config.getMaxInflightPerThread());

        while (true) {
            QueueItem item;
            try {
                if (scanningComplete && queue.isEmpty()) break;
                item = queue.poll(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            if (item == null) {
                if (scanningComplete && queue.isEmpty()) break;
                continue;
            }

            metrics.setQueueDepth(queue.size());

            try {
                if (item instanceof DecodedVertex) {
                    writeVertexAsync((DecodedVertex) item, inflight);
                } else if (item instanceof EdgeChunk) {
                    writeEdgeChunkAsync((EdgeChunk) item, inflight);
                }
            } catch (Exception e) {
                metrics.incrWriteErrors();
                LOG.error("Failed to process item {}", item, e);
            }
        }

        // Drain: wait for all in-flight async writes to complete
        try {
            inflight.acquire(config.getMaxInflightPerThread());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Write a vertex to target Cassandra asynchronously.
     * All statements (vertex, indexes, edges) are fired individually — no cross-partition
     * batching. Each edge writes to edges_out, edges_in, edges_by_id (3 different partition
     * keys), so batching them would span many partitions with no atomicity benefit.
     * Idempotent writes + token-range resume handle partial failures.
     */
    private void writeVertexAsync(DecodedVertex vertex, Semaphore inflight) throws InterruptedException {
        List<BoundStatement> allStatements = buildVertexStatements(vertex);

        for (BoundStatement stmt : allStatements) {
            fireAsyncStatement(stmt, inflight, vertex.getVertexId());
        }

        // Count metrics optimistically — async writes to local Cassandra nearly always succeed
        metrics.incrVerticesWritten();
        metrics.incrEdgesWritten(vertex.getOutEdges().size());

        // Pipeline ES indexing: enqueue the vertex for ES bulk indexing in parallel
        if (parallelEsIndexer != null) {
            String vertexId = resolveVertexId(vertex);
            String propsJson;
            try {
                propsJson = MAPPER.writeValueAsString(vertex.getProperties());
            } catch (Exception e) {
                propsJson = "{}";
            }
            parallelEsIndexer.enqueue(vertexId, propsJson, vertex.getTypeName(), vertex.getState());
        }

        // Pipeline ES edge indexing: enqueue edges for ES edge index
        if (parallelEsEdgeIndexer != null) {
            enqueueEdgesForEsIndexing(vertex.getOutEdges());
        }
    }

    /**
     * Write an edge chunk (from a super vertex that was split during scanning).
     * Only writes edge table INSERTs — no vertex row or indexes.
     * Each edge's 3 table inserts are fired individually (no cross-partition batching).
     */
    private void writeEdgeChunkAsync(EdgeChunk chunk, Semaphore inflight) throws InterruptedException {
        List<DecodedEdge> edges = chunk.getEdges();
        Instant now = Instant.now();

        for (DecodedEdge edge : edges) {
            List<BoundStatement> edgeStmts = new ArrayList<>();
            buildEdgeStatementsWithResolvedOutVertex(edge, now, edgeStmts, chunk.getResolvedVertexId());

            for (BoundStatement stmt : edgeStmts) {
                fireAsyncStatement(stmt, inflight, chunk.getResolvedVertexId());
            }
        }

        metrics.incrEdgesWritten(edges.size());

        // Pipeline ES edge indexing for this chunk
        if (parallelEsEdgeIndexer != null) {
            enqueueEdgesForEsIndexing(edges);
        }
    }

    /**
     * Build edge statements using a pre-resolved out-vertex ID (for EdgeChunks).
     */
    private void buildEdgeStatementsWithResolvedOutVertex(DecodedEdge edge, Instant now,
                                                           List<BoundStatement> stmts,
                                                           String outVertexId) {
        String edgePropsJson;
        try {
            edgePropsJson = edge.getProperties().isEmpty() ? "{}" :
                MAPPER.writeValueAsString(edge.getProperties());
        } catch (Exception e) {
            edgePropsJson = "{}";
        }

        Object edgeState = edge.getProperties().get("__state");
        String state = edgeState != null ? edgeState.toString() : "ACTIVE";

        String inVertexId = resolveVertexId(edge.getInVertexJgId());
        String edgeId;
        if (config.getIdStrategy() == IdStrategy.HASH_IDENTITY) {
            edgeId = DeterministicIdUtil.edgeIdFromIdentity(outVertexId, edge.getLabel(), inVertexId);
        } else {
            edgeId = DeterministicIdUtil.edgeIdFromJg(edge.getJgRelationId(), config.getIdStrategy());
        }

        stmts.add(insertEdgeOutStmt.bind(
            outVertexId, edge.getLabel(), edgeId,
            inVertexId, edgePropsJson, state, now, now));
        stmts.add(insertEdgeInStmt.bind(
            inVertexId, edge.getLabel(), edgeId,
            outVertexId, edgePropsJson, state, now, now));
        stmts.add(insertEdgeByIdStmt.bind(
            edgeId, outVertexId, inVertexId,
            edge.getLabel(), edgePropsJson, state, now, now));

        Object relGuid = edge.getProperties().get("_r__guid");
        if (relGuid != null) {
            stmts.add(insertEdgeIndexStmt.bind("_r__guid_idx", String.valueOf(relGuid), edgeId));
        }
    }

    /**
     * Enqueue edges for ES edge indexing via the parallel ES edge indexer.
     */
    private void enqueueEdgesForEsIndexing(List<DecodedEdge> edges) {
        for (DecodedEdge edge : edges) {
            try {
                Map<String, Object> edgeDoc = buildEsEdgeDoc(edge);
                String edgeDocJson = MAPPER.writeValueAsString(edgeDoc);
                // Use the edge ID as the ES doc ID, and edge label as "typeName" for filtering
                parallelEsEdgeIndexer.enqueueEdge(edge.getEdgeId(), edgeDocJson);
            } catch (Exception e) {
                LOG.trace("Failed to enqueue edge {} for ES indexing", edge.getEdgeId(), e);
            }
        }
    }

    /**
     * Build an ES document for an edge. Includes all edge properties plus metadata fields.
     */
    private Map<String, Object> buildEsEdgeDoc(DecodedEdge edge) {
        Map<String, Object> doc = new LinkedHashMap<>();

        // Add all edge properties with dot→underscore sanitization
        for (Map.Entry<String, Object> entry : edge.getProperties().entrySet()) {
            String key = entry.getKey();
            if (key.contains(".")) {
                key = key.replace('.', '_');
            }
            doc.put(key, entry.getValue());
        }

        // Add edge metadata
        doc.put("__edgeId", edge.getEdgeId());
        doc.put("__edgeLabel", edge.getLabel());
        doc.put("__outVertexId", edge.getOutVertexId());
        doc.put("__inVertexId", edge.getInVertexId());

        Object edgeState = edge.getProperties().get("__state");
        doc.putIfAbsent("__state", edgeState != null ? edgeState.toString() : "ACTIVE");

        return doc;
    }

    /**
     * Build a flat list of all statements for a vertex: vertex INSERT, index INSERTs,
     * typedef INSERTs, and edge INSERTs. All fired as individual async statements —
     * no cross-partition batching.
     *
     * Each edge writes to edges_out (partition: out_vertex_id), edges_in (partition:
     * in_vertex_id), and edges_by_id (partition: edge_id) — 3 different partition keys.
     * Batching these would span 30+ partitions for a typical vertex, triggering Cassandra
     * warnings and providing no atomicity benefit (UNLOGGED batches across partitions
     * are just grouped network calls with no failure guarantees).
     *
     * Atomicity is handled at a higher level: writes are idempotent (Cassandra INSERT
     * is an upsert), and the token-range resume mechanism re-processes failed ranges.
     */
    private List<BoundStatement> buildVertexStatements(DecodedVertex vertex) {
        String vertexId = resolveVertexId(vertex);
        Instant now = Instant.now();

        Map<String, Object> props = vertex.getProperties();

        String propsJson;
        try {
            propsJson = MAPPER.writeValueAsString(props);
        } catch (Exception e) {
            propsJson = "{}";
            LOG.warn("Failed to serialize properties for vertex {}", vertexId, e);
        }

        int propsBytes = propsJson.length();
        if (propsBytes > 50_000) {
            LOG.debug("Large vertex detected: jgId={} vertexId={} typeName={} propsJsonBytes={} propCount={} edgeCount={}",
                      vertex.getJgVertexId(), vertexId, vertex.getTypeName(),
                      String.format("%,d", propsBytes), props.size(), vertex.getOutEdges().size());
        }

        List<BoundStatement> stmts = new ArrayList<>();

        // 1. Vertex INSERT
        stmts.add(insertVertexStmt.bind(
            vertexId, propsJson, vertex.getVertexLabel(),
            vertex.getTypeName(), vertex.getState(), now, now));

        // 2. Index INSERTs
        int indexCount = buildIndexStatements(vertex, stmts);
        metrics.incrIndexesWritten(indexCount);

        // 3. TypeDef table INSERTs (if this is a TypeDef vertex)
        String typeName = (String) props.get("__type_name");
        Object rawCategory = props.get("__type_category");
        String typeCategory = rawCategory != null ? String.valueOf(rawCategory) : null;
        if ("typeSystem".equals(vertex.getVertexLabel()) && typeName != null && typeCategory != null) {
            stmts.add(insertTypeDefStmt.bind(typeName, typeCategory, vertexId, now, now));
            stmts.add(insertTypeDefByCategoryStmt.bind(typeCategory, typeName, vertexId));
            metrics.incrTypeDefsWritten();
        }

        // 4. Edge INSERTs (individual statements, not batched)
        for (DecodedEdge edge : vertex.getOutEdges()) {
            buildEdgeStatements(edge, now, stmts);
        }

        return stmts;
    }

    /**
     * Fire a single statement (BoundStatement or BatchStatement) asynchronously,
     * respecting the per-thread inflight semaphore.
     * Retries up to maxRetries times with exponential backoff on failure.
     */
    private void fireAsyncStatement(com.datastax.oss.driver.api.core.cql.Statement<?> stmt,
                                      Semaphore inflight, String vertexId) throws InterruptedException {
        inflight.acquire();
        writeAttempts.incrementAndGet();

        fireAsyncWithRetry(stmt, inflight, vertexId, 0);
    }

    private void fireAsyncWithRetry(com.datastax.oss.driver.api.core.cql.Statement<?> stmt,
                                      Semaphore inflight, String vertexId, int attempt) {
        targetSession.executeAsync(stmt).whenComplete((result, error) -> {
            if (error != null) {
                int maxRetries = config.getMaxRetries();
                if (attempt < maxRetries) {
                    // Exponential backoff: 500ms, 1s, 2s, ...
                    long delayMs = 500L * (1L << attempt);
                    LOG.warn("Async write failed for vertex {} (attempt {}/{}), retrying in {}ms: {}",
                             vertexId, attempt + 1, maxRetries, delayMs, error.toString());
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        inflight.release();
                        metrics.incrWriteErrors();
                        writeErrors.incrementAndGet();
                        return;
                    }
                    writeAttempts.incrementAndGet();
                    fireAsyncWithRetry(stmt, inflight, vertexId, attempt + 1);
                } else {
                    inflight.release();
                    metrics.incrWriteErrors();
                    long errCount = writeErrors.incrementAndGet();
                    if (errCount <= WRITE_SAMPLE_LOG_LIMIT) {
                        LOG.error("Async write PERMANENTLY failed for vertex {} after {} retries: {}",
                                  vertexId, maxRetries, error.toString());
                    }
                }
            } else {
                inflight.release();
            }
        });
    }

    /**
     * Add the 3 edge table INSERTs (edges_out, edges_in, edges_by_id) to the statement list.
     */
    private void buildEdgeStatements(DecodedEdge edge, Instant now, List<BoundStatement> stmts) {
        String edgePropsJson;
        try {
            edgePropsJson = edge.getProperties().isEmpty() ? "{}" :
                MAPPER.writeValueAsString(edge.getProperties());
        } catch (Exception e) {
            edgePropsJson = "{}";
        }

        Object edgeState = edge.getProperties().get("__state");
        String state = edgeState != null ? edgeState.toString() : "ACTIVE";

        String outVertexId = resolveVertexId(edge.getOutVertexJgId());
        String inVertexId = resolveVertexId(edge.getInVertexJgId());
        String edgeId;
        if (config.getIdStrategy() == IdStrategy.HASH_IDENTITY) {
            edgeId = DeterministicIdUtil.edgeIdFromIdentity(outVertexId, edge.getLabel(), inVertexId);
        } else {
            edgeId = DeterministicIdUtil.edgeIdFromJg(edge.getJgRelationId(), config.getIdStrategy());
        }

        stmts.add(insertEdgeOutStmt.bind(
            outVertexId, edge.getLabel(), edgeId,
            inVertexId, edgePropsJson, state, now, now));
        stmts.add(insertEdgeInStmt.bind(
            inVertexId, edge.getLabel(), edgeId,
            outVertexId, edgePropsJson, state, now, now));
        stmts.add(insertEdgeByIdStmt.bind(
            edgeId, outVertexId, inVertexId,
            edge.getLabel(), edgePropsJson, state, now, now));

        // Populate edge_index for relationship GUID lookups (_r__guid → edge_id)
        Object relGuid = edge.getProperties().get("_r__guid");
        if (relGuid != null) {
            stmts.add(insertEdgeIndexStmt.bind("_r__guid_idx", String.valueOf(relGuid), edgeId));
        }
    }

    /**
     * Add composite index INSERTs to the statement list.
     * Returns the number of index entries added.
     */
    private int buildIndexStatements(DecodedVertex vertex, List<BoundStatement> stmts) {
        String vertexId = resolveVertexId(vertex);
        Map<String, Object> props = vertex.getProperties();
        int indexCount = 0;

        // 1:1 unique index: __guid → vertex_id
        String guid = vertex.getGuid();
        if (guid != null) {
            stmts.add(insertIndexStmt.bind("__guid_idx", guid, vertexId));
            indexCount++;
        }

        // 1:1 composite index: qualifiedName + typeName → vertex_id
        Object qn = props.get("qualifiedName");
        if (qn == null) qn = props.get("Referenceable.qualifiedName");
        String typeName = vertex.getTypeName();
        if (qn != null && typeName != null) {
            stmts.add(insertIndexStmt.bind("qn_type_idx", qn + ":" + typeName, vertexId));
            indexCount++;
        }

        // 1:N index: __typeName → vertex_id
        if (typeName != null) {
            stmts.add(insertPropertyIndexStmt.bind("type_typename_idx", typeName, vertexId));
            indexCount++;
        }

        // 1:N index: TypeDef category lookup
        Object typeVertexType = props.get("__type");
        Object typeCategory = props.get("__type_category");
        if (typeVertexType != null && typeCategory != null) {
            stmts.add(insertPropertyIndexStmt.bind(
                "type_category_idx", typeVertexType + ":" + typeCategory, vertexId));
            indexCount++;
        }

        // 1:1 index: TypeDef name lookup
        if (typeVertexType != null) {
            Object typeDefName = props.get("__type_name");
            if (typeDefName == null) typeDefName = props.get("__type.name");
            if (typeDefName != null) {
                stmts.add(insertIndexStmt.bind(
                    "type_typename_idx", typeVertexType + ":" + typeDefName, vertexId));
                indexCount++;
            }
        }

        return indexCount;
    }

    private String resolveVertexId(DecodedVertex vertex) {
        String computed;
        if (config.getIdStrategy() == IdStrategy.HASH_IDENTITY) {
            // Try identity-based ID (matches runtime GraphIdUtil)
            Object qnObj = vertex.getProperties().get("qualifiedName");
            if (qnObj == null) qnObj = vertex.getProperties().get("Referenceable.qualifiedName");
            if (qnObj == null) {
                Object hierarchy = vertex.getProperties().get("__qualifiedNameHierarchy");
                if (hierarchy instanceof List && !((List<?>) hierarchy).isEmpty()) {
                    qnObj = ((List<?>) hierarchy).get(0);
                }
            }
            String identityId = DeterministicIdUtil.vertexIdFromIdentity(
                    vertex.getTypeName(), qnObj != null ? String.valueOf(qnObj) : null);
            computed = identityId != null ? identityId
                    : DeterministicIdUtil.vertexIdFromJg(vertex.getJgVertexId(), IdStrategy.HASH_JG);
        } else {
            computed = DeterministicIdUtil.vertexIdFromJg(vertex.getJgVertexId(), config.getIdStrategy());
        }

        // Always populate the override map so edge endpoint resolution works
        vertexIdOverrides.put(vertex.getJgVertexId(), computed);

        if (!config.isClaimEnabled()) {
            return computed;
        }

        Object qnObj = vertex.getProperties().get("qualifiedName");
        if (qnObj == null) {
            qnObj = vertex.getProperties().get("Referenceable.qualifiedName");
        }
        if (qnObj == null) {
            Object hierarchy = vertex.getProperties().get("__qualifiedNameHierarchy");
            if (hierarchy instanceof List && !((List<?>) hierarchy).isEmpty()) {
                qnObj = ((List<?>) hierarchy).get(0);
            }
        }
        String identityKey = DeterministicIdUtil.buildIdentityKey(vertex.getTypeName(),
                                                                   qnObj != null ? String.valueOf(qnObj) : null);
        if (identityKey == null) {
            return computed;
        }

        String claimedVertexId = claimVertexId(identityKey, computed);
        if (!claimedVertexId.equals(computed)) {
            vertexIdOverrides.put(vertex.getJgVertexId(), claimedVertexId);
            long count = claimRedirectCount.incrementAndGet();
            if (count <= 10 || count % 10000 == 0) {
                LOG.debug("Vertex claim redirect #{}: jgVertexId={} computedId={} claimedId={} identityKey={}",
                        count, vertex.getJgVertexId(), computed, claimedVertexId, identityKey);
            }
        }

        return claimedVertexId;
    }

    private String resolveVertexId(long jgVertexId) {
        String overridden = vertexIdOverrides.get(jgVertexId);
        if (overridden != null) {
            return overridden;
        }
        // Fallback: for HASH_IDENTITY, the in-vertex may not be processed yet.
        // Use HASH_JG as a stable fallback — the edge will still land on a deterministic ID,
        // just not the identity-based one.
        if (config.getIdStrategy() == IdStrategy.HASH_IDENTITY) {
            long count = edgeFallbackCount.incrementAndGet();
            if (count <= 10 || count % 10000 == 0) {
                LOG.debug("Edge endpoint jgVertexId={} not in override map (#{}); falling back to HASH_JG", jgVertexId, count);
            }
        }
        return DeterministicIdUtil.vertexIdFromJg(jgVertexId,
                config.getIdStrategy() == IdStrategy.HASH_IDENTITY ? IdStrategy.HASH_JG : config.getIdStrategy());
    }

    private String claimVertexId(String identityKey, String candidateVertexId) {
        try {
            Row appliedRow = targetSession.execute(insertClaimStmt.bind(
                    identityKey, candidateVertexId, Instant.now(), "migrator")).one();
            if (appliedRow != null && appliedRow.getBoolean("[applied]")) {
                return candidateVertexId;
            }
        } catch (Exception e) {
            LOG.warn("claimVertexId insert failed for identityKey={} candidateVertexId={}: {}",
                    identityKey, candidateVertexId, e.getMessage());
        }

        try {
            ResultSet rs = targetSession.execute(selectClaimStmt.bind(identityKey));
            Row row = rs.one();
            if (row != null && row.getString("vertex_id") != null) {
                return row.getString("vertex_id");
            }
        } catch (Exception e) {
            LOG.warn("claimVertexId select failed for identityKey={}: {}", identityKey, e.getMessage());
        }

        return candidateVertexId;
    }

    /**
     * Truncate all data tables in the target keyspace.
     * Called during --fresh mode to ensure a clean slate before migration.
     * Does NOT drop the keyspace or schema — only removes data.
     */
    public void truncateAll() {
        String[] tables = {
            "vertices", "edges_out", "edges_in", "edges_by_id",
            "vertex_index", "vertex_property_index", "edge_index",
            "entity_claims", "type_definitions", "type_definitions_by_category",
            "schema_registry"
        };
        for (String table : tables) {
            try {
                LOG.info("Truncating {}.{}", ks, table);
                targetSession.execute("TRUNCATE " + ks + "." + table);
            } catch (Exception e) {
                // Table may not exist yet on first run — that's fine
                LOG.debug("Could not truncate {}.{} (may not exist yet): {}", ks, table, e.getMessage());
            }
        }
        LOG.info("All target tables truncated in keyspace '{}'", ks);
    }

    @Override
    public void close() {
        if (!writerPool.isShutdown()) {
            writerPool.shutdownNow();
        }
    }
}
