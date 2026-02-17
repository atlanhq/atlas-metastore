package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.RelationType;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.WriteByteBuffer;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Hybrid CQL Scanner: scans JanusGraph's edgestore table directly via CQL,
 * then uses JanusGraph's own EdgeSerializer to decode the binary data.
 *
 * This gives CQL-speed reads with correct JanusGraph decoding — validated by
 * JanusGraph's own OLAP framework (VertexProgramScanJob / VertexJobConverter).
 *
 * Architecture:
 *   CQL token-range scan  →  JG EdgeSerializer.parseRelation()  →  DecodedVertex
 *
 * Each edge is stored in both vertices' adjacency lists in JanusGraph.
 * We only process OUT-direction edges to ensure exactly-once processing.
 */
public class JanusGraphScanner implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JanusGraphScanner.class);

    private final MigratorConfig   config;
    private final MigrationMetrics metrics;
    private final CqlSession       sourceSession;

    // JanusGraph internals for decoding
    private final StandardJanusGraph   janusGraph;
    private final IDManager            idManager;
    private final EdgeSerializer       edgeSerializer;
    private final ThreadLocal<StandardJanusGraphTx> threadLocalTx;

    public JanusGraphScanner(MigratorConfig config, MigrationMetrics metrics, CqlSession sourceSession) {
        this.config        = config;
        this.metrics       = metrics;
        this.sourceSession = sourceSession;

        // Open JanusGraph instance ONLY for schema/type resolution (read-only).
        // This connects to the same Cassandra as the source but only reads
        // schema metadata (PropertyKey names, EdgeLabel names, etc.)
        LOG.info("Opening JanusGraph for schema resolution from config: {}",
                 config.getSourceJanusGraphConfig());

        JanusGraph jg = JanusGraphFactory.open(config.getSourceJanusGraphConfig());
        this.janusGraph     = (StandardJanusGraph) jg;
        this.idManager      = janusGraph.getIDManager();
        this.edgeSerializer = janusGraph.getEdgeSerializer();

        // Each scanner thread gets its own read-only tx for thread-safe type resolution.
        // The tx caches type definitions after first lookup.
        this.threadLocalTx = ThreadLocal.withInitial(() ->
            (StandardJanusGraphTx) janusGraph.buildTransaction()
                .readOnly()
                .vertexCacheSize(200)
                .start()
        );
    }

    /**
     * Split the Murmur3 token range [-2^63, 2^63-1] into N equal segments.
     */
    public List<long[]> splitTokenRanges(int numRanges) {
        BigInteger minToken = BigInteger.valueOf(Long.MIN_VALUE);
        BigInteger maxToken = BigInteger.valueOf(Long.MAX_VALUE);
        BigInteger totalRange = maxToken.subtract(minToken).add(BigInteger.ONE);
        BigInteger rangeSize  = totalRange.divide(BigInteger.valueOf(numRanges));

        List<long[]> ranges = new ArrayList<>(numRanges);
        BigInteger current = minToken;

        for (int i = 0; i < numRanges; i++) {
            BigInteger end = (i == numRanges - 1) ? maxToken : current.add(rangeSize).subtract(BigInteger.ONE);
            ranges.add(new long[]{current.longValueExact(), end.longValueExact()});
            current = end.add(BigInteger.ONE);
        }

        LOG.info("Split token range into {} segments", ranges.size());
        return ranges;
    }

    /**
     * Scan all token ranges in parallel, decode vertices, and feed them to the consumer.
     *
     * @param consumer    receives each decoded vertex
     * @param stateStore  for resume support (skip completed ranges)
     * @param phase       migration phase name for state tracking
     */
    public void scanAll(Consumer<DecodedVertex> consumer, MigrationStateStore stateStore, String phase) {
        List<long[]> tokenRanges = splitTokenRanges(config.getScannerThreads());
        metrics.setTokenRangesTotal(tokenRanges.size());

        Set<Long> completedRanges = config.isResume()
            ? stateStore.getCompletedRanges(phase)
            : Collections.emptySet();

        String edgestoreTable = config.getSourceCassandraKeyspace() + "." + config.getSourceEdgestoreTable();

        PreparedStatement scanStmt = sourceSession.prepare(
            "SELECT key, column1, value FROM " + edgestoreTable +
            " WHERE token(key) >= ? AND token(key) <= ?");

        ExecutorService scannerPool = Executors.newFixedThreadPool(config.getScannerThreads(),
            r -> {
                Thread t = new Thread(r);
                t.setName("scanner-" + t.getId());
                t.setDaemon(true);
                return t;
            });

        List<Future<?>> futures = new ArrayList<>();

        for (long[] range : tokenRanges) {
            long rangeStart = range[0];
            long rangeEnd   = range[1];

            if (completedRanges.contains(rangeStart)) {
                LOG.debug("Skipping completed token range [{}, {}]", rangeStart, rangeEnd);
                metrics.incrTokenRangesDone();
                continue;
            }

            futures.add(scannerPool.submit(() -> {
                try {
                    scanTokenRange(scanStmt, rangeStart, rangeEnd, consumer, stateStore, phase);
                } catch (Exception e) {
                    LOG.error("Failed scanning token range [{}, {}]", rangeStart, rangeEnd, e);
                    stateStore.markRangeFailed(phase, rangeStart, rangeEnd);
                }
            }));
        }

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Scanner interrupted", e);
            } catch (ExecutionException e) {
                LOG.error("Scanner task failed", e.getCause());
            }
        }

        scannerPool.shutdown();
        LOG.info("All token ranges scanned");
    }

    /**
     * Scan a single token range. Groups CQL rows by vertex key,
     * decodes each vertex's full adjacency list, and passes to consumer.
     */
    private void scanTokenRange(PreparedStatement scanStmt, long rangeStart, long rangeEnd,
                                 Consumer<DecodedVertex> consumer,
                                 MigrationStateStore stateStore, String phase) {
        stateStore.markRangeStarted(phase, rangeStart, rangeEnd);

        ResultSet rs = sourceSession.execute(
            scanStmt.bind(rangeStart, rangeEnd)
                    .setPageSize(config.getScanFetchSize()));

        ByteBuffer currentKey = null;
        long currentVertexId = -1;
        List<Entry> currentEntries = new ArrayList<>(64);
        long rangeVertices = 0;
        long rangeEdges = 0;
        long rowCount = 0;

        for (Row row : rs) {
            rowCount++;
            ByteBuffer keyBuf = row.getByteBuffer("key");
            ByteBuffer colBuf = row.getByteBuffer("column1");
            ByteBuffer valBuf = row.getByteBuffer("value");

            if (currentKey == null || !keyBuf.equals(currentKey)) {
                // New vertex — flush the previous one
                if (currentKey != null && !currentEntries.isEmpty()) {
                    DecodedVertex decoded = decodeVertex(currentVertexId, currentEntries);
                    if (decoded != null) {
                        consumer.accept(decoded);
                        rangeVertices++;
                        rangeEdges += decoded.getOutEdges().size();
                    }
                }

                currentKey = keyBuf.duplicate();
                currentVertexId = extractVertexId(keyBuf);
                currentEntries.clear();
            }

            Entry entry = buildEntry(colBuf, valBuf);
            if (entry != null) {
                currentEntries.add(entry);
            }
        }

        // Process the last vertex in this range
        if (currentKey != null && !currentEntries.isEmpty()) {
            DecodedVertex decoded = decodeVertex(currentVertexId, currentEntries);
            if (decoded != null) {
                consumer.accept(decoded);
                rangeVertices++;
                rangeEdges += decoded.getOutEdges().size();
            }
        }

        metrics.incrCqlRowsRead(rowCount);
        stateStore.markRangeCompleted(phase, rangeStart, rangeEnd, rangeVertices, rangeEdges);
        metrics.incrTokenRangesDone();

        LOG.debug("Token range [{}, {}]: {} rows, {} vertices, {} out-edges",
                  rangeStart, rangeEnd, rowCount, rangeVertices, rangeEdges);
    }

    private long extractVertexId(ByteBuffer keyBuf) {
        byte[] bytes = new byte[keyBuf.remaining()];
        keyBuf.duplicate().get(bytes);
        StaticBuffer key = new StaticArrayBuffer(bytes);
        Object id = idManager.getKeyID(key);
        return ((Number) id).longValue();
    }

    /**
     * Combine column1 + value into a JanusGraph Entry (the format EdgeSerializer expects).
     * See: JanusGraph's VertexJobConverter and the Scala decoder gist.
     */
    private Entry buildEntry(ByteBuffer colBuf, ByteBuffer valBuf) {
        try {
            byte[] colBytes = new byte[colBuf.remaining()];
            colBuf.duplicate().get(colBytes);

            byte[] valBytes = new byte[valBuf.remaining()];
            valBuf.duplicate().get(valBytes);

            WriteByteBuffer wb = new WriteByteBuffer(colBytes.length + valBytes.length);
            wb.putBytes(colBytes);
            int valuePos = wb.getPosition();
            wb.putBytes(valBytes);

            return new StaticArrayEntry(wb.getStaticBuffer(), valuePos);
        } catch (Exception e) {
            LOG.trace("Failed to build entry", e);
            return null;
        }
    }

    /**
     * Decode a vertex's full adjacency list from JanusGraph's binary format.
     * Extracts properties and OUT-direction edges.
     */
    private DecodedVertex decodeVertex(long vertexId, List<Entry> entries) {
        // Skip invisible/internal JanusGraph system vertices
        try {
            if (IDManager.VertexIDType.Invisible.is(vertexId)) {
                return null;
            }
        } catch (Exception e) {
            // If check fails, process the vertex anyway
        }

        DecodedVertex vertex = new DecodedVertex(vertexId);
        StandardJanusGraphTx tx = threadLocalTx.get();

        for (Entry entry : entries) {
            try {
                RelationCache rel = edgeSerializer.parseRelation(entry, false, tx);

                // Skip JanusGraph internal system relations (VertexExists, SchemaName, etc.)
                if (isSystemRelation(rel.typeId)) {
                    continue;
                }

                RelationType type = tx.getExistingRelationType(rel.typeId);
                if (type == null) {
                    continue;
                }

                String relTypeName = type.name();

                if (type.isPropertyKey()) {
                    Object value = rel.getValue();
                    if (value != null) {
                        vertex.addProperty(relTypeName, value);
                    }
                } else {
                    // Edge — only process OUT direction to avoid double-counting.
                    // Each undirected edge in JG is stored twice: once in each endpoint's
                    // adjacency list (as OUT in source, IN in target).
                    Direction dir = rel.direction;
                    if (dir == Direction.OUT || dir == Direction.BOTH) {
                        long otherVertexId = ((Number) rel.getOtherVertexId()).longValue();
                        long relationId    = rel.relationId;

                        DecodedEdge edge = new DecodedEdge(
                            relationId, vertexId, otherVertexId, relTypeName);

                        // Extract edge properties via RelationCache iteration.
                        // RelationCache implements Iterable<LongObjectCursor<Object>>
                        // where key = property type ID, value = property value.
                        extractEdgeProperties(edge, rel, tx);

                        vertex.addOutEdge(edge);
                    }
                }
            } catch (Exception e) {
                metrics.incrDecodeErrors();
                LOG.trace("Decode error for vertex {} entry", vertexId, e);
            }
        }

        if (!vertex.getProperties().isEmpty() || !vertex.getOutEdges().isEmpty()) {
            metrics.incrVerticesScanned();
            return vertex;
        }

        return null;
    }

    /**
     * Extract edge properties from a RelationCache.
     * Uses iteration over the cache's internal property map.
     */
    private void extractEdgeProperties(DecodedEdge edge, RelationCache rel, StandardJanusGraphTx tx) {
        if (!rel.hasProperties()) {
            return;
        }
        try {
            // RelationCache implements Iterable over its property entries.
            // Each entry has: key (long typeId), value (Object propertyValue)
            for (Object cursor : rel) {
                // The cursor is a LongObjectCursor<Object> from HPPC.
                // We use reflection-free access via the Iterable contract.
                // In JanusGraph 1.0.x, RelationCache's iterator yields entries
                // with .key (long) and .value (Object) fields.
                try {
                    // Access via HPPC LongObjectCursor fields
                    java.lang.reflect.Field keyField = cursor.getClass().getField("key");
                    java.lang.reflect.Field valueField = cursor.getClass().getField("value");
                    long propTypeId = keyField.getLong(cursor);
                    Object propValue = valueField.get(cursor);

                    RelationType propType = tx.getExistingRelationType(propTypeId);
                    if (propType != null && propValue != null) {
                        edge.addProperty(propType.name(), propValue);
                    }
                } catch (Exception e) {
                    // Skip individual property on error
                }
            }
        } catch (Exception e) {
            LOG.trace("Failed to extract edge properties for edge {}", edge.getEdgeId(), e);
        }
    }

    private boolean isSystemRelation(long typeId) {
        try {
            return IDManager.isSystemRelationTypeId(typeId);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void close() {
        try {
            janusGraph.close();
        } catch (Exception e) {
            LOG.warn("Error closing JanusGraph instance", e);
        }
    }
}
