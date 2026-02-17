package org.apache.atlas.repository.graphdb.migrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe metrics tracking for the migration process.
 * Provides live progress reporting.
 */
public class MigrationMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationMetrics.class);

    private final AtomicLong verticesScanned  = new AtomicLong(0);
    private final AtomicLong verticesWritten  = new AtomicLong(0);
    private final AtomicLong edgesWritten     = new AtomicLong(0);
    private final AtomicLong indexesWritten   = new AtomicLong(0);
    private final AtomicLong esDocsIndexed    = new AtomicLong(0);
    private final AtomicLong decodeErrors     = new AtomicLong(0);
    private final AtomicLong writeErrors      = new AtomicLong(0);
    private final AtomicLong tokenRangesTotal = new AtomicLong(0);
    private final AtomicLong tokenRangesDone  = new AtomicLong(0);
    private final AtomicLong cqlRowsRead      = new AtomicLong(0);

    private volatile long startTimeMs;

    public void start() {
        this.startTimeMs = System.currentTimeMillis();
    }

    public void incrVerticesScanned()           { verticesScanned.incrementAndGet(); }
    public void incrVerticesWritten()            { verticesWritten.incrementAndGet(); }
    public void incrEdgesWritten(long count)     { edgesWritten.addAndGet(count); }
    public void incrIndexesWritten(long count)   { indexesWritten.addAndGet(count); }
    public void incrEsDocsIndexed(long count)    { esDocsIndexed.addAndGet(count); }
    public void incrDecodeErrors()               { decodeErrors.incrementAndGet(); }
    public void incrWriteErrors()                { writeErrors.incrementAndGet(); }
    public void incrCqlRowsRead(long count)      { cqlRowsRead.addAndGet(count); }
    public void setTokenRangesTotal(long total)  { tokenRangesTotal.set(total); }
    public void incrTokenRangesDone()            { tokenRangesDone.incrementAndGet(); }

    public long getVerticesScanned()  { return verticesScanned.get(); }
    public long getVerticesWritten()  { return verticesWritten.get(); }
    public long getEdgesWritten()     { return edgesWritten.get(); }
    public long getEsDocsIndexed()    { return esDocsIndexed.get(); }
    public long getDecodeErrors()     { return decodeErrors.get(); }
    public long getWriteErrors()      { return writeErrors.get(); }
    public long getCqlRowsRead()      { return cqlRowsRead.get(); }

    public double getElapsedSeconds() {
        return (System.currentTimeMillis() - startTimeMs) / 1000.0;
    }

    public void logProgress() {
        double elapsed = getElapsedSeconds();
        double vertexRate = elapsed > 0 ? verticesScanned.get() / elapsed : 0;
        double rowRate = elapsed > 0 ? cqlRowsRead.get() / elapsed : 0;

        LOG.info("=== Migration Progress ===");
        LOG.info("  Elapsed: {:.1f}s | Token ranges: {}/{}", elapsed,
                 tokenRangesDone.get(), tokenRangesTotal.get());
        LOG.info("  CQL rows read: {} ({:.0f}/sec)", cqlRowsRead.get(), rowRate);
        LOG.info("  Vertices: scanned={}, written={} ({:.0f}/sec)",
                 verticesScanned.get(), verticesWritten.get(), vertexRate);
        LOG.info("  Edges written: {} | Indexes: {} | ES docs: {}",
                 edgesWritten.get(), indexesWritten.get(), esDocsIndexed.get());
        LOG.info("  Errors: decode={}, write={}", decodeErrors.get(), writeErrors.get());
    }

    /** Formatted summary string for the final report */
    public String summary() {
        double elapsed = getElapsedSeconds();
        return String.format(
            "Migration complete in %.1fs — Vertices: %d, Edges: %d, ES docs: %d, " +
            "Decode errors: %d, Write errors: %d, CQL rows: %d",
            elapsed, verticesWritten.get(), edgesWritten.get(), esDocsIndexed.get(),
            decodeErrors.get(), writeErrors.get(), cqlRowsRead.get());
    }
}
