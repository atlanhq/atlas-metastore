package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Parallel migrator for the tags keyspace tables (tags_by_id and propagated_tags_by_source).
 *
 * Splits each table's Murmur3 token range into N segments (one per scanner thread),
 * scans each segment in parallel, and writes to the target using individual async
 * statements with Semaphore-bounded backpressure. Supports resume via MigrationStateStore.
 *
 * Each row is written as a separate async statement (no cross-partition batching)
 * because tags_by_id has PK (bucket, id) and propagated_tags_by_source has
 * PK (source_id, tag_type_name) — every row targets a different partition,
 * making UNLOGGED batches counterproductive (Cassandra warns on batches
 * spanning &gt;10 partitions).
 *
 * Each scanner thread does its own writing (no separate queue/writer pool needed)
 * because tags migration is a pure row copy with no decoding or transformation.
 */
public class ParallelTagsMigrator {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelTagsMigrator.class);

    private static final String TAGS_KEYSPACE         = "tags";
    private static final String TAGS_BY_ID_TABLE      = "tags_by_id";
    private static final String PROPAGATED_TAGS_TABLE = "propagated_tags_by_source";

    static final String PHASE_TAGS_BY_ID      = "tags_by_id";
    static final String PHASE_TAGS_PROPAGATED = "tags_propagated";

    private static final int  PAGE_SIZE    = 5000;    // CQL page size for scans
    private static final long LOG_INTERVAL = 100_000; // log every 100K rows per thread

    private final CqlSession sourceSession;
    private final CqlSession targetSession;
    private final MigratorConfig config;
    private final MigrationStateStore stateStore;

    public ParallelTagsMigrator(CqlSession sourceSession, CqlSession targetSession,
                                 MigratorConfig config, MigrationStateStore stateStore) {
        this.sourceSession = sourceSession;
        this.targetSession = targetSession;
        this.config        = config;
        this.stateStore    = stateStore;
    }

    /**
     * Migrate both tags tables in sequence.
     * Returns total rows migrated across both tables.
     */
    public long migrateAll() {
        long tagsByIdCount = migrateTable(
            TAGS_KEYSPACE + "." + TAGS_BY_ID_TABLE,
            PHASE_TAGS_BY_ID,
            buildTagsByIdScanCql(),
            buildTagsByIdInsertCql(),
            this::bindTagsByIdRow
        );

        long propagatedCount = migrateTable(
            TAGS_KEYSPACE + "." + PROPAGATED_TAGS_TABLE,
            PHASE_TAGS_PROPAGATED,
            buildPropagatedTagsScanCql(),
            buildPropagatedTagsInsertCql(),
            this::bindPropagatedTagsRow
        );

        LOG.info("Tags migration complete: tags_by_id={}, propagated_tags_by_source={}",
                 String.format("%,d", tagsByIdCount), String.format("%,d", propagatedCount));
        return tagsByIdCount + propagatedCount;
    }

    /**
     * Split the Murmur3 token range [-2^63, 2^63-1] into N equal segments.
     * Same algorithm as JanusGraphScanner.splitTokenRanges().
     */
    static List<long[]> splitTokenRanges(int numRanges) {
        BigInteger minToken  = BigInteger.valueOf(Long.MIN_VALUE);
        BigInteger maxToken  = BigInteger.valueOf(Long.MAX_VALUE);
        BigInteger totalRange = maxToken.subtract(minToken).add(BigInteger.ONE);
        BigInteger rangeSize  = totalRange.divide(BigInteger.valueOf(numRanges));

        List<long[]> ranges = new ArrayList<>(numRanges);
        BigInteger current = minToken;

        for (int i = 0; i < numRanges; i++) {
            BigInteger end = (i == numRanges - 1) ? maxToken
                : current.add(rangeSize).subtract(BigInteger.ONE);
            ranges.add(new long[]{current.longValueExact(), end.longValueExact()});
            current = end.add(BigInteger.ONE);
        }
        return ranges;
    }

    /**
     * Generic parallel table migrator. Splits the source table's token range,
     * assigns one range per scanner thread. Each thread scans its range
     * and fires individual async writes with Semaphore bounding.
     * Supports resume via MigrationStateStore.
     */
    private long migrateTable(String tableName, String phase,
                               String scanCql, String insertCql,
                               RowBinder rowBinder) {
        int numThreads = config.getScannerThreads();
        LOG.info("Starting parallel migration of {} (phase: {}, threads: {}, maxInflight: {})",
                 tableName, phase, numThreads, config.getMaxInflightPerThread());

        List<long[]> tokenRanges = splitTokenRanges(numThreads);

        Set<Long> completedRanges = config.isResume()
            ? stateStore.getCompletedRanges(phase)
            : Collections.emptySet();

        PreparedStatement scanStmt   = sourceSession.prepare(scanCql);
        PreparedStatement insertStmt = targetSession.prepare(insertCql);

        AtomicLong totalRows = new AtomicLong(0);

        ExecutorService pool = Executors.newFixedThreadPool(numThreads, r -> {
            Thread t = new Thread(r);
            t.setName("tags-scanner-" + t.getId());
            t.setDaemon(true);
            return t;
        });

        List<Future<?>> futures = new ArrayList<>();

        for (long[] range : tokenRanges) {
            long rangeStart = range[0];
            long rangeEnd   = range[1];

            if (completedRanges.contains(rangeStart)) {
                LOG.info("Skipping completed token range [{}, {}] for {} (resume)",
                         rangeStart, rangeEnd, phase);
                continue;
            }

            futures.add(pool.submit(() -> {
                try {
                    long count = scanAndWriteRange(scanStmt, insertStmt, rowBinder,
                                                    rangeStart, rangeEnd, phase, tableName);
                    totalRows.addAndGet(count);
                } catch (Exception e) {
                    LOG.error("Failed scanning token range [{}, {}] for {}",
                              rangeStart, rangeEnd, tableName, e);
                    stateStore.markRangeFailed(phase, rangeStart, rangeEnd);
                }
            }));
        }

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Tags scanner interrupted", e);
            } catch (ExecutionException e) {
                LOG.error("Tags scanner task failed for {}", tableName, e.getCause());
            }
        }

        pool.shutdown();
        LOG.info("Parallel migration of {} complete: {} rows migrated",
                 tableName, String.format("%,d", totalRows.get()));
        return totalRows.get();
    }

    /**
     * Scan a single token range from the source table and write each row
     * individually to the target using async statements with Semaphore bounding.
     */
    private long scanAndWriteRange(PreparedStatement scanStmt, PreparedStatement insertStmt,
                                    RowBinder rowBinder, long rangeStart, long rangeEnd,
                                    String phase, String tableName) throws InterruptedException {
        stateStore.markRangeStarted(phase, rangeStart, rangeEnd);

        ResultSet rs = sourceSession.execute(
            scanStmt.bind(rangeStart, rangeEnd).setPageSize(PAGE_SIZE));

        Semaphore inflight = new Semaphore(config.getMaxInflightPerThread());
        long rangeRows = 0;

        for (Row row : rs) {
            BoundStatement stmt = rowBinder.bind(insertStmt, row);
            fireAsyncStatement(stmt, inflight, tableName);
            rangeRows++;

            if (rangeRows % LOG_INTERVAL == 0) {
                LOG.info("  {} range [{}, {}]: {} rows migrated so far...",
                         tableName, rangeStart, rangeEnd,
                         String.format("%,d", rangeRows));
            }
        }

        // Drain: wait for all in-flight async writes to complete
        inflight.acquire(config.getMaxInflightPerThread());

        stateStore.markRangeCompleted(phase, rangeStart, rangeEnd, rangeRows, 0);
        LOG.debug("Token range completed: [{}, {}] for {} — {} rows",
                  rangeStart, rangeEnd, tableName, String.format("%,d", rangeRows));

        return rangeRows;
    }

    /**
     * Fire a single statement asynchronously, using the Semaphore to bound
     * in-flight writes. Releases the permit on completion or error.
     */
    private void fireAsyncStatement(BoundStatement stmt, Semaphore inflight,
                                     String tableName) throws InterruptedException {
        inflight.acquire();

        targetSession.executeAsync(stmt).whenComplete((result, error) -> {
            inflight.release();
            if (error != null) {
                LOG.error("Async write failed for {}: {}", tableName, error.getMessage());
            }
        });
    }

    // ---- CQL builders ----

    private String buildTagsByIdScanCql() {
        return "SELECT bucket, id, is_propagated, source_id, tag_type_name, " +
               "tag_meta_json, asset_metadata, updated_at, is_deleted " +
               "FROM " + TAGS_KEYSPACE + "." + TAGS_BY_ID_TABLE +
               " WHERE token(bucket, id) >= ? AND token(bucket, id) <= ?";
    }

    private String buildTagsByIdInsertCql() {
        return "INSERT INTO " + TAGS_KEYSPACE + "." + TAGS_BY_ID_TABLE +
               " (bucket, id, is_propagated, source_id, tag_type_name, " +
               "tag_meta_json, asset_metadata, updated_at, is_deleted) " +
               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    private String buildPropagatedTagsScanCql() {
        return "SELECT source_id, tag_type_name, propagated_asset_id, " +
               "asset_metadata, updated_at " +
               "FROM " + TAGS_KEYSPACE + "." + PROPAGATED_TAGS_TABLE +
               " WHERE token(source_id, tag_type_name) >= ? " +
               "AND token(source_id, tag_type_name) <= ?";
    }

    private String buildPropagatedTagsInsertCql() {
        return "INSERT INTO " + TAGS_KEYSPACE + "." + PROPAGATED_TAGS_TABLE +
               " (source_id, tag_type_name, propagated_asset_id, asset_metadata, updated_at) " +
               "VALUES (?, ?, ?, ?, ?)";
    }

    // ---- Row binders ----

    @FunctionalInterface
    private interface RowBinder {
        BoundStatement bind(PreparedStatement insertStmt, Row row);
    }

    private BoundStatement bindTagsByIdRow(PreparedStatement insertStmt, Row row) {
        return insertStmt.bind(
            row.getInt("bucket"),
            row.getString("id"),
            row.getBoolean("is_propagated"),
            row.getString("source_id"),
            row.getString("tag_type_name"),
            row.getString("tag_meta_json"),
            row.getString("asset_metadata"),
            row.getInstant("updated_at"),
            row.getBoolean("is_deleted"));
    }

    private BoundStatement bindPropagatedTagsRow(PreparedStatement insertStmt, Row row) {
        return insertStmt.bind(
            row.getString("source_id"),
            row.getString("tag_type_name"),
            row.getString("propagated_asset_id"),
            row.getString("asset_metadata"),
            row.getInstant("updated_at"));
    }
}
