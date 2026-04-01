package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Main entry point for the JanusGraph → Cassandra+ES migrator.
 *
 * Orchestrates three phases:
 *   Phase 1: Scan JanusGraph edgestore via CQL, decode with JG EdgeSerializer,
 *            write vertices + edges + indexes to new Cassandra schema
 *   Phase 2: Re-index all vertices into Elasticsearch
 *   Phase 3: Validate migration completeness
 *
 * Usage:
 *   java -jar atlas-graphdb-migrator.jar /path/to/migration.properties
 *
 * The migrator is designed to:
 *   - Run as a separate process (not inside Atlas pods)
 *   - Be fully resumable (tracks completed token ranges in Cassandra)
 *   - Apply backpressure (bounded queue between scanner and writer)
 *   - Report live progress metrics
 */
public class MigratorMain {

    private static final Logger LOG = LoggerFactory.getLogger(MigratorMain.class);

    private static final String PHASE_SCAN   = "scan";
    private static final String PHASE_ES     = "es_reindex";

    public static void main(String[] args) throws Exception {
        // Suppress verbose Cassandra driver CQL logging (query text, server-side warnings)
        suppressDriverLogs();

        if (args.length < 1) {
            System.err.println("Usage: java -jar atlas-graphdb-migrator.jar <config-file>");
            System.err.println("       java -jar atlas-graphdb-migrator.jar <config-file> --validate-only");
            System.err.println("       java -jar atlas-graphdb-migrator.jar <config-file> --es-only");
            System.err.println("       java -jar atlas-graphdb-migrator.jar <config-file> --fresh (clears state, starts over)");
            System.err.println("       java -jar atlas-graphdb-migrator.jar <config-file> --analyze (read-only graph analysis)");
            System.err.println("       java -jar atlas-graphdb-migrator.jar <config-file> --convert-ids <hex1> <hex2> ...");
            System.exit(1);
        }

        String configPath = args[0];
        boolean validateOnly = args.length > 1 && "--validate-only".equals(args[1]);
        boolean esOnly = args.length > 1 && "--es-only".equals(args[1]);
        boolean fresh = args.length > 1 && "--fresh".equals(args[1]);
        boolean analyze = args.length > 1 && "--analyze".equals(args[1]);
        boolean convertIds = args.length > 2 && "--convert-ids".equals(args[1]);

        MigratorConfig config = new MigratorConfig(configPath);

        // --convert-ids: convert hex partition keys (from old Mixpanel reports) to JG vertex IDs
        if (convertIds) {
            java.util.List<String> hexKeys = new java.util.ArrayList<>();
            for (int i = 2; i < args.length; i++) {
                hexKeys.add(args[i]);
            }
            if (hexKeys.isEmpty()) {
                System.err.println("Usage: --convert-ids <hex1> <hex2> ...");
                System.err.println("Example: --convert-ids c800000008127480 f8000000088e2b00");
                System.exit(1);
            }

            LOG.info("=== Convert Hex Partition Keys → JanusGraph Vertex IDs ===");
            CqlSession sourceSession = buildCqlSession(
                config.getSourceCassandraHostname(), config.getSourceCassandraPort(),
                config.getSourceCassandraDatacenter(), config.getSourceCassandraKeyspace(),
                config.getSourceCassandraUsername(), config.getSourceCassandraPassword(),
                false, Duration.ofSeconds(30), config.getSourceConsistencyLevel());

            try (JanusGraphAnalyzer analyzer = new JanusGraphAnalyzer(sourceSession, config)) {
                Map<String, String> results = analyzer.convertHexKeysToVertexIds(hexKeys);
                LOG.info("Conversion results ({} keys):", results.size());
                System.out.println();
                System.out.println("hex_partition_key          -> janus_vertex_id");
                System.out.println("-------------------------------------------");
                for (Map.Entry<String, String> entry : results.entrySet()) {
                    System.out.printf("%-26s -> %s%n", entry.getKey(), entry.getValue());
                }
                System.out.println();
            } finally {
                sourceSession.close();
            }
            return;
        }

        // Auto-size thread pools based on estate size (skip for analyze/validate modes)
        if (!analyze && !validateOnly) {
            MigrationSizer.autoSize(config);
        }

        // --analyze mode: read-only graph analysis
        if (analyze) {
            String mode = config.getAnalyzeMode();
            if ("janus".equalsIgnoreCase(mode)) {
                // JanusGraph pre-migration analysis: source Cassandra + ES
                LOG.info("=== Graph Analysis Mode (JanusGraph — pre-migration) ===");
                LOG.info("Source Cassandra: {}:{}/{}", config.getSourceCassandraHostname(),
                         config.getSourceCassandraPort(), config.getSourceCassandraKeyspace());
                LOG.info("Edgestore table: {}", config.getSourceEdgestoreTable());
                LOG.info("Source ES index: {}", config.getSourceEsIndex());

                CqlSession sourceSession = buildCqlSession(
                    config.getSourceCassandraHostname(), config.getSourceCassandraPort(),
                    config.getSourceCassandraDatacenter(), config.getSourceCassandraKeyspace(),
                    config.getSourceCassandraUsername(), config.getSourceCassandraPassword(),
                    false, Duration.ofSeconds(120), config.getSourceConsistencyLevel());

                try {
                    runJanusGraphAnalysis(config, sourceSession);
                } finally {
                    sourceSession.close();
                }
            } else {
                // CassandraGraph post-migration analysis
                LOG.info("=== Graph Analysis Mode (CassandraGraph — post-migration) ===");
                LOG.info("Target Cassandra: {}:{}/{}", config.getTargetCassandraHostname(),
                         config.getTargetCassandraPort(), config.getTargetCassandraKeyspace());

                CqlSession targetSession = buildCqlSession(
                    config.getTargetCassandraHostname(), config.getTargetCassandraPort(),
                    config.getTargetCassandraDatacenter(), config.getTargetCassandraKeyspace(),
                    config.getTargetCassandraUsername(), config.getTargetCassandraPassword(),
                    false, Duration.ofSeconds(120), config.getTargetConsistencyLevel());

                try {
                    runAnalysis(config, targetSession);
                } finally {
                    targetSession.close();
                }
            }
            return;
        }

        MigrationMetrics metrics = new MigrationMetrics();

        LOG.info("=== JanusGraph → Cassandra Migrator ===");
        LOG.info("Source Cassandra: {}:{}/{}", config.getSourceCassandraHostname(),
                 config.getSourceCassandraPort(), config.getSourceCassandraKeyspace());
        LOG.info("Target Cassandra: {}:{}/{}", config.getTargetCassandraHostname(),
                 config.getTargetCassandraPort(), config.getTargetCassandraKeyspace());
        LOG.info("Target ES: {}://{}:{}/{}", config.getTargetEsProtocol(),
                 config.getTargetEsHostname(), config.getTargetEsPort(), config.getTargetEsIndex());
        LOG.info("Scanner threads: {}, Writer threads: {}, Batch size: {}",
                 config.getScannerThreads(), config.getWriterThreads(), config.getWriterBatchSize());
        LOG.info("Async writes: maxInflight/thread={}, edgesOutOnly={}, maxEdges/batch={}",
                 config.getMaxInflightPerThread(), config.isEdgesOutOnly(), config.getMaxEdgesPerBatch());
        LOG.info("Resume: {}", config.isResume());
        LOG.info("ID strategy: {}, claim enabled: {}", config.getIdStrategy(), config.isClaimEnabled());
        LOG.info("Skip flags: esReindex={}, classifications={}, tasks={}",
                 config.isSkipEsReindex(), config.isSkipClassifications(), config.isSkipTasks());
        LOG.info("ES field limit: {}, Max retries: {}, ES parallel: {}", config.getEsFieldLimit(), config.getMaxRetries(), config.isEsParallel());
        LOG.info("ES edge index: {}, Super vertex chunk size: {}", config.getTargetEsEdgeIndex(), config.getSuperVertexEdgeChunkSize());
        LOG.info("Auxiliary migration: configStore={}, tags={}, sameCluster={}",
                 config.isMigrateConfigStore(), config.isMigrateTags(), config.isSameCassandraCluster());

        // Open Cassandra sessions
        // Source: long timeout for token-range scans, consistency ONE for fast reads
        // Target: tuned for high-throughput writes, consistency LOCAL_QUORUM for durability
        LOG.info("Source Cassandra consistency: {}, Target Cassandra consistency: {}",
                 config.getSourceConsistencyLevel(), config.getTargetConsistencyLevel());

        CqlSession sourceSession = buildCqlSession(
            config.getSourceCassandraHostname(), config.getSourceCassandraPort(),
            config.getSourceCassandraDatacenter(), config.getSourceCassandraKeyspace(),
            config.getSourceCassandraUsername(), config.getSourceCassandraPassword(),
            false, Duration.ofSeconds(120), config.getSourceConsistencyLevel());

        CqlSession targetSession = buildCqlSession(
            config.getTargetCassandraHostname(), config.getTargetCassandraPort(),
            config.getTargetCassandraDatacenter(), config.getTargetCassandraKeyspace(),
            config.getTargetCassandraUsername(), config.getTargetCassandraPassword(),
            true, null, config.getTargetConsistencyLevel());

        // Create target keyspace + tables first (must happen before stateStore.init()
        // since stateStore creates its table in the same keyspace)
        CassandraTargetWriter writer = new CassandraTargetWriter(config, metrics, targetSession);
        writer.init();

        MigrationStateStore stateStore = new MigrationStateStore(targetSession, config.getTargetCassandraKeyspace());
        stateStore.init();

        if (fresh) {
            LOG.info("=== FRESH MODE: Clearing ALL previous migration data ===");

            // 1. Clear migration state (resume tracking)
            stateStore.clearState(PHASE_SCAN);
            stateStore.clearState(PHASE_ES);
            stateStore.clearState(ParallelTagsMigrator.PHASE_TAGS_BY_ID);
            stateStore.clearState(ParallelTagsMigrator.PHASE_TAGS_PROPAGATED);

            // 2. Truncate all target Cassandra data tables
            writer.truncateAll();

            // 3. Delete target ES indexes (vertex + edge) so they're recreated fresh
            ElasticsearchReindexer tempEs = new ElasticsearchReindexer(config, metrics, targetSession);
            tempEs.deleteIndex();
            tempEs.deleteEdgeIndex();
            tempEs.close();

            LOG.info("=== FRESH MODE: All previous migration data cleared ===");
        }

        try {
            if (validateOnly) {
                runValidation(config, sourceSession, targetSession, stateStore);
            } else if (esOnly) {
                runEsReindex(config, metrics, targetSession);
            } else {
                runFullMigration(config, metrics, sourceSession, targetSession, stateStore, writer);
            }
        } finally {
            sourceSession.close();
            targetSession.close();
        }
    }

    private static void runFullMigration(MigratorConfig config, MigrationMetrics metrics,
                                          CqlSession sourceSession, CqlSession targetSession,
                                          MigrationStateStore stateStore,
                                          CassandraTargetWriter writer) throws Exception {
        metrics.start();

        // Source baseline collector — captures stats during Phase 1 scan at zero extra cost
        SourceBaselineCollector baselineCollector = new SourceBaselineCollector(
            config.getSuperVertexThreshold(), config.getSuperVertexTopN());

        // Start progress reporter — logs every 10 seconds
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        reporter.scheduleAtFixedRate(metrics::logProgress, 10, 10, TimeUnit.SECONDS);

        try {
            // ========== Phase 1: Scan + Write ==========
            LOG.info("========================================");
            LOG.info("=== Phase 1/3: Scan JanusGraph edgestore + Write to target ===");
            LOG.info("  Source: {}.{}", config.getSourceCassandraKeyspace(), config.getSourceEdgestoreTable());
            LOG.info("  Target keyspace: {}", config.getTargetCassandraKeyspace());
            LOG.info("  Scanner threads: {}, Writer threads: {}", config.getScannerThreads(), config.getWriterThreads());
            LOG.info("  Source baseline collection: ENABLED (super vertex threshold={})", config.getSuperVertexThreshold());
            LOG.info("========================================");

            // Set up parallel ES indexers if enabled (indexes into ES during Phase 1)
            ParallelEsIndexer parallelEsIndexer = null;
            ParallelEsIndexer parallelEsEdgeIndexer = null;
            if (config.isEsParallel() && !config.isSkipEsReindex()) {
                ElasticsearchReindexer tempReindexer = new ElasticsearchReindexer(config, metrics, targetSession);

                // Vertex ES indexer
                parallelEsIndexer = new ParallelEsIndexer(config, metrics);
                parallelEsIndexer.ensureIndex(tempReindexer);
                writer.setParallelEsIndexer(parallelEsIndexer);
                parallelEsIndexer.start();

                // Edge ES indexer (separate instance targeting the edge index)
                parallelEsEdgeIndexer = new ParallelEsIndexer(config, metrics,
                    config.getTargetEsEdgeIndex(), "es-parallel-edge-indexer", true);
                tempReindexer.ensureEdgeIndexExists(config.getTargetEsEdgeIndex());
                writer.setParallelEsEdgeIndexer(parallelEsEdgeIndexer);
                parallelEsEdgeIndexer.start();

                tempReindexer.close();
                LOG.info("Parallel ES indexing ENABLED (vertex + edge) — Phase 2 will be skipped");
            }

            writer.startWriters();

            LOG.info("Initializing JanusGraph schema resolution...");
            JanusGraphScanner scanner = new JanusGraphScanner(config, metrics, sourceSession);
            scanner.setBaselineCollector(baselineCollector);
            LOG.info("JanusGraph schema resolution ready. Starting CQL token-range scan...");

            scanner.scanAll(
                item -> writer.enqueue(item),   // Scanner → queue → writer (vertices + edge chunks)
                stateStore,
                PHASE_SCAN
            );

            // Save expected token range count for validation cross-check
            stateStore.saveMetadata("scan_total_ranges", String.valueOf(scanner.getTokenRangeCount()));

            LOG.info("All token ranges scanned. Waiting for writer threads to drain queue...");
            writer.signalScanComplete();
            writer.awaitCompletion();

            // ========== Phase 1.1: Retry failed token ranges ==========
            retryFailedRanges(config, metrics, sourceSession, stateStore, writer, scanner, PHASE_SCAN);

            scanner.close();
            writer.close();

            // Complete parallel ES indexing (drain remaining queues)
            if (parallelEsIndexer != null) {
                parallelEsIndexer.signalComplete();
                parallelEsIndexer.awaitCompletion();
                parallelEsIndexer.close();
            }
            if (parallelEsEdgeIndexer != null) {
                parallelEsEdgeIndexer.signalComplete();
                parallelEsEdgeIndexer.awaitCompletion();
                parallelEsEdgeIndexer.close();
            }

            // Save source baseline for Phase 3 comparison
            baselineCollector.saveBaseline(stateStore);
            SourceBaselineCollector.BaselineSnapshot baseline = baselineCollector.buildSnapshot();
            LOG.info("Source baseline: vertices={}, edges={}, maxEdgeCount={}, types={}",
                     baseline.totalVertices, baseline.totalEdges, baseline.maxEdgeCount,
                     baseline.typeVertexCounts != null ? baseline.typeVertexCounts.size() : 0);

            // Log source super vertices from Phase 1
            SuperVertexReport sourceSvReport = baselineCollector.buildSuperVertexReport(
                (long) metrics.getElapsedSeconds() * 1000);
            if (sourceSvReport.getTotalSuperVertexCount() > 0) {
                LOG.info("Source super vertices (from scan): {} found, max edges={}",
                         sourceSvReport.getTotalSuperVertexCount(), sourceSvReport.getMaxEdgeCount());
                for (SuperVertexReport.SuperVertexEntry entry : sourceSvReport.getTopSuperVertices()) {
                    LOG.info("  Super vertex: id={}, type={}, edges={}", entry.getVertexId(),
                             entry.getTypeName(), String.format("%,d", entry.getEdgeCount()));
                }
            }

            LOG.info("========================================");
            LOG.info("Phase 1 complete: {}", metrics.summary());
            LOG.info("========================================");

            // ========== Phase 1.5: Auxiliary Keyspace Migration ==========
            AuxiliaryKeyspaceMigrator auxMigrator = new AuxiliaryKeyspaceMigrator(sourceSession, targetSession, config, stateStore);
            auxMigrator.migrate();

            // ========== Phase 2: ES Re-index ==========
            boolean esAlreadyDone = parallelEsIndexer != null;
            if (esAlreadyDone) {
                LOG.info("========================================");
                LOG.info("=== Phase 2 SKIPPED (ES indexed in parallel during Phase 1) ===");
                LOG.info("  {} ES vertex docs, {} ES edge docs indexed during Phase 1",
                         String.format("%,d", metrics.getEsDocsIndexed()),
                         String.format("%,d", metrics.getEsEdgeDocsIndexed()));
                LOG.info("========================================");
            } else if (!config.isSkipEsReindex()) {
                LOG.info("========================================");
                LOG.info("=== Phase 2/3: Elasticsearch re-indexing ===");
                LOG.info("  Source: {}.vertices + {}.edges_by_id", config.getTargetCassandraKeyspace(), config.getTargetCassandraKeyspace());
                LOG.info("  Target ES vertex index: {}", config.getTargetEsIndex());
                LOG.info("  Target ES edge index: {}", config.getTargetEsEdgeIndex());
                LOG.info("  Bulk size: {} ", config.getEsBulkSize());
                LOG.info("========================================");

                ElasticsearchReindexer esReindexer = new ElasticsearchReindexer(config, metrics, targetSession);
                esReindexer.reindexAll();
                esReindexer.reindexEdges();
                esReindexer.close();

                LOG.info("Phase 2 complete: {} ES vertex docs, {} ES edge docs indexed",
                         String.format("%,d", metrics.getEsDocsIndexed()),
                         String.format("%,d", metrics.getEsEdgeDocsIndexed()));
            } else {
                LOG.info("========================================");
                LOG.info("=== Phase 2 SKIPPED (migration.skip.es.reindex=true) ===");
                LOG.info("  Existing ES index will be reused by the new graph layer");
                LOG.info("========================================");
            }

            // ========== Phase 3: Validation ==========
            LOG.info("========================================");
            LOG.info("=== Phase 3/3: Post-Migration Validation ===");
            LOG.info("========================================");

            MigrationValidator validator = new MigrationValidator(config, targetSession, stateStore);
            validator.setSourceBaseline(baseline);
            ValidationReport report = validator.validateAll();

            // Final summary
            LOG.info("========================================");
            LOG.info("  {}", metrics.summary());
            LOG.info("========================================");

            if (!report.isOverallPassed()) {
                LOG.error("========================================");
                LOG.error("  MIGRATION FAILED VALIDATION");
                LOG.error("  Migration data is written but NOT safe for cutover.");
                LOG.error("  Review the validation report above and fix issues before re-running.");
                LOG.error("========================================");
                System.exit(1);
            }

            LOG.info("========================================");
            LOG.info("  MIGRATION COMPLETED SUCCESSFULLY");
            LOG.info("  All {} validation checks PASSED.", report.getChecks().size());
            LOG.info("  Review the validation report above before proceeding with cutover.");
            LOG.info("========================================");
        } finally {
            reporter.shutdownNow();
        }
    }

    private static void runEsReindex(MigratorConfig config, MigrationMetrics metrics,
                                      CqlSession targetSession) throws IOException {
        metrics.start();
        LOG.info("=== ES Re-index Only Mode ===");

        ElasticsearchReindexer reindexer = new ElasticsearchReindexer(config, metrics, targetSession);
        reindexer.reindexAll();
        reindexer.reindexEdges();
        reindexer.close();

        LOG.info("ES re-indexing complete: {} vertex docs, {} edge docs",
                 metrics.getEsDocsIndexed(), metrics.getEsEdgeDocsIndexed());
    }

    private static void runValidation(MigratorConfig config, CqlSession sourceSession,
                                       CqlSession targetSession, MigrationStateStore stateStore) {
        LOG.info("=== Validation Only Mode ===");

        // Try to load source baseline from a previous migration run
        SourceBaselineCollector.BaselineSnapshot baseline =
            SourceBaselineCollector.loadBaseline(stateStore);

        MigrationValidator validator = new MigrationValidator(config, targetSession, stateStore);
        validator.setSourceSession(sourceSession);
        if (baseline != null) {
            validator.setSourceBaseline(baseline);
        }

        ValidationReport report = validator.validateAll();

        if (!report.isOverallPassed()) {
            LOG.error("VALIDATION FAILED. Migration is NOT safe for cutover.");
            System.exit(1);
        }

        LOG.info("========================================");
        LOG.info("  VALIDATION PASSED");
        LOG.info("  All {} checks passed. Migration is safe for cutover.", report.getChecks().size());
        LOG.info("========================================");
    }

    /**
     * Retry failed token ranges up to maxRetries times.
     * After each retry round, re-checks for remaining FAILED ranges.
     */
    private static void retryFailedRanges(MigratorConfig config, MigrationMetrics metrics,
                                            CqlSession sourceSession, MigrationStateStore stateStore,
                                            CassandraTargetWriter writer, JanusGraphScanner scanner,
                                            String phase) {
        int maxRetries = config.getMaxRetries();
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            Map<String, Long> statusCounts = stateStore.getStatusCounts(phase);
            long failed = statusCounts.getOrDefault("FAILED", 0L);

            if (failed == 0) {
                LOG.info("No failed token ranges — retry not needed");
                return;
            }

            LOG.info("========================================");
            LOG.info("=== Retry attempt {}/{}: {} failed token ranges ===", attempt, maxRetries, failed);
            LOG.info("========================================");

            // Clear FAILED status so they will be re-scanned (resume mode skips COMPLETED only)
            // The scanner.scanAll with resume=true will pick up non-COMPLETED ranges
            try {
                writer.startWriters();
                scanner.scanAll(
                    item -> writer.enqueue(item),
                    stateStore,
                    phase
                );
                writer.signalScanComplete();
                writer.awaitCompletion();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Retry interrupted at attempt {}", attempt);
                return;
            }
        }

        // Final check
        Map<String, Long> finalCounts = stateStore.getStatusCounts(phase);
        long remainingFailed = finalCounts.getOrDefault("FAILED", 0L);
        if (remainingFailed > 0) {
            LOG.error("After {} retries, {} token ranges still FAILED", maxRetries, remainingFailed);
        } else {
            LOG.info("All token ranges completed after retries");
        }
    }

    /**
     * Read-only graph analysis: detect super vertices, count vertex types,
     * and print a structured report suitable for Mixpanel ingestion.
     */
    private static void runAnalysis(MigratorConfig config, CqlSession targetSession) {
        LOG.info("Target: {}:{}/{}", config.getTargetCassandraHostname(),
                 config.getTargetCassandraPort(), config.getTargetCassandraKeyspace());
        LOG.info("Super vertex threshold: {}, Top-N: {}",
                 config.getSuperVertexThreshold(), config.getSuperVertexTopN());

        String domainName = System.getenv("DOMAIN_NAME");
        if (domainName == null || domainName.isEmpty()) {
            domainName = config.getValidationTenantId();
        }

        // 1. Super vertex detection (reuses existing SuperVertexDetector)
        LOG.info("Step 1/2: Running super vertex detection...");
        SuperVertexDetector detector = new SuperVertexDetector(
            targetSession, config.getTargetCassandraKeyspace(),
            config.getSuperVertexThreshold(), config.getSuperVertexTopN());
        SuperVertexReport svReport = detector.detect();

        // 2. Lightweight vertex type count scan
        LOG.info("Step 2/2: Scanning vertex types...");
        Map<String, Long> typeCounts = countVertexTypes(
            targetSession, config.getTargetCassandraKeyspace());

        // 3. Build report
        AnalysisReport report = new AnalysisReport();
        report.populate(domainName, config, svReport, typeCounts);

        // 4. Print formatted report to console
        LOG.info(report.toPrettyString());

        // 5. Print Mixpanel payload preview
        LOG.info("========================================");
        LOG.info("=== Mixpanel Payload (engage $set) ===");
        LOG.info("========================================");
        LOG.info("\n{}", report.toJson());

        // 6. Push to Mixpanel (only if analyze.push.to.mixpanel=true AND MIXPANEL_TOKEN is set)
        if (config.isPushToMixpanel()) {
            MixpanelReporter mixpanel = MixpanelReporter.fromEnv();
            if (mixpanel != null) {
                mixpanel.send(report);
            } else {
                LOG.info("Mixpanel push enabled but MIXPANEL_TOKEN env var is not set — skipping push");
            }
        } else {
            LOG.info("Mixpanel push disabled (analyze.push.to.mixpanel=false) — printed payload above for reference");
        }
    }

    /**
     * Read-only JanusGraph pre-migration analysis: uses ES for vertex type distribution
     * and edgestore CQL scan for super vertex detection.
     */
    private static void runJanusGraphAnalysis(MigratorConfig config, CqlSession sourceSession) {
        LOG.info("Super vertex threshold: {}, Top-N: {}",
                 config.getSuperVertexThreshold(), config.getSuperVertexTopN());

        String domainName = System.getenv("DOMAIN_NAME");
        if (domainName == null || domainName.isEmpty()) {
            domainName = config.getValidationTenantId();
        }

        try (JanusGraphAnalyzer analyzer = new JanusGraphAnalyzer(sourceSession, config)) {
            // 1. Vertex + edge counts from ES (kept as diagnostics / fallback)
            LOG.info("Step 1/3: Querying ES for vertex/edge counts and type distribution...");
            long totalVertices = analyzer.getTotalVertexCount();
            long totalEdges = analyzer.getTotalEdgeCount();
            Map<String, Long> esTypeCounts = analyzer.getVertexTypeCounts();

            // 2. Super vertex detection from edgestore (also extracts __typeName counts)
            LOG.info("Step 2/3: Scanning edgestore for super vertices...");
            SuperVertexReport svReport = analyzer.detectSuperVertices();

            // 3. Prefer Cassandra-sourced type counts over ES (ground truth).
            //    Fall back to ES if edgestore extraction yielded nothing (shouldn't happen).
            Map<String, Long> typeCounts = analyzer.getEdgestoreTypeCounts();
            if (typeCounts.isEmpty()) {
                LOG.warn("Edgestore yielded 0 type counts — falling back to ES type distribution");
                typeCounts = esTypeCounts;
            } else {
                LOG.info("Using Cassandra-sourced type counts ({} types) instead of ES ({} types)",
                         typeCounts.size(), esTypeCounts.size());
            }

            // 3b. Build report
            LOG.info("Step 3/3: Building report...");
            AnalysisReport report = new AnalysisReport();
            report.populateFromJanusGraph(domainName, config, svReport, typeCounts, totalVertices, totalEdges);

            // 4. Print formatted report to console
            LOG.info(report.toPrettyString());

            // 5. Print Mixpanel payload
            LOG.info("========================================");
            LOG.info("=== Mixpanel Payload (engage $set) ===");
            LOG.info("========================================");
            LOG.info("\n{}", report.toJson());

            // 6. Push to Mixpanel (only if analyze.push.to.mixpanel=true AND MIXPANEL_TOKEN is set)
            if (config.isPushToMixpanel()) {
                MixpanelReporter mixpanel = MixpanelReporter.fromEnv();
                if (mixpanel != null) {
                    mixpanel.send(report);
                } else {
                    LOG.info("Mixpanel push enabled but MIXPANEL_TOKEN env var is not set — skipping push");
                }
            } else {
                LOG.info("Mixpanel push disabled (analyze.push.to.mixpanel=false) — printed payload above for reference");
            }
        } catch (Exception e) {
            LOG.error("JanusGraph analysis failed: {}", e.getMessage(), e);
        }
    }

    /**
     * Scan the vertices table to count vertices per type_name.
     * Uses the same streaming approach as SuperVertexDetector — pages through
     * results, aggregates counts in memory.
     */
    private static Map<String, Long> countVertexTypes(CqlSession session, String keyspace) {
        SimpleStatement stmt = SimpleStatement.builder(
                "SELECT type_name FROM " + keyspace + ".vertices")
            .setPageSize(5000)
            .build();

        Map<String, Long> typeCounts = new HashMap<>();
        ResultSet rs = session.execute(stmt);
        long rowsRead = 0;

        for (Row row : rs) {
            String typeName = row.getString("type_name");
            if (typeName != null) {
                typeCounts.merge(typeName, 1L, Long::sum);
            }
            rowsRead++;
            if (rowsRead % 1_000_000 == 0) {
                LOG.info("  ... scanned {} vertex rows for type counts, {} distinct types",
                         String.format("%,d", rowsRead), typeCounts.size());
            }
        }

        LOG.info("Vertex type scan complete: {} rows, {} distinct types",
                 String.format("%,d", rowsRead), typeCounts.size());
        return typeCounts;
    }

    /**
     * Build a CqlSession. Does NOT specify keyspace at session level so we can
     * create keyspaces and query across them.
     *
     * @param tuneForWrites  if true, configure connection pool for high-throughput async writes:
     *                       4 connections per local node, 1024 max concurrent requests per connection
     */
    private static CqlSession buildCqlSession(String hostname, int port, String datacenter,
                                               String keyspace, String username, String password,
                                               boolean tuneForWrites, Duration requestTimeout,
                                               String consistencyLevel) {
        CqlSessionBuilder builder = CqlSession.builder()
            .addContactPoint(new InetSocketAddress(hostname, port))
            .withLocalDatacenter(datacenter);

        if (username != null && !username.isEmpty()) {
            builder.withAuthCredentials(username, password);
        }

        // Always create config builder to suppress driver warnings
        var configBuilder = DriverConfigLoader.programmaticBuilder();

        // Suppress server-side CQL warnings (unlogged batch size, etc.)
        configBuilder.withString(DefaultDriverOption.REQUEST_LOG_WARNINGS, "false");

        // Set consistency level
        if (consistencyLevel != null && !consistencyLevel.isEmpty()) {
            configBuilder.withString(DefaultDriverOption.REQUEST_CONSISTENCY, consistencyLevel);
            LOG.info("Session consistency level: {}", consistencyLevel);
        }

        if (tuneForWrites) {
            // 3 nodes × 4 connections × 1024 requests = 12,288 max concurrent requests
            configBuilder
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 4)
                .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, 2)
                .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 1024);
            LOG.info("Session tuned for writes: 4 connections/node, 1024 max requests/connection");
        }

        if (requestTimeout != null) {
            configBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, requestTimeout);
            LOG.info("Session request timeout: {}s", requestTimeout.getSeconds());
        } else if (tuneForWrites) {
            configBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30));
        }

        builder.withConfigLoader(configBuilder.build());

        CqlSession session = builder.build();
        LOG.info("Connected to Cassandra at {}:{} (datacenter: {})", hostname, port, datacenter);
        return session;
    }

    /**
     * Suppress verbose Cassandra driver logging (CQL query text, server-side warnings).
     * Must be called before any CqlSession is created.
     */
    private static void suppressDriverLogs() {
        // Use reflection to set logback level, avoiding compile-time dependency on logback
        try {
            Class<?> levelClass = Class.forName("ch.qos.logback.classic.Level");
            Object warnLevel = levelClass.getField("WARN").get(null);
            Class<?> loggerClass = Class.forName("ch.qos.logback.classic.Logger");
            java.lang.reflect.Method setLevel = loggerClass.getMethod("setLevel", levelClass);

            for (String name : new String[]{"com.datastax.oss.driver", "com.datastax.oss.driver.internal"}) {
                Object logger = LoggerFactory.getLogger(name);
                if (loggerClass.isInstance(logger)) {
                    setLevel.invoke(logger, warnLevel);
                }
            }

            // Set JanusGraph and Gremlin loggers to INFO (suppress verbose DEBUG)
            Object infoLevel = levelClass.getField("INFO").get(null);
            for (String name : new String[]{"org.janusgraph", "org.apache.tinkerpop"}) {
                Object logger = LoggerFactory.getLogger(name);
                if (loggerClass.isInstance(logger)) {
                    setLevel.invoke(logger, infoLevel);
                }
            }

            LOG.info("Log levels set: Cassandra driver=WARN, JanusGraph/Gremlin=INFO ");
        } catch (Exception e) {
            LOG.debug("Could not set Cassandra driver log level (non-fatal): {}", e.getMessage());
        }
    }
}
