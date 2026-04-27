package org.apache.atlas.repository.graphdb.migrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Collects migration lifecycle data and produces a flat Map for Mixpanel ingestion.
 *
 * Usage:
 *   1. recordStart(config) at the beginning of migration
 *   2. recordMetrics(metrics) after Phase 1 (or in catch block for partial data)
 *   3. recordValidation(report) after Phase 3
 *   4. recordCompletion("success"|"failed"|"error") when done
 *   5. recordError(message) on exception (before recordCompletion)
 *   6. toFlatMap() to get Mixpanel-ready properties
 *
 * Follows the same flat-map pattern as {@link AnalysisReport#toFlatMap()}.
 */
public class MigrationMixpanelReport {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT);

    // Identity
    private String tenantId;
    private String domainName;
    private String distinctId;

    // Environment
    private String ring;
    private String env;
    private String atlasImageTag;

    // Config snapshot
    private String idStrategy;
    private boolean claimEnabled;
    private int scannerThreads;
    private int writerThreads;
    private int queueCapacity;
    private boolean esParallel;
    private int esBulkSize;
    private int maxInflightPerThread;
    private boolean autoSized;
    private boolean skipEsReindex;
    private boolean skipClassifications;
    private boolean skipTasks;
    private boolean migrateConfigStore;
    private boolean migrateTags;
    private boolean freshMode;

    // Timing
    private Instant startTime;
    private Instant endTime;

    // Results
    private String status;       // "success", "failed", "error"
    private String errorMessage;

    // Metrics snapshot
    private long verticesScanned;
    private long verticesWritten;
    private long verticesSkipped;
    private long edgesWritten;
    private long indexesWritten;
    private long typeDefsWritten;
    private long esDocsIndexed;
    private long esEdgeDocsIndexed;
    private long decodeErrors;
    private long writeErrors;
    private long cqlRowsRead;
    private long tokenRangesTotal;
    private long tokenRangesDone;
    private long superVertexChunksEmitted;
    private double durationSeconds;

    // Validation snapshot
    private boolean validationRan;
    private boolean validationPassed;
    private int validationCheckCount;
    private int validationPassCount;
    private int validationWarnCount;
    private int validationFailCount;
    private long targetVertexCount;
    private long targetEdgeOutCount;
    private long targetEsDocCount;

    /**
     * Record migration start: captures config, env vars, and start time.
     */
    public void recordStart(MigratorConfig config, boolean fresh) {
        this.startTime = Instant.now();

        // Tenant identity — prefer DOMAIN_NAME env, then VCLUSTER_NAME, fall back to config
        this.domainName = getEnvOrDefault("DOMAIN_NAME",
            getEnvOrDefault("VCLUSTER_NAME", config.getValidationTenantId()));
        this.tenantId = domainName;
        this.distinctId = tenantId + "_" + startTime.getEpochSecond();

        // Environment metadata from pod env vars
        this.ring = getEnvOrDefault("RING", "unknown");
        this.env = getEnvOrDefault("ENV", getEnvOrDefault("ENVIRONMENT", "dev"));
        this.atlasImageTag = getEnvOrDefault("ATLAS_IMAGE_TAG",
            getEnvOrDefault("IMAGE_TAG", "unknown"));

        // Config snapshot
        this.idStrategy = config.getIdStrategy().name();
        this.claimEnabled = config.isClaimEnabled();
        this.scannerThreads = config.getScannerThreads();
        this.writerThreads = config.getWriterThreads();
        this.queueCapacity = config.getQueueCapacity();
        this.esParallel = config.isEsParallel();
        this.esBulkSize = config.getEsBulkSize();
        this.maxInflightPerThread = config.getMaxInflightPerThread();
        this.autoSized = config.isAutoSized();
        this.skipEsReindex = config.isSkipEsReindex();
        this.skipClassifications = config.isSkipClassifications();
        this.skipTasks = config.isSkipTasks();
        this.migrateConfigStore = config.isMigrateConfigStore();
        this.migrateTags = config.isMigrateTags();
        this.freshMode = fresh;
    }

    /**
     * Snapshot all metric counters. Safe to call multiple times (last call wins).
     */
    public void recordMetrics(MigrationMetrics metrics) {
        this.verticesScanned = metrics.getVerticesScanned();
        this.verticesWritten = metrics.getVerticesWritten();
        this.verticesSkipped = metrics.getVerticesSkipped();
        this.edgesWritten = metrics.getEdgesWritten();
        this.indexesWritten = metrics.getIndexesWritten();
        this.typeDefsWritten = metrics.getTypeDefsWritten();
        this.esDocsIndexed = metrics.getEsDocsIndexed();
        this.esEdgeDocsIndexed = metrics.getEsEdgeDocsIndexed();
        this.decodeErrors = metrics.getDecodeErrors();
        this.writeErrors = metrics.getWriteErrors();
        this.cqlRowsRead = metrics.getCqlRowsRead();
        this.tokenRangesTotal = metrics.getTokenRangesTotal();
        this.tokenRangesDone = metrics.getTokenRangesDone();
        this.superVertexChunksEmitted = metrics.getSuperVertexChunksEmitted();
        this.durationSeconds = metrics.getElapsedSeconds();
    }

    /**
     * Capture validation results.
     */
    public void recordValidation(ValidationReport report) {
        this.validationRan = true;
        this.validationPassed = report.isOverallPassed();
        this.validationCheckCount = report.getChecks().size();
        this.validationPassCount = (int) report.getChecks().stream()
            .filter(ValidationCheckResult::isPassed).count();
        this.validationFailCount = (int) report.getChecks().stream()
            .filter(c -> c.getSeverity() == ValidationCheckResult.Severity.FAIL).count();
        this.validationWarnCount = (int) report.getChecks().stream()
            .filter(c -> c.getSeverity() == ValidationCheckResult.Severity.WARN).count();
        this.targetVertexCount = report.getVertexCount();
        this.targetEdgeOutCount = report.getEdgeOutCount();
        this.targetEsDocCount = report.getEsDocCount();
    }

    /**
     * Mark migration status: "success", "failed" (validation failed), or "error" (exception).
     */
    public void recordCompletion(String status) {
        this.status = status;
        this.endTime = Instant.now();
    }

    /**
     * Record error message from an exception.
     */
    public void recordError(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * Flat key-value map for Mixpanel /engage $set and /import properties.
     * No nested objects — all values are scalar.
     */
    public Map<String, Object> toFlatMap() {
        Map<String, Object> m = new LinkedHashMap<>();

        // Identity
        m.put("tenant_id", tenantId);
        m.put("domain_name", domainName);

        // Environment
        m.put("ring", ring);
        m.put("env", env);
        m.put("atlas_image_tag", atlasImageTag);

        // Config
        m.put("id_strategy", idStrategy);
        m.put("claim_enabled", claimEnabled);
        m.put("scanner_threads", scannerThreads);
        m.put("writer_threads", writerThreads);
        m.put("queue_capacity", queueCapacity);
        m.put("es_parallel", esParallel);
        m.put("es_bulk_size", esBulkSize);
        m.put("max_inflight_per_thread", maxInflightPerThread);
        m.put("auto_sized", autoSized);
        m.put("skip_es_reindex", skipEsReindex);
        m.put("skip_classifications", skipClassifications);
        m.put("skip_tasks", skipTasks);
        m.put("migrate_config_store", migrateConfigStore);
        m.put("migrate_tags", migrateTags);
        m.put("fresh_mode", freshMode);

        // Timing
        m.put("start_time", startTime != null ? startTime.toString() : null);
        m.put("end_time", endTime != null ? endTime.toString() : null);
        m.put("duration_seconds", Math.round(durationSeconds));
        m.put("duration_human", MigrationMetrics.formatDuration(Math.round(durationSeconds)));

        // Results
        m.put("status", status != null ? status : "unknown");
        if (errorMessage != null) {
            // Truncate to 500 chars to avoid Mixpanel property size limits
            m.put("error_message", errorMessage.length() > 500
                ? errorMessage.substring(0, 497) + "..." : errorMessage);
        }

        // Metrics
        m.put("vertices_scanned", verticesScanned);
        m.put("vertices_written", verticesWritten);
        m.put("vertices_skipped", verticesSkipped);
        m.put("edges_written", edgesWritten);
        m.put("indexes_written", indexesWritten);
        m.put("typedefs_written", typeDefsWritten);
        m.put("es_docs_indexed", esDocsIndexed);
        m.put("es_edge_docs_indexed", esEdgeDocsIndexed);
        m.put("decode_errors", decodeErrors);
        m.put("write_errors", writeErrors);
        m.put("cql_rows_read", cqlRowsRead);
        m.put("token_ranges_total", tokenRangesTotal);
        m.put("token_ranges_done", tokenRangesDone);
        m.put("super_vertex_chunks_emitted", superVertexChunksEmitted);

        // Throughput
        if (durationSeconds > 0) {
            m.put("avg_vertices_per_second", Math.round(verticesWritten / durationSeconds));
            m.put("avg_cql_rows_per_second", Math.round(cqlRowsRead / durationSeconds));
        }

        // Validation
        m.put("validation_ran", validationRan);
        if (validationRan) {
            m.put("validation_passed", validationPassed);
            m.put("validation_check_count", validationCheckCount);
            m.put("validation_pass_count", validationPassCount);
            m.put("validation_warn_count", validationWarnCount);
            m.put("validation_fail_count", validationFailCount);
            m.put("target_vertex_count", targetVertexCount);
            m.put("target_edge_out_count", targetEdgeOutCount);
            m.put("target_es_doc_count", targetEsDocCount);
        }

        return m;
    }

    /**
     * Pretty-printed JSON of the Mixpanel payload for logging.
     */
    public String toJson() {
        try {
            return MAPPER.writeValueAsString(toFlatMap());
        } catch (Exception e) {
            return "{\"error\": \"" + e.getMessage() + "\"}";
        }
    }

    public String getDistinctId() {
        return distinctId;
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isEmpty()) ? val : defaultValue;
    }
}
