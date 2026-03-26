package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Enhanced post-migration validation.
 *
 * Runs 17 correctness checks against the target Cassandra (and optionally ES).
 * Returns a structured {@link ValidationReport} with per-check results, per-type
 * statistics, super vertex detection, and source baseline comparison.
 *
 * If any check fails, the migration is considered unsafe for cutover.
 */
public class MigrationValidator {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationValidator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * The product's connector aggregation query. Sent to the target ES index to verify
     * that the search UI will be able to load connector-based asset counts after cutover.
     * Filters for ACTIVE assets with recognized superType/typeName, excludes internal types,
     * and aggregates by connectorName (top 50). size=0 means no docs returned, only aggs.
     */
    private static final String ES_CONNECTOR_AGG_QUERY = "{\n" +
        "  \"size\": 0,\n" +
        "  \"query\": {\n" +
        "    \"bool\": {\n" +
        "      \"filter\": {\n" +
        "        \"bool\": {\n" +
        "          \"must\": [\n" +
        "            {\n" +
        "              \"bool\": {\n" +
        "                \"should\": [\n" +
        "                  {\n" +
        "                    \"terms\": {\n" +
        "                      \"__superTypeNames.keyword\": [\n" +
        "                        \"AI\", \"SQL\", \"BI\", \"SaaS\", \"ObjectStore\", \"EventStore\",\n" +
        "                        \"DataQuality\", \"Dbt\", \"API\", \"Airflow\", \"Spark\", \"MWAA\",\n" +
        "                        \"GCP_Cloud_Composer\", \"Astronomer\", \"SchemaRegistry\", \"Matillion\",\n" +
        "                        \"NoSQL\", \"MultiDimensionalDataset\", \"ERD\", \"DataModeling\", \"ADF\",\n" +
        "                        \"model\", \"Fivetran\", \"App\", \"Custom\", \"SAP\", \"Alteryx\", \"Flow\",\n" +
        "                        \"Sagemaker\", \"Partial\"\n" +
        "                      ]\n" +
        "                    }\n" +
        "                  },\n" +
        "                  {\n" +
        "                    \"terms\": {\n" +
        "                      \"__typeName.keyword\": [\n" +
        "                        \"Query\", \"Collection\", \"AtlasGlossary\", \"AtlasGlossaryCategory\",\n" +
        "                        \"AtlasGlossaryTerm\", \"Connection\", \"File\", \"Process\", \"DbtProcess\"\n" +
        "                      ]\n" +
        "                    }\n" +
        "                  }\n" +
        "                ],\n" +
        "                \"must_not\": [\n" +
        "                  {\n" +
        "                    \"terms\": {\n" +
        "                      \"__typeName.keyword\": [\n" +
        "                        \"MCIncident\", \"DbtColumnProcess\", \"BIProcess\", \"MatillionComponent\",\n" +
        "                        \"ModelVersion\", \"FlowDatasetOperation\", \"FlowFieldOperation\",\n" +
        "                        \"FabricVisual\", \"SnowflakeTag\", \"DbtTag\", \"BigqueryTag\"\n" +
        "                      ]\n" +
        "                    }\n" +
        "                  },\n" +
        "                  {\n" +
        "                    \"terms\": {\n" +
        "                      \"__superTypeNames.keyword\": [\"Tag\"]\n" +
        "                    }\n" +
        "                  }\n" +
        "                ],\n" +
        "                \"minimum_should_match\": 1\n" +
        "              }\n" +
        "            },\n" +
        "            {\n" +
        "              \"term\": {\n" +
        "                \"__state\": \"ACTIVE\"\n" +
        "              }\n" +
        "            }\n" +
        "          ],\n" +
        "          \"must_not\": [\n" +
        "            {\n" +
        "              \"term\": {\n" +
        "                \"connectorName\": \"\"\n" +
        "              }\n" +
        "            }\n" +
        "          ]\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"aggs\": {\n" +
        "    \"by_connector\": {\n" +
        "      \"terms\": {\n" +
        "        \"field\": \"connectorName\",\n" +
        "        \"size\": 50\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}";

    private final MigratorConfig     config;
    private final CqlSession         targetSession;
    private final MigrationStateStore stateStore;

    // Source session (for direct source-side counting in --validate-only mode)
    private CqlSession sourceSession;

    // Source baseline (may be null if --validate-only without prior run)
    private SourceBaselineCollector.BaselineSnapshot sourceBaseline;

    public MigrationValidator(MigratorConfig config, CqlSession targetSession,
                              MigrationStateStore stateStore) {
        this.config        = config;
        this.targetSession = targetSession;
        this.stateStore    = stateStore;
    }

    public void setSourceSession(CqlSession sourceSession) {
        this.sourceSession = sourceSession;
    }

    public void setSourceBaseline(SourceBaselineCollector.BaselineSnapshot baseline) {
        this.sourceBaseline = baseline;
    }

    /**
     * Run all validation checks. Returns a structured report.
     * The caller should check {@link ValidationReport#isOverallPassed()} to decide
     * whether the migration is safe for cutover.
     */
    public ValidationReport validateAll() {
        String ks = config.getTargetCassandraKeyspace();
        ValidationReport report = new ValidationReport(config.getValidationTenantId());
        report.setSourceBaseline(sourceBaseline);

        // Set source/target metadata for display
        report.setSourceKeyspace(config.getSourceCassandraKeyspace());
        report.setSourceEsIndex(config.getSourceEsIndex());
        report.setTargetKeyspace(config.getTargetCassandraKeyspace());
        report.setTargetEsIndex(config.getTargetEsIndex());

        LOG.info("=== Starting Enhanced Post-Migration Validation ===");
        LOG.info("  Source: {} (Cassandra) | {} (ES)", config.getSourceCassandraKeyspace(), config.getSourceEsIndex());
        LOG.info("  Target: {} (Cassandra) | {} (ES)", config.getTargetCassandraKeyspace(), config.getTargetEsIndex());
        LOG.info("  Vertex sample: {}, Edge sample: {}, Index sample: {}",
                 config.getValidationVertexSampleSize(),
                 config.getValidationEdgeSampleSize(),
                 config.getValidationIndexSampleSize());

        // --- Source-side counts (direct query, independent of baseline) ---
        runSourceCounts(report);

        // --- Check 17: ES connector aggregation (product readiness, run early) ---
        if (!config.isSkipEsCountValidation()) {
            runEsConnectorAggregationCheck(report);
        }

        // --- GroupBy type_name scan (runs first to get exact vertex count) ---
        runVertexCountByType(ks, report);

        // --- Check 1: Vertex count (uses exact count from GroupBy scan above) ---
        runVertexCountCheck(ks, report);

        // --- Check 11 (run early): Super vertex detection ---
        // Runs before edge consistency checks so actual row counts from full scan
        // can be used instead of unreliable system.size_estimates.
        if (!config.isSkipSuperVertexDetection()) {
            runSuperVertexDetection(ks, report);
        }

        // --- Check 2 & 3: Edge consistency ---
        runEdgeConsistencyChecks(ks, report);

        // --- Check 4: GUID index sample ---
        runGuidIndexCheck(ks, report);

        // --- Check 5: TypeDef cross-table consistency ---
        runTypeDefConsistencyCheck(ks, report);

        // --- Check 6: Deep vertex correctness (core new check) ---
        runDeepVertexChecks(ks, report);

        // --- Check 7: Cross-table integrity (vertex_index → vertices) ---
        runCrossTableIntegrityCheck(ks, report);

        // --- Check 8: Property corruption detection ---
        runPropertyCorruptionCheck(ks, report);

        // --- Check 9: ES document count comparison ---
        if (!config.isSkipEsCountValidation()) {
            runEsCountCheck(report);
        }

        // --- Check 10: Orphan edge detection ---
        runOrphanEdgeCheck(ks, report);

        // --- Check 12: Edge index table count ---
        runEdgeIndexCheck(ks, report);

        // --- Check 12b: ES edge index doc count ---
        if (!config.isSkipEsCountValidation()) {
            runEsEdgeCountCheck(ks, report);
        }

        // --- Check 13: config_store accessibility ---
        runConfigStoreCheck(report);

        // --- Check 14: tags tables accessibility ---
        runTagsTablesCheck(report);

        // --- Check 15: Token range completion ---
        runTokenRangeCompletionCheck(report);

        // --- Check 16: Disk space adequacy ---
        runDiskSpaceCheck(report);

        report.complete();

        // Print beautified report to console
        System.out.println(report.toPrettyString());

        // Write full JSON report to file for programmatic use
        String jsonFile = "validation-report-" + config.getValidationTenantId() + ".json";
        try (FileWriter fw = new FileWriter(jsonFile)) {
            fw.write(report.toJson());
            LOG.info("Full JSON report written to: {}", jsonFile);
        } catch (IOException e) {
            LOG.warn("Failed to write JSON report to file: {}", e.getMessage());
            LOG.info("Validation Report (JSON):\n{}", report.toJson());
        }

        return report;
    }

    // ========================================================================
    // Check 1: Vertex count
    // ========================================================================

    private void runVertexCountCheck(String ks, ValidationReport report) {
        // Use exact count already set by runVertexCountByType() (full table scan)
        long vertexCount = report.getVertexCount();

        long[] phaseSummary = stateStore.getPhaseSummary("scan");

        boolean passed = vertexCount > 0;
        String message = String.format("vertices=%d, state_store_reports=%d vertices from %d ranges",
                                       vertexCount, phaseSummary[1], phaseSummary[0]);

        // Compare with source baseline if available
        if (sourceBaseline != null && sourceBaseline.totalVertices > 0) {
            double ratio = (double) vertexCount / sourceBaseline.totalVertices;
            message += String.format(", source_baseline=%d, ratio=%.4f",
                                     sourceBaseline.totalVertices, ratio);
            // Fail if target count is outside 5% tolerance of source
            if (ratio < 0.95 || ratio > 1.05) {
                passed = false;
                if (ratio < 0.95) {
                    message += " [FAIL: target has >5% fewer vertices than source]";
                } else {
                    message += " [FAIL: target has >5% more vertices than source]";
                }
            }
        }

        ValidationCheckResult result = new ValidationCheckResult(
            "vertex_count", "Target has vertices matching source baseline",
            passed ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.FAIL,
            message);
        result.addDetail("vertex_count", vertexCount);
        result.addDetail("state_store_vertex_count", phaseSummary[1]);
        if (sourceBaseline != null) {
            result.addDetail("source_baseline_vertex_count", sourceBaseline.totalVertices);
        }
        report.addCheck(result);
    }

    // ========================================================================
    // GroupBy type_name — full scan of vertices table
    // ========================================================================

    private void runVertexCountByType(String ks, ValidationReport report) {
        LOG.info("Counting vertices grouped by type_name...");

        Map<String, Long> countByType = new LinkedHashMap<>();
        long totalScanned = 0;

        ResultSet rs = targetSession.execute(
            SimpleStatement.builder("SELECT type_name FROM " + ks + ".vertices")
                .setPageSize(5000)
                .build());

        for (Row row : rs) {
            String typeName = row.getString("type_name");
            String key = (typeName != null && !typeName.isEmpty()) ? typeName : "UNKNOWN";
            countByType.merge(key, 1L, Long::sum);
            totalScanned++;

            if (totalScanned % 100_000 == 0) {
                LOG.info("  ... scanned {} vertices for type grouping", String.format("%,d", totalScanned));
            }
        }

        report.setVertexCountByType(countByType);
        report.setVertexCount(totalScanned);

        // Log top types
        LOG.info("Vertex count by type ({} distinct types, {} total vertices):", countByType.size(), totalScanned);
        countByType.entrySet().stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .limit(30)
            .forEach(e -> LOG.info("  {}: {}", e.getKey(), String.format("%,d", e.getValue())));

        // Compare with source baseline if available
        if (report.getSourceBaseline() != null && report.getSourceBaseline().typeVertexCounts != null) {
            Map<String, Long> sourceTypes = report.getSourceBaseline().typeVertexCounts;
            int missingTypes = 0;
            long missingVertices = 0;
            for (Map.Entry<String, Long> src : sourceTypes.entrySet()) {
                long targetCount = countByType.getOrDefault(src.getKey(), 0L);
                long diff = targetCount - src.getValue();
                if (diff < 0) {
                    missingTypes++;
                    missingVertices += Math.abs(diff);
                    if (missingTypes <= 10) {
                        LOG.warn("  Type '{}': source={}, target={}, missing={}",
                                 src.getKey(), src.getValue(), targetCount, Math.abs(diff));
                    }
                }
            }
            if (missingTypes > 0) {
                LOG.warn("Source-target type comparison: {} types have fewer vertices (total missing: {})",
                         missingTypes, missingVertices);
            } else {
                LOG.info("Source-target type comparison: all types have >= source counts");
            }
        }
    }

    // ========================================================================
    // Check 2 & 3: Edge consistency
    // ========================================================================

    private void runEdgeConsistencyChecks(String ks, ValidationReport report) {
        // Use actual row counts from super vertex detection full scan when available.
        // Falls back to system.size_estimates which are unreliable after bulk writes.
        SuperVertexReport svReport = report.getSuperVertexReport();
        boolean usingActualCounts = svReport != null;

        long edgeOutCount, edgeInCount;
        if (usingActualCounts) {
            edgeOutCount = svReport.getEdgesOutRowCount();
            edgeInCount  = svReport.getEdgesInRowCount();
            LOG.info("Edge consistency: using actual counts from super vertex full scan " +
                     "(edges_out={}, edges_in={})", String.format("%,d", edgeOutCount),
                     String.format("%,d", edgeInCount));
        } else {
            edgeOutCount = countTable(ks + ".edges_out");
            edgeInCount  = countTable(ks + ".edges_in");
            LOG.info("Edge consistency: using estimated counts from size_estimates " +
                     "(edges_out={}, edges_in={})", String.format("%,d", edgeOutCount),
                     String.format("%,d", edgeInCount));
        }
        long edgeByIdCount;
        if (usingActualCounts) {
            edgeByIdCount = svReport.getEdgesByIdRowCount();
            LOG.info("Edge consistency: edges_by_id={} (actual full scan)",
                     String.format("%,d", edgeByIdCount));
        } else {
            edgeByIdCount = countTable(ks + ".edges_by_id");
            LOG.info("Edge consistency: edges_by_id={} (estimated from size_estimates)",
                     String.format("%,d", edgeByIdCount));
        }

        report.setEdgeOutCount(edgeOutCount);
        report.setEdgeInCount(edgeInCount);
        report.setEdgeByIdCount(edgeByIdCount);

        // Check 2: edges_out vs edges_in
        // With actual counts (full scan): exact match required, FAIL on mismatch (real data issue)
        // With estimates (size_estimates): 5% tolerance, FAIL on mismatch (edge consistency is critical)
        double outInTolerance = usingActualCounts ? 0.0 : 0.05;
        boolean outInMatch = isWithinTolerance(edgeOutCount, edgeInCount, outInTolerance);
        String countSource = usingActualCounts ? "actual full scan" : "estimated, 5% tolerance";
        ValidationCheckResult outInCheck = new ValidationCheckResult(
            "edge_out_in_consistency",
            "edges_out count matches edges_in count (" + countSource + ")",
            outInMatch ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.FAIL,
            String.format("edges_out=%d, edges_in=%d, diff=%d (source: %s)",
                          edgeOutCount, edgeInCount, Math.abs(edgeOutCount - edgeInCount), countSource));
        outInCheck.addDetail("edges_out_count", edgeOutCount);
        outInCheck.addDetail("edges_in_count", edgeInCount);
        outInCheck.addDetail("count_source", usingActualCounts ? "full_scan" : "size_estimates");
        report.addCheck(outInCheck);

        // Check 3: edges_by_id vs edges_out
        // With actual counts (full scan): exact match required, FAIL on mismatch
        // With estimates (size_estimates): 5% tolerance, WARN on mismatch
        double byIdTolerance = usingActualCounts ? 0.0 : 0.05;
        boolean byIdMatch = isWithinTolerance(edgeByIdCount, edgeOutCount, byIdTolerance);
        String byIdSource = usingActualCounts ? "actual full scan" : "estimated, 5% tolerance";
        ValidationCheckResult byIdCheck = new ValidationCheckResult(
            "edge_by_id_consistency",
            "edges_by_id count matches edges_out count (" + byIdSource + ")",
            byIdMatch ? ValidationCheckResult.Severity.PASS :
                (usingActualCounts ? ValidationCheckResult.Severity.FAIL : ValidationCheckResult.Severity.WARN),
            String.format("edges_by_id=%d, edges_out=%d, diff=%d (source: %s)",
                          edgeByIdCount, edgeOutCount, Math.abs(edgeByIdCount - edgeOutCount), byIdSource));
        byIdCheck.addDetail("edges_by_id_count", edgeByIdCount);
        byIdCheck.addDetail("edges_out_count", edgeOutCount);
        byIdCheck.addDetail("count_source", usingActualCounts ? "full_scan" : "size_estimates");
        report.addCheck(byIdCheck);
    }

    // ========================================================================
    // Check 4: GUID index sample
    // ========================================================================

    private void runGuidIndexCheck(String ks, ValidationReport report) {
        int sampleSize = config.getValidationVertexSampleSize();
        LOG.info("GUID index validation: sampling {} vertices...", sampleSize);

        int checked = 0, found = 0, missing = 0;
        List<String> sampleFailures = new ArrayList<>();

        // Token-based random sampling
        int probes = config.getValidationTokenProbes();
        int perProbe = (sampleSize / probes) + 1;

        for (int probe = 0; probe < probes && checked < sampleSize; probe++) {
            long randomToken = ThreadLocalRandom.current().nextLong();
            ResultSet rs = targetSession.execute(SimpleStatement.newInstance(
                "SELECT vertex_id, properties FROM " + ks + ".vertices " +
                "WHERE token(vertex_id) >= ? LIMIT ?",
                randomToken, perProbe));

            for (Row row : rs) {
                if (checked >= sampleSize) break;

                String vertexId = row.getString("vertex_id");
                String propsJson = row.getString("properties");

                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> props = MAPPER.readValue(propsJson, Map.class);
                    String guid = getStringProp(props, "__guid");

                    if (guid != null) {
                        checked++;
                        ResultSet idxRs = targetSession.execute(SimpleStatement.newInstance(
                            "SELECT vertex_id FROM " + ks + ".vertex_index " +
                            "WHERE index_name = '__guid_idx' AND index_value = ?",
                            guid));
                        Row idxRow = idxRs.one();
                        if (idxRow != null && vertexId.equals(idxRow.getString("vertex_id"))) {
                            found++;
                        } else {
                            missing++;
                            if (sampleFailures.size() < 10) {
                                sampleFailures.add(String.format("vertex=%s guid=%s index_returns=%s",
                                    vertexId, guid,
                                    idxRow != null ? idxRow.getString("vertex_id") : "null"));
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.trace("Error parsing properties for vertex {}", vertexId, e);
                }
            }
        }

        ValidationCheckResult result = new ValidationCheckResult(
            "guid_index_sample",
            "GUID index completeness (sampled)",
            missing == 0 ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.FAIL,
            String.format("checked=%d, found=%d, missing=%d", checked, found, missing));
        result.addDetail("checked", checked);
        result.addDetail("found", found);
        result.addDetail("missing", missing);
        for (String f : sampleFailures) result.addSampleFailure(f);
        report.addCheck(result);
    }

    // ========================================================================
    // Check 5: TypeDef cross-table consistency
    // ========================================================================

    private void runTypeDefConsistencyCheck(String ks, ValidationReport report) {
        long typeDefCount      = scanTableCount(ks + ".type_definitions");
        long typeDefByCatCount = scanTableCount(ks + ".type_definitions_by_category");

        report.setTypeDefCount(typeDefCount);
        report.setTypeDefByCategoryCount(typeDefByCatCount);

        boolean countMatch = typeDefCount == typeDefByCatCount;
        boolean nonEmpty   = typeDefCount > 0;

        ValidationCheckResult.Severity severity;
        if (countMatch && nonEmpty) severity = ValidationCheckResult.Severity.PASS;
        else if (nonEmpty)          severity = ValidationCheckResult.Severity.WARN;
        else                        severity = ValidationCheckResult.Severity.FAIL;

        ValidationCheckResult result = new ValidationCheckResult(
            "typedef_consistency",
            "type_definitions and type_definitions_by_category are consistent and non-empty (exact count)",
            severity,
            String.format("type_definitions=%d, type_definitions_by_category=%d", typeDefCount, typeDefByCatCount));
        result.addDetail("type_definitions_count", typeDefCount);
        result.addDetail("type_definitions_by_category_count", typeDefByCatCount);
        report.addCheck(result);
    }

    // ========================================================================
    // Check 6: Deep vertex correctness (core new check)
    // ========================================================================

    private void runDeepVertexChecks(String ks, ValidationReport report) {
        int sampleSize = config.getValidationVertexSampleSize();
        int probes     = config.getValidationTokenProbes();
        int perProbe   = Math.max(1, (sampleSize / probes) + 1);

        LOG.info("Deep vertex validation: {} probes x {} rows (target {} samples)...",
                 probes, perProbe, sampleSize);

        int totalChecked = 0, totalValid = 0;
        int entityVertices = 0, systemVertices = 0;
        int missingGuid = 0, missingQualifiedName = 0;
        int guidIndexMismatch = 0, qnIndexMismatch = 0;
        int edgeTripleMissing = 0, malformedJson = 0;
        int edgeEndpointMissing = 0;

        List<String> sampleFailures = new ArrayList<>();
        Map<String, ValidationReport.TypeStats> typeStats = report.getTypeStatsMap();

        for (int probe = 0; probe < probes && totalChecked < sampleSize; probe++) {
            long randomToken = ThreadLocalRandom.current().nextLong();

            ResultSet rs = targetSession.execute(SimpleStatement.newInstance(
                "SELECT vertex_id, properties, type_name, state FROM " + ks + ".vertices " +
                "WHERE token(vertex_id) >= ? LIMIT ?",
                randomToken, perProbe));

            for (Row row : rs) {
                if (totalChecked >= sampleSize) break;
                totalChecked++;

                String vertexId  = row.getString("vertex_id");
                String propsJson = row.getString("properties");
                // Use column-level type_name and state (normalized during migration)
                String typeName  = row.getString("type_name");
                String state     = row.getString("state");

                // Classify: entity vertex vs system/internal vertex
                // Entity vertices have a non-null, non-empty type_name column.
                // System vertices (JanusGraph schema defs, index entries, tasks without type)
                // have null/empty type_name.
                boolean isEntityVertex = typeName != null && !typeName.isEmpty();

                if (isEntityVertex) {
                    entityVertices++;
                } else {
                    systemVertices++;
                }

                // Sub-check A: JSON well-formedness
                Map<String, Object> props;
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> parsed = MAPPER.readValue(propsJson, Map.class);
                    props = parsed;
                    if (props == null || props.isEmpty()) {
                        malformedJson++;
                        addSampleFailure(sampleFailures, vertexId + ": empty properties JSON");
                        continue;
                    }
                } catch (Exception e) {
                    malformedJson++;
                    addSampleFailure(sampleFailures, vertexId + ": invalid JSON: " + e.getMessage());
                    continue;
                }

                // Sub-check B: Essential properties (only for entity vertices)
                // __guid lives in JSON properties; type_name and state are column-level
                String guid = getStringProp(props, "__guid");
                String qn = getStringProp(props, "qualifiedName");
                if (qn == null) qn = getStringProp(props, "Referenceable.qualifiedName");

                if (isEntityVertex && guid == null) missingGuid++;
                // qualifiedName check only for entity vertices with state=ACTIVE
                if (isEntityVertex && qn == null && "ACTIVE".equals(state)) missingQualifiedName++;

                // Per-type stats
                String typeKey = isEntityVertex ? typeName : "SYSTEM";
                ValidationReport.TypeStats ts = typeStats.computeIfAbsent(
                    typeKey, k -> new ValidationReport.TypeStats());
                ts.incrVertexCount();

                // Sub-check C: GUID index correctness (entity vertices only)
                if (guid != null) {
                    ResultSet idxRs = targetSession.execute(SimpleStatement.newInstance(
                        "SELECT vertex_id FROM " + ks + ".vertex_index " +
                        "WHERE index_name = '__guid_idx' AND index_value = ?",
                        guid));
                    Row idxRow = idxRs.one();
                    if (idxRow == null || !vertexId.equals(idxRow.getString("vertex_id"))) {
                        guidIndexMismatch++;
                        ts.incrErrorCount();
                        addSampleFailure(sampleFailures,
                            vertexId + ": GUID index mismatch (guid=" + guid + ")");
                    }
                }

                // Sub-check D: qn_type_idx correctness (entity vertices with QN only)
                if (qn != null && typeName != null) {
                    String indexValue = qn + ":" + typeName;
                    ResultSet qnRs = targetSession.execute(SimpleStatement.newInstance(
                        "SELECT vertex_id FROM " + ks + ".vertex_index " +
                        "WHERE index_name = 'qn_type_idx' AND index_value = ?",
                        indexValue));
                    Row qnRow = qnRs.one();
                    if (qnRow == null || !vertexId.equals(qnRow.getString("vertex_id"))) {
                        qnIndexMismatch++;
                        addSampleFailure(sampleFailures,
                            vertexId + ": qn_type_idx mismatch (qn=" + qn + ", type=" + typeName + ")");
                    }
                }

                // Sub-check E: Edge triple-table consistency (sample first 5 edges)
                ResultSet outRs = targetSession.execute(SimpleStatement.newInstance(
                    "SELECT edge_id, in_vertex_id FROM " + ks + ".edges_out " +
                    "WHERE out_vertex_id = ? LIMIT 5",
                    vertexId));
                List<Row> outEdges = outRs.all();
                ts.addEdgeCount(outEdges.size());

                for (Row edgeRow : outEdges) {
                    String edgeId     = edgeRow.getString("edge_id");
                    String inVertexId = edgeRow.getString("in_vertex_id");

                    // Check edges_by_id has this edge
                    ResultSet byIdRs = targetSession.execute(SimpleStatement.newInstance(
                        "SELECT edge_id FROM " + ks + ".edges_by_id WHERE edge_id = ?",
                        edgeId));
                    if (byIdRs.one() == null) {
                        edgeTripleMissing++;
                        addSampleFailure(sampleFailures,
                            vertexId + ": edge " + edgeId + " missing from edges_by_id");
                    }

                    // Sub-check F: Edge endpoint vertex exists
                    ResultSet inVtxRs = targetSession.execute(SimpleStatement.newInstance(
                        "SELECT vertex_id FROM " + ks + ".vertices WHERE vertex_id = ?",
                        inVertexId));
                    if (inVtxRs.one() == null) {
                        edgeEndpointMissing++;
                        addSampleFailure(sampleFailures,
                            vertexId + ": edge " + edgeId + " target vertex " + inVertexId + " missing");
                    }
                }

                // A vertex is "valid" if it's an entity with guid, type_name, and state,
                // OR if it's a system vertex (system vertices don't need Atlas properties)
                boolean valid;
                if (isEntityVertex) {
                    valid = guid != null && state != null;
                } else {
                    valid = true; // system vertices are valid by definition
                }
                if (valid) {
                    totalValid++;
                    ts.incrValidCount();
                }
            }
        }

        boolean passed = malformedJson == 0 && guidIndexMismatch == 0 &&
                          qnIndexMismatch == 0 && edgeEndpointMissing == 0 && edgeTripleMissing == 0;

        ValidationCheckResult result = new ValidationCheckResult(
            "deep_vertex_correctness",
            "Deep validation of sampled vertices (properties, indexes, edges, endpoints)",
            passed ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.FAIL,
            String.format("checked=%d, valid=%d (entity=%d, system=%d), malformedJson=%d, " +
                          "guidMismatch=%d, qnMismatch=%d, edgeTripleMissing=%d, edgeEndpointMissing=%d",
                          totalChecked, totalValid, entityVertices, systemVertices,
                          malformedJson, guidIndexMismatch, qnIndexMismatch,
                          edgeTripleMissing, edgeEndpointMissing));

        result.addDetail("total_checked", totalChecked);
        result.addDetail("total_valid", totalValid);
        result.addDetail("entity_vertices", entityVertices);
        result.addDetail("system_vertices", systemVertices);
        result.addDetail("missing_guid", missingGuid);
        result.addDetail("missing_qualifiedName", missingQualifiedName);
        result.addDetail("guid_index_mismatch", guidIndexMismatch);
        result.addDetail("qn_index_mismatch", qnIndexMismatch);
        result.addDetail("edge_triple_missing", edgeTripleMissing);
        result.addDetail("edge_endpoint_missing", edgeEndpointMissing);
        result.addDetail("malformed_json", malformedJson);
        for (String f : sampleFailures) result.addSampleFailure(f);
        report.addCheck(result);

        LOG.info("Deep vertex check: checked={}, valid={} (entity={}, system={}), issues={}",
                 totalChecked, totalValid, entityVertices, systemVertices,
                 (malformedJson + guidIndexMismatch + edgeEndpointMissing));
    }

    // ========================================================================
    // Check 7: Cross-table integrity (vertex_index → vertices)
    // ========================================================================

    private void runCrossTableIntegrityCheck(String ks, ValidationReport report) {
        int sampleSize = config.getValidationIndexSampleSize();
        LOG.info("Cross-table integrity: sampling {} vertex_index entries...", sampleSize);

        int checked = 0, found = 0, orphanIndexEntries = 0;
        List<String> sampleFailures = new ArrayList<>();

        long randomToken = ThreadLocalRandom.current().nextLong();
        ResultSet rs = targetSession.execute(SimpleStatement.newInstance(
            "SELECT index_name, index_value, vertex_id FROM " + ks + ".vertex_index " +
            "WHERE token(index_name, index_value) >= ? LIMIT ?",
            randomToken, sampleSize));

        for (Row row : rs) {
            checked++;
            String indexName  = row.getString("index_name");
            String indexValue = row.getString("index_value");
            String vertexId   = row.getString("vertex_id");

            ResultSet vtxRs = targetSession.execute(SimpleStatement.newInstance(
                "SELECT vertex_id FROM " + ks + ".vertices WHERE vertex_id = ?",
                vertexId));
            if (vtxRs.one() != null) {
                found++;
            } else {
                orphanIndexEntries++;
                addSampleFailure(sampleFailures,
                    indexName + "=" + indexValue + " → vertex " + vertexId + " NOT FOUND");
            }
        }

        ValidationCheckResult result = new ValidationCheckResult(
            "cross_table_integrity",
            "vertex_index entries point to existing vertices",
            orphanIndexEntries == 0 ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.WARN,
            String.format("checked=%d, found=%d, orphan=%d", checked, found, orphanIndexEntries));
        result.addDetail("checked", checked);
        result.addDetail("found", found);
        result.addDetail("orphan_index_entries", orphanIndexEntries);
        for (String f : sampleFailures) result.addSampleFailure(f);
        report.addCheck(result);
    }

    // ========================================================================
    // Check 8: Property corruption detection
    // ========================================================================

    private void runPropertyCorruptionCheck(String ks, ValidationReport report) {
        int sampleSize = config.getValidationVertexSampleSize();
        LOG.info("Property corruption check: sampling {} vertices...", sampleSize);

        int checked = 0, corrupted = 0;
        List<String> sampleFailures = new ArrayList<>();
        int probes = config.getValidationTokenProbes();
        int perProbe = Math.max(1, (sampleSize / probes) + 1);

        for (int probe = 0; probe < probes && checked < sampleSize; probe++) {
            long randomToken = ThreadLocalRandom.current().nextLong();
            ResultSet rs = targetSession.execute(SimpleStatement.newInstance(
                "SELECT vertex_id, properties FROM " + ks + ".vertices " +
                "WHERE token(vertex_id) >= ? LIMIT ?",
                randomToken, perProbe));

            for (Row row : rs) {
                if (checked >= sampleSize) break;
                checked++;

                String propsJson = row.getString("properties");
                if (propsJson == null) continue;

                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> props = MAPPER.readValue(propsJson, Map.class);
                    for (String key : props.keySet()) {
                        if (isCorruptedPropertyName(key)) {
                            corrupted++;
                            addSampleFailure(sampleFailures,
                                row.getString("vertex_id") + ": " + key);
                            break; // one corruption per vertex is enough
                        }
                    }
                } catch (Exception e) {
                    // JSON parse errors handled in deep vertex check
                }
            }
        }

        ValidationCheckResult result = new ValidationCheckResult(
            "property_corruption",
            "No __type.Asset.* / __type.Referenceable.* corrupted property names",
            corrupted == 0 ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.FAIL,
            String.format("checked=%d, corrupted=%d", checked, corrupted));
        result.addDetail("checked", checked);
        result.addDetail("corrupted", corrupted);
        for (String f : sampleFailures) result.addSampleFailure(f);
        report.addCheck(result);
    }

    // ========================================================================
    // Check 9: ES document count comparison
    // ========================================================================

    private void runEsCountCheck(ValidationReport report) {
        LOG.info("ES count check: comparing source ES index vs target Cassandra vertices...");

        RestClient esClient = null;
        try {
            esClient = createEsClient();

            // Count source ES index (the original JanusGraph index — the ground truth)
            String sourceIndex = config.getSourceEsIndex();
            long sourceEsCount = getEsIndexCount(esClient, sourceIndex);

            // Count target ES index (may not exist yet if migration hasn't run Phase 2)
            String targetIndex = config.getTargetEsIndex();
            long targetEsCount = getEsIndexCount(esClient, targetIndex);

            long cassandraCount = report.getVertexCount();
            report.setEsDocCount(sourceEsCount);

            LOG.info("ES counts: source_index({})={}, target_index({})={}, cassandra_vertices={}",
                     sourceIndex, sourceEsCount, targetIndex, targetEsCount, cassandraCount);

            // Primary comparison: source ES vs target Cassandra
            // Cassandra should have >= source ES docs (it also includes system vertices)
            String message;
            ValidationCheckResult.Severity severity;

            if (sourceEsCount == 0) {
                severity = ValidationCheckResult.Severity.FAIL;
                message = String.format("source_es(%s)=0 — cannot validate", sourceIndex);
            } else {
                double cassandraVsSourceRatio = (double) cassandraCount / sourceEsCount;
                severity = cassandraVsSourceRatio >= 1.0 ? ValidationCheckResult.Severity.PASS :
                           cassandraVsSourceRatio > 0.9  ? ValidationCheckResult.Severity.WARN :
                                                            ValidationCheckResult.Severity.FAIL;
                message = String.format(
                    "source_es(%s)=%d, target_cassandra=%d, ratio=%.2f%%; target_es(%s)=%d",
                    sourceIndex, sourceEsCount, cassandraCount,
                    cassandraVsSourceRatio * 100, targetIndex, targetEsCount);
            }

            ValidationCheckResult result = new ValidationCheckResult(
                "es_vertex_count",
                "Source ES doc count vs target Cassandra vertex count",
                severity, message);
            result.addDetail("source_es_index", sourceIndex);
            result.addDetail("source_es_count", sourceEsCount);
            result.addDetail("target_es_index", targetIndex);
            result.addDetail("target_es_count", targetEsCount);
            result.addDetail("cassandra_count", cassandraCount);
            report.addCheck(result);
        } catch (Exception e) {
            LOG.warn("ES count check failed: {}", e.getMessage());
            ValidationCheckResult result = new ValidationCheckResult(
                "es_vertex_count",
                "Source ES doc count vs target Cassandra vertex count",
                ValidationCheckResult.Severity.FAIL,
                "ES unreachable: " + e.getMessage());
            report.addCheck(result);
        } finally {
            if (esClient != null) {
                try { esClient.close(); } catch (Exception ignored) {}
            }
        }
    }

    private long getEsIndexCount(RestClient esClient, String indexName) {
        try {
            Request countReq = new Request("GET", "/" + indexName + "/_count");
            Response resp = esClient.performRequest(countReq);
            String body = EntityUtils.toString(resp.getEntity());
            @SuppressWarnings("unchecked")
            Map<String, Object> parsed = MAPPER.readValue(body, Map.class);
            return ((Number) parsed.get("count")).longValue();
        } catch (Exception e) {
            LOG.warn("Failed to count ES index '{}': {}", indexName, e.getMessage());
            return -1;
        }
    }

    // ========================================================================
    // Check 17: ES connector aggregation (product readiness)
    // ========================================================================

    private void runEsConnectorAggregationCheck(ValidationReport report) {
        String targetIndex = config.getTargetEsIndex();
        LOG.info("ES connector aggregation check: querying {}/{}...", targetIndex, "_search");

        RestClient esClient = null;
        try {
            esClient = createEsClient();

            Request searchReq = new Request("POST", "/" + targetIndex + "/_search");
            searchReq.setJsonEntity(ES_CONNECTOR_AGG_QUERY);
            Response resp = esClient.performRequest(searchReq);

            int statusCode = resp.getStatusLine().getStatusCode();
            String responseBody = EntityUtils.toString(resp.getEntity());

            @SuppressWarnings("unchecked")
            Map<String, Object> parsed = MAPPER.readValue(responseBody, Map.class);

            // Extract hits.total.value
            @SuppressWarnings("unchecked")
            Map<String, Object> hits = (Map<String, Object>) parsed.get("hits");
            long totalHits = 0;
            if (hits != null) {
                Object total = hits.get("total");
                if (total instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> totalMap = (Map<String, Object>) total;
                    totalHits = ((Number) totalMap.get("value")).longValue();
                } else if (total instanceof Number) {
                    totalHits = ((Number) total).longValue();
                }
            }

            if (totalHits == 0) {
                // FAIL: no searchable assets — product search will be empty after cutover
                ValidationCheckResult result = new ValidationCheckResult(
                    "es_connector_aggregation",
                    "Product connector aggregation query returns assets from target ES",
                    ValidationCheckResult.Severity.FAIL,
                    "ES connector aggregation returned 0 results — product search will be empty after cutover");
                result.addDetail("target_index", targetIndex);
                result.addDetail("total_hits", 0);
                report.addCheck(result);
                return;
            }

            // PASS: extract per-connector breakdown from aggregations.by_connector.buckets
            @SuppressWarnings("unchecked")
            Map<String, Object> aggs = (Map<String, Object>) parsed.get("aggregations");
            Map<String, Long> connectorCounts = new LinkedHashMap<>();
            if (aggs != null) {
                @SuppressWarnings("unchecked")
                Map<String, Object> byConnector = (Map<String, Object>) aggs.get("by_connector");
                if (byConnector != null) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> buckets = (List<Map<String, Object>>) byConnector.get("buckets");
                    if (buckets != null) {
                        for (Map<String, Object> bucket : buckets) {
                            String key = String.valueOf(bucket.get("key"));
                            long docCount = ((Number) bucket.get("doc_count")).longValue();
                            connectorCounts.put(key, docCount);
                        }
                    }
                }
            }

            LOG.info("ES connector aggregation: total_hits={}, connectors={}", totalHits, connectorCounts.size());
            for (Map.Entry<String, Long> entry : connectorCounts.entrySet()) {
                LOG.info("  connector '{}': {} assets", entry.getKey(), String.format("%,d", entry.getValue()));
            }

            ValidationCheckResult result = new ValidationCheckResult(
                "es_connector_aggregation",
                "Product connector aggregation query returns assets from target ES",
                ValidationCheckResult.Severity.PASS,
                String.format("total_hits=%d, connectors=%d", totalHits, connectorCounts.size()));
            result.addDetail("target_index", targetIndex);
            result.addDetail("total_hits", totalHits);
            result.addDetail("connector_count", connectorCounts.size());
            for (Map.Entry<String, Long> entry : connectorCounts.entrySet()) {
                result.addDetail("connector_" + entry.getKey(), entry.getValue());
            }
            report.addCheck(result);

        } catch (ResponseException re) {
            // HTTP error response (e.g. 400 Bad Request)
            int statusCode = re.getResponse().getStatusLine().getStatusCode();
            String errorBody;
            try {
                errorBody = EntityUtils.toString(re.getResponse().getEntity());
            } catch (Exception ex) {
                errorBody = re.getMessage();
            }

            String failMessage;
            if (statusCode == 400) {
                failMessage = String.format(
                    "Product would not load if we pass. Request: %s. Response: %s. The above request returned 400.",
                    ES_CONNECTOR_AGG_QUERY, errorBody);
            } else {
                failMessage = String.format(
                    "ES connector aggregation returned HTTP %d. Response: %s", statusCode, errorBody);
            }

            LOG.error("ES connector aggregation check failed (HTTP {}): {}", statusCode, errorBody);
            ValidationCheckResult result = new ValidationCheckResult(
                "es_connector_aggregation",
                "Product connector aggregation query returns assets from target ES",
                ValidationCheckResult.Severity.FAIL,
                failMessage);
            result.addDetail("target_index", targetIndex);
            result.addDetail("http_status", statusCode);
            report.addCheck(result);

        } catch (IOException e) {
            LOG.error("ES connector aggregation check failed: {}", e.getMessage(), e);
            ValidationCheckResult result = new ValidationCheckResult(
                "es_connector_aggregation",
                "Product connector aggregation query returns assets from target ES",
                ValidationCheckResult.Severity.FAIL,
                "ES connector aggregation check failed: " + e.getMessage());
            result.addDetail("target_index", targetIndex);
            report.addCheck(result);
        } finally {
            if (esClient != null) {
                try { esClient.close(); } catch (Exception ignored) {}
            }
        }
    }

    // ========================================================================
    // Check 10: Orphan edge detection
    // ========================================================================

    private void runOrphanEdgeCheck(String ks, ValidationReport report) {
        int sampleSize = config.getValidationEdgeSampleSize();
        LOG.info("Orphan edge check: sampling {} edges from edges_by_id...", sampleSize);

        int checked = 0, orphanEdges = 0;
        List<String> sampleFailures = new ArrayList<>();

        long randomToken = ThreadLocalRandom.current().nextLong();
        ResultSet rs = targetSession.execute(SimpleStatement.newInstance(
            "SELECT edge_id, out_vertex_id, in_vertex_id FROM " + ks + ".edges_by_id " +
            "WHERE token(edge_id) >= ? LIMIT ?",
            randomToken, sampleSize));

        for (Row row : rs) {
            checked++;
            String edgeId    = row.getString("edge_id");
            String outVid    = row.getString("out_vertex_id");
            String inVid     = row.getString("in_vertex_id");

            ResultSet outRs = targetSession.execute(SimpleStatement.newInstance(
                "SELECT vertex_id FROM " + ks + ".vertices WHERE vertex_id = ?", outVid));
            ResultSet inRs = targetSession.execute(SimpleStatement.newInstance(
                "SELECT vertex_id FROM " + ks + ".vertices WHERE vertex_id = ?", inVid));

            boolean outExists = outRs.one() != null;
            boolean inExists  = inRs.one() != null;

            if (!outExists || !inExists) {
                orphanEdges++;
                addSampleFailure(sampleFailures,
                    String.format("edge=%s out=%s(%s) in=%s(%s)",
                        edgeId, outVid, outExists ? "ok" : "MISSING",
                        inVid, inExists ? "ok" : "MISSING"));
            }
        }

        ValidationCheckResult result = new ValidationCheckResult(
            "orphan_edge_detection",
            "Both endpoints of sampled edges exist in vertices table",
            orphanEdges == 0 ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.FAIL,
            String.format("checked=%d, orphan=%d", checked, orphanEdges));
        result.addDetail("checked", checked);
        result.addDetail("orphan_edges", orphanEdges);
        for (String f : sampleFailures) result.addSampleFailure(f);
        report.addCheck(result);
    }

    // ========================================================================
    // Check 11: Super vertex detection
    // ========================================================================

    private void runSuperVertexDetection(String ks, ValidationReport report) {
        LOG.info("Super vertex detection: threshold={}, topN={}...",
                 config.getSuperVertexThreshold(), config.getSuperVertexTopN());

        SuperVertexDetector detector = new SuperVertexDetector(
            targetSession, ks,
            config.getSuperVertexThreshold(),
            config.getSuperVertexTopN());

        SuperVertexReport svReport = detector.detect();
        report.setSuperVertexReport(svReport);

        // Log top super vertices
        for (SuperVertexReport.SuperVertexEntry entry : svReport.getTopSuperVertices()) {
            LOG.info("  Super vertex: id={}, type={}, edges={}, labels={}",
                     entry.getVertexId(), entry.getTypeName(),
                     String.format("%,d", entry.getEdgeCount()), entry.getEdgeLabelCounts());
        }

        // This is informational — always passes
        ValidationCheckResult result = new ValidationCheckResult(
            "super_vertex_detection",
            "Super vertex detection completed",
            ValidationCheckResult.Severity.PASS,
            String.format("found=%d super vertices (threshold=%d), max_edges=%d, scanned=%d vertices",
                          svReport.getTotalSuperVertexCount(),
                          config.getSuperVertexThreshold(),
                          svReport.getMaxEdgeCount(),
                          svReport.getTotalVerticesScanned()));
        result.addDetail("total_super_vertices", svReport.getTotalSuperVertexCount());
        result.addDetail("max_edge_count", svReport.getMaxEdgeCount());
        result.addDetail("vertices_over_1k", svReport.getVerticesOver1kEdges());
        result.addDetail("vertices_over_10k", svReport.getVerticesOver10kEdges());
        result.addDetail("vertices_over_100k", svReport.getVerticesOver100kEdges());
        result.addDetail("vertices_over_1m", svReport.getVerticesOver1mEdges());
        result.addDetail("scan_duration_ms", svReport.getScanDurationMs());
        report.addCheck(result);
    }

    // ========================================================================
    // Check 12: Edge index table count
    // ========================================================================

    private void runEdgeIndexCheck(String ks, ValidationReport report) {
        LOG.info("Edge index check: counting rows in {}.edge_index...", ks);

        long edgeIndexCount = countTable(ks + ".edge_index");
        report.setEdgeIndexCount(edgeIndexCount);

        // Edge index should have at least as many entries as there are relationship edges
        // (each edge with a __relationshipGuid gets one entry). A count of 0 is a problem
        // if there are edges in the graph.
        long edgeOutCount = report.getEdgeOutCount();
        boolean passed = edgeIndexCount > 0 || edgeOutCount == 0;

        String message = String.format("edge_index=%d (estimated), edges_out=%d (estimated)", edgeIndexCount, edgeOutCount);
        if (edgeOutCount > 0 && edgeIndexCount == 0) {
            message += " [FAIL: edge_index is empty but edges exist — relationship GUID lookups will fail]";
        }

        ValidationCheckResult result = new ValidationCheckResult(
            "edge_index_count",
            "Edge index table populated for relationship GUID lookups (estimated via size_estimates)",
            passed ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.FAIL,
            message);
        result.addDetail("edge_index_count", edgeIndexCount);
        result.addDetail("edges_out_count", edgeOutCount);
        report.addCheck(result);
    }

    // ========================================================================
    // Check 12b: ES edge index doc count
    // ========================================================================

    private void runEsEdgeCountCheck(String ks, ValidationReport report) {
        String esEdgeIndex = config.getTargetEsEdgeIndex();
        LOG.info("ES edge count check: comparing ES edge index '{}' vs Cassandra edges_by_id...", esEdgeIndex);

        RestClient esClient = null;
        try {
            esClient = createEsClient();

            long esEdgeCount = getEsIndexCount(esClient, esEdgeIndex);
            long edgeByIdCount = report.getEdgeByIdCount();
            if (edgeByIdCount <= 0) {
                edgeByIdCount = countTable(ks + ".edges_by_id");
            }

            LOG.info("ES edge counts: es_edge_index({})={}, edges_by_id={}",
                     esEdgeIndex, esEdgeCount, edgeByIdCount);

            String message;
            ValidationCheckResult.Severity severity;

            if (esEdgeCount < 0) {
                severity = ValidationCheckResult.Severity.WARN;
                message = String.format("ES edge index '%s' not found or not accessible — " +
                    "directRelationshipIndexSearch will not work post-cutover", esEdgeIndex);
            } else if (esEdgeCount == 0 && edgeByIdCount > 0) {
                severity = ValidationCheckResult.Severity.FAIL;
                message = String.format("ES edge index '%s' is empty (0 docs) but %d edges exist — " +
                    "directRelationshipIndexSearch will be broken post-cutover",
                    esEdgeIndex, edgeByIdCount);
            } else if (edgeByIdCount > 0) {
                double ratio = (double) esEdgeCount / edgeByIdCount;
                // Allow some tolerance since size_estimates for edges_by_id may be inaccurate
                severity = ratio >= 0.90 ? ValidationCheckResult.Severity.PASS :
                           ratio >= 0.50 ? ValidationCheckResult.Severity.WARN :
                                           ValidationCheckResult.Severity.FAIL;
                message = String.format("es_edge_index(%s)=%d, edges_by_id=%d (estimated), ratio=%.2f%%",
                    esEdgeIndex, esEdgeCount, edgeByIdCount, ratio * 100);
            } else {
                severity = ValidationCheckResult.Severity.PASS;
                message = String.format("es_edge_index(%s)=%d, edges_by_id=%d (both match: no edges)",
                    esEdgeIndex, esEdgeCount, edgeByIdCount);
            }

            ValidationCheckResult result = new ValidationCheckResult(
                "es_edge_count",
                "ES edge index populated for directRelationshipIndexSearch",
                severity, message);
            result.addDetail("es_edge_index", esEdgeIndex);
            result.addDetail("es_edge_count", esEdgeCount);
            result.addDetail("edges_by_id_count", edgeByIdCount);
            report.addCheck(result);

        } catch (Exception e) {
            LOG.warn("ES edge count check failed: {}", e.getMessage());
            ValidationCheckResult result = new ValidationCheckResult(
                "es_edge_count",
                "ES edge index populated for directRelationshipIndexSearch",
                ValidationCheckResult.Severity.WARN,
                "ES edge count check failed: " + e.getMessage());
            report.addCheck(result);
        } finally {
            if (esClient != null) {
                try { esClient.close(); } catch (Exception ignored) {}
            }
        }
    }

    // ========================================================================
    // Check 13: config_store accessibility
    // ========================================================================

    private void runConfigStoreCheck(ValidationReport report) {
        if (config.isSameCassandraCluster() || !config.isMigrateConfigStore()) {
            LOG.info("config_store check: skipped (sameCluster={}, migrateConfigStore={})",
                     config.isSameCassandraCluster(), config.isMigrateConfigStore());
            report.addCheck(new ValidationCheckResult(
                "config_store_accessible",
                "config_store.configs is readable on target Cassandra",
                ValidationCheckResult.Severity.PASS,
                "skipped (config_store migration not applicable)"));
            return;
        }

        LOG.info("config_store check: verifying accessibility on target...");

        long count = -1;
        String message;
        ValidationCheckResult.Severity severity;

        try {
            ResultSet rs = targetSession.execute(
                SimpleStatement.builder("SELECT count(*) FROM config_store.configs")
                    .setTimeout(java.time.Duration.ofSeconds(30))
                    .build());
            Row row = rs.one();
            count = row != null ? row.getLong(0) : 0;
            report.setConfigStoreEntryCount(count);

            severity = count > 0 ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.WARN;
            message = String.format("config_store.configs accessible, entries=%d", count);
            if (count == 0) {
                message += " [WARN: config_store is empty — dynamic configs may not work]";
            }
        } catch (Exception e) {
            severity = ValidationCheckResult.Severity.WARN;
            message = "config_store.configs not accessible: " + e.getMessage();
            LOG.warn("config_store check: {}", message);
        }

        ValidationCheckResult result = new ValidationCheckResult(
            "config_store_accessible",
            "config_store.configs is readable on target Cassandra",
            severity, message);
        if (count >= 0) result.addDetail("config_store_entry_count", count);
        report.addCheck(result);
    }

    // ========================================================================
    // Check 14: tags tables accessibility
    // ========================================================================

    private void runTagsTablesCheck(ValidationReport report) {
        if (config.isSameCassandraCluster() || !config.isMigrateTags()) {
            LOG.info("tags tables check: skipped (sameCluster={}, migrateTags={})",
                     config.isSameCassandraCluster(), config.isMigrateTags());
            report.addCheck(new ValidationCheckResult(
                "tags_tables_accessible",
                "tags keyspace tables are readable on target Cassandra",
                ValidationCheckResult.Severity.PASS,
                "skipped (tags migration not applicable)"));
            return;
        }

        LOG.info("tags tables check: verifying accessibility on target...");

        long tagsCount = -1;
        long propagatedCount = -1;
        List<String> issues = new ArrayList<>();

        try {
            ResultSet rs = targetSession.execute(
                SimpleStatement.builder("SELECT count(*) FROM tags.tags_by_id")
                    .setTimeout(java.time.Duration.ofSeconds(30))
                    .build());
            Row row = rs.one();
            tagsCount = row != null ? row.getLong(0) : 0;
            report.setTagsCount(tagsCount);
        } catch (Exception e) {
            issues.add("tags.tags_by_id not accessible: " + e.getMessage());
            LOG.warn("tags check: tags_by_id not accessible: {}", e.getMessage());
        }

        try {
            ResultSet rs = targetSession.execute(
                SimpleStatement.builder("SELECT count(*) FROM tags.propagated_tags_by_source")
                    .setTimeout(java.time.Duration.ofSeconds(30))
                    .build());
            Row row = rs.one();
            propagatedCount = row != null ? row.getLong(0) : 0;
            report.setPropagatedTagsCount(propagatedCount);
        } catch (Exception e) {
            issues.add("tags.propagated_tags_by_source not accessible: " + e.getMessage());
            LOG.warn("tags check: propagated_tags_by_source not accessible: {}", e.getMessage());
        }

        ValidationCheckResult.Severity severity;
        String message;

        if (issues.isEmpty()) {
            severity = (tagsCount > 0 || propagatedCount > 0)
                ? ValidationCheckResult.Severity.PASS
                : ValidationCheckResult.Severity.WARN;
            message = String.format("tags_by_id=%d, propagated_tags_by_source=%d", tagsCount, propagatedCount);
            if (tagsCount == 0 && propagatedCount == 0) {
                message += " [WARN: both tags tables are empty]";
            }
        } else {
            severity = ValidationCheckResult.Severity.WARN;
            message = String.join("; ", issues);
        }

        ValidationCheckResult result = new ValidationCheckResult(
            "tags_tables_accessible",
            "tags keyspace tables are readable on target Cassandra",
            severity, message);
        if (tagsCount >= 0) result.addDetail("tags_by_id_count", tagsCount);
        if (propagatedCount >= 0) result.addDetail("propagated_tags_count", propagatedCount);
        report.addCheck(result);
    }

    // ========================================================================
    // Check 15: Token range completion
    // ========================================================================

    private void runTokenRangeCompletionCheck(ValidationReport report) {
        LOG.info("Token range completion check: verifying all scan ranges are COMPLETED...");

        Map<String, Long> statusCounts = stateStore.getStatusCounts("scan");
        long completed  = statusCounts.getOrDefault("COMPLETED", 0L);
        long failed     = statusCounts.getOrDefault("FAILED", 0L);
        long inProgress = statusCounts.getOrDefault("IN_PROGRESS", 0L);
        long total      = statusCounts.values().stream().mapToLong(Long::longValue).sum();

        // Cross-check: compare tracked range count against the expected count
        // saved during the scan phase. If scanner threads changed between runs,
        // old completed ranges may be from a different token range partition.
        String savedExpected = stateStore.loadMetadata("scan_total_ranges");
        int expectedRanges = savedExpected != null ? Integer.parseInt(savedExpected.trim()) : -1;
        boolean rangeMismatch = expectedRanges > 0 && total != expectedRanges;

        ValidationCheckResult.Severity severity;
        String message;

        if (total == 0) {
            severity = ValidationCheckResult.Severity.FAIL;
            message = "No token ranges found in migration_state — migration may not have run";
        } else if (failed > 0) {
            severity = ValidationCheckResult.Severity.FAIL;
            message = String.format("total=%d, COMPLETED=%d, FAILED=%d, IN_PROGRESS=%d " +
                "[FAIL: %d ranges failed — data may be incomplete]", total, completed, failed, inProgress, failed);
        } else if (inProgress > 0) {
            severity = ValidationCheckResult.Severity.FAIL;
            message = String.format("total=%d, COMPLETED=%d, FAILED=%d, IN_PROGRESS=%d " +
                "[FAIL: %d ranges still in progress — migration did not finish cleanly]",
                total, completed, failed, inProgress, inProgress);
        } else if (completed == total && rangeMismatch) {
            severity = ValidationCheckResult.Severity.WARN;
            message = String.format("All %d tracked ranges COMPLETED, but expected %d based on scan config " +
                "[WARN: ranges may be from a different scanner thread configuration — consider re-running with --fresh]",
                total, expectedRanges);
        } else if (completed == total) {
            severity = ValidationCheckResult.Severity.PASS;
            message = String.format("All %d token ranges COMPLETED", total);
            if (expectedRanges > 0) {
                message += String.format(" (matches expected %d)", expectedRanges);
            }
        } else {
            severity = ValidationCheckResult.Severity.WARN;
            message = String.format("total=%d, COMPLETED=%d — unexpected statuses: %s", total, completed, statusCounts);
        }

        LOG.info("Token range completion: {}", message);

        ValidationCheckResult result = new ValidationCheckResult(
            "token_range_completion",
            "All migration token ranges completed successfully",
            severity, message);
        result.addDetail("total_ranges", total);
        result.addDetail("completed", completed);
        result.addDetail("failed", failed);
        result.addDetail("in_progress", inProgress);
        if (expectedRanges > 0) {
            result.addDetail("expected_ranges", expectedRanges);
        }
        for (Map.Entry<String, Long> entry : statusCounts.entrySet()) {
            result.addDetail("status_" + entry.getKey().toLowerCase(), entry.getValue());
        }
        report.addCheck(result);
    }

    // ========================================================================
    // Check 16: Disk space adequacy
    // ========================================================================

    private void runDiskSpaceCheck(ValidationReport report) {
        LOG.info("Disk space check: verifying ES and Cassandra have adequate free space...");

        List<String> issues = new ArrayList<>();
        boolean esPassed = true;
        boolean cassandraPassed = true;

        // Check ES disk space via _cat/allocation API
        RestClient esClient = null;
        try {
            esClient = createEsClient();
            Request req = new Request("GET", "/_cat/allocation?format=json&bytes=b");
            Response resp = esClient.performRequest(req);
            String body = EntityUtils.toString(resp.getEntity());

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodes = MAPPER.readValue(body,
                MAPPER.getTypeFactory().constructCollectionType(List.class, Map.class));

            for (Map<String, Object> node : nodes) {
                String diskUsedStr = String.valueOf(node.getOrDefault("disk.used", "0"));
                String diskAvailStr = String.valueOf(node.getOrDefault("disk.avail", "0"));
                String nodeName = String.valueOf(node.getOrDefault("node", "unknown"));

                long diskUsed  = parseLong(diskUsedStr);
                long diskAvail = parseLong(diskAvailStr);

                if (diskUsed > 0 && diskAvail < 2 * diskUsed) {
                    esPassed = false;
                    issues.add(String.format("ES node '%s': used=%s, avail=%s (need 2x used = %s)",
                        nodeName, humanBytes(diskUsed), humanBytes(diskAvail), humanBytes(2 * diskUsed)));
                }
            }
        } catch (Exception e) {
            LOG.warn("ES disk space check failed: {}", e.getMessage());
            issues.add("ES disk check failed: " + e.getMessage());
            esPassed = false;
        } finally {
            if (esClient != null) {
                try { esClient.close(); } catch (Exception ignored) {}
            }
        }

        // Check Cassandra disk space via system keyspace
        try {
            // Query system.local for data_file_directories info
            // Use size_estimates as a proxy for Cassandra data size
            String ks = config.getTargetCassandraKeyspace();
            ResultSet rs = targetSession.execute(
                "SELECT mean_partition_size, partitions_count FROM system.size_estimates " +
                "WHERE keyspace_name = '" + config.getSourceCassandraKeyspace() + "'");

            long totalEstimatedBytes = 0;
            for (Row row : rs) {
                long meanSize = row.getLong("mean_partition_size");
                long partCount = row.getLong("partitions_count");
                totalEstimatedBytes += meanSize * partCount;
            }

            if (totalEstimatedBytes > 0) {
                LOG.info("Cassandra source keyspace estimated size: {}", humanBytes(totalEstimatedBytes));
                // Note: We can't directly query free disk from CQL. Log the estimate for operator review.
                // The shell script handles the actual df check on the Cassandra pod.
            }
        } catch (Exception e) {
            LOG.warn("Cassandra size estimation failed: {}", e.getMessage());
            issues.add("Cassandra size check: " + e.getMessage());
        }

        // Disk checks are advisory only — never block migration
        ValidationCheckResult.Severity severity = issues.isEmpty()
            ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.WARN;
        String message = issues.isEmpty()
            ? "ES nodes have >= 2x free disk space relative to used"
            : "[WARNING] " + String.join("; ", issues);

        ValidationCheckResult result = new ValidationCheckResult(
            "disk_space_adequacy",
            "ES and Cassandra disk space advisory (warning only, does not block migration)",
            severity, message);
        result.addDetail("es_passed", esPassed);
        result.addDetail("cassandra_passed", cassandraPassed);
        report.addCheck(result);
    }

    private static long parseLong(String s) {
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String humanBytes(long bytes) {
        if (bytes < 1024) return bytes + "B";
        if (bytes < 1024 * 1024) return String.format("%.1fKB", bytes / 1024.0);
        if (bytes < 1024L * 1024 * 1024) return String.format("%.1fMB", bytes / (1024.0 * 1024));
        return String.format("%.1fGB", bytes / (1024.0 * 1024 * 1024));
    }

    // ========================================================================
    // Source-side counting (direct query against source Cassandra/ES)
    // ========================================================================

    private void runSourceCounts(ValidationReport report) {
        if (sourceSession == null) {
            LOG.info("Source session not available — skipping source-side counts.");
            return;
        }

        String sourceKs = config.getSourceCassandraKeyspace();
        String edgestoreTable = config.getSourceEdgestoreTable();

        // Estimate source edgestore rows via system.size_estimates (instant, ~95% accurate)
        long sourceEdgestoreCount = -1;
        try {
            ResultSet rs = sourceSession.execute(
                SimpleStatement.builder(
                    "SELECT partitions_count FROM system.size_estimates " +
                    "WHERE keyspace_name = ? AND table_name = ?")
                    .addPositionalValue(sourceKs)
                    .addPositionalValue(edgestoreTable)
                    .setTimeout(java.time.Duration.ofSeconds(30))
                    .build());
            for (Row row : rs) {
                sourceEdgestoreCount = (sourceEdgestoreCount < 0 ? 0 : sourceEdgestoreCount)
                                       + row.getLong("partitions_count");
            }
            LOG.info("Source edgestore estimated rows ({}.{}): {}", sourceKs, edgestoreTable,
                     String.format("%,d", sourceEdgestoreCount));
        } catch (Exception e) {
            LOG.warn("Failed to estimate source edgestore ({}.{}): {}",
                     sourceKs, edgestoreTable, e.getMessage());
        }
        report.setSourceEdgestoreCount(sourceEdgestoreCount);

        // Count source ES docs (janusgraph_vertex_index)
        long sourceEsDocCount = -1;
        RestClient esClient = null;
        try {
            esClient = createEsClient();
            String sourceIndex = config.getSourceEsIndex();
            sourceEsDocCount = getEsIndexCount(esClient, sourceIndex);
            LOG.info("Source ES docs ({}): {}", sourceIndex,
                     String.format("%,d", sourceEsDocCount));
        } catch (Exception e) {
            LOG.warn("Failed to count source ES index: {}", e.getMessage());
        } finally {
            if (esClient != null) {
                try { esClient.close(); } catch (Exception ignored) {}
            }
        }
        report.setSourceEsDocCount(sourceEsDocCount);
    }

    // ========================================================================
    // Utilities
    // ========================================================================

    /**
     * Estimate table row count using system.size_estimates.
     * This is instant even for tables with millions of rows, unlike SELECT count(*).
     * <p>
     * WARNING: Accuracy is only ~95% in steady state — immediately after bulk writes
     * (e.g. migration), estimates can be wildly inaccurate (8-30% of actual) because
     * data in memtables or un-compacted SSTables is underrepresented.
     * Prefer actual counts from SuperVertexDetector full scans when available.
     */
    private long countTable(String fullyQualifiedTable) {
        // Parse "keyspace.table" format
        int dot = fullyQualifiedTable.indexOf('.');
        if (dot < 0) {
            LOG.warn("Invalid table name (expected keyspace.table): {}", fullyQualifiedTable);
            return -1;
        }
        String keyspace = fullyQualifiedTable.substring(0, dot);
        String table    = fullyQualifiedTable.substring(dot + 1);

        try {
            ResultSet rs = targetSession.execute(
                SimpleStatement.builder(
                    "SELECT partitions_count FROM system.size_estimates " +
                    "WHERE keyspace_name = ? AND table_name = ?")
                    .addPositionalValue(keyspace)
                    .addPositionalValue(table)
                    .setTimeout(java.time.Duration.ofSeconds(30))
                    .build());

            long totalPartitions = 0;
            for (Row row : rs) {
                totalPartitions += row.getLong("partitions_count");
            }

            LOG.info("Estimated row count for {}: {} (via system.size_estimates)", fullyQualifiedTable,
                     String.format("%,d", totalPartitions));
            return totalPartitions;
        } catch (Exception e) {
            LOG.warn("Failed to estimate count for {} via size_estimates: {}", fullyQualifiedTable, e.getMessage());
            return -1;
        }
    }

    /**
     * Exact table row count via paginated scan.
     * Iterates all rows counting client-side — avoids coordinator-level count(*)
     * which can timeout on some Cassandra configurations.
     * Fast for small tables (type_definitions ~hundreds of rows).
     */
    private long scanTableCount(String fullyQualifiedTable) {
        try {
            long count = 0;
            ResultSet rs = targetSession.execute(
                SimpleStatement.builder("SELECT * FROM " + fullyQualifiedTable)
                    .setPageSize(5000)
                    .build());
            for (Row row : rs) {
                count++;
            }
            LOG.info("Scanned row count for {}: {}", fullyQualifiedTable, String.format("%,d", count));
            return count;
        } catch (Exception e) {
            LOG.warn("Failed to scan-count {}: {}", fullyQualifiedTable, e.getMessage());
            return -1;
        }
    }

    /** Check if two values are within the given tolerance (e.g. 0.05 = 5%). Handles zero/negative. */
    private static boolean isWithinTolerance(long a, long b, double tolerance) {
        if (a < 0 || b < 0) return false;
        if (a == 0 && b == 0) return true;
        long max = Math.max(a, b);
        if (max == 0) return true;
        double diff = Math.abs(a - b);
        return (diff / max) <= tolerance;
    }

    private static String getStringProp(Map<String, Object> props, String key) {
        Object val = props.get(key);
        return val != null ? val.toString() : null;
    }

    private static boolean isCorruptedPropertyName(String key) {
        return key.startsWith("__type.Asset.") ||
               key.startsWith("__type.Referenceable.") ||
               key.startsWith("__type.SQL.") ||
               key.startsWith("__type.Catalog.") ||
               key.startsWith("__type.Process.") ||
               key.startsWith("__type.DataSet.") ||
               key.startsWith("__type.Infrastructure.");
    }

    private static void addSampleFailure(List<String> failures, String failure) {
        if (failures.size() < 10) {
            failures.add(failure);
        }
    }

    private RestClient createEsClient() {
        RestClientBuilder builder = RestClient.builder(
            new HttpHost(config.getTargetEsHostname(), config.getTargetEsPort(), config.getTargetEsProtocol()));

        String username = config.getTargetEsUsername();
        String password = config.getTargetEsPassword();
        if (username != null && !username.isEmpty()) {
            BasicCredentialsProvider creds = new BasicCredentialsProvider();
            creds.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(b -> b.setDefaultCredentialsProvider(creds));
        }

        builder.setRequestConfigCallback(b -> b
            .setConnectTimeout(10_000)
            .setSocketTimeout(30_000));

        return builder.build();
    }
}
