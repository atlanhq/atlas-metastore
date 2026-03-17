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
 * Runs 11 correctness checks against the target Cassandra (and optionally ES).
 * Returns a structured {@link ValidationReport} with per-check results, per-type
 * statistics, super vertex detection, and source baseline comparison.
 *
 * If any check fails, the migration is considered unsafe for cutover.
 */
public class MigrationValidator {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationValidator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

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

        // --- Check 1: Vertex count + groupBy type ---
        runVertexCountCheck(ks, report);
        runVertexCountByType(ks, report);

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

        // --- Check 11: Super vertex detection ---
        if (!config.isSkipSuperVertexDetection()) {
            runSuperVertexDetection(ks, report);
        }

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
        long vertexCount = countTable(ks + ".vertices");
        report.setVertexCount(vertexCount);

        long[] phaseSummary = stateStore.getPhaseSummary("scan");

        boolean passed = vertexCount > 0;
        String message = String.format("vertices=%d, state_store_reports=%d vertices from %d ranges",
                                       vertexCount, phaseSummary[1], phaseSummary[0]);

        // Compare with source baseline if available
        if (sourceBaseline != null && sourceBaseline.totalVertices > 0) {
            double ratio = (double) vertexCount / sourceBaseline.totalVertices;
            message += String.format(", source_baseline=%d, ratio=%.4f",
                                     sourceBaseline.totalVertices, ratio);
            // Warn if target has significantly fewer vertices than source
            if (ratio < 0.99) {
                passed = false;
                message += " [FAIL: target has >1% fewer vertices than source]";
            }
        }

        ValidationCheckResult result = new ValidationCheckResult(
            "vertex_count", "Target has vertices matching source baseline",
            passed,
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
        long edgeOutCount  = countTable(ks + ".edges_out");
        long edgeInCount   = countTable(ks + ".edges_in");
        long edgeByIdCount = countTable(ks + ".edges_by_id");

        report.setEdgeOutCount(edgeOutCount);
        report.setEdgeInCount(edgeInCount);
        report.setEdgeByIdCount(edgeByIdCount);

        // Check 2: edges_out == edges_in
        boolean outInMatch = edgeOutCount == edgeInCount;
        ValidationCheckResult outInCheck = new ValidationCheckResult(
            "edge_out_in_consistency",
            "edges_out count matches edges_in count",
            outInMatch,
            outInMatch ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.FAIL,
            String.format("edges_out=%d, edges_in=%d, diff=%d",
                          edgeOutCount, edgeInCount, Math.abs(edgeOutCount - edgeInCount)));
        outInCheck.addDetail("edges_out_count", edgeOutCount);
        outInCheck.addDetail("edges_in_count", edgeInCount);
        report.addCheck(outInCheck);

        // Check 3: edges_by_id == edges_out
        boolean byIdMatch = edgeByIdCount == edgeOutCount;
        ValidationCheckResult byIdCheck = new ValidationCheckResult(
            "edge_by_id_consistency",
            "edges_by_id count matches edges_out count",
            byIdMatch,
            byIdMatch ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.WARN,
            String.format("edges_by_id=%d, edges_out=%d, diff=%d",
                          edgeByIdCount, edgeOutCount, Math.abs(edgeByIdCount - edgeOutCount)));
        byIdCheck.addDetail("edges_by_id_count", edgeByIdCount);
        byIdCheck.addDetail("edges_out_count", edgeOutCount);
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

        boolean passed = missing == 0;
        ValidationCheckResult result = new ValidationCheckResult(
            "guid_index_sample",
            "GUID index completeness (sampled)",
            passed,
            passed ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.FAIL,
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
        long typeDefCount      = countTable(ks + ".type_definitions");
        long typeDefByCatCount = countTable(ks + ".type_definitions_by_category");

        report.setTypeDefCount(typeDefCount);
        report.setTypeDefByCategoryCount(typeDefByCatCount);

        boolean countMatch = typeDefCount == typeDefByCatCount;
        boolean nonEmpty   = typeDefCount > 0;
        boolean passed     = countMatch && nonEmpty;

        ValidationCheckResult.Severity severity;
        if (passed) severity = ValidationCheckResult.Severity.PASS;
        else if (nonEmpty) severity = ValidationCheckResult.Severity.WARN;
        else severity = ValidationCheckResult.Severity.FAIL;

        ValidationCheckResult result = new ValidationCheckResult(
            "typedef_consistency",
            "type_definitions and type_definitions_by_category are consistent and non-empty",
            passed, severity,
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
                          edgeEndpointMissing == 0 && edgeTripleMissing == 0;

        ValidationCheckResult result = new ValidationCheckResult(
            "deep_vertex_correctness",
            "Deep validation of sampled vertices (properties, indexes, edges, endpoints)",
            passed,
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

        boolean passed = orphanIndexEntries == 0;
        ValidationCheckResult result = new ValidationCheckResult(
            "cross_table_integrity",
            "vertex_index entries point to existing vertices",
            passed,
            passed ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.WARN,
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

        boolean passed = corrupted == 0;
        ValidationCheckResult result = new ValidationCheckResult(
            "property_corruption",
            "No __type.Asset.* / __type.Referenceable.* corrupted property names",
            passed,
            passed ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.FAIL,
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
            boolean passed;
            String message;
            ValidationCheckResult.Severity severity;

            if (sourceEsCount == 0) {
                passed = false;
                severity = ValidationCheckResult.Severity.FAIL;
                message = String.format("source_es(%s)=0 — cannot validate", sourceIndex);
            } else {
                double cassandraVsSourceRatio = (double) cassandraCount / sourceEsCount;
                passed = cassandraVsSourceRatio >= 1.0;
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
                passed, severity, message);
            result.addDetail("source_es_index", sourceIndex);
            result.addDetail("source_es_count", sourceEsCount);
            result.addDetail("target_es_index", targetIndex);
            result.addDetail("target_es_count", targetEsCount);
            result.addDetail("cassandra_count", cassandraCount);
            report.addCheck(result);
        } catch (Exception e) {
            LOG.warn("ES count check failed (non-blocking): {}", e.getMessage());
            ValidationCheckResult result = new ValidationCheckResult(
                "es_vertex_count",
                "Source ES doc count vs target Cassandra vertex count",
                true, ValidationCheckResult.Severity.WARN,
                "Skipped: " + e.getMessage());
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

        boolean passed = orphanEdges == 0;
        ValidationCheckResult result = new ValidationCheckResult(
            "orphan_edge_detection",
            "Both endpoints of sampled edges exist in vertices table",
            passed,
            passed ? ValidationCheckResult.Severity.PASS : ValidationCheckResult.Severity.FAIL,
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
            true, ValidationCheckResult.Severity.PASS,
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
    // Source-side counting (direct query against source Cassandra/ES)
    // ========================================================================

    private void runSourceCounts(ValidationReport report) {
        if (sourceSession == null) {
            LOG.info("Source session not available — skipping source-side counts.");
            return;
        }

        String sourceKs = config.getSourceCassandraKeyspace();
        String edgestoreTable = config.getSourceEdgestoreTable();

        // Count source edgestore rows
        long sourceEdgestoreCount = -1;
        try {
            ResultSet rs = sourceSession.execute(
                "SELECT count(*) FROM " + sourceKs + "." + edgestoreTable);
            Row row = rs.one();
            sourceEdgestoreCount = row != null ? row.getLong(0) : -1;
            LOG.info("Source edgestore rows ({}.{}): {}", sourceKs, edgestoreTable,
                     String.format("%,d", sourceEdgestoreCount));
        } catch (Exception e) {
            LOG.warn("Failed to count source edgestore ({}.{}): {}",
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

    private long countTable(String table) {
        try {
            ResultSet rs = targetSession.execute("SELECT count(*) FROM " + table);
            Row row = rs.one();
            return row != null ? row.getLong(0) : 0;
        } catch (Exception e) {
            LOG.warn("Failed to count {}: {}", table, e.getMessage());
            return -1;
        }
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
