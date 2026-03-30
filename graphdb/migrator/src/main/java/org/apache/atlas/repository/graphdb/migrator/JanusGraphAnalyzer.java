package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Pre-migration graph analysis for JanusGraph tenants.
 *
 * Analyzes a JanusGraph keyspace WITHOUT running any migration:
 *   - Vertex type distribution via ES (janusgraph_vertex_index)
 *   - Super vertex detection via CQL edgestore scan with JanusGraph EdgeSerializer
 *     decoding to get EXACT per-vertex edge counts (OUT+IN, matching CassandraGraph SuperVertexDetector)
 *
 * The edgestore table has schema: (key blob, column1 blob, value blob)
 * where each partition key = one vertex, each row = one edge or property.
 * We use EdgeSerializer.parseRelation() + type.isPropertyKey() to distinguish
 * edges from properties, counting both OUT and IN edges per vertex for total degree.
 */
public class JanusGraphAnalyzer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JanusGraphAnalyzer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Prefix used by Atlas for JanusGraph properties in atlas-application.properties */
    private static final String ATLAS_GRAPH_PREFIX = "atlas.graph.";

    private final CqlSession sourceSession;
    private final RestClient esClient;
    private final MigratorConfig config;

    // JanusGraph internals for decoding (same pattern as JanusGraphScanner)
    private final StandardJanusGraph  janusGraph;
    private final IDManager           idManager;
    private final EdgeSerializer      edgeSerializer;
    private final CachedTypeInspector cachedTypeInspector;

    // Cassandra-sourced type counts from the last detectSuperVertices() run
    private Map<String, Long> lastEdgestoreTypeCounts = Collections.emptyMap();

    public JanusGraphAnalyzer(CqlSession sourceSession, MigratorConfig config) {
        this.sourceSession = sourceSession;
        this.config = config;
        this.esClient = createEsClient();

        // Open JanusGraph for schema/type resolution (read-only)
        String janusConfigPath = config.getSourceJanusGraphConfig();
        if (janusConfigPath != null && !janusConfigPath.isEmpty()) {
            LOG.info("Opening JanusGraph for type resolution from config: {}", janusConfigPath);
            Configuration janusConfig = buildJanusGraphConfig(janusConfigPath);

            // Override storage backend to CQL — the pod's atlas-application.properties
            // may have hbase2 which isn't on the migrator classpath. We connect via CQL
            // using the source Cassandra settings from the analyzer config.
            janusConfig.setProperty("storage.backend", "cql");
            janusConfig.setProperty("storage.hostname", config.getSourceCassandraHostname());
            janusConfig.setProperty("storage.port", config.getSourceCassandraPort());
            janusConfig.setProperty("storage.cql.keyspace", config.getSourceCassandraKeyspace());
            janusConfig.setProperty("storage.cql.local-datacenter", config.getSourceCassandraDatacenter());
            LOG.info("Overriding storage backend to CQL: {}:{}/{}",
                     config.getSourceCassandraHostname(), config.getSourceCassandraPort(),
                     config.getSourceCassandraKeyspace());

            JanusGraph jg = JanusGraphFactory.open(janusConfig);
            this.janusGraph         = (StandardJanusGraph) jg;
            this.idManager          = janusGraph.getIDManager();
            this.edgeSerializer     = janusGraph.getEdgeSerializer();

            LOG.info("Pre-loading all type definitions into CachedTypeInspector...");
            this.cachedTypeInspector = new CachedTypeInspector(janusGraph);
        } else {
            throw new IllegalArgumentException(
                "source.janusgraph.config is required for JanusGraph analysis. " +
                "Point it to atlas-application.properties in the pod " +
                "(e.g., /opt/apache-atlas/conf/atlas-application.properties)");
        }
    }

    /**
     * Get total vertex count from the JanusGraph ES index.
     */
    public long getTotalVertexCount() throws IOException {
        String index = config.getSourceEsIndex();
        Request request = new Request("GET", "/" + index + "/_count");

        Response response = esClient.performRequest(request);
        String body = EntityUtils.toString(response.getEntity());
        Map<String, Object> result = MAPPER.readValue(body, Map.class);

        long count = ((Number) result.get("count")).longValue();
        LOG.info("ES vertex count from '{}': {}", index, String.format("%,d", count));
        return count;
    }

    /**
     * Get total edge count from the JanusGraph ES edge index.
     * Index name follows the pattern: {prefix}edge_index (e.g. janusgraph_edge_index).
     */
    public long getTotalEdgeCount() throws IOException {
        // Derive edge index name from vertex index: janusgraph_vertex_index -> janusgraph_edge_index
        String vertexIndex = config.getSourceEsIndex();
        String edgeIndex = vertexIndex.replace("vertex_index", "edge_index");

        try {
            Request request = new Request("GET", "/" + edgeIndex + "/_count");
            Response response = esClient.performRequest(request);
            String body = EntityUtils.toString(response.getEntity());
            Map<String, Object> result = MAPPER.readValue(body, Map.class);

            long count = ((Number) result.get("count")).longValue();
            LOG.info("ES edge count from '{}': {}", edgeIndex, String.format("%,d", count));
            return count;
        } catch (Exception e) {
            LOG.warn("Could not get edge count from '{}': {}", edgeIndex, e.getMessage());
            return -1;
        }
    }

    /**
     * Get vertex type distribution from ES using a terms aggregation on __typeName.keyword.
     * Returns a map of typeName -> count, covering all types (up to 10,000 buckets).
     */
    @SuppressWarnings("unchecked")
    public Map<String, Long> getVertexTypeCounts() throws IOException {
        String index = config.getSourceEsIndex();

        // Terms aggregation with large size to cover all types
        String query = "{\n" +
            "  \"size\": 0,\n" +
            "  \"aggs\": {\n" +
            "    \"type_counts\": {\n" +
            "      \"terms\": {\n" +
            "        \"field\": \"__typeName.keyword\",\n" +
            "        \"size\": 10000\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

        Request request = new Request("POST", "/" + index + "/_search");
        request.setJsonEntity(query);

        Response response = esClient.performRequest(request);
        String body = EntityUtils.toString(response.getEntity());
        Map<String, Object> result = MAPPER.readValue(body, Map.class);

        Map<String, Object> aggs = (Map<String, Object>) result.get("aggregations");
        Map<String, Object> typeCounts = (Map<String, Object>) aggs.get("type_counts");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) typeCounts.get("buckets");

        Map<String, Long> counts = new LinkedHashMap<>();
        for (Map<String, Object> bucket : buckets) {
            String key = (String) bucket.get("key");
            long docCount = ((Number) bucket.get("doc_count")).longValue();
            counts.put(key, docCount);
        }

        LOG.info("ES type distribution from '{}': {} distinct types", index, counts.size());
        return counts;
    }

    /**
     * Detect super vertices by scanning the JanusGraph edgestore table and decoding
     * each row using EdgeSerializer to get EXACT per-vertex edge counts (OUT+IN).
     *
     * Counts both OUT and IN edges per vertex (total degree), matching CassandraGraph's
     * SuperVertexDetector which scans both edges_out and edges_in tables.
     * Skips properties and system relations.
     *
     * Returns a SuperVertexReport with accurate edge counts + edge label tracking.
     */
    public SuperVertexReport detectSuperVertices() {
        long startMs = System.currentTimeMillis();

        String keyspace = config.getSourceCassandraKeyspace();
        String table = config.getSourceEdgestoreTable();
        int threshold = config.getSuperVertexThreshold();
        int topN = config.getSuperVertexTopN();

        // Split into token ranges to avoid Cassandra driver's ~2B row iteration limit.
        // A single SELECT * scan silently stops at Integer.MAX_VALUE rows.
        // Token-range scanning ensures each sub-scan stays well under that limit.
        int numRanges = 16; // 16 ranges → each handles ~500M rows for a 8B-row edgestore
        List<long[]> tokenRanges = splitTokenRanges(numRanges);
        String edgestoreTable = keyspace + "." + table;

        PreparedStatement scanStmt = sourceSession.prepare(
            "SELECT key, column1, value FROM " + edgestoreTable +
            " WHERE token(key) >= ? AND token(key) <= ?");

        LOG.info("Scanning {}.{} with EdgeSerializer decode for super vertices (threshold={}, tokenRanges={})...",
                 keyspace, table, threshold, numRanges);

        // STREAMING approach: edgestore rows are grouped by partition key, so we track
        // per-vertex edge counts using a running counter that resets on key transition.
        // This uses O(topN) memory instead of O(numVertices) — safe for 100M+ vertices.
        Map<String, Long> typeCountsFromEdgestore = new HashMap<>();

        long totalRows = 0;
        long totalOutEdges = 0;
        long totalInEdges = 0;
        long totalProperties = 0;
        long totalSystemRows = 0;
        long totalNullType = 0;
        long totalDecodeErrors = 0;
        long totalDistinctKeys = 0;
        long distinctVerticesWithEdges = 0;

        // Distribution buckets and super vertex tracking (computed inline)
        long maxEdgeCount = 0;
        int superVertexCount = 0;
        long over1k = 0, over10k = 0, over100k = 0, over1m = 0;
        PriorityQueue<SuperVertexCandidate> topQueue = new PriorityQueue<>(topN + 1);
        // Map from vertexId (long) to edge label counts — only kept for top-N super vertices
        Map<Long, Map<String, Long>> superVertexLabels = new HashMap<>();

        for (int rangeIdx = 0; rangeIdx < tokenRanges.size(); rangeIdx++) {
            long[] range = tokenRanges.get(rangeIdx);
            long rangeRows = 0;

            LOG.info("Scanning token range {}/{}: [{}, {}]", rangeIdx + 1, numRanges,
                     range[0], range[1]);

            ResultSet rs = sourceSession.execute(scanStmt.bind(range[0], range[1])
                .setPageSize(5000));

            ByteBuffer previousKey = null;
            long currentVertexId = -1;
            long currentVertexEdgeCount = 0;
            String currentTypeName = null;
            String currentGuid = null;
            Map<String, Long> currentVertexLabels = new LinkedHashMap<>();

            for (Row row : rs) {
                totalRows++;
                rangeRows++;

                ByteBuffer keyBuf = row.getByteBuffer("key");
                ByteBuffer colBuf = row.getByteBuffer("column1");
                ByteBuffer valBuf = row.getByteBuffer("value");

                // Detect key transition — flush the previous vertex's edge count
                if (previousKey == null || !previousKey.equals(keyBuf)) {
                    if (previousKey != null && currentVertexEdgeCount > 0) {
                        distinctVerticesWithEdges++;
                        if (currentVertexEdgeCount > maxEdgeCount) maxEdgeCount = currentVertexEdgeCount;
                        if (currentVertexEdgeCount > 1000)      over1k++;
                        if (currentVertexEdgeCount > 10_000)    over10k++;
                        if (currentVertexEdgeCount > 100_000)   over100k++;
                        if (currentVertexEdgeCount > 1_000_000) over1m++;

                        if (currentVertexEdgeCount >= threshold) {
                            superVertexCount++;
                            topQueue.add(new SuperVertexCandidate(
                                currentVertexId, currentVertexEdgeCount,
                                currentTypeName, currentGuid));
                            if (!currentVertexLabels.isEmpty()) {
                                superVertexLabels.put(currentVertexId, new LinkedHashMap<>(currentVertexLabels));
                            }
                            if (topQueue.size() > topN) {
                                SuperVertexCandidate evicted = topQueue.poll();
                                superVertexLabels.remove(evicted.getVertexId());
                            }
                        }
                    }
                    currentVertexEdgeCount = 0;
                    currentTypeName = null;
                    currentGuid = null;
                    currentVertexLabels.clear();
                    totalDistinctKeys++;
                    previousKey = keyBuf;
                    // Extract real JanusGraph vertex ID from partition key
                    try {
                        currentVertexId = extractVertexId(keyBuf);
                    } catch (Exception e) {
                        currentVertexId = -1;
                        if (totalDecodeErrors <= 5) {
                            LOG.debug("Could not extract vertex ID from partition key: {}", e.getMessage());
                        }
                    }
                }

                try {
                    Entry entry = buildEntry(colBuf, valBuf);
                    if (entry == null) {
                        totalDecodeErrors++;
                        continue;
                    }

                    RelationCache rel = edgeSerializer.parseRelation(entry, true, cachedTypeInspector);

                    if (isSystemRelation(rel.typeId)) {
                        totalSystemRows++;
                        continue;
                    }

                    RelationType type = cachedTypeInspector.getExistingRelationType(rel.typeId);
                    if (type == null) {
                        totalNullType++;
                        continue;
                    }

                    if (type.isPropertyKey()) {
                        totalProperties++;
                        String propName = type.name();
                        String normalizedName = normalizePropertyName(propName);

                        // Capture __typeName for this vertex (both aggregate and per-vertex)
                        if ("__typeName".equals(propName) || "__typeName".equals(normalizedName)) {
                            try {
                                Object value = rel.getValue();
                                if (value != null) {
                                    String typeName = value.toString();
                                    if (!typeName.isEmpty()) {
                                        typeCountsFromEdgestore.merge(typeName, 1L, Long::sum);
                                        currentTypeName = typeName;
                                    }
                                }
                            } catch (Exception e) {
                                // Skip — property value extraction is best-effort
                            }
                        }

                        // Capture __guid for this vertex
                        if ("__guid".equals(propName) || "__guid".equals(normalizedName)) {
                            try {
                                Object value = rel.getValue();
                                if (value != null) {
                                    String guid = value.toString();
                                    if (!guid.isEmpty()) {
                                        currentGuid = guid;
                                    }
                                }
                            } catch (Exception e) {
                                // Skip — property value extraction is best-effort
                            }
                        }

                        continue;
                    }

                    if (rel.direction == Direction.OUT) {
                        totalOutEdges++;
                    } else {
                        totalInEdges++;
                    }
                    currentVertexEdgeCount++;

                    if (currentVertexEdgeCount >= threshold / 2) {
                        currentVertexLabels.merge(type.name(), 1L, Long::sum);
                    }
                } catch (Exception e) {
                    totalDecodeErrors++;
                    if (totalDecodeErrors <= 20) {
                        LOG.debug("Decode error at row {}: {}", totalRows, e.getMessage());
                    }
                }

                if (totalRows % 5_000_000 == 0) {
                    LOG.info("  ... scanned {} edgestore rows (range {}/{}): outEdges={}, inEdges={}, properties={}, system={}, errors={}",
                             String.format("%,d", totalRows), rangeIdx + 1, numRanges,
                             String.format("%,d", totalOutEdges),
                             String.format("%,d", totalInEdges),
                             String.format("%,d", totalProperties),
                             String.format("%,d", totalSystemRows),
                             String.format("%,d", totalDecodeErrors));
                }
            }

            // Flush last vertex of this range
            if (previousKey != null && currentVertexEdgeCount > 0) {
                distinctVerticesWithEdges++;
                if (currentVertexEdgeCount > maxEdgeCount) maxEdgeCount = currentVertexEdgeCount;
                if (currentVertexEdgeCount > 1000)      over1k++;
                if (currentVertexEdgeCount > 10_000)    over10k++;
                if (currentVertexEdgeCount > 100_000)   over100k++;
                if (currentVertexEdgeCount > 1_000_000) over1m++;

                if (currentVertexEdgeCount >= threshold) {
                    superVertexCount++;
                    topQueue.add(new SuperVertexCandidate(
                        currentVertexId, currentVertexEdgeCount,
                        currentTypeName, currentGuid));
                    if (!currentVertexLabels.isEmpty()) {
                        superVertexLabels.put(currentVertexId, new LinkedHashMap<>(currentVertexLabels));
                    }
                    if (topQueue.size() > topN) {
                        SuperVertexCandidate evicted = topQueue.poll();
                        superVertexLabels.remove(evicted.getVertexId());
                    }
                }
            }

            LOG.info("Token range {}/{} complete: {} rows", rangeIdx + 1, numRanges,
                     String.format("%,d", rangeRows));
        }

        LOG.info("Edgestore scan complete: {} total rows (outEdges={}, inEdges={}, properties={}, system={}, nullType={}, errors={})",
                 String.format("%,d", totalRows),
                 String.format("%,d", totalOutEdges),
                 String.format("%,d", totalInEdges),
                 String.format("%,d", totalProperties),
                 String.format("%,d", totalSystemRows),
                 String.format("%,d", totalNullType),
                 String.format("%,d", totalDecodeErrors));

        // Build super vertex entries from candidates (type names already captured from edgestore)
        LOG.info("Building super vertex report for {} candidates (type names from edgestore scan)...", topQueue.size());

        List<SuperVertexReport.SuperVertexEntry> topList = new ArrayList<>();
        List<SuperVertexCandidate> candidates = new ArrayList<>(topQueue);
        candidates.sort(Comparator.comparingLong(SuperVertexCandidate::getEdgeCount).reversed());

        for (SuperVertexCandidate candidate : candidates) {
            String vertexIdStr = String.valueOf(candidate.getVertexId());
            String typeName = candidate.getTypeName() != null ? candidate.getTypeName() : "unknown";
            String guid = candidate.getGuid();

            Map<String, Long> labelCounts = superVertexLabels.getOrDefault(
                candidate.getVertexId(), new LinkedHashMap<>());

            SuperVertexReport.SuperVertexEntry entry = new SuperVertexReport.SuperVertexEntry(
                vertexIdStr, guid, typeName, candidate.getEdgeCount(), labelCounts);
            topList.add(entry);

            LOG.info("  Super vertex: jgId={}, guid={}, type={}, edges={}",
                     vertexIdStr, guid != null ? guid : "N/A", typeName,
                     String.format("%,d", candidate.getEdgeCount()));
        }

        superVertexLabels = null; // allow GC

        // Build report
        long durationMs = System.currentTimeMillis() - startMs;

        SuperVertexReport report = new SuperVertexReport();
        report.setTotalSuperVertexCount(superVertexCount);
        report.setMaxEdgeCount(maxEdgeCount);
        report.setTotalVerticesScanned(totalDistinctKeys);
        report.setScanDurationMs(durationMs);
        report.setVerticesOver1kEdges(over1k);
        report.setVerticesOver10kEdges(over10k);
        report.setVerticesOver100kEdges(over100k);
        report.setVerticesOver1mEdges(over1m);
        report.setEdgesOutRowCount(totalRows);   // total edgestore rows (diagnostic)
        report.setEdgesInRowCount(totalProperties);  // reuse for property count (diagnostic)
        report.setEdgesByIdRowCount(totalSystemRows); // reuse for system row count (diagnostic)
        report.setDecodedOutEdges(totalOutEdges);
        report.setDecodedInEdges(totalInEdges);
        report.setTopSuperVertices(topList);

        LOG.info("JanusGraph super vertex detection complete in {}ms: {} super vertices, " +
                 "max edges (OUT+IN)={}, distinct keys (total={}, with edges={})",
                 durationMs, superVertexCount, maxEdgeCount, totalDistinctKeys, distinctVerticesWithEdges);
        LOG.info("Decoded totals: OUT-edges={}, IN-edges={}, properties={}, system={}, nullType={}, errors={}",
                 String.format("%,d", totalOutEdges),
                 String.format("%,d", totalInEdges),
                 String.format("%,d", totalProperties),
                 String.format("%,d", totalSystemRows),
                 String.format("%,d", totalNullType),
                 String.format("%,d", totalDecodeErrors));

        // Store Cassandra-sourced type counts for the caller
        this.lastEdgestoreTypeCounts = typeCountsFromEdgestore;
        LOG.info("Extracted {} distinct __typeName values from edgestore property scan",
                 typeCountsFromEdgestore.size());

        return report;
    }

    // lookupTypeNameFromEs() removed — type names are now captured directly from
    // the edgestore scan (via __typeName property decoding), which is both correct
    // and faster than N individual ES lookups.

    // ---- JanusGraph entry decode helpers (reused from JanusGraphScanner) ----

    /**
     * Combine column1 + value into a JanusGraph Entry (the format EdgeSerializer expects).
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
     * Normalize JanusGraph property key names (same logic as JanusGraphScanner).
     */
    static String normalizePropertyName(String name) {
        if (name == null) return null;
        if (name.startsWith("__")) return name;
        int dotIndex = name.indexOf('.');
        if (dotIndex > 0 && dotIndex < name.length() - 1) {
            name = name.substring(dotIndex + 1);
        }
        return name;
    }

    private boolean isSystemRelation(long typeId) {
        try {
            return IDManager.isSystemRelationTypeId(typeId);
        } catch (Exception e) {
            return false;
        }
    }

    // ---- JanusGraph config loading (reused from JanusGraphScanner) ----

    /**
     * Load a config file and produce a JanusGraph-compatible Configuration.
     * Handles atlas-application.properties (atlas.graph.* prefix) or plain JanusGraph config.
     */
    private static Configuration buildJanusGraphConfig(String configPath) {
        Properties fileProps = new Properties();
        try (FileInputStream fis = new FileInputStream(configPath)) {
            fileProps.load(fis);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config: " + configPath, e);
        }

        BaseConfiguration janusConfig = new BaseConfiguration();

        boolean isAtlasFormat = fileProps.containsKey("atlas.graph.storage.backend");

        if (isAtlasFormat) {
            LOG.info("Detected Atlas-format config (atlas.graph.* prefix), stripping prefix");
            for (String key : fileProps.stringPropertyNames()) {
                if (key.startsWith(ATLAS_GRAPH_PREFIX)) {
                    String janusKey = key.substring(ATLAS_GRAPH_PREFIX.length());
                    janusConfig.setProperty(janusKey, fileProps.getProperty(key));
                }
            }
        } else {
            LOG.info("Detected plain JanusGraph config (no prefix)");
            for (String key : fileProps.stringPropertyNames()) {
                janusConfig.setProperty(key, fileProps.getProperty(key));
            }
        }

        // Add Atlas's custom serializers (must match AtlasJanusGraphDatabase exactly)
        if (!janusConfig.containsKey("attributes.custom.attribute1.attribute-class")) {
            janusConfig.setProperty("attributes.custom.attribute1.attribute-class",
                "org.apache.atlas.typesystem.types.DataTypes$TypeCategory");
            janusConfig.setProperty("attributes.custom.attribute1.serializer-class",
                "org.apache.atlas.repository.graphdb.janus.serializer.TypeCategorySerializer");

            janusConfig.setProperty("attributes.custom.attribute2.attribute-class",
                ArrayList.class.getName());
            janusConfig.setProperty("attributes.custom.attribute2.serializer-class",
                "org.janusgraph.graphdb.database.serialize.attribute.SerializableSerializer");

            janusConfig.setProperty("attributes.custom.attribute3.attribute-class",
                BigInteger.class.getName());
            janusConfig.setProperty("attributes.custom.attribute3.serializer-class",
                "org.apache.atlas.repository.graphdb.janus.serializer.BigIntegerSerializer");

            janusConfig.setProperty("attributes.custom.attribute4.attribute-class",
                BigDecimal.class.getName());
            janusConfig.setProperty("attributes.custom.attribute4.serializer-class",
                "org.apache.atlas.repository.graphdb.janus.serializer.BigDecimalSerializer");
        }

        LOG.info("JanusGraph config: storage.backend={}, storage.cql.keyspace={}, storage.hostname={}",
                 janusConfig.getString("storage.backend"),
                 janusConfig.getString("storage.cql.keyspace"),
                 janusConfig.getString("storage.hostname"));

        return janusConfig;
    }

    // ---- ES client ----

    private RestClient createEsClient() {
        RestClientBuilder builder = RestClient.builder(
            new HttpHost(config.getTargetEsHostname(), config.getTargetEsPort(),
                         config.getTargetEsProtocol()));

        String username = config.getTargetEsUsername();
        String password = config.getTargetEsPassword();
        if (username != null && !username.isEmpty()) {
            BasicCredentialsProvider creds = new BasicCredentialsProvider();
            creds.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(b -> b.setDefaultCredentialsProvider(creds));
        }

        builder.setRequestConfigCallback(b -> b
            .setConnectTimeout(30_000)
            .setSocketTimeout(120_000));

        return builder.build();
    }

    /**
     * Get the vertex type counts extracted from the edgestore during the last
     * {@link #detectSuperVertices()} call. These are Cassandra-sourced (ground truth)
     * rather than ES-sourced. Returns an empty map if detectSuperVertices() hasn't run.
     */
    public Map<String, Long> getEdgestoreTypeCounts() {
        return lastEdgestoreTypeCounts;
    }

    @Override
    public void close() throws IOException {
        if (esClient != null) {
            esClient.close();
        }
        if (cachedTypeInspector != null) {
            try {
                cachedTypeInspector.close();
            } catch (Exception e) {
                LOG.debug("Error closing CachedTypeInspector", e);
            }
        }
        if (janusGraph != null) {
            try {
                janusGraph.close();
            } catch (Exception e) {
                LOG.warn("Error closing JanusGraph instance", e);
            }
        }
    }

    /**
     * Convert hex partition keys (from old Mixpanel reports) to real JanusGraph vertex IDs.
     * Useful for looking up vertices that were reported before the ID fix.
     *
     * @param hexKeys list of hex strings like "c800000008127480"
     * @return map of hexKey → JanusGraph vertex ID (long as string)
     */
    public Map<String, String> convertHexKeysToVertexIds(List<String> hexKeys) {
        Map<String, String> results = new LinkedHashMap<>();
        for (String hexKey : hexKeys) {
            try {
                byte[] bytes = hexToBytes(hexKey);
                StaticBuffer key = new StaticArrayBuffer(bytes);
                Object id = idManager.getKeyID(key);
                long vertexId = ((Number) id).longValue();
                results.put(hexKey, String.valueOf(vertexId));
            } catch (Exception e) {
                LOG.warn("Could not convert hex key '{}': {}", hexKey, e.getMessage());
                results.put(hexKey, "ERROR: " + e.getMessage());
            }
        }
        return results;
    }

    /**
     * Convert a hex string to byte array.
     */
    private static byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] bytes = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            bytes[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                  + Character.digit(hex.charAt(i + 1), 16));
        }
        return bytes;
    }

    /**
     * Convert a ByteBuffer to a hex string (Cassandra partition key representation).
     */
    private static String bytesToHex(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    /**
     * Split the full Cassandra token range (Long.MIN_VALUE .. Long.MAX_VALUE) into
     * numRanges contiguous sub-ranges. Each sub-range can be scanned independently
     * using WHERE token(key) >= ? AND token(key) <= ?, avoiding the Cassandra driver's
     * ~2 billion row iteration limit on a single ResultSet.
     */
    private List<long[]> splitTokenRanges(int numRanges) {
        BigInteger minToken = BigInteger.valueOf(Long.MIN_VALUE);
        BigInteger maxToken = BigInteger.valueOf(Long.MAX_VALUE);
        BigInteger totalRange = maxToken.subtract(minToken).add(BigInteger.ONE);
        BigInteger rangeSize = totalRange.divide(BigInteger.valueOf(numRanges));

        List<long[]> ranges = new ArrayList<>(numRanges);
        BigInteger current = minToken;
        for (int i = 0; i < numRanges; i++) {
            BigInteger end = (i == numRanges - 1) ? maxToken : current.add(rangeSize).subtract(BigInteger.ONE);
            ranges.add(new long[]{current.longValueExact(), end.longValueExact()});
            current = end.add(BigInteger.ONE);
        }
        return ranges;
    }

    /**
     * Extract the actual JanusGraph vertex ID from the Cassandra edgestore partition key.
     * The partition key is a StaticBuffer encoding — IDManager.getKeyID() decodes it
     * to the real vertex ID (a long). This matches JanusGraphScanner.extractVertexId().
     */
    private long extractVertexId(ByteBuffer keyBuf) {
        byte[] bytes = new byte[keyBuf.remaining()];
        keyBuf.duplicate().get(bytes);
        StaticBuffer key = new StaticArrayBuffer(bytes);
        Object id = idManager.getKeyID(key);
        return ((Number) id).longValue();
    }

    /**
     * Candidate super vertex from edgestore scan with captured metadata.
     */
    private static class SuperVertexCandidate implements Comparable<SuperVertexCandidate> {
        private final long vertexId;
        private final long edgeCount;
        private final String typeName;
        private final String guid;

        SuperVertexCandidate(long vertexId, long edgeCount, String typeName, String guid) {
            this.vertexId  = vertexId;
            this.edgeCount = edgeCount;
            this.typeName  = typeName;
            this.guid      = guid;
        }

        long getVertexId()      { return vertexId; }
        long getEdgeCount()     { return edgeCount; }
        String getTypeName()    { return typeName; }
        String getGuid()        { return guid; }

        @Override
        public int compareTo(SuperVertexCandidate other) {
            return Long.compare(this.edgeCount, other.edgeCount); // ascending for min-heap
        }
    }
}
