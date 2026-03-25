package org.apache.atlas.repository.graphdb.migrator;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Configuration for the JanusGraph-to-Cassandra migrator.
 * All source/target details are configurable via a properties file.
 */
public class MigratorConfig {

    private final Properties props;

    // Source JanusGraph (used for schema resolution / EdgeSerializer)
    private final String sourceJanusGraphConfig;

    // Source Cassandra (raw CQL scan of edgestore)
    private final String sourceCassandraHostname;
    private final int    sourceCassandraPort;
    private final String sourceCassandraKeyspace;
    private final String sourceCassandraDatacenter;
    private final String sourceCassandraUsername;
    private final String sourceCassandraPassword;
    private final String sourceEdgestoreTable;

    // Target Cassandra (new atlas_graph schema)
    private final String targetCassandraHostname;
    private final int    targetCassandraPort;
    private final String targetCassandraKeyspace;
    private final String targetCassandraDatacenter;
    private final String targetCassandraUsername;
    private final String targetCassandraPassword;

    // Source Elasticsearch (for copying mappings from JanusGraph index during migration)
    private final String sourceEsIndex;

    // Target Elasticsearch
    private final String targetEsHostname;
    private final int    targetEsPort;
    private final String targetEsProtocol;
    private final String targetEsIndex;
    private final String targetEsUsername;
    private final String targetEsPassword;

    // Target Cassandra replication
    private final String targetReplicationStrategy;
    private final int    targetReplicationFactor;

    // Migration tuning
    private final int scannerThreads;
    private final int writerThreads;
    private final int writerBatchSize;
    private final int esBulkSize;
    private final int scanFetchSize;
    private final int queueCapacity;
    private final boolean resume;

    // Write optimizations
    private final int maxInflightPerThread;
    private final boolean edgesOutOnly;
    private final int maxEdgesPerBatch;

    // ES index settings
    private final int esFieldLimit;

    // Parallel ES indexing during Phase 1
    private final boolean esParallel;

    // ES edge index
    private final String targetEsEdgeIndex;

    // Super vertex chunking
    private final int superVertexEdgeChunkSize;

    // Skip flags
    private final boolean skipEsReindex;
    private final boolean skipClassifications;
    private final boolean skipTasks;

    // Auxiliary keyspace migration flags
    private final boolean migrateConfigStore;
    private final boolean migrateTags;

    // Retry
    private final int maxRetries;

    // ID strategy / claim
    private final IdStrategy idStrategy;
    private final boolean claimEnabled;

    // Cassandra consistency levels
    private final String sourceConsistencyLevel;
    private final String targetConsistencyLevel;

    // Analyze mode ("cassandra" for post-migration, "janus" for pre-migration)
    private final String analyzeMode;

    // Validation settings
    private final int     validationVertexSampleSize;
    private final int     validationEdgeSampleSize;
    private final int     validationIndexSampleSize;
    private final int     validationTokenProbes;
    private final int     superVertexThreshold;
    private final int     superVertexTopN;
    private final boolean skipSuperVertexDetection;
    private final boolean skipEsCountValidation;
    private final String  validationTenantId;

    public MigratorConfig(String configPath) throws IOException {
        this.props = new Properties();
        try (FileInputStream fis = new FileInputStream(configPath)) {
            props.load(fis);
        }

        // Source JanusGraph
        this.sourceJanusGraphConfig = get("source.janusgraph.config", "");

        // Source Cassandra
        this.sourceCassandraHostname   = get("source.cassandra.hostname", "localhost");
        this.sourceCassandraPort       = getInt("source.cassandra.port", 9042);
        this.sourceCassandraKeyspace   = get("source.cassandra.keyspace", "atlas_janus");
        this.sourceCassandraDatacenter = get("source.cassandra.datacenter", "datacenter1");
        this.sourceCassandraUsername   = get("source.cassandra.username", "");
        this.sourceCassandraPassword   = get("source.cassandra.password", "");
        this.sourceEdgestoreTable      = get("source.cassandra.edgestore.table", "edgestore");

        // Source ES (same cluster as target, just a different index name)
        this.sourceEsIndex = get("source.elasticsearch.index", "janusgraph_vertex_index");

        // Target Cassandra
        this.targetCassandraHostname   = get("target.cassandra.hostname", "localhost");
        this.targetCassandraPort       = getInt("target.cassandra.port", 9042);
        this.targetCassandraKeyspace   = get("target.cassandra.keyspace", "atlas_graph");
        this.targetCassandraDatacenter = get("target.cassandra.datacenter", "datacenter1");
        this.targetCassandraUsername   = get("target.cassandra.username", "");
        this.targetCassandraPassword   = get("target.cassandra.password", "");

        // Target ES
        this.targetEsHostname = get("target.elasticsearch.hostname", "localhost");
        this.targetEsPort     = getInt("target.elasticsearch.port", 9200);
        this.targetEsProtocol = get("target.elasticsearch.protocol", "http");
        this.targetEsIndex    = get("target.elasticsearch.index", "atlas_graph_vertex_index");
        this.targetEsUsername = get("target.elasticsearch.username", "");
        this.targetEsPassword = get("target.elasticsearch.password", "");

        // Target replication
        this.targetReplicationStrategy = get("target.cassandra.replication.strategy", "NetworkTopologyStrategy");
        this.targetReplicationFactor   = getInt("target.cassandra.replication.factor", 3);

        // Tuning
        this.scannerThreads  = getInt("migration.scanner.threads", 32);
        this.writerThreads   = getInt("migration.writer.threads", 8);
        this.writerBatchSize = getInt("migration.writer.batch.size", 500);
        this.esBulkSize      = getInt("migration.es.bulk.size", 1000);
        this.scanFetchSize   = getInt("migration.scan.fetch.size", 5000);
        this.queueCapacity   = getInt("migration.queue.capacity", 10000);
        this.resume          = getBoolean("migration.resume", true);

        // Write optimizations
        this.maxInflightPerThread = getInt("migration.writer.max.inflight.per.thread", 50);
        this.edgesOutOnly         = getBoolean("migration.edges.out.only", true);
        this.maxEdgesPerBatch     = getInt("migration.writer.max.edges.per.batch", 15);

        // ES index settings
        this.esFieldLimit = getInt("target.elasticsearch.field.limit", 10000);

        // Parallel ES indexing during Phase 1 (eliminates Phase 2)
        this.esParallel = getBoolean("migration.es.parallel", true);

        // ES edge index
        this.targetEsEdgeIndex = get("target.elasticsearch.edge.index", "atlas_graph_edge_index");

        // Super vertex chunking
        this.superVertexEdgeChunkSize = getInt("migration.super.vertex.edge.chunk.size", 10000);

        // Skip flags
        this.skipEsReindex      = getBoolean("migration.skip.es.reindex", false);
        this.skipClassifications = getBoolean("migration.skip.classifications", false);
        this.skipTasks           = getBoolean("migration.skip.tasks", false);

        // Auxiliary keyspace migration flags
        this.migrateConfigStore = getBoolean("migration.migrate.config.store", false);
        this.migrateTags        = getBoolean("migration.migrate.tags", false);

        // Retry
        this.maxRetries = getInt("migration.max.retries", 3);

        // ID strategy / claim
        this.idStrategy = IdStrategy.from(get("migration.id.strategy", "legacy"));
        this.claimEnabled = getBoolean("migration.claim.enabled", false);

        // Cassandra consistency levels
        this.sourceConsistencyLevel = get("source.cassandra.consistency", "ONE");
        this.targetConsistencyLevel = get("target.cassandra.consistency", "LOCAL_QUORUM");

        // Analyze mode
        this.analyzeMode = get("analyze.mode", "janus");

        // Validation
        this.validationVertexSampleSize  = getInt("validation.vertex.sample.size", 1000);
        this.validationEdgeSampleSize    = getInt("validation.edge.sample.size", 500);
        this.validationIndexSampleSize   = getInt("validation.index.sample.size", 500);
        this.validationTokenProbes       = getInt("validation.token.probes", 5);
        this.superVertexThreshold        = getInt("validation.super.vertex.threshold", 100000);
        this.superVertexTopN             = getInt("validation.super.vertex.topn", 100);
        this.skipSuperVertexDetection    = getBoolean("validation.skip.super.vertex.detection", false);
        this.skipEsCountValidation       = getBoolean("validation.skip.es.count", false);
        this.validationTenantId          = get("validation.tenant.id", "unknown");
    }

    private String get(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    private int getInt(String key, int defaultValue) {
        String val = props.getProperty(key);
        return val != null ? Integer.parseInt(val.trim()) : defaultValue;
    }

    private boolean getBoolean(String key, boolean defaultValue) {
        String val = props.getProperty(key);
        return val != null ? Boolean.parseBoolean(val.trim()) : defaultValue;
    }

    // Getters
    public String getSourceJanusGraphConfig()    { return sourceJanusGraphConfig; }
    public String getSourceCassandraHostname()   { return sourceCassandraHostname; }
    public int    getSourceCassandraPort()        { return sourceCassandraPort; }
    public String getSourceCassandraKeyspace()   { return sourceCassandraKeyspace; }
    public String getSourceCassandraDatacenter() { return sourceCassandraDatacenter; }
    public String getSourceCassandraUsername()    { return sourceCassandraUsername; }
    public String getSourceCassandraPassword()   { return sourceCassandraPassword; }
    public String getSourceEdgestoreTable()      { return sourceEdgestoreTable; }
    public String getSourceEsIndex()             { return sourceEsIndex; }

    public String getTargetCassandraHostname()   { return targetCassandraHostname; }
    public int    getTargetCassandraPort()        { return targetCassandraPort; }
    public String getTargetCassandraKeyspace()   { return targetCassandraKeyspace; }
    public String getTargetCassandraDatacenter() { return targetCassandraDatacenter; }
    public String getTargetCassandraUsername()    { return targetCassandraUsername; }
    public String getTargetCassandraPassword()   { return targetCassandraPassword; }

    public String getTargetEsHostname()  { return targetEsHostname; }
    public int    getTargetEsPort()       { return targetEsPort; }
    public String getTargetEsProtocol()  { return targetEsProtocol; }
    public String getTargetEsIndex()     { return targetEsIndex; }
    public String getTargetEsUsername()  { return targetEsUsername; }
    public String getTargetEsPassword()  { return targetEsPassword; }

    public String  getTargetReplicationStrategy() { return targetReplicationStrategy; }
    public int     getTargetReplicationFactor()  { return targetReplicationFactor; }

    public int     getScannerThreads()   { return scannerThreads; }
    public int     getWriterThreads()    { return writerThreads; }
    public int     getWriterBatchSize()  { return writerBatchSize; }
    public int     getEsBulkSize()       { return esBulkSize; }
    public int     getScanFetchSize()    { return scanFetchSize; }
    public int     getQueueCapacity()    { return queueCapacity; }
    public boolean isResume()            { return resume; }

    public int     getMaxInflightPerThread() { return maxInflightPerThread; }
    public boolean isEdgesOutOnly()          { return edgesOutOnly; }
    public int     getMaxEdgesPerBatch()     { return maxEdgesPerBatch; }

    public int     getEsFieldLimit()      { return esFieldLimit; }
    public boolean isEsParallel()         { return esParallel; }
    public String  getTargetEsEdgeIndex() { return targetEsEdgeIndex; }
    public int     getSuperVertexEdgeChunkSize() { return superVertexEdgeChunkSize; }

    public boolean isSkipEsReindex()      { return skipEsReindex; }
    public boolean isSkipClassifications() { return skipClassifications; }
    public boolean isSkipTasks()           { return skipTasks; }

    public boolean isMigrateConfigStore()  { return migrateConfigStore; }
    public boolean isMigrateTags()         { return migrateTags; }

    public boolean isSameCassandraCluster() {
        return sourceCassandraHostname.equals(targetCassandraHostname)
            && sourceCassandraPort == targetCassandraPort;
    }

    public int     getMaxRetries()        { return maxRetries; }

    public IdStrategy getIdStrategy()      { return idStrategy; }
    public boolean isClaimEnabled()        { return claimEnabled; }

    public String getSourceConsistencyLevel() { return sourceConsistencyLevel; }
    public String getTargetConsistencyLevel() { return targetConsistencyLevel; }

    public int     getValidationVertexSampleSize()  { return validationVertexSampleSize; }
    public int     getValidationEdgeSampleSize()    { return validationEdgeSampleSize; }
    public int     getValidationIndexSampleSize()   { return validationIndexSampleSize; }
    public int     getValidationTokenProbes()       { return validationTokenProbes; }
    public int     getSuperVertexThreshold()        { return superVertexThreshold; }
    public int     getSuperVertexTopN()             { return superVertexTopN; }
    public boolean isSkipSuperVertexDetection()     { return skipSuperVertexDetection; }
    public boolean isSkipEsCountValidation()        { return skipEsCountValidation; }
    public String  getValidationTenantId()          { return validationTenantId; }
    public String  getAnalyzeMode()                 { return analyzeMode; }
}
