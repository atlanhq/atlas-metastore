package org.apache.atlas.repository.store.graph.v2.tags;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository; // Keep Spring annotation

import javax.inject.Inject; // Or import org.springframework.beans.factory.annotation.Autowired;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.store.graph.v2.CassandraConnector.convertRowToMap;

/**
 * Cassandra implementation of the TagDAO interface, providing data access
 * for tags, vertices, and related operations.
 */
@Repository // Indicates this is a Spring-managed repository bean
public class TagDAOCassandraImpl implements TagDAO {
    private static final Logger LOG = LoggerFactory.getLogger(TagDAOCassandraImpl.class);

    // --- Configuration Constants ---
    private static final String CASSANDRA_BUCKET_POWER_PROPERTY = "atlas.graph.new.bucket.power";
    private static final String CASSANDRA_KEYSPACE_PROPERTY = "atlas.graph.new.keyspace"; // Unified keyspace property
    private static final String CASSANDRA_VERTEX_TABLE_PROPERTY = "atlas.graph.new.keyspace.vertex.table.name";
    private static final String CASSANDRA_HOSTNAME_PROPERTY = "atlas.graph.storage.hostname";
    private static final String EFFECTIVE_TAGS_DIRECT_TABLE_NAME = "effective_tags"; // Example table name
    private static final String VERTICES_ID_TABLE_NAME = "vertices_id"; // Example table name


    // --- Session and Helpers ---
    private static CqlSession cassSession = null;
    private static final ObjectMapper objectMapper = new ObjectMapper(); // Instance field is fine
    private final int numBuckets;
    private final String keyspace;
//    private final String vertexTableName;

    // --- Query Templates / Prepared Statements ---
    // Use final strings for templates or PreparedStatement fields
    private static final String SELECT_BY_ID_TEMPLATE = "SELECT * FROM %s.%s where id = ? AND bucket = ?"; // Use keyspace.table, use placeholders for PreparedStatement
    private static final String UPDATE_BY_ID_TEMPLATE = "UPDATE %s.%s SET json_data = ? WHERE id = ? AND bucket = ?"; // Use placeholders
    private static final String SELECT_DIRECT_TAG_TEMPLATE = "SELECT * FROM tags.effective_tags WHERE bucket = ? AND id = ? AND tag_type_name = ? ALLOW FILTERING;"; // Use placeholders
    private static final String INSERT_PROPAGATED_TAG_TEMPLATE = "INSERT into %s.%s (bucket, id, effective_tags, source_id) values (?, ?, ?, ?)"; // Use placeholders
    private static final String SELECT_ALL_VERTICES_ID_QUERY = "SELECT id, bucket FROM %s.%s"; // Use keyspace.table
    private static final String TEMP_INSERT_TAG_VERTEX_TEMPLATE = "INSERT INTO %s.%s (id, name, created_at, json_data, bucket) VALUES (?, ?, ?, ?, ?)"; // Use placeholders

    // Prepared Statements (initialize in constructor)
    private final PreparedStatement findTagsStmt;
//    private final PreparedStatement getVertexByIdStmt;
//    private final PreparedStatement updateVertexByIdStmt;
    private final PreparedStatement getDirectTagStmt;
    private final PreparedStatement insertPropagatedTagStmt;
//    private final PreparedStatement insertTempTagVertexStmt;

    // Inject ApplicationProperties or read directly in constructor
    @Inject
    public TagDAOCassandraImpl(ApplicationProperties applicationProperties) throws AtlasException {
        try {
            // Read configuration
            this.keyspace = applicationProperties.getString(CASSANDRA_KEYSPACE_PROPERTY, "tags"); // Default keyspace "atlas"
//            this.vertexTableName = applicationProperties.getString(CASSANDRA_VERTEX_TABLE_PROPERTY, "vertices");
            int bucketPower = applicationProperties.getInt(CASSANDRA_BUCKET_POWER_PROPERTY, 6); // Default power 6 (64 buckets)
            this.numBuckets = 1 << bucketPower;
            String hostname = applicationProperties.getString(CASSANDRA_HOSTNAME_PROPERTY, "localhost");

//            LOG.info("Initializing TagDAOCassandraImpl: Keyspace={}, VertexTable={}, Hostname={}, Buckets={}(2^{})",
//                    this.keyspace, this.vertexTableName, hostname, this.numBuckets, bucketPower);

            // Initialize Cassandra connection (reuse existing logic)
            cassSession = initializeCassandraSession(hostname, this.keyspace);
            LOG.info("Cassandra session established successfully to keyspace '{}'", cassSession.getKeyspace().map(ks -> ks.asCql(true)).orElse("N/A"));

            // Prepare statements for reuse
            // Note: Using String.format here to include keyspace/table names dynamically
            // Alternatively, pass keyspace/table to methods if they can vary, but usually they are fixed per DAO instance.
            findTagsStmt = cassSession.prepare(String.format("SELECT * FROM %s.%s WHERE id = ? AND bucket = ?", keyspace, EFFECTIVE_TAGS_DIRECT_TABLE_NAME));
            //getDirectTagStmt = cassSession.prepare(String.format("SELECT * FROM %s.%s WHERE bucket = ? AND id = ? AND tag_type_name = ?", keyspace, TAGS_DIRECT_TABLE_NAME));
//            getVertexByIdStmt = cassSession.prepare(String.format(SELECT_BY_ID_TEMPLATE, keyspace, vertexTableName));
//            updateVertexByIdStmt = cassSession.prepare(String.format(UPDATE_BY_ID_TEMPLATE, keyspace, vertexTableName));
            getDirectTagStmt = cassSession.prepare(String.format(SELECT_DIRECT_TAG_TEMPLATE, keyspace, EFFECTIVE_TAGS_DIRECT_TABLE_NAME));
            insertPropagatedTagStmt = cassSession.prepare(String.format(INSERT_PROPAGATED_TAG_TEMPLATE, keyspace, EFFECTIVE_TAGS_DIRECT_TABLE_NAME));
//            insertTempTagVertexStmt = cassSession.prepare(String.format(TEMP_INSERT_TAG_VERTEX_TEMPLATE, keyspace, vertexTableName));

        } catch (Exception e) {
            LOG.error("Failed to initialize TagDAOCassandraImpl", e);
            throw new RuntimeException("Failed to initialize TagDAOCassandraImpl", e); // Or throw AtlasException
        }
    }

    // --- TagDAO Method Implementations ---

    @Override
    public List<AtlasClassification> getTagsForVertex(String vertexId) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getTagsForVertex");
        List<AtlasClassification> tags = new ArrayList<>();
        validateVertexId(vertexId);
        ensureSession();

        try {
            int bucket = calculateBucket(vertexId); // Use the instance method
            BoundStatement bound = findTagsStmt.bind(vertexId, bucket);
            ResultSet rs = cassSession.execute(bound);

            for (Row row : rs) {
                // Use the existing conversion logic, assuming it's correct
                AtlasClassification classification = convertToAtlasClassification(
                        row.getString("tag_meta_json"), // Column name from TagDAOCassandraImpl
                        row.getString("tag_type_name")
                );
                if (classification != null) {
                    tags.add(classification);
                }
            }
            LOG.debug("Found {} tags for vertex {}", tags.size(), vertexId);
        } catch (Exception e) {
            LOG.error("Error fetching tags for vertex {}", vertexId, e);
            // Consider re-throwing a runtime exception or returning empty list based on policy
            throw new RuntimeException("Error fetching tags for vertex " + vertexId, e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
        return tags;
    }

    @Override
    public void putPropagatedTags(String sourceAssetId, String tagTypeName, Set<String> propagatedAssetVertexIds) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("putPropagatedTags");
        ensureSession();
        if (sourceAssetId == null || tagTypeName == null || propagatedAssetVertexIds == null || propagatedAssetVertexIds.isEmpty()) {
            LOG.warn("putPropagatedTags called with null or empty arguments. source={}, type={}, targets={}",
                    sourceAssetId, tagTypeName, propagatedAssetVertexIds == null ? 0 : propagatedAssetVertexIds.size());
            RequestContext.get().endMetricRecord(recorder);
            return;
        }

        // Using BATCH query string merge simplicity. Consider BatchStatement.
        StringBuilder batchQuery = new StringBuilder("BEGIN UNLOGGED BATCH ");
        int validInserts = 0;
        try {
            // Prepare statement outside loop for efficiency if using BatchStatement API
            // PreparedStatement pStmt = insertPropagatedTagStmt; // Already prepared

            for (String propagatedAssetVertexId : propagatedAssetVertexIds) {
                if (propagatedAssetVertexId == null || propagatedAssetVertexId.trim().isEmpty()) {
                    LOG.warn("Skipping null or empty propagatedAssetVertexId in batch insert for source={}, type={}", sourceAssetId, tagTypeName);
                    continue;
                }
                int bucket = calculateBucket(propagatedAssetVertexId);

                // Add to batch using simple query string format (less safe)
                String insert = String.format("INSERT into %s.%s (bucket, id, tag_type_name, source_id) values (%d, '%s', '%s', '%s');",
                        keyspace, EFFECTIVE_TAGS_DIRECT_TABLE_NAME, bucket, propagatedAssetVertexId.replace("'", "''"),
                        tagTypeName.replace("'", "''"), sourceAssetId.replace("'", "''")); // Basic escaping
                batchQuery.append(insert);

                // ---- OR ----
                // If using BatchStatement API (preferred):
                // BoundStatement bound = insertPropagatedTagStmt.bind(bucket, propagatedAssetVertexId, tagTypeName, sourceAssetId);
                // batchStatementBuilder.addStatement(bound);
                // -------------
                validInserts++;
            }

            if (validInserts > 0) {
                batchQuery.append("APPLY BATCH;");
                String finalQuery = batchQuery.toString();
                LOG.debug("Executing batch insert query for {} propagated tags.", validInserts);
                cassSession.execute(finalQuery); // Execute BATCH string

                // ---- OR ----
                // If using BatchStatement API (preferred):
                // BatchStatement batch = batchStatementBuilder.build();
                // cassSession.execute(batch);
                // -------------

                LOG.info("Successfully executed batch insert for {} propagated tags (source={}, type={}).", validInserts, sourceAssetId, tagTypeName);
            } else {
                LOG.info("Batch insert for propagated tags skipped as no valid target vertex IDs were provided.");
            }

        } catch (Exception e) {
            LOG.error("Error executing batch insert for propagated tags (source={}, type={})", sourceAssetId, tagTypeName, e);
            throw new RuntimeException("Failed to execute batch propagated tags insert in Cassandra", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public int calculateBucket(String vertexId) {
        if (vertexId == null) {
            throw new IllegalArgumentException("Cannot calculate bucket for null value");
        }
        try {
            long vertexID = Long.parseLong(vertexId);
            return (int)(vertexID % this.numBuckets);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Value must be a numeric string", e);
        }
    }


    // --- Helper Methods ---

    /** Populates common fields for a temporary tag vertex JSON map */
    private Map<String, Object> constructTagJsonMap(String tagVertexId, String typeName, String entityGuid) {
        Map<String, Object> jsonDataMap = new HashMap<>();
        jsonDataMap.put("id", tagVertexId); // Consider if Atlas uses "__guid" here
        jsonDataMap.put("__typeName", typeName);
        jsonDataMap.put("__state", "ACTIVE");
        jsonDataMap.put("__entityGuid", entityGuid);
        // Get user from RequestContext if available and non-null
        String user = RequestContext.get() != null ? RequestContext.get().getUser() : "unknown";
        long timestamp = System.currentTimeMillis();
        jsonDataMap.put("__createdBy", user);
        jsonDataMap.put("__timestamp", timestamp);
        jsonDataMap.put("__modificationTimestamp", timestamp);
        jsonDataMap.put("__modifiedBy", user);
        jsonDataMap.put("__entityStatus", "ACTIVE"); // Status of the entity the tag is attached to?
        // Default propagation settings from original code
        jsonDataMap.put("__propagate", true);
        jsonDataMap.put("__restrictPropagationThroughLineage", false);
        jsonDataMap.put("__removePropagations", true); // Original name was __removePropagationsOnEntityDelete ? Check Atlas model.
        jsonDataMap.put("__restrictPropagationThroughHierarchy", false);
        return jsonDataMap;
    }

    /** Converts JSON string and type name to AtlasClassification */
    private AtlasClassification convertToAtlasClassification(String tagMetaJson, String tagTypeName) {
        if (tagMetaJson == null || tagMetaJson.isEmpty()) {
            LOG.warn("Cannot convert to AtlasClassification, tagMetaJson is null or empty for type {}", tagTypeName);
            return null;
        }
        try {
            // Use TypeReference for safety if JSON structure is known
            Map<String, Object> jsonMap = objectMapper.readValue(tagMetaJson, new TypeReference<Map<String, Object>>() {});
            AtlasClassification classification = new AtlasClassification();

            // Set basic properties using safe defaults if keys are missing
            classification.setTypeName((String) jsonMap.getOrDefault("__typeName", tagTypeName));
            classification.setEntityGuid((String) jsonMap.get("__entityGuid"));

            // Handle potential nulls or incorrect types from JSON map for boolean flags
            classification.setPropagate((Boolean) jsonMap.getOrDefault("__propagate", false));
            classification.setRemovePropagationsOnEntityDelete((Boolean) jsonMap.getOrDefault("__removePropagations", false)); // Check Atlas field name
            classification.setRestrictPropagationThroughLineage((Boolean) jsonMap.getOrDefault("__restrictPropagationThroughLineage", false));
            classification.setRestrictPropagationThroughHierarchy((Boolean) jsonMap.getOrDefault("__restrictPropagationThroughHierarchy", false));

            // Add attributes if they exist under a specific key (e.g., "attributes")
            if(jsonMap.containsKey("attributes") && jsonMap.get("attributes") instanceof Map) {
                classification.setAttributes((Map<String, Object>) jsonMap.get("attributes"));
            }

            return classification;
        } catch (Exception e) {
            // Log the error but potentially return null instead of throwing runtime exception,
            // so one bad tag doesn't fail the whole getTagsForVertex call.
            LOG.error("Error converting JSON to AtlasClassification for type {}. JSON: {}",
                    tagTypeName, tagMetaJson, e);
            // throw new RuntimeException("Unable to map JSON to AtlasClassification", e); // Option: Fail hard
            return null; // Option: Log and skip this tag
        }
    }

    public Map<String, Object> getTag(String assetVertexId, String tagTypeName) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getTag");
        ensureSession();
        validateVertexId(assetVertexId);
        if (tagTypeName == null || tagTypeName.trim().isEmpty()) {
            throw new IllegalArgumentException("Tag type name cannot be null or empty");
        }

        try {
            int bucket = calculateBucket(assetVertexId);
            LOG.debug("Getting direct tag '{}' for asset '{}', bucket={}", tagTypeName, assetVertexId, bucket);

            BoundStatement bound = getDirectTagStmt.bind(bucket, assetVertexId, tagTypeName);
            ResultSet resultSet = cassSession.execute(bound);
            Row row = resultSet.one();

            if (row != null) {
                // Use the generic row converter
                return convertRowToMap(assetVertexId, bucket, row);
            } else {
                LOG.debug("Direct tag '{}' not found for asset '{}'", tagTypeName, assetVertexId);
                return null;
            }
        } catch(Exception e) {
            LOG.error("Error getting tag '{}' for asset '{}'", tagTypeName, assetVertexId, e);
            throw new RuntimeException("Failed to get tag from Cassandra", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }


    /** Initializes the CqlSession */
    private CqlSession initializeCassandraSession(String hostname, String keyspaceName) {
        // Using the builder logic from the original classes
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(hostname, 9042)) // Standard port
                .withConfigLoader(
                        DriverConfigLoader.programmaticBuilder()
                                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(10))
                                .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(15))
                                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(15)) // Default request timeout
                                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, Duration.ofSeconds(20))
                                .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofMillis(500))
                                // .withDuration(DefaultDriverOption.REQUEST_TRACE_ATTEMPTS, Duration.ofSeconds(20)) // Check driver docs for this option
                                .build())
                .withLocalDatacenter("datacenter1") // Consider making this configurable
                .withKeyspace(keyspaceName)
                .build();
    }

    /** Checks if the session is available */
    private void ensureSession() {
        if (cassSession == null || cassSession.isClosed()) {
            LOG.error("Cassandra session is null or closed when required.");
            throw new IllegalStateException("TagDAOCassandraImpl CqlSession is not available.");
        }
    }

    /** Validates vertex ID */
    private void validateVertexId(String vertexId) {
        if (vertexId == null || vertexId.trim().isEmpty()) {
            throw new IllegalArgumentException("Vertex ID cannot be null or empty.");
        }
    }

    // Optional: Implement close() method if needed for resource management (e.g., if not managed by Spring)
    // public void close() {
    //     if (cassSession != null && !cassSession.isClosed()) {
    //         LOG.info("Closing Cassandra CqlSession for TagDAOCassandraImpl.");
    //         cassSession.close();
    //     }
    // }
}