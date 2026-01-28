package org.apache.atlas.repository.store.graph.v2.versioned;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.VersionedEntry;
import org.apache.atlas.utils.AtlasJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.atlas.repository.store.graph.v2.versioned.VersionedCassandraConfig.*;

public class VersionedStore implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(VersionedStore.class);
    private static final VersionedStore INSTANCE;

    private final CqlSession cassSession;
    private final PreparedStatement insertStmt;
    private final PreparedStatement selectAllStmt;
    private final PreparedStatement selectByVersionStmt;

    static {
        VersionedStore instance = null;
        if (VERSIONED_ENABLED) {
            try {
                instance = new VersionedStore();
            } catch (AtlasBaseException e) {
                LOG.error("FATAL: Failed to initialize VersionedStore", e);
            }
        }
        INSTANCE = instance;
    }

    public static boolean isEnabled() {
        return VERSIONED_ENABLED;
    }

    public static VersionedStore getInstance() {
        if (!VERSIONED_ENABLED || INSTANCE == null) {
            throw new IllegalStateException("Versioned metadata is disabled. Set atlas.versioned.enabled=true to enable.");
        }
        return INSTANCE;
    }

    public static UUID newVersion() {
        return Uuids.timeBased();
    }

    private VersionedStore() throws AtlasBaseException {
        try {
            cassSession = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(HOST_NAME, PORT))
                    .withLocalDatacenter(LOCAL_DATACENTER)
                    .build();

            initializeSchema();

            insertStmt = cassSession.prepare(String.format(
                    "INSERT INTO %s.%s (type_name, base_qn, version, qualified_name, created_at_ts, original_json) VALUES (?, ?, ?, ?, ?, ?)",
                    KEYSPACE, TABLE_NAME));

            selectAllStmt = cassSession.prepare(String.format(
                    "SELECT version, qualified_name, created_at_ts, original_json, created_at FROM %s.%s WHERE type_name = ? AND base_qn = ? ORDER BY version DESC",
                    KEYSPACE, TABLE_NAME));

            selectByVersionStmt = cassSession.prepare(String.format(
                    "SELECT version, qualified_name, created_at_ts, original_json, created_at FROM %s.%s WHERE type_name = ? AND base_qn = ? AND version = ?",
                    KEYSPACE, TABLE_NAME));
        } catch (Exception e) {
            throw new AtlasBaseException(e);
        }
    }

    public void insert(String typeName, String baseQualifiedName, UUID version, String qualifiedName, long createdAtMillis, String originalJson) throws AtlasBaseException {
        try {
            Instant createdAt = Instant.ofEpochMilli(createdAtMillis);
            BoundStatement bound = insertStmt.bind(typeName, baseQualifiedName, version, qualifiedName, createdAt, originalJson);
            cassSession.execute(bound);
        } catch (Exception e) {
            throw new AtlasBaseException(e);
        }
    }

    public List<VersionedEntry> listAll(String typeName, String baseQualifiedName) throws AtlasBaseException {
        try {
            BoundStatement bound = selectAllStmt.bind(typeName, baseQualifiedName);
            List<VersionedEntry> results = new ArrayList<>();
            for (Row row : cassSession.execute(bound)) {
                results.add(toEntry(typeName, baseQualifiedName, row));
            }
            return results;
        } catch (Exception e) {
            throw new AtlasBaseException(e);
        }
    }

    public VersionedEntry getByVersion(String typeName, String baseQualifiedName, UUID version) throws AtlasBaseException {
        try {
            BoundStatement bound = selectByVersionStmt.bind(typeName, baseQualifiedName, version);
            Row row = cassSession.execute(bound).one();
            return row == null ? null : toEntry(typeName, baseQualifiedName, row);
        } catch (Exception e) {
            throw new AtlasBaseException(e);
        }
    }


    @Override
    public void close() {
        if (cassSession != null) {
            cassSession.close();
        }
    }

    private void initializeSchema() {
        String replicationConfig = String.format("{'class':'SimpleStrategy','replication_factor':'%s'}", REPLICATION_FACTOR);
        String createKeyspace = String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = %s",
                KEYSPACE, replicationConfig);

        String createTable = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                        "type_name text, " +
                        "base_qn text, " +
                        "version timeuuid, " +
                        "qualified_name text, " +
                        "created_at timeuuid, " +
                        "created_at_ts timestamp, " +
                        "original_json text, " +
                        "PRIMARY KEY ((type_name, base_qn), version)" +
                        ") WITH CLUSTERING ORDER BY (version DESC)",
                KEYSPACE, TABLE_NAME);

        cassSession.execute(SimpleStatement.builder(createKeyspace).setTimeout(Duration.ofSeconds(30)).build());
        cassSession.execute(SimpleStatement.builder(createTable).setTimeout(Duration.ofSeconds(30)).build());
    }

    private static VersionedEntry toEntry(String typeName, String baseQualifiedName, Row row) {
        UUID version = row.getUuid("version");
        String qualifiedName = row.getString("qualified_name");
        Instant createdAt = row.getInstant("created_at_ts");
        if (createdAt == null) {
            UUID legacy = row.getUuid("created_at");
            createdAt = legacy == null ? null : Instant.ofEpochMilli(legacy.timestamp());
        }
        Long createdAtMillis = createdAt == null ? null : createdAt.toEpochMilli();
        String originalJson = row.getString("original_json");
        Map<String, Object> originalMap = AtlasJson.fromJson(originalJson, new TypeReference<Map<String, Object>>() {});
        return new VersionedEntry(typeName, baseQualifiedName, version, qualifiedName, createdAtMillis, originalMap);
    }
}
