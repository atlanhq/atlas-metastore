package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Migrates auxiliary Cassandra keyspaces (config_store, tags) from source to target.
 *
 * Only needed when source and target are on DIFFERENT Cassandra clusters.
 * When on the same cluster, these keyspaces are already shared and no migration is needed.
 *
 * Gated by config flags:
 *   migration.migrate.config.store=true
 *   migration.migrate.tags=true
 */
public class AuxiliaryKeyspaceMigrator {

    private static final Logger LOG = LoggerFactory.getLogger(AuxiliaryKeyspaceMigrator.class);

    private static final String CONFIG_STORE_KEYSPACE = "config_store";
    private static final String CONFIG_STORE_TABLE    = "configs";

    private static final String TAGS_KEYSPACE              = "tags";
    private static final String TAGS_BY_ID_TABLE           = "tags_by_id";
    private static final String PROPAGATED_TAGS_TABLE      = "propagated_tags_by_source";

    private static final int BATCH_SIZE = 100;

    private final CqlSession sourceSession;
    private final CqlSession targetSession;
    private final MigratorConfig config;
    private final MigrationStateStore stateStore;

    public AuxiliaryKeyspaceMigrator(CqlSession sourceSession, CqlSession targetSession,
                                      MigratorConfig config, MigrationStateStore stateStore) {
        this.sourceSession = sourceSession;
        this.targetSession = targetSession;
        this.config        = config;
        this.stateStore    = stateStore;
    }

    /**
     * Run auxiliary keyspace migrations based on config flags.
     * Logs warnings if source and target are different clusters but flags are not set.
     */
    public void migrate() {
        boolean sameCluster = config.isSameCassandraCluster();

        if (sameCluster) {
            LOG.info("Same Cassandra cluster detected ({}:{}), skipping config_store/tags migration (shared keyspaces)",
                     config.getSourceCassandraHostname(), config.getSourceCassandraPort());
            return;
        }

        LOG.info("Different Cassandra clusters detected: source={}:{}, target={}:{}",
                 config.getSourceCassandraHostname(), config.getSourceCassandraPort(),
                 config.getTargetCassandraHostname(), config.getTargetCassandraPort());

        if (config.isMigrateConfigStore()) {
            migrateConfigStore();
        } else {
            LOG.warn("config_store migration is  disabled (migration.migrate.config.store=false). " +
                     "Source and target are different clusters — config_store data will NOT be available on target.");
        }

        if (config.isMigrateTags()) {
            migrateTagsKeyspace();
        } else {
            LOG.warn("tags migration is disabled (migration.migrate.tags=false). " +
                     "Source and target are different clusters — tags data will NOT be available on target.");
        }
    }

    private void migrateConfigStore() {
        LOG.info("Migrating config_store keyspace...");

        String replication = buildReplication();

        targetSession.execute(
            "CREATE KEYSPACE IF NOT EXISTS " + CONFIG_STORE_KEYSPACE +
            " WITH replication = " + replication + " AND durable_writes = true");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + CONFIG_STORE_KEYSPACE + "." + CONFIG_STORE_TABLE + " (" +
            "  app_name text," +
            "  config_key text," +
            "  config_value text," +
            "  updated_by text," +
            "  last_updated timestamp," +
            "  PRIMARY KEY ((app_name), config_key))");

        PreparedStatement insertStmt = targetSession.prepare(
            "INSERT INTO " + CONFIG_STORE_KEYSPACE + "." + CONFIG_STORE_TABLE +
            " (app_name, config_key, config_value, updated_by, last_updated) VALUES (?, ?, ?, ?, ?)");

        ResultSet rs = sourceSession.execute(
            SimpleStatement.builder("SELECT * FROM " + CONFIG_STORE_KEYSPACE + "." + CONFIG_STORE_TABLE)
                .setPageSize(1000)
                .build());

        long count = 0;
        List<BatchableStatement<?>> batch = new ArrayList<>();
        for (Row row : rs) {
            batch.add(insertStmt.bind(
                row.getString("app_name"),
                row.getString("config_key"),
                row.getString("config_value"),
                row.getString("updated_by"),
                row.getInstant("last_updated")));

            if (batch.size() >= BATCH_SIZE) {
                targetSession.execute(BatchStatement.newInstance(BatchType.UNLOGGED, batch));
                count += batch.size();
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            targetSession.execute(BatchStatement.newInstance(BatchType.UNLOGGED, batch));
            count += batch.size();
        }

        LOG.info("config_store migration complete: {} config entries migrated", count);
    }

    private void migrateTagsKeyspace() {
        LOG.info("Migrating tags keyspace...");

        String replication = buildReplication();

        targetSession.execute(
            "CREATE KEYSPACE IF NOT EXISTS " + TAGS_KEYSPACE +
            " WITH replication = " + replication + " AND durable_writes = true");

        // tags_by_id table
        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + TAGS_KEYSPACE + "." + TAGS_BY_ID_TABLE + " (" +
            "  id text," +
            "  bucket int," +
            "  is_propagated boolean," +
            "  source_id text," +
            "  tag_type_name text," +
            "  tag_meta_json text," +
            "  asset_metadata text," +
            "  updated_at timestamp," +
            "  is_deleted boolean," +
            "  PRIMARY KEY ((bucket, id), is_propagated, source_id, tag_type_name)" +
            ") WITH compaction = {'class': 'SizeTieredCompactionStrategy'}");

        // propagated_tags_by_source table
        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + TAGS_KEYSPACE + "." + PROPAGATED_TAGS_TABLE + " (" +
            "  source_id text," +
            "  tag_type_name text," +
            "  propagated_asset_id text," +
            "  asset_metadata text," +
            "  updated_at timestamp," +
            "  PRIMARY KEY ((source_id, tag_type_name), propagated_asset_id)" +
            ") WITH compaction = {'class': 'SizeTieredCompactionStrategy'}");

        // Delegate to parallel migrator for the actual data copy
        ParallelTagsMigrator parallelMigrator =
            new ParallelTagsMigrator(sourceSession, targetSession, config, stateStore);
        parallelMigrator.migrateAll();
    }

    private String buildReplication() {
        String strategy = config.getTargetReplicationStrategy();
        int rf = config.getTargetReplicationFactor();
        String dc = config.getTargetCassandraDatacenter();

        if ("SimpleStrategy".equals(strategy)) {
            return "{'class': 'SimpleStrategy', 'replication_factor': " + rf + "}";
        } else {
            return "{'class': 'NetworkTopologyStrategy', '" + dc + "': " + rf + "}";
        }
    }
}
