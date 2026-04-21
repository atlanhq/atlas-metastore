package org.apache.atlas.service.config;

/**
 * Common interface for Cassandra connection configuration.
 *
 * Implemented by both DynamicConfigStoreConfig and StaticConfigStoreConfig
 * so that CassandraConfigDAO can be initialized by whichever store starts first.
 */
public interface CassandraConnectionConfig {

    String getKeyspace();

    String getTable();

    String getAppName();

    String getHostname();

    int getCassandraPort();

    int getReplicationFactor();

    String getDatacenter();

    String getConsistencyLevel();
}
