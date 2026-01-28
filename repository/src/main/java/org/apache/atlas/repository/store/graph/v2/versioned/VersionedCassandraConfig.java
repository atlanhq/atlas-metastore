package org.apache.atlas.repository.store.graph.v2.versioned;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;

public final class VersionedCassandraConfig {
    public static final String CASSANDRA_HOSTNAME_PROPERTY = "atlas.graph.storage.hostname";
    public static final String CASSANDRA_PORT_PROPERTY = "atlas.graph.storage.cql.port";
    public static final String CASSANDRA_KEYSPACE_PROPERTY = "atlas.graph.storage.cql.keyspace";
    public static final String CASSANDRA_REPLICATION_FACTOR_PROPERTY = "atlas.graph.storage.cql.replication-factor";
    public static final String CASSANDRA_LOCAL_DC_PROPERTY = "atlas.graph.storage.cql.local-datacenter";
    public static final String VERSIONED_ENABLED_PROPERTY = "atlas.versioned.enabled";
    public static final String VERSIONED_TABLE_NAME_PROPERTY = "atlas.versioned.table.name";

    public static final int DEFAULT_CASSANDRA_PORT = 9042;

    public static final String KEYSPACE;
    public static final String HOST_NAME;
    public static final int PORT;
    public static final String LOCAL_DATACENTER;
    public static final String TABLE_NAME;
    public static final String REPLICATION_FACTOR;
    public static final boolean VERSIONED_ENABLED;

    static {
        try {
            KEYSPACE = ApplicationProperties.get().getString(CASSANDRA_KEYSPACE_PROPERTY, "atlas");
            HOST_NAME = ApplicationProperties.get().getString(CASSANDRA_HOSTNAME_PROPERTY, "localhost");
            PORT = ApplicationProperties.get().getInt(CASSANDRA_PORT_PROPERTY, DEFAULT_CASSANDRA_PORT);
            LOCAL_DATACENTER = ApplicationProperties.get().getString(CASSANDRA_LOCAL_DC_PROPERTY, "datacenter1");
            TABLE_NAME = ApplicationProperties.get().getString(VERSIONED_TABLE_NAME_PROPERTY, "versioned_entries");
            REPLICATION_FACTOR = ApplicationProperties.get().getString(CASSANDRA_REPLICATION_FACTOR_PROPERTY, "3");
            VERSIONED_ENABLED = ApplicationProperties.get().getBoolean(VERSIONED_ENABLED_PROPERTY, false);
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }

    private VersionedCassandraConfig() {
    }
}
