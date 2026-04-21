package org.apache.atlas.service.config;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Configuration class for the Static Config Store.
 *
 * Loads configuration properties from atlas-application.properties:
 * - atlas.static.config.store.enabled: Enable/disable static config store (default: true)
 * - atlas.static.config.store.app.name: Cassandra partition key (default: atlas_static)
 *
 * Reuses the same Cassandra connection settings as DynamicConfigStoreConfig
 * (hostname, port, keyspace, table, datacenter, consistency level).
 */
@Component
public class StaticConfigStoreConfig {
    private static final Logger LOG = LoggerFactory.getLogger(StaticConfigStoreConfig.class);

    // Static config store specific properties
    public static final String PROP_ENABLED = "atlas.static.config.store.enabled";
    public static final String PROP_APP_NAME = "atlas.static.config.store.app.name";

    // Defaults
    private static final boolean DEFAULT_ENABLED = true;
    private static final String DEFAULT_APP_NAME = "atlas_static";

    private final boolean enabled;
    private final String appName;

    // Reused from DynamicConfigStoreConfig property keys
    private final String keyspace;
    private final String table;
    private final String hostname;
    private final int cassandraPort;
    private final int replicationFactor;
    private final String datacenter;
    private final String consistencyLevel;

    public StaticConfigStoreConfig() throws AtlasException {
        Configuration props = ApplicationProperties.get();

        this.enabled = props.getBoolean(PROP_ENABLED, DEFAULT_ENABLED);
        this.appName = props.getString(PROP_APP_NAME, DEFAULT_APP_NAME);

        // Reuse Cassandra connection settings from DynamicConfigStoreConfig property keys
        this.keyspace = props.getString(DynamicConfigStoreConfig.PROP_KEYSPACE, "config_store");
        this.table = props.getString(DynamicConfigStoreConfig.PROP_TABLE, "configs");
        this.replicationFactor = props.getInt(DynamicConfigStoreConfig.PROP_REPLICATION_FACTOR, 3);
        this.datacenter = props.getString(DynamicConfigStoreConfig.PROP_DATACENTER, "datacenter1");
        this.consistencyLevel = props.getString(DynamicConfigStoreConfig.PROP_CONSISTENCY_LEVEL, "LOCAL_QUORUM");

        // Hostname: use dedicated property or fall back to graph storage hostname
        String configuredHostname = props.getString(DynamicConfigStoreConfig.PROP_HOSTNAME, null);
        if (configuredHostname != null && !configuredHostname.isEmpty()) {
            this.hostname = configuredHostname;
        } else {
            this.hostname = props.getString("atlas.graph.storage.hostname", "localhost");
        }

        // Port: use dedicated property or fall back to graph storage CQL port
        int configuredPort = props.getInt(DynamicConfigStoreConfig.PROP_PORT, -1);
        if (configuredPort > 0) {
            this.cassandraPort = configuredPort;
        } else {
            this.cassandraPort = props.getInt("atlas.graph.storage.cql.port", 9042);
        }

        LOG.info("StaticConfigStoreConfig initialized - enabled: {}, appName: {}, keyspace: {}, table: {}, hostname: {}, port: {}",
                enabled, appName, keyspace, table, hostname, cassandraPort);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getAppName() {
        return appName;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getTable() {
        return table;
    }

    public String getHostname() {
        return hostname;
    }

    public int getCassandraPort() {
        return cassandraPort;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public String getDatacenter() {
        return datacenter;
    }

    public String getConsistencyLevel() {
        return consistencyLevel;
    }
}
