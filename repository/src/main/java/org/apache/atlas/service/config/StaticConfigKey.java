package org.apache.atlas.service.config;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Enum defining valid static configuration keys with their default values.
 *
 * Static configs are read once at startup from Cassandra and never change
 * during the lifetime of the process. A restart is required to pick up changes.
 *
 * These are fundamentally different from {@link ConfigKey} (dynamic configs)
 * which can be changed at runtime without restart.
 */
public enum StaticConfigKey {

    GRAPH_BACKEND("atlas.graphdb.backend", "janus"),

    // NOTE: atlas.graph.index.search.es.prefix is intentionally NOT a static config.
    // The ES index prefix is derived strictly from GRAPH_BACKEND in Constants.java
    // ("cassandra" → "atlas_graph_", "janus" → "janusgraph_"). Allowing an explicit
    // override caused a drift bug where backend and prefix could disagree, pointing
    // reads at the wrong index. Flip GRAPH_BACKEND and the prefix follows.

    CASSANDRA_GRAPH_HOSTNAME("atlas.cassandra.graph.hostname", "localhost"),

    CASSANDRA_GRAPH_PORT("atlas.cassandra.graph.port", "9042"),

    CASSANDRA_GRAPH_KEYSPACE("atlas.cassandra.graph.keyspace", "atlas_graph"),

    CASSANDRA_GRAPH_DATACENTER("atlas.cassandra.graph.datacenter", "datacenter1"),

    GRAPH_ID_STRATEGY("atlas.graph.id.strategy", "legacy"),

    GRAPH_CLAIM_ENABLED("atlas.graph.claim.enabled", "false"),

    // Shadow mode — when true, suppresses CDC notifications, entity_audits writes,
    // search logs, and Keycloak role mutations. Used during WAL replay rollback so
    // downstream systems don't see duplicate events.
    // Lives in StaticConfigStore so mothership can flip it via the admin API and
    // the next pod restart picks it up; AtlasConfiguration.SHADOW_MODE_ENABLED
    // reads from ApplicationProperties, which StaticConfigStore overlays at init.
    ATLAS_SHADOW_MODE_ENABLED("atlas.shadow.mode.enabled", "false");

    private final String key;
    private final String defaultValue;

    private static final Map<String, StaticConfigKey> KEY_TO_CONFIG_MAP =
            Arrays.stream(values())
                  .collect(Collectors.toMap(StaticConfigKey::getKey, Function.identity()));

    StaticConfigKey(String key, String defaultValue) {
        this.key = key;
        this.defaultValue = defaultValue;
    }

    public String getKey() {
        return key;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Get StaticConfigKey from string key.
     * @param key the config key string
     * @return StaticConfigKey or null if not found
     */
    public static StaticConfigKey fromKey(String key) {
        return KEY_TO_CONFIG_MAP.get(key);
    }

    /**
     * Check if a key is valid (exists in this enum).
     * @param key the config key string
     * @return true if valid, false otherwise
     */
    public static boolean isValidKey(String key) {
        return KEY_TO_CONFIG_MAP.containsKey(key);
    }

    /**
     * Get all valid static config keys as string array.
     * @return array of all valid keys
     */
    public static String[] getAllKeys() {
        return Arrays.stream(values())
                     .map(StaticConfigKey::getKey)
                     .toArray(String[]::new);
    }
}
