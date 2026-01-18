package org.apache.atlas.service.config;

import org.apache.atlas.service.FeatureFlag;
import org.apache.atlas.service.FeatureFlagStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

/**
 * One-time migration service to copy feature flags from Redis to Cassandra.
 *
 * This service runs at startup when Cassandra config store is enabled.
 * It copies existing feature flag values from Redis to Cassandra.
 *
 * Design principles:
 * - Idempotent: Safe to run multiple times
 * - Non-destructive: Never deletes from Redis
 * - Skip if exists: Does not overwrite existing Cassandra values
 * - Uses a migration marker to track completion
 */
@Component
public class ConfigMigrationService {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigMigrationService.class);

    private static final String MIGRATION_MARKER_KEY = "_migration_completed";
    private static final String MIGRATION_SYSTEM_USER = "system:migration";

    private final DynamicConfigStore configStore;
    private final DynamicConfigStoreConfig config;

    @Inject
    public ConfigMigrationService(DynamicConfigStore configStore, DynamicConfigStoreConfig config) {
        this.configStore = configStore;
        this.config = config;
    }

    @PostConstruct
    public void migrateIfNeeded() {
        if (!config.isEnabled()) {
            LOG.debug("Config store disabled, skipping migration");
            return;
        }

        if (!configStore.isInitialized()) {
            LOG.warn("ConfigStore not initialized, skipping migration");
            return;
        }

        try {
            // Check if migration already completed
            String migrationMarker = DynamicConfigStore.getConfig(MIGRATION_MARKER_KEY);
            if ("true".equals(migrationMarker)) {
                LOG.info("Migration already completed (marker found), skipping");
                return;
            }

            LOG.info("Starting Redis to Cassandra feature flag migration...");
            migrateFeatureFlags();

            // Set migration marker
            DynamicConfigStore.setConfig(MIGRATION_MARKER_KEY, "true", MIGRATION_SYSTEM_USER);
            LOG.info("Migration completed and marker set");

        } catch (Exception e) {
            LOG.error("Migration failed - feature flags may need manual migration", e);
            // Don't fail startup - existing Redis feature flags still work
        }
    }

    private void migrateFeatureFlags() {
        int migrated = 0;
        int skipped = 0;

        for (FeatureFlag flag : FeatureFlag.values()) {
            String key = flag.getKey();

            try {
                // Check if already exists in Cassandra
                String existingValue = DynamicConfigStore.getConfig(key);
                if (existingValue != null && !existingValue.equals(flag.getDefaultValue() + "")) {
                    LOG.debug("Config {} already exists in Cassandra with value {}, skipping", key, existingValue);
                    skipped++;
                    continue;
                }

                // Get value from Redis via FeatureFlagStore
                String redisValue = null;
                try {
                    redisValue = FeatureFlagStore.getFlag(key);
                } catch (Exception e) {
                    LOG.warn("Could not read flag {} from Redis: {}", key, e.getMessage());
                }

                if (redisValue != null) {
                    // Only migrate if the config key is valid in the new system
                    if (ConfigKey.isValidKey(key)) {
                        DynamicConfigStore.setConfig(key, redisValue, MIGRATION_SYSTEM_USER);
                        LOG.info("Migrated flag {} with value {} from Redis to Cassandra", key, redisValue);
                        migrated++;
                    } else {
                        LOG.debug("Skipping flag {} - not a valid ConfigKey", key);
                        skipped++;
                    }
                } else {
                    LOG.debug("Flag {} not found in Redis, skipping", key);
                    skipped++;
                }

            } catch (Exception e) {
                LOG.warn("Failed to migrate flag {}: {}", key, e.getMessage());
            }
        }

        LOG.info("Migration complete - migrated: {}, skipped: {}", migrated, skipped);
    }
}
