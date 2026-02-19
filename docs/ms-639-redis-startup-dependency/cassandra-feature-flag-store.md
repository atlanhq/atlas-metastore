# Cassandra Feature Flag Store Design

## Overview

This document describes the design for moving feature flags from Redis to Cassandra, eliminating the startup-blocking dependency on Redis.

## Current State

### How Feature Flags Work Today

**File:** `common/src/main/java/org/apache/atlas/service/FeatureFlagStore.java`

```java
@Component
@DependsOn("redisServiceImpl")  // PROBLEM: Waits for Redis
public class FeatureFlagStore {

    @PostConstruct
    public void initialize() {
        while (true) {  // PROBLEM: Infinite loop
            try {
                validateDependencies();  // Tests Redis connectivity
                preloadAllFlags();       // Loads from Redis
                break;
            } catch (Exception e) {
                Thread.sleep(20000);  // Retry every 20s
            }
        }
    }
}
```

### Current Redis Operations

| Operation | Method | Line | Purpose |
|-----------|--------|------|---------|
| Health check | `putValue`, `getValue`, `removeValue` | 99-102 | Validate Redis connectivity |
| Read flag | `getValue(namespacedKey)` | 138 | Fetch flag value |
| Write flag | `putValue(namespacedKey, value)` | 330 | Update flag value |
| Delete flag | `removeValue(namespacedKey)` | 361 | Remove flag |

### Current Key Format

```
ff:{flagName}
```

Example: `ff:ENABLE_JANUS_OPTIMISATION`

---

## Proposed Cassandra Schema

### Keyspace

Use the existing `atlas` keyspace (same as JanusGraph) to avoid creating new infrastructure:

```cql
-- Using existing atlas keyspace
-- If separate keyspace preferred:
CREATE KEYSPACE IF NOT EXISTS atlas_config
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    '<datacenter>': 3  -- Match JanusGraph replication
};
```

### Feature Flags Table

```cql
CREATE TABLE IF NOT EXISTS atlas.feature_flags (
    flag_name   TEXT PRIMARY KEY,
    flag_value  TEXT,
    updated_at  TIMESTAMP,
    updated_by  TEXT
);
```

### Example Data

```cql
INSERT INTO atlas.feature_flags (flag_name, flag_value, updated_at, updated_by)
VALUES ('ENABLE_JANUS_OPTIMISATION', 'true', toTimestamp(now()), 'atlas-pod-0');

INSERT INTO atlas.feature_flags (flag_name, flag_value, updated_at, updated_by)
VALUES ('ENABLE_TAGSV2', 'false', toTimestamp(now()), 'atlas-pod-1');
```

---

## Implementation

### CassandraFeatureFlagDAO

```java
package org.apache.atlas.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.time.Instant;

@Component
public class CassandraFeatureFlagDAO {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraFeatureFlagDAO.class);

    private static final String TABLE = "feature_flags";
    private static final String KEYSPACE = "atlas";  // Use existing keyspace

    private final CqlSession session;

    private PreparedStatement selectStmt;
    private PreparedStatement insertStmt;
    private PreparedStatement deleteStmt;

    @Inject
    public CassandraFeatureFlagDAO(CqlSession session) {
        this.session = session;
    }

    @PostConstruct
    public void init() {
        ensureTableExists();
        prepareStatements();
        LOG.info("CassandraFeatureFlagDAO initialized successfully");
    }

    private void ensureTableExists() {
        try {
            session.execute(
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE + " (" +
                "flag_name TEXT PRIMARY KEY, " +
                "flag_value TEXT, " +
                "updated_at TIMESTAMP, " +
                "updated_by TEXT" +
                ")"
            );
            LOG.info("Feature flags table verified/created");
        } catch (Exception e) {
            LOG.error("Failed to create feature flags table", e);
            throw new RuntimeException("Cannot initialize feature flag storage", e);
        }
    }

    private void prepareStatements() {
        selectStmt = session.prepare(
            "SELECT flag_value FROM " + KEYSPACE + "." + TABLE +
            " WHERE flag_name = ?"
        );

        insertStmt = session.prepare(
            "INSERT INTO " + KEYSPACE + "." + TABLE +
            " (flag_name, flag_value, updated_at, updated_by) VALUES (?, ?, ?, ?)"
        );

        deleteStmt = session.prepare(
            "DELETE FROM " + KEYSPACE + "." + TABLE +
            " WHERE flag_name = ?"
        );
    }

    /**
     * Get a feature flag value.
     * @return flag value or null if not found
     */
    public String getValue(String flagName) {
        try {
            ResultSet rs = session.execute(selectStmt.bind(flagName));
            Row row = rs.one();
            return row != null ? row.getString("flag_value") : null;
        } catch (Exception e) {
            LOG.error("Failed to get feature flag: {}", flagName, e);
            throw new RuntimeException("Failed to read feature flag: " + flagName, e);
        }
    }

    /**
     * Set a feature flag value.
     */
    public void putValue(String flagName, String value) {
        try {
            String podId = System.getenv().getOrDefault("HOSTNAME", "unknown");
            session.execute(insertStmt.bind(
                flagName,
                value,
                Instant.now(),
                podId
            ));
            LOG.debug("Set feature flag: {} = {}", flagName, value);
        } catch (Exception e) {
            LOG.error("Failed to set feature flag: {}", flagName, e);
            throw new RuntimeException("Failed to write feature flag: " + flagName, e);
        }
    }

    /**
     * Delete a feature flag.
     */
    public void removeValue(String flagName) {
        try {
            session.execute(deleteStmt.bind(flagName));
            LOG.debug("Removed feature flag: {}", flagName);
        } catch (Exception e) {
            LOG.error("Failed to remove feature flag: {}", flagName, e);
            throw new RuntimeException("Failed to delete feature flag: " + flagName, e);
        }
    }

    /**
     * Health check - verify Cassandra connectivity.
     */
    public boolean isHealthy() {
        try {
            session.execute("SELECT release_version FROM system.local");
            return true;
        } catch (Exception e) {
            LOG.warn("Cassandra health check failed", e);
            return false;
        }
    }
}
```

### Obtaining the CqlSession

The `CqlSession` can be obtained from the existing JanusGraph configuration. Atlas already connects to Cassandra for JanusGraph:

```java
@Configuration
public class CassandraConfig {

    @Bean
    public CqlSession cassandraSession() {
        // Option 1: Reuse JanusGraph's session (preferred)
        // The session is already configured in atlas-application.properties

        // Option 2: Create from existing config
        Configuration atlasConfig = ApplicationProperties.get();
        String[] hosts = atlasConfig.getStringArray("atlas.graph.storage.hostname");
        int port = atlasConfig.getInt("atlas.graph.storage.port", 9042);

        return CqlSession.builder()
            .addContactPoint(new InetSocketAddress(hosts[0], port))
            .withLocalDatacenter(atlasConfig.getString("atlas.graph.storage.cql.local-datacenter"))
            .withKeyspace("atlas")
            .build();
    }
}
```

---

## Changes to FeatureFlagStore

### Before (Redis-dependent)

```java
@Component
@DependsOn("redisServiceImpl")
public class FeatureFlagStore {

    private final RedisService redisService;

    @Inject
    public FeatureFlagStore(RedisService redisService, ...) {
        this.redisService = redisService;
    }

    private void validateDependencies() {
        String testKey = "ff:_health_check";
        redisService.putValue(testKey, "test");
        redisService.getValue(testKey);
        redisService.removeValue(testKey);
    }

    private String loadFlagFromRedisWithRetry(String namespacedKey, String flagKey) {
        return redisService.getValue(namespacedKey);
    }

    void setFlagInternal(String key, String value) {
        redisService.putValue(namespacedKey, value);
    }

    void deleteFlagInternal(String key) {
        redisService.removeValue(namespacedKey);
    }
}
```

### After (Cassandra-based)

```java
@Component
// REMOVED: @DependsOn("redisServiceImpl")
public class FeatureFlagStore {

    private final CassandraFeatureFlagDAO flagDAO;

    @Inject
    public FeatureFlagStore(CassandraFeatureFlagDAO flagDAO, ...) {
        this.flagDAO = flagDAO;
    }

    private void validateDependencies() {
        // Cassandra health check - no test write needed
        if (!flagDAO.isHealthy()) {
            throw new RuntimeException("Cassandra not available");
        }
    }

    private String loadFlagFromCassandraWithRetry(String flagKey) {
        // Retry logic remains the same, just uses Cassandra
        for (int attempt = 1; attempt <= config.getRetryAttempts(); attempt++) {
            try {
                return flagDAO.getValue(flagKey);
            } catch (Exception e) {
                if (attempt == config.getRetryAttempts()) {
                    throw e;
                }
                Thread.sleep(calculateBackoffDelay(attempt));
            }
        }
        return null;
    }

    void setFlagInternal(String key, String value) {
        flagDAO.putValue(key, value);
        cacheStore.putInBothCaches(addFeatureFlagNamespace(key), value);
    }

    void deleteFlagInternal(String key) {
        flagDAO.removeValue(key);
        cacheStore.removeFromBothCaches(addFeatureFlagNamespace(key));
    }
}
```

---

## Migration: Redis to Cassandra

### One-Time Migration Script

Run once per tenant to migrate existing flags:

```java
public class FeatureFlagMigration {

    public void migrate(RedisService redis, CassandraFeatureFlagDAO cassandra) {
        List<String> knownFlags = FeatureFlag.getAllKeys();

        for (String flagKey : knownFlags) {
            String redisKey = "ff:" + flagKey;
            String value = redis.getValue(redisKey);

            if (value != null) {
                cassandra.putValue(flagKey, value);
                LOG.info("Migrated flag {} = {}", flagKey, value);
            }
        }

        LOG.info("Migration complete: {} flags processed", knownFlags.size());
    }
}
```

### Migration Steps

1. **Deploy new code** with CassandraFeatureFlagDAO (feature flagged off)
2. **Run migration script** on each tenant
3. **Enable Cassandra backend** via config flag
4. **Verify flags** match between Redis and Cassandra
5. **Remove Redis flag code** in subsequent release

---

## Testing

### Unit Tests

```java
@Test
public void testPutAndGetFlag() {
    dao.putValue("TEST_FLAG", "true");
    assertEquals("true", dao.getValue("TEST_FLAG"));
}

@Test
public void testGetMissingFlag() {
    assertNull(dao.getValue("NONEXISTENT_FLAG"));
}

@Test
public void testDeleteFlag() {
    dao.putValue("DELETE_ME", "value");
    dao.removeValue("DELETE_ME");
    assertNull(dao.getValue("DELETE_ME"));
}

@Test
public void testHealthCheck() {
    assertTrue(dao.isHealthy());
}
```

### Integration Tests

```java
@Test
public void testFeatureFlagStoreWithCassandra() {
    // Verify FeatureFlagStore initializes without Redis
    FeatureFlagStore store = new FeatureFlagStore(cassandraDAO, config, cacheStore);
    store.initialize();

    assertTrue(store.isInitialized());
}

@Test
public void testStartupWithoutRedis() {
    // Stop Redis
    redisContainer.stop();

    // Atlas should still start
    AtlasApplication app = new AtlasApplication();
    app.start();

    assertTrue(app.isRunning());
    assertTrue(FeatureFlagStore.getInstance().isInitialized());
}
```

---

## Performance Comparison

| Operation | Redis | Cassandra | Notes |
|-----------|-------|-----------|-------|
| Read latency | ~1ms | ~2-5ms | Acceptable with caching |
| Write latency | ~1ms | ~5-10ms | Writes are infrequent |
| Throughput | Higher | Lower | Flags are cached; throughput irrelevant |

### Why Performance is Not a Concern

1. **Strong caching:** `FeatureFlagCacheStore` has 30-minute primary cache TTL
2. **Infrequent writes:** Flags rarely change in production
3. **Preloading:** All known flags loaded at startup
4. **Fallback cache:** Permanent fallback cache prevents repeated reads

---

## Rollback Plan

If issues arise after deployment:

1. **Re-enable Redis backend:**
   ```properties
   atlas.feature.flag.backend=redis  # Switch back
   ```

2. **Flags are still in Redis** (migration doesn't delete them)

3. **Restart Atlas pods** to pick up new config

---

## Configuration Properties

```properties
# Feature flag backend selection (for phased rollout)
atlas.feature.flag.backend=cassandra  # or 'redis' for rollback

# Cassandra settings (usually inherited from JanusGraph config)
atlas.feature.flag.cassandra.keyspace=atlas
atlas.feature.flag.cassandra.table=feature_flags

# Existing cache settings (unchanged)
atlas.feature.flag.cache.primary.ttl.minutes=30
atlas.feature.flag.redis.retry.attempts=5
atlas.feature.flag.redis.retry.delay.ms=1000
```
