# Code Changes Required - Targeted Approach

## Summary

This document lists the specific code changes needed for the **targeted fix**: moving feature flags to Cassandra and making Redis optional at startup.

**Total: 1 new file, 4-5 modified files**

---

## New Files

### 1. CassandraFeatureFlagDAO.java

**Path:** `common/src/main/java/org/apache/atlas/service/CassandraFeatureFlagDAO.java`

**Purpose:** Feature flag CRUD operations using Cassandra

**Size:** ~150 lines

See [cassandra-feature-flag-store.md](./cassandra-feature-flag-store.md) for full implementation.

---

## Modified Files

### 1. FeatureFlagStore.java

**Path:** `common/src/main/java/org/apache/atlas/service/FeatureFlagStore.java`

**Changes:**
- Remove `@DependsOn("redisServiceImpl")` annotation
- Replace `RedisService` with `CassandraFeatureFlagDAO`
- Update all Redis calls to Cassandra calls

```diff
 package org.apache.atlas.service;

-import org.apache.atlas.service.redis.RedisService;
+import org.apache.atlas.service.CassandraFeatureFlagDAO;
 // ... other imports

 @Component
-@DependsOn("redisServiceImpl")
 public class FeatureFlagStore implements ApplicationContextAware {

-    private final RedisService redisService;
+    private final CassandraFeatureFlagDAO flagDAO;
     private final FeatureFlagConfig config;
     private final FeatureFlagCacheStore cacheStore;

     @Inject
-    public FeatureFlagStore(RedisService redisService, FeatureFlagConfig config,
+    public FeatureFlagStore(CassandraFeatureFlagDAO flagDAO, FeatureFlagConfig config,
                            FeatureFlagCacheStore cacheStore) {
-        this.redisService = Objects.requireNonNull(redisService, "RedisService cannot be null");
+        this.flagDAO = Objects.requireNonNull(flagDAO, "CassandraFeatureFlagDAO cannot be null");
         this.config = Objects.requireNonNull(config, "FeatureFlagConfig cannot be null");
         this.cacheStore = Objects.requireNonNull(cacheStore, "FeatureFlagCacheStore cannot be null");
-
-        LOG.info("FeatureFlagStore dependencies injected - RedisService: {}",
-                redisService.getClass().getSimpleName());
+        LOG.info("FeatureFlagStore dependencies injected - using Cassandra backend");
     }

     private void validateDependencies() {
         LOG.info("Validating FeatureFlagStore dependencies...");
-        try {
-            String testKey = "ff:_health_check";
-            redisService.putValue(testKey, "test");
-            String testValue = redisService.getValue(testKey);
-            redisService.removeValue(testKey);
-
-            if (!"test".equals(testValue)) {
-                throw new RuntimeException("Redis connectivity test failed");
-            }
-            LOG.info("Redis connectivity validated successfully");
-        } catch (Exception e) {
-            throw new RuntimeException("Redis dependency validation failed", e);
+        if (!flagDAO.isHealthy()) {
+            throw new RuntimeException("Cassandra not available for feature flags");
         }
+        LOG.info("Cassandra connectivity validated successfully");
     }

-    private String loadFlagFromRedisWithRetry(String namespacedKey, String flagKey) {
+    private String loadFlagFromCassandraWithRetry(String flagKey) {
         for (int attempt = 1; attempt <= config.getRedisRetryAttempts(); attempt++) {
             try {
-                return redisService.getValue(namespacedKey);
+                return flagDAO.getValue(flagKey);
             } catch (Exception e) {
                 boolean isLastAttempt = (attempt == config.getRedisRetryAttempts());
                 if (isLastAttempt) {
                     LOG.error("Cassandra operation failed for flag '{}' after {} attempts", flagKey, attempt, e);
                     throw new RuntimeException("Failed to load flag " + flagKey, e);
                 }
                 long backoffDelay = calculateBackoffDelay(attempt);
                 LOG.warn("Cassandra operation failed for flag '{}' (attempt {}/{}), retrying...",
                         flagKey, attempt, config.getRedisRetryAttempts());
                 Thread.sleep(backoffDelay);
             }
         }
         return null;
     }

     private void preloadAllFlags() {
         LOG.info("Preloading all known feature flags from Cassandra...");
         for (String flagKey : KNOWN_FLAGS) {
             FeatureFlag flag = FeatureFlag.fromKey(flagKey);
-            String namespacedKey = addFeatureFlagNamespace(flagKey);
-            String value = loadFlagFromRedisWithRetry(namespacedKey, flagKey);
+            String value = loadFlagFromCassandraWithRetry(flagKey);

+            String namespacedKey = addFeatureFlagNamespace(flagKey);
             if (!StringUtils.isEmpty(value)) {
                 cacheStore.putInBothCaches(namespacedKey, value);
             } else {
                 String defaultValue = String.valueOf(flag.getDefaultValue());
                 cacheStore.putInBothCaches(namespacedKey, defaultValue);
             }
         }
     }

     private String fetchFromCassandraAndCache(String namespacedKey, String key) {
         try {
-            String value = loadFlagFromRedisWithRetry(namespacedKey, key);
+            String value = loadFlagFromCassandraWithRetry(key);
             if (value != null) {
                 cacheStore.putInBothCaches(namespacedKey, value);
             }
             return value;
         } catch (Exception e) {
             LOG.error("Failed to fetch flag '{}' from Cassandra", key, e);
             return null;
         }
     }

     synchronized void setFlagInternal(String key, String value) {
         if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
             return;
         }
         String namespacedKey = addFeatureFlagNamespace(key);
         try {
-            redisService.putValue(namespacedKey, value);
+            flagDAO.putValue(key, value);
             cacheStore.putInBothCaches(namespacedKey, value);
             LOG.info("Set feature flag '{}' to value: {}", key, value);
         } catch (Exception e) {
             LOG.error("Failed to set feature flag '{}'", key, e);
         }
     }

     synchronized void deleteFlagInternal(String key) {
         if (StringUtils.isEmpty(key)) {
             return;
         }
         String namespacedKey = addFeatureFlagNamespace(key);
         try {
-            redisService.removeValue(namespacedKey);
+            flagDAO.removeValue(key);
             cacheStore.removeFromBothCaches(namespacedKey);
             LOG.info("Deleted feature flag: {}", key);
         } catch (Exception e) {
             LOG.error("Failed to delete feature flag '{}'", key, e);
         }
     }
 }
```

---

### 2. FeatureFlagCacheStore.java

**Path:** `common/src/main/java/org/apache/atlas/service/FeatureFlagCacheStore.java`

**Changes:**
- Remove `@DependsOn("redisServiceImpl")` if present

```diff
 @Component
-@DependsOn("redisServiceImpl")
 public class FeatureFlagCacheStore {
     // ... rest unchanged
 }
```

---

### 3. RedisServiceImpl.java

**Path:** `common/src/main/java/org/apache/atlas/service/redis/RedisServiceImpl.java`

**Changes:**
- Make `init()` non-blocking with bounded retries
- Add `isAvailable()` method
- Add graceful degradation for all operations

See [redis-lazy-init.md](./redis-lazy-init.md) for full implementation.

**Key diff:**

```diff
 @Component
 @Profile("!local")
 @Order(Ordered.HIGHEST_PRECEDENCE)
 public class RedisServiceImpl extends AbstractRedisService {

+    private static final int MAX_INIT_RETRIES = 3;
+    private volatile boolean initialized = false;
+    private volatile boolean available = false;
+    private final Object initLock = new Object();

     @PostConstruct
     public void init() throws InterruptedException {
-        LOG.info("Starting Redis service initialization.");
-
-        // This loop will block until connection is successful
-        while (true) {
-            try {
-                redisClient = Redisson.create(getProdConfig());
-                redisCacheClient = Redisson.create(getCacheImplConfig());
-                testRedisConnectivity();
-                LOG.info("Redis initialization completed successfully!");
-                break;
-            } catch (Exception e) {
-                LOG.warn("Redis connection failed. Application startup is BLOCKED...");
-                Thread.sleep(RETRY_DELAY_MS);
-            }
-        }
+        LOG.info("RedisServiceImpl registered - will connect lazily on first use");
+        // Non-blocking: start background initialization
+        startBackgroundInitialization();
+    }
+
+    private void startBackgroundInitialization() {
+        Thread initThread = new Thread(() -> {
+            try {
+                Thread.sleep(5000);
+                ensureInitialized();
+            } catch (Exception e) {
+                LOG.warn("Background Redis init failed: {}", e.getMessage());
+            }
+        }, "redis-init-background");
+        initThread.setDaemon(true);
+        initThread.start();
+    }
+
+    private void ensureInitialized() {
+        if (initialized && available) return;
+
+        synchronized (initLock) {
+            if (initialized && available) return;
+
+            for (int attempt = 1; attempt <= MAX_INIT_RETRIES; attempt++) {
+                try {
+                    redisClient = Redisson.create(getProdConfig());
+                    redisCacheClient = Redisson.create(getCacheImplConfig());
+                    testRedisConnectivity();
+                    initialized = true;
+                    available = true;
+                    LOG.info("Redis connection established");
+                    return;
+                } catch (Exception e) {
+                    LOG.warn("Redis connection attempt {}/{} failed", attempt, MAX_INIT_RETRIES);
+                    if (attempt < MAX_INIT_RETRIES) {
+                        Thread.sleep(RETRY_DELAY_MS);
+                    }
+                }
+            }
+            initialized = true;
+            available = false;
+            LOG.error("Redis initialization failed after {} attempts", MAX_INIT_RETRIES);
+        }
+    }
+
+    @Override
+    public boolean isAvailable() {
+        if (!initialized) ensureInitialized();
+        return available;
     }
 }
```

---

### 4. RedisService.java (Interface)

**Path:** `common/src/main/java/org/apache/atlas/service/redis/RedisService.java`

**Changes:**
- Add `isAvailable()` method with default implementation

```diff
 public interface RedisService {

     boolean acquireDistributedLock(String key) throws Exception;
     Lock acquireDistributedLockV2(String key) throws Exception;
     void releaseDistributedLock(String key);
     void releaseDistributedLockV2(Lock lock, String key);
     String getValue(String key);
     String getValue(String key, String defaultValue);
     String putValue(String key, String value);
     String putValue(String key, String value, int timeout);
     void removeValue(String key);
     Logger getLogger();

+    /**
+     * Check if Redis is available for operations.
+     * @return true if Redis is connected and healthy
+     */
+    default boolean isAvailable() {
+        return true;  // Default for backward compatibility
+    }
 }
```

---

### 5. AbstractRedisService.java

**Path:** `common/src/main/java/org/apache/atlas/service/redis/AbstractRedisService.java`

**Changes:**
- Add base `isAvailable()` implementation

```diff
 public abstract class AbstractRedisService implements RedisService {

     protected volatile RedissonClient redisClient;
     protected volatile RedissonClient redisCacheClient;

     // ... existing code ...

+    @Override
+    public boolean isAvailable() {
+        try {
+            return redisClient != null &&
+                   !redisClient.isShutdown() &&
+                   redisCacheClient != null &&
+                   !redisCacheClient.isShutdown();
+        } catch (Exception e) {
+            return false;
+        }
+    }
 }
```

---

## Optional Enhancements

### TypesREST.java (Better Error Messages)

**Path:** `webapp/src/main/java/org/apache/atlas/web/rest/TypesREST.java`

**Changes:** Add Redis availability check for better error messages

```diff
     public AtlasTypesDef createTypeDefs(AtlasTypesDef typesDef) {
+        if (!redisService.isAvailable()) {
+            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR,
+                "TypeDef operations temporarily unavailable - distributed lock service not connected");
+        }
+
         Lock lock = redisService.acquireDistributedLockV2(typeDefLock);
         if (lock == null) {
             throw new AtlasBaseException(AtlasErrorCode.FAILED_TO_OBTAIN_TYPE_UPDATE_LOCK);
         }
         // ... rest unchanged
     }
```

---

## Files NOT Changed

These files already handle Redis failures gracefully:

| File | Reason |
|------|--------|
| `TaskQueueWatcher.java` | Already sleeps and retries on lock failure |
| `IndexRecoveryService.java` | Already sleeps and retries on lock failure |
| `TypeDefRefreshConfig.java` | Uses `getValue(key, defaultValue)` - returns default on failure |
| `SearchContextCache.java` | Returns null on failure - acceptable for cache |
| `DataMeshQNMigrationService.java` | Migration status - not critical |
| `MigrationREST.java` | Returns "not found" on failure - acceptable |

---

## Summary Table

| File | Change Type | Lines Changed (Est.) |
|------|-------------|---------------------|
| `CassandraFeatureFlagDAO.java` | NEW | ~150 |
| `FeatureFlagStore.java` | MODIFY | ~50 |
| `FeatureFlagCacheStore.java` | MODIFY | ~2 |
| `RedisServiceImpl.java` | MODIFY | ~80 |
| `RedisService.java` | MODIFY | ~10 |
| `AbstractRedisService.java` | MODIFY | ~15 |
| **Total** | | **~300 lines** |

---

## Testing Checklist

- [ ] Unit tests for `CassandraFeatureFlagDAO`
- [ ] Unit tests for lazy Redis initialization
- [ ] Integration test: Atlas starts with Redis down
- [ ] Integration test: Feature flags work from Cassandra
- [ ] Integration test: Lock operations fail gracefully when Redis down
- [ ] Load test: No performance regression with Cassandra feature flags
- [ ] Canary deployment: 5-10 tenants for 48 hours
