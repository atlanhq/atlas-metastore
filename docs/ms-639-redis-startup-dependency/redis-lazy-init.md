# Redis Lazy Initialization Design

## Overview

This document describes how to make Redis initialization **non-blocking** at Atlas startup, allowing Atlas to start even when Redis is unavailable.

## Current State: Blocking Initialization

### RedisServiceImpl.java (lines 26-54)

```java
@Component
@Profile("!local")
@Order(Ordered.HIGHEST_PRECEDENCE)  // Initializes first
public class RedisServiceImpl extends AbstractRedisService {

    @PostConstruct
    public void init() throws InterruptedException {
        LOG.info("Starting Redis service initialization.");

        // THIS BLOCKS FOREVER until Redis connects
        while (true) {
            try {
                redisClient = Redisson.create(getProdConfig());
                redisCacheClient = Redisson.create(getCacheImplConfig());
                testRedisConnectivity();
                LOG.info("Redis initialization completed successfully!");
                break;
            } catch (Exception e) {
                LOG.warn("Redis connection failed. Application startup is BLOCKED. Retrying...");
                shutdownClients();
                Thread.sleep(RETRY_DELAY_MS);  // 1 second
            }
        }
    }
}
```

### Problem

- **Infinite loop** - Never gives up trying to connect
- **Blocks Spring context** - Other beans wait for Redis
- **Single point of failure** - Partial Redis outage = complete Atlas outage

---

## Proposed Solution: Lazy Initialization

### Approach

1. **Remove blocking `@PostConstruct`** - Don't connect at startup
2. **Connect on first use** - Lazy initialization pattern
3. **Add `isAvailable()` checks** - Callers can check before using
4. **Graceful degradation** - Return errors instead of hanging

### New RedisServiceImpl

```java
@Component
@Profile("!local")
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RedisServiceImpl extends AbstractRedisService {

    private static final Logger LOG = LoggerFactory.getLogger(RedisServiceImpl.class);
    private static final int MAX_INIT_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 1000L;

    private volatile boolean initialized = false;
    private volatile boolean available = false;
    private volatile Exception lastError = null;

    private final Object initLock = new Object();

    @PostConstruct
    public void init() {
        LOG.info("RedisServiceImpl registered - will connect lazily on first use");
        // NO BLOCKING HERE - just log and return

        // Optionally start background connection attempt
        startBackgroundInitialization();
    }

    /**
     * Attempt Redis connection in background thread.
     * Does not block startup.
     */
    private void startBackgroundInitialization() {
        Thread initThread = new Thread(() -> {
            try {
                Thread.sleep(5000);  // Wait for other services to start
                ensureInitialized();
                LOG.info("Background Redis initialization successful");
            } catch (Exception e) {
                LOG.warn("Background Redis initialization failed - will retry on first use: {}",
                    e.getMessage());
            }
        }, "redis-init-background");
        initThread.setDaemon(true);
        initThread.start();
    }

    /**
     * Lazy initialization - called before any Redis operation.
     * Uses bounded retries, not infinite loop.
     */
    private void ensureInitialized() {
        if (initialized && available) {
            return;  // Already connected
        }

        synchronized (initLock) {
            if (initialized && available) {
                return;  // Double-check after acquiring lock
            }

            for (int attempt = 1; attempt <= MAX_INIT_RETRIES; attempt++) {
                try {
                    LOG.info("Attempting Redis connection (attempt {}/{})", attempt, MAX_INIT_RETRIES);

                    if (redisClient != null) {
                        shutdownClients();
                    }

                    redisClient = Redisson.create(getProdConfig());
                    redisCacheClient = Redisson.create(getCacheImplConfig());

                    testRedisConnectivity();

                    initialized = true;
                    available = true;
                    lastError = null;
                    LOG.info("Redis connection established successfully");
                    return;

                } catch (Exception e) {
                    lastError = e;
                    LOG.warn("Redis connection attempt {}/{} failed: {}",
                        attempt, MAX_INIT_RETRIES, e.getMessage());

                    if (attempt < MAX_INIT_RETRIES) {
                        try {
                            Thread.sleep(RETRY_DELAY_MS);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }

            // All retries exhausted
            initialized = true;  // Mark as "tried"
            available = false;   // But not available
            LOG.error("Redis initialization failed after {} attempts", MAX_INIT_RETRIES);
        }
    }

    /**
     * Check if Redis is available for use.
     */
    @Override
    public boolean isAvailable() {
        if (!initialized) {
            ensureInitialized();
        }
        return available;
    }

    /**
     * Get the last connection error (for diagnostics).
     */
    public Exception getLastError() {
        return lastError;
    }

    /**
     * Attempt to reconnect if previously failed.
     */
    public void reconnect() {
        synchronized (initLock) {
            initialized = false;
            available = false;
            ensureInitialized();
        }
    }

    // ========== Override Redis Operations with Availability Checks ==========

    @Override
    public boolean acquireDistributedLock(String key) throws Exception {
        if (!isAvailable()) {
            throw new RedisUnavailableException("Redis is not available", lastError);
        }
        return super.acquireDistributedLock(key);
    }

    @Override
    public void releaseDistributedLock(String key) {
        if (!isAvailable()) {
            LOG.warn("Cannot release lock {} - Redis unavailable", key);
            return;
        }
        super.releaseDistributedLock(key);
    }

    @Override
    public String getValue(String key) {
        if (!isAvailable()) {
            LOG.warn("Cannot get value for {} - Redis unavailable", key);
            return null;
        }
        return super.getValue(key);
    }

    @Override
    public String getValue(String key, String defaultValue) {
        if (!isAvailable()) {
            LOG.warn("Cannot get value for {} - Redis unavailable, returning default", key);
            return defaultValue;
        }
        return super.getValue(key, defaultValue);
    }

    @Override
    public String putValue(String key, String value) {
        if (!isAvailable()) {
            throw new RedisUnavailableException("Redis is not available for writes", lastError);
        }
        return super.putValue(key, value);
    }

    @Override
    public String putValue(String key, String value, int timeout) {
        if (!isAvailable()) {
            throw new RedisUnavailableException("Redis is not available for writes", lastError);
        }
        return super.putValue(key, value, timeout);
    }

    @Override
    public void removeValue(String key) {
        if (!isAvailable()) {
            LOG.warn("Cannot remove value for {} - Redis unavailable", key);
            return;
        }
        super.removeValue(key);
    }
}
```

### RedisUnavailableException

```java
package org.apache.atlas.service.redis;

public class RedisUnavailableException extends RuntimeException {

    public RedisUnavailableException(String message) {
        super(message);
    }

    public RedisUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

---

## Changes to RedisService Interface

Add availability check method:

```java
public interface RedisService {

    // Existing methods...
    boolean acquireDistributedLock(String key) throws Exception;
    void releaseDistributedLock(String key);
    String getValue(String key);
    String getValue(String key, String defaultValue);
    String putValue(String key, String value);
    String putValue(String key, String value, int timeout);
    void removeValue(String key);

    // NEW: Availability check
    default boolean isAvailable() {
        return true;  // Default for backward compatibility
    }
}
```

---

## Changes to AbstractRedisService

Add base implementation:

```java
public abstract class AbstractRedisService implements RedisService {

    protected volatile RedissonClient redisClient;
    protected volatile RedissonClient redisCacheClient;

    // ... existing code ...

    /**
     * Check if Redis clients are connected and healthy.
     */
    @Override
    public boolean isAvailable() {
        try {
            return redisClient != null &&
                   !redisClient.isShutdown() &&
                   redisCacheClient != null &&
                   !redisCacheClient.isShutdown();
        } catch (Exception e) {
            return false;
        }
    }
}
```

---

## Impact on Lock Consumers

### TaskQueueWatcher (Already Handles Failures)

```java
// Current code (line 121-124)
if (!redisService.acquireDistributedLock(ATLAS_TASK_LOCK)) {
    Thread.sleep(AtlasConstants.TASK_WAIT_TIME_MS);  // 15 seconds
    continue;  // Retry
}
```

**No changes needed** - already handles lock failure with retry.

### IndexRecoveryService (Already Handles Failures)

```java
// Current code (line 180-183)
if (!redisService.acquireDistributedLock(ATLAS_INDEX_RECOVERY_LOCK)) {
    Thread.sleep(AtlasConstants.TASK_WAIT_TIME_MS);
    continue;
}
```

**No changes needed** - already handles lock failure with retry.

### TypesREST (Add Better Error Message)

```java
// Current code throws exception on lock failure
Lock lock = redisService.acquireDistributedLockV2(typeDefLock);
if (lock == null) {
    throw new AtlasBaseException(AtlasErrorCode.FAILED_TO_OBTAIN_TYPE_UPDATE_LOCK);
}
```

**Enhancement:** Add Redis availability context to error:

```java
if (!redisService.isAvailable()) {
    throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR,
        "TypeDef operations temporarily unavailable - Redis not connected");
}
Lock lock = redisService.acquireDistributedLockV2(typeDefLock);
if (lock == null) {
    throw new AtlasBaseException(AtlasErrorCode.FAILED_TO_OBTAIN_TYPE_UPDATE_LOCK);
}
```

---

## Behavior Comparison

| Scenario | Current Behavior | New Behavior |
|----------|-----------------|--------------|
| Redis healthy at startup | Blocks until connected, then starts | Connects in background, starts immediately |
| Redis down at startup | **Blocks forever** - Atlas never starts | **Starts normally** - Redis ops fail gracefully |
| Redis goes down at runtime | Lock ops fail, retry | Same - no change |
| Redis recovers | Automatic reconnect | Automatic reconnect (on next operation) |

---

## Metrics and Monitoring

Add metrics to track Redis availability:

```java
@Component
public class RedisMetrics {

    private final MeterRegistry registry;

    public RedisMetrics(MeterRegistry registry) {
        this.registry = registry;

        // Gauge for availability
        Gauge.builder("atlas.redis.available", this::isRedisAvailable)
            .description("Redis availability (1=available, 0=unavailable)")
            .register(registry);
    }

    private double isRedisAvailable() {
        RedisService redis = getRedisService();
        return redis != null && redis.isAvailable() ? 1.0 : 0.0;
    }

    public void recordConnectionFailure() {
        registry.counter("atlas.redis.connection.failures").increment();
    }

    public void recordLockFailure(String lockName) {
        registry.counter("atlas.redis.lock.failures",
            "lock", lockName).increment();
    }
}
```

### Alerts

```yaml
# Alert if Redis unavailable for extended period
- alert: AtlasRedisUnavailable
  expr: atlas_redis_available == 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Redis unavailable for Atlas"
    description: "Redis has been unavailable for 5+ minutes. Lock operations will fail."

# Alert on connection failures
- alert: AtlasRedisConnectionFailures
  expr: increase(atlas_redis_connection_failures[5m]) > 10
  labels:
    severity: warning
  annotations:
    summary: "Redis connection failures detected"
```

---

## Testing

### Unit Tests

```java
@Test
public void testStartupWithoutRedis() {
    // Redis not available
    RedisServiceImpl service = new RedisServiceImpl(brokenConfig);
    service.init();  // Should not block

    assertFalse(service.isAvailable());
    assertNotNull(service.getLastError());
}

@Test
public void testLazyInitialization() {
    // Redis becomes available after startup
    RedisServiceImpl service = new RedisServiceImpl(config);
    service.init();  // Non-blocking

    // First operation triggers connection
    startRedis();
    assertTrue(service.isAvailable());
}

@Test
public void testGracefulDegradation() {
    RedisServiceImpl service = new RedisServiceImpl(brokenConfig);
    service.init();

    // Operations should fail gracefully, not hang
    assertThrows(RedisUnavailableException.class,
        () -> service.acquireDistributedLock("test"));

    assertNull(service.getValue("test"));
    assertEquals("default", service.getValue("test", "default"));
}
```

### Integration Tests

```java
@Test
public void testAtlasStartsWithoutRedis() {
    // Stop Redis
    redisContainer.stop();

    // Start Atlas
    AtlasApplication atlas = new AtlasApplication();
    atlas.start();

    // Atlas should be running
    assertTrue(atlas.isRunning());

    // Feature flags should work (from Cassandra)
    assertNotNull(FeatureFlagStore.getFlag("ENABLE_JANUS_OPTIMISATION"));

    // Lock operations should fail gracefully
    assertThrows(RedisUnavailableException.class,
        () -> redisService.acquireDistributedLock("test-lock"));
}
```

---

## Configuration

```properties
# Maximum connection retries (default: 3)
atlas.redis.init.max.retries=3

# Retry delay in ms (default: 1000)
atlas.redis.init.retry.delay.ms=1000

# Background init delay in ms (default: 5000)
atlas.redis.init.background.delay.ms=5000
```

---

## Rollback

If issues arise:

1. Revert to blocking initialization:
   ```properties
   atlas.redis.init.blocking=true  # Feature flag for old behavior
   ```

2. Or revert the code changes entirely
