# MS-639: Redis Startup Dependency Reduction

## Problem

Atlas pods block indefinitely on startup if any Redis node is unreachable, causing complete tenant outages even when 2/3 Redis pods are healthy.

**Root cause:** `RedisServiceImpl.init()` and `FeatureFlagStore.initialize()` both use infinite retry loops that block the Spring startup sequence.

## Solution

1. **DynamicConfigStore** - Replaces `FeatureFlagStore` for feature flag reads using Cassandra (already a required dependency), removing the startup-blocking Redis dependency.
2. **Lazy Redis initialization** - `RedisServiceImpl` connects in background; all Redis operations return safe defaults when unavailable (locks return `false`, values return `null`/defaults).
3. **Graceful degradation** - Lock-dependent components (`TaskQueueWatcher`, `IndexRecoveryService`, `TypesREST`) already handle lock acquisition failures with retries or error responses. Locks use Redisson's watchdog mechanism with configurable timeout (`atlas.redis.lock.watchdog_timeout.ms`, default 10min) for auto-recovery if a node dies.

## Result

| Scenario | Before | After |
|----------|--------|-------|
| Redis partial failure at startup | Atlas won't start (outage) | Atlas starts, locks degrade gracefully |
| Redis healthy | Normal operation | Normal operation (no change) |
