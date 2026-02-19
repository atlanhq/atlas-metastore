# Redis Startup Dependency Reduction in Atlas Metastore

## Overview

This document outlines the targeted approach to eliminate Atlas's **hard startup dependency** on Redis, improving system resilience when Redis is partially or fully unavailable during pod initialization.

## Problem Statement

### Incident Context

On Feb 19, 2026, a Redis Helm chart issue caused `redis-node-2` to fail during rollout restarts. Even though 2 of 3 Redis pods were healthy, **Atlas pods could not start** because:

1. Redis Helm chart had a templating bug with startup probes
2. `redis-node-2` failed to start
3. Atlas's `RedisServiceImpl.init()` blocks with infinite retry until ALL Redis nodes are resolvable
4. DNS lookup for `redis-node-2` fails (headless service removes unhealthy endpoints)
5. Atlas startup blocks indefinitely - **complete tenant outage**

### Root Cause: Startup Blocking

**Two blocking points identified:**

1. **`RedisServiceImpl.java:26-54`**
```java
@PostConstruct
public void init() throws InterruptedException {
    while (true) {  // INFINITE LOOP - blocks startup
        try {
            redisClient = Redisson.create(getProdConfig());
            testRedisConnectivity();
            break;
        } catch (Exception e) {
            LOG.warn("Redis connection failed... Application startup is BLOCKED...");
            Thread.sleep(RETRY_DELAY_MS);
        }
    }
}
```

2. **`FeatureFlagStore.java:69-93`**
```java
@DependsOn("redisServiceImpl")  // Waits for Redis first
@PostConstruct
public void initialize() throws InterruptedException {
    while (true) {  // ANOTHER INFINITE LOOP
        try {
            validateDependencies();  // Tests Redis
            preloadAllFlags();       // Reads from Redis
            break;
        } catch (Exception e) {
            Thread.sleep(INIT_RETRY_DELAY_MS);
        }
    }
}
```

### Impact

- 8-9 tenants affected with complete Atlas unavailability
- Single Redis pod failure causes complete Atlas outage
- No graceful degradation - Atlas is all-or-nothing with Redis at startup

### References

- [Slack Thread](https://atlan.enterprise.slack.com/archives/C09SEEUK9DK/p1771492220078869)
- [Confluence: Redis-Atlas Incident Summary](https://atlanhq.atlassian.net/wiki/spaces/~712020e39f7acb04854242b3da42ad8e58a8bb/pages/1674346577)
- [Linear: SHA-476 - Decouple Redis clients from pod IP discovery](https://linear.app/atlan-epd/issue/SHA-476)

---

## Solution: Targeted Fix

### Approach

Instead of eliminating Redis entirely, we implement a **targeted fix** that:

1. **Move Feature Flags to Cassandra** - Remove the startup-blocking dependency
2. **Make Redis optional at startup** - Lazy/async initialization
3. **Keep Redis for distributed locking** - Locks can fail gracefully at runtime

### Why This Approach?

| Aspect | Full Redis Elimination | Targeted Fix (Chosen) |
|--------|----------------------|----------------------|
| **Complexity** | High (LWT locking, 10+ files) | **Medium (4-5 files)** |
| **Risk** | Medium-High | **Low-Medium** |
| **Time to implement** | 4-6 weeks | **1-2 weeks** |
| **Solves original problem?** | Yes | **Yes** |
| **Lock performance** | Slower (LWT 10-30ms) | **Unchanged** |

### Key Insight: Startup vs Runtime Failures

| Failure Type | Current Behavior | With Targeted Fix |
|--------------|------------------|-------------------|
| **Startup** (Redis unavailable) | Atlas won't start - **OUTAGE** | Atlas starts normally |
| **Runtime** (lock acquisition fails) | Operation retries or errors | Same - acceptable |

**Runtime lock failures are acceptable because:**
- `TaskQueueWatcher` - Sleeps and retries, tasks queue up
- `IndexRecoveryService` - Background work, retries automatically
- `TypesREST` - Returns HTTP error, client can retry

---

## Architecture

### Current State (Problematic)

```
┌─────────────────────────────────────────────────────────────────┐
│                     ATLAS STARTUP SEQUENCE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. RedisServiceImpl.init()     ──► BLOCKS until Redis ready    │
│           │                              │                       │
│           ▼                              ▼                       │
│  2. FeatureFlagStore.init()     ──► BLOCKS until Redis ready    │
│           │                              │                       │
│           ▼                              ▼                       │
│  3. Rest of Atlas starts        ──► Never reached if Redis down │
│                                                                  │
│  Result: Redis partial failure = Complete Atlas outage          │
└─────────────────────────────────────────────────────────────────┘
```

### Proposed State (Resilient)

```
┌─────────────────────────────────────────────────────────────────┐
│                     ATLAS STARTUP SEQUENCE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. CassandraFeatureFlagDAO     ──► Uses existing Cassandra     │
│           │                          (already required)          │
│           ▼                                                      │
│  2. FeatureFlagStore.init()     ──► Reads from Cassandra        │
│           │                          (no Redis dependency)       │
│           ▼                                                      │
│  3. RedisServiceImpl.init()     ──► LAZY init, non-blocking     │
│           │                          (connects on first use)     │
│           ▼                                                      │
│  4. Rest of Atlas starts        ──► Always succeeds             │
│                                                                  │
│  Result: Redis failure = Locks degrade, but Atlas runs          │
└─────────────────────────────────────────────────────────────────┘
```

---

## What Changes

### Moved to Cassandra (Startup Critical)

| Component | Current | After |
|-----------|---------|-------|
| Feature Flags | Redis (blocking) | **Cassandra** |

### Stays on Redis (Runtime, Graceful Degradation)

| Component | Behavior on Redis Failure |
|-----------|--------------------------|
| Distributed Locks | Acquisition fails, operation retries/errors |
| TypeDef Version | Falls back to local version |
| Search Context Cache | Cache miss, re-fetches from ES |

---

## Implementation Components

### New Components

| Component | Purpose |
|-----------|---------|
| `CassandraFeatureFlagDAO` | Feature flag CRUD using Cassandra |

### Modified Components

| Component | Change |
|-----------|--------|
| `FeatureFlagStore` | Use Cassandra, remove `@DependsOn("redisServiceImpl")` |
| `RedisServiceImpl` | Lazy initialization, non-blocking startup |
| `AbstractRedisService` | Add `isAvailable()` method for graceful checks |

### Unchanged Components

| Component | Reason |
|-----------|--------|
| `TaskQueueWatcher` | Already handles lock failures with retry |
| `IndexRecoveryService` | Already handles lock failures with retry |
| `TypesREST` | Already returns errors on lock failure |

---

## Benefits

1. **Solves the Original Problem** - Atlas starts even when Redis is unavailable
2. **Minimal Changes** - Only 4-5 files modified
3. **Low Risk** - Feature flags are read-heavy with caching; Cassandra is battle-tested
4. **Fast Implementation** - 1-2 weeks vs 4-6 weeks for full elimination
5. **Preserves Lock Performance** - Redis locks are fast (~1ms vs 10-30ms for Cassandra LWT)
6. **Stepping Stone** - Doesn't preclude full Redis elimination later if desired

---

## Trade-offs Accepted

1. **Redis Still Required for Locks** - But lock failures are runtime errors, not startup blockers
2. **Two Storage Systems** - Cassandra for flags, Redis for locks (already have both)
3. **Operational Complexity** - Redis cluster still needs maintenance

---

## Documents

| Document | Description |
|----------|-------------|
| [Cassandra Feature Flag Store](./cassandra-feature-flag-store.md) | Schema and implementation for feature flags |
| [Redis Lazy Initialization](./redis-lazy-init.md) | Making Redis optional at startup |
| [Code Changes Required](./code-changes-required.md) | Specific file changes |
| [Migration Plan](./migration-plan.md) | 2-week rollout plan |

---

## Success Criteria

1. **Atlas starts successfully** when Redis is completely unavailable
2. **Feature flags work** from Cassandra with no Redis dependency
3. **Lock operations degrade gracefully** - return errors, don't crash
4. **No performance regression** in normal operation
5. **Existing behavior preserved** when Redis is healthy
