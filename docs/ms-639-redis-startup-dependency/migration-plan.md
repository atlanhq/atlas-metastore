# Migration Plan - Targeted Approach

## Overview

This document outlines the 2-week migration plan for the targeted fix: moving feature flags to Cassandra and making Redis optional at startup.

## Timeline

| Week | Phase | Deliverables |
|------|-------|--------------|
| Week 1 | Implementation | CassandraFeatureFlagDAO, FeatureFlagStore changes, Redis lazy init |
| Week 2 | Testing & Rollout | Canary deployment, GA release |

---

## Week 1: Implementation

### Day 1-2: Cassandra Feature Flags

**Tasks:**

| Task | Owner | Status |
|------|-------|--------|
| Create Cassandra schema for feature flags | Backend | |
| Implement `CassandraFeatureFlagDAO.java` | Backend | |
| Unit tests for CassandraFeatureFlagDAO | Backend | |
| Update `FeatureFlagStore.java` to use Cassandra | Backend | |
| Remove `@DependsOn("redisServiceImpl")` annotations | Backend | |

**Deliverables:**
- [ ] `CassandraFeatureFlagDAO.java` created
- [ ] `FeatureFlagStore.java` updated
- [ ] Unit tests passing

### Day 3-4: Redis Lazy Initialization

**Tasks:**

| Task | Owner | Status |
|------|-------|--------|
| Modify `RedisServiceImpl.init()` to be non-blocking | Backend | |
| Add `isAvailable()` to `RedisService` interface | Backend | |
| Implement bounded retry logic (3 attempts max) | Backend | |
| Add background initialization thread | Backend | |
| Unit tests for lazy initialization | Backend | |

**Deliverables:**
- [ ] `RedisServiceImpl.java` updated
- [ ] `RedisService.java` interface updated
- [ ] `AbstractRedisService.java` updated
- [ ] Unit tests passing

### Day 5: Integration Testing

**Tasks:**

| Task | Owner | Status |
|------|-------|--------|
| Integration test: Atlas starts without Redis | QA | |
| Integration test: Feature flags work from Cassandra | QA | |
| Integration test: Locks degrade gracefully | QA | |
| Performance test: No regression with Cassandra flags | QA | |

**Test Scenarios:**

```
Scenario 1: Normal startup (Redis healthy)
  - Atlas starts normally
  - Feature flags loaded from Cassandra
  - Redis connects in background
  - Locks work as expected
  Expected: No behavior change

Scenario 2: Startup without Redis
  - Redis not running
  - Atlas should start successfully
  - Feature flags work from Cassandra
  - Lock operations return errors/retry
  Expected: Atlas runs, locks degraded

Scenario 3: Redis becomes available after startup
  - Start Atlas without Redis
  - Start Redis after Atlas is running
  - Lock operations should start working
  Expected: Automatic recovery

Scenario 4: Redis goes down during runtime
  - Start Atlas with Redis healthy
  - Stop Redis
  - Feature flags continue working (cached + Cassandra)
  - Lock operations fail gracefully
  Expected: Graceful degradation
```

---

## Week 2: Testing & Rollout

### Day 6-7: Code Review & Bug Fixes

**Tasks:**

| Task | Owner | Status |
|------|-------|--------|
| Code review for all changes | Team | |
| Address review feedback | Backend | |
| Fix any bugs found in integration testing | Backend | |
| Documentation review | Tech Writer | |

### Day 8-9: Canary Deployment

**Canary Tenants (5-10):**

| Tenant | Type | Reason |
|--------|------|--------|
| `hellofresh-sandbox` | Sandbox | Active testing tenant |
| `zoom2` | Production-like | High usage |
| `autodesk-sandbox2` | Sandbox | Good test coverage |
| (+ 2-3 more) | Mixed | Coverage |

**Canary Validation:**

| Check | Method | Success Criteria |
|-------|--------|------------------|
| Atlas startup | Monitor pod status | All pods reach Ready state |
| Feature flags | Check flag values | Flags match expected values |
| Lock operations | Monitor logs | Locks acquired/released normally |
| Performance | Grafana dashboards | No latency regression |
| Error rate | Prometheus alerts | No increase in errors |

**Monitoring During Canary:**
- Watch Atlas application logs for errors
- Monitor `atlas_redis_available` metric
- Check feature flag access latency
- Verify task processing continues

### Day 10: GA Release

**Pre-GA Checklist:**

- [ ] Canary tenants stable for 48+ hours
- [ ] No new errors in logs
- [ ] Performance metrics normal
- [ ] Rollback procedure tested
- [ ] On-call team briefed

**Release Steps:**

1. **Prepare release:**
   ```bash
   git tag v3.x.x-redis-lazy-init
   docker build -t atlas-metastore:v3.x.x .
   ```

2. **Update Helm values:**
   ```yaml
   # No special config needed - changes are in code
   ```

3. **Deploy to production:**
   - Rolling update across all tenants
   - Monitor deployment progress
   - Watch for pod startup issues

4. **Post-deployment validation:**
   - Verify all pods healthy
   - Check feature flag functionality
   - Monitor lock operations
   - Watch error rates

---

## Data Migration

### Feature Flags Migration

Feature flags need to be migrated from Redis to Cassandra for each tenant.

**Migration Script:**

```java
public class FeatureFlagMigration {

    public void migrate(RedisService redis, CassandraFeatureFlagDAO cassandra) {
        LOG.info("Starting feature flag migration...");

        List<String> knownFlags = FeatureFlag.getAllKeys();
        int migrated = 0;
        int skipped = 0;

        for (String flagKey : knownFlags) {
            try {
                String redisKey = "ff:" + flagKey;
                String value = redis.getValue(redisKey);

                if (value != null) {
                    cassandra.putValue(flagKey, value);
                    LOG.info("Migrated: {} = {}", flagKey, value);
                    migrated++;
                } else {
                    LOG.debug("Skipped (not in Redis): {}", flagKey);
                    skipped++;
                }
            } catch (Exception e) {
                LOG.error("Failed to migrate flag: {}", flagKey, e);
            }
        }

        LOG.info("Migration complete: {} migrated, {} skipped", migrated, skipped);
    }
}
```

**Migration Options:**

1. **Option A: Run migration during deployment**
   - Add migration step to Helm hooks
   - Runs automatically on upgrade

2. **Option B: Run migration manually**
   - Execute migration script per tenant
   - More control, but more effort

3. **Option C: Lazy migration (Recommended)**
   - Don't migrate upfront
   - Feature flags use defaults if not in Cassandra
   - First write to a flag populates Cassandra
   - Existing flags eventually migrated through normal usage

---

## Rollback Procedure

### Rollback Trigger Criteria

Initiate rollback if any of:
- Atlas pods failing to start
- Feature flag errors in logs
- Significant increase in error rate
- Critical functionality broken

### Rollback Steps

**Step 1: Revert to Previous Image**

```bash
# Get previous image tag
kubectl get deployment atlas -o jsonpath='{.spec.template.spec.containers[0].image}'

# Rollback to previous version
kubectl rollout undo deployment/atlas

# Or specify exact revision
kubectl rollout undo deployment/atlas --to-revision=<N>
```

**Step 2: Verify Rollback**

```bash
# Check pod status
kubectl get pods -l app=atlas

# Check logs for startup
kubectl logs -l app=atlas --tail=100

# Verify feature flags working
curl -s http://atlas:21000/api/atlas/admin/status
```

**Step 3: Notify Team**

- Post in #project-bedrock
- Create incident if customer-impacting
- Document what went wrong

---

## Communication Plan

### Stakeholder Updates

| When | Who | What |
|------|-----|------|
| Start of Week 1 | Team | Kick-off, scope confirmation |
| End of Day 2 | Team | Cassandra FF implementation complete |
| End of Day 4 | Team | Redis lazy init complete |
| End of Week 1 | Stakeholders | Ready for canary |
| During Canary | On-call | Status updates |
| GA Release | All | Release announcement |

### Slack Updates

Post updates to `#project-bedrock`:

```
📢 Redis Startup Dependency Fix - Status Update

Day X:
✅ Completed: [task]
🔄 In Progress: [task]
📋 Next: [task]

Blockers: None / [blocker]
```

---

## Success Metrics

| Metric | Target | How to Measure |
|--------|--------|----------------|
| Atlas startup without Redis | 100% success | Integration test |
| Feature flag read latency | < 10ms p99 | Prometheus |
| Lock acquisition (when Redis healthy) | < 50ms p99 | Prometheus |
| Error rate | No increase | Prometheus alerts |
| Customer incidents | Zero | Incident reports |

---

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Cassandra schema issues | Low | High | Test on sandbox first |
| Feature flag migration data loss | Low | Medium | Keep Redis data, lazy migration |
| Lock degradation causes issues | Medium | Medium | Document expected behavior |
| Rollback needed | Low | Low | Clear rollback procedure |
| Performance regression | Low | Medium | Load testing before GA |

---

## Post-Release

### Monitoring (First Week)

- Daily check of Atlas startup metrics
- Monitor Redis availability metric
- Watch for lock-related errors
- Track feature flag access patterns

### Documentation Updates

- [ ] Update Atlas runbooks
- [ ] Update troubleshooting guides
- [ ] Update architecture diagrams
- [ ] Add new metrics to dashboards

### Future Considerations

After this targeted fix is stable, consider:
1. Full Redis elimination (if desired for operational simplicity)
2. Moving other Redis usages to Cassandra (typedef version, migration status)
3. Improving lock resilience further
