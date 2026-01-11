# WARE-16 Implementation Notes

## Issue
CMA CGM Snowflake workflow failing with 404 and 400 errors when publishing entities that reference non-existent parent entities (specifically Database entities).

## Root Cause Analysis

The workflow was hitting two error scenarios:

1. **ATLAS-404-00-00A**: "Referenced entity ... is not found"
   - Triggered in `UniqAttrBasedEntityResolver.java` and `IDBasedEntityResolver.java`
   - Occurs when an entity references a parent entity by unique attributes that doesn't exist in Atlas

2. **ATLAS-400-00-01A**: "invalid parameters: no entities to create/update"
   - Triggered in `EntityREST.java` and `AtlasEntityStoreV2.java`
   - Occurs when all entities in a batch are filtered out due to validation errors

The core issue was that `REST_API_CREATE_SHELL_ENTITY_FOR_NON_EXISTING_REF` was set to `false` by default, while the notification consumer had this set to `true`. This inconsistency meant that:
- Entities ingested via Kafka notifications would create shell entities for missing references
- Entities ingested via REST API would fail with 404 errors

## Solution Architecture

### Two-Pronged Approach

1. **Default Behavior Change** (Long-term fix)
   - Changed `REST_API_CREATE_SHELL_ENTITY_FOR_NON_EXISTING_REF` default from `false` to `true`
   - Makes REST API consistent with notification consumer
   - Prevents future failures across all workflows

2. **Query Parameter Override** (Flexible control)
   - Added `createShellEntityForNonExistingReference` query parameter support
   - Allows clients to override default behavior per-request
   - Enables backward compatibility if needed

### Shell Entity Mechanism

When `createShellEntityForNonExistingReference` is true:

1. Entity resolution happens in `UniqAttrBasedEntityResolver.resolveEntityReferences()`
2. If referenced entity not found, checks `RequestContext.get().isCreateShellEntityForNonExistingReference()`
3. If true, calls `entityGraphMapper.createShellEntityVertex(objId, context)`
4. Shell entity is created with:
   - System-generated GUID
   - Type name from reference
   - Unique attributes from reference
   - `isIncomplete=true` marker

This allows child entities to maintain referential integrity while parent data is being resolved.

## Code Flow

```
HTTP Request with query param
    ↓
AuditFilter.doFilter()
    ↓ (reads query param)
RequestContext.setCreateShellEntityForNonExistingReference()
    ↓
EntityREST.createOrUpdate()
    ↓
AtlasEntityStoreV2.createOrUpdate()
    ↓
preCreateOrUpdate() → resolveEntityReferences()
    ↓
UniqAttrBasedEntityResolver.resolveEntityReferences()
    ↓ (if entity not found)
Checks RequestContext.get().isCreateShellEntityForNonExistingReference()
    ↓ (if true)
EntityGraphMapper.createShellEntityVertex()
```

## Implementation Details

### AuditFilter Changes
```java
// Extract query parameter
final String createShellEntityParam = httpRequest.getParameter("createShellEntityForNonExistingReference");

// Override default if parameter present
boolean shouldCreateShellEntity = createShellEntityForNonExistingReference;
if (StringUtils.isNotEmpty(createShellEntityParam)) {
    shouldCreateShellEntity = Boolean.parseBoolean(createShellEntityParam);
    LOG.debug("Overriding createShellEntityForNonExistingReference with query parameter value: {}", shouldCreateShellEntity);
}
requestContext.setCreateShellEntityForNonExistingReference(shouldCreateShellEntity);
```

### AtlasConfiguration Changes
```java
// Before
REST_API_CREATE_SHELL_ENTITY_FOR_NON_EXISTING_REF("atlas.rest.create.shell.entity.for.non-existing.ref", false)

// After  
REST_API_CREATE_SHELL_ENTITY_FOR_NON_EXISTING_REF("atlas.rest.create.shell.entity.for.non-existing.ref", true)
```

## Testing Strategy

### Unit Tests
Created `AuditFilterTest.java` with test cases for:
- Query parameter enables shell entity creation
- Query parameter disables shell entity creation  
- Default behavior without query parameter
- Combination with other parameters (skipFailedEntities)
- Both parameters used together

### Integration Testing Recommendations
1. Test with real Snowflake workflow data
2. Verify shell entities created with correct attributes
3. Monitor for any performance impact
4. Verify existing tests still pass

## Deployment Considerations

### Configuration Override
If needed to revert globally:
```properties
atlas.rest.create.shell.entity.for.non-existing.ref=false
```

### Gradual Rollout
Can enable per-workflow by adding query parameter:
```
?createShellEntityForNonExistingReference=true
```

### Monitoring Metrics
- Count of shell entities created
- 404 error rate (should decrease)
- Workflow success rate (should increase)
- API response times (should not significantly change)

## Backward Compatibility

✅ **Fully backward compatible**
- Old clients without query parameter get new default behavior (create shell entities)
- Clients can explicitly disable via query parameter if needed
- Configuration property can override default
- No breaking changes to API contracts

## Performance Impact

Expected to be **minimal**:
- Shell entity creation is lightweight (only unique attributes)
- Already implemented and used by notification consumer
- No additional database queries
- Prevents multiple retry attempts (saves resources)

## Security Considerations

✅ **No security impact**
- Query parameter follows existing pattern (skipFailedEntities)
- No new authorization required
- Shell entities created with same permissions as regular entities
- Audit logging unchanged

## Known Limitations

1. Shell entities need to be completed later with full data
2. Queries on incomplete entities may return partial results
3. Shell entities increase storage slightly
4. May mask underlying data quality issues if overused

## Future Enhancements

Potential improvements:
1. Background job to detect and complete shell entities
2. Metrics dashboard for shell entity tracking
3. Configurable TTL for incomplete shell entities
4. Notification system for shell entity creation
5. API endpoint to query incomplete entities

## References

### Related Code Files
- Error codes: `intg/src/main/java/org/apache/atlas/AtlasErrorCode.java`
- Entity REST: `webapp/src/main/java/org/apache/atlas/web/rest/EntityREST.java`
- Entity store: `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/AtlasEntityStoreV2.java`
- Resolvers: 
  - `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/UniqAttrBasedEntityResolver.java`
  - `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/IDBasedEntityResolver.java`
- Mapper: `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/EntityGraphMapper.java`

### Related Issues
- Linear: WARE-16
- Workflow: https://cmacgm-qua.atlan.com/workflows/profile/atlan-snowflake-1721978654/runs?name=atlan-snowflake-1721978654-cron-1767567600
