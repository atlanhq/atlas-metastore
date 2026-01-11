# WARE-16: Snowflake Workflow Failure Fix

## Quick Summary

Fixed the CMA CGM Snowflake workflow failure by enabling automatic creation of shell entities for missing entity references in the Atlas REST API.

## What Was Changed

### Code Changes (2 files)

1. **`intg/src/main/java/org/apache/atlas/AtlasConfiguration.java`**
   - Changed default of `REST_API_CREATE_SHELL_ENTITY_FOR_NON_EXISTING_REF` from `false` to `true`
   - Aligns REST API behavior with notification consumer

2. **`webapp/src/main/java/org/apache/atlas/web/filters/AuditFilter.java`**
   - Added support for `createShellEntityForNonExistingReference` query parameter
   - Allows per-request override of shell entity creation behavior

### Test Coverage (1 file)

3. **`webapp/src/test/java/org/apache/atlas/web/filters/AuditFilterTest.java`**
   - Comprehensive unit tests for query parameter handling
   - Tests default behavior and overrides

### Documentation (4 files)

4. **`WARE-16-SOLUTION.md`** - Complete problem/solution documentation
5. **`CHANGE_SUMMARY.md`** - High-level summary of changes
6. **`IMPLEMENTATION_NOTES.md`** - Detailed technical implementation notes
7. **`USAGE_EXAMPLES.md`** - Practical usage examples and code samples

## The Problem

CMA CGM's Snowflake workflow was failing with:
- **404 errors**: `ATLAS-404-00-00A: Referenced entity ... is not found`
- **400 errors**: `ATLAS-400-00-01A: invalid parameters: no entities to create/update`

When entities referenced non-existent parent entities (e.g., Columns referencing deleted/missing Tables).

## The Solution

**Shell Entity Creation**: Automatically create incomplete placeholder entities for missing references instead of throwing errors.

### Before
```
Column references Table → Table doesn't exist → 404 Error → Workflow fails
```

### After
```
Column references Table → Table doesn't exist → Create shell Table → Workflow succeeds
```

## How to Use

### Default Behavior (Recommended)
```bash
POST /api/atlas/v2/entity/bulk
# Shell entities automatically created for missing references
```

### Explicit Control
```bash
# Force enable
POST /api/atlas/v2/entity/bulk?createShellEntityForNonExistingReference=true

# Force disable (old behavior)
POST /api/atlas/v2/entity/bulk?createShellEntityForNonExistingReference=false
```

### Configuration Override
```properties
# In application.properties (if needed)
atlas.rest.create.shell.entity.for.non-existing.ref=false
```

## Impact

### ✅ Benefits
- Workflow resilience - no more failures on missing references
- Automatic recovery - shell entities created seamlessly
- Consistency - REST API now matches notification consumer behavior
- Flexibility - query parameter allows per-request control

### ⚠️ Considerations
- Shell entities (marked `isIncomplete=true`) need to be completed later
- May mask underlying data quality issues if overused
- Slightly increased storage for incomplete entities

## Verification

### Check if Fix is Applied
```bash
# Should now succeed instead of 404
curl -X POST "http://atlas-server:21000/api/atlas/v2/entity/bulk" \
  -H "Content-Type: application/json" \
  -d '{
    "entities": [{
      "typeName": "Column",
      "attributes": {
        "qualifiedName": "test.column@cluster",
        "name": "test_column",
        "table": {
          "typeName": "Table",
          "uniqueAttributes": {
            "qualifiedName": "non.existent.table@cluster"
          }
        }
      }
    }]
  }'
```

### Query Shell Entities
```bash
curl -X POST "http://atlas-server:21000/api/atlas/v2/search/basic" \
  -H "Content-Type: application/json" \
  -d '{
    "typeName": "Asset",
    "query": "isIncomplete = true"
  }'
```

## Monitoring

After deployment, monitor:
- 404 error rate (should decrease)
- Workflow success rate (should increase)
- Shell entity creation count
- API response times (should remain similar)

## Testing

```bash
# Run unit tests
mvn test -Dtest=AuditFilterTest

# Run integration tests
mvn verify -pl webapp
```

## Files Reference

| File | Purpose | Lines Changed |
|------|---------|---------------|
| `AtlasConfiguration.java` | Change default config | 1 |
| `AuditFilter.java` | Add query parameter support | 13 |
| `AuditFilterTest.java` | Unit tests | 167 (new) |
| `WARE-16-SOLUTION.md` | Problem/solution docs | - |
| `CHANGE_SUMMARY.md` | Change summary | - |
| `IMPLEMENTATION_NOTES.md` | Technical details | - |
| `USAGE_EXAMPLES.md` | Usage examples | - |

## Related Resources

- **Linear Issue**: WARE-16
- **Workflow**: [CMA CGM Snowflake Workflow](https://cmacgm-qua.atlan.com/workflows/profile/atlan-snowflake-1721978654/runs?name=atlan-snowflake-1721978654-cron-1767567600)
- **Error Codes**: ATLAS-404-00-00A, ATLAS-400-00-01A
- **Configuration**: `atlas.rest.create.shell.entity.for.non-existing.ref`

## Support

For questions or issues:
1. Check `USAGE_EXAMPLES.md` for common scenarios
2. Review `IMPLEMENTATION_NOTES.md` for technical details
3. See `WARE-16-SOLUTION.md` for complete documentation

## Quick Links

- [Solution Documentation](./WARE-16-SOLUTION.md)
- [Usage Examples](./USAGE_EXAMPLES.md)
- [Implementation Notes](./IMPLEMENTATION_NOTES.md)
- [Change Summary](./CHANGE_SUMMARY.md)
