# WARE-16: Fix for Snowflake Workflow Failure

## Summary

Fixed CMA CGM's Snowflake workflow failure caused by references to non-existent parent entities in Atlas. The workflow was failing with 404 and 400 errors during the publishing stage when trying to create entities that referenced missing Database entities.

## Changes Made

### 1. Query Parameter Support for Shell Entity Creation
**File**: `webapp/src/main/java/org/apache/atlas/web/filters/AuditFilter.java`
- Added support for `createShellEntityForNonExistingReference` query parameter
- Allows per-request override of the shell entity creation behavior
- Similar pattern to existing `skipFailedEntities` parameter

### 2. Default Configuration Change
**File**: `intg/src/main/java/org/apache/atlas/AtlasConfiguration.java`
- Changed default value of `REST_API_CREATE_SHELL_ENTITY_FOR_NON_EXISTING_REF` from `false` to `true`
- Aligns REST API behavior with notification consumer (which already uses `true`)
- Makes the system more resilient to missing entity references

### 3. Unit Tests
**File**: `webapp/src/test/java/org/apache/atlas/web/filters/AuditFilterTest.java`
- Added comprehensive unit tests for the query parameter handling
- Tests default behavior, parameter override, and combination with other parameters

### 4. Documentation
**File**: `WARE-16-SOLUTION.md`
- Comprehensive documentation of the problem, solution, and usage
- Includes examples and monitoring recommendations

## Technical Details

### Shell Entity Creation
When enabled, the system automatically creates "shell entities" for missing references:
- Shell entities are incomplete placeholders with minimal information
- Contain: type name, unique attributes, system-generated GUID
- Marked with `isIncomplete=true` flag
- Allow child entities to maintain references while parent data is resolved

### API Usage
```bash
# Explicit enable via query parameter
POST /api/atlas/v2/entity/bulk?createShellEntityForNonExistingReference=true

# Explicit disable via query parameter  
POST /api/atlas/v2/entity/bulk?createShellEntityForNonExistingReference=false

# Use default (now true)
POST /api/atlas/v2/entity/bulk
```

## Impact Analysis

### Before Changes
- REST API threw 404 errors when entities referenced non-existent parents
- Workflows would fail and require manual intervention
- Inconsistent behavior between REST API and notification consumer

### After Changes
- REST API creates shell entities for missing references (like notification consumer)
- Workflows complete successfully even with missing parent entities
- Consistent behavior across all data ingestion paths
- Query parameter allows fine-grained control when needed

## Testing

### Manual Testing Steps
1. Call `/api/atlas/v2/entity/bulk` with entities that reference non-existent parents
2. Verify no 404 errors are thrown
3. Verify shell entities are created with `isIncomplete=true`
4. Test query parameter override with `createShellEntityForNonExistingReference=false`

### Automated Tests
```bash
mvn test -Dtest=AuditFilterTest
```

## Rollback Plan

If needed, revert to old behavior by setting in configuration:
```properties
atlas.rest.create.shell.entity.for.non-existing.ref=false
```

Or use query parameter on a per-request basis:
```
?createShellEntityForNonExistingReference=false
```

## Files Modified
- `intg/src/main/java/org/apache/atlas/AtlasConfiguration.java` (1 line changed)
- `webapp/src/main/java/org/apache/atlas/web/filters/AuditFilter.java` (13 lines added)
- `webapp/src/test/java/org/apache/atlas/web/filters/AuditFilterTest.java` (new file, 167 lines)
- `WARE-16-SOLUTION.md` (new documentation file)

## Related Code
The changes leverage existing functionality:
- `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/UniqAttrBasedEntityResolver.java`
- `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/EntityGraphMapper.java`
- `server-api/src/main/java/org/apache/atlas/RequestContext.java`

## Next Steps
1. Deploy to CMA CGM environment
2. Monitor workflow success rates
3. Check for reduction in 404 errors
4. Monitor shell entity creation patterns
