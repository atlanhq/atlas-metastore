# Fix for Snowflake Workflow Failure (WARE-16)

## Problem Statement

CMA CGM's Snowflake workflow was failing during the publishing stage with the following errors:

1. **404 Error**: `Referenced entity AtlasObjectId{guid='null', typeName='Database', uniqueAttributes={qualifiedName:default/snowflake/1721978654/INT_CDP_CMACGM, attributes={qualifiedName:default/snowflake/1721978654/INT_CDP_CMACGM}} is not found`

2. **400 Error**: `invalid parameters: no entities to create/update.`

### Root Cause

The workflow was attempting to publish entities that referenced non-existent parent entities (e.g., Database entities) in Atlas. This mismatch typically occurs when:

- The database was deleted but child entities still reference it
- The extraction filter changed and excluded the parent entity
- There's a state mismatch between the workflow and Atlas

The REST API was configured to throw errors when encountering references to non-existent entities, unlike the notification consumer which creates "shell entities" (incomplete placeholder entities) for missing references.

## Solution

Two changes have been made to address this issue:

### 1. Enable Query Parameter Override for Shell Entity Creation

**File**: `webapp/src/main/java/org/apache/atlas/web/filters/AuditFilter.java`

Added support for a new query parameter `createShellEntityForNonExistingReference` that allows clients to override the default configuration on a per-request basis.

**Usage**:
```
POST /api/atlas/v2/entity/bulk?createShellEntityForNonExistingReference=true
```

When set to `true`, the API will automatically create shell entities for any referenced entities that don't exist, preventing 404 errors.

**Example**:
```bash
curl -X POST "http://atlas-ratelimited.atlas.svc.cluster.local:80/api/atlas/v2/entity/bulk?createShellEntityForNonExistingReference=true&replaceClassifications=false&replaceBusinessAttributes=false&overwriteBusinessAttributes=false" \
  -H "Content-Type: application/json" \
  -d @entities.json
```

### 2. Changed Default Configuration to Enable Shell Entity Creation

**File**: `intg/src/main/java/org/apache/atlas/AtlasConfiguration.java`

Changed the default value of `REST_API_CREATE_SHELL_ENTITY_FOR_NON_EXISTING_REF` from `false` to `true`.

**Rationale**: The notification consumer already uses `true` as the default for this setting. Aligning the REST API with the notification consumer ensures consistent behavior across all data ingestion paths and prevents workflow failures due to missing parent entities.

**Configuration Property**: `atlas.rest.create.shell.entity.for.non-existing.ref`

If needed, this can be overridden in the configuration file to revert to the old behavior:
```properties
atlas.rest.create.shell.entity.for.non-existing.ref=false
```

## Impact

### Positive Impacts

1. **Workflow Resilience**: Snowflake and other data source workflows will no longer fail when encountering references to non-existent entities
2. **Automatic Recovery**: Shell entities will be automatically created for missing references, allowing the workflow to complete successfully
3. **Consistency**: REST API behavior now aligns with the notification consumer behavior
4. **Backward Compatibility**: The query parameter allows fine-grained control for specific requests

### Behavior Changes

- **Before**: REST API would throw a 404 error when an entity referenced a non-existent parent entity
- **After**: REST API will create a shell entity (incomplete placeholder) for the non-existent parent entity and continue processing

### Shell Entities

A shell entity is an incomplete entity that contains:
- A system-generated GUID
- The type name
- Unique attributes (e.g., qualifiedName)
- A marker indicating it's incomplete (`isIncomplete=true`)

Shell entities allow child entities to maintain their references while the parent entity information is being resolved or re-extracted.

## Testing

### Manual Testing

1. **Test with Query Parameter**:
   ```bash
   # Test enabling shell entity creation via query parameter
   curl -X POST "http://localhost:21000/api/atlas/v2/entity/bulk?createShellEntityForNonExistingReference=true" \
     -H "Content-Type: application/json" \
     -d '{
       "entities": [
         {
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
         }
       ]
     }'
   ```

2. **Test Default Behavior**:
   ```bash
   # Test default behavior (should now create shell entities by default)
   curl -X POST "http://localhost:21000/api/atlas/v2/entity/bulk" \
     -H "Content-Type: application/json" \
     -d @entities.json
   ```

### Unit Tests

A comprehensive unit test has been added: `webapp/src/test/java/org/apache/atlas/web/filters/AuditFilterTest.java`

Run the tests with:
```bash
mvn test -Dtest=AuditFilterTest
```

## Rollout Strategy

### For Immediate Fix (CMA CGM)

Update the workflow configuration to add the query parameter to the bulk entity API calls:
```
createShellEntityForNonExistingReference=true
```

This can be done without redeploying Atlas.

### For Permanent Fix (All Environments)

Deploy the updated Atlas code with the new default configuration. This will automatically enable shell entity creation for all REST API calls.

## Monitoring

After deployment, monitor for:

1. **Reduction in 404 errors**: Check logs for `ATLAS-404-00-00A` error codes
2. **Increase in shell entities**: Query for entities with `isIncomplete=true`
3. **Workflow success rates**: Monitor Snowflake and other workflow completion rates

## Related Files

- `webapp/src/main/java/org/apache/atlas/web/filters/AuditFilter.java` - Query parameter handling
- `intg/src/main/java/org/apache/atlas/AtlasConfiguration.java` - Default configuration
- `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/UniqAttrBasedEntityResolver.java` - Shell entity creation logic
- `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/EntityGraphMapper.java` - Shell entity implementation
- `webapp/src/main/java/org/apache/atlas/web/rest/EntityREST.java` - Bulk entity endpoint

## References

- Linear Issue: WARE-16
- Error Codes: ATLAS-404-00-00A, ATLAS-400-00-01A
- Configuration: `atlas.rest.create.shell.entity.for.non-existing.ref`
- Query Parameter: `createShellEntityForNonExistingReference`
