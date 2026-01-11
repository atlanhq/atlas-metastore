# Usage Examples for Shell Entity Creation Feature

## Problem Scenario

You have a Snowflake Column that references a Table, but the Table doesn't exist in Atlas yet:

```json
{
  "entities": [
    {
      "typeName": "Column",
      "attributes": {
        "qualifiedName": "column1@snowflake",
        "name": "column1",
        "table": {
          "typeName": "Table",
          "uniqueAttributes": {
            "qualifiedName": "missing_table@snowflake"
          }
        }
      }
    }
  ]
}
```

### Before the Fix
```bash
POST /api/atlas/v2/entity/bulk

Response: 404 Not Found
{
  "errorCode": "ATLAS-404-00-00A",
  "errorMessage": "Referenced entity AtlasObjectId{typeName='Table', uniqueAttributes={qualifiedName='missing_table@snowflake'}} is not found"
}
```

### After the Fix (Default Behavior)
```bash
POST /api/atlas/v2/entity/bulk

Response: 200 OK
{
  "mutatedEntities": {
    "CREATE": [
      {
        "typeName": "Table",
        "guid": "generated-guid-123",
        "attributes": {
          "qualifiedName": "missing_table@snowflake",
          "__state": "ACTIVE",
          "__modificationTimestamp": 1234567890,
          "isIncomplete": true  # Shell entity marker
        }
      },
      {
        "typeName": "Column", 
        "guid": "generated-guid-456",
        "attributes": {
          "qualifiedName": "column1@snowflake",
          "name": "column1",
          "table": {
            "guid": "generated-guid-123"
          }
        }
      }
    ]
  }
}
```

## Usage Examples

### Example 1: Default Behavior (Recommended)
```bash
curl -X POST "http://atlas-server:21000/api/atlas/v2/entity/bulk" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'user:password' | base64)" \
  -d @entities.json
```
**Result**: Shell entities are automatically created for missing references (new default)

### Example 2: Explicitly Enable Shell Entity Creation
```bash
curl -X POST "http://atlas-server:21000/api/atlas/v2/entity/bulk?createShellEntityForNonExistingReference=true" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'user:password' | base64)" \
  -d @entities.json
```
**Result**: Same as default, but explicit

### Example 3: Disable Shell Entity Creation (Old Behavior)
```bash
curl -X POST "http://atlas-server:21000/api/atlas/v2/entity/bulk?createShellEntityForNonExistingReference=false" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'user:password' | base64)" \
  -d @entities.json
```
**Result**: 404 error if referenced entities don't exist (old behavior)

### Example 4: Combine with Other Parameters
```bash
curl -X POST "http://atlas-server:21000/api/atlas/v2/entity/bulk?createShellEntityForNonExistingReference=true&replaceClassifications=true&skipFailedEntities=true" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'user:password' | base64)" \
  -d @entities.json
```
**Result**: 
- Shell entities created for missing references
- Classifications replaced
- Failed entities skipped (continues processing)

### Example 5: Snowflake Workflow Configuration
```yaml
# In workflow configuration
api_params:
  createShellEntityForNonExistingReference: true
  replaceClassifications: false
  replaceBusinessAttributes: false
  overwriteBusinessAttributes: false
```

## Python SDK Example

```python
import requests
import json

# Prepare entities
entities = {
    "entities": [
        {
            "typeName": "Column",
            "attributes": {
                "qualifiedName": "column1@snowflake",
                "name": "column1",
                "table": {
                    "typeName": "Table",
                    "uniqueAttributes": {
                        "qualifiedName": "missing_table@snowflake"
                    }
                }
            }
        }
    ]
}

# Call API with shell entity creation enabled
response = requests.post(
    "http://atlas-server:21000/api/atlas/v2/entity/bulk",
    params={
        "createShellEntityForNonExistingReference": True
    },
    headers={
        "Content-Type": "application/json",
        "Authorization": "Basic dXNlcjpwYXNzd29yZA=="
    },
    json=entities
)

if response.status_code == 200:
    result = response.json()
    print(f"Created {len(result['mutatedEntities']['CREATE'])} entities")
    
    # Check for shell entities
    for entity in result['mutatedEntities']['CREATE']:
        if entity.get('attributes', {}).get('isIncomplete'):
            print(f"Shell entity created: {entity['typeName']} - {entity['guid']}")
else:
    print(f"Error: {response.status_code} - {response.text}")
```

## Java Client Example

```java
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;

public class AtlasClientExample {
    public static void main(String[] args) throws Exception {
        AtlasClientV2 client = new AtlasClientV2(
            new String[]{"http://atlas-server:21000"}, 
            new String[]{"user", "password"}
        );
        
        // Create column entity referencing non-existent table
        AtlasEntity column = new AtlasEntity("Column");
        column.setAttribute("qualifiedName", "column1@snowflake");
        column.setAttribute("name", "column1");
        
        AtlasObjectId tableRef = new AtlasObjectId("Table");
        tableRef.setUniqueAttributes(
            Collections.singletonMap("qualifiedName", "missing_table@snowflake")
        );
        column.setAttribute("table", tableRef);
        
        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        entities.addEntity(column);
        
        // Create with shell entity support (now default)
        EntityMutationResponse response = client.createEntities(entities);
        
        System.out.println("Created entities: " + response.getCreatedEntities().size());
        
        // Check for shell entities
        response.getCreatedEntities().forEach(header -> {
            if (Boolean.TRUE.equals(header.getAttribute("isIncomplete"))) {
                System.out.println("Shell entity: " + header.getTypeName() + " - " + header.getGuid());
            }
        });
    }
}
```

## Querying Shell Entities

### Find All Shell Entities
```bash
curl -X POST "http://atlas-server:21000/api/atlas/v2/search/basic" \
  -H "Content-Type: application/json" \
  -d '{
    "typeName": "Asset",
    "query": "isIncomplete = true"
  }'
```

### Find Shell Entities of Specific Type
```bash
curl -X POST "http://atlas-server:21000/api/atlas/v2/search/basic" \
  -H "Content-Type: application/json" \
  -d '{
    "typeName": "Table",
    "query": "isIncomplete = true"
  }'
```

## Troubleshooting

### Issue: Still Getting 404 Errors
**Solution**: 
1. Check configuration: `atlas.rest.create.shell.entity.for.non-existing.ref=true`
2. Add query parameter explicitly: `?createShellEntityForNonExistingReference=true`
3. Check logs for DEBUG messages about shell entity creation

### Issue: Too Many Shell Entities
**Solution**:
1. Review extraction filters in workflow
2. Ensure parent entities are being extracted
3. Run Current State Syncer workflow
4. Consider disabling feature for specific workflows if data quality is poor

### Issue: Performance Degradation
**Solution**:
1. Monitor shell entity creation rate
2. Consider batching entity creation
3. Review and optimize extraction order (parents before children)

## Best Practices

1. **Prefer Complete Entities**: Extract parent entities before children when possible
2. **Monitor Shell Entities**: Set up alerts for high shell entity creation rates
3. **Regular Cleanup**: Periodically check for and resolve incomplete entities
4. **Data Quality**: Shell entities may indicate data quality or extraction issues
5. **Use Defaults**: The new default (enabled) is recommended for most workflows

## Configuration Reference

### Application Properties
```properties
# Enable/disable globally (default is now true)
atlas.rest.create.shell.entity.for.non-existing.ref=true

# Notification consumer setting (already true)
atlas.notification.consumer.create.shell.entity.for.non-existing.ref=true
```

### Query Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `createShellEntityForNonExistingReference` | boolean | true | Enable/disable shell entity creation for this request |
| `skipFailedEntities` | boolean | false | Continue processing even if some entities fail |
| `replaceClassifications` | boolean | false | Replace existing classifications |
| `replaceBusinessAttributes` | boolean | false | Replace existing business attributes |

## Migration Guide

### For Existing Workflows

**No action required** - The new default enables shell entity creation automatically.

### To Opt-Out

Add to workflow configuration:
```yaml
api_params:
  createShellEntityForNonExistingReference: false
```

Or in application.properties:
```properties
atlas.rest.create.shell.entity.for.non-existing.ref=false
```
