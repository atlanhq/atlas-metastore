# Atlas Metastore - Custom Name Feature Implementation

## Overview
Implementation of `allowCustomName` query parameter for ClassificationDef and BusinessMetadataDef type creation, allowing users to provide custom names instead of auto-generated random names.

## Requirements
- Add `allowCustomName` query parameter to typedef endpoints
- When `allowCustomName=true`: Use user-provided names, throw error if names are missing
- When `allowCustomName=false` (default): Generate random names for backward compatibility
- Maintain complete backward compatibility

## Implementation Details

### 1. Model Changes
Modified constructors in both `AtlasBusinessMetadataDef` and `AtlasClassificationDef`:
- Changed from `generateRandomName()` to `null` in constructors
- This ensures names come as null from JSON deserialization

**Files Modified:**
- `/intg/src/main/java/org/apache/atlas/model/typedef/AtlasBusinessMetadataDef.java`
- `/intg/src/main/java/org/apache/atlas/model/typedef/AtlasClassificationDef.java`

### 2. REST API Changes
Updated `TypesREST.java` to handle all name generation logic:

**Files Modified:**
- `/webapp/src/main/java/org/apache/atlas/web/rest/TypesREST.java`

#### Create Operation (`createAtlasTypeDefs`):
- Added `@QueryParam("allowCustomName") @DefaultValue("false") boolean allowCustomName`
- When `allowCustomName=true`:
  - Validates that names are provided for types and attributes
  - Throws `AtlasBaseException` with `MISSING_MANDATORY_ATTRIBUTE` if any name is missing
  - Uses user-provided names directly
- When `allowCustomName=false`:
  - Always generates random names for types
  - Always generates random names for all attributes

#### Update Operation (`updateAtlasTypeDefs`):
- Added same query parameter
- When `allowCustomName=true`:
  - Validates names for new types and attributes
  - Existing types/attributes keep their names
- When `allowCustomName=false`:
  - Generates random names for new types only
  - Generates random names for new attributes only
  - Existing types/attributes keep their names

### 3. Helper Methods
Added `isExistingAttribute()` method to check if an attribute already exists in a type definition.

## API Usage Examples

### With Custom Names (allowCustomName=true)
```bash
POST /api/atlas/v2/types/typedefs?allowCustomName=true
{
  "businessMetadataDefs": [{
    "name": "myCustomBusinessMetadata",
    "displayName": "My Business Metadata",
    "attributeDefs": [{
      "name": "myAttribute",
      "typeName": "string"
    }]
  }]
}
```

### With Auto-generated Names (default)
```bash
POST /api/atlas/v2/types/typedefs
{
  "businessMetadataDefs": [{
    "displayName": "My Business Metadata",
    "attributeDefs": [{
      "typeName": "string"
    }]
  }]
}
```

## Key Design Decisions

1. **Query Parameter vs Request Body**: Used query parameter for cleaner API design and to avoid modifying the type definition schema

2. **Centralized Logic**: All name generation/validation logic is in TypesREST, keeping model classes simple

3. **Backward Compatibility**: Default behavior (allowCustomName=false) maintains exact same behavior as before

4. **Error Handling**: Clear error messages when custom names are required but not provided

## Testing Considerations

1. **Backward Compatibility**: Ensure existing API calls without the parameter work as before
2. **Validation**: Test that missing names throw appropriate errors when allowCustomName=true
3. **Update Operations**: Verify that only new attributes get random names during updates
4. **Mixed Usage**: Test that some types can have custom names while others have auto-generated names in the same request

## Future Enhancements

1. Consider adding validation for name formats/patterns
2. Add support for custom name prefixes/suffixes
3. Consider extending to other type definitions (EntityDef, StructDef, etc.)