# Entity Diff Calculation Documentation

This document explains how Apache Atlas calculates differences between entities during bulk operations, optimizing performance and enabling differential auditing.

## Overview

During bulk entity operations, Atlas uses the `AtlasEntityComparator` to determine what has changed between the incoming entity and the existing stored entity. This diff calculation is crucial for:
- Skipping unchanged entities to improve performance
- Enabling differential audits (only tracking changes)
- Optimizing database operations
- Providing accurate change tracking

## Core Components

### AtlasEntityComparator

**Location**: `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/AtlasEntityComparator.java`

The main class responsible for entity comparison:

```java
public class AtlasEntityComparator {
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private final Map<String, String> guidAssignments;
    private final BulkRequestContext bulkRequestContext;
    
    public AtlasEntityDiffResult getDiffResult(AtlasEntity updatedEntity, 
                                              AtlasEntity storedEntity,
                                              boolean findOnlyFirstDiff) {
        // Main comparison logic
    }
}
```

### AtlasEntityDiffResult

The result object containing diff information:

```java
public class AtlasEntityDiffResult {
    private AtlasEntity diffEntity;                        // Entity with only changed attributes
    private boolean hasDifference = false;                 // Any differences found
    private boolean hasDifferenceOnlyInCustomAttributes;   // Only custom attributes changed
    private boolean hasDifferenceOnlyInBusinessAttributes; // Only business attributes changed
}
```

## Diff Calculation Flow

### 1. Entry Point

The diff calculation is triggered in `AtlasEntityStoreV2` during bulk operations:

```java
// In AtlasEntityStoreV2.createOrUpdate()
for (AtlasEntity entity : context.getUpdatedEntities()) {
    AtlasVertex vertex = context.getVertex(entity.getGuid());
    AtlasEntity storedEntity = entityRetriever.toAtlasEntityWithExtInfo(vertex).getEntity();
    
    AtlasEntityComparator entityComparator = new AtlasEntityComparator(
        typeRegistry, entityRetriever, 
        context.getGuidAssignments(), bulkRequestContext
    );
    
    AtlasEntityDiffResult diffResult = entityComparator.getDiffResult(
        entity, storedEntity, vertex, !storeDifferentialAudits
    );
    
    if (!diffResult.hasDifference()) {
        entitiesToSkipUpdate.add(entity);
        continue;
    }
}
```

### 2. Main Comparison Method

```java
private AtlasEntityDiffResult getDiffResult(AtlasEntity updatedEntity, 
                                           AtlasEntity storedEntity,
                                           AtlasVertex storedVertex,
                                           boolean findOnlyFirstDiff) {
    AtlasEntityDiffResult ret = new AtlasEntityDiffResult();
    AtlasEntity diffEntity = new AtlasEntity();
    boolean hasDiff = false;
    int sectionsWithDiff = 0;
    
    // Set basic properties
    diffEntity.setTypeName(updatedEntity.getTypeName());
    diffEntity.setGuid(updatedEntity.getGuid());
    
    // Compare sections
    if (diffAttributes(updatedEntity, storedEntity, diffEntity, findOnlyFirstDiff)) {
        hasDiff = true;
        sectionsWithDiff++;
    }
    
    if (!hasDiff || !findOnlyFirstDiff) {
        if (diffRelationshipAttributes(...)) {
            hasDiff = true;
            sectionsWithDiff++;
        }
    }
    
    // Continue with other sections...
    
    ret.hasDifference = hasDiff;
    ret.diffEntity = diffEntity;
    return ret;
}
```

## Comparison Sections

### 1. Regular Attributes

Compares all entity attributes defined in the type system:

```java
private boolean diffAttributes(AtlasEntity updatedEntity, 
                             AtlasEntity storedEntity,
                             AtlasEntity diffEntity,
                             boolean findOnlyFirstDiff) {
    boolean ret = false;
    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(updatedEntity.getTypeName());
    
    for (AtlasAttribute attribute : entityType.getAllAttributes().values()) {
        String attrName = attribute.getName();
        Object updatedValue = updatedEntity.getAttribute(attrName);
        Object storedValue = storedEntity.getAttribute(attrName);
        
        // Handle default values
        if (updatedValue == null && storedValue == null) {
            continue;
        }
        
        if (updatedValue == null && attribute.getAttributeDef().getIsDefaultValueNull() == false) {
            updatedValue = getDefaultValue(attribute);
        }
        
        // Type-specific comparison
        AtlasType attrType = attribute.getAttributeType();
        if (!attrType.areEqualValues(updatedValue, storedValue, guidAssignments)) {
            diffEntity.setAttribute(attrName, updatedValue);
            ret = true;
            
            if (findOnlyFirstDiff) {
                return ret;
            }
        }
    }
    
    return ret;
}
```

### 2. Relationship Attributes

Handles entity relationships separately:

```java
private boolean diffRelationshipAttributes(AtlasEntity updatedEntity,
                                         AtlasEntity storedEntity,
                                         AtlasEntity diffEntity,
                                         boolean findOnlyFirstDiff) {
    boolean ret = false;
    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(updatedEntity.getTypeName());
    
    for (AtlasAttribute attribute : entityType.getRelationshipAttributes().values()) {
        String attrName = attribute.getName();
        Object updatedValue = getRelationshipAttributeValue(updatedEntity, attrName);
        Object storedValue = getRelationshipAttributeValue(storedEntity, attrName);
        
        if (!attribute.getAttributeType().areEqualValues(updatedValue, storedValue, guidAssignments)) {
            diffEntity.setRelationshipAttribute(attrName, updatedValue);
            ret = true;
            
            if (findOnlyFirstDiff) {
                return ret;
            }
        }
    }
    
    return ret;
}
```

### 3. Classifications (Tags)

Three modes of classification comparison:

```java
private boolean diffClassifications(AtlasEntity updatedEntity,
                                  AtlasEntity storedEntity,
                                  AtlasEntity diffEntity) {
    List<AtlasClassification> updatedClassifications = updatedEntity.getClassifications();
    List<AtlasClassification> storedClassifications;
    
    // Optimize with JanusGraph if enabled
    if (ENABLE_JANUS_OPTIMISATION && storedVertex != null) {
        storedClassifications = entityRetriever.getAllClassifications(storedVertex);
    } else {
        storedClassifications = storedEntity.getClassifications();
    }
    
    if (bulkRequestContext.isReplaceClassifications()) {
        // Full replacement mode
        diffEntity.setClassifications(updatedClassifications);
        return !Objects.equals(updatedClassifications, storedClassifications);
    } else if (bulkRequestContext.isReplaceTags() || bulkRequestContext.isAppendTags()) {
        // Diff-based mode
        List<AtlasClassification> diffClassifications;
        
        if (bulkRequestContext.isReplaceTags()) {
            diffClassifications = AtlasEntityUtils.getTagsDiffForReplace(
                storedClassifications, updatedClassifications
            );
        } else {
            diffClassifications = AtlasEntityUtils.getTagsDiffForAppend(
                storedClassifications, updatedClassifications
            );
        }
        
        if (CollectionUtils.isNotEmpty(diffClassifications)) {
            diffEntity.setClassifications(diffClassifications);
            return true;
        }
    }
    
    return false;
}
```

### 4. Custom Attributes

Simple map comparison for custom attributes:

```java
private boolean diffCustomAttributes(AtlasEntity updatedEntity,
                                   AtlasEntity storedEntity,
                                   AtlasEntity diffEntity) {
    Map<String, String> updatedCustom = updatedEntity.getCustomAttributes();
    Map<String, String> storedCustom = storedEntity.getCustomAttributes();
    
    // Only process if updated entity has custom attributes
    // This prevents accidental removal
    if (MapUtils.isNotEmpty(updatedCustom)) {
        if (!Objects.equals(updatedCustom, storedCustom)) {
            diffEntity.setCustomAttributes(updatedCustom);
            return true;
        }
    }
    
    return false;
}
```

### 5. Business Attributes

Nested map comparison with replacement options:

```java
private boolean diffBusinessAttributes(AtlasEntity updatedEntity,
                                     AtlasEntity storedEntity,
                                     AtlasEntity diffEntity) {
    Map<String, Map<String, Object>> updatedBusiness = updatedEntity.getBusinessAttributes();
    Map<String, Map<String, Object>> storedBusiness = storedEntity.getBusinessAttributes();
    
    // Extract from attributes if not explicitly set
    if (MapUtils.isEmpty(updatedBusiness)) {
        updatedBusiness = getBusinessAttributesFromAttributes(updatedEntity);
    }
    
    if (bulkRequestContext.isReplaceBusinessAttributes()) {
        // Full replacement
        diffEntity.setBusinessAttributes(updatedBusiness);
        return !Objects.equals(updatedBusiness, storedBusiness);
    } else {
        // Merge mode - only include changed business attributes
        Map<String, Map<String, Object>> diffBusiness = new HashMap<>();
        
        for (Map.Entry<String, Map<String, Object>> entry : updatedBusiness.entrySet()) {
            String bmName = entry.getKey();
            Map<String, Object> updatedAttrs = entry.getValue();
            Map<String, Object> storedAttrs = storedBusiness.get(bmName);
            
            if (!Objects.equals(updatedAttrs, storedAttrs)) {
                diffBusiness.put(bmName, updatedAttrs);
            }
        }
        
        if (MapUtils.isNotEmpty(diffBusiness)) {
            diffEntity.setBusinessAttributes(diffBusiness);
            return true;
        }
    }
    
    return false;
}
```

## Type-Specific Value Comparison

### AtlasObjectId Comparison

For entity references, special handling for GUID assignments:

```java
// In AtlasObjectIdType.areEqualValues()
@Override
public boolean areEqualValues(Object val1, Object val2, Map<String, String> guidAssignments) {
    AtlasObjectId objId1 = (AtlasObjectId) val1;
    AtlasObjectId objId2 = (AtlasObjectId) val2;
    
    if (objId1 == objId2) {
        return true;
    }
    
    boolean isAssignedGuid1 = AtlasTypeUtil.isAssignedGuid(objId1.getGuid());
    boolean isAssignedGuid2 = AtlasTypeUtil.isAssignedGuid(objId2.getGuid());
    
    if (isAssignedGuid1 == isAssignedGuid2) {
        // Both assigned or both unassigned - compare GUIDs
        return StringUtils.equals(objId1.getGuid(), getMappedGuid(objId2.getGuid(), guidAssignments));
    } else {
        // Mixed - compare by type and unique attributes
        return StringUtils.equals(objId1.getTypeName(), objId2.getTypeName()) &&
               Objects.equals(objId1.getUniqueAttributes(), objId2.getUniqueAttributes());
    }
}
```

### Default Value Handling

```java
private Object getDefaultValue(AtlasAttribute attribute) {
    AtlasType attrType = attribute.getAttributeType();
    
    switch (attrType.getTypeCategory()) {
        case PRIMITIVE:
            return attrType.createDefaultValue();
            
        case ARRAY:
            return new ArrayList<>();
            
        case MAP:
            return new HashMap<>();
            
        default:
            return null;
    }
}
```

## Performance Optimizations

### 1. Early Exit Strategy

The `findOnlyFirstDiff` parameter allows stopping at the first difference:

```java
// When only checking if entity has changed (not collecting all diffs)
AtlasEntityDiffResult diffResult = entityComparator.getDiffResult(
    entity, storedEntity, vertex, true  // findOnlyFirstDiff = true
);

if (!diffResult.hasDifference()) {
    // Skip this entity
}
```

### 2. Section Tracking

Tracks which sections have differences to optimize single-section changes:

```java
int sectionsWithDiff = 0;

if (diffAttributes(...)) sectionsWithDiff++;
if (diffRelationshipAttributes(...)) sectionsWithDiff++;
if (diffClassifications(...)) sectionsWithDiff++;
// etc.

// Optimize for single-section changes
if (sectionsWithDiff == 1) {
    if (onlyCustomAttributesChanged) {
        ret.hasDifferenceOnlyInCustomAttributes = true;
    } else if (onlyBusinessAttributesChanged) {
        ret.hasDifferenceOnlyInBusinessAttributes = true;
    }
}
```

### 3. JanusGraph Optimization

Feature flag enables direct classification retrieval:

```java
if (ENABLE_JANUS_OPTIMISATION && storedVertex != null) {
    // Retrieve classifications directly from vertex
    storedClassifications = entityRetriever.getAllClassifications(storedVertex);
} else {
    // Use entity object
    storedClassifications = storedEntity.getClassifications();
}
```

## Usage in Bulk Operations

### Skip Unchanged Entities

```java
List<AtlasEntity> entitiesToSkipUpdate = new ArrayList<>();

for (AtlasEntity entity : context.getUpdatedEntities()) {
    AtlasEntityDiffResult diffResult = entityComparator.getDiffResult(...);
    
    if (!diffResult.hasDifference()) {
        entitiesToSkipUpdate.add(entity);
        RequestContext.get().recordEntityToSkip(entity.getGuid());
        continue;
    }
    
    // Process changed entity
}

// Remove skipped entities from update context
context.getUpdatedEntities().removeAll(entitiesToSkipUpdate);
```

### Differential Auditing

```java
if (diffResult.hasDifference() && storeDifferentialAudits) {
    // Store only the diff for audit
    AtlasEntity diffEntity = diffResult.getDiffEntity();
    diffEntity.setGuid(entity.getGuid());
    RequestContext.get().cacheDifferentialEntity(diffEntity, storedVertex);
}
```

### Metrics Tracking

```java
// Track custom/business attribute updates
if (diffResult.hasDifferenceOnlyInCustomAttributes()) {
    RequestContext.get().addEntityGuidWithCustomAttributeUpdate(entity.getGuid());
}

if (diffResult.hasDifferenceOnlyInBusinessAttributes()) {
    RequestContext.get().addEntityGuidWithBusinessAttributeUpdate(entity.getGuid());
}
```

## Configuration Options

### BulkRequestContext Settings

```java
public class BulkRequestContext {
    private boolean replaceClassifications;     // Replace all classifications
    private boolean replaceTags;               // Replace tags with diff
    private boolean appendTags;                // Append tags with diff
    private boolean replaceBusinessAttributes; // Replace business attributes
    private boolean overwriteBusinessAttributes; // Overwrite mode
}
```

### System Configuration

```properties
# Enable differential auditing
atlas.entity.audit.differential=true

# Enable JanusGraph optimizations
atlas.feature.janusgraph.optimisation=true
```

## Best Practices

1. **Use Differential Audits**: Enable `atlas.entity.audit.differential` to reduce audit storage
2. **Batch Similar Operations**: Group entities with similar update patterns
3. **Monitor Skip Metrics**: Track how many entities are skipped to optimize bulk operations
4. **Choose Appropriate Modes**: Use replace vs append modes based on use case
5. **Profile Performance**: Use metrics to identify bottlenecks in diff calculation

## Summary

The entity diff calculation in Atlas is a sophisticated mechanism that:
- Compares all aspects of entities comprehensively
- Optimizes bulk operations by skipping unchanged entities
- Supports various update modes (replace, append, merge)
- Enables differential auditing for storage efficiency
- Provides performance optimizations through early exit and caching
- Handles complex scenarios like GUID assignments and relationship updates

This diff mechanism is fundamental to Atlas's ability to handle large-scale metadata operations efficiently while maintaining data integrity and providing detailed change tracking.