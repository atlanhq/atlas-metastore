# Integration Module Documentation

This document provides detailed guidance for working with the Atlas intg (integration) module.

## Module Overview

The `intg` module serves as the **integration and contract layer** for Apache Atlas. It defines all core data models, interfaces, and type definitions that other Atlas components depend on. This module acts as the shared API between different Atlas modules and external systems.

## Package Structure

```
intg/src/main/java/org/apache/atlas/
├── model/                    # Core data models
│   ├── instance/            # Entity instances
│   ├── typedef/             # Type definitions
│   ├── discovery/           # Search models
│   ├── glossary/            # Business glossary
│   ├── impexp/              # Import/export
│   ├── notification/        # Event models
│   ├── audit/               # Audit models
│   ├── metrics/             # Metrics
│   └── tasks/               # Task management
├── type/                     # Type system
├── exception/                # Exception hierarchy
├── security/                 # Security interfaces
├── utils/                    # Common utilities
└── v1/                      # Legacy V1 API support
```

## Type System

### Type Definition Hierarchy

```
AtlasBaseTypeDef (abstract base)
├── AtlasEnumDef              # Enumeration types
├── AtlasStructDef            # Structured types
│   ├── AtlasEntityDef        # Entity types
│   ├── AtlasClassificationDef # Classification types
│   └── AtlasRelationshipDef  # Relationship types
└── AtlasBusinessMetadataDef  # Business metadata
```

### Core Type Definitions

#### AtlasEntityDef
Defines entity types (e.g., hive_table, hdfs_path):
```java
public class AtlasEntityDef extends AtlasStructDef {
    private Set<String> superTypes;        // Inheritance
    private Set<String> subTypes;          // Derived types
    private List<AtlasRelationshipAttributeDef> relationshipAttributeDefs;
    private Map<String, Object> options;   // Extensibility
}
```

#### AtlasClassificationDef
Defines classification/tag types:
```java
public class AtlasClassificationDef extends AtlasStructDef {
    private Set<String> entityTypes;       // Applicable entity types
    private Set<String> subTypes;          // Derived classifications
}
```

#### AtlasRelationshipDef
Defines relationships between entities:
```java
public class AtlasRelationshipDef extends AtlasStructDef {
    private RelationshipCategory relationshipCategory;  // ASSOCIATION, AGGREGATION, COMPOSITION
    private AtlasRelationshipEndDef endDef1;
    private AtlasRelationshipEndDef endDef2;
    private PropagateTags propagateTags;              // NONE, ONE_TO_TWO, TWO_TO_ONE, BOTH
}
```

### Attribute Definition

```java
public class AtlasAttributeDef {
    private String name;
    private String typeName;
    private boolean isOptional;
    private boolean isUnique;
    private boolean isIndexable;
    private String defaultValue;
    private Cardinality cardinality;  // SINGLE, LIST, SET
    private List<AtlasConstraintDef> constraints;
}
```

## Entity Models

### Core Entity Classes

#### AtlasEntity
The primary entity representation:
```java
public class AtlasEntity extends AtlasStruct {
    private String guid;
    private String homeId;
    private Status status;                    // ACTIVE, DELETED, PURGED
    private String provenanceType;
    private String proxy;
    private Map<String, Object> relationshipAttributes;
    private List<AtlasClassification> classifications;
    private List<AtlasTermAssignmentHeader> meanings;
    private Map<String, Map<String, Object>> businessAttributes;
    private Set<String> labels;
    private Boolean isIncomplete;
}
```

#### AtlasClassification
Classification/tag instance:
```java
public class AtlasClassification extends AtlasStruct {
    private String entityGuid;
    private Status entityStatus;
    private Boolean propagate;
    private List<AtlasTermAssignmentHeader> meanings;
    private Boolean removePropagationsOnEntityDelete;
}
```

#### AtlasRelationship
Relationship between entities:
```java
public class AtlasRelationship extends AtlasStruct {
    private String guid;
    private Status status;
    private AtlasObjectId end1;
    private AtlasObjectId end2;
    private String label;
    private PropagateTags propagateTags;
}
```

### Entity Operations Response

```java
public class EntityMutationResponse {
    private Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities;
    private List<AtlasEntityHeader> partialUpdatedEntities;
    private String guidAssignments;
}
```

## Search and Discovery

### SearchParameters
Comprehensive search request:
```java
public class SearchParameters {
    private String query;                    // DSL or full-text query
    private String typeName;
    private String classification;
    private List<SearchParameters.FilterCriteria> entityFilters;
    private List<SearchParameters.FilterCriteria> tagFilters;
    private Set<String> attributes;          // Attributes to return
    private int limit;
    private int offset;
    private String sortBy;
    private SortOrder sortOrder;
}
```

### FilterCriteria
Complex filter expressions:
```java
public class FilterCriteria {
    private String attributeName;
    private Operator operator;              // EQ, NEQ, LT, GT, CONTAINS, etc.
    private String attributeValue;
    private FilterCriteria.Condition condition;  // AND, OR
    private List<FilterCriteria> criterion;     // Nested criteria
}
```

## Notification System

### Hook Notifications (External to Atlas)
```java
public abstract class HookNotification {
    public enum HookNotificationType {
        TYPE_CREATE, TYPE_UPDATE,
        ENTITY_CREATE, ENTITY_PARTIAL_UPDATE, 
        ENTITY_FULL_UPDATE, ENTITY_DELETE,
        ENTITY_CREATE_V2, ENTITY_PARTIAL_UPDATE_V2,
        ENTITY_FULL_UPDATE_V2, ENTITY_DELETE_V2
    }
}
```

### Entity Notifications (Atlas Internal)
```java
public class EntityNotification {
    private EntityNotificationV2 entity;
    private OperationType operationType;
    private List<struct> traits;
}
```

## Glossary Models

### AtlasGlossary
Business glossary container:
```java
public class AtlasGlossary extends AtlasBaseModelObject {
    private String qualifiedName;
    private String shortDescription;
    private String longDescription;
    private String language;
    private String usage;
    private Set<AtlasRelatedTermHeader> terms;
    private Set<AtlasRelatedCategoryHeader> categories;
}
```

### AtlasGlossaryTerm
Business term with rich relationships:
```java
public class AtlasGlossaryTerm extends AtlasBaseModelObject {
    private String qualifiedName;
    private String shortDescription;
    private String longDescription;
    private Set<AtlasRelatedTermHeader> seeAlso;
    private Set<AtlasRelatedTermHeader> synonyms;
    private Set<AtlasRelatedTermHeader> antonyms;
    private Set<AtlasRelatedTermHeader> preferredTerms;
    private Set<AtlasRelatedTermHeader> preferredToTerms;
    private Set<AtlasRelatedTermHeader> replacementTerms;
    private Set<AtlasRelatedTermHeader> replacedBy;
    private Set<AtlasRelatedTermHeader> translationTerms;
    private Set<AtlasRelatedTermHeader> translatedTerms;
    private Set<AtlasRelatedTermHeader> isA;
    private Set<AtlasRelatedTermHeader> classifies;
    private Set<AtlasRelatedObjectId> assignedEntities;
}
```

## Exception Handling

### AtlasErrorCode
Comprehensive error codes with HTTP status mapping:
```java
public enum AtlasErrorCode {
    // 400 Bad Request errors
    TYPE_NAME_NOT_FOUND(400, "ATLAS-400-00-002", "Type {0} does not exist"),
    EMPTY_RESULTS(400, "ATLAS-400-00-004", "No result found for {0}"),
    
    // 403 Forbidden errors
    UNAUTHORIZED_ACCESS(403, "ATLAS-403-00-001", "{0} is not authorized to perform {1}"),
    
    // 404 Not Found errors
    INSTANCE_GUID_NOT_FOUND(404, "ATLAS-404-00-005", "Entity with guid {0} not found"),
    
    // 500 Internal Server errors
    INTERNAL_ERROR(500, "ATLAS-500-00-001", "Internal server error");
}
```

### AtlasBaseException
Base exception class:
```java
public class AtlasBaseException extends Exception {
    private AtlasErrorCode atlasErrorCode;
    
    public AtlasBaseException(AtlasErrorCode errorCode, String... params) {
        super(errorCode.getFormattedErrorMessage(params));
        this.atlasErrorCode = errorCode;
    }
}
```

## Type Registry

### AtlasTypeRegistry
Central registry for all types:
```java
public class AtlasTypeRegistry {
    // Type retrieval
    AtlasType getType(String typeName);
    AtlasEntityType getEntityTypeByName(String name);
    AtlasClassificationType getClassificationTypeByName(String name);
    
    // Type registration
    void addTypes(AtlasTypesDef typesDef);
    void updateTypes(AtlasTypesDef typesDef);
    
    // Type queries
    Collection<String> getAllTypeNames();
    Collection<AtlasEntityType> getAllEntityTypes();
}
```

## Utils and Constants

### AtlasJson
JSON serialization with custom handlers:
```java
public class AtlasJson {
    // Custom serialization for Atlas types
    public static <T> T fromJson(String json, Class<T> type);
    public static String toJson(Object obj);
    public static String toV1Json(Object obj);  // V1 compatibility
}
```

### AtlasConstants
Common constants:
```java
public final class AtlasConstants {
    public static final String QUALIFIED_NAME = "qualifiedName";
    public static final String GUID = "guid";
    public static final String TYPE_NAME_PROPERTY_KEY = "__typeName";
    public static final String STATE_PROPERTY_KEY = "__state";
}
```

## Best Practices

### Working with Types
1. Always use AtlasTypeRegistry to resolve types
2. Check type existence before creating entities
3. Use type inheritance for common attributes
4. Version types when making changes

### Entity Management
1. Use AtlasEntity for full entity representation
2. Use AtlasEntityHeader for lightweight operations
3. Always set required attributes (qualifiedName)
4. Handle entity lifecycle states properly

### Search Operations
1. Use SearchParameters for complex queries
2. Leverage FilterCriteria for precise filtering
3. Use pagination for large result sets
4. Request only needed attributes

### Error Handling
1. Use appropriate AtlasErrorCode
2. Include meaningful error parameters
3. Handle AtlasBaseException properly
4. Log errors with context

## Module Integration Points

The intg module provides contracts for:
- **Repository Module**: Entity storage interfaces
- **WebApp Module**: REST API models
- **Notification Module**: Event models
- **Client Module**: Java client interfaces
- **Common Module**: Shared utilities

## Testing Patterns

### Type Definition Testing
```java
AtlasEntityDef entityDef = new AtlasEntityDef();
entityDef.setName("test_entity");
entityDef.setServiceType("test");
entityDef.setAttributeDefs(Arrays.asList(nameAttribute, ownerAttribute));
```

### Entity Testing
```java
AtlasEntity entity = new AtlasEntity("test_entity");
entity.setAttribute("name", "test");
entity.setAttribute("owner", "user1");
entity.setClassifications(Arrays.asList(piiClassification));
```

### Search Testing
```java
SearchParameters params = new SearchParameters();
params.setTypeName("hive_table");
params.setQuery("name:test*");
params.setLimit(10);
```