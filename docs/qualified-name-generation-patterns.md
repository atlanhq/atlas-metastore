# Qualified Name Generation Patterns in Atlas Preprocessors

This document describes how different entity types auto-generate their `qualifiedName` attribute during entity creation and update operations in Apache Atlas.

## Overview

Many Atlas entity types automatically generate their `qualifiedName` attribute regardless of what value is provided by the user during entity creation. This ensures consistency and uniqueness across the system while maintaining hierarchical relationships.

## Entity Types with Auto-Generated Qualified Names

### 1. Glossary Entities

**Location**: `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/preprocessor/glossary/`

#### AtlasGlossary
- **Pattern**: Uses UUID
- **Code**: `entity.setAttribute(QUALIFIED_NAME, createQualifiedName())`
- **Format**: `{UUID}`
- **Update Behavior**: Preserves existing qualifiedName

#### AtlasGlossaryTerm
- **Pattern**: Uses UUID
- **Code**: `entity.setAttribute(QUALIFIED_NAME, createQualifiedName())`
- **Format**: `{UUID}`
- **Update Behavior**: Preserves existing qualifiedName

#### AtlasGlossaryCategory
- **Pattern**: Uses UUID
- **Code**: `entity.setAttribute(QUALIFIED_NAME, createQualifiedName())`
- **Format**: `{UUID}`
- **Update Behavior**: Preserves existing qualifiedName

### 2. DataMesh Entities

**Location**: `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/preprocessor/datamesh/`

#### DataDomain
- **Pattern**: Hierarchical with parent domain
- **Code**: `entity.setAttribute(QUALIFIED_NAME, createQualifiedName(parentDomainQualifiedName))`
- **Format**: `{parentDomainQualifiedName}/{UUID}` or `default/{UUID}` for root domains
- **Update Behavior**: Updates if parent domain changes

#### DataProduct
- **Pattern**: Hierarchical with parent domain
- **Code**: `entity.setAttribute(QUALIFIED_NAME, createQualifiedName(parentDomainQualifiedName))`
- **Format**: `{parentDomainQualifiedName}/product/{UUID}`
- **Update Behavior**: Updates if parent domain changes

### 3. Query Entities

**Location**: `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/preprocessor/sql/`

#### Query
- **Pattern**: Includes collection, username, and UUID
- **Code**: `String.format("%s/query/%s/%s", collectionQualifiedName, currentUserName, getUUID())`
- **Format**: `{collectionQualifiedName}/query/{username}/{UUID}`
- **Update Behavior**: Updates if parent collection changes

#### QueryFolder
- **Pattern**: Hierarchical with parent folder or collection
- **Code**: `String.format("%s/folder/%s", parentQualifiedName, getUUID())`
- **Format**: `{parentQualifiedName}/folder/{UUID}`
- **Update Behavior**: Updates if parent changes

#### QueryCollection
- **Pattern**: Includes namespace and name
- **Code**: `String.format("%s/%s", namespace, collectionName)`
- **Format**: `{namespace}/{collectionName}`
- **Update Behavior**: Preserves existing qualifiedName

### 4. Auth Policy Entities

**Location**: `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/preprocessor/`

#### AuthPolicy
- **Pattern**: Different formats for Persona vs Purpose policies
- **Persona Policy**: `String.format("%s/%s", personaQualifiedName, getUUID())`
- **Purpose Policy**: `String.format("%s/%s", purposeQualifiedName, getUUID())`
- **Format**: `{parentEntityQualifiedName}/{UUID}`
- **Update Behavior**: Preserves existing qualifiedName

### 5. Access Control Entities

**Location**: `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/preprocessor/accesscontrol/`

#### Persona
- **Pattern**: Simple UUID
- **Code**: `entity.setAttribute(QUALIFIED_NAME, getUUID())`
- **Format**: `{UUID}`
- **Update Behavior**: Preserves existing qualifiedName

#### Purpose
- **Pattern**: Simple UUID
- **Code**: `entity.setAttribute(QUALIFIED_NAME, getUUID())`
- **Format**: `{UUID}`
- **Update Behavior**: Preserves existing qualifiedName

#### Stakeholder
- **Pattern**: Includes domain context
- **Code**: `String.format("default/%s/%s", getUUID(), domainQualifiedName)`
- **Format**: `default/{UUID}/{domainQualifiedName}`
- **Update Behavior**: Can update based on domain changes

### 6. Contract Entities

**Location**: `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/preprocessor/contract/`

#### DataContract
- **Pattern**: Versioned with contract name
- **Code**: `String.format("%s/V%s", contractQName, newVersionNumber)`
- **Format**: `{contractName}/V{version}`
- **Update Behavior**: Creates new qualifiedName with incremented version

### 7. Resource Entities

**Location**: `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/preprocessor/resource/`

#### Link
- **Pattern**: Simple UUID
- **Code**: `entity.setAttribute(QUALIFIED_NAME, getUUID())`
- **Format**: `{UUID}`
- **Update Behavior**: Preserves existing qualifiedName

#### Readme
- **Pattern**: Simple UUID
- **Code**: `entity.setAttribute(QUALIFIED_NAME, getUUID())`
- **Format**: `{UUID}`
- **Update Behavior**: Preserves existing qualifiedName

### 8. Connection Entity

**Location**: `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/preprocessor/`

#### Connection
- **Pattern**: Uses name attribute
- **Code**: `entity.setAttribute(QUALIFIED_NAME, entity.getAttribute(NAME))`
- **Format**: `{name}`
- **Update Behavior**: Updates if name changes

## Key Patterns Summary

1. **UUID-based**: Most simple entities (Glossary, Persona, Purpose, Link, Readme)
2. **Hierarchical**: Entities with parent relationships (DataDomain, DataProduct, Query, QueryFolder)
3. **Versioned**: Contract entities include version numbers
4. **User-scoped**: Query entities include the current username
5. **Domain-scoped**: Stakeholder entities include domain qualified name
6. **Name-based**: Connection entities use their name attribute

## Implementation Details

All preprocessors extend from `PreProcessor` base class and implement the `processAttributes` method where qualified name generation occurs. The general pattern is:

```java
@Override
public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context, 
                            EntityMutations.EntityOperation operation) throws AtlasBaseException {
    if (operation == EntityMutations.EntityOperation.CREATE) {
        AtlasEntity entity = (AtlasEntity) entityStruct;
        entity.setAttribute(QUALIFIED_NAME, createQualifiedName(...));
    }
}
```

## Important Notes

1. User-provided qualifiedName values are ignored during entity creation for these types
2. Some entities update their qualifiedName during updates (e.g., when parent relationships change)
3. The qualifiedName serves as a unique identifier and maintains hierarchical relationships
4. Contract entities create new qualifiedNames with version increments rather than updates

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>