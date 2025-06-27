# Atlas Preprocessors Documentation

This document provides detailed information about all preprocessors in Apache Atlas and their functionality.

## Overview

Preprocessors in Atlas intercept entity operations (CREATE, UPDATE, DELETE) before persistence, performing validations, enrichments, and business logic enforcement. They implement the `PreProcessor` interface and are registered based on entity type.

## Base Interface

```java
public interface PreProcessor {
    void processAttributes(AtlasStruct entity, EntityMutationContext context, 
                          EntityMutations.EntityOperation operation) throws AtlasBaseException;
    
    default void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        // Override for delete operations
    }
}
```

## Preprocessor Registration

Located in `AtlasEntityStoreV2.getPreProcessor()`:
```java
private PreProcessor getPreProcessor(String typeName) {
    PreProcessor preProcessor = null;
    switch (typeName) {
        case "AtlasGlossary":
            preProcessor = new GlossaryPreProcessor(typeRegistry, entityRetriever, graph, taskManagement);
            break;
        case "AtlasGlossaryTerm":
            preProcessor = new TermPreProcessor(typeRegistry, entityRetriever, graph);
            break;
        // ... other preprocessors
    }
    
    // AssetPreProcessor is applied to all entities
    if (preProcessor != null) {
        return new ChainedPreProcessor(assetPreProcessor, preProcessor);
    }
    return assetPreProcessor;
}
```

## 1. Asset Preprocessor (Global)

**Class**: `AssetPreProcessor`  
**Applies to**: All entity types  
**Purpose**: Handles domain linking for all assets

### Key Functionality

#### Domain Link Validation
```java
private void validateDomainLink(AtlasEntity entity) {
    // Assets can only be linked to one domain
    Collection<AtlasObjectId> domainLinks = (Collection<AtlasObjectId>) entity.getAttribute(DOMAIN_ATTRIBUTES);
    if (CollectionUtils.isNotEmpty(domainLinks) && domainLinks.size() > 1) {
        throw new AtlasBaseException(BAD_REQUEST, "Asset can only be linked to one domain");
    }
}
```

#### Authorization Check
```java
private void verifyAssetDomainAccess(AtlasEntity entity) {
    Set<String> resources = new HashSet<>();
    resources.add("entity-type:" + entity.getTypeName());
    
    AtlasEntityHeader entityHeader = new AtlasEntityHeader(entity.getTypeName(), entity.getAttributes());
    resources.add("entity:" + entityHeader.getGuid());
    
    verifyAccess(new AtlasEntityAccessRequest(typeRegistry, UPDATE_ENTITY, entity, entityHeader), resources);
}
```

### Excluded Types
- Glossary entities (`AtlasGlossary`, `AtlasGlossaryTerm`, `AtlasGlossaryCategory`)
- Data Products and Data Domains
- Base process entities

## 2. Glossary Preprocessors

### 2.1 GlossaryPreProcessor

**Class**: `GlossaryPreProcessor`  
**Entity Type**: `AtlasGlossary`  
**Purpose**: Manages glossary lifecycle

#### Key Operations

**CREATE**:
```java
private void processCreateGlossary(AtlasEntity entity) {
    // Generate qualified name
    String glossaryName = (String) entity.getAttribute(NAME);
    entity.setAttribute(QUALIFIED_NAME, createQualifiedName());
    
    // Check for duplicates
    boolean exists = AtlasGraphUtilsV2.glossaryExists(graph, glossaryName);
    if (exists) {
        throw new AtlasBaseException(GLOSSARY_ALREADY_EXISTS, glossaryName);
    }
    
    // Set lexicographical order
    String lexicographicalValue = LexoRank.genNextLexoRank(topLexicographicalValue, null);
    entity.setAttribute(LEXICOGRAPHICAL_SORT_ORDER, lexicographicalValue);
}
```

**UPDATE**:
- Validates name changes
- Updates qualified names if needed
- Maintains lexicographical order

### 2.2 TermPreProcessor

**Class**: `TermPreProcessor`  
**Entity Type**: `AtlasGlossaryTerm`  
**Purpose**: Manages glossary terms

#### Key Operations

**CREATE**:
```java
private void processCreateTerm(AtlasEntity entity) {
    // Validate glossary anchor
    AtlasObjectId glossaryId = (AtlasObjectId) entity.getAttribute(ANCHOR);
    if (glossaryId == null) {
        throw new AtlasBaseException(MISSING_GLOSSARY_ID);
    }
    
    // Check term uniqueness in glossary
    String termName = (String) entity.getAttribute(NAME);
    if (isNameInvalid(termName)) {
        throw new AtlasBaseException(INVALID_DISPLAY_NAME);
    }
    
    // Set qualified name
    String qualifiedName = glossaryQualifiedName + "@" + termName;
    entity.setAttribute(QUALIFIED_NAME, qualifiedName);
    
    // Authorization check
    verifyAccess(new AtlasEntityAccessRequest(typeRegistry, CREATE_ENTITY), 
                "entity-type:" + ATLAS_GLOSSARY_TERM_ENTITY_TYPE);
}
```

**UPDATE**:
- Handles glossary movement
- Updates entity meanings on rename (via tasks)
- Maintains category relationships

**Special Feature - Meaning Updates**:
```java
private void updateMeanings(AtlasEntity entity, AtlasVertex vertex) {
    // Create task to update all entities that reference this term
    Map<String, Object> taskParams = new HashMap<>();
    taskParams.put("termGuid", entity.getGuid());
    taskParams.put("termName", entity.getAttribute(NAME));
    taskParams.put("glossaryName", glossaryName);
    
    taskManagement.createTask(UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE, 
                             RequestContext.get().getUser(), taskParams);
}
```

### 2.3 CategoryPreProcessor

**Class**: `CategoryPreProcessor`  
**Entity Type**: `AtlasGlossaryCategory`  
**Purpose**: Manages glossary categories

#### Key Operations

**CREATE/UPDATE**:
- Validates parent-child hierarchy
- Ensures name uniqueness within parent
- Handles category movement between glossaries
- Updates child categories on parent changes

```java
private void processParentCategory(AtlasEntity entity) {
    AtlasObjectId parentCategoryId = (AtlasObjectId) entity.getAttribute(PARENT_CATEGORY);
    if (parentCategoryId != null) {
        // Validate no circular references
        if (parentCategoryId.getGuid().equals(entity.getGuid())) {
            throw new AtlasBaseException(INVALID_PARENT_CATEGORY);
        }
        
        // Check parent exists and is in same glossary
        AtlasVertex parentVertex = entityRetriever.getEntityVertex(parentCategoryId);
        String parentGlossaryId = getGlossaryGuid(parentVertex);
        if (!glossaryId.equals(parentGlossaryId)) {
            throw new AtlasBaseException(CATEGORY_PARENT_DIFFERENT_GLOSSARY);
        }
    }
}
```

## 3. Access Control Preprocessors

### 3.1 PersonaPreProcessor

**Class**: `PersonaPreProcessor`  
**Entity Type**: `Persona`  
**Purpose**: Manages personas (access control groups)

#### Key Operations

**CREATE**:
```java
private void processCreatePersona(AtlasEntity entity) {
    // Generate qualified name
    String personaName = (String) entity.getAttribute(NAME);
    entity.setAttribute(QUALIFIED_NAME, "persona/" + personaName);
    
    // Set role ID
    String roleId = "persona_" + personaName;
    entity.setAttribute(PERSONA_ROLE_ID, roleId);
    
    // Create Keycloak role
    keycloakStore.createRole(roleId, personaName);
    
    // Create ES alias
    createESAlias(personaName);
    
    // Set enabled state
    entity.setAttribute(IS_ENABLED, true);
}
```

**UPDATE**:
- Updates Keycloak roles
- Manages user/group assignments
- Updates policy relationships

**DELETE**:
```java
public void processDelete(AtlasVertex vertex) {
    // Delete Keycloak role
    String roleId = vertex.getProperty(PERSONA_ROLE_ID, String.class);
    keycloakStore.deleteRole(roleId);
    
    // Delete ES alias
    deleteESAlias(vertex.getProperty(NAME, String.class));
    
    // Delete associated policies
    deletePersonaPolicies(vertex);
}
```

### 3.2 PurposePreProcessor

**Class**: `PurposePreProcessor`  
**Entity Type**: `Purpose`  
**Purpose**: Manages data access purposes

#### Key Operations

**CREATE/UPDATE**:
```java
private void processPurpose(AtlasEntity entity) {
    // Validate purpose name
    String purposeName = (String) entity.getAttribute(NAME);
    
    // Create/update tag
    String tagName = "Purpose" + purposeName;
    entity.setAttribute(PURPOSE_CLASSIFICATION_NAME, tagName);
    
    // Manage ES alias
    if (isEnabledChanged(entity, existingEntity)) {
        if (Boolean.TRUE.equals(entity.getAttribute(IS_ENABLED))) {
            createESAlias(purposeName);
        } else {
            deleteESAlias(purposeName);
        }
    }
    
    // Sync policy states
    updatePolicyStates(entity);
}
```

### 3.3 AuthPolicyPreProcessor

**Class**: `AuthPolicyPreProcessor`  
**Entity Type**: `AuthPolicy`  
**Purpose**: Manages authorization policies

#### Key Operations
- Validates policy configurations
- Manages policy-resource relationships
- Handles policy enable/disable states
- Synchronizes with parent entities (personas, purposes)

## 4. Data Mesh Preprocessors

### 4.1 DataDomainPreProcessor

**Class**: `DataDomainPreProcessor`  
**Entity Type**: `DataDomain`  
**Purpose**: Manages data domains in data mesh

#### Key Operations

**CREATE**:
```java
private void processCreateDomain(AtlasEntity entity) {
    // Set parent domain
    AtlasObjectId parentDomainId = (AtlasObjectId) entity.getAttribute(PARENT_DOMAIN_REF);
    
    // Generate qualified name
    String domainName = (String) entity.getAttribute(NAME);
    String qualifiedName;
    if (parentDomainId != null) {
        String parentQN = getParentQualifiedName(parentDomainId);
        qualifiedName = parentQN + "/" + domainName;
    } else {
        qualifiedName = "domain/" + domainName;
    }
    entity.setAttribute(QUALIFIED_NAME, qualifiedName);
    
    // Validate stakeholders
    validateStakeholders(entity);
}
```

**UPDATE**:
- Handles domain hierarchy changes
- Cascades qualified name updates to subdomains and products
- Updates policy resources

### 4.2 DataProductPreProcessor

**Class**: `DataProductPreProcessor`  
**Entity Type**: `DataProduct`  
**Purpose**: Manages data products

#### Key Operations

**CREATE/UPDATE**:
```java
private void processDataProduct(AtlasEntity entity) {
    // Validate parent domain
    AtlasObjectId domainId = (AtlasObjectId) entity.getAttribute(DATA_DOMAIN);
    if (domainId == null) {
        throw new AtlasBaseException(MISSING_DATA_DOMAIN);
    }
    
    // Generate qualified name
    String productName = (String) entity.getAttribute(NAME);
    String domainQN = getDomainQualifiedName(domainId);
    entity.setAttribute(QUALIFIED_NAME, domainQN + "/product/" + productName);
    
    // Handle output ports
    processOutputPorts(entity);
    
    // Set DAAP status
    updateDaapStatus(entity);
    
    // Generate asset DSL
    generateAssetDSL(entity);
}
```

#### DAAP Status Calculation
```java
private void updateDaapStatus(AtlasEntity entity) {
    DaapStatus status = DAAP_REGISTERED;
    
    if (hasOutputPorts(entity)) {
        status = DAAP_IN_PROGRESS;
        
        if (hasCertifiedAssets(entity)) {
            status = DAAP_ENABLED;
        }
    }
    
    entity.setAttribute(DAAP_STATUS, status.name());
}
```

## 5. Query/SQL Preprocessors

### 5.1 QueryPreProcessor

**Class**: `QueryPreProcessor`  
**Entity Type**: `Query`  
**Purpose**: Manages saved queries

#### Key Operations
```java
private void processQuery(AtlasEntity entity) {
    // Validate parent folder
    AtlasObjectId parentId = (AtlasObjectId) entity.getAttribute(PARENT_FOLDER_RELATION_TYPE);
    if (parentId == null) {
        parentId = (AtlasObjectId) entity.getAttribute(__PARENT_FOLDER_RELATION_TYPE);
    }
    
    // Set qualified name based on parent
    String queryName = (String) entity.getAttribute(NAME);
    String parentQN = getParentQualifiedName(parentId);
    entity.setAttribute(QUALIFIED_NAME, parentQN + "/" + queryName);
    
    // Update collection reference
    updateCollectionReference(entity);
}
```

### 5.2 QueryFolderPreProcessor

**Class**: `QueryFolderPreProcessor`  
**Entity Type**: `Folder`  
**Purpose**: Manages query folders

#### Key Operations
- Validates folder hierarchy
- Ensures name uniqueness within parent
- Cascades qualified name changes to child folders and queries
- Handles folder movement

### 5.3 QueryCollectionPreProcessor

**Class**: `QueryCollectionPreProcessor`  
**Entity Type**: `QueryCollection`  
**Purpose**: Manages query collections

#### Key Operations
- Validates collection administrators
- Manages collection namespaces
- Handles collection-query relationships

## 6. Resource Preprocessors

### 6.1 LinkPreProcessor

**Class**: `LinkPreProcessor`  
**Entity Type**: `Link`  
**Purpose**: Manages external links

#### Key Operations
```java
private void processLink(AtlasEntity entity) {
    // Validate URL
    String url = (String) entity.getAttribute(LINK);
    if (!isValidURL(url)) {
        throw new AtlasBaseException(INVALID_URL);
    }
    
    // Validate asset association
    Collection<AtlasObjectId> assets = (Collection<AtlasObjectId>) entity.getAttribute(ASSET);
    if (CollectionUtils.isEmpty(assets)) {
        throw new AtlasBaseException(MISSING_ASSET);
    }
    
    // Generate qualified name
    entity.setAttribute(QUALIFIED_NAME, generateUniqueQualifiedName());
}
```

### 6.2 ReadmePreProcessor

**Class**: `ReadmePreProcessor`  
**Entity Type**: `Readme`  
**Purpose**: Manages documentation

#### Key Operations
- Validates readme content
- Ensures single readme per asset
- Manages asset associations

## 7. Contract Preprocessor

**Class**: `ContractPreProcessor`  
**Entity Type**: `Contract`  
**Purpose**: Manages data contracts

### Key Operations

**CREATE**:
```java
private void processCreateContract(AtlasEntity entity) {
    // Validate contract type
    String contractType = (String) entity.getAttribute(CONTRACT_TYPE);
    if (!isValidContractType(contractType)) {
        throw new AtlasBaseException(INVALID_CONTRACT_TYPE);
    }
    
    // Validate JSON
    String contractJson = (String) entity.getAttribute(CONTRACT_JSON);
    validateContractJson(contractJson);
    
    // Set version
    entity.setAttribute(CONTRACT_VERSION, 1L);
    
    // Generate qualified name
    String assetQN = getAssetQualifiedName(entity);
    entity.setAttribute(QUALIFIED_NAME, assetQN + "/contract/" + contractType);
}
```

**UPDATE**:
```java
private void processUpdateContract(AtlasEntity entity, AtlasEntity existing) {
    // Handle certification
    String newStatus = (String) entity.getAttribute(STATUS);
    String oldStatus = (String) existing.getAttribute(STATUS);
    
    if (CERTIFIED.equals(newStatus) && !CERTIFIED.equals(oldStatus)) {
        // Create new version
        Long currentVersion = (Long) existing.getAttribute(CONTRACT_VERSION);
        entity.setAttribute(CONTRACT_VERSION, currentVersion + 1);
        
        // Link to previous version
        entity.setAttribute(CONTRACT_PREVIOUS_VERSION, new AtlasObjectId(existing.getGuid(), CONTRACT_ENTITY_TYPE));
    }
}
```

## 8. Connection Preprocessor

**Class**: `ConnectionPreProcessor`  
**Entity Type**: `Connection`  
**Purpose**: Manages data source connections

### Key Operations

**CREATE**:
```java
private void processCreateConnection(AtlasEntity entity) {
    // Generate qualified name
    String connectionName = (String) entity.getAttribute(NAME);
    String qualifiedName = entity.getAttribute(CONNECTION_TYPE) + "/" + connectionName;
    entity.setAttribute(QUALIFIED_NAME, qualifiedName);
    
    // Process admins
    processAdminUsers(entity);
    processAdminGroups(entity);
    processAdminRoles(entity);
    
    // Create Keycloak role
    String roleId = createConnectionRole(connectionName);
    entity.setAttribute(CONNECTION_ROLE_ID, roleId);
    
    // Bootstrap policies
    createDefaultPolicies(entity);
}
```

**DELETE**:
- Removes Keycloak roles
- Deletes associated policies
- Cleans up admin relationships

## Common Utilities

### PreProcessorUtils
Provides shared utilities:
```java
public class PreProcessorUtils {
    // UUID generation
    public static String getUUID() {
        return UUID.randomUUID().toString();
    }
    
    // Name validation
    public static boolean isNameInvalid(String name) {
        return StringUtils.isBlank(name) || name.contains("@");
    }
    
    // Qualified name helpers
    public static String createQualifiedName(String... parts) {
        return String.join("/", parts);
    }
    
    // Lexicographical ranking
    public static String generateLexicographicalRank(String prev, String next) {
        return LexoRank.genLexoRank(prev, next);
    }
}
```

## Best Practices

1. **Validation First**: Always validate inputs before modifications
2. **Authorization**: Check permissions early in the process
3. **Atomic Operations**: Ensure all related changes happen together
4. **Error Handling**: Provide clear error messages
5. **Performance**: Use tasks for expensive operations
6. **Consistency**: Maintain data integrity across relationships

## Configuration

Key configuration properties:
```properties
# Enable/disable specific preprocessors
atlas.preprocessor.enabled=true

# Task management for deferred operations
atlas.task.management.enabled=true

# Feature flags
atlas.feature.persona.enabled=true
atlas.feature.mesh.enabled=true
```

## Summary

Atlas preprocessors form a critical layer in the entity processing pipeline, ensuring:
- Data validation and integrity
- Business rule enforcement
- Relationship management
- External system integration
- Authorization enforcement
- Performance optimization through deferred processing

Each preprocessor is specialized for its entity type while following common patterns for consistency across the system.