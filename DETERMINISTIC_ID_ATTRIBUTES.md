# Deterministic ID Generation - Attributes Reference

This document describes all attributes used for deterministic GUID and QualifiedName generation in Atlas. The goal is to ensure two Atlas instances generate identical IDs for the same entities.

## Configuration

Deterministic ID generation is controlled by the following configuration property:

```properties
atlas.deterministic.id.generation.enabled=true|false
```

**Default**: `false` (disabled)

When **disabled**, IDs are generated randomly (original behavior).
When **enabled**, IDs are generated deterministically based on entity attributes.

## Overview

All deterministic IDs are generated using **SHA-256 hashing** of concatenated input attributes. The hash is then formatted as either:
- **UUID format** (for GUIDs): `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
- **NanoId format** (for QualifiedNames):
  - **21 characters** for most entities (Glossary, Term, Category, Domain, Product, Query resources)
  - **22 characters** for access control entities (Persona, Purpose, Stakeholder, StakeholderTitle, AuthPolicy) - for backward compatibility with existing data

---

## GUID Generation

### 1. Entity GUID

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"entity"` | Constant | Prefix to namespace entity GUIDs |
| `typeName` | `AtlasEntity.getTypeName()` | The entity type (e.g., `Table`, `Column`, `AtlasGlossary`) |
| `qualifiedName` | `entity.getAttribute(QUALIFIED_NAME)` | The entity's unique qualified name |

**Hash Input**: `"entity" + typeName + qualifiedName`

**Uniqueness Guarantee**: QualifiedName is unique per type in Atlas.

---

### 2. TypeDef GUID

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"typedef"` | Constant | Prefix to namespace typedef GUIDs |
| `typeName` | `AtlasBaseTypeDef.getName()` | The type definition name (e.g., `Table`, `PII`) |
| `serviceType` | `AtlasBaseTypeDef.getServiceType()` | Service type (defaults to `"atlas"` if empty) |

**Hash Input**: `"typedef" + typeName + serviceType`

**Uniqueness Guarantee**: TypeDef names are unique within Atlas. ServiceType provides additional context for the same name across different services.

---

### 3. Relationship GUID

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"relationship"` | Constant | Prefix to namespace relationship GUIDs |
| `relationshipType` | `AtlasRelationship.getTypeName()` | The relationship type name |
| `end1Guid` | End1 entity GUID | GUID of first end entity |
| `end2Guid` | End2 entity GUID | GUID of second end entity |

**Hash Input**: `"relationship" + relationshipType + sorted(end1Guid, end2Guid)`

**Important**: End GUIDs are **sorted alphabetically** before hashing to ensure the same relationship generates the same GUID regardless of which end is specified first.

**Uniqueness Guarantee**: A relationship of a given type between two specific entities is unique.

---

## QualifiedName Generation

### 4. Glossary QualifiedName

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"glossary"` | Constant | Prefix to namespace glossary QNs |
| `glossaryName` | `entity.getAttribute(NAME)` | The glossary display name |

**Hash Input**: `"glossary" + glossaryName`

**Uniqueness Guarantee**: Glossary names are **unique system-wide** in Atlas.

**Final QN Format**: `{nanoId}` (just the generated ID)

---

### 5. Term QualifiedName

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"term"` | Constant | Prefix to namespace term QNs |
| `termName` | `entity.getAttribute(NAME)` | The term display name |
| `anchorGlossaryQN` | Anchor glossary's `qualifiedName` | The parent glossary's QN |

**Hash Input**: `"term" + termName + anchorGlossaryQN`

**Uniqueness Guarantee**: Term names can be duplicated across different glossaries, but are unique within a single glossary. Including `anchorGlossaryQN` ensures uniqueness.

**Final QN Format**: `{nanoId}@{anchorGlossaryQN}`

---

### 6. Category QualifiedName

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"category"` | Constant | Prefix to namespace category QNs |
| `categoryName` | `entity.getAttribute(NAME)` | The category display name |
| `parentCategoryQN` | Parent category's `qualifiedName` | Parent category QN (empty string if root category) |
| `anchorGlossaryQN` | Anchor glossary's `qualifiedName` | The parent glossary's QN |

**Hash Input**: `"category" + categoryName + parentCategoryQN + anchorGlossaryQN`

**Uniqueness Guarantee**: Category names can be duplicated across different glossaries or at different levels of the hierarchy. Including both `parentCategoryQN` and `anchorGlossaryQN` ensures uniqueness.

**Final QN Format**: `{nanoId}@{anchorGlossaryQN}`

---

### 7. DataDomain QualifiedName

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"domain"` | Constant | Prefix to namespace domain QNs |
| `domainName` | `entity.getAttribute(NAME)` | The domain display name |
| `parentDomainQN` | Parent domain's `qualifiedName` | Parent domain QN (empty string if root domain) |

**Hash Input**: `"domain" + domainName + parentDomainQN`

**Uniqueness Guarantee**: Domain names can be duplicated under different parent domains. Including `parentDomainQN` ensures uniqueness.

**Final QN Format**:
- Root domain: `default/domain/{nanoId}/super`
- Child domain: `{parentDomainQN}/domain/{nanoId}`

---

### 8. DataProduct QualifiedName

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"product"` | Constant | Prefix to namespace product QNs |
| `productName` | `entity.getAttribute(NAME)` | The product display name |
| `parentDomainQN` | Parent domain's `qualifiedName` | The parent domain's QN (required) |

**Hash Input**: `"product" + productName + parentDomainQN`

**Uniqueness Guarantee**: Product names can be duplicated across different domains. Including `parentDomainQN` ensures uniqueness.

**Final QN Format**: `{parentDomainQN}/product/{nanoId}`

---

### 9. Persona QualifiedName

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"persona"` | Constant | Type identifier |
| `personaName` | `entity.getAttribute(NAME)` | The persona display name |
| `tenantId` | `RequestContext.get().getCurrentTenantId()` | The tenant identifier |

**Hash Input**: `"persona" + personaName + tenantId`

**NanoId Length**: **22 characters** (for backward compatibility)

**Uniqueness Guarantee**: Persona names are unique within a tenant.

**Final QN Format**: `{tenantId}/{nanoId}`

---

### 10. Purpose QualifiedName

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"purpose"` | Constant | Type identifier |
| `purposeName` | `entity.getAttribute(NAME)` | The purpose display name |
| `tenantId` | `RequestContext.get().getCurrentTenantId()` | The tenant identifier |

**Hash Input**: `"purpose" + purposeName + tenantId`

**NanoId Length**: **22 characters** (for backward compatibility)

**Uniqueness Guarantee**: Purpose names are unique within a tenant.

**Final QN Format**: `{tenantId}/{nanoId}`

---

### 11. Stakeholder QualifiedName

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"stakeholder"` | Constant | Type identifier |
| `stakeholderName` | `entity.getAttribute(NAME)` | The stakeholder display name |
| `domainQualifiedName` | Linked domain's `qualifiedName` | The domain this stakeholder belongs to |

**Hash Input**: `"stakeholder" + stakeholderName + domainQualifiedName`

**NanoId Length**: **22 characters** (for backward compatibility)

**Uniqueness Guarantee**: Stakeholder names are unique within a domain.

**Final QN Format**: `default/{nanoId}/{domainQualifiedName}`

---

### 12. StakeholderTitle QualifiedName

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"stakeholdertitle"` | Constant | Type identifier |
| `titleName` | `entity.getAttribute(NAME)` | The stakeholder title display name |
| `contextQN` | Domain QN or `"default"` | Domain QN for domain-specific titles, `"default"` for all-domain titles |

**Hash Input**: `"stakeholdertitle" + titleName + contextQN`

**NanoId Length**: **22 characters** (for backward compatibility)

**Uniqueness Guarantee**: Stakeholder title names are unique system-wide.

**Final QN Format**:
- All domains: `stakeholderTitle/domain/default/{nanoId}`
- Specific domain: `stakeholderTitle/domain/{nanoId}`

---

### 13. AuthPolicy QualifiedName

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"policy"` | Constant | Type identifier |
| `policyName` | `policy.getAttribute(NAME)` | The policy display name |
| `parentEntityQN` | Parent entity's `qualifiedName` | QN of the Persona/Purpose/Stakeholder this policy belongs to |

**Hash Input**: `"policy" + policyName + parentEntityQN`

**NanoId Length**: **22 characters** (for backward compatibility)

**Uniqueness Guarantee**: Policy names are unique within their parent access control entity.

**Final QN Format**: `{parentEntityQN}/{nanoId}`

---

### 14. QueryCollection QualifiedName

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"collection"` | Constant | Type identifier |
| `collectionName` | `entity.getAttribute(NAME)` | The collection display name |
| `""` | Empty string | Placeholder (collections have no parent) |
| `userName` | `AtlasAuthorizationUtils.getCurrentUserName()` | The creating user's name |

**Hash Input**: `"collection" + collectionName + "" + userName`

**Uniqueness Guarantee**: Collection names are unique per user.

**Final QN Format**: `default/collection/{userName}/{nanoId}`

---

### 15. QueryFolder QualifiedName

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"folder"` | Constant | Type identifier |
| `folderName` | `entity.getAttribute(NAME)` | The folder display name |
| `collectionQualifiedName` | Parent collection's `qualifiedName` | The parent collection's QN |
| `userName` | `AtlasAuthorizationUtils.getCurrentUserName()` | The creating user's name |

**Hash Input**: `"folder" + folderName + collectionQualifiedName + userName`

**Uniqueness Guarantee**: Folder names can be duplicated across different collections. Including both `collectionQualifiedName` and `userName` ensures uniqueness.

**Final QN Format**: `{collectionQualifiedName}/folder/{userName}/{nanoId}`

---

### 16. Query QualifiedName

| Attribute | Source | Description |
|-----------|--------|-------------|
| `"query"` | Constant | Type identifier |
| `queryName` | `entity.getAttribute(NAME)` | The query display name |
| `collectionQualifiedName` | Parent collection's `qualifiedName` | The parent collection's QN |
| `userName` | `AtlasAuthorizationUtils.getCurrentUserName()` | The creating user's name |

**Hash Input**: `"query" + queryName + collectionQualifiedName + userName`

**Uniqueness Guarantee**: Query names can be duplicated across different collections. Including both `collectionQualifiedName` and `userName` ensures uniqueness.

**Final QN Format**: `{collectionQualifiedName}/query/{userName}/{nanoId}`

---

## Summary Table

| Entity Type | Hash Inputs | Uniqueness Scope | NanoId Length |
|-------------|-------------|------------------|---------------|
| **Entity GUID** | entity + typeName + qualifiedName | Global (QN is unique) | UUID |
| **TypeDef GUID** | typedef + typeName + serviceType | Global (type names unique) | UUID |
| **Relationship GUID** | relationship + relType + sorted(guid1, guid2) | Global (endpoints unique) | UUID |
| **Glossary QN** | glossary + name | System-wide | 21 |
| **Term QN** | term + name + glossaryQN | Within glossary | 21 |
| **Category QN** | category + name + parentCatQN + glossaryQN | Within glossary hierarchy | 21 |
| **Domain QN** | domain + name + parentDomainQN | Within domain hierarchy | 21 |
| **Product QN** | product + name + parentDomainQN | Within domain | 21 |
| **Persona QN** | persona + name + tenantId | Within tenant | **22** |
| **Purpose QN** | purpose + name + tenantId | Within tenant | **22** |
| **Stakeholder QN** | stakeholder + name + domainQN | Within domain | **22** |
| **StakeholderTitle QN** | stakeholdertitle + name + contextQN | System-wide | **22** |
| **Policy QN** | policy + name + parentEntityQN | Within parent entity | **22** |
| **Collection QN** | collection + name + "" + userName | Per user | 21 |
| **Folder QN** | folder + name + collectionQN + userName | Within collection | 21 |
| **Query QN** | query + name + collectionQN + userName | Within collection | 21 |

---

## Key Design Decisions

1. **Type Prefixes**: Each hash includes a constant type prefix (e.g., `"entity"`, `"term"`, `"glossary"`) to prevent collisions between different entity types that might have similar attributes.

2. **Null-byte Separators**: Each component in the hash is followed by a null byte (`0x00`) to prevent "ab" + "c" from colliding with "a" + "bc".

3. **Sorted Relationship Endpoints**: Relationship GUIDs sort the two end GUIDs alphabetically before hashing, ensuring the same relationship generates the same GUID regardless of the order endpoints are specified.

4. **Empty String Defaults**: When optional attributes are null/empty (like `parentCategoryQN` for root categories), they default to empty string to maintain consistent hash input structure.

5. **Hierarchical Context**: For entities in hierarchies (terms, categories, domains, products), the parent's QualifiedName is included to ensure uniqueness at each level.

6. **22-Character NanoIds for Access Control Entities**: Access control entities (Persona, Purpose, Stakeholder, StakeholderTitle, AuthPolicy) use 22-character NanoIds instead of the standard 21 characters. This is for **backward compatibility** with existing data that was created using the original `AccessControlUtils.getUUID()` method which generates 22-character random NanoIds.

7. **Configuration-Controlled**: The entire deterministic ID generation feature is controlled by the `atlas.deterministic.id.generation.enabled` configuration property. When disabled (default), all IDs are generated randomly to maintain original Atlas behavior. This allows gradual rollout and easy rollback if needed.
