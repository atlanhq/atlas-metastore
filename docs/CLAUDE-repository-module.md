# Repository Module Documentation

This document provides detailed guidance for working with the Atlas repository module.

## Module Overview

The `repository` module is the core persistence layer of Atlas, providing robust storage and retrieval mechanisms for metadata.

## Package Structure

```
repository/src/main/java/org/apache/atlas/repository/
├── store/                    # Core storage layer
│   ├── graph/               # Graph-based storage
│   │   ├── v2/             # Current implementation
│   │   └── v1/             # Legacy implementation
│   ├── bootstrap/          # Initialization services
│   ├── aliasstore/         # ES alias management
│   └── users/              # User store (Keycloak)
├── audit/                   # Audit tracking system
├── impexp/                  # Import/export functionality
├── patches/                 # Migration and patches
├── graph/                   # Graph utilities
├── ogm/                     # Object-Graph Mapping
├── converters/              # Type converters
├── businesslineage/         # Business lineage
└── userprofile/            # User profiles
```

## Core Store Components

### AtlasEntityStore & AtlasEntityStoreV2
Primary interface for entity operations located in `store/graph/v2/AtlasEntityStoreV2.java`:
- Create/update/delete entities (single and bulk)
- Classification management (add/update/remove)
- Business attributes and labels
- Soft delete and purge operations
- Access control integration at method level

### AtlasRelationshipStore & AtlasRelationshipStoreV2
Manages entity relationships in `store/graph/v2/AtlasRelationshipStoreV2.java`:
- First-class relationship objects
- Bulk relationship operations
- Propagates classifications through relationships
- Handles relationship constraints

### EntityGraphMapper
The workhorse class in `store/graph/v2/EntityGraphMapper.java` that:
- Maps entities to graph vertices
- Handles all attribute types (primitive, object, array, map)
- Manages classifications and their propagation
- Integrates with preprocessors for validation
- Creates audit records
- Handles custom attributes and business metadata
- Implements differential auditing when enabled

Key methods:
- `mapAttributesAndClassifications()` - Core mapping logic
- `createVertex()` - New entity creation
- `updateVertex()` - Entity updates
- `mapClassifications()` - Classification management

### EntityGraphRetriever
Retrieves entities from graph in `store/graph/v2/EntityGraphRetriever.java`:
- Converts vertices to entity objects
- Supports minimal/full retrieval modes
- Handles relationship lazy loading
- Integrates access control filtering
- Optimizes for bulk retrieval

## Transaction Management

### @GraphTransaction Annotation
- Applied to service methods for transaction boundaries
- Located in `graph/AtlasGraphProvider.java`
- Automatic rollback on exceptions
- Configurable retry mechanism

### TransactionInterceptHelper
- Manages transaction lifecycle
- Handles nested transactions
- Provides hooks for pre/post transaction logic

## Bulk Operations

### BulkImporter System
Located in `store/graph/v2/bulkimport/`:
- **Producer-Consumer pattern** for scalability
- **EntityCreationManager**: Orchestrates bulk creation
- **EntityConsumer**: Processes entities in parallel
- Configurable batch sizes (default: 10)
- Automatic retry on failures
- Progress tracking and reporting

Configuration properties:
```properties
atlas.bulk.import.batch.size=10
atlas.bulk.import.consumer.count=8
atlas.bulk.import.retry.count=3
```

## Preprocessor System

Preprocessors in `store/graph/v2/preprocessor/` validate and enrich entities:
- **PreProcessorUtils**: Manages preprocessor chain
- **EntityPreProcessor**: Base class for all preprocessors
- Specific preprocessors:
  - `GlossaryPreProcessor` - Glossary validation
  - `AssetPreProcessor` - Generic asset handling
  - `DataProductPreProcessor` - Data mesh support
  - `PersonaPreProcessor` - Access control personas
  - `ContractPreProcessor` - Data contracts

## Audit System

Located in `audit/`:
- **EntityAuditRepository**: Interface for audit storage
- **ESBasedAuditRepository**: Elasticsearch implementation
- **EntityAuditListener**: Creates audit events
- Differential auditing support (only changed attributes)

Audit configuration:
```properties
atlas.entity.audit.differential=true
```

## Import/Export System

Comprehensive system in `impexp/`:
- **ExportService**: Exports with relationships
- **ImportService**: Imports with conflict resolution
- **ImportTransformer**: Entity transformation
- **ZipSource/ZipSink**: File handling
- **MigrationProgressService**: Progress tracking

Key features:
- Incremental export support
- Type system migration
- Relationship preservation
- Audit trail maintenance

## Patch Management

Located in `patches/`:
- **AtlasPatchManager**: Orchestrates patches
- **AtlasPatchRegistry**: Available patches
- Patches run automatically on startup
- Examples:
  - `UniqueAttributePatch` - Ensures uniqueness
  - `IndexConsistencyPatch` - Fixes index issues
  - `ReIndexPatch` - Rebuilds search indices

## Performance Optimizations

1. **Request-level caching**: Via RequestContext
2. **Batch processing**: Configurable batch sizes
3. **Parallel processing**: Multi-threaded consumers
4. **Lazy loading**: On-demand relationship loading
5. **Minimal retrieval**: Fetch only required attributes
6. **Index usage**: Strategic graph indices
7. **Differential auditing**: Reduced audit volume

## Key Utility Classes

- **GraphHelper** (`graph/GraphHelper.java`): Core graph operations
- **AtlasGraphUtilsV2** (`graph/AtlasGraphUtilsV2.java`): V2 specific utilities
- **AtlasEntityUtils** (`util/AtlasEntityUtils.java`): Entity manipulation
- **RequestContext** (`RequestContext.java`): Request-scoped context
- **FilterUtil** (`util/FilterUtil.java`): Query filtering

## Common Development Patterns

### Working with entities
```java
// Always use AtlasEntityStore, not direct graph access
AtlasEntityWithExtInfo entity = entityStore.getById(guid);

// Use RequestContext for caching
RequestContext.get().cacheEntity(entity);
```

### Adding preprocessors
```java
// Extend EntityPreProcessor
// Register in PreProcessorUtils.getPreProcessor()
```

### Custom attributes
```java
// Use entity.setCustomAttributes()
// Mapped automatically by EntityGraphMapper
```

### Bulk operations
```java
// Use EntityMutationContext for bulk ops
// Leverage BulkImporter for large datasets
```

### Transaction handling
```java
// Use @GraphTransaction annotation
// Avoid manual transaction management
```

## Debugging Tips

1. Enable debug logging:
```properties
log4j.logger.org.apache.atlas.repository=DEBUG
```

2. Monitor Elasticsearch for audit/search issues
3. Check graph transaction logs for failures
4. Use RequestContext.get() for request debugging
5. Enable metrics for performance tracking

## Testing Repository Components

### Unit Tests
```bash
# Run all repository tests
mvn test -pl repository

# Run specific test class
mvn test -pl repository -Dtest=EntityGraphMapperTest

# Run with debug output
mvn test -pl repository -Dtest=EntityGraphMapperTest -Dmaven.surefire.debug
```

### Integration Tests
```bash
# Run integration tests
mvn verify -pl repository -Pdistributed
```

### Common Test Patterns
- Use `TestUtilsV2` for test data creation
- Mock graph operations with `AtlasGraphMock`
- Use `RequestContext` for test isolation
- Clear caches between tests