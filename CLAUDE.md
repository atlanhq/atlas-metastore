# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Atlas is an enterprise-scale metadata management and data governance platform for Hadoop ecosystems. It provides:
- Metadata repository with business taxonomical enrichment
- Data lineage tracking and impact analysis
- Security through RBAC and ABAC
- Integration with Hadoop ecosystem (Hive, HBase, Kafka, etc.)

## Build Commands

```bash
# Set memory options (required for large builds)
export MAVEN_OPTS="-Xms2g -Xmx2g"

# Quick build without tests
./build.sh

# Full build with tests
mvn clean install

# Build specific module with dependencies
mvn clean install -pl repository,webapp -am

# Build without tests, license checks, and docs
mvn clean install -DskipTests -Drat.skip=true -DskipEnunciate=true

# Package distribution
mvn clean package -Pdist
```

## Test Commands

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=TestClassName

# Run specific test method
mvn test -Dtest=TestClassName#testMethodName

# Run tests in specific module
mvn test -pl module-name

# Run integration tests
mvn verify
```

## Code Quality Commands

```bash
# Run checkstyle
mvn checkstyle:check

# Run findbugs
mvn findbugs:check

# Check licenses
mvn apache-rat:check
```

## Architecture Overview

### Core Components

1. **Graph Database Layer** (`graphdb/` module)
   - `AtlasGraph` interface - Primary abstraction for graph operations
   - JanusGraph implementation with Elasticsearch/Solr indexing
   - Transaction management and graph traversals
   - Detailed documentation: @docs/CLAUDE-graphdb-module.md

2. **Type System** (`intg/` module)
   - `AtlasTypeRegistry` - Central registry for all metadata types
   - Entity types, classifications, structs, and enums
   - Type inheritance and relationships
   - Detailed documentation: @docs/CLAUDE-intg-module.md

3. **Repository Layer** (`repository/` module)
   - `AtlasEntityStore` - Entity CRUD operations
   - `EntityGraphMapper` - Maps domain models to graph storage
   - `EntityGraphRetriever` - Efficient entity retrieval
   - Detailed documentation: @docs/CLAUDE-repository-module.md

4. **REST API Layer** (`webapp/` module)
   - Jersey-based REST endpoints
   - Main resources: EntityREST, TypesREST, DiscoveryREST, LineageREST
   - Consistent JSON media type handling
   - Detailed documentation: @docs/CLAUDE-webapp-module.md

5. **Notification System** (`notification/` module)
   - Kafka-based event streaming
   - Entity change notifications
   - Message compression and splitting for large payloads

### Key Design Patterns

- **Repository Pattern**: Clean separation between domain and data access
- **Factory Pattern**: Type creation and graph provider instantiation
- **Observer Pattern**: Change notifications and audit logging
- **Dependency Injection**: Spring framework with @Component, @Service annotations

### Important Interfaces

- `AtlasGraph` - Graph database operations
- `AtlasTypeRegistry` - Type system access
- `AtlasEntityStore` - Entity CRUD operations
- `NotificationInterface` - Event notifications
- `AtlasEntityWithExtInfo` - Entity with related data

### Module Structure

- `intg/` - Integration interfaces and models
- `common/` - Shared utilities
- `repository/` - Core repository implementation
- `graphdb/` - Graph database abstractions
- `webapp/` - REST API and web application
- `notification/` - Event notification system
- `authorization/` - Security implementation
- `client/` - Java client libraries

### Key Configuration Files

- `atlas-application.properties` - Main application configuration
- `atlas-logback.xml` - Logging configuration
- `keycloak.json` - Authentication configuration
- `users-credentials.properties` - File-based auth credentials

## Important Flows

- **Bulk Entity Ingestion**: End-to-end flow from HTTP to database - @docs/CLAUDE-bulk-entity-ingestion-flow.md
- **Entity Preprocessors**: Detailed documentation of all preprocessors - @docs/CLAUDE-preprocessors.md
- **Entity Diff Calculation**: How Atlas calculates entity changes during bulk operations - @docs/CLAUDE-entity-diff-calculation.md
- **Index Search Flow**: Direct Elasticsearch search with relationship prefetching optimization - @docs/CLAUDE-index-search-flow.md

## Code Quality

- **Coding Standards**: Java conventions, logging, metrics, and best practices - @docs/CLAUDE-code-quality-guide.md
- **Prometheus Metrics**: How Atlas collects and publishes metrics - @docs/CLAUDE-prometheus-metrics.md

## Vertex ID-Only Implementation (Cassandra Offloading)

- **Implementation Details**: Technical architecture and design decisions - @docs/CLAUDE-vertex-idonly-implementation.md
- **Code Review Guide**: Key changes, security considerations, and review checklist - @docs/CLAUDE-vertex-idonly-code-review.md
- **Testing Guide**: Comprehensive testing strategies and examples - @docs/CLAUDE-vertex-idonly-testing-guide.md

## Development Tips

### Running Local Development Stack

```bash
cd dev-support/atlas-docker/
./download-archives.sh
docker-compose -f docker-compose.atlas-base.yml \
               -f docker-compose.atlas.yml \
               -f docker-compose.atlas-kafka.yml up -d
```

### Common Development Tasks

1. **Adding a new entity type**: Define in JSON and load via TypesREST API
2. **Extending REST API**: Add methods to appropriate REST resource class
3. **Graph operations**: Use AtlasGraph interface, not direct JanusGraph APIs
4. **Adding notifications**: Extend AbstractNotification class

### Testing REST APIs

Use scripts in `dev-support/atlas-scripts/`:
```bash
# Configure server
vi env_atlas.sh

# Test endpoints
./admin_status.sh
./entity_create.sh entity.json
```

### Performance Considerations

- Use bulk operations when possible
- Leverage Elasticsearch for searches instead of graph traversals
- Entity relationships are lazy-loaded
- Type registry maintains caches - clear when updating types

## Current Branch Information

- Current branch: `idonlymaster`
- Main branch for PRs: `master`
- CI runs on: alpha, beta, development, master, lineageondemand, idonlymaster