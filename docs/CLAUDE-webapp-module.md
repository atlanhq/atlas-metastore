# WebApp Module Documentation

This document provides detailed guidance for working with the Atlas webapp module, which serves as the REST API layer and web interface.

## Module Overview

The `webapp` module is the web application layer of Atlas, providing:
- RESTful APIs for all Atlas operations
- Authentication and authorization integration
- Web security filters and protections
- Administrative interfaces
- Notification system integration
- Embedded Jetty server

## Module Structure

```
webapp/src/main/java/org/apache/atlas/
├── Atlas.java                 # Main application entry point
├── notification/             # Hook notification processing
│   ├── NotificationHookConsumer      # Kafka consumer
│   ├── preprocessor/                 # Data source preprocessors
│   └── spool/                        # Message spooling
├── util/                     # Utilities
│   └── AccessAuditLogsIndexCreator   # ES index management
└── web/                      # Web layer
    ├── dao/                  # Data access objects
    ├── errors/               # Exception mappers
    ├── filters/              # Request/response filters
    ├── listeners/            # Login event listeners
    ├── model/                # Web-specific models
    ├── params/               # Request parameter handling
    ├── resources/            # Legacy v1 REST endpoints
    ├── rest/                 # Modern v2 REST endpoints
    ├── security/             # Authentication providers
    ├── service/              # Web services
    ├── servlets/             # Login and error servlets
    ├── setup/                # Bootstrap and setup
    └── util/                 # Web utilities
```

## REST API Architecture

### Base URL Patterns
- `/api/atlas/v2/*` - Version 2 REST APIs (current)
- `/api/atlas/v1/*` - Version 1 REST APIs (legacy)
- `/api/atlas/admin/*` - Administrative endpoints

### Core REST Resources

#### EntityREST
**Path**: `/api/atlas/v2/entity`  
**Purpose**: Entity CRUD operations

Key endpoints:
- `POST /bulk` - Bulk create/update entities
- `GET /guid/{guid}` - Get entity by GUID
- `PUT /guid/{guid}` - Update entity by GUID
- `DELETE /guid/{guid}` - Delete entity by GUID
- `POST /bulk/classification` - Bulk add classifications
- `POST /bulk/setClassifications` - Bulk set classifications
- `GET /bulk/headers` - Get entity headers in bulk
- `GET /bulk/uniqueAttribute/type/{typeName}` - Get entities by unique attributes

#### TypesREST
**Path**: `/api/atlas/v2/types`  
**Purpose**: Type definition management

Key endpoints:
- `GET /typedefs` - Get all type definitions
- `POST /typedefs` - Create type definitions
- `PUT /typedefs` - Update type definitions
- `DELETE /typedefs` - Delete type definitions
- `GET /typedef/name/{name}` - Get type by name
- `GET /typedef/guid/{guid}` - Get type by GUID

#### GlossaryREST
**Path**: `/api/atlas/v2/glossary`  
**Purpose**: Business glossary management

Key endpoints:
- `POST /` - Create glossary
- `GET /` - List all glossaries
- `GET /{glossaryGuid}` - Get glossary details
- `PUT /{glossaryGuid}` - Update glossary
- `DELETE /{glossaryGuid}` - Delete glossary
- `POST /term` - Create glossary term
- `GET /term/{termGuid}` - Get term details
- `POST /category` - Create category
- `GET /category/{categoryGuid}` - Get category details

#### DiscoveryREST
**Path**: `/api/atlas/v2/search`  
**Purpose**: Search and discovery

Key endpoints:
- `POST /basic` - Basic search
- `POST /dsl` - DSL-based search
- `GET /fulltext` - Full-text search
- `POST /attribute` - Attribute search
- `POST /relationship` - Relationship search
- `GET /quick` - Quick search with suggestions
- `GET /searchWithParameters` - Advanced search with parameters

#### LineageREST
**Path**: `/api/atlas/v2/lineage`  
**Purpose**: Data lineage operations

Key endpoints:
- `GET /{guid}` - Get lineage for entity
- `GET /uniqueAttribute/type/{typeName}` - Get lineage by unique attribute
- `POST /lineage-list` - Get lineage for multiple entities
- `POST /getlineage` - Get lineage with advanced options

#### RelationshipREST
**Path**: `/api/atlas/v2/relationship`  
**Purpose**: Relationship management

Key endpoints:
- `POST /` - Create relationship
- `PUT /` - Update relationship
- `GET /guid/{guid}` - Get relationship by GUID
- `DELETE /guid/{guid}` - Delete relationship

### REST API Patterns

#### Request/Response Format
- Content-Type: `application/json`
- Accept: `application/json`
- Character encoding: UTF-8

#### Common Query Parameters
- `minExtInfo` - Minimize extended information
- `ignoreRelationships` - Exclude relationship attributes
- `limit` - Pagination limit
- `offset` - Pagination offset
- `sortBy` - Sort field
- `sortOrder` - ASC or DESC

#### Error Response Format
```json
{
    "errorCode": "ATLAS-404-00-005",
    "errorMessage": "Entity with guid 123 not found",
    "error": {
        "entity-guid": "123"
    }
}
```

## Security Architecture

### Filter Chain (Execution Order)

1. **MetricsFilter** - Records request metrics
2. **AuditFilter** - Audit logging for all requests
3. **AtlasHeaderFilter** - Manages request/response headers
4. **BrotliCompressionFilter** - Response compression
5. **SpringSecurityFilterChain** - Spring Security integration
6. **AtlasAuthenticationFilter** - Basic/form authentication
7. **AtlasKnoxSSOAuthenticationFilter** - Knox SSO support
8. **AtlasCSRFPreventionFilter** - CSRF protection
9. **AtlasXSSPreventionFilter** - XSS prevention
10. **StaleTransactionCleanupFilter** - Clean stale transactions
11. **ActiveServerFilter** - HA server filtering

### Authentication Providers

#### File-based Authentication
- **Provider**: `AtlasFileAuthenticationProvider`
- **Config**: `users-credentials.properties`
- **Format**: `username=group::sha256-password`

#### LDAP Authentication
- **Provider**: `AtlasLdapAuthenticationProvider`
- **Config**: `atlas.authentication.method.ldap.*` properties
- **Features**: User/group search, bind authentication

#### Keycloak SSO
- **Provider**: `AtlasKeycloakAuthenticationProvider`
- **Config**: `keycloak.json`
- **Features**: OAuth2/OIDC support, role mapping

#### Kerberos Authentication
- **Config**: `atlas.authentication.method.kerberos.*` properties
- **Features**: SPNEGO support, keytab authentication

### Security Headers

Automatically added by filters:
- `X-Frame-Options: DENY`
- `X-Content-Type-Options: nosniff`
- `X-XSS-Protection: 1; mode=block`
- `Strict-Transport-Security` (when HTTPS)
- `Cache-Control: no-cache, no-store` (for sensitive endpoints)

## Notification System Integration

### Hook Consumer
**Class**: `NotificationHookConsumer`
- Consumes messages from Kafka
- Processes entity create/update/delete notifications
- Supports multiple data sources (Hive, HBase, RDBMS, S3)

### Preprocessors
- **HivePreprocessor** - Processes Hive metadata
- **RdbmsPreprocessor** - Processes RDBMS metadata
- **AwsS3Preprocessor** - Processes S3 events
- **SqoopPreprocessor** - Processes Sqoop imports

### Spooling
- Local file-based spooling for reliability
- Handles Kafka unavailability
- Configurable spool directory

## Administrative Features

### Admin Resource Endpoints

#### Server Status
- `GET /admin/status` - Server status and HA mode
- `GET /admin/version` - Version information
- `GET /admin/server/{serverName}` - Specific server info

#### Metrics
- `GET /admin/metrics` - JVM and application metrics
- `GET /admin/metrics/prometheus` - Prometheus format metrics

#### Import/Export
- `POST /admin/import` - Import entities
- `POST /admin/export` - Export entities
- `GET /admin/export/{guid}` - Export specific entity

#### Maintenance
- `GET /admin/patches` - List applied patches
- `POST /admin/activeSearches` - Manage active searches
- `DELETE /admin/purge` - Purge deleted entities
- `POST /admin/audit` - Query audit logs

## Application Bootstrap

### Main Entry Point
**Class**: `Atlas.java`

Bootstrap sequence:
1. Configure system properties
2. Initialize OpenTelemetry
3. Create Elasticsearch indices
4. Set up embedded Jetty server
5. Configure SSL/TLS if enabled
6. Start notification consumers
7. Initialize authentication
8. Start web server

### Setup Steps
**Class**: `SetupSteps`
- Runs one-time setup with Zookeeper coordination
- Creates default types
- Initializes graph indices
- Sets up patches

### Embedded Server Configuration
```java
// Key configurations
atlas.server.bind.address=0.0.0.0
atlas.server.http.port=21000
atlas.server.https.port=21443
atlas.enableTLS=false
atlas.server.http.header.size=65536
atlas.jetty.request.buffer.size=16192
```

## Error Handling

### Exception Mappers

#### AtlasBaseExceptionMapper
Maps Atlas exceptions to HTTP responses:
- Extracts error code and message
- Includes entity GUID if available
- Returns appropriate HTTP status

#### Generic Exception Handling
- All uncaught exceptions return 500
- Stack traces logged but not exposed
- Structured error response format

### Error Pages
- Custom error servlets for 404, 500
- Configured in web.xml
- User-friendly error messages

## Performance Optimizations

### Thread Pool Configuration
```properties
atlas.jetty.server.thread.pool.minThreads=10
atlas.jetty.server.thread.pool.maxThreads=100
atlas.jetty.queue.thread.pool.idleTimeout=60000
```

### Request Size Limits
```properties
atlas.jetty.request.buffer.size=16192
atlas.servlet.upload.max.size=104857600
```

### Compression
- Brotli compression for responses
- Configurable compression levels
- MIME type filtering

## Development Guidelines

### Adding New REST Endpoints

1. Create REST class in `web/rest` package
2. Annotate with Jersey annotations:
   ```java
   @Path("/api/atlas/v2/newresource")
   @Singleton
   @Service
   @Consumes({Servlets.JSON_MEDIA_TYPE})
   @Produces({Servlets.JSON_MEDIA_TYPE})
   ```
3. Inject required services via `@Inject`
4. Use `@Timed` for metrics
5. Follow existing patterns for error handling

### Security Considerations

1. Use `@AtlasAuthorize` for method-level authorization
2. Validate all input parameters
3. Use `Servlets.validateQueryParamLength()` for string parameters
4. Encode output to prevent XSS
5. Follow OWASP guidelines

### Testing REST Endpoints

1. Use integration tests with embedded server
2. Test authentication and authorization
3. Verify error handling
4. Check metrics generation
5. Validate response formats

## Configuration Reference

### Key Properties

```properties
# Server configuration
atlas.server.bind.address=0.0.0.0
atlas.server.http.port=21000
atlas.enableTLS=false

# Authentication
atlas.authentication.method.file=true
atlas.authentication.method.ldap=false
atlas.authentication.method.kerberos=false

# Session management
atlas.server.session.timeout.secs=3600

# Security headers
atlas.server.xframe.options=DENY
atlas.server.xcontent.type.options=nosniff

# Notification
atlas.notification.hook.consumer.threads=4
atlas.notification.hook.max.retries=3

# Performance
atlas.rest.filter.enabled=true
atlas.http.header.size=65536
```

## Monitoring and Troubleshooting

### Health Checks
- `/api/atlas/admin/status` - Overall health
- `/api/atlas/admin/metrics` - Detailed metrics
- Check `application.log` for errors

### Common Issues

1. **Authentication failures**
   - Check authentication provider configuration
   - Verify user credentials
   - Check LDAP/Keycloak connectivity

2. **Performance issues**
   - Monitor thread pool usage
   - Check GC logs
   - Review slow query logs

3. **Import/Export failures**
   - Check file size limits
   - Verify entity types exist
   - Monitor memory usage

## Summary

The webapp module provides:
- Comprehensive REST APIs for all Atlas operations
- Robust security with multiple authentication providers
- Web filters for security and monitoring
- Administrative interfaces for maintenance
- Integration with notification system
- Embedded Jetty server with production-ready configuration

It serves as the primary interface for clients to interact with Atlas, providing a secure, performant, and feature-rich API layer.