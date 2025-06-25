# Atlas Code Quality Guide

This guide outlines coding standards and best practices based on patterns observed throughout the Atlas codebase.

## Java Coding Standards

### Naming Conventions
- **REST endpoints**: Suffix with `REST` (EntityREST, GlossaryREST)
- **Services**: Suffix with `Service` (AtlasEntityService)
- **Stores**: Suffix with `Store` (AtlasEntityStore)
- **Preprocessors**: Suffix with `PreProcessor` (AssetPreProcessor)
- **Utilities**: Suffix with `Utils` or `Util` (GraphHelper, AtlasEntityUtils)
- **Constants**: Use UPPER_SNAKE_CASE
- **Package structure**: org.apache.atlas.module.submodule.feature

### Code Organization
- One class per file (except inner classes)
- Order: static fields → instance fields → constructors → public methods → private methods
- Keep methods focused and concise (aim for under 50 lines)
- Use dependency injection via @Inject, @Component, @Service annotations
- Prefer constructor injection over field injection

## Logging Guidelines

### Logger Setup
- Declare as: `private static final Logger LOG = LoggerFactory.getLogger(ClassName.class)`
- For performance logging: `private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.ClassName")`

### Logging Levels
- **DEBUG**: Method entry/exit, detailed flow (guard with isDebugEnabled())
- **INFO**: Important state changes, business events
- **WARN**: Recoverable errors, fallback scenarios
- **ERROR**: Exceptions, unrecoverable errors with stack traces

### Logging Best Practices
- Include contextual information in log messages
- Use parameterized messages to avoid string concatenation
- Log exceptions at ERROR level with full stack trace
- Guard expensive log operations with level checks

## Metrics and Performance

### Performance Tracking
- Use AtlasPerfTracer for REST endpoint performance
- Use MetricRecorder in RequestContext for internal operations
- Always close metrics in finally blocks
- Name metrics descriptively: "methodName" or "operation.subOperation"

### Performance Optimization
- Use @GraphTransaction for automatic transaction management
- Leverage bulk APIs for multiple entity operations
- Implement pagination for large result sets
- Cache frequently accessed data (type definitions, entities)
- Use RequestContext for request-scoped caching

## Error Handling

### Exception Usage
- Throw AtlasBaseException with appropriate error codes
- Re-throw Atlas exceptions, wrap others in AtlasBaseException
- Use specific error codes from AtlasErrorCode enum
- Include meaningful error messages with context

### Validation
- Validate parameters early in methods
- Use Servlets.validateQueryParamLength() for REST parameters
- Leverage Apache Commons: CollectionUtils.isNotEmpty(), StringUtils.isNotBlank()
- Check for null before accessing properties

## Reusing Existing Functions

### Core Utilities to Use
- **GraphHelper**: Graph operations, vertex/edge manipulation
- **AtlasGraphUtilsV2**: Encoded properties, graph utilities
- **AtlasEntityUtils**: Entity manipulation and comparison
- **AtlasTypeUtil**: Type system utilities
- **RequestContext**: Thread-local request state
- **AtlasJson**: JSON serialization/deserialization

### When to Create New Utilities
- Check existing utilities first before creating new ones
- New utilities should be generic and reusable
- Place in appropriate utils package
- Follow existing naming patterns

## Graph Operations Best Practices

### Safety Checks
- Always verify vertex/edge existence before operations
- Use encoded properties via AtlasGraphUtilsV2
- Handle graph transactions properly
- Check property existence with hasProperty()

### Property Management
- Use typed getters/setters for properties
- Handle default values appropriately
- Use setJsonProperty() for complex objects
- Clean up properties when removing relationships

## REST API Guidelines

### Endpoint Design
- Use @Timed annotation for monitoring
- Validate all input parameters
- Return appropriate HTTP status codes
- Use streaming for large data responses
- Follow RESTful naming conventions

### Authorization
- Use AtlasAuthorizationUtils.verifyAccess()
- Check permissions early in request processing
- Include resource context in authorization checks
- Handle authorization exceptions appropriately

## Testing Patterns

### Code Design for Testing
- Use @VisibleForTesting for test-only access
- Design with dependency injection for mockability
- Separate business logic from infrastructure
- Create test-specific implementations when needed

### Test Organization
- Follow Maven structure: src/test/java
- Mirror package structure of main code
- Use descriptive test method names
- Group related tests in test classes

## Documentation Standards

### JavaDoc Requirements
- Document all public methods
- Include @param, @return, @throws tags
- Describe the "what" and "why", not just "how"
- Keep documentation concise and clear

### Code Comments
- Prefer self-documenting code over comments
- Comment complex algorithms or business logic
- Document workarounds with explanation
- Remove outdated comments

## General Best Practices

### Code Reusability
- Extract common logic into utility methods
- Use existing Atlas utilities before creating new ones
- Follow DRY principle
- Create abstractions for repeated patterns

### Resource Management
- Use try-with-resources for AutoCloseable resources
- Clean up resources in finally blocks
- Avoid resource leaks in long-running operations
- Monitor resource usage in performance-critical code

### Thread Safety
- Use RequestContext for thread-local state
- Avoid shared mutable state
- Synchronize access to shared resources
- Consider concurrent collections when appropriate

## Summary

Following these guidelines ensures consistency across the Atlas codebase, improves maintainability, and leverages existing functionality effectively. When in doubt, look at similar code in the repository for patterns to follow.