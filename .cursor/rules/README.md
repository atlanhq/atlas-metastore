# Atlas Cursor Rules

This directory contains Cursor IDE rules for Apache Atlas development. These rules provide context-aware guidance and code patterns.

## Rule Categories

### Core Rules (Always/Auto-attached)
- **build-commands.mdc** - Maven build commands and configurations
- **coding-standards.mdc** - Java naming conventions and code organization standards
- **project-overview.mdc** - Atlas architecture overview and core components

### Module-Specific Rules (Auto-attached by glob patterns)
- **graphdb-module.mdc** - Graph database operations (globs: `graphdb/**/*.java`)
- **repository-module.mdc** - Repository layer patterns (globs: `repository/**/*.java`)
- **webapp-module.mdc** - REST API guidelines (globs: `webapp/**/*.java`)
- **intg-module.mdc** - Type system and models (globs: `intg/**/*.java`)
- **testing-patterns.mdc** - Test writing patterns (globs: `**/src/test/**/*.java`)

### Feature-Specific Rules (Agent-requested/Manual)
- **bulk-operations.mdc** - Bulk entity ingestion patterns
- **preprocessors.mdc** - Entity preprocessor implementation guide
- **metrics-prometheus.mdc** - Metrics and monitoring implementation
- **id-only-vertex.mdc** - Cassandra offloading feature guide
- **error-handling.mdc** - Exception handling patterns
- **performance-optimization.mdc** - Performance best practices

### Development Workflow Rules (Manual)
- **local-development.mdc** - Docker setup and local environment
- **rest-api-testing.mdc** - API testing with scripts

## Usage

### Automatic Application
Rules with glob patterns are automatically included when you work on matching files. For example, when editing a file in `repository/`, the repository-module rule will be available.

### Manual Invocation
Reference rules manually using `@ruleName` in your prompts to Cursor. For example:
- `@bulk-operations` - Get guidance on bulk operations
- `@error-handling` - Get error handling patterns

### Agent-Requested Rules
Rules marked as "agent-requested" are available to the AI, which decides whether to include them based on your query context.

## Rule Structure

Each rule follows the MDC format:
```mdc
---
description: Brief description of the rule
globs: 
  - "pattern/**/*.java"  # Optional: file patterns for auto-attachment
alwaysApply: false       # Whether to always include this rule
---

# Rule Content
Markdown content with code examples and guidelines
```

## Maintenance

- Keep rules under 500 lines for optimal performance
- Update rules when Atlas patterns change
- Add new rules for new features or modules
- Test rules are providing helpful context

## Original Documentation

These rules are derived from:
- `/CLAUDE.md` - Main project documentation
- `/docs/CLAUDE-*.md` - Detailed module and feature documentation

Refer to the original documentation for comprehensive details not included in the rules.