# Extended Integration Tests

This document describes the extended integration testing setup that combines **atlas-metastore** and **atlan-java** test suites.

## Overview

The extended integration tests run both test suites against a **shared set of testcontainers**, dramatically expanding test coverage from 2 to 40+ integration tests.

### What It Does

```
┌─────────────────────────────────────────────────┐
│  1. Build Atlas                                 │
│  2. Start Testcontainers (with reuse enabled)  │
│  3. Run atlas-metastore tests                   │
│  4. Keep containers alive                       │
│  5. Run atlan-java tests (on same containers)  │
│  6. Cleanup                                     │
└─────────────────────────────────────────────────┘
```

### Test Coverage

**Atlas-metastore tests (2)**:
- `BasicServiceAvailabilityTest` - Service health checks
- `BasicSanityForAttributesTypesTest` - Type system validation

**Atlan-java tests (40+)**:
- `ConnectionTest` - Connection management
- `SearchTest` - Search functionality
- `GlossaryTest` - Glossary operations
- `CustomMetadataTest` - Custom metadata
- `LineageTest` - Lineage operations
- `AtlanTagTest` - Tag management
- ...and 34 more!

## Prerequisites

- Docker Desktop running
- Java 17
- Maven 3.8+
- ~10GB free disk space
- ~30 minutes for full test run

## Usage

### Basic Usage

```bash
# Run everything (atlas-metastore + atlan-java tests)
./run-extended-integration-tests.sh
```

### Skip Build (Faster Iterations)

```bash
# If you already have Atlas built
./run-extended-integration-tests.sh --skip-build
```

### Run Specific Atlan-Java Tests

```bash
# Run only specific tests
./run-extended-integration-tests.sh --tests "ConnectionTest SearchTest"

# Run a single test
./run-extended-integration-tests.sh --tests "GlossaryTest"
```

### Skip Atlas-Metastore Tests

```bash
# Only run atlan-java tests (useful for debugging)
./run-extended-integration-tests.sh --skip-atlas-tests
```

### Debug Mode

```bash
# Enable verbose logging
./run-extended-integration-tests.sh --debug
```

### Combined Options

```bash
# Skip build, run specific tests with debug
./run-extended-integration-tests.sh --skip-build --tests "ConnectionTest" --debug
```

## How It Works

### 1. Testcontainers Reuse

The script enables testcontainers reuse:

```bash
export TESTCONTAINERS_REUSE_ENABLE=true
echo "testcontainers.reuse.enable=true" > ~/.testcontainers.properties
```

This allows containers to persist across JVM boundaries.

### 2. Background Execution

Atlas-metastore tests run in background:

```bash
mvn test -pl webapp ... &
MAVEN_PID=$!
```

This keeps the JVM (and containers) alive.

### 3. Container Discovery

While tests run, the script extracts container info:

```bash
ATLAS_CONTAINER=$(docker ps --filter "name=atlas" --format "{{.Names}}")
ATLAS_PORT=$(docker port $ATLAS_CONTAINER 21000 | cut -d: -f2)
```

### 4. Atlan-Java Execution

Atlan-java tests connect to the running containers:

```bash
export ATLAN_BASE_URL="http://localhost:${ATLAS_PORT}"
./gradlew -PintegrationTests integration-tests:test --tests "..."
```

### 5. Cleanup

When the script exits, testcontainers Ryuk automatically cleans up all containers.

## Troubleshooting

### Containers Not Starting

```bash
# Check Docker is running
docker info

# Check available disk space
df -h

# Clean up old containers
docker system prune -a
```

### API Key Issues

The atlan-java tests require an `ATLAN_API_KEY`. Set it before running:

```bash
export ATLAN_API_KEY="your-api-key-here"
./run-extended-integration-tests.sh
```

Or generate one dynamically (you'll need to implement this):

```bash
# TODO: Add API key generation logic
ATLAN_API_KEY=$(generate_atlas_api_key)
```

### Port Conflicts

If Atlas ports (21000, 9200, 9042, etc.) are already in use:

```bash
# Find what's using the port
lsof -i :21000

# Kill the process or stop conflicting services
```

### Tests Timing Out

Increase timeouts in the script:

```bash
# In run-extended-integration-tests.sh
MAX_RETRIES=120  # Increase from 60
```

### Atlan-Java Clone Fails

```bash
# Manually clone to troubleshoot
git clone https://github.com/atlanhq/atlan-java.git /tmp/atlan-java
cd /tmp/atlan-java
./gradlew testClasses
```

## GitHub Actions

The extended tests run automatically:

- **Schedule**: Nightly at 2 AM UTC
- **Manual**: Via workflow_dispatch
- **Duration**: ~30-45 minutes

### Trigger Manually

1. Go to Actions → Extended Integration Tests
2. Click "Run workflow"
3. Optionally specify tests to run
4. Click "Run workflow"

### View Results

- Artifacts: Test logs and reports
- Summary: Pass/fail status
- Details: Individual test results

## Adding More Tests

To add more atlan-java tests:

1. Find the test name in `atlan-java/integration-tests/src/test/java/com/atlan/java/sdk/`
2. Add to the command:

```bash
./run-extended-integration-tests.sh --tests "ConnectionTest SearchTest YourNewTest"
```

Or modify the script's default:

```bash
# In run-extended-integration-tests.sh line 28
ATLAN_JAVA_TESTS="ConnectionTest SearchTest GlossaryTest YourNewTest"
```

## Performance Tips

### Faster Builds

```bash
# Use --skip-build for repeated runs
./run-extended-integration-tests.sh --skip-build
```

### Parallel Testing

Currently tests run sequentially. To run in parallel (advanced):

1. Start containers once
2. Run multiple Gradle commands in parallel
3. Collect results

### Container Caching

Testcontainers automatically caches images. To pre-pull:

```bash
docker pull zookeeper:3.7
docker pull cassandra:2.1
docker pull elasticsearch:7.16.2
docker pull redis:6.2.14
docker pull confluentinc/cp-kafka:7.4.0
```

## Known Limitations

1. **API Key**: Currently uses placeholder, needs real API key generation
2. **Sequential Execution**: Atlan-java tests run sequentially (could be parallelized)
3. **Resource Intensive**: Requires ~8GB RAM for all containers
4. **Network Requirements**: Needs internet to clone atlan-java repo

## Future Enhancements

- [ ] Generate Atlas API key automatically
- [ ] Parallel atlan-java test execution
- [ ] Selective test discovery (run only relevant tests based on changes)
- [ ] Results aggregation and reporting
- [ ] Integration with PR checks
- [ ] Docker layer caching for faster builds
- [ ] Multi-platform support (ARM64, x86_64)

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                  Extended Integration Tests                  │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────┐
        │  Build Atlas & Docker Image          │
        └──────────────────────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────┐
        │  Enable Testcontainers Reuse         │
        └──────────────────────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────┐
        │  Run atlas-metastore tests (bg)      │
        │  ┌────────────────────────────────┐  │
        │  │ Testcontainers start:          │  │
        │  │  - Zookeeper                   │  │
        │  │  - Kafka                       │  │
        │  │  - Cassandra                   │  │
        │  │  - Elasticsearch               │  │
        │  │  - Redis                       │  │
        │  │  - Atlas                       │  │
        │  └────────────────────────────────┘  │
        └──────────────────────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────┐
        │  Extract Container Info              │
        │  - Port: 21000 → 54321 (dynamic)     │
        │  - URL: http://localhost:54321       │
        └──────────────────────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────┐
        │  Wait for atlas-metastore tests      │
        └──────────────────────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────┐
        │  Clone atlan-java repo               │
        └──────────────────────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────┐
        │  Run atlan-java tests                │
        │  - ATLAN_BASE_URL=http://...:54321   │
        │  - Connects to running containers    │
        └──────────────────────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────┐
        │  Script exits → Ryuk cleanup         │
        │  All containers removed              │
        └──────────────────────────────────────┘
```

## Contributing

To improve the extended testing:

1. Fork the repository
2. Make changes to `run-extended-integration-tests.sh`
3. Test locally
4. Submit PR with description

## Support

For issues or questions:

1. Check this documentation
2. Review logs in `target/test-logs/`
3. Create GitHub issue with logs attached

---

**Last Updated**: 2025-11-03  
**Maintainer**: Atlan Engineering Team

