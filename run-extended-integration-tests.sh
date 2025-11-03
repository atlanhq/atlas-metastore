#!/bin/bash
# run-extended-integration-tests.sh
# Extended integration testing with atlan-java test suite

set -e

echo "============================================"
echo "Extended Integration Tests Runner"
echo "Atlas-metastore + Atlan-java Test Suites"
echo "============================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Set Java 17
# Handle both macOS (/usr/libexec/java_home) and Linux (JAVA_HOME already set)
if [ -x "/usr/libexec/java_home" ]; then
    # macOS
    export JAVA_HOME=$(/usr/libexec/java_home -v 17)
elif [ -z "$JAVA_HOME" ]; then
    # Linux - try to find Java 17
    if [ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
    elif [ -d "/usr/lib/jvm/temurin-17-jdk-amd64" ]; then
        export JAVA_HOME="/usr/lib/jvm/temurin-17-jdk-amd64"
    else
        # Use whatever Java is in PATH
        export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java) 2>/dev/null || which java)))
    fi
fi
echo "Using JAVA_HOME: $JAVA_HOME"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Docker is not running. Please start Docker first.${NC}"
    exit 1
fi

# Check for GITHUB_TOKEN (required for Maven packages)
if [ -z "$GITHUB_TOKEN" ]; then
    echo -e "${RED}ERROR: GITHUB_TOKEN environment variable is required${NC}"
    echo -e "${YELLOW}Maven needs this to download dependencies from GitHub Packages${NC}"
    echo ""
    echo "To fix this:"
    echo "  1. Create a GitHub Personal Access Token with 'read:packages' scope"
    echo "     https://github.com/settings/tokens/new"
    echo "  2. Export it: export GITHUB_TOKEN='your-token-here'"
    echo "  3. Run this script again"
    exit 1
fi

# Configure Maven settings for GitHub Packages
MAVEN_SETTINGS_FILE="$HOME/.m2/settings.xml"
if [ ! -f "$MAVEN_SETTINGS_FILE" ]; then
    echo -e "${YELLOW}Creating Maven settings.xml for GitHub Packages authentication...${NC}"
    mkdir -p "$HOME/.m2"
    cat > "$MAVEN_SETTINGS_FILE" << EOF
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
  http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <servers>
    <server>
      <id>github</id>
      <username>\${env.GITHUB_USERNAME}</username>
      <password>\${env.GITHUB_TOKEN}</password>
    </server>
  </servers>
</settings>
EOF
    echo -e "${GREEN}✓ Maven settings.xml created${NC}"
else
    echo -e "${GREEN}✓ Maven settings.xml already exists${NC}"
fi

# Set GITHUB_USERNAME if not set (use git config or default)
if [ -z "$GITHUB_USERNAME" ]; then
    GITHUB_USERNAME=$(git config user.name 2>/dev/null || echo "github-user")
    export GITHUB_USERNAME
    echo -e "${YELLOW}Using GITHUB_USERNAME: $GITHUB_USERNAME${NC}"
fi

# Parse arguments
SKIP_BUILD=false
SKIP_ATLAS_TESTS=false
DEBUG=false
ATLAN_JAVA_TESTS="ConnectionTest SearchTest"

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --skip-build) SKIP_BUILD=true ;;
        --skip-atlas-tests) SKIP_ATLAS_TESTS=true ;;
        --debug) DEBUG=true ;;
        --tests) shift; ATLAN_JAVA_TESTS="$1" ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --skip-build         Skip building Atlas WAR and Docker image (still installs Maven modules)"
            echo "  --skip-atlas-tests   Skip atlas-metastore tests, only run atlan-java"
            echo "  --debug             Enable debug logging"
            echo "  --tests <tests>     Specify atlan-java tests to run (default: ConnectionTest SearchTest)"
            echo "  -h, --help          Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                                           # Run everything"
            echo "  $0 --skip-build                              # Skip build, run all tests"
            echo "  $0 --tests 'ConnectionTest GlossaryTest'     # Run specific atlan-java tests"
            exit 0
            ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# Cleanup function
cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    
    # Kill background processes
    if [ -n "$MAVEN_PID" ]; then
        kill $MAVEN_PID 2>/dev/null || true
    fi
    
    # Stop log capture
    if [ -n "$MONITOR_PID" ]; then
        kill $MONITOR_PID 2>/dev/null || true
    fi
    
    if [ -f /tmp/log-capture-pids.txt ]; then
        while read pid; do
            kill "$pid" 2>/dev/null || true
        done < /tmp/log-capture-pids.txt
        rm -f /tmp/log-capture-pids.txt
    fi
    
    # Let testcontainers Ryuk handle container cleanup
    echo -e "${YELLOW}Containers will be cleaned up by testcontainers Ryuk${NC}"
}

trap cleanup EXIT

# Step 1: Build Atlas if not skipping
if [ "$SKIP_BUILD" = false ]; then
    echo -e "${YELLOW}Building Atlas WAR package...${NC}"
    
    # Detect OS classifier
    if [[ "$OSTYPE" == "darwin"* ]]; then
        OS_CLASSIFIER="osx-x86_64"
    else
        OS_CLASSIFIER="linux-x86_64"
    fi
    echo -e "${BLUE}Detected OS classifier: $OS_CLASSIFIER${NC}"
    
    # Try clean first, but don't fail if it errors (file locks on macOS)
    echo -e "${BLUE}Attempting mvn clean... (may fail on macOS, that's OK)${NC}"
    mvn clean -B -Dos.detected.classifier=$OS_CLASSIFIER 2>&1 | tee /tmp/maven-clean.log || {
        echo -e "${YELLOW}⚠ Clean failed (likely file locks). Continuing with incremental build...${NC}"
    }
    
    # Now do the actual build
    # Use 'install' instead of 'package' to install modules to local Maven repo
    # This makes them available when we later run 'mvn test -pl webapp'
    echo -e "${BLUE}Building and installing to local Maven repo...${NC}"
    mvn -B -Dos.detected.classifier=$OS_CLASSIFIER -Dmaven.test.skip -DskipTests -Drat.skip=true -DskipOverlay -DskipEnunciate=true install -Pdist
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to build Atlas WAR${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Atlas WAR built successfully${NC}"
    
    echo -e "${YELLOW}Building Atlas Docker image...${NC}"
    docker buildx build --load -t atlanhq/atlas:test .
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to build Docker image${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Docker image built successfully${NC}"
else
    echo -e "${YELLOW}Skipping Atlas WAR and Docker build (--skip-build flag set)${NC}"
    echo -e "${YELLOW}But we still need to install Maven modules for dependencies...${NC}"
    
    # Even with --skip-build, we need to install modules to local Maven repo
    # so webapp tests can find dependencies like auth-plugin-atlas
    echo -e "${BLUE}Installing Maven modules (without tests)...${NC}"
    mvn install -B -DskipTests -Drat.skip=true
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to install Maven modules${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Maven modules installed to local repository${NC}"
fi

# Step 2: Configure testcontainers for reuse
echo -e "${YELLOW}Configuring testcontainers...${NC}"
mkdir -p ~/.testcontainers
echo "testcontainers.reuse.enable=true" > ~/.testcontainers.properties
export TESTCONTAINERS_REUSE_ENABLE=true

if [ "$DEBUG" = true ]; then
    export TESTCONTAINERS_DEBUG=true
fi

echo -e "${GREEN}✓ Testcontainers configured for reuse${NC}"

# Step 3: Clean up any existing test containers from previous runs
echo -e "${YELLOW}Cleaning up existing test containers...${NC}"
docker rm -f $(docker ps -a --filter "name=testcontainers" --format "{{.Names}}") 2>/dev/null || true

# Step 4: Run atlas-metastore tests (or just start containers)
mkdir -p target/test-logs

if [ "$SKIP_ATLAS_TESTS" = false ]; then
    echo -e "${BLUE}======================================${NC}"
    echo -e "${BLUE}STAGE 1: Atlas-metastore tests${NC}"
    echo -e "${BLUE}======================================${NC}"
    
    # Start container log capture
    echo "Starting container log monitor..."
    bash -c '
        while true; do
            for container_id in $(docker ps -q 2>/dev/null); do
                container_name=$(docker inspect --format="{{.Name}}" "$container_id" 2>/dev/null | sed "s/^\///")
                if [[ "$container_name" == testcontainers-* ]] || [[ "$container_name" == *atlas* ]]; then
                    log_file="target/test-logs/${container_name}.log"
                    if [ ! -f "${log_file}.capturing" ]; then
                        echo "Capturing logs from: $container_name"
                        touch "${log_file}.capturing"
                        docker logs -f "$container_id" > "$log_file" 2>&1 &
                        echo "$!" >> /tmp/log-capture-pids.txt
                    fi
                fi
            done
            sleep 5
        done
    ' &
    MONITOR_PID=$!
    
    # Run atlas-metastore tests in background
    echo -e "${YELLOW}Starting atlas-metastore integration tests...${NC}"
    
    if [ "$DEBUG" = true ]; then
        mvn test -B -pl webapp -Dtest=BasicServiceAvailabilityTest,BasicSanityForAttributesTypesTest \
                 -Dorg.slf4j.simpleLogger.defaultLogLevel=debug -Dsurefire.useFile=false &
    else
        mvn test -B -pl webapp -Dtest=BasicServiceAvailabilityTest,BasicSanityForAttributesTypesTest \
                 -Dsurefire.useFile=false &
    fi
    
    MAVEN_PID=$!
    echo -e "${YELLOW}Atlas-metastore tests running in background (PID: $MAVEN_PID)${NC}"
    
    # Wait for containers to start and Atlas to be ready
    echo -e "${YELLOW}Waiting for Atlas containers to start...${NC}"
    sleep 30
    
    # Find Atlas container and extract connection info
    MAX_RETRIES=30
    RETRY_COUNT=0
    ATLAS_CONTAINER=""
    
    while [ -z "$ATLAS_CONTAINER" ] && [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        ATLAS_CONTAINER=$(docker ps --filter "name=atlas" --filter "status=running" --format "{{.Names}}" | grep -v "ryuk" | head -1)
        if [ -z "$ATLAS_CONTAINER" ]; then
            echo "Waiting for Atlas container... ($RETRY_COUNT/$MAX_RETRIES)"
            sleep 2
            RETRY_COUNT=$((RETRY_COUNT + 1))
        fi
    done
    
    if [ -z "$ATLAS_CONTAINER" ]; then
        echo -e "${RED}Failed to find Atlas container after $MAX_RETRIES retries${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Found Atlas container: $ATLAS_CONTAINER${NC}"
    
    # Extract port mapping
    ATLAS_PORT=$(docker port $ATLAS_CONTAINER 21000 2>/dev/null | cut -d: -f2)
    if [ -z "$ATLAS_PORT" ]; then
        echo -e "${RED}Failed to get Atlas port mapping${NC}"
        exit 1
    fi
    
    ATLAS_URL="http://localhost:${ATLAS_PORT}"
    echo -e "${GREEN}✓ Atlas URL: $ATLAS_URL${NC}"
    
    # Wait for Atlas to be fully ready
    echo -e "${YELLOW}Waiting for Atlas to be ready...${NC}"
    RETRY_COUNT=0
    MAX_RETRIES=60
    
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        if curl -s -f "${ATLAS_URL}/api/atlas/admin/version" > /dev/null 2>&1; then
            echo -e "${GREEN}✓ Atlas is ready!${NC}"
            break
        fi
        echo "Atlas not ready yet... ($RETRY_COUNT/$MAX_RETRIES)"
        sleep 5
        RETRY_COUNT=$((RETRY_COUNT + 1))
    done
    
    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
        echo -e "${RED}Atlas did not become ready in time${NC}"
        exit 1
    fi
    
    # Wait for atlas-metastore tests to complete
    echo -e "${YELLOW}Waiting for atlas-metastore tests to complete...${NC}"
    wait $MAVEN_PID
    ATLAS_TEST_RESULT=$?
    
    if [ $ATLAS_TEST_RESULT -ne 0 ]; then
        echo -e "${RED}Atlas-metastore tests failed!${NC}"
        echo -e "${YELLOW}Check target/test-logs for details${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Atlas-metastore tests passed!${NC}"
    
else
    echo -e "${YELLOW}Skipping atlas-metastore tests (--skip-atlas-tests flag set)${NC}"
    echo -e "${YELLOW}Starting containers only...${NC}"
    
    # Start a minimal test just to spin up containers
    mvn test -B -pl webapp -Dtest=BasicServiceAvailabilityTest &
    MAVEN_PID=$!
    
    sleep 30
    
    # Find Atlas container
    ATLAS_CONTAINER=$(docker ps --filter "name=atlas" --filter "status=running" --format "{{.Names}}" | grep -v "ryuk" | head -1)
    ATLAS_PORT=$(docker port $ATLAS_CONTAINER 21000 2>/dev/null | cut -d: -f2)
    ATLAS_URL="http://localhost:${ATLAS_PORT}"
    
    # Wait for Maven to finish
    wait $MAVEN_PID
fi

# Step 5: Run atlan-java tests
echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}STAGE 2: Atlan-java tests${NC}"
echo -e "${BLUE}======================================${NC}"

# Clone atlan-java if not already present
ATLAN_JAVA_DIR="/tmp/atlan-java-$(date +%s)"
if [ -d "$ATLAN_JAVA_DIR" ]; then
    echo -e "${YELLOW}Cleaning existing atlan-java directory...${NC}"
    rm -rf "$ATLAN_JAVA_DIR"
fi

echo -e "${YELLOW}Cloning atlan-java repository...${NC}"
git clone --depth 1 https://github.com/atlanhq/atlan-java.git "$ATLAN_JAVA_DIR"

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to clone atlan-java repository${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Atlan-java cloned to $ATLAN_JAVA_DIR${NC}"

# Configure atlan-java tests
cd "$ATLAN_JAVA_DIR"

echo -e "${YELLOW}Configuring atlan-java test environment...${NC}"

# Configure atlan-java SDK for local Atlas testing
# The SDK supports a special "LOCAL" mode that:
# 1. Automatically connects to localhost:21000 
# 2. Uses LocalTokenManager with Basic Authentication (same as atlas-metastore)
# 3. Sends "Authorization: Basic <base64>" headers instead of "Bearer"

# Use "LOCAL" mode which defaults to localhost:21000
# This matches the Atlas container's internal port (which testcontainers should map to host)
export ATLAN_BASE_URL="LOCAL"

# Set Basic Auth credentials for LocalTokenManager
# The SDK will automatically:
# - Connect to localhost:21000
# - Use LocalTokenManager for authentication
# - Base64 encode these credentials  
# - Send as "Authorization: Basic <base64>" (same as atlas-metastore tests)
export ATLAN_API_KEY="admin:admin"

echo "ATLAN_BASE_URL=LOCAL (localhost:21000)"
echo "ATLAN_API_KEY=admin:admin (Basic Auth)"
echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}⚠  Port Mapping Note:${NC}"
echo -e "   Atlas container is mapped to: http://localhost:${ATLAS_PORT:-unknown}"
echo -e "   Atlan-java SDK will connect to: http://localhost:21000"
echo -e ""
if [ "$ATLAS_PORT" != "21000" ]; then
    echo -e "${YELLOW}   ⚠  PORT MISMATCH DETECTED!${NC}"
    echo -e "   The Atlas container is NOT on port 21000."
    echo -e "   Atlan-java tests will likely fail unless you either:"
    echo -e "   1. Use fixed port mapping in AtlasDockerIntegrationTest:"
    echo -e "      .withFixedExposedPort(21000, 21000)"
    echo -e "   2. Or skip atlan-java tests for now (--skip-atlas-tests)"
    echo -e ""
else
    echo -e "${GREEN}   ✓ Port mapping is correct (21000 → 21000)${NC}"
fi
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""

# Build test classes
echo -e "${YELLOW}Building atlan-java test classes...${NC}"
./gradlew testClasses -x spotlessCheck

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to build atlan-java test classes${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Test classes built${NC}"

# Run specified atlan-java tests
echo -e "${YELLOW}Running atlan-java integration tests...${NC}"
echo -e "${YELLOW}Tests: $ATLAN_JAVA_TESTS${NC}"

ATLAN_JAVA_RESULT=0

for test in $ATLAN_JAVA_TESTS; do
    echo -e "${BLUE}Running: $test${NC}"
    
    ./gradlew -PintegrationTests integration-tests:test \
              --tests "com.atlan.java.sdk.${test}" \
              -x spotlessCheck
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ $test failed${NC}"
        ATLAN_JAVA_RESULT=1
    else
        echo -e "${GREEN}✓ $test passed${NC}"
    fi
done

# Step 6: Report results
echo ""
echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}Test Results Summary${NC}"
echo -e "${BLUE}============================================${NC}"

if [ "$SKIP_ATLAS_TESTS" = false ]; then
    if [ $ATLAS_TEST_RESULT -eq 0 ]; then
        echo -e "${GREEN}✓ Atlas-metastore tests: PASSED${NC}"
    else
        echo -e "${RED}✗ Atlas-metastore tests: FAILED${NC}"
    fi
fi

if [ $ATLAN_JAVA_RESULT -eq 0 ]; then
    echo -e "${GREEN}✓ Atlan-java tests: PASSED${NC}"
else
    echo -e "${RED}✗ Atlan-java tests: FAILED${NC}"
fi

echo ""
echo -e "${YELLOW}Container logs saved to: target/test-logs/${NC}"
echo -e "${YELLOW}Atlan-java logs at: $ATLAN_JAVA_DIR/integration-tests/build/reports/${NC}"

# Final result
if [ $ATLAN_JAVA_RESULT -eq 0 ]; then
    echo ""
    echo -e "${GREEN}============================================${NC}"
    echo -e "${GREEN}All tests completed successfully! 🎉${NC}"
    echo -e "${GREEN}============================================${NC}"
    exit 0
else
    echo ""
    echo -e "${RED}============================================${NC}"
    echo -e "${RED}Some tests failed!${NC}"
    echo -e "${RED}============================================${NC}"
    exit 1
fi

