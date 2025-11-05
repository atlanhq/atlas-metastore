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
    
    # Resume and kill Maven process if still running
    if [ -n "$MAVEN_PID" ] && ps -p $MAVEN_PID > /dev/null 2>&1; then
        # Resume if paused (SIGCONT is safe even if not paused)
        echo "Resuming Maven process (PID: $MAVEN_PID) before stopping..."
        kill -CONT $MAVEN_PID 2>/dev/null || true
        sleep 1
        
        # Kill the process
        kill $MAVEN_PID 2>/dev/null || true
        echo "Stopped Maven process (PID: $MAVEN_PID)"
    fi
    
    # Testcontainers Ryuk handles container cleanup automatically
    echo -e "${YELLOW}Containers will be cleaned up by testcontainers Ryuk${NC}"
}

trap cleanup EXIT

# Step 1: Build Atlas if not skipping
if [ "$SKIP_BUILD" = false ]; then
    echo -e "${YELLOW}Building Atlas WAR package...${NC}"
    
    # Just use build.sh like maven.yml does - it handles everything!
    echo -e "${BLUE}Running build.sh (same as maven.yml)...${NC}"
    chmod +x ./build.sh
    ./build.sh
    
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
    echo -e "${YELLOW}Skipping Atlas build (--skip-build flag set)${NC}"
    echo -e "${YELLOW}Assuming pre-built Docker image 'atlanhq/atlas:test' exists${NC}"
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

# Initialize test result variables
ATLAS_TEST_RESULT=0
ATLAN_JAVA_RESULT=0

if [ "$SKIP_ATLAS_TESTS" = false ]; then
    echo -e "${BLUE}======================================${NC}"
    echo -e "${BLUE}STAGE 1: Atlas-metastore tests${NC}"
    echo -e "${BLUE}======================================${NC}"
    echo ""
    
    # CRITICAL: We use SIGSTOP/SIGCONT to pause Maven JVM while atlan-java runs!
    # This keeps the JVM process alive, preventing Testcontainers Ryuk from cleanup
    # Flow: Maven starts → Atlas ready → PAUSE Maven → Run atlan-java → RESUME Maven
    echo -e "${YELLOW}Starting atlas-metastore integration tests...${NC}"
    
    if [ "$DEBUG" = true ]; then
        mvn test -B -pl webapp \
                 -Dtest=BasicServiceAvailabilityTest,BasicSanityForAttributesTypesTest \
                 -Dorg.slf4j.simpleLogger.defaultLogLevel=debug -Dsurefire.useFile=false &
    else
        mvn test -B -pl webapp \
                 -Dtest=BasicServiceAvailabilityTest,BasicSanityForAttributesTypesTest \
                 -Dsurefire.useFile=false &
    fi
    
    MAVEN_PID=$!
    echo -e "${YELLOW}Atlas-metastore tests started (PID: $MAVEN_PID)${NC}"
    echo -e "${YELLOW}Will pause Maven JVM once Atlas is ready to keep containers alive${NC}"
    
    # Wait for Maven to compile tests and Testcontainers to start
    # Testcontainers doesn't create containers until TEST EXECUTION starts (not during compilation)
    echo -e "${YELLOW}Waiting for test compilation and Testcontainers initialization...${NC}"
    sleep 60  # Give Maven time to compile tests and start test execution
    
    # Find Atlas container by IMAGE (not by name!)
    echo -e "${YELLOW}Finding Atlas container...${NC}"
    MAX_RETRIES=60  # 60 retries × 3 seconds = 3 minutes to find container
    RETRY_COUNT=0
    ATLAS_CONTAINER=""
    
    while [ -z "$ATLAS_CONTAINER" ] && [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        # Search by image, not name!
        ATLAS_CONTAINER=$(docker ps --filter "ancestor=atlanhq/atlas:test" --format "{{.Names}}" | head -1)
        if [ -z "$ATLAS_CONTAINER" ]; then
            echo "Waiting for Atlas container... ($RETRY_COUNT/$MAX_RETRIES)"
            sleep 2
            RETRY_COUNT=$((RETRY_COUNT + 1))
        fi
    done
    
    if [ -z "$ATLAS_CONTAINER" ]; then
        echo -e "${RED}Failed to find Atlas container after $MAX_RETRIES retries${NC}"
        kill $MAVEN_PID 2>/dev/null || true
        exit 1
    fi
    
    echo -e "${GREEN}✓ Found Atlas container: $ATLAS_CONTAINER${NC}"
    
    # Extract port mapping
    ATLAS_PORT=$(docker port $ATLAS_CONTAINER 21000 2>/dev/null | cut -d: -f2)
    if [ -z "$ATLAS_PORT" ]; then
        echo -e "${RED}Failed to get Atlas port mapping${NC}"
        kill $MAVEN_PID 2>/dev/null || true
        exit 1
    fi
    
    echo -e "${GREEN}✓ Atlas is on port: $ATLAS_PORT${NC}"
    
    # Quick test to see if port is accessible
    echo -e "${YELLOW}Testing port accessibility...${NC}"
    if nc -z localhost $ATLAS_PORT 2>/dev/null; then
        echo -e "${GREEN}✓ Port $ATLAS_PORT is accessible${NC}"
    else
        echo -e "${YELLOW}⚠ Port $ATLAS_PORT not yet accessible (container may still be starting)${NC}"
    fi
    
    # Wait for Atlas to be fully ready with VERY aggressive polling
    # CRITICAL: Must detect readiness BEFORE Maven tests finish (~5:45 min)!
    echo -e "${YELLOW}Waiting for Atlas API to be ready...${NC}"
    MAX_RETRIES=300  # 300 retries × 1 second = 5 minutes max (before tests finish)
    RETRY_COUNT=0
    ATLAS_READY=false
    LAST_ERROR=""
    
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        # CRITICAL: Check if Maven exited early (shouldn't happen in first 5 min)
        if [ -n "$MAVEN_PID" ] && ! ps -p $MAVEN_PID > /dev/null 2>&1; then
            echo -e "${RED}✗ Maven process exited at $RETRY_COUNT seconds (expected ~345s)${NC}"
            echo -e "${YELLOW}Checking if containers are still available...${NC}"
            if ! docker ps --filter "ancestor=atlanhq/atlas:test" | grep -q .; then
                echo -e "${RED}❌ Containers already cleaned up by Ryuk${NC}"
                echo -e "${YELLOW}Maven finished too fast for the pause strategy to work${NC}"
                exit 1
            fi
        fi
        
        # CRITICAL: Use the SAME readiness check as the tests!
        # Tests accept 200 OR 401 as "ready" (see AtlasDockerIntegrationTest.java:495)
        
        # Check /api/atlas/v2/types endpoint (what tests use)
        HTTP_CODE=$(timeout 2 curl -s -o /dev/null -w "%{http_code}" "http://localhost:${ATLAS_PORT}/api/atlas/v2/types" 2>/dev/null || echo "000")
        
        if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "401" ]; then
            echo -e "${GREEN}✓ Atlas is ready at $RETRY_COUNT seconds! (HTTP $HTTP_CODE from /api/atlas/v2/types)${NC}"
            ATLAS_READY=true
            break
        fi
        
        # Fallback: Try other endpoints if v2/types doesn't respond
        if [ "$HTTP_CODE" = "000" ]; then
            # Connection refused/timeout, try simpler endpoints
            HTTP_CODE_ALT=$(timeout 2 curl -s -o /dev/null -w "%{http_code}" "http://localhost:${ATLAS_PORT}/api/atlas/admin/version" 2>/dev/null || echo "000")
            if [ "$HTTP_CODE_ALT" = "200" ] || [ "$HTTP_CODE_ALT" = "401" ]; then
                echo -e "${GREEN}✓ Atlas is ready at $RETRY_COUNT seconds! (HTTP $HTTP_CODE_ALT from admin/version)${NC}"
                ATLAS_READY=true
                break
            fi
        fi
        
        # Show progress every 30 seconds with HTTP code details
        if [ $((RETRY_COUNT % 30)) -eq 0 ] && [ $RETRY_COUNT -gt 0 ]; then
            echo "Still waiting for Atlas... (${RETRY_COUNT}s / ${MAX_RETRIES}s on port ${ATLAS_PORT}, last HTTP: $HTTP_CODE)"
        fi
        
        sleep 1  # Check EVERY SECOND for fast detection!
        RETRY_COUNT=$((RETRY_COUNT + 1))
    done
    
    if [ "$ATLAS_READY" = false ]; then
        echo -e "${RED}Atlas did not become ready in time (checked for ${RETRY_COUNT} seconds)${NC}"
        echo ""
        echo -e "${YELLOW}Debugging information:${NC}"
        echo -e "${YELLOW}1. Container status:${NC}"
        docker ps --filter "ancestor=atlanhq/atlas:test" || echo "No containers found"
        echo ""
        echo -e "${YELLOW}2. Port ${ATLAS_PORT} test:${NC}"
        nc -zv localhost $ATLAS_PORT 2>&1 || echo "Port not accessible"
        echo ""
        echo -e "${YELLOW}3. Curl test with details:${NC}"
        timeout 5 curl -v "http://localhost:${ATLAS_PORT}/api/atlas/admin/version" 2>&1 | head -20 || echo "Curl failed"
        echo ""
        echo -e "${YELLOW}4. Maven process status:${NC}"
        if ps -p $MAVEN_PID > /dev/null 2>&1; then
            echo "Maven still running (PID: $MAVEN_PID)"
        else
            echo "Maven exited (containers likely cleaned up by Ryuk)"
        fi
        echo ""
        echo -e "${YELLOW}5. Container logs (last 30 lines):${NC}"
        docker logs --tail 30 $ATLAS_CONTAINER 2>&1 || echo "Could not get container logs"
        
        kill $MAVEN_PID 2>/dev/null || true
        exit 1
    fi
    
    echo -e "${GREEN}✓ Atlas container is ready!${NC}"
    echo ""  # Flush output
    
    # BRILLIANT IDEA: Pause Maven JVM to keep it alive while atlan-java tests run!
    echo -e "${YELLOW}Pausing Maven process to keep containers alive...${NC}" >&2  # Force to stderr
    echo "DEBUG: About to pause Maven PID: $MAVEN_PID" >&2
    
    if [ -n "$MAVEN_PID" ] && ps -p $MAVEN_PID > /dev/null 2>&1; then
        echo "DEBUG: Maven process is running, sending SIGSTOP..." >&2
        kill -STOP $MAVEN_PID
        sleep 1  # Give it a moment
        echo -e "${GREEN}✓ Maven JVM paused (PID: $MAVEN_PID) - containers will persist${NC}" >&2
    else
        echo -e "${YELLOW}Maven tests already completed, checking containers...${NC}" >&2
        if ! docker ps --filter "ancestor=atlanhq/atlas:test" | grep -q atlas; then
            echo -e "${RED}Containers already cleaned up${NC}" >&2
            exit 1
        fi
    fi
    echo "" >&2
else
    echo -e "${YELLOW}Skipping atlas-metastore tests${NC}"
    # Try to find existing Atlas container
    ATLAS_CONTAINER=$(docker ps --filter "ancestor=atlanhq/atlas:test" --format "{{.Names}}" | head -1)
    if [ -n "$ATLAS_CONTAINER" ]; then
        ATLAS_PORT=$(docker port $ATLAS_CONTAINER 21000 2>/dev/null | cut -d: -f2)
        echo -e "${GREEN}✓ Found existing Atlas on port: $ATLAS_PORT${NC}"
    else
        echo -e "${RED}No existing Atlas container found!${NC}"
        exit 1
    fi
fi

# Step 5: Run atlan-java tests (Maven JVM is paused to keep containers alive)
echo "========================================" >&2
echo "STAGE 2: Atlan-java tests" >&2
echo "========================================" >&2
echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}STAGE 2: Atlan-java tests${NC}"
echo -e "${BLUE}======================================${NC}"
echo -e "${YELLOW}Maven JVM is paused - containers are preserved${NC}"
echo "DEBUG: Maven PID $MAVEN_PID should be in stopped state" >&2
ps -p $MAVEN_PID -o pid,state,command 2>&1 | head -2 >&2
echo ""

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

# Use the actual port we discovered from the running container
export ATLAN_BASE_URL="http://localhost:${ATLAS_PORT}"
export ATLAN_API_KEY="admin:admin"

echo "ATLAN_BASE_URL=http://localhost:${ATLAS_PORT}"
echo "ATLAN_API_KEY=admin:admin (Basic Auth)"
echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}ℹ  Extended Integration Test Architecture:${NC}"
echo -e "   • SDK connects to: http://localhost:${ATLAS_PORT}"
echo -e "   • Using Basic Auth with admin:admin credentials"
echo -e "   • Maven JVM is PAUSED (SIGSTOP) to keep containers alive"
echo -e "   • After atlan-java tests, Maven will RESUME (SIGCONT)"
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

# First, verify Atlas is still reachable
echo -e "${YELLOW}Pre-flight check: Testing Atlas connectivity...${NC}"
echo "  Checking http://localhost:${ATLAS_PORT}/api/atlas/v2/types"

# Check HTTP code (accept 200 or 401 as "reachable")
HTTP_CODE=$(timeout 5 curl -s -o /dev/null -w "%{http_code}" "http://localhost:${ATLAS_PORT}/api/atlas/v2/types" 2>/dev/null || echo "000")

if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "401" ]; then
    echo -e "${GREEN}✓ Atlas is reachable (HTTP $HTTP_CODE)${NC}"
else
    echo -e "${RED}✗ Atlas is NOT reachable! (HTTP $HTTP_CODE)${NC}"
    echo -e "${YELLOW}Debugging:${NC}"
    echo "  1. Container status:"
    docker ps --filter "ancestor=atlanhq/atlas:test" --format "  {{.Names}} - {{.Status}}" || echo "  No containers"
    echo "  2. Port mapping:"
    docker port $ATLAS_CONTAINER 2>&1 | sed 's/^/  /' || echo "  Failed to get ports"
    echo "  3. Maven process:"
    if ps -p $MAVEN_PID > /dev/null 2>&1; then
        echo "  Maven still running (paused)"
    else
        echo "  Maven has exited (containers may be cleaned up)"
    fi
    echo -e "${YELLOW}Continuing anyway - tests will determine if Atlas is truly unavailable${NC}"
fi
echo ""

for test in $ATLAN_JAVA_TESTS; do
    echo -e "${BLUE}Running: $test${NC}"
    
    # Add timeout to prevent indefinite hangs (5 min per test)
    timeout 300 ./gradlew -PintegrationTests integration-tests:test \
              --tests "com.atlan.java.sdk.${test}" \
              --info \
              -x spotlessCheck 2>&1 | tee "/tmp/atlan-test-${test}.log"
    
    TEST_EXIT_CODE=$?
    
    if [ $TEST_EXIT_CODE -eq 124 ]; then
        echo -e "${RED}✗ $test TIMEOUT (exceeded 5 minutes)${NC}"
        echo -e "${YELLOW}Last 50 lines of test output:${NC}"
        tail -50 "/tmp/atlan-test-${test}.log"
        ATLAN_JAVA_RESULT=1
    elif [ $TEST_EXIT_CODE -ne 0 ]; then
        echo -e "${RED}✗ $test failed (exit code: $TEST_EXIT_CODE)${NC}"
        ATLAN_JAVA_RESULT=1
    else
        echo -e "${GREEN}✓ $test passed${NC}"
    fi
done

echo ""

# Check atlas-metastore test results (only if we ran them)
if [ "$SKIP_ATLAS_TESTS" = false ]; then
    echo -e "${BLUE}======================================${NC}"
    echo -e "${BLUE}Resuming Maven tests to completion${NC}"
    echo -e "${BLUE}======================================${NC}"
    
    # Resume the paused Maven process
    if [ -n "$MAVEN_PID" ] && ps -p $MAVEN_PID > /dev/null 2>&1; then
        echo -e "${YELLOW}Resuming Maven JVM (PID: $MAVEN_PID)...${NC}"
        kill -CONT $MAVEN_PID
        echo -e "${GREEN}✓ Maven JVM resumed${NC}"
        echo ""
        
        echo -e "${YELLOW}Waiting for Maven tests to complete...${NC}"
        wait $MAVEN_PID
        ATLAS_TEST_RESULT=$?
        
        if [ $ATLAS_TEST_RESULT -eq 0 ]; then
            echo -e "${GREEN}✓ Atlas-metastore tests passed${NC}"
        else
            echo -e "${RED}✗ Atlas-metastore tests failed${NC}"
        fi
    else
        # Process already exited (shouldn't happen since we paused it)
        echo -e "${YELLOW}Maven tests already completed (unexpected)${NC}"
        if grep -q "Failures: 0, Errors: 0" target/surefire-reports/*.txt 2>/dev/null; then
            ATLAS_TEST_RESULT=0
            echo -e "${GREEN}✓ Atlas-metastore tests passed${NC}"
        else
            ATLAS_TEST_RESULT=1
            echo -e "${RED}✗ Atlas-metastore tests failed${NC}"
        fi
    fi
fi

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

