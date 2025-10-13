#!/bin/bash
# test-failing-image.sh
# Test the specific failing image c983751abcd using current integration test setup

set -e

echo "============================================"
echo "Testing Failing Image: c983751abcd"
echo "============================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# The failing image from Azure
FAILING_IMAGE="ghcr.io/atlanhq/atlas-metastore-beta:c983751abcd"
TEST_IMAGE_TAG="atlanhq/atlas:test"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Docker is not running. Please start Docker first.${NC}"
    exit 1
fi

echo -e "${YELLOW}Step 1: Pulling the failing image...${NC}"
echo "Image: $FAILING_IMAGE"

# Try to pull the image (you might need to authenticate first)
if docker pull "$FAILING_IMAGE" 2>/dev/null; then
    echo -e "${GREEN}Image pulled successfully${NC}"
else
    echo -e "${RED}Failed to pull image. Checking if it exists locally...${NC}"
    if ! docker image inspect "$FAILING_IMAGE" > /dev/null 2>&1; then
        echo -e "${RED}Image not found. You may need to authenticate:${NC}"
        echo "  docker login ghcr.io -u YOUR_USERNAME"
        exit 1
    fi
    echo -e "${YELLOW}Using locally cached image${NC}"
fi

echo -e "${YELLOW}Step 2: Retagging image for integration tests...${NC}"
docker tag "$FAILING_IMAGE" "$TEST_IMAGE_TAG"

echo -e "${YELLOW}Step 3: Verifying image...${NC}"
docker images | grep "atlanhq/atlas"

echo -e "${YELLOW}Step 4: Cleaning up existing test containers...${NC}"
docker rm -f atlas-test-zookeeper atlas-test-kafka atlas-test-cassandra \
             atlas-test-elasticsearch atlas-test-redis atlas-test-atlas 2>/dev/null || true

echo -e "${YELLOW}Step 5: Setting up test environment...${NC}"

# Detect Java 17
if [ -n "$JAVA_HOME" ]; then
    echo "Using existing JAVA_HOME: $JAVA_HOME"
elif [ -d "/usr/local/Cellar/openjdk@17" ]; then
    # Homebrew on Intel Mac
    JAVA_17_VERSION=$(ls -1 /usr/local/Cellar/openjdk@17/ | head -1)
    export JAVA_HOME="/usr/local/Cellar/openjdk@17/${JAVA_17_VERSION}/libexec/openjdk.jdk/Contents/Home"
    echo "Found Java 17 (Homebrew Intel): $JAVA_HOME"
elif [ -d "/opt/homebrew/opt/openjdk@17" ]; then
    # Homebrew on Apple Silicon
    export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
    echo "Found Java 17 (Homebrew ARM): $JAVA_HOME"
elif command -v /usr/libexec/java_home &> /dev/null && /usr/libexec/java_home -v 17 &> /dev/null; then
    # System java_home tool
    export JAVA_HOME=$(/usr/libexec/java_home -v 17)
    echo "Found Java 17 (System): $JAVA_HOME"
else
    echo -e "${RED}Java 17 not found. Please install Java 17.${NC}"
    echo "Install via: brew install openjdk@17"
    exit 1
fi

# Check Maven
if ! command -v mvn &> /dev/null; then
    echo -e "${RED}Maven not found. Please install Maven.${NC}"
    echo "Install via: brew install maven"
    exit 1
fi

export TESTCONTAINERS_REUSE_ENABLE=true
export TESTCONTAINERS_RYUK_DISABLED=false

echo -e "${YELLOW}Step 6: Running integration tests with the FAILING IMAGE...${NC}"
echo ""
echo "⚠️  CRITICAL: If the test FAILS, it means GitHub Actions CAN catch this bug!"
echo "⚠️  CRITICAL: If the test PASSES, it means GitHub Actions CANNOT catch this bug!"
echo ""

# Run the tests
mvn test -pl webapp -Dtest=AtlasDockerIntegrationTest \
         -Dorg.slf4j.simpleLogger.defaultLogLevel=debug \
         -Dorg.testcontainers.log.level=DEBUG \
         -Dsurefire.useFile=false

TEST_RESULT=$?

# Collect logs if tests failed
if [ $TEST_RESULT -ne 0 ]; then
    echo -e "${RED}============================================${NC}"
    echo -e "${RED}Tests FAILED with the c983751abcd image!${NC}"
    echo -e "${RED}============================================${NC}"
    echo ""
    echo -e "${GREEN}✅ GOOD NEWS: GitHub Actions CAN catch this bug!${NC}"
    echo -e "${GREEN}✅ This means Option 1 (current setup) is viable!${NC}"
    echo ""
    
    echo -e "${YELLOW}Collecting container logs...${NC}"
    mkdir -p target/test-logs
    for container in $(docker ps -a --filter "name=atlas-test" --format "{{.Names}}"); do
        echo "Collecting logs from $container..."
        docker logs "$container" > "target/test-logs/${container}.log" 2>&1
    done
    echo -e "${YELLOW}Logs saved to target/test-logs/${NC}"
    
    exit 1
else
    echo -e "${GREEN}============================================${NC}"
    echo -e "${GREEN}Tests PASSED with the c983751abcd image!${NC}"
    echo -e "${GREEN}============================================${NC}"
    echo ""
    echo -e "${RED}❌ BAD NEWS: GitHub Actions CANNOT catch this bug${NC}"
    echo -e "${RED}❌ You need cloud-specific runners (Option 3 or 4)${NC}"
    echo ""
    exit 0
fi

