#!/bin/bash

##############################################################################
# Atlan-Java SDK Integration Test Script
# 
# Tests atlan-java SDK against a deployed Atlan tenant (post smoke-test)
# 
# Usage:
#   ./atlan-java-sdk-test.sh <atlan-domain> <client-id> <client-secret>
# 
# Example:
#   ./atlan-java-sdk-test.sh meta02.atlan.com atlan-backend <secret>
#
# Prerequisites:
#   - curl and jq installed
#   - Atlan tenant is deployed and healthy
#   - Valid OAuth credentials
##############################################################################

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check arguments
if [ $# -ne 3 ]; then
  echo -e "${RED}Error: Missing required arguments${NC}"
  echo "Usage: $0 <atlan-domain> <client-id> <client-secret>"
  echo "Example: $0 meta02.atlan.com atlan-backend <secret>"
  exit 1
fi

ATLAN_DOMAIN=$1
CLIENT_ID=$2
CLIENT_SECRET=$3

echo -e "${BLUE}=================================================="
echo -e "ATLAN-JAVA SDK INTEGRATION TESTS"
echo -e "==================================================${NC}"
echo "Target: https://${ATLAN_DOMAIN}"
echo "Client: ${CLIENT_ID}"
echo ""

# Create logs directory
mkdir -p sdk-test-logs

# Step 1: Get OAuth token
echo -e "${YELLOW}[1/4] Authenticating with Keycloak...${NC}"
TOKEN_RESPONSE=$(curl -s --fail --location "https://${ATLAN_DOMAIN}/auth/realms/default/protocol/openid-connect/token" \
  --header 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode "client_id=${CLIENT_ID}" \
  --data-urlencode "client_secret=${CLIENT_SECRET}" \
  --data-urlencode 'grant_type=client_credentials' 2>&1)

if [ $? -ne 0 ]; then
  echo -e "${RED}❌ ERROR: Failed to get OAuth token${NC}"
  echo "Response: $TOKEN_RESPONSE"
  exit 1
fi

TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.access_token' 2>/dev/null)

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo -e "${RED}❌ ERROR: Invalid token response${NC}"
  echo "Response: $TOKEN_RESPONSE"
  exit 1
fi

echo -e "${GREEN}✓ Authentication successful${NC}"
echo "Token acquired (${#TOKEN} chars)"
echo ""

# Step 2: Clone atlan-java repository
echo -e "${YELLOW}[2/4] Cloning atlan-java repository...${NC}"
ATLAN_JAVA_DIR="./atlan-java-sdk-tests"

if [ -d "$ATLAN_JAVA_DIR" ]; then
  echo "Cleaning up existing directory..."
  rm -rf "$ATLAN_JAVA_DIR"
fi

if ! git clone --depth 1 https://github.com/atlanhq/atlan-java.git "$ATLAN_JAVA_DIR" > sdk-test-logs/clone.log 2>&1; then
  echo -e "${RED}❌ ERROR: Failed to clone atlan-java repository${NC}"
  cat sdk-test-logs/clone.log
  exit 1
fi

# Patch AtlanLiveTest to use a configurable FIXED_USER
# The SDK tests hardcode a user named "chris" which may not exist in all tenants
ATLAN_LIVE_TEST_FILE="$ATLAN_JAVA_DIR/integration-tests/src/test/java/com/atlan/java/sdk/AtlanLiveTest.java"

if [ -n "$ATLAN_FIXED_USER" ]; then
  echo -e "${YELLOW}Patching FIXED_USER from 'chris' to '$ATLAN_FIXED_USER'...${NC}"
  
  # Backup original file
  cp "$ATLAN_LIVE_TEST_FILE" "$ATLAN_LIVE_TEST_FILE.bak"
  
  # Replace the hardcoded "chris" with the configured user
  sed -i.tmp "s/public static final String FIXED_USER = \"chris\";/public static final String FIXED_USER = \"$ATLAN_FIXED_USER\";/" "$ATLAN_LIVE_TEST_FILE"
  rm -f "$ATLAN_LIVE_TEST_FILE.tmp"
  
  # Verify the patch was applied
  if grep -q "FIXED_USER = \"$ATLAN_FIXED_USER\"" "$ATLAN_LIVE_TEST_FILE"; then
    echo -e "${GREEN}✓ FIXED_USER patched successfully${NC}"
  else
    echo -e "${RED}⚠️  WARNING: Failed to patch FIXED_USER, tests may fail${NC}"
  fi
else
  echo -e "${YELLOW}⚠️  ATLAN_FIXED_USER not set, using default 'chris'${NC}"
  echo "   Tests requiring user operations may fail if 'chris' doesn't exist in tenant"
fi

echo -e "${GREEN}✓ Repository cloned${NC}"
echo ""

# Step 3: Verify tenant connectivity
echo -e "${YELLOW}[3/4] Verifying tenant connectivity...${NC}"
STATUS_RESPONSE=$(curl -s --fail -H "Authorization: Bearer ${TOKEN}" \
  "https://${ATLAN_DOMAIN}/api/atlas/admin/version" 2>&1)

if [ $? -ne 0 ]; then
  echo -e "${RED}❌ ERROR: Cannot reach Atlan tenant${NC}"
  echo "Response: $STATUS_RESPONSE"
  exit 1
fi

VERSION=$(echo "$STATUS_RESPONSE" | jq -r '.Version' 2>/dev/null || echo "unknown")
echo -e "${GREEN}✓ Tenant is reachable${NC}"
echo "Atlas Version: $VERSION"
echo ""

# Step 4: Run SDK tests
echo -e "${YELLOW}[4/4] Running atlan-java SDK tests...${NC}"
echo ""

cd "$ATLAN_JAVA_DIR"

# Export environment variables for SDK
export ATLAN_BASE_URL="https://${ATLAN_DOMAIN}"
export ATLAN_API_KEY="${TOKEN}"

echo "SDK Configuration:"
echo "  Base URL: ${ATLAN_BASE_URL}"
echo "  API Key: ${ATLAN_API_KEY:0:20}...${ATLAN_API_KEY: -10}"
echo ""

# Autodiscover all integration test classes
echo -e "${YELLOW}Discovering integration tests...${NC}"

TEST_DIR="integration-tests/src/test/java/com/atlan/java/sdk"
if [ ! -d "$TEST_DIR" ]; then
  echo -e "${RED}❌ ERROR: Integration tests directory not found${NC}"
  exit 1
fi

# Find all *Test.java files and extract class names
# Mimics atlan-java's test.yml autodiscovery approach
TESTS=()
while IFS= read -r test_file; do
  # Extract just the class name (e.g., GlossaryTest.java -> GlossaryTest)
  test_name=$(basename "$test_file" .java)
  TESTS+=("$test_name")
done < <(find "$TEST_DIR" -maxdepth 1 -name "*Test.java" -type f | sort)

if [ ${#TESTS[@]} -eq 0 ]; then
  echo -e "${RED}❌ ERROR: No test files found in $TEST_DIR${NC}"
  exit 1
fi

echo -e "${GREEN}✓ Discovered ${#TESTS[@]} integration tests${NC}"
echo ""

echo -e "${BLUE}Running ${#TESTS[@]} tests in parallel:${NC}"
for test in "${TESTS[@]}"; do
  echo "  • $test"
done
echo ""

FAILED_TESTS=()
PASSED_TESTS=()
START_TIME=$(date +%s)

# Build Gradle command with all tests
# Run all tests in a SINGLE Gradle invocation to avoid lock contention
echo -e "${YELLOW}Building test command...${NC}"

GRADLE_TEST_ARGS=""
for test in "${TESTS[@]}"; do
  GRADLE_TEST_ARGS+="--tests \"com.atlan.java.sdk.${test}\" "
done

echo -e "${GREEN}✓ Ready to run ${#TESTS[@]} tests${NC}"
echo ""

# Run all tests in a single Gradle invocation
# This avoids Gradle lock contention issues that occur with parallel processes
echo -e "${YELLOW}Executing tests (this may take 5-10 minutes)...${NC}"
echo ""

# Check if timeout command is available (Linux/CI has it, macOS doesn't by default)
if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_CMD="timeout 1200"
  echo -e "${YELLOW}Using timeout: 20 minutes max${NC}"
elif command -v gtimeout >/dev/null 2>&1; then
  TIMEOUT_CMD="gtimeout 1200"
  echo -e "${YELLOW}Using gtimeout: 20 minutes max${NC}"
else
  TIMEOUT_CMD=""
  echo -e "${YELLOW}⚠️  timeout command not available (macOS), running without timeout${NC}"
  echo -e "${YELLOW}   Install via: brew install coreutils (optional)${NC}"
fi
echo ""

if $TIMEOUT_CMD ./gradlew -PintegrationTests integration-tests:test \
  ${GRADLE_TEST_ARGS} \
  -x assemble \
  -x testClasses \
  --continue \
  --no-daemon \
  > "../sdk-test-logs/all-tests.log" 2>&1; then
  GRADLE_EXIT_CODE=0
else
  GRADLE_EXIT_CODE=$?
fi

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

echo -e "${GREEN}✓ Test execution completed${NC} (${TOTAL_DURATION}s)"
echo ""

# Parse Gradle test results from XML files
echo -e "${YELLOW}Parsing test results...${NC}"

TEST_RESULTS_DIR="integration-tests/build/test-results/test"
if [ -d "$TEST_RESULTS_DIR" ]; then
  for test in "${TESTS[@]}"; do
    # Gradle creates TEST-com.atlan.java.sdk.TestName.xml files
    RESULT_FILE="$TEST_RESULTS_DIR/TEST-com.atlan.java.sdk.${test}.xml"
    
    if [ -f "$RESULT_FILE" ]; then
      # Check if test passed (look for failures/errors)
      FAILURES=$(grep -o 'failures="[0-9]*"' "$RESULT_FILE" | grep -o '[0-9]*')
      ERRORS=$(grep -o 'errors="[0-9]*"' "$RESULT_FILE" | grep -o '[0-9]*')
      
      if [ "$FAILURES" = "0" ] && [ "$ERRORS" = "0" ]; then
        PASSED_TESTS+=("$test")
        echo -e "  ${GREEN}✓ $test PASSED${NC}"
      else
        FAILED_TESTS+=("$test")
        echo -e "  ${RED}✗ $test FAILED${NC} (failures: $FAILURES, errors: $ERRORS)"
      fi
    else
      # Test didn't run or result file not found
      FAILED_TESTS+=("$test")
      echo -e "  ${RED}✗ $test FAILED${NC} (no result file)"
    fi
  done
else
  echo -e "${RED}⚠️  Test results directory not found${NC}"
  echo "Marking all tests as failed"
  FAILED_TESTS=("${TESTS[@]}")
fi

cd ..

# Step 5: Summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${BLUE}TEST SUMMARY${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Total Tests: ${#TESTS[@]}"
echo -e "${GREEN}Passed: ${#PASSED_TESTS[@]}${NC}"
echo -e "${RED}Failed: ${#FAILED_TESTS[@]}${NC}"
echo "Total Duration: ${TOTAL_DURATION}s (~$((TOTAL_DURATION / 60))m $((TOTAL_DURATION % 60))s)"
echo ""

if [ ${#PASSED_TESTS[@]} -gt 0 ]; then
  echo -e "${GREEN}✓ Passed Tests:${NC}"
  for test in "${PASSED_TESTS[@]}"; do
    echo "  • $test"
  done
  echo ""
fi

if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
  echo -e "${RED}✗ Failed Tests:${NC}"
  for test in "${FAILED_TESTS[@]}"; do
    echo "  • $test"
  done
  echo ""
  
  # Show last 20 lines of each failed test log
  echo -e "${YELLOW}Failed Test Logs (last 20 lines):${NC}"
  for test in "${FAILED_TESTS[@]}"; do
    echo ""
    echo "━━━ $test ━━━"
    if [ -f "sdk-test-logs/${test}.log" ]; then
      tail -20 "sdk-test-logs/${test}.log"
    else
      echo "  (log file not found)"
    fi
  done
  echo ""
  
  echo -e "${YELLOW}⚠️  SDK tests failed but workflow will continue${NC}"
  echo "This is informational only and does not block the release."
  echo ""
  echo "Logs are available in: sdk-test-logs/"
  exit 0  # Exit 0 to not block workflow
else
  echo -e "${GREEN}✅ All SDK tests passed!${NC}"
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

