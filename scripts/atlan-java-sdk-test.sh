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

# Critical path tests (curated list)
# These tests cover core functionality and work with standard Atlan deployments
TESTS=(
  "GlossaryTest"
  "CustomMetadataTest"
  "ConnectionTest"
  "LineageTest"
  "SearchTest"
)

echo -e "${BLUE}Running ${#TESTS[@]} critical path tests:${NC}"
for test in "${TESTS[@]}"; do
  echo "  • $test"
done
echo ""

FAILED_TESTS=()
PASSED_TESTS=()
TOTAL_DURATION=0

for test in "${TESTS[@]}"; do
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo -e "${YELLOW}Running: ${test}${NC}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  
  START_TIME=$(date +%s)
  
  # Run test with timeout
  if timeout 600 ./gradlew -PintegrationTests integration-tests:test \
    --tests "com.atlan.java.sdk.${test}" \
    -x assemble \
    -x testClasses \
    > "../sdk-test-logs/${test}.log" 2>&1; then
    
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    TOTAL_DURATION=$((TOTAL_DURATION + DURATION))
    
    echo -e "${GREEN}✓ ${test} PASSED${NC} (${DURATION}s)"
    PASSED_TESTS+=("$test")
  else
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    TOTAL_DURATION=$((TOTAL_DURATION + DURATION))
    
    echo -e "${RED}✗ ${test} FAILED${NC} (${DURATION}s)"
    echo "Last 20 lines of log:"
    tail -20 "../sdk-test-logs/${test}.log"
    FAILED_TESTS+=("$test")
  fi
  
  echo ""
done

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
  echo -e "${YELLOW}⚠️  SDK tests failed but workflow will continue${NC}"
  echo "This is informational only and does not block the release."
  echo ""
  echo "Logs are available in: sdk-test-logs/"
  exit 0  # Exit 0 to not block workflow
else
  echo -e "${GREEN}✅ All SDK tests passed!${NC}"
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

