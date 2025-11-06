#!/bin/bash

# ============================================================
# 🧪 LOCAL SDK TEST RUNNER
# ============================================================
# Quick script to run SDK integration tests locally
# 
# Usage:
#   1. Set environment variables (see below)
#   2. Run: ./run-sdk-tests-local.sh
# ============================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo ""
echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}🧪 LOCAL SDK TEST RUNNER${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

# Check if required env vars are set
MISSING_VARS=()

if [ -z "$ATLAN_DOMAIN" ]; then
  MISSING_VARS+=("ATLAN_DOMAIN")
fi

if [ -z "$CLIENT_ID" ]; then
  MISSING_VARS+=("CLIENT_ID")
fi

if [ -z "$CLIENT_SECRET" ]; then
  MISSING_VARS+=("CLIENT_SECRET")
fi

if [ ${#MISSING_VARS[@]} -gt 0 ]; then
  echo -e "${RED}❌ ERROR: Missing required environment variables${NC}"
  echo ""
  echo "Missing variables:"
  for var in "${MISSING_VARS[@]}"; do
    echo "  - $var"
  done
  echo ""
  echo -e "${YELLOW}Please set them first:${NC}"
  echo ""
  echo "  export ATLAN_DOMAIN='meta02.atlan.com'      # Your AWS tenant domain"
  echo "  export CLIENT_ID='atlan-backend'            # Your OAuth client ID"
  echo "  export CLIENT_SECRET='your-secret-here'     # Your OAuth client secret"
  echo "  export ATLAN_FIXED_USER='your-username'     # Optional: verified user"
  echo ""
  echo "Then run this script again:"
  echo "  ./run-sdk-tests-local.sh"
  echo ""
  exit 1
fi

# Show configuration
echo -e "${GREEN}✅ Environment configured${NC}"
echo ""
echo "Configuration:"
echo "  Domain: ${ATLAN_DOMAIN}"
echo "  Client: ${CLIENT_ID}"
echo "  Secret: ${CLIENT_SECRET:0:10}...${CLIENT_SECRET: -5}"

if [ -n "$ATLAN_FIXED_USER" ]; then
  echo "  User: ${ATLAN_FIXED_USER} ${GREEN}(configured)${NC}"
else
  echo "  User: chris ${YELLOW}(default - may cause test failures)${NC}"
fi

echo ""
echo -e "${YELLOW}⚠️  Note: This will take 5-10 minutes and requires:${NC}"
echo "  - Internet access"
echo "  - Access to GitHub (to clone atlan-java)"
echo "  - Access to your Atlan tenant"
echo "  - Java 21 installed"
echo ""
echo -n "Press Enter to continue or Ctrl+C to cancel... "
read

echo ""
echo -e "${BLUE}Starting SDK tests...${NC}"
echo ""

# Run the SDK test script
./scripts/atlan-java-sdk-test.sh "$ATLAN_DOMAIN" "$CLIENT_ID" "$CLIENT_SECRET"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  echo ""
  echo -e "${GREEN}✅ Local SDK test completed successfully${NC}"
else
  echo ""
  echo -e "${YELLOW}⚠️  Local SDK test completed with failures (informational only)${NC}"
fi

echo ""
echo "Check sdk-test-logs/ for detailed logs"
echo ""

exit $EXIT_CODE

