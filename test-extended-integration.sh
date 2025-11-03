#!/bin/bash
# Quick smoke test for extended integration tests

set -e

echo "=========================================="
echo "Quick Smoke Test"
echo "=========================================="

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

fail_count=0

# Test 1: Check script exists and is executable
echo -n "1. Script exists and executable... "
if [ -x ./run-extended-integration-tests.sh ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
    ((fail_count++))
fi

# Test 2: Check Java
echo -n "2. Java available... "
# Try to find Java 17
if /usr/libexec/java_home -v 17 > /dev/null 2>&1; then
    export JAVA_HOME=$(/usr/libexec/java_home -v 17)
    export PATH=$JAVA_HOME/bin:$PATH
    echo -e "${GREEN}✓${NC} (Java 17 found and set)"
elif java -version 2>&1 | grep -q "version"; then
    java_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$java_version" -ge 17 ] 2>/dev/null; then
        echo -e "${GREEN}✓${NC} (Java $java_version)"
    else
        echo -e "${YELLOW}⚠${NC} Java $java_version (recommend 17+)"
    fi
else
    echo -e "${RED}✗${NC}"
    echo -e "   ${YELLOW}Install Java 17: brew install openjdk@17${NC}"
    ((fail_count++))
fi

# Test 3: Check Docker
echo -n "3. Docker running... "
if docker info > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
    echo -e "   ${YELLOW}Start Docker Desktop${NC}"
    ((fail_count++))
fi

# Test 4: Check Maven
echo -n "4. Maven available... "
if mvn --version > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
    echo -e "   ${YELLOW}Install Maven${NC}"
    ((fail_count++))
fi

# Test 5: Check disk space
echo -n "5. Sufficient disk space (>10GB)... "
available=$(df -h . | awk 'NR==2 {print $4}' | sed 's/Gi//')
if [ "$available" -gt 10 ] 2>/dev/null; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${YELLOW}⚠${NC} Low disk space: ${available}GB"
fi

# Test 6: Check testcontainers config
echo -n "6. Testcontainers config... "
if [ -f ~/.testcontainers.properties ]; then
    echo -e "${GREEN}✓${NC} (already exists)"
else
    echo -e "${YELLOW}⚠${NC} Will be created"
fi

# Test 7: Check script syntax
echo -n "7. Script syntax valid... "
if bash -n ./run-extended-integration-tests.sh; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
    ((fail_count++))
fi

# Test 8: Check GitHub Actions workflow
echo -n "8. GitHub Actions workflow exists... "
if [ -f .github/workflows/extended-integration-tests.yml ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
    ((fail_count++))
fi

# Test 9: Check documentation
echo -n "9. Documentation exists... "
if [ -f EXTENDED_INTEGRATION_TESTS.md ] && [ -f QUICK_START_EXTENDED_TESTS.md ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
    ((fail_count++))
fi

# Test 10: Test --help flag
echo -n "10. Script --help works... "
if ./run-extended-integration-tests.sh --help > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
    ((fail_count++))
fi

# Test 11: GITHUB_TOKEN configured
echo -n "11. GITHUB_TOKEN configured... "
if [ -n "$GITHUB_TOKEN" ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${YELLOW}✗${NC}"
    echo -e "   ${YELLOW}Required for Maven GitHub Packages${NC}"
    echo -e "   ${YELLOW}Create token: https://github.com/settings/tokens/new${NC}"
    echo -e "   ${YELLOW}export GITHUB_TOKEN='your-token'${NC}"
    ((fail_count++))
fi

echo "=========================================="
if [ $fail_count -eq 0 ]; then
    echo -e "${GREEN}All checks passed! Ready to run.${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. ./run-extended-integration-tests.sh --skip-build  # If already built"
    echo "  2. ./run-extended-integration-tests.sh              # Full run"
    exit 0
else
    echo -e "${RED}$fail_count checks failed. Fix issues above.${NC}"
    exit 1
fi

