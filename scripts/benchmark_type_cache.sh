#!/bin/bash
#
# Performance Benchmark Script for TypeRegistry Caching
#
# This script measures the performance improvement from request-scoped
# TypeRegistry caching in EntityGraphRetriever.
#
# Prerequisites:
# - Atlas running on localhost:21000
# - curl, python3, bc installed
# - Test data (entities) in Atlas
#
# Usage:
#   ./benchmark_type_cache.sh [ATLAS_URL] [USERNAME] [PASSWORD]
#
# Example:
#   ./benchmark_type_cache.sh http://localhost:21000 admin admin
#

set -e

# Configuration
ATLAS_URL="${1:-http://localhost:21000}"
USERNAME="${2:-admin}"
PASSWORD="${3:-admin}"
AUTH="$USERNAME:$PASSWORD"

# Benchmark parameters
WARMUP_ITERATIONS=5
BENCHMARK_ITERATIONS=20
CONCURRENT_REQUESTS=10

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=======================================${NC}"
echo -e "${BLUE}  TypeRegistry Cache Benchmark Tool${NC}"
echo -e "${BLUE}=======================================${NC}"
echo ""
echo "Atlas URL: $ATLAS_URL"
echo "Username: $USERNAME"
echo ""

# Function to check Atlas connectivity
check_atlas() {
    echo -n "Checking Atlas connectivity... "
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u "$AUTH" "$ATLAS_URL/api/atlas/v2/types/typedefs/headers" 2>/dev/null)
    if [ "$HTTP_CODE" == "200" ]; then
        echo -e "${GREEN}OK${NC}"
        return 0
    else
        echo -e "${RED}FAILED (HTTP $HTTP_CODE)${NC}"
        return 1
    fi
}

# Function to get entity GUIDs for testing
get_test_guids() {
    local TYPE_NAME="$1"
    local LIMIT="${2:-100}"

    curl -s -u "$AUTH" "$ATLAS_URL/api/atlas/v2/search/basic" \
        -H "Content-Type: application/json" \
        -d "{\"typeName\": \"$TYPE_NAME\", \"limit\": $LIMIT, \"excludeDeletedEntities\": true}" 2>/dev/null | \
        python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    entities = data.get('entities', [])
    for e in entities:
        print(e.get('guid', ''))
except:
    pass
" 2>/dev/null
}

# Function to find available entity type with data
find_test_type() {
    echo "Finding entity type with test data..."
    for TYPE in "Table" "Column" "Asset" "AtlasGlossaryTerm" "AtlasGlossary" "Persona" "Purpose" "Referenceable"; do
        COUNT=$(curl -s -u "$AUTH" "$ATLAS_URL/api/atlas/v2/search/basic" \
            -H "Content-Type: application/json" \
            -d "{\"typeName\": \"$TYPE\", \"limit\": 1}" 2>/dev/null | \
            python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('entities', [])))" 2>/dev/null)

        if [ "$COUNT" -gt "0" ] 2>/dev/null; then
            echo "  Found: $TYPE"
            echo "$TYPE"
            return 0
        fi
    done
    echo ""
    return 1
}

# Function to run single entity retrieval benchmark
benchmark_single_entity() {
    local GUID="$1"
    local START=$(python3 -c "import time; print(int(time.time() * 1000))")

    curl -s -o /dev/null -u "$AUTH" "$ATLAS_URL/api/atlas/v2/entity/guid/$GUID" 2>/dev/null

    local END=$(python3 -c "import time; print(int(time.time() * 1000))")
    echo $((END - START))
}

# Function to run bulk entity retrieval benchmark
benchmark_bulk_entities() {
    local GUIDS="$1"
    local GUID_PARAMS=$(echo "$GUIDS" | tr '\n' '&' | sed 's/^/guid=/' | sed 's/&/\&guid=/g' | sed 's/&$//')

    local START=$(python3 -c "import time; print(int(time.time() * 1000))")

    curl -s -o /dev/null -u "$AUTH" "$ATLAS_URL/api/atlas/v2/entity/bulk?$GUID_PARAMS" 2>/dev/null

    local END=$(python3 -c "import time; print(int(time.time() * 1000))")
    echo $((END - START))
}

# Function to run search benchmark
benchmark_search() {
    local TYPE_NAME="$1"
    local LIMIT="${2:-100}"

    local START=$(python3 -c "import time; print(int(time.time() * 1000))")

    curl -s -o /dev/null -u "$AUTH" "$ATLAS_URL/api/atlas/v2/search/basic" \
        -H "Content-Type: application/json" \
        -d "{\"typeName\": \"$TYPE_NAME\", \"limit\": $LIMIT, \"excludeDeletedEntities\": true}" 2>/dev/null

    local END=$(python3 -c "import time; print(int(time.time() * 1000))")
    echo $((END - START))
}

# Function to calculate statistics
calculate_stats() {
    local VALUES="$1"
    python3 -c "
import sys
values = [int(v) for v in '''$VALUES'''.strip().split('\n') if v.strip()]
if values:
    avg = sum(values) / len(values)
    sorted_vals = sorted(values)
    p50 = sorted_vals[len(values) // 2]
    p95 = sorted_vals[int(len(values) * 0.95)]
    p99 = sorted_vals[int(len(values) * 0.99)] if len(values) >= 100 else sorted_vals[-1]
    min_val = min(values)
    max_val = max(values)
    print(f'{avg:.2f},{p50},{p95},{p99},{min_val},{max_val}')
else:
    print('0,0,0,0,0,0')
"
}

# Main benchmark
main() {
    echo ""

    # Check connectivity
    if ! check_atlas; then
        echo -e "${RED}Cannot connect to Atlas. Please check URL and credentials.${NC}"
        exit 1
    fi

    # Find test type
    TEST_TYPE=$(find_test_type)
    if [ -z "$TEST_TYPE" ]; then
        echo -e "${RED}No entity types with data found. Please create test data first.${NC}"
        exit 1
    fi

    echo ""
    echo "Using entity type: $TEST_TYPE"

    # Get test GUIDs
    echo "Fetching test entity GUIDs..."
    GUIDS=$(get_test_guids "$TEST_TYPE" 100)
    GUID_COUNT=$(echo "$GUIDS" | grep -v '^$' | wc -l | tr -d ' ')

    if [ "$GUID_COUNT" -lt "10" ]; then
        echo -e "${RED}Not enough test entities (found: $GUID_COUNT, need: 10+)${NC}"
        exit 1
    fi

    echo "Found $GUID_COUNT test entities"
    FIRST_GUID=$(echo "$GUIDS" | head -1)
    BULK_GUIDS=$(echo "$GUIDS" | head -50)

    echo ""
    echo -e "${YELLOW}========================================${NC}"
    echo -e "${YELLOW}  Running Benchmarks${NC}"
    echo -e "${YELLOW}========================================${NC}"

    # ==================== Single Entity Benchmark ====================
    echo ""
    echo -e "${BLUE}1. Single Entity Retrieval (GET /entity/guid/{guid})${NC}"
    echo "   Warmup: $WARMUP_ITERATIONS iterations"

    # Warmup
    for i in $(seq 1 $WARMUP_ITERATIONS); do
        benchmark_single_entity "$FIRST_GUID" > /dev/null
    done

    # Benchmark
    echo "   Benchmark: $BENCHMARK_ITERATIONS iterations"
    SINGLE_RESULTS=""
    for i in $(seq 1 $BENCHMARK_ITERATIONS); do
        RESULT=$(benchmark_single_entity "$FIRST_GUID")
        SINGLE_RESULTS="$SINGLE_RESULTS$RESULT"$'\n'
        echo -n "."
    done
    echo ""

    SINGLE_STATS=$(calculate_stats "$SINGLE_RESULTS")
    IFS=',' read -r SINGLE_AVG SINGLE_P50 SINGLE_P95 SINGLE_P99 SINGLE_MIN SINGLE_MAX <<< "$SINGLE_STATS"

    echo "   Results:"
    echo "     Average: ${SINGLE_AVG}ms"
    echo "     P50: ${SINGLE_P50}ms, P95: ${SINGLE_P95}ms, P99: ${SINGLE_P99}ms"
    echo "     Min: ${SINGLE_MIN}ms, Max: ${SINGLE_MAX}ms"

    # ==================== Bulk Entity Benchmark ====================
    echo ""
    echo -e "${BLUE}2. Bulk Entity Retrieval (GET /entity/bulk?guid=...)${NC}"
    BULK_COUNT=$(echo "$BULK_GUIDS" | grep -v '^$' | wc -l | tr -d ' ')
    echo "   Entities per request: $BULK_COUNT"
    echo "   Warmup: $WARMUP_ITERATIONS iterations"

    # Warmup
    for i in $(seq 1 $WARMUP_ITERATIONS); do
        benchmark_bulk_entities "$BULK_GUIDS" > /dev/null
    done

    # Benchmark
    echo "   Benchmark: $BENCHMARK_ITERATIONS iterations"
    BULK_RESULTS=""
    for i in $(seq 1 $BENCHMARK_ITERATIONS); do
        RESULT=$(benchmark_bulk_entities "$BULK_GUIDS")
        BULK_RESULTS="$BULK_RESULTS$RESULT"$'\n'
        echo -n "."
    done
    echo ""

    BULK_STATS=$(calculate_stats "$BULK_RESULTS")
    IFS=',' read -r BULK_AVG BULK_P50 BULK_P95 BULK_P99 BULK_MIN BULK_MAX <<< "$BULK_STATS"

    echo "   Results:"
    echo "     Average: ${BULK_AVG}ms"
    echo "     P50: ${BULK_P50}ms, P95: ${BULK_P95}ms, P99: ${BULK_P99}ms"
    echo "     Min: ${BULK_MIN}ms, Max: ${BULK_MAX}ms"
    echo "     Throughput: $(echo "scale=2; $BULK_COUNT * 1000 / $BULK_AVG" | bc) entities/sec"

    # ==================== Search Benchmark ====================
    echo ""
    echo -e "${BLUE}3. Search API (POST /search/basic)${NC}"
    echo "   Type: $TEST_TYPE, Limit: 100"
    echo "   Warmup: $WARMUP_ITERATIONS iterations"

    # Warmup
    for i in $(seq 1 $WARMUP_ITERATIONS); do
        benchmark_search "$TEST_TYPE" 100 > /dev/null
    done

    # Benchmark
    echo "   Benchmark: $BENCHMARK_ITERATIONS iterations"
    SEARCH_RESULTS=""
    for i in $(seq 1 $BENCHMARK_ITERATIONS); do
        RESULT=$(benchmark_search "$TEST_TYPE" 100)
        SEARCH_RESULTS="$SEARCH_RESULTS$RESULT"$'\n'
        echo -n "."
    done
    echo ""

    SEARCH_STATS=$(calculate_stats "$SEARCH_RESULTS")
    IFS=',' read -r SEARCH_AVG SEARCH_P50 SEARCH_P95 SEARCH_P99 SEARCH_MIN SEARCH_MAX <<< "$SEARCH_STATS"

    echo "   Results:"
    echo "     Average: ${SEARCH_AVG}ms"
    echo "     P50: ${SEARCH_P50}ms, P95: ${SEARCH_P95}ms, P99: ${SEARCH_P99}ms"
    echo "     Min: ${SEARCH_MIN}ms, Max: ${SEARCH_MAX}ms"

    # ==================== Summary ====================
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  Benchmark Summary${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo "Test Configuration:"
    echo "  - Atlas URL: $ATLAS_URL"
    echo "  - Entity Type: $TEST_TYPE"
    echo "  - Iterations: $BENCHMARK_ITERATIONS"
    echo ""
    echo "Results (milliseconds):"
    echo ""
    printf "%-25s %10s %10s %10s %10s\n" "Operation" "Avg" "P50" "P95" "P99"
    printf "%-25s %10s %10s %10s %10s\n" "-------------------------" "----------" "----------" "----------" "----------"
    printf "%-25s %10.2f %10d %10d %10d\n" "Single Entity GET" "$SINGLE_AVG" "$SINGLE_P50" "$SINGLE_P95" "$SINGLE_P99"
    printf "%-25s %10.2f %10d %10d %10d\n" "Bulk Entity GET ($BULK_COUNT)" "$BULK_AVG" "$BULK_P50" "$BULK_P95" "$BULK_P99"
    printf "%-25s %10.2f %10d %10d %10d\n" "Search (limit=100)" "$SEARCH_AVG" "$SEARCH_P50" "$SEARCH_P95" "$SEARCH_P99"
    echo ""

    # Save results to file
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    RESULTS_FILE="/tmp/atlas_benchmark_$TIMESTAMP.csv"
    echo "operation,avg_ms,p50_ms,p95_ms,p99_ms,min_ms,max_ms" > "$RESULTS_FILE"
    echo "single_entity,$SINGLE_AVG,$SINGLE_P50,$SINGLE_P95,$SINGLE_P99,$SINGLE_MIN,$SINGLE_MAX" >> "$RESULTS_FILE"
    echo "bulk_entity_$BULK_COUNT,$BULK_AVG,$BULK_P50,$BULK_P95,$BULK_P99,$BULK_MIN,$BULK_MAX" >> "$RESULTS_FILE"
    echo "search_100,$SEARCH_AVG,$SEARCH_P50,$SEARCH_P95,$SEARCH_P99,$SEARCH_MIN,$SEARCH_MAX" >> "$RESULTS_FILE"

    echo "Results saved to: $RESULTS_FILE"
    echo ""
    echo -e "${YELLOW}To compare before/after:${NC}"
    echo "  1. Run this script BEFORE deploying the changes (baseline)"
    echo "  2. Deploy the TypeRegistry caching changes"
    echo "  3. Run this script AFTER deploying (comparison)"
    echo "  4. Compare the CSV files to see improvement"
}

# Run main
main "$@"
