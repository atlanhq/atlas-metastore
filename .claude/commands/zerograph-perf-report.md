# ZeroGraph Performance Comparison Report

Generate a before/after performance comparison report for ZeroGraph migration tenants. Produces a single-page HTML report suitable for Slack screenshots.

## Usage

**Ring 1 (embedded tenants — no args needed):**
```
/zerograph-perf-report
```

**Custom tenants:**
```
/zerograph-perf-report tenant1:before_start:before_end:after_start tenant2:before_start:before_end:after_start
```
Example: `/zerograph-perf-report mytenantprod01:2026-04-01:2026-04-08:2026-04-08`

After end is always "now" (current time).

## Ring 1 Embedded Tenants & Windows

If no arguments are provided, use these Ring 1 tenants with their fixed before windows and after window = last Thursday to now:

| Tenant | Before Start | Before End | After Start |
|--------|-------------|-----------|-------------|
| hellofresh-sandbox | 2026-03-25 07:00 UTC | 2026-03-27 07:00 UTC | 2026-03-27 07:00 UTC |
| alcon02 | 2026-03-12 00:00 UTC | 2026-03-19 08:30 UTC | 2026-03-19 08:30 UTC |
| hyatthot02 | 2026-03-16 00:00 UTC | 2026-03-23 00:00 UTC | 2026-03-23 00:00 UTC |
| mesoljap01 | 2026-03-12 00:00 UTC | 2026-03-19 00:00 UTC | 2026-03-19 00:00 UTC |

For Ring 1, the after end is always "now" (current UTC time). This gives the longest possible ZeroGraph observation window.

## Step 1: Parse Arguments

If arguments are provided, parse each as `tenant:before_start:before_end:after_start`. After end = now.

If no arguments, use the Ring 1 table above.

State the windows clearly before pulling any data:
```
Before (JanusGraph): {before_start} → {before_end}
After (ZeroGraph): {after_start} → now
```

## Step 2: Pull Metrics (all tenants in parallel)

For EACH tenant, pull these for BOTH before and after windows:

### 2a. HTTP Performance (3 endpoint-filtered queries per window)

Use `analyze_http_performance` MCP tool:
- `endpointFilter=entity/bulk` — bulk ingestion latency
- `endpointFilter=search` — search/indexsearch latency
- `includeErrorAnalysis=false` (keep it fast)

From the results extract:
- **Bulk read-heavy**: the `atlas-ratelimited.../api/atlas/v2/entity/bulk` with lower avg (high volume)
- **Bulk write-heavy**: the endpoint with higher avg (lower volume, often 500ms+)
- **Internal search**: `atlas-ratelimited.../api/meta/search` (ratelimited path)
- **External indexsearch**: `{tenant}.atlan.com/api/meta/search/indexsearch` (external path)

### 2b. Elasticsearch Direct Metrics

Use `analyze_elasticsearch_performance` with `slowQueryThreshold=500`:
- Extract: avg query time, p99 query time, slow query count/percentage

### 2c. Prometheus (JVM Heap + CPU)

Query Grafana Prometheus API:
```
API: POST https://observability.atlan.com/api/ds/query
Auth: Bearer $GRAFANA_API_KEY (from environment or ~/.claude/projects config)
Datasource UID: cd27b984-0b24-4adf-8bf8-1711c6b21061
```

Note: The Grafana API key is stored in the project memory (`staging-keycloak.md`) or can be set via `GRAFANA_API_KEY` environment variable. Do NOT hardcode it.

Metrics (replace TENANT with clusterName):
```
Heap %: 100 * sum(atlas_memory_HeapMemoryUsage_used{clusterName="TENANT"}) by (pod) / sum(atlas_memory_HeapMemoryUsage_max{clusterName="TENANT"}) by (pod)
CPU %:  100 * max(atlas_metastore_process_cpu_usage{clusterName="TENANT"}) by (pod)
```

Use `intervalMs=3600000` and compute avg/min/max from returned values.

For before window, only query Prometheus for Ring 1 (we have the baselines). For custom tenants, query both windows.

### Ring 1 Before Baselines (JanusGraph — cached, don't re-query)

These are fixed baselines from the original JanusGraph measurements:

| Metric | hellofresh | alcon02 | hyatthot02 | mesoljap01 |
|--------|-----------|---------|------------|------------|
| Bulk write-heavy avg | 2,760 ms | 1,305 ms | 3,066 ms | 1,563 ms |
| Bulk read p99 | 1,373 ms | 2,037 ms | 866 ms | 159 ms |
| Internal search p99 | 111 ms | 149 ms | 102 ms | 554 ms |
| External indexsearch avg | 484 ms | 372 ms | N/A | 68 ms |
| ES avg query | 74.8 ms | 6.7 ms | 13.6 ms | 8.4 ms |
| ES p99 query | 1,797 ms | 95 ms | 89 ms | 248 ms |
| HTTP error rate | 12.08% | 0.83% | 0.00% | 0.01% |
| Heap avg % | 29% | 15% | 9% | 10% |
| CPU avg (highest pod) | 55% | 3% | 34% | 0.2% |

## Step 3: Build Consolidated HTML Report

Generate a single HTML file at `zerograph-consolidated-report.html` with this structure:

### Header
- Title: "ZeroGraph Consolidated Report — Ring 1" (or "Custom Tenants" if args provided)
- Badge: "N/N PASSED" based on assessment
- Subtitle: Before/After window description, report date

### Table
Single table with columns: Metric | Tenant1 | Tenant2 | ... | Trend

Each cell shows `before → after` values in monospace font.
Trend column shows a color-coded badge.

### Sections (in order):
1. **Bulk Ingestion** — write-heavy avg, bulk read p99
2. **Search (indexsearch)** — internal search p99, external indexsearch avg
3. **Elasticsearch Queries** — ES avg query time, ES p99 query time
4. **Errors** — HTTP error rate
5. **JVM Heap** — heap avg %
6. **CPU** — process CPU avg (highest pod)

### Badge Colors
- Dark green (`#38a169`): Major improvement (>2x better)
- Green (`#48bb78`): Improvement
- Orange (`#ed8936`): Regression or needs attention
- Gray (`#a0aec0`): Neutral / mixed

### Callouts
- **Orange callout**: "Updates since last report" — note any regressions or changes from previous run
- **Green callout**: Bottom-line summary — "All N tenants passed. Bulk ingestion Nx faster. ..."

### Styling
- Max-width 960px, white card with shadow
- Dark gradient header (#1a1a2e → #16213e)
- 12px table font, 10px section headers
- Monospace values (SF Mono / Fira Code)
- Compact enough for a single browser screenshot

## Step 4: Open Report

Always run `open {report_file}` so the user can screenshot immediately.

## Step 5: Assessment Logic

**PASSED** if:
- Bulk ingestion avg is same or better than JG
- Internal search p99 is same or better than JG
- Heap is lower than JG
- No critical error rate increase (>5% absolute increase)

**NEEDS ATTENTION** if any:
- Bulk or search significantly slower (>50% regression)
- Error rate increased by >5%
- Heap increased

## Notes

- For Ring 1, NEVER re-query the before baselines — they're fixed JanusGraph measurements
- Always pull the after window fresh (it extends to "now")
- If a tenant has no calls for an endpoint (e.g., no lineage calls), show "N/A"
- Low sample count (<100 calls) means p99 is unreliable — note in the callout
- The report should fit in ONE screenshot at normal browser zoom
