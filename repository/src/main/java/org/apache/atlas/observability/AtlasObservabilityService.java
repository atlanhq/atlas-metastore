package org.apache.atlas.observability;

import io.micrometer.core.instrument.*;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.springframework.stereotype.Service;
import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Duration;
import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.atlas.repository.store.graph.v2.utils.MDCScope;

@Service
public class AtlasObservabilityService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityStoreV2.class);
    private static final String METRIC_COMPONENT = "atlas_observability_bulk";
    private static final String UNKNOWN_CLIENT_ORIGIN = "unknown";
    private static final String UNKNOWN_AGENT_ID = "unknown";
    private final MeterRegistry meterRegistry;

    private final Map<String, Counter> counterCache = new ConcurrentHashMap<>();
    private final Map<String, Timer> timerCache = new ConcurrentHashMap<>();
    private final Map<String, DistributionSummary> distributionSummaryCache = new ConcurrentHashMap<>();
    
    // Gauges for real-time monitoring
    private final AtomicInteger operationsInProgress = new AtomicInteger(0);
    private final AtomicInteger totalOperations = new AtomicInteger(0);
    
    @Inject
    public AtlasObservabilityService() {
        this(getMeterRegistry());
    }
    
    // Constructor for testing
    AtlasObservabilityService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Initialize gauges for real-time monitoring
        Gauge.builder(METRIC_COMPONENT + "_operations_in_progress", operationsInProgress, AtomicInteger::get)
                .description("Current number of Atlas operations in progress")
                .tag("component", "observability")
                .register(meterRegistry);

        Gauge.builder(METRIC_COMPONENT + "_total_operations", totalOperations, AtomicInteger::get)
                .description("Total number of Atlas operations processed")
                .tag("component", "observability")
                .register(meterRegistry);
    }
    
    private String getMetricKey(String metricName, String... tags) {
        StringBuilder key = new StringBuilder(metricName);
        for (String tag : tags) {
            key.append(":").append(tag);
        }
        return key.toString();
    }
    
    /**
     * Normalizes client origin to prevent null values in Micrometer tags.
     * Micrometer tags cannot have null values.
     */
    private String normalizeClientOrigin(String clientOrigin) {
        return clientOrigin != null ? clientOrigin : UNKNOWN_CLIENT_ORIGIN;
    }
    
    /**
     * Normalizes agent ID by replacing numeric segments with a single placeholder to reduce cardinality.
     * Examples:
     * - "atlan-snowflake-miner-1708514534-cron-1764734400" -> "atlan-snowflake-miner-X-cron-X"
     * - "atlan-dbt-1743112562-cron-1764748800" -> "atlan-dbt-X-cron-X"
     * - "atlan-snowflake-miner-cron" -> "atlan-snowflake-miner-cron" (unchanged)
     * - null -> "unknown"
     */
    private String normalizeAgentId(String agentId) {
        if (agentId == null || agentId.isEmpty()) {
            return UNKNOWN_AGENT_ID;
        }
        // Replace sequences of digits with a single X to normalize timestamps/IDs while preserving structure
        // This reduces cardinality while keeping the agent type pattern visible
        return agentId.replaceAll("\\d+", "X");
    }
    
    public void recordCreateOrUpdateDuration(AtlasObservabilityData data) {
        Timer timer = getOrCreateTimer("duration", data.getXAtlanClientOrigin(), data.getXAtlanAgentId());
        timer.record(data.getDuration(), TimeUnit.MILLISECONDS);
    }
    
    public void recordPayloadSize(AtlasObservabilityData data) {
        DistributionSummary summary = getOrCreateDistributionSummary("payload_size", data.getXAtlanClientOrigin(), data.getXAtlanAgentId());
        summary.record(data.getPayloadAssetSize());
    }
    
    public void recordPayloadBytes(AtlasObservabilityData data) {
        DistributionSummary summary = getOrCreateDistributionSummary("payload_bytes", data.getXAtlanClientOrigin(), data.getXAtlanAgentId());
        summary.record(data.getPayloadRequestBytes());
    }
    
    public void recordArrayRelationships(AtlasObservabilityData data) {
        String clientOrigin = data.getXAtlanClientOrigin();
        String agentId = data.getXAtlanAgentId();
        // Record total count
        DistributionSummary summary = getOrCreateDistributionSummary("array_relationships", clientOrigin, agentId);
        summary.record(data.getTotalArrayRelationships());
        
        // Record individual relationship types and counts
        recordRelationshipMap("relationship_attributes", data.getRelationshipAttributes(), clientOrigin, agentId);
        recordRelationshipMap("append_relationship_attributes", data.getAppendRelationshipAttributes(), clientOrigin, agentId);
        recordRelationshipMap("remove_relationship_attributes", data.getRemoveRelationshipAttributes(), clientOrigin, agentId);
    }
    
    public void recordArrayAttributes(AtlasObservabilityData data) {
        String clientOrigin = data.getXAtlanClientOrigin();
        String agentId = data.getXAtlanAgentId();
        // Record total count
        DistributionSummary summary = getOrCreateDistributionSummary("array_attributes", clientOrigin, agentId);
        summary.record(data.getTotalArrayAttributes());
        // Record individual attribute types and counts
        if (data.getArrayAttributes() != null && !data.getArrayAttributes().isEmpty()) {
            for (Map.Entry<String, Integer> entry : data.getArrayAttributes().entrySet()) {
                Counter counter = getOrCreateCounter("attributes", clientOrigin, agentId,
                    "attribute_name", entry.getKey());
                counter.increment(entry.getValue());
            }
        }
    }
    
    private void recordRelationshipMap(String metricName, Map<String, Integer> relationshipMap, String clientOrigin, String agentId) {
        if (relationshipMap != null && !relationshipMap.isEmpty()) {
            for (Map.Entry<String, Integer> entry : relationshipMap.entrySet()) {
                Counter counter = getOrCreateCounter(metricName, clientOrigin, agentId,
                    "relationship_name", entry.getKey());
                counter.increment(entry.getValue());
            }
        }
    }
    
    
    
    public void recordTimingMetrics(AtlasObservabilityData data) {
        String clientOrigin = data.getXAtlanClientOrigin();
        String agentId = data.getXAtlanAgentId();
        recordTimingMetric("diff_calc", data.getDiffCalcTime(), clientOrigin, agentId);
        recordTimingMetric("lineage_calc", data.getLineageCalcTime(), clientOrigin, agentId);
        recordTimingMetric("validation", data.getValidationTime(), clientOrigin, agentId);
        recordTimingMetric("ingestion", data.getIngestionTime(), clientOrigin, agentId);
    }
    
    private void recordTimingMetric(String operation, long durationMs, String clientOrigin, String agentId) {
        Timer timer = getOrCreateTimer(operation + "_time", clientOrigin, agentId);
        timer.record(durationMs, TimeUnit.MILLISECONDS);
    }
    
    public void recordOperationCount(String operation, String status, String clientOrigin, String agentId) {
        Counter counter = getOrCreateCounter("operations", clientOrigin, agentId, 
            "operation", operation, "status", status);
        counter.increment();
        // Note: totalOperations is incremented by recordOperationEnd/recordOperationFailure, not here
    }
    
    /**
     * Record operation start - increments operations in progress
     */
    public void recordOperationStart(String operation, String clientOrigin, String agentId) {
        operationsInProgress.incrementAndGet();
    }
    
    /**
     * Record operation end - decrements operations in progress and increments total
     */
    public void recordOperationEnd(String operation, String status, String clientOrigin, String agentId) {
        operationsInProgress.decrementAndGet();
        totalOperations.incrementAndGet();
        recordOperationCount(operation, status, clientOrigin, agentId);
    }
    
    /**
     * Record operation failure - decrements operations in progress and records failure
     */
    public void recordOperationFailure(String operation, String errorType, String clientOrigin, String agentId) {
        operationsInProgress.decrementAndGet();
        totalOperations.incrementAndGet();
        recordOperationCount(operation, "failure", clientOrigin, agentId);
        
        // Record specific error type
        Counter errorCounter = getOrCreateCounter("operation_errors", clientOrigin, agentId,
            "operation", operation, "error_type", errorType);
        errorCounter.increment();
    }
    
    /**
     * Log detailed observability data for error cases only.
     * This includes high-cardinality fields like traceId, vertexIds, assetGuids
     * that should NOT be sent to Prometheus.
     */
    public void logErrorDetails(AtlasObservabilityData data, String errorMessage, Throwable throwable) {
        // Use MDCScope for proper MDC management - automatically restores previous state
        try (MDCScope scope = MDCScope.of("filter", "atlas-observability")) {
            // Log structured data for debugging - goes to ClickHouse
            // This includes traceId, vertexIds, assetGuids for error correlation
            LOG.error("Atlas createOrUpdate error: {} | traceId: {} | assetGuids: {} | vertexIds: {} | error: {}", 
                errorMessage,
                data.getTraceId(),
                data.getAssetGuids(),
                data.getVertexIds(),
                throwable != null ? throwable.getMessage() : "unknown");
        }
    }
    
    private Timer getOrCreateTimer(String metricName, String clientOrigin, String agentId, String... additionalTags) {
        String normalizedClientOrigin = normalizeClientOrigin(clientOrigin);
        String normalizedAgentId = normalizeAgentId(agentId);
        String key;
        if (additionalTags != null && additionalTags.length > 0) {
            // Include clientOrigin and agentId in cache key to prevent collisions
            String[] allTags = new String[additionalTags.length + 2];
            allTags[0] = normalizedClientOrigin;
            allTags[1] = normalizedAgentId;
            System.arraycopy(additionalTags, 0, allTags, 2, additionalTags.length);
            key = getMetricKey(metricName, allTags);
        } else {
            key = getMetricKey(metricName, normalizedClientOrigin, normalizedAgentId);
        }
        
        return timerCache.computeIfAbsent(key, k -> {
            Tags tags = Tags.of("client_origin", normalizedClientOrigin, "agent_id", normalizedAgentId);
            if (additionalTags != null && additionalTags.length > 0) {
                tags = tags.and(Tags.of(additionalTags));
            }
            
            return Timer.builder(METRIC_COMPONENT + "_" + metricName)
                    .description("Atlas observability timing metric")
                    .tags(tags)
                    .publishPercentiles(0.5, 0.75, 0.95, 0.99) // Key percentiles
                    .publishPercentileHistogram()
                    .serviceLevelObjectives(
                            // fast operations
                            Duration.ofSeconds(1),      // 1s
                            Duration.ofSeconds(10),     // 10s
                            Duration.ofSeconds(30),     // 30s
                            // intermediate slow operations
                            Duration.ofMinutes(1),      // 1m
                            Duration.ofMinutes(5),      // 5m
                            Duration.ofMinutes(15),     // 15m
                            Duration.ofMinutes(30),     // 30m
                            // slow operations
                            Duration.ofHours(1),        // 1h
                            Duration.ofHours(2),        // 2h
                            Duration.ofHours(4),        // 4h
                            Duration.ofHours(8),        // 8h
                            Duration.ofHours(12)        // 12h
                    )
                    .minimumExpectedValue(Duration.ofSeconds(1))
                    .maximumExpectedValue(Duration.ofHours(24))
                    .register(meterRegistry);
        });
    }

    private Counter getOrCreateCounter(String metricName, String clientOrigin, String agentId, String... additionalTags) {
        String normalizedClientOrigin = normalizeClientOrigin(clientOrigin);
        String normalizedAgentId = normalizeAgentId(agentId);
        String key;
        if (additionalTags != null && additionalTags.length > 0) {
            // Include clientOrigin and agentId in cache key to prevent collisions
            String[] allTags = new String[additionalTags.length + 2];
            allTags[0] = normalizedClientOrigin;
            allTags[1] = normalizedAgentId;
            System.arraycopy(additionalTags, 0, allTags, 2, additionalTags.length);
            key = getMetricKey(metricName, allTags);
        } else {
            key = getMetricKey(metricName, normalizedClientOrigin, normalizedAgentId);
        }
        
        return counterCache.computeIfAbsent(key, k -> {
            Tags tags = Tags.of("client_origin", normalizedClientOrigin, "agent_id", normalizedAgentId);
            if (additionalTags != null && additionalTags.length > 0) {
                tags = tags.and(Tags.of(additionalTags));
            }

            return Counter.builder(METRIC_COMPONENT + "_" + metricName)
                    .description("Atlas observability counter metric")
                    .tags(tags)
                    .register(meterRegistry);
        });
    }

    
    private DistributionSummary getOrCreateDistributionSummary(String metricName, String clientOrigin, String agentId, String... additionalTags) {
        String normalizedClientOrigin = normalizeClientOrigin(clientOrigin);
        String normalizedAgentId = normalizeAgentId(agentId);
        String key;
        if (additionalTags != null && additionalTags.length > 0) {
            // Include clientOrigin and agentId in cache key to prevent collisions
            String[] allTags = new String[additionalTags.length + 2];
            allTags[0] = normalizedClientOrigin;
            allTags[1] = normalizedAgentId;
            System.arraycopy(additionalTags, 0, allTags, 2, additionalTags.length);
            key = getMetricKey(metricName, allTags);
        } else {
            key = getMetricKey(metricName, normalizedClientOrigin, normalizedAgentId);
        }
        
        return distributionSummaryCache.computeIfAbsent(key, k -> {
            Tags tags = Tags.of("client_origin", normalizedClientOrigin, "agent_id", normalizedAgentId);
            if (additionalTags != null && additionalTags.length > 0) {
                tags = tags.and(Tags.of(additionalTags));
            }
            
            // Configure SLOs based on metric type
            DistributionSummary.Builder builder = DistributionSummary.builder(METRIC_COMPONENT + "_" + metricName)
                    .description("Atlas observability distribution metric")
                    .tags(tags)
                    .publishPercentiles(0.5, 0.75, 0.95, 0.99) // Key percentiles
                    .publishPercentileHistogram();
            
            // Configure SLOs based on metric type
            if (metricName.contains("payload_bytes")) {
                // Payload bytes SLOs (bytes)
                builder.serviceLevelObjectives(
                    1024.0,           // 1KB
                    10240.0,          // 10KB
                    102400.0,         // 100KB
                    1048576.0,        // 1MB
                    10485760.0,       // 10MB
                    104857600.0,      // 100MB
                    1073741824.0,     // 1GB
                    10737418240.0     // 10GB
                )
                .minimumExpectedValue(1.0)
                .maximumExpectedValue(107374182400.0) // 100GB
                .baseUnit("bytes");
            } else if (metricName.contains("payload_size") || metricName.contains("array")) {
                // Payload size and array metrics SLOs (count)
                builder.serviceLevelObjectives(
                    1.0,              // 1 item
                    10.0,             // 10 items
                    100.0,            // 100 items
                    1000.0,           // 1K items
                    10000.0,          // 10K items
                    100000.0,         // 100K items
                    1000000.0,        // 1M items
                    10000000.0        // 10M items
                )
                .minimumExpectedValue(1.0)
                .maximumExpectedValue(100000000.0) // 100M items
                .baseUnit("items");
            } else {
                // Default SLOs for other metrics
                builder.serviceLevelObjectives(
                    1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0
                )
                .minimumExpectedValue(1.0)
                .maximumExpectedValue(10000000.0);
            }
            
            return builder.register(meterRegistry);
        });
    }
    
    
}