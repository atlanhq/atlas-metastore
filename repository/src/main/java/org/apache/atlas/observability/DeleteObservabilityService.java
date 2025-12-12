package org.apache.atlas.observability;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.springframework.stereotype.Service;
import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;

@Service
public class DeleteObservabilityService {

    private static final String METRIC_COMPONENT = "atlas_observability_bulk";
    private static final String UNKNOWN_CLIENT_ORIGIN = "unknown";
    private final MeterRegistry meterRegistry;

    private final Map<String, Counter> counterCache = new ConcurrentHashMap<>();
    private final Map<String, Timer> timerCache = new ConcurrentHashMap<>();
    private final Map<String, DistributionSummary> distributionSummaryCache = new ConcurrentHashMap<>();

    @Inject
    public DeleteObservabilityService() {
        this(getMeterRegistry());
    }

    DeleteObservabilityService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void record(DeleteObservabilityData data) {
        if (data == null) return;

        recordDeleteDuration(data);
        recordBulkDeleteEntitiesCount(data.getDeleteEntitiesCount(),
            data.getXAtlanClientOrigin(), data.getXAtlanAgentId(), data.getImplementation());
            
        if (data.getOperationStatus() != null) {
            recordBulkOperationsTotal("delete_bulk", data.getOperationStatus(),
                data.getXAtlanClientOrigin(), data.getXAtlanAgentId(), data.getImplementation());
        }

        if (data.getErrorType() != null) {
            recordBulkOperationErrors("delete_bulk", data.getErrorType(),
                data.getXAtlanClientOrigin(), data.getXAtlanAgentId(), data.getImplementation());
        }
    }

    public void recordDeleteDuration(DeleteObservabilityData data) {
        String clientOrigin = normalizeClientOrigin(data.getXAtlanClientOrigin());
        String agentId = normalizeAgentId(data.getXAtlanAgentId());
        String implementation = data.getImplementation();

        Timer timer = getOrCreateTimer("delete_duration", clientOrigin,
            "x_atlan_agent_id", agentId,
            "implementation", implementation,
            "operation", "delete_bulk");
        timer.record(data.getDuration(), TimeUnit.MILLISECONDS);

        // Also record step timings
        recordDeleteStepTime("lookup_vertices", data.getLookupVerticesTime(), clientOrigin, agentId, implementation);
        recordDeleteStepTime("authorization_check", data.getAuthorizationCheckTime(), clientOrigin, agentId, implementation);
        recordDeleteStepTime("preprocessing", data.getPreprocessingTime(), clientOrigin, agentId, implementation);
        recordDeleteStepTime("remove_has_lineage", data.getRemoveHasLineageTime(), clientOrigin, agentId, implementation);
        recordDeleteStepTime("delete_entities", data.getDeleteEntitiesTime(), clientOrigin, agentId, implementation);
        recordDeleteStepTime("delete_type_vertex", data.getDeleteTypeVertexTime(), clientOrigin, agentId, implementation);
        recordDeleteStepTime("term_cleanup", data.getTermCleanupTime(), clientOrigin, agentId, implementation);
        recordDeleteStepTime("notifications", data.getNotificationTime(), clientOrigin, agentId, implementation);
    }

    private void recordDeleteStepTime(String stepName, long duration, String clientOrigin, String agentId, String implementation) {
        if (duration > 0) {
            Timer timer = getOrCreateTimer("delete_" + stepName + "_time", clientOrigin,
                "x_atlan_agent_id", agentId,
                "implementation", implementation,
                "operation", "delete_bulk");
            timer.record(duration, TimeUnit.MILLISECONDS);
        }
    }

    public void recordBulkDeleteEntitiesCount(int count, String clientOrigin, String agentId, String implementation) {
        String normalizedClientOrigin = normalizeClientOrigin(clientOrigin);
        String normalizedAgentId = normalizeAgentId(agentId);

        DistributionSummary summary = getOrCreateDistributionSummary("delete_entities_count", normalizedClientOrigin,
            "x_atlan_agent_id", normalizedAgentId,
            "implementation", implementation,
            "operation", "delete_bulk");
        summary.record(count);
    }

    public void recordBulkOperationErrors(String operation, String errorType, String clientOrigin, String agentId, String implementation) {
        String normalizedClientOrigin = normalizeClientOrigin(clientOrigin);
        String normalizedAgentId = normalizeAgentId(agentId);

        Counter counter = getOrCreateCounter("operation_errors", normalizedClientOrigin,
            "operation", operation,
            "error_type", errorType,
            "x_atlan_agent_id", normalizedAgentId,
            "implementation", implementation);
        counter.increment();
    }

    public void recordBulkOperationsTotal(String operation, String status, String clientOrigin, String agentId, String implementation) {
        String normalizedClientOrigin = normalizeClientOrigin(clientOrigin);
        String normalizedAgentId = normalizeAgentId(agentId);

        Counter counter = getOrCreateCounter("operations", normalizedClientOrigin,
            "operation", operation,
            "status", status,
            "x_atlan_agent_id", normalizedAgentId,
            "implementation", implementation);
        counter.increment();
    }

    private String normalizeAgentId(String agentId) {
        return agentId != null ? agentId : "unknown";
    }

    private String getMetricKey(String metricName, String... tags) {
        StringBuilder key = new StringBuilder(metricName);
        for (String tag : tags) {
            key.append(":").append(tag);
        }
        return key.toString();
    }

    private String normalizeClientOrigin(String clientOrigin) {
        return clientOrigin != null ? clientOrigin : UNKNOWN_CLIENT_ORIGIN;
    }

    private Timer getOrCreateTimer(String metricName, String clientOrigin, String... additionalTags) {
        String normalizedClientOrigin = normalizeClientOrigin(clientOrigin);
        String key;
        if (additionalTags != null && additionalTags.length > 0) {
            // Include clientOrigin in cache key to prevent collisions
            String[] allTags = new String[additionalTags.length + 1];
            allTags[0] = normalizedClientOrigin;
            System.arraycopy(additionalTags, 0, allTags, 1, additionalTags.length);
            key = getMetricKey(metricName, allTags);
        } else {
            key = getMetricKey(metricName, normalizedClientOrigin);
        }

        return timerCache.computeIfAbsent(key, k -> {
            Tags tags = Tags.of("client_origin", normalizedClientOrigin);
            if (additionalTags != null && additionalTags.length > 0) {
                tags = tags.and(Tags.of(additionalTags));
            }

            return Timer.builder(METRIC_COMPONENT + "_" + metricName)
                    .description("Atlas observability timing metric")
                    .tags(tags)
                    .publishPercentiles(0.5, 0.75, 0.95, 0.99) // Key percentiles
                    .publishPercentileHistogram()
                    .serviceLevelObjectives(
                            // Short tasks
                            Duration.ofSeconds(1),      // 1s
                            Duration.ofSeconds(10),     // 10s
                            Duration.ofSeconds(30),     // 30s
                            // Medium tasks
                            Duration.ofMinutes(1),      // 1m
                            Duration.ofMinutes(5),      // 5m
                            Duration.ofMinutes(15),     // 15m
                            Duration.ofMinutes(30),     // 30m
                            // Long tasks
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

    private Counter getOrCreateCounter(String metricName, String clientOrigin, String... additionalTags) {
        String normalizedClientOrigin = normalizeClientOrigin(clientOrigin);
        String key;
        if (additionalTags != null && additionalTags.length > 0) {
            String[] allTags = new String[additionalTags.length + 1];
            allTags[0] = normalizedClientOrigin;
            System.arraycopy(additionalTags, 0, allTags, 1, additionalTags.length);
            key = getMetricKey(metricName, allTags);
        } else {
            key = getMetricKey(metricName, normalizedClientOrigin);
        }

        return counterCache.computeIfAbsent(key, k -> {
            Tags tags = Tags.of("client_origin", normalizedClientOrigin);
            if (additionalTags != null && additionalTags.length > 0) {
                tags = tags.and(Tags.of(additionalTags));
            }

            return Counter.builder(METRIC_COMPONENT + "_" + metricName)
                    .description("Atlas observability counter metric")
                    .tags(tags)
                    .register(meterRegistry);
        });
    }

    private DistributionSummary getOrCreateDistributionSummary(String metricName, String clientOrigin, String... additionalTags) {
        String normalizedClientOrigin = normalizeClientOrigin(clientOrigin);
        String key;
        if (additionalTags != null && additionalTags.length > 0) {
            String[] allTags = new String[additionalTags.length + 1];
            allTags[0] = normalizedClientOrigin;
            System.arraycopy(additionalTags, 0, allTags, 1, additionalTags.length);
            key = getMetricKey(metricName, allTags);
        } else {
            key = getMetricKey(metricName, normalizedClientOrigin);
        }

        return distributionSummaryCache.computeIfAbsent(key, k -> {
            Tags tags = Tags.of("client_origin", normalizedClientOrigin);
            if (additionalTags != null && additionalTags.length > 0) {
                tags = tags.and(Tags.of(additionalTags));
            }

            return DistributionSummary.builder(METRIC_COMPONENT + "_" + metricName)
                    .description("Atlas observability distribution metric")
                    .tags(tags)
                    .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                    .publishPercentileHistogram()
                    .serviceLevelObjectives(
                        1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0
                    )
                    .minimumExpectedValue(1.0)
                    .maximumExpectedValue(10000000.0)
                    .register(meterRegistry);
        });
    }
}
