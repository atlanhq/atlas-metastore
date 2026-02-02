package org.apache.atlas.service.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.service.config.ConfigKey;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;

/**
 * Service for exporting maintenance mode status as a Prometheus metric.
 * 
 * This service registers a gauge metric that exposes the current maintenance mode status.
 * The metric value is:
 *   - 1.0 when maintenance mode is enabled
 *   - 0.0 when maintenance mode is disabled
 * 
 * The metric is automatically scraped via /api/atlas/admin/metrics/prometheus endpoint
 * and flows to VictoriaMetrics through the existing Telegraf/VMAgent pipeline.
 */
@Service
public class MaintenanceModeMetricsService {
    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceModeMetricsService.class);
    
    private static final String METRIC_NAME = "atlas_maintenance_mode_enabled";
    private static final String SERVICE_TAG = "service";
    private static final String SERVICE_NAME = "atlas-metastore";
    private static final String COMPONENT_TAG = "component";
    private static final String COMPONENT_NAME = "config";
    
    private final MeterRegistry meterRegistry;
    
    @Inject
    public MaintenanceModeMetricsService() {
        this(getMeterRegistry());
    }
    
    // Constructor for testing
    MaintenanceModeMetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }
    
    @PostConstruct
    public void init() {
        registerMaintenanceModeGauge();
        LOG.info("MaintenanceModeMetricsService initialized - maintenance mode metric registered");
    }
    
    /**
     * Register the maintenance mode gauge metric.
     * 
     * The gauge uses a supplier function that reads maintenance mode status on each scrape.
     * This ensures the metric always reflects the current maintenance mode status.
     */
    private void registerMaintenanceModeGauge() {
        try {
            Gauge.builder(METRIC_NAME, this::getMaintenanceModeValue)
                    .description("Whether maintenance mode is currently enabled (1=enabled, 0=disabled)")
                    .tag(SERVICE_TAG, SERVICE_NAME)
                    .tag(COMPONENT_TAG, COMPONENT_NAME)
                    .register(meterRegistry);
            
            LOG.debug("Registered maintenance mode gauge metric: {}", METRIC_NAME);
        } catch (Exception e) {
            LOG.error("Failed to register maintenance mode gauge metric", e);
        }
    }
    
    /**
     * Get the current maintenance mode status as a numeric value.
     * 
     * Uses the same logic as TaskQueueWatcher, EntityGraphMapper, and ActiveServerFilter
     * to ensure consistent behavior across the codebase.
     * 
     * @return 1.0 if maintenance mode is enabled, 0.0 otherwise
     */
    private double getMaintenanceModeValue() {
        return isMaintenanceModeEnabled() ? 1.0 : 0.0;
    }
    
    /**
     * Check if maintenance mode is enabled.
     * 
     * Logic mirrors TaskQueueWatcher, EntityGraphMapper, and ActiveServerFilter:
     * 1. If DynamicConfigStore is enabled, read from Cassandra cache
     * 2. Otherwise, fall back to static AtlasConfiguration
     * 
     * @return true if maintenance mode is enabled, false otherwise
     */
    private boolean isMaintenanceModeEnabled() {
        try {
            if (DynamicConfigStore.isEnabled()) {
                return DynamicConfigStore.getConfigAsBoolean(ConfigKey.MAINTENANCE_MODE.getKey());
            }
        } catch (Exception e) {
            LOG.debug("Error checking DynamicConfigStore for maintenance mode, falling back to static config", e);
        }
        return AtlasConfiguration.ATLAS_MAINTENANCE_MODE.getBoolean();
    }
}
