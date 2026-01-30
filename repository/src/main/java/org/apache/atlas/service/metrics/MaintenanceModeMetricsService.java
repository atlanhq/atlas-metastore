package org.apache.atlas.service.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;

/**
 * Service for exporting maintenance mode status as a Prometheus metric.
 * 
 * This service registers a gauge metric that exposes the current maintenance mode status.
 * The metric value is:
 *   - 1 when maintenance mode is enabled
 *   - 0 when maintenance mode is disabled
 * 
 * The metric is automatically scraped via /api/atlas/admin/metrics/prometheus endpoint
 * and flows to VictoriaMetrics through the existing Telegraf/VMAgent pipeline.
 * 
 * Performance: Uses AtomicInteger for O(1) reads. Call {@link #updateMaintenanceModeMetric(boolean)}
 * when maintenance mode changes to update the metric value.
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
    
    // AtomicInteger for O(1) gauge reads - updated when maintenance mode changes
    private final AtomicInteger maintenanceModeValue = new AtomicInteger(0);
    
    // Singleton instance for static access
    private static volatile MaintenanceModeMetricsService instance;
    
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
        instance = this;
        registerMaintenanceModeGauge();
        // Initialize with current value
        refreshMaintenanceModeValue();
        LOG.info("MaintenanceModeMetricsService initialized - maintenance mode metric registered");
    }
    
    /**
     * Register the maintenance mode gauge metric.
     * 
     * Uses AtomicInteger for O(1) reads on each Prometheus scrape.
     */
    private void registerMaintenanceModeGauge() {
        try {
            Gauge.builder(METRIC_NAME, maintenanceModeValue, AtomicInteger::get)
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
     * Update the maintenance mode metric value.
     * Call this method when maintenance mode is enabled or disabled.
     * 
     * @param enabled true if maintenance mode is enabled, false otherwise
     */
    public void updateMaintenanceModeMetric(boolean enabled) {
        int newValue = enabled ? 1 : 0;
        int oldValue = maintenanceModeValue.getAndSet(newValue);
        if (oldValue != newValue) {
            LOG.info("Maintenance mode metric updated: {} -> {}", oldValue, newValue);
        }
    }
    
    /**
     * Refresh the metric value from DynamicConfigStore.
     * Called during initialization and can be called to sync the metric value.
     */
    public void refreshMaintenanceModeValue() {
        try {
            boolean enabled = DynamicConfigStore.isMaintenanceModeEnabled();
            maintenanceModeValue.set(enabled ? 1 : 0);
            LOG.debug("Maintenance mode metric refreshed: {}", enabled);
        } catch (Exception e) {
            LOG.warn("Failed to refresh maintenance mode metric, keeping current value", e);
        }
    }
    
    /**
     * Static method to update maintenance mode metric.
     * Can be called from DynamicConfigStore when maintenance mode changes.
     * 
     * @param enabled true if maintenance mode is enabled, false otherwise
     */
    public static void updateMetric(boolean enabled) {
        MaintenanceModeMetricsService svc = instance;
        if (svc != null) {
            svc.updateMaintenanceModeMetric(enabled);
        }
    }
}
