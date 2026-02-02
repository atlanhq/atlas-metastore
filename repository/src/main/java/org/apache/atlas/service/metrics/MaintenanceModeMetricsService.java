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
 * Service for exporting maintenance mode status as Prometheus metrics.
 * 
 * Exports two metrics:
 * 
 * 1. atlas_maintenance_mode_enabled - Whether maintenance mode is enabled (1=yes, 0=no)
 *    Priority order for reading:
 *      - DynamicConfigStore (if enabled)
 *      - AtlasConfiguration (fallback)
 * 
 * 2. atlas_dynamic_config_store_enabled - Whether DynamicConfigStore is the source (1=yes, 0=no)
 *    This helps identify which source is being used for the maintenance mode value.
 * 
 * The metrics are automatically scraped via /api/atlas/admin/metrics/prometheus endpoint
 * and flow to VictoriaMetrics through the existing Telegraf/VMAgent pipeline.
 */
@Service
public class MaintenanceModeMetricsService {
    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceModeMetricsService.class);
    
    private static final String MAINTENANCE_MODE_METRIC = "atlas_maintenance_mode_enabled";
    private static final String CONFIG_STORE_METRIC = "atlas_dynamic_config_store_enabled";
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
        registerMetrics();
        LOG.info("MaintenanceModeMetricsService initialized - metrics registered");
    }
    
    /**
     * Register all metrics.
     */
    private void registerMetrics() {
        try {
            // Metric 1: Maintenance mode enabled/disabled
            Gauge.builder(MAINTENANCE_MODE_METRIC, this::getMaintenanceModeValue)
                    .description("Whether maintenance mode is currently enabled (1=enabled, 0=disabled)")
                    .tag(SERVICE_TAG, SERVICE_NAME)
                    .tag(COMPONENT_TAG, COMPONENT_NAME)
                    .register(meterRegistry);
            
            // Metric 2: Is DynamicConfigStore the source?
            Gauge.builder(CONFIG_STORE_METRIC, this::isDynamicConfigStoreEnabled)
                    .description("Whether DynamicConfigStore is enabled as the config source (1=yes, 0=no)")
                    .tag(SERVICE_TAG, SERVICE_NAME)
                    .tag(COMPONENT_TAG, COMPONENT_NAME)
                    .register(meterRegistry);
            
            LOG.debug("Registered maintenance mode metrics");
        } catch (Exception e) {
            LOG.error("Failed to register metrics", e);
        }
    }
    
    /**
     * Get the current maintenance mode status.
     * 
     * Priority:
     *   1. DynamicConfigStore (if enabled)
     *   2. AtlasConfiguration (fallback)
     * 
     * @return 1.0 if maintenance mode is enabled, 0.0 otherwise
     */
    private double getMaintenanceModeValue() {
        // Priority 1: Try DynamicConfigStore first
        try {
            if (DynamicConfigStore.isEnabled()) {
                boolean enabled = DynamicConfigStore.getConfigAsBoolean(ConfigKey.MAINTENANCE_MODE.getKey());
                return enabled ? 1.0 : 0.0;
            }
        } catch (Exception e) {
            LOG.debug("Error reading from DynamicConfigStore, falling back to AtlasConfiguration", e);
        }
        
        // Priority 2: Fall back to AtlasConfiguration
        try {
            boolean enabled = AtlasConfiguration.ATLAS_MAINTENANCE_MODE.getBoolean();
            return enabled ? 1.0 : 0.0;
        } catch (Exception e) {
            LOG.warn("Failed to read maintenance mode from AtlasConfiguration, returning 0.0", e);
            return 0.0;
        }
    }
    
    /**
     * Check if DynamicConfigStore is enabled.
     * 
     * @return 1.0 if DynamicConfigStore is enabled, 0.0 otherwise
     */
    private double isDynamicConfigStoreEnabled() {
        try {
            return DynamicConfigStore.isEnabled() ? 1.0 : 0.0;
        } catch (Exception e) {
            LOG.debug("Error checking DynamicConfigStore.isEnabled()", e);
            return 0.0;
        }
    }
}
