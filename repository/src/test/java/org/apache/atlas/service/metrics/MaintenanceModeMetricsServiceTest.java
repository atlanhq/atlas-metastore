package org.apache.atlas.service.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MaintenanceModeMetricsService.
 */
class MaintenanceModeMetricsServiceTest {

    private MaintenanceModeMetricsService metricsService;
    private MeterRegistry meterRegistry;

    @BeforeEach
    void setup() {
        meterRegistry = new SimpleMeterRegistry();
        metricsService = new MaintenanceModeMetricsService(meterRegistry);
        metricsService.init();
    }

    @Test
    void testMaintenanceModeGaugeIsRegistered() {
        Gauge gauge = meterRegistry.find("atlas_maintenance_mode_enabled")
                .tag("service", "atlas-metastore")
                .tag("component", "config")
                .gauge();

        assertNotNull(gauge, "Maintenance mode gauge should be registered");
    }

    @Test
    void testDynamicConfigStoreGaugeIsRegistered() {
        Gauge gauge = meterRegistry.find("atlas_dynamic_config_store_enabled")
                .tag("service", "atlas-metastore")
                .tag("component", "config")
                .gauge();

        assertNotNull(gauge, "DynamicConfigStore enabled gauge should be registered");
    }

    @Test
    void testMaintenanceModeGaugeHasCorrectTags() {
        Gauge gauge = meterRegistry.find("atlas_maintenance_mode_enabled")
                .tag("service", "atlas-metastore")
                .tag("component", "config")
                .gauge();

        assertNotNull(gauge, "Gauge should have service and component tags");
    }

    @Test
    void testMaintenanceModeGaugeDefaultValue() {
        // In test environment, maintenance mode defaults to disabled (0.0)
        Gauge gauge = meterRegistry.find("atlas_maintenance_mode_enabled").gauge();

        assertNotNull(gauge);
        assertEquals(0.0, gauge.value(), 0.01, 
            "Gauge should return 0.0 when maintenance mode is disabled (default)");
    }

    @Test
    void testDynamicConfigStoreGaugeDefaultValue() {
        // In test environment, DynamicConfigStore is not enabled (0.0)
        Gauge gauge = meterRegistry.find("atlas_dynamic_config_store_enabled").gauge();

        assertNotNull(gauge);
        assertEquals(0.0, gauge.value(), 0.01, 
            "Gauge should return 0.0 when DynamicConfigStore is not enabled");
    }

    @Test
    void testMaintenanceModeGaugeHasDescription() {
        Gauge gauge = meterRegistry.find("atlas_maintenance_mode_enabled").gauge();

        assertNotNull(gauge);
        assertNotNull(gauge.getId().getDescription(), "Gauge should have a description");
        assertTrue(gauge.getId().getDescription().contains("maintenance mode"),
            "Description should mention maintenance mode");
    }

    @Test
    void testDynamicConfigStoreGaugeHasDescription() {
        Gauge gauge = meterRegistry.find("atlas_dynamic_config_store_enabled").gauge();

        assertNotNull(gauge);
        assertNotNull(gauge.getId().getDescription(), "Gauge should have a description");
        assertTrue(gauge.getId().getDescription().contains("DynamicConfigStore"),
            "Description should mention DynamicConfigStore");
    }
}
