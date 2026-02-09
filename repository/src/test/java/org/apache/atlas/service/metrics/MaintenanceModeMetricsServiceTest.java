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
        // Then - verify the gauge is registered with correct name
        Gauge gauge = meterRegistry.find("atlas_maintenance_mode_enabled")
                .tag("service", "atlas-metastore")
                .tag("component", "config")
                .gauge();

        assertNotNull(gauge, "Maintenance mode gauge should be registered");
    }

    @Test
    void testMaintenanceModeGaugeHasCorrectTags() {
        // Then - verify all expected tags are present
        Gauge gauge = meterRegistry.find("atlas_maintenance_mode_enabled")
                .tag("service", "atlas-metastore")
                .gauge();

        assertNotNull(gauge, "Gauge should have service tag");

        gauge = meterRegistry.find("atlas_maintenance_mode_enabled")
                .tag("component", "config")
                .gauge();

        assertNotNull(gauge, "Gauge should have component tag");
    }

    @Test
    void testMaintenanceModeGaugeValueWhenDynamicConfigStoreNotInitialized() {
        // When DynamicConfigStore is not initialized (test environment),
        // the gauge should return 0.0 (graceful handling)
        Gauge gauge = meterRegistry.find("atlas_maintenance_mode_enabled")
                .tag("service", "atlas-metastore")
                .tag("component", "config")
                .gauge();

        assertNotNull(gauge);
        // When DynamicConfigStore is not initialized, it should return 0.0
        assertEquals(0.0, gauge.value(), 0.01, 
            "Gauge should return 0.0 when DynamicConfigStore is not initialized");
    }

    @Test
    void testMaintenanceModeGaugeHasDescription() {
        // Then - verify the gauge has a description
        Gauge gauge = meterRegistry.find("atlas_maintenance_mode_enabled")
                .gauge();

        assertNotNull(gauge);
        assertNotNull(gauge.getId().getDescription(), "Gauge should have a description");
        assertTrue(gauge.getId().getDescription().contains("maintenance mode"),
            "Description should mention maintenance mode");
    }
}
