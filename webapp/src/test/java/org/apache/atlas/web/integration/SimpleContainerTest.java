package org.apache.atlas.web.integration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleContainerTest {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleContainerTest.class);

    @BeforeAll
    public static void setup() {
        System.setProperty("docker.host", "unix://" + System.getProperty("user.home") + "/.colima/default/docker.sock");

        // Increase timeout for Ryuk connection
        System.setProperty("testcontainers.ryuk.container.timeout", "120");

        // Enable debug logging
        System.setProperty("org.testcontainers.shaded.org.slf4j.simpleLogger.defaultLogLevel", "debug");
    }

    @Test
    public void testSimpleContainer() {
        LOG.info("Starting simple container test");
        LOG.info("Docker host: {}", System.getenv("DOCKER_HOST"));
        LOG.info("Testcontainers Ryuk disabled: {}", System.getProperty("testcontainers.ryuk.disabled"));

        try (GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse("hello-world:latest"))
                .withCommand("echo", "Hello from container")) {

            container.start();
            String logs = container.getLogs();
            LOG.info("Container ID: {}", container.getContainerId());
            LOG.info("Container logs: {}", logs);
            assertTrue(container.isRunning(), "Container should be running");
        } catch (Exception e) {
            LOG.error("Failed to start container", e);
            throw e;
        }
    }
}