package org.apache.atlas.web.integration;

import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.security.SecurityProperties;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.Atlas;
import org.apache.atlas.web.service.EmbeddedServer;
import org.apache.atlas.web.service.ServiceState;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import com.redis.testcontainers.RedisContainer;
import org.testcontainers.utility.MountableFile;
import org.testcontainers.containers.GenericContainer;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BaseAtlasIT {
    private static final Logger LOG = LoggerFactory.getLogger(BaseAtlasIT.class);

    static {
        try {
            // Log Docker environment
            LOG.info("Environment variables:");
            LOG.info("DOCKER_HOST={}", System.getenv("DOCKER_HOST"));
            LOG.info("HOME={}", System.getenv("HOME"));

            // Try to detect Docker socket
            String[] possibleSockets = new String[] {
                "/var/run/docker.sock",
                System.getenv("HOME") + "/.colima/docker.sock",
                "/Users/sriram.aravamuthan/.colima/docker.sock"
            };

            for (String socket : possibleSockets) {
                File f = new File(socket);
                LOG.info("Checking socket at {}: exists={}, canRead={}", socket, f.exists(), f.canRead());
            }

            // Configure Testcontainers
            System.setProperty("testcontainers.ryuk.disabled", "true");
            System.setProperty("testcontainers.reuse.enable", "true");
            System.setProperty("docker.host", "unix://" + System.getenv("HOME") + "/.colima/docker.sock");
            System.setProperty("testcontainers.docker.socket", System.getenv("HOME") + "/.colima/docker.sock");
        } catch (Exception e) {
            LOG.error("Error during static initialization", e);
        }
    }

    protected static final Network NETWORK = Network.newNetwork();

    protected static final ElasticsearchContainer elasticsearch = new ElasticsearchContainer(
            DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:7.17.10"))
            .withNetwork(NETWORK)
            .withNetworkAliases("elasticsearch")
            .withEnv("discovery.type", "single-node")
            .withEnv("xpack.security.enabled", "false")
            .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
            .withStartupTimeout(Duration.ofMinutes(5))
            .withReuse(true)
            .waitingFor(Wait.forLogMessage(".*started.*", 1));

    protected static final CassandraContainer<?> cassandra = new CassandraContainer<>(
            DockerImageName.parse("cassandra:3.11.18"))
            .withNetwork(NETWORK)
            .withNetworkAliases("cassandra")
            .withStartupTimeout(Duration.ofMinutes(5))
            .withReuse(true)
            .waitingFor(Wait.forLogMessage(".*Starting listening for CQL clients.*", 1));

    protected static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka")
            .withStartupTimeout(Duration.ofMinutes(5))
            .withReuse(true);

    protected static final RedisContainer redis = new RedisContainer(
            DockerImageName.parse("redis/redis-stack:7.2.0-v0"))
            .withNetwork(NETWORK)
            .withNetworkAliases("redis")
            .withStartupTimeout(Duration.ofMinutes(5))
            .withReuse(true)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1));

    protected AnnotationConfigApplicationContext applicationContext;
    protected AtlasTypeRegistry typeRegistry;
    protected AtlasGraph atlasGraph;
    protected RedisService redisService;
    protected Atlas atlas;
    protected ServiceState serviceState;
    private EmbeddedServer server;

    @BeforeAll
    public void setUp() throws Exception {
        LOG.info("Starting test setup...");

        // First verify Docker connectivity
        try {
            Process process = Runtime.getRuntime().exec("docker ps");
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            LOG.info("Current Docker containers:");
            while ((line = reader.readLine()) != null) {
                LOG.info(line);
            }
            int exitCode = process.waitFor();
            LOG.info("Docker ps exit code: {}", exitCode);
        } catch (Exception e) {
            LOG.error("Failed to run docker ps", e);
            throw e;
        }

        // Try with a simple container first
        try {
            LOG.info("Testing with simple Redis container...");
            GenericContainer<?> simpleContainer = new GenericContainer<>(DockerImageName.parse("redis:7.2.0"))
                    .withExposedPorts(6379)
                    .withStartupTimeout(Duration.ofMinutes(1));

            LOG.info("Starting simple container...");
            simpleContainer.start();
            LOG.info("Simple container started successfully!");

            // If simple container works, try our actual containers
            LOG.info("Starting actual test containers...");
            redis.start();
            cassandra.start();
            elasticsearch.start();
            kafka.start();

            simpleContainer.stop();
        } catch (Exception e) {
            LOG.error("Failed to start containers", e);
            stopContainers();
            throw e;
        }

        // Continue with Atlas setup...
        try {
            // Create Atlas configuration
            Configuration configuration = new PropertiesConfiguration();

            // Set system properties for Atlas configuration
            configuration.setProperty("atlas.graph.storage.hostname", cassandra.getHost());
            configuration.setProperty("atlas.graph.storage.port", cassandra.getMappedPort(9042));
            configuration.setProperty("atlas.notification.kafka.zookeeper.connect", kafka.getBootstrapServers());
            configuration.setProperty("atlas.kafka.bootstrap.servers", kafka.getBootstrapServers());
            configuration.setProperty("atlas.graph.index.search.hostname", elasticsearch.getHost());
            configuration.setProperty("atlas.graph.index.search.port", elasticsearch.getMappedPort(9200));
            configuration.setProperty("atlas.redis.host", redis.getHost());
            configuration.setProperty("atlas.redis.port", redis.getMappedPort(6379));

            // Additional required properties
            configuration.setProperty("atlas.graph.storage.backend", "cassandra");
            configuration.setProperty("atlas.graph.storage.clustername", "Atlas");
            configuration.setProperty("atlas.graph.storage.cassandra.keyspace", "atlas_test");
            configuration.setProperty("atlas.notification.embedded", "false");
            configuration.setProperty("atlas.graph.index.search.backend", "elasticsearch");
            configuration.setProperty("atlas.graph.index.search.elasticsearch.protocol", "http");
            configuration.setProperty("atlas.EntityAuditRepository.impl", "org.apache.atlas.repository.audit.NoopEntityAuditRepository");
            configuration.setProperty("atlas.graph.storage.lock.backend", "redis");
            configuration.setProperty("atlas.server.ha.enabled", "false");
            configuration.setProperty("atlas.rest.address", "http://localhost:21000");

            // Initialize Atlas
            serviceState = new ServiceState();
            final String appHost = configuration.getString(SecurityProperties.BIND_ADDRESS, EmbeddedServer.ATLAS_DEFAULT_BIND_ADDRESS);
            server = EmbeddedServer.newServer(appHost, 21000, "", false);
            server.start();

            waitForServicesReady();
        } catch (Exception e) {
            LOG.error("Failed to start Atlas", e);
            stopContainers();
            throw e;
        }
    }

    private void stopContainers() {
        LOG.info("Stopping containers...");
        try {
            if (redis != null && redis.isRunning()) {
                redis.stop();
            }
        } catch (Exception e) {
            LOG.error("Error stopping Redis", e);
        }
        try {
            if (kafka != null && kafka.isRunning()) {
                kafka.stop();
            }
        } catch (Exception e) {
            LOG.error("Error stopping Kafka", e);
        }
        try {
            if (elasticsearch != null && elasticsearch.isRunning()) {
                elasticsearch.stop();
            }
        } catch (Exception e) {
            LOG.error("Error stopping Elasticsearch", e);
        }
        try {
            if (cassandra != null && cassandra.isRunning()) {
                cassandra.stop();
            }
        } catch (Exception e) {
            LOG.error("Error stopping Cassandra", e);
        }
    }

    @AfterAll
    public void tearDown() {
        LOG.info("Stopping Atlas and test containers...");

        try {
            if (server != null) {
                server.stop();
            }
        } catch (Exception e) {
            LOG.error("Error stopping Atlas", e);
        }

        stopContainers();

        // Clean up Atlas components
        AtlasGraphProvider.cleanup();
        if (applicationContext != null) {
            applicationContext.close();
        }
    }

    private void waitForServicesReady() throws InterruptedException {
        int maxAttempts = 3;
        int attempt = 0;
        boolean ready = false;

        while (!ready && attempt < maxAttempts) {
            try {
                if (serviceState.getState().equals(ServiceState.ServiceStateValue.ACTIVE)) {
                    ready = true;
                    LOG.info("Atlas service is ready");
                } else {
                    LOG.info("Waiting for Atlas service to be ready...");
                    TimeUnit.SECONDS.sleep(1);
                }
            } catch (Exception e) {
                LOG.warn("Services not ready yet, attempt {}/{}", attempt, maxAttempts);
                TimeUnit.SECONDS.sleep(1);
            }
            attempt++;
        }

        if (!ready) {
            throw new RuntimeException("Services failed to become ready within timeout");
        }
    }

    protected void clearRequestContext() {
        RequestContext.clear();
        RequestContext.get().setUser("admin", null);
    }
}