package org.apache.atlas.service.redis;

import org.apache.atlas.AtlasException;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.redisson.Redisson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@ConditionalOnAtlasProperty(property = "atlas.redis.service.impl")
@Order(Ordered.HIGHEST_PRECEDENCE)
@Profile("!local")  // Don't use in tests (local profile) - has ConfigMap timing bug
public class RedisServiceImpl extends AbstractRedisService {

    private static final Logger LOG = LoggerFactory.getLogger(RedisServiceImpl.class);
    private static final long RETRY_DELAY_MS = 1000L;
    private static final int MAX_RETRY_ATTEMPTS = 60; // 60 seconds total

    @PostConstruct
    public void init() throws InterruptedException {
        LOG.info("==> RedisServiceImpl.init() - Starting Redis service initialization.");

        int attemptCount = 0;
        Exception lastException = null;

        // Try to connect with a maximum number of retries to avoid infinite hang
        while (attemptCount < MAX_RETRY_ATTEMPTS) {
            attemptCount++;
            try {
                LOG.info("Attempting to connect to Redis (attempt {}/{})...", attemptCount, MAX_RETRY_ATTEMPTS);

                redisClient = Redisson.create(getProdConfig());
                redisCacheClient = Redisson.create(getCacheImplConfig());

                if (redisClient == null || redisCacheClient == null) {
                    throw new AtlasException("Failed to create Sentinel redis client.");
                }

                // Test basic connectivity to ensure clients are working.
                testRedisConnectivity();

                LOG.info("RedisServiceImpl initialization completed successfully!");
                return; // Success!
            } catch (Exception e) {
                lastException = e;
                LOG.warn("Redis connection failed (attempt {}/{}): {}. Retrying in {} seconds...", 
                        attemptCount, MAX_RETRY_ATTEMPTS, e.getMessage(), RETRY_DELAY_MS / 1000);
                // Clean up any partially created clients before retrying.
                shutdownClients();
                
                if (attemptCount < MAX_RETRY_ATTEMPTS) {
                    Thread.sleep(RETRY_DELAY_MS);
                }
            }
        }

        // Failed after all retries - fail fast so K8s can restart the pod
        String errorMsg = String.format(
            "Failed to initialize Redis after %d attempts (waited %d seconds). " +
            "This may be due to slow ConfigMap mounting, Redis unavailability, or misconfiguration. " +
            "Kubernetes will restart the pod with exponential backoff, which may allow ConfigMaps to mount successfully.",
            MAX_RETRY_ATTEMPTS, MAX_RETRY_ATTEMPTS * RETRY_DELAY_MS / 1000);
        LOG.error(errorMsg, lastException);
        throw new RuntimeException(errorMsg, lastException);
    }
    
    /**
     * Test Redis connectivity with basic operations
     */
    private void testRedisConnectivity() throws Exception {
        String testKey = "atlas:startup:connectivity:test:" + System.currentTimeMillis();
        String testValue = "connectivity-test";
        
        try {
            // Test cache client
            LOG.debug("Testing Redis cache client connectivity");
            redisCacheClient.getBucket(testKey).set(testValue);
            String retrievedValue = (String) redisCacheClient.getBucket(testKey).get();
            
            if (!testValue.equals(retrievedValue)) {
                throw new RuntimeException("Redis cache client connectivity test failed - value mismatch");
            }
            
            redisCacheClient.getBucket(testKey).delete();
            LOG.debug("Redis connectivity test completed successfully");
            
        } catch (Exception e) {
            throw new Exception("Redis connectivity test failed", e);
        }
    }

    private void shutdownClients() {
        if (redisClient != null && !redisClient.isShutdown()) {
            redisClient.shutdown();
        }
        if (redisCacheClient != null && !redisCacheClient.isShutdown()) {
            redisCacheClient.shutdown();
        }
        redisClient = null;
        redisCacheClient = null;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

}
