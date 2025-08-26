package org.apache.atlas.service.redis;

import org.apache.atlas.AtlasException;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.redisson.Redisson;
import org.redisson.client.RedisConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnAtlasProperty(property = "atlas.redis.service.impl")
public class RedisServiceLocalImpl extends AbstractRedisService {

    private static final Logger LOG = LoggerFactory.getLogger(RedisServiceLocalImpl.class);

    @PostConstruct
    public void init() throws AtlasException {
        long backoffMs = 2_000;         // initial backoff
        long maxBackoffMs = 30_000;     // cap the backoff
        int attempt = 0;

        while (!Thread.currentThread().isInterrupted()) {
            attempt++;
            try {
                // Create clients
                redisClient = Redisson.create(getLocalConfig());
                redisCacheClient = Redisson.create(getLocalConfig());

                redisClient.getKeys().count();
                redisCacheClient.getKeys().count();

                LOG.info("Connected to Redis (attempt {}). Sentinel redis client created successfully.", attempt);
                return;
            } catch (RedisConnectionException e) {
                LOG.warn("Redis connection failed (attempt {}). Retrying in {} ms…",
                        attempt, backoffMs, e);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new AtlasException("Startup interrupted while waiting for Redis", ie);
                }
                backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
            }
        }

        // If we were interrupted, fail the startup cleanly
        throw new AtlasException("Startup interrupted while waiting for Redis");
    }

    @Override
    public String getValue(String key) {
        // If value doesn't exist, return null else return the value
        return (String) redisCacheClient.getBucket(convertToNamespace(key)).get();
    }

    @Override
    public String putValue(String key, String value) {
        // Put the value in the redis cache with TTL
        redisCacheClient.getBucket(convertToNamespace(key)).set(value);
        return value;
    }

    @Override
    public String putValue(String key, String value, int timeout) {
        // Put the value in the redis cache with TTL
        redisCacheClient.getBucket(convertToNamespace(key)).set(value, timeout, TimeUnit.SECONDS);
        return value;
    }

    @Override
    public void removeValue(String key)  {
        // Remove the value from the redis cache
        redisCacheClient.getBucket(convertToNamespace(key)).delete();
    }

    private String convertToNamespace(String key){
        // Append key with namespace :atlas
        return "atlas:"+key;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
