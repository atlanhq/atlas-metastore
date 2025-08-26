package org.apache.atlas.service.redis;

import org.apache.atlas.AtlasException;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.redisson.Redisson;
import org.redisson.client.RedisConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@ConditionalOnAtlasProperty(property = "atlas.redis.service.impl")
public class RedisServiceImpl extends AbstractRedisService{

    private static final Logger LOG = LoggerFactory.getLogger(RedisServiceImpl.class);

    @PostConstruct
    public void init() throws AtlasException {
        long backoffMs = 2_000;         // initial backoff
        long maxBackoffMs = 30_000;     // cap the backoff
        int attempt = 0;

        while (!Thread.currentThread().isInterrupted()) {
            attempt++;
            try {
                redisClient = Redisson.create(getProdConfig());
                redisCacheClient = Redisson.create(getCacheImplConfig());

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
    public Logger getLogger() {
        return LOG;
    }

}
