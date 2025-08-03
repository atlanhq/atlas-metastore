package org.apache.atlas.service.redis;

import org.apache.atlas.AtlasException;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.redisson.Redisson;
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
        // Memory optimization: Use single Redisson client for both operations to halve thread count
        // This prevents dual Netty thread pools (8 threads → 4 threads)
        redisClient = Redisson.create(getProdConfig());
        redisCacheClient = redisClient;  // Reuse same client for cache operations
        LOG.info("Sentinel redis client created successfully with shared instance (memory optimized).");
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

}
