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
@Profile("!local")
public class RedisServiceImpl extends AbstractRedisService{

    private static final Logger LOG = LoggerFactory.getLogger(RedisServiceImpl.class);

    @PostConstruct
    public void init() throws AtlasException {
        LOG.info("Initializing RedisServiceImpl");

        redisClient = Redisson.create(getProdConfig());
        redisCacheClient = Redisson.create(getCacheImplConfig());
        LOG.debug("Sentinel redis client created successfully.");
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

}