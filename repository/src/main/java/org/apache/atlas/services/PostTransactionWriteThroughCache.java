package org.apache.atlas.services;

import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.graphdb.janus.EntityDistributedCache;
import org.apache.atlas.service.redis.RedisServiceImpl;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

public class PostTransactionWriteThroughCache extends GraphTransactionInterceptor.PostTransactionHook {
    private static final Logger LOG = LoggerFactory.getLogger(PostTransactionWriteThroughCache.class);
    public EntityDistributedCache entityDistributedCache;
    public List<AtlasEntityHeader> entitiesToCache = new ArrayList<>();
    public List<AtlasEntityHeader> entitiesToEvict = new ArrayList<>();

    public PostTransactionWriteThroughCache(RedisServiceImpl redisService) {
        entityDistributedCache = new EntityDistributedCache(redisService);
    }

    public void setEntitiesToCache(List<AtlasEntityHeader> entitiesToCache) {
        this.entitiesToCache = entitiesToCache;
    }

    public void setEntitiesToEvict(List<AtlasEntityHeader> entitiesToEvict) {
        this.entitiesToEvict = entitiesToEvict;
    }

    @Override
    public void onComplete(boolean isSuccess) {
        try {

            if (CollectionUtils.isNotEmpty(entitiesToCache) && isSuccess) {
                    for (AtlasEntityHeader entity : entitiesToCache) {
                        entityDistributedCache.storeEntity(entity);
                    }
            }

            if (CollectionUtils.isNotEmpty(entitiesToEvict) && isSuccess) {
                for (AtlasEntityHeader entity : entitiesToEvict) {
                    entityDistributedCache.evictEntity(entity.getTypeName(), entity.getGuid());
                }
            }
        } catch (Exception e) {
            LOG.error("Error while updating cache", e);
        }
    }
}
