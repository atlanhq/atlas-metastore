package org.apache.atlas.repository.graphdb.janus;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.service.redis.RedisService;
import org.redisson.api.RedissonClient;
import org.redisson.api.RMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class EntityDistributedCache {
    public static List<String> allowedEntityTypes = Arrays.asList(AtlasConfiguration.ATLAS_ENTITY_CACHE_ALLOWED_ENTITY_TYPES.getStringArray());
    private static RMap<String, AtlasEntityHeader> entityCache = null;

    @Autowired
    public EntityDistributedCache(@Qualifier("redisServiceImpl") RedisService redisService) {
        RedissonClient redissonClient = redisService.getRedisCacheClient();
        entityCache = redissonClient.getMap("entityCache");
    }


    public static void storeEntity(AtlasEntityHeader entity) {
        if (!allowedEntityTypes.contains(entity.getTypeName())) {
            return;
        }
        entityCache.put(entity.getGuid(), entity);
    }

    public static void storeEntities(List<AtlasEntityHeader> entities) {
        for (AtlasEntityHeader entity : entities) {
            if (!allowedEntityTypes.contains(entity.getTypeName())) {
                continue;
            }
            entityCache.put(entity.getGuid(), entity);
        }
    }
    public static void evictEntity(String entityType, String guid) {
        if (!allowedEntityTypes.contains(entityType)) {
            return;
        }
        entityCache.remove(guid);
    }

    public static AtlasEntityHeader getEntity(String guid) {
        return entityCache.get(guid);
    }

    public static Integer getEntityCount() {
        return entityCache.size();
    }

}
