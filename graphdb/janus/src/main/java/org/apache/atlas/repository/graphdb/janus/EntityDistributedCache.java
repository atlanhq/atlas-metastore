package org.apache.atlas.repository.graphdb.janus;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.service.redis.RedisService;
import org.redisson.api.RedissonClient;
import org.redisson.api.RMap;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
    public class EntityDistributedCache {
    private final RedissonClient redissonClient;
    public static List<String> allowedEntityTypes = Arrays.asList("Connection", "Persona", "Purpose");

    public EntityDistributedCache(@Qualifier("redisServiceImpl") RedisService redisService) {
        redissonClient = redisService.getRedisCacheClient();
    }

    private static String getCacheKey(String entityType) {
        return "entityCache:" + entityType;
    }

    public void storeEntity(String entityType,AtlasEntityHeader entity) {
        if (!allowedEntityTypes.contains(entityType)) {
            return;
        }
        String qualifiedName = (String) entity.getAttribute("qualifiedName");
        RMap<String, AtlasEntityHeader> entityCache = redissonClient.getMap(getCacheKey(entityType));
        entityCache.put(qualifiedName, entity);
    }
    public void evictEntity(String entityType, String qualifiedName) {
        if (!allowedEntityTypes.contains(entityType)) {
            return;
        }
        RMap<String, AtlasEntityHeader> entityCache = redissonClient.getMap(getCacheKey(entityType));
        entityCache.remove(qualifiedName);
    }

    public List<AtlasEntityHeader> getEntitiesByType(String entityType) throws AtlasBaseException {
        if (!allowedEntityTypes.contains(entityType)) {
            throw new AtlasBaseException("Invalid entity type");
        }
        RMap<String, AtlasEntityHeader> entityCache = redissonClient.getMap(getCacheKey(entityType));
        return (List<AtlasEntityHeader>) entityCache.values();
    }

    public AtlasEntityHeader getEntityByQualifiedName(String entityType, String qualifiedName) {
        if (!allowedEntityTypes.contains(entityType)) {
            return null;
        }
        RMap<String, AtlasEntityHeader> entityCache = redissonClient.getMap(getCacheKey(entityType));
        return entityCache.get(qualifiedName);
    }

}
