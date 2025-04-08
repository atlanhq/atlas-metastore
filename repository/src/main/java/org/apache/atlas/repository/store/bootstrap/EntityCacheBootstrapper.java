package org.apache.atlas.repository.store.bootstrap;

import org.apache.atlas.AtlasException;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.repository.graphdb.janus.EntityDistributedCache;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
@DependsOn("atlasTypeDefStoreInitializer")
public class EntityCacheBootstrapper implements ActiveStateChangeHandler, Service {
    public static final Logger LOG = LoggerFactory.getLogger(EntityCacheBootstrapper.class);
    private final AtlasEntityStoreV2 entityStoreV2;

    @Autowired
    public EntityCacheBootstrapper(AtlasEntityStoreV2 entityStoreV2) {
        this.entityStoreV2 = entityStoreV2;
    }

    private void startInternal() {
        try {
            LOG.info("EntityCacheBootstrapper: startInternal: start");
            if (entityStoreV2 == null) {
                LOG.error("EntityCacheBootstrapper: startInternal: entityStoreV2 is null");
                return;
            }
            Integer entityCacheSize = EntityDistributedCache.getEntityCount();
            Integer totalEntityCount = entityStoreV2.getEntityCountByTypeNames(Arrays.asList(AtlasEntityStoreV2.entityTypesToCache));
            if (entityCacheSize > 0) {
                if (entityCacheSize < totalEntityCount) {
                    LOG.info("EntityCacheBootstrapper: startInternal: entityCacheSize: {} is less than totalEntityCount: {}", entityCacheSize, totalEntityCount);
                    entityStoreV2.initCache();
                }
            } else {
                LOG.info("EntityCacheBootstrapper: startInternal: entityCacheSize: {}", entityCacheSize);
                entityStoreV2.initCache();
            }
        } catch (Exception e) {
            LOG.error("Failed to init after becoming active", e);
        } finally {
            LOG.info("EntityCacheBootstrapper: startInternal: done");
        }
    }


    @Override
    public void instanceIsActive() throws AtlasException {
        startInternal();
    }

    @Override
    public void instanceIsPassive() throws AtlasException {

    }

    @Override
    public int getHandlerOrder() {
        return 0;
    }

    @Override
    public void start() throws AtlasException {
        startInternal();
    }

    @Override
    public void stop() throws AtlasException {

    }
}
