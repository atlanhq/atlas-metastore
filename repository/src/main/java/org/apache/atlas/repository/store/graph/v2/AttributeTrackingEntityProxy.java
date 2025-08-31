package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AttributeTrackingEntityProxy implements InvocationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AttributeTrackingEntityProxy.class);

    private static final Set<String> TRACKED_METHODS = new HashSet<>(Arrays.asList(
            "setAttribute",
            "setRelationshipAttribute",
            "setBusinessAttribute",
            "setCustomAttributes"
    ));

    private final AtlasEntity entity;
    private final EntityMutationContext context;
    private final String preprocessorName;

    private AttributeTrackingEntityProxy(AtlasEntity entity, EntityMutationContext context, String preprocessorName) {
        this.entity = entity;
        this.context = context;
        this.preprocessorName = preprocessorName;
    }

    public static AtlasEntity createProxy(AtlasEntity entity, EntityMutationContext context, String preprocessorName) {
        if (entity == null) {
            return null;
        }

        try {
            return (AtlasEntity) Proxy.newProxyInstance(
                    PreProcessor.class.getClassLoader(),
                    new Class[]{AtlasEntity.class},
                    new AttributeTrackingEntityProxy(entity, context, preprocessorName)
            );
        } catch (Exception e) {
            LOG.error("Failed to create proxy for entity {}", entity.getGuid(), e);
            return entity; // Fallback to original entity if proxy creation fails
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        // Track attribute changes for specific methods
        if (TRACKED_METHODS.contains(methodName) && args != null && args.length >= 2) {
            String attrName = (String) args[0];
            Object attrValue = args[1];

            if (LOG.isDebugEnabled()) {
                LOG.debug("Tracking attribute change: entity={}, attribute={}", 
                         entity.getGuid(), attrName);
            }

            try {
                // Just use the attribute name directly, no method prefix or preprocessor name
                context.getPreProcessorTracker(entity.getGuid())
                       .trackAttribute(preprocessorName, attrName, attrValue);
            } catch (Exception e) {
                LOG.error("Failed to track attribute change: entity={}, attribute={}", 
                         entity.getGuid(), attrName, e);
                // Continue execution even if tracking fails
            }
        }

        // Invoke the actual method on the real entity
        return method.invoke(entity, args);
    }
} 