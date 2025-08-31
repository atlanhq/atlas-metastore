package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AttributeTrackingEntityProxy;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreProcessorWrapper implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PreProcessorWrapper.class);
    
    private final PreProcessor delegate;
    
    public PreProcessorWrapper(PreProcessor delegate) {
        this.delegate = delegate;
    }
    
    @Override
    public void processAttributes(AtlasStruct struct, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (struct instanceof AtlasEntity) {
            AtlasEntity entity = (AtlasEntity) struct;
            String preprocessorName = delegate.getClass().getSimpleName();
            
            try {
                AtlasEntity proxiedEntity = AttributeTrackingEntityProxy.createProxy(entity, context, preprocessorName);
                
                // Call the delegate's processAttributes with the proxied entity
                delegate.processAttributes(proxiedEntity != null ? proxiedEntity : entity, context, operation);
            } catch (Exception e) {
                LOG.error("Error in preprocessor {}: {}", preprocessorName, e.getMessage());
                // Continue with original entity if proxy fails
                delegate.processAttributes(entity, context, operation);
            }
        } else {
            // Pass through for non-entity structs
            delegate.processAttributes(struct, context, operation);
        }
    }
    
    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        delegate.processDelete(vertex);
    }
} 