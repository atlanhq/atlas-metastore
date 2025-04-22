/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.converters;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.model.EntityAuditEvent;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.instance.GuidMapping;
import org.apache.atlas.model.legacy.EntityResult;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.instance.Struct;
import org.apache.atlas.repository.converters.AtlasFormatConverter.ConverterContext;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Singleton
@Component
public class AtlasInstanceConverter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasInstanceConverter.class);

    private final AtlasTypeRegistry     typeRegistry;
    private final AtlasFormatConverters instanceFormatters;
    private final EntityGraphRetriever  entityGraphRetriever;
    private final EntityGraphRetriever  entityGraphRetrieverIgnoreRelationshipAttrs;

    @Inject
    public AtlasInstanceConverter(AtlasGraph graph, AtlasTypeRegistry typeRegistry, AtlasFormatConverters instanceFormatters) {
        this.typeRegistry                                = typeRegistry;
        this.instanceFormatters                          = instanceFormatters;
        this.entityGraphRetriever                        = new EntityGraphRetriever(graph, typeRegistry);
        this.entityGraphRetrieverIgnoreRelationshipAttrs = new EntityGraphRetriever(graph, typeRegistry, true);
    }

    public Referenceable[] getReferenceables(Collection<AtlasEntity> entities) throws AtlasBaseException {
        Referenceable[] ret = new Referenceable[entities.size()];

        AtlasFormatConverter.ConverterContext ctx = new AtlasFormatConverter.ConverterContext();

        for(Iterator<AtlasEntity> i = entities.iterator(); i.hasNext(); ) {
            ctx.addEntity(i.next());
        }

        Iterator<AtlasEntity> entityIterator = entities.iterator();
        for (int i = 0; i < entities.size(); i++) {
            ret[i] = getReferenceable(entityIterator.next(), ctx);
        }

        return ret;
    }

    public Referenceable getReferenceable(AtlasEntity entity) throws AtlasBaseException {
        return getReferenceable(entity, new ConverterContext());
    }

    public Referenceable getReferenceable(String guid) throws AtlasBaseException {
        AtlasEntityWithExtInfo entity = getAndCacheEntityExtInfo(guid);

        return getReferenceable(entity);
    }

    public Referenceable getReferenceable(AtlasEntityWithExtInfo entity) throws AtlasBaseException {
        AtlasFormatConverter.ConverterContext ctx = new AtlasFormatConverter.ConverterContext();

        ctx.addEntity(entity.getEntity());
        for(Map.Entry<String, AtlasEntity> entry : entity.getReferredEntities().entrySet()) {
            ctx.addEntity(entry.getValue());
        }

        return getReferenceable(entity.getEntity(), ctx);
    }

    public Referenceable getReferenceable(AtlasEntity entity, final ConverterContext ctx) throws AtlasBaseException {
        AtlasFormatConverter converter  = instanceFormatters.getConverter(TypeCategory.ENTITY);
        AtlasType            entityType = typeRegistry.getType(entity.getTypeName());
        Referenceable        ref        = (Referenceable) converter.fromV2ToV1(entity, entityType, ctx);

        return ref;
    }

    public Struct getTrait(AtlasClassification classification) throws AtlasBaseException {
        AtlasFormatConverter converter          = instanceFormatters.getConverter(TypeCategory.CLASSIFICATION);
        AtlasType            classificationType = typeRegistry.getType(classification.getTypeName());
        Struct               trait               = (Struct)converter.fromV2ToV1(classification, classificationType, new ConverterContext());

        return trait;
    }

    public AtlasEntitiesWithExtInfo toAtlasEntity(Referenceable referenceable) throws AtlasBaseException {
        AtlasEntityFormatConverter converter  = (AtlasEntityFormatConverter) instanceFormatters.getConverter(TypeCategory.ENTITY);
        AtlasEntityType            entityType = typeRegistry.getEntityTypeByName(referenceable.getTypeName());

        if (entityType == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), referenceable.getTypeName());
        }

        ConverterContext ctx    = new ConverterContext();
        AtlasEntity      entity = converter.fromV1ToV2(referenceable, entityType, ctx);

        ctx.addEntity(entity);

        return ctx.getEntities();
    }

    public AtlasEntity getAndCacheEntity(String guid) throws AtlasBaseException {
        return getAndCacheEntity(guid, false);
    }

    public AtlasEntity getAndCacheEntity(String guid, boolean ignoreRelationshipAttributes) throws AtlasBaseException {
        RequestContext context = RequestContext.get();
        AtlasEntity    entity  = context.getEntity(guid);

        if (entity == null) {
            if (ignoreRelationshipAttributes) {
                entity = entityGraphRetrieverIgnoreRelationshipAttrs.toAtlasEntity(guid);
            } else {
                entity = entityGraphRetriever.toAtlasEntity(guid);
            }

            if (entity != null) {
                context.cache(entity);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cache miss -> GUID = {}", guid);
                }
            }
        }

        return entity;
    }

    public AtlasEntity getEntity(String guid, boolean ignoreRelationshipAttributes) throws AtlasBaseException {
        AtlasEntity entity = null;
        if (ignoreRelationshipAttributes) {
            entity = entityGraphRetrieverIgnoreRelationshipAttrs.toAtlasEntity(guid);
        } else {
            entity = entityGraphRetriever.toAtlasEntity(guid);
        }
        return entity;
    }


    public AtlasEntityWithExtInfo getAndCacheEntityExtInfo(String guid) throws AtlasBaseException {
        RequestContext         context           = RequestContext.get();
        AtlasEntityWithExtInfo entityWithExtInfo = context.getEntityWithExtInfo(guid);

        if (entityWithExtInfo == null) {
            entityWithExtInfo = entityGraphRetriever.toAtlasEntityWithExtInfo(guid);

            if (entityWithExtInfo != null) {
                context.cache(entityWithExtInfo);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cache miss -> GUID = {}", guid);
                }
            }
        }

        return entityWithExtInfo;
    }
}