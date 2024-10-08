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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class IDBasedEntityResolver implements EntityResolver {
    private static final Logger LOG = LoggerFactory.getLogger(IDBasedEntityResolver.class);

    private final AtlasGraph        graph;
    private final AtlasTypeRegistry typeRegistry;

    public IDBasedEntityResolver(AtlasGraph graph, AtlasTypeRegistry typeRegistry) {
        this.graph             = graph;
        this.typeRegistry      = typeRegistry;
    }

    public EntityGraphDiscoveryContext resolveEntityReferences(EntityGraphDiscoveryContext context) throws AtlasBaseException {
        if (context == null) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "IDBasedEntityResolver.resolveEntityReferences(): context is null");
        }
        LOG.info("Resolving entity references");
        EntityStream entityStream = context.getEntityStream();
        LOG.info("Resolving entity references: {}", entityStream.hasNext());

        Map<String, String> referencedGuids = context.getReferencedGuids();
        List<String> entityGuids = new ArrayList<>(referencedGuids.keySet());
        GraphTransactionInterceptor.lockObjectAndReleasePostCommit(entityGuids);
        LOG.info("Resolving entity references: {}", referencedGuids.size());
        for (Map.Entry<String, String> element : referencedGuids.entrySet()) {
            String guid = element.getKey();
            LOG.info("Resolving entity reference: {}", guid);
            boolean isAssignedGuid = AtlasTypeUtil.isAssignedGuid(guid);
            AtlasVertex vertex = isAssignedGuid ? AtlasGraphUtilsV2.findByGuid(this.graph, guid) : null;

            if (vertex == null && !RequestContext.get().isImportInProgress()) { // if not found in the store, look if the entity is present in the stream
                AtlasEntity entity = entityStream.getByGuid(guid);

                if (entity != null) { // look for the entity in the store using unique-attributes
                    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

                    if (entityType == null) {
                        throw new AtlasBaseException(element.getValue(), AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), entity.getTypeName());
                    }

    //                -------

                    if (
                            ((entity.getAttributes().get(Constants.QUALIFIED_NAME) == null) && (entity.getAttributes().get(Constants.MODEL_QUALIFIED_NAME_PATTERN)!=null))
                                    &&
                                    ((entity.getTypeName().equals(Constants.MODEL_ENTITY)) || (entity.getTypeName().equals(Constants.MODEL_ATTRIBUTE)))) {

                        String qualifiedNamePrefix = (String) entity.getAttributes().get(Constants.MODEL_QUALIFIED_NAME_PATTERN);
                        if (qualifiedNamePrefix.isEmpty()){
                            throw new AtlasBaseException(AtlasErrorCode.QUALIFIED_NAME_PREFIX_NOT_EXIST);
                        }
                         vertex = AtlasGraphUtilsV2.findLatestEntityAttributeVerticesByType(entity.getTypeName(), qualifiedNamePrefix);

                        if (vertex == null) {
                            // no entity exists with this qualifiedName, set qualifiedName and let entity be created
                            entity.setAttribute(Constants.QUALIFIED_NAME, qualifiedNamePrefix + "_" + RequestContext.get().getRequestTime());
                            return context;
                        }

                        //   if guidFromVertex is found let entity be updated
                      //      entity.setGuid(AtlasGraphUtilsV2.getIdFromVertex(vertex));
                        // else find qualifiedName and set qualifiedName : as it is mandatory
                        context.addResolvedGuid(guid, vertex);
                    }else {
                        vertex = AtlasGraphUtilsV2.findByUniqueAttributes(this.graph, entityType, entity.getAttributes());
                    }

                } else if (!isAssignedGuid) { // for local-guids, entity must be in the stream
                    throw new AtlasBaseException(element.getValue(), AtlasErrorCode.REFERENCED_ENTITY_NOT_FOUND, guid);
                }
            }

            if (vertex != null) {
                context.addResolvedGuid(guid, vertex);
            } else {
                if (isAssignedGuid && !RequestContext.get().isImportInProgress()) {
                    throw new AtlasBaseException(element.getValue(), AtlasErrorCode.REFERENCED_ENTITY_NOT_FOUND, guid);
                } else {
                    context.addLocalGuidReference(guid);
                }
            }
        }

        return context;
    }
}