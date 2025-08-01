/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.AtlasRelationshipStoreV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;

import javax.inject.Inject;

import java.util.Collection;

import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.repository.Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.MODIFIED_BY_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.graph.GraphHelper.getPropagatableClassifications;
import static org.apache.atlas.repository.graph.GraphHelper.getPropagatableClassificationsV2;

public class SoftDeleteHandlerV1 extends DeleteHandlerV1 {

    @Inject
    public SoftDeleteHandlerV1(AtlasGraph graph, AtlasTypeRegistry typeRegistry, TaskManagement taskManagement, EntityGraphRetriever entityRetriever) {
        super(graph, typeRegistry, false, true, taskManagement, entityRetriever);
    }

    @Override
    protected void _deleteVertex(AtlasVertex instanceVertex, boolean force) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> SoftDeleteHandlerV1._deleteVertex({}, {})", GraphHelper.string(instanceVertex), force);
        }

        if (force) {
            graphHelper.removeVertex(instanceVertex);
        } else {
            Status state = AtlasGraphUtilsV2.getState(instanceVertex);

            if (state != DELETED) {
                AtlasGraphUtilsV2.setEncodedProperty(instanceVertex, STATE_PROPERTY_KEY, DELETED.name());
                AtlasGraphUtilsV2.setEncodedProperty(instanceVertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
                AtlasGraphUtilsV2.setEncodedProperty(instanceVertex, MODIFIED_BY_KEY, RequestContext.get().getUser());
            }
        }
    }

    @Override
    protected void deleteEdge(AtlasEdge edge, boolean force) throws AtlasBaseException {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> SoftDeleteHandlerV1.deleteEdge({}, {})", GraphHelper.string(edge), force);
            }
            boolean isRelationshipEdge = isRelationshipEdge(edge);
            authorizeRemoveRelation(edge);


            if (DEFERRED_ACTION_ENABLED && RequestContext.get().getCurrentTask() == null) {
                Collection propagatableTags = org.apache.atlas.service.FeatureFlagStore.isTagV2Enabled()
                        ? getPropagatableClassificationsV2(edge)
                        : getPropagatableClassifications(edge);
                if (CollectionUtils.isNotEmpty(propagatableTags)) {
                    RequestContext.get().addToDeletedEdgesIds(edge.getIdForDisplay());
                }
            } else {
                removeTagPropagation(edge);
            }

            if (force) {
                graphHelper.removeEdge(edge);
            } else {
                Status state = AtlasGraphUtilsV2.getState(edge);

                if (state != DELETED) {
                    AtlasGraphUtilsV2.setEncodedProperty(edge, STATE_PROPERTY_KEY, DELETED.name());
                    AtlasGraphUtilsV2.setEncodedProperty(edge, MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
                    AtlasGraphUtilsV2.setEncodedProperty(edge, MODIFIED_BY_KEY, RequestContext.get().getUser());
                }
            }
            if (isRelationshipEdge)
                AtlasRelationshipStoreV2.recordRelationshipMutation(AtlasRelationshipStoreV2.RelationshipMutation.RELATIONSHIP_SOFT_DELETE, edge, entityRetriever);
        } catch (NullPointerException npe) {
            LOG.error("Error while deleting edge {}", GraphHelper.string(edge), npe);
            throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_SERVER_ERROR, npe);
        } catch (Exception e) {
            LOG.error("Error while deleting edge {}", GraphHelper.string(edge), e);
            throw new AtlasBaseException(e);
        }

    }
}
