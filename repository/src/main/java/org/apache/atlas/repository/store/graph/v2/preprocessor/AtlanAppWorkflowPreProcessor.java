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
package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.repository.Constants.ATTR_OWNER_USERS;

/**
 * PreProcessor for AtlanAppWorkflow entities. Sets ownerUsers to the creator on create
 * so that ABAC policies can restrict edit access to the creator and service accounts.
 */
public class AtlanAppWorkflowPreProcessor implements PreProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(AtlanAppWorkflowPreProcessor.class);

    private static final String ATLAN_APP_WORKFLOW_ENTITY_TYPE = "AtlanAppWorkflow";

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (!ATLAN_APP_WORKFLOW_ENTITY_TYPE.equals(entityStruct.getTypeName())) {
            return;
        }

        if (operation == EntityMutations.EntityOperation.CREATE) {
            AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateAtlanAppWorkflow");
            try {
                setCreatorAsOwner((AtlasEntity) entityStruct);
            } finally {
                RequestContext.get().endMetricRecord(metricRecorder);
            }
        }
    }

    private void setCreatorAsOwner(AtlasEntity entity) {
        String creatorUser = RequestContext.get().getUser();
        if (StringUtils.isEmpty(creatorUser)) {
            LOG.warn("AtlanAppWorkflow created without user context; ownerUsers will not be set");
            return;
        }

        List<String> ownerUsers = new ArrayList<>();
        ownerUsers.add(creatorUser);
        entity.setAttribute(ATTR_OWNER_USERS, ownerUsers);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Set ownerUsers for AtlanAppWorkflow to creator: {}", creatorUser);
        }
    }
}
