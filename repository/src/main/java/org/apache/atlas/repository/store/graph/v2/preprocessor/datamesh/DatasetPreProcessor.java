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

package org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.DeleteType;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.VALID_DATASET_TYPES;
import static org.apache.atlas.v1.model.instance.Id.EntityState.DELETED;

public class DatasetPreProcessor extends AbstractDomainPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DatasetPreProcessor.class);

    private static final String DATASET_PATH_SEGMENT = "dataset";

    private EntityMutationContext context;

    public DatasetPreProcessor(AtlasTypeRegistry typeRegistry,
                               EntityGraphRetriever entityRetriever,
                               AtlasGraph graph) {
        super(typeRegistry, entityRetriever, graph);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("DatasetPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }
        this.context = context;

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                processCreateDataset(entity, vertex);
                break;
            case UPDATE:
                processUpdateDataset(entity, vertex);
                break;
        }
    }

    private void processCreateDataset(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateDataset");

        try {
            validateDatasetType(entity);

            entity.setAttribute(QUALIFIED_NAME, createQualifiedName());

            if (!entity.hasAttribute(ELEMENT_COUNT_ATTR) || entity.getAttribute(ELEMENT_COUNT_ATTR) == null) {
                entity.setAttribute(ELEMENT_COUNT_ATTR, 0L);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void processUpdateDataset(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateDataset");

        try {
            String state = vertex.getProperty(STATE_PROPERTY_KEY, String.class);

            if (DELETED.name().equals(state)) {
                boolean isBeingRestored = false;

                if (context != null && context.getEntitiesToRestore() != null) {
                    isBeingRestored = context.getEntitiesToRestore().contains(vertex);
                }

                if (!isBeingRestored) {
                    throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Cannot update Dataset that is Archived!");
                }
            }

            if (entity.hasAttribute(DATASET_TYPE_ATTR) && entity.getAttribute(DATASET_TYPE_ATTR) != null) {
                validateDatasetType(entity);
            }

            if (entity.hasAttribute(ELEMENT_COUNT_ATTR) && entity.getAttribute(ELEMENT_COUNT_ATTR) != null) {
                long elementCount = ((Number) entity.getAttribute(ELEMENT_COUNT_ATTR)).longValue();
                if (elementCount < 0) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS,
                            "elementCount must be non-negative");
                }
            }

            // TODO (V2): Calculate the elementCount based on dataElements linked to this dataset.
            // calculateElementCount(entity.getGuid());

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void validateDatasetType(AtlasEntity entity) throws AtlasBaseException {
        Object datasetTypeObj = entity.getAttribute(DATASET_TYPE_ATTR);

        if (datasetTypeObj == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS,
                    "datasetType is mandatory for Dataset. Valid values: RAW, REFINED, AGGREGATED");
        }

        String datasetType = (String) datasetTypeObj;
        if (!VALID_DATASET_TYPES.contains(datasetType.toUpperCase())) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS,
                    "Invalid datasetType: " + datasetType + ". Valid values: RAW, REFINED, AGGREGATED");
        }
    }

    private static String createQualifiedName() {
        return DEFAULT_TENANT_ID + "/" + DATASET_PATH_SEGMENT + "/" + PreProcessorUtils.getUUID();
    }

    /**
     * TODO (V2): Auto-calculate elementCount from dataElements linked to this dataset
     * Query assets with datasetGUIDs = datasetGuid and return the count
     */
    @SuppressWarnings("unused")
    private int calculateElementCount(String datasetGuid) {
        return 0;
    }
}
