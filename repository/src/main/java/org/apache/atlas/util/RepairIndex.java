/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.util;


import org.apache.atlas.model.Tag;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graph.IFullTextMapper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.atlas.repository.graphdb.janus.cassandra.ESConnector;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.repository.util.AtlasEntityUtils;
import org.apache.atlas.repository.util.TagDeNormAttributesUtil;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasEntityUtil;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.graph.GraphHelper.getGuid;

@Component
public class RepairIndex {
    private static final Logger LOG = LoggerFactory.getLogger(RepairIndex.class);

    private static final int  MAX_TRIES_ON_FAILURE = 3;

    private static final String INDEX_NAME_VERTEX_INDEX = "vertex_index";
    private static final String INDEX_NAME_FULLTEXT_INDEX = "fulltext_index";
    private static final String INDEX_NAME_EDGE_INDEX = "edge_index";

    private static TagDAO tagDAO;
    private static AtlasGraph graph;
    private static AtlasTypeRegistry typeRegistry;
    private static IFullTextMapper fullTextMapperV2;

    @Inject
    public RepairIndex(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                       TagDAO tagDAO, IFullTextMapper fullTextMapperV2) {
        this.graph = graph;
        this.tagDAO = tagDAO;
        this.typeRegistry = typeRegistry;
        this.fullTextMapperV2 = fullTextMapperV2;
    }

    public void restoreSelective(String guid, Map<String,
                                         AtlasEntity> referredEntities) throws Exception {
        Set<String> referencedGUIDs = new HashSet<>(getEntityAndReferenceGuids(guid, referredEntities));
        LOG.info("processing referencedGuids => " + referencedGUIDs);

        long startTime = System.currentTimeMillis();
        reindexVertex(referencedGUIDs);

        LOG.info(": Time taken: " + (System.currentTimeMillis() - startTime) + " ms");
        LOG.info(": Done!");
    }

    public void restoreByIds(Set<String> guids) throws Exception {
        long startTime = System.currentTimeMillis();
        reindexVertex(guids);

        LOG.info(": Time taken: " + (System.currentTimeMillis() - startTime) + " ms");
        LOG.info(": Done!");
    }

    private Set<String> getEntityAndReferenceGuids(String guid, Map<String, AtlasEntity> referredEntities) {
        Set<String> set = new HashSet<>();
        set.add(guid);
        if (referredEntities == null || referredEntities.isEmpty()) {
            return set;
        }
        set.addAll(referredEntities.keySet());
        return set;
    }

    private void reindexVertex(Set<String> entityGUIDs) throws Exception {
        Set<AtlasVertex> vertices = new HashSet<>(entityGUIDs.size());

        for (String entityGuid : entityGUIDs){
            vertices.add(AtlasGraphUtilsV2.findByGuid(entityGuid));
        }

        Map<String, Map<String, Object>> toReIndex = ((AtlasJanusGraph) graph).getESPropertiesForUpdateFromVertices(vertices, this.typeRegistry);
        for (AtlasVertex vertex : vertices) {
            String vertexId = vertex.getIdForDisplay();
            List<AtlasClassification> tags = tagDAO.getTagsForVertex(vertexId);
            if (!tags.isEmpty()) {
                toReIndex.get(vertexId).putAll(TagDeNormAttributesUtil.getAllTagAttributes(getGuid(vertex), tags, typeRegistry, fullTextMapperV2));
            }
        }

        try {
            ESConnector.syncToEs(toReIndex, true, null);
        } catch (Exception e){
            LOG.info("Exception: " + e.getMessage());
        }
    }
}
