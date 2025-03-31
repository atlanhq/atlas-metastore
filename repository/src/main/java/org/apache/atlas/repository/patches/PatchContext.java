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

package org.apache.atlas.repository.patches;

import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapperV2;
import org.apache.atlas.type.AtlasTypeRegistry;

public class PatchContext {
    private final AtlasGraph               graph;
    private final AtlasTypeRegistry        typeRegistry;
    private final GraphBackedSearchIndexer indexer;
    private final AtlasPatchRegistry       patchRegistry;
    private final EntityGraphMapperV2 entityGraphMapperV2;

    public PatchContext(AtlasGraph graph, AtlasTypeRegistry typeRegistry, GraphBackedSearchIndexer indexer, EntityGraphMapperV2 entityGraphMapperV2) {
        this.graph         = graph;
        this.typeRegistry  = typeRegistry;
        this.indexer       = indexer;
        this.patchRegistry = new AtlasPatchRegistry(this.graph);
        this.entityGraphMapperV2 = entityGraphMapperV2;
    }

    public AtlasGraph getGraph() {
        return graph;
    }

    public AtlasTypeRegistry getTypeRegistry() {
        return typeRegistry;
    }

    public GraphBackedSearchIndexer getIndexer() {
        return indexer;
    }

    public AtlasPatchRegistry getPatchRegistry() {
        return patchRegistry;
    }
    public EntityGraphMapperV2 getEntityGraphMapperV2() { return entityGraphMapperV2;}
}
