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
package org.apache.atlas.discovery.searchpipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * ZeroGraph (CassandraGraph) implementation: ES document {@code _id} IS the
 * Cassandra {@code vertex_id}. Extract directly — zero CQL.
 *
 * Current code does:
 * <pre>
 *   result.getVertex()  // → CassandraGraph.getVertex(docId) → SYNC CQL per hit
 *   vertex.getId()      // → return the ID we already had
 * </pre>
 *
 * This extractor eliminates those N sequential sync CQL reads by extracting
 * the ID from the ES hit directly.
 */
public class DirectVertexIdExtractor implements VertexIdExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(DirectVertexIdExtractor.class);

    @Override
    public List<String> extractVertexIds(List<ESHitResult> esHits) {
        List<String> ids = new ArrayList<>(esHits.size());
        for (ESHitResult hit : esHits) {
            String docId = hit.getDocumentId();
            if (docId != null && !"null".equals(docId)) {
                ids.add(docId);
            } else {
                LOG.warn("DirectVertexIdExtractor: ES hit with null _id, skipping");
            }
        }
        return ids;
    }
}
