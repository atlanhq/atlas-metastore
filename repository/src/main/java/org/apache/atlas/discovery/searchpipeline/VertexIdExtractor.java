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

import java.util.List;

/**
 * Strategy interface for extracting Cassandra vertex IDs from Elasticsearch hit results.
 *
 * Two implementations:
 * <ul>
 *   <li>{@link DirectVertexIdExtractor} — ZeroGraph: ES {@code _id} IS the vertex ID. 0 CQL.</li>
 *   <li>{@link JanusGraphVertexIdExtractor} — JanusGraph: decode LongEncoding from ES {@code _id}.</li>
 * </ul>
 */
public interface VertexIdExtractor {

    /**
     * Extract vertex IDs from ES hits, preserving result order.
     *
     * @param esHits list of ES hit results
     * @return ordered list of vertex ID strings (same order as esHits, nulls excluded)
     */
    List<String> extractVertexIds(List<ESHitResult> esHits);
}
