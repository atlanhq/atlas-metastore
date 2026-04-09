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
package org.apache.atlas.repository.graphdb;

import org.apache.atlas.model.discovery.AtlasAggregationEntry;

import java.util.List;
import java.util.Map;

/**
 * Represents a graph client work with indices used by Jansgraph.
 */
public interface AtlasGraphIndexClient {

    /**
     * Gets aggregated metrics for the given query string and aggregation field names.
     * @return A map of aggregation field to value-count pairs.
     */
    Map<String, List<AtlasAggregationEntry>> getAggregatedMetrics(AggregationContext aggregationContext);

    /**
     * Returns top 5 suggestions for the given prefix string.
     * @param prefixString the prefix string whose value needs to be retrieved.
     * @param indexFieldName the indexed field name from which to retrieve suggestions
     * @return top 5 suggestion strings with prefix String
     */
    List<String> getSuggestions(String prefixString, String indexFieldName);

    /**
     *  The implementers should apply the search weights for the passed in properties.
     *  @param collectionName                the name of the collection for which the search weight needs to be applied
     *  @param indexFieldName2SearchWeightMap the map containing search weights from index field name to search weights.
     */
    void applySearchWeight(String collectionName, Map<String, Integer> indexFieldName2SearchWeightMap);

    /**
     * The implementors should take the passed in list of suggestion properties for suggestions functionality.
     * @param collectionName the name of the collection to which the suggestions properties should be applied to.
     * @param suggestionProperties the list of suggestion properties.
     */
    void applySuggestionFields(String collectionName, List<String> suggestionProperties);

    /**
     * Returns status of index client
     * @return returns true if index client is active
     */
    boolean isHealthy();

    /**
     * Ensures the JanusGraph vertex index has the required Atlan analysis settings
     * (normalizers, analyzers) before field mappings that reference them are registered.
     *
     * <p>JanusGraph creates {@code janusgraph_vertex_index} with empty settings. Atlan fields
     * like {@code ENTITY_TYPE_PROPERTY_KEY} and {@code SUPER_TYPES_PROPERTY_KEY} use
     * {@code atlan_normalizer} on their keyword sub-fields. If the normalizer is not defined
     * on the index at registration time, Elasticsearch rejects the PUT mapping with HTTP 400,
     * causing the Spring context to fail and Atlas to crash-loop.
     *
     * <p>Behaviour:
     * <ul>
     *   <li>Index has correct settings → no-op, returns {@code true}.</li>
     *   <li>Index exists without settings AND has 0 documents → deletes and recreates with
     *       correct settings, returns {@code true}.</li>
     *   <li>Index exists without settings AND has documents → logs a critical error and returns
     *       {@code false} (cannot safely change analysis settings on a populated index without
     *       a full reindex).</li>
     *   <li>Index does not exist → creates it with correct settings so that JanusGraph inherits
     *       them when it registers the index in its own schema, returns {@code true}.</li>
     * </ul>
     *
     * @return {@code true} if the index has (or was brought to have) the correct settings.
     */
    boolean ensureVertexIndexSettings();
}
