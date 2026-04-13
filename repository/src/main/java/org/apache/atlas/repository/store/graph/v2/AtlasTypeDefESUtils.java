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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.repository.graph.AtlasGraphProvider.getGraphInstance;

/**
 * ES-based pre-delete reference checks for typedef deletion.
 *
 * These checks query Elasticsearch to determine whether a type still has
 * associated entities, preventing accidental typedef deletion. On failure,
 * all methods conservatively return true (assume references exist).
 */
public final class AtlasTypeDefESUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasTypeDefESUtils.class);

    private AtlasTypeDefESUtils() {}

    /**
     * Check if an entity/struct type has any instances in Elasticsearch.
     *
     * @param typeName the entity or struct type name to check
     * @return true if any entities of this type exist, false otherwise
     */
    public static boolean typeHasInstanceVertex(String typeName) throws AtlasBaseException {
        try {
            String indexName = Constants.VERTEX_INDEX_NAME;
            AtlasIndexQuery indexQuery = getGraphInstance().elasticsearchQuery(indexName);

            String esQuery = String.format(
                "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"%s\":\"%s\"}}]}}}",
                Constants.TYPE_NAME_PROPERTY_KEY, typeName
            );
            Long count = indexQuery.countIndexQuery(esQuery);

            boolean hasInstance = count != null && count > 0;

            if (LOG.isDebugEnabled()) {
                LOG.debug("typeName {} has instance vertex {} (ES count: {})", typeName, hasInstance, count);
            }

            return hasInstance;
        } catch (Exception e) {
            LOG.error("Error checking type instances for {}: {}", typeName, e.getMessage(), e);
            // If ES fails, assume the type is still linked to entities to prevent accidental deletion
            return true;
        }
    }

    /**
     * Check if a classification type has references — entities with this
     * classification attached directly or via propagation.
     *
     * @param typeName the classification type name to check
     * @return true if any entities have this classification, false otherwise
     */
    public static boolean classificationHasReferences(String typeName) {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Checking if classification {} has references using ES", typeName);
            }

            String indexName = Constants.VERTEX_INDEX_NAME;
            AtlasIndexQuery indexQuery = getGraphInstance().elasticsearchQuery(indexName);

            String esQuery = buildClassificationReferenceQuery(typeName);
            Long count = indexQuery.countIndexQuery(esQuery);

            boolean hasReferences = count != null && count > 0;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Classification {} has references: {} (count: {})", typeName, hasReferences, count);
            }

            return hasReferences;
        } catch (Exception e) {
            LOG.error("Error checking classification references for {}: {}", typeName, e.getMessage(), e);
            // If ES fails, assume the classification is still linked to entities to prevent accidental deletion
            return true;
        }
    }

    private static String buildClassificationReferenceQuery(String typeName) {
        return String.format(
            "{\n" +
            "  \"query\": {\n" +
            "    \"bool\": {\n" +
            "      \"filter\": [\n" +
            "        {\n" +
            "          \"bool\": {\n" +
            "            \"should\": [\n" +
            "              {\"term\": {\"%s\": \"%s\"}},\n" +
            "              {\"term\": {\"%s\": \"%s\"}}\n" +
            "            ],\n" +
            "            \"minimum_should_match\": 1\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}",
            Constants.TRAIT_NAMES_PROPERTY_KEY, typeName,
            Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, typeName
        );
    }
}
