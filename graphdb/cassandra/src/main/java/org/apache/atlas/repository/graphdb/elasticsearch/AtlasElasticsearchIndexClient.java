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
package org.apache.atlas.repository.graphdb.elasticsearch;

import org.apache.atlas.model.discovery.AtlasAggregationEntry;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AggregationContext;
import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.commons.configuration.Configuration;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.*;

public class AtlasElasticsearchIndexClient implements AtlasGraphIndexClient {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasElasticsearchIndexClient.class);

    private static final long   HEALTH_CHECK_LOG_FREQUENCY_MS = 60000;
    private static long         prevHealthCheckTime;

    private final Configuration configuration;

    public AtlasElasticsearchIndexClient(Configuration configuration) {
        this.configuration = configuration;
    }

    public boolean isHealthy() {
        boolean isHealthy   = false;
        long    currentTime = System.currentTimeMillis();

        try {
            isHealthy = isElasticsearchHealthy();
        } catch (Exception exception) {
            if (LOG.isDebugEnabled()) {
                LOG.error("Error: isHealthy", exception);
            }
        }

        if (!isHealthy && (prevHealthCheckTime == 0 || currentTime - prevHealthCheckTime > HEALTH_CHECK_LOG_FREQUENCY_MS)) {
            LOG.info("Index Health: Unhealthy!");
            prevHealthCheckTime = currentTime;
        }

        return isHealthy;
    }

    @Override
    public void applySearchWeight(String collectionName, Map<String, Integer> indexFieldName2SearchWeightMap) {
    }

    @Override
    public Map<String, List<AtlasAggregationEntry>> getAggregatedMetrics(AggregationContext aggregationContext) {
        return Collections.EMPTY_MAP;
    }

    @Override
    public void applySuggestionFields(String collectionName, List<String> suggestionProperties) {
        LOG.info("Applied suggestion fields request handler for collection {}.", collectionName);
    }

    @Override
    public List<String> getSuggestions(String prefixString, String indexFieldName) {
        return Collections.EMPTY_LIST;
    }

    /**
     * Ensures janusgraph_vertex_index exists in ES with the Atlan analysis settings
     * (atlan_normalizer and associated analyzers) that field mappings depend on.
     *
     * JanusGraph creates the index with empty settings via its management API. Atlan's
     * GraphBackedSearchIndexer then registers field mappings that reference atlan_normalizer.
     * Without this method, any startup after an index deletion causes a HTTP 400 from ES
     * ("normalizer not found"), crashing the Spring context in a permanent restart loop.
     */
    @Override
    public boolean ensureVertexIndexSettings() {
        RestHighLevelClient client = AtlasElasticsearchDatabase.getClient();
        String indexName = VERTEX_INDEX;

        try {
            boolean indexExists = client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);

            if (indexExists) {
                GetSettingsResponse settingsResponse = client.indices()
                        .getSettings(new GetSettingsRequest().indices(indexName), RequestOptions.DEFAULT);
                Settings idxSettings = settingsResponse.getIndexToSettings().get(indexName);

                boolean hasNormalizer = idxSettings != null
                        && idxSettings.get("index.analysis.normalizer.atlan_normalizer.type") != null;

                if (hasNormalizer) {
                    LOG.info("ensureVertexIndexSettings: {} has correct analysis settings.", indexName);
                    return true;
                }

                // Normalizer missing — check whether it is safe to recreate
                CountResponse countResponse = client.count(new CountRequest(indexName), RequestOptions.DEFAULT);
                long docCount = countResponse.getCount();

                if (docCount > 0) {
                    LOG.error("ensureVertexIndexSettings: {} is missing atlan_normalizer but has {} documents. "
                            + "Atlas will fail to register field mappings. Manual intervention required: "
                            + "close the index, apply analysis settings, then reopen.", indexName, docCount);
                    return false;
                }

                LOG.warn("ensureVertexIndexSettings: {} is missing atlan_normalizer with 0 documents — "
                        + "deleting and recreating with correct settings.", indexName);
                AcknowledgedResponse deleteResponse = client.indices()
                        .delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
                if (!deleteResponse.isAcknowledged()) {
                    LOG.error("ensureVertexIndexSettings: failed to delete {} for recreation.", indexName);
                    return false;
                }
            }

            // Index does not exist (or was just deleted) — create with correct settings.
            // JanusGraph will attempt its own createVertexMixedIndex call and ES will respond
            // with resource_already_exists_exception, which JanusGraph handles gracefully.
            CreateIndexRequest createRequest = new CreateIndexRequest(indexName);
            createRequest.settings(buildVertexIndexSettings());
            client.indices().create(createRequest, RequestOptions.DEFAULT);
            LOG.info("ensureVertexIndexSettings: created {} with correct analysis settings.", indexName);
            return true;

        } catch (Exception e) {
            LOG.error("ensureVertexIndexSettings: failed for {}: {}", indexName, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Builds the ES index settings required by Atlan's janusgraph_vertex_index.
     * Mirrors addons/elasticsearch/es-settings.json — the two must be kept in sync.
     * The critical entry is atlan_normalizer, referenced by KEYWORD_MULTIFIELD in
     * GraphBackedSearchIndexer for fields like ENTITY_TYPE_PROPERTY_KEY and SUPER_TYPES_PROPERTY_KEY.
     */
    private Map<String, Object> buildVertexIndexSettings() {
        // --- normalizer ---
        Map<String, Object> atlNormalizer = new HashMap<>();
        atlNormalizer.put("type", "custom");
        atlNormalizer.put("filter", Arrays.asList("lowercase"));

        Map<String, Object> normalizers = new HashMap<>();
        normalizers.put("atlan_normalizer", atlNormalizer);

        // --- tokenizers ---
        Map<String, Object> atlanTokenizer = new HashMap<>();
        atlanTokenizer.put("type", "pattern");
        atlanTokenizer.put("pattern", "( |_|-|'|/|@)");

        Map<String, Object> atlanGlossaryTokenizer = new HashMap<>();
        atlanGlossaryTokenizer.put("type", "char_group");
        atlanGlossaryTokenizer.put("tokenize_on_chars", Arrays.asList("whitespace", "punctuation", "symbol"));

        Map<String, Object> atlanCommaTokenizer = new HashMap<>();
        atlanCommaTokenizer.put("type", "pattern");
        atlanCommaTokenizer.put("pattern", ",");

        Map<String, Object> atlanPathTokenizer = new HashMap<>();
        atlanPathTokenizer.put("type", "path_hierarchy");
        atlanPathTokenizer.put("delimiter", "/");

        Map<String, Object> tokenizers = new HashMap<>();
        tokenizers.put("atlan_tokenizer", atlanTokenizer);
        tokenizers.put("atlan_glossary_tokenizer", atlanGlossaryTokenizer);
        tokenizers.put("atlan_comma_tokenizer", atlanCommaTokenizer);
        tokenizers.put("atlan_path_tokenizer", atlanPathTokenizer);

        // --- char filters ---
        Map<String, Object> numberFilter = new HashMap<>();
        numberFilter.put("type", "pattern_replace");
        numberFilter.put("pattern", "\\d+");
        numberFilter.put("replacement", " $0");

        Map<String, Object> truncateFilter = new HashMap<>();
        truncateFilter.put("type", "pattern_replace");
        truncateFilter.put("pattern", "(.{0,100000}).*");
        truncateFilter.put("replacement", "$1");
        truncateFilter.put("flags", "DOTALL");

        Map<String, Object> letterNumberFilter = new HashMap<>();
        letterNumberFilter.put("type", "pattern_replace");
        letterNumberFilter.put("pattern", "\\d+");
        letterNumberFilter.put("replacement", " $0 ");

        Map<String, Object> charFilters = new HashMap<>();
        charFilters.put("number_filter", numberFilter);
        charFilters.put("truncate_filter", truncateFilter);
        charFilters.put("letter_number_filter", letterNumberFilter);

        // --- analyzers ---
        Map<String, Object> atlanTextAnalyzer = new HashMap<>();
        atlanTextAnalyzer.put("type", "custom");
        atlanTextAnalyzer.put("tokenizer", "atlan_tokenizer");
        atlanTextAnalyzer.put("filter", Arrays.asList("apostrophe", "lowercase"));
        atlanTextAnalyzer.put("char_filter", Arrays.asList("truncate_filter", "number_filter"));

        Map<String, Object> atlanGlossaryAnalyzer = new HashMap<>();
        atlanGlossaryAnalyzer.put("type", "custom");
        atlanGlossaryAnalyzer.put("tokenizer", "atlan_glossary_tokenizer");
        atlanGlossaryAnalyzer.put("filter", Arrays.asList("lowercase"));
        atlanGlossaryAnalyzer.put("char_filter", Arrays.asList("letter_number_filter"));

        Map<String, Object> atlanTextCommaAnalyzer = new HashMap<>();
        atlanTextCommaAnalyzer.put("type", "custom");
        atlanTextCommaAnalyzer.put("tokenizer", "atlan_comma_tokenizer");
        atlanTextCommaAnalyzer.put("filter", Arrays.asList("lowercase"));

        Map<String, Object> atlanPathAnalyzer = new HashMap<>();
        atlanPathAnalyzer.put("type", "custom");
        atlanPathAnalyzer.put("tokenizer", "atlan_path_tokenizer");
        atlanPathAnalyzer.put("filter", Arrays.asList("trim", "remove_duplicates"));

        Map<String, Object> analyzers = new HashMap<>();
        analyzers.put("atlan_text_analyzer", atlanTextAnalyzer);
        analyzers.put("atlan_glossary_analyzer", atlanGlossaryAnalyzer);
        analyzers.put("atlan_text_comma_analyzer", atlanTextCommaAnalyzer);
        analyzers.put("atlan_path_analyzer", atlanPathAnalyzer);

        // --- analysis block ---
        Map<String, Object> analysis = new HashMap<>();
        analysis.put("normalizer", normalizers);
        analysis.put("tokenizer", tokenizers);
        analysis.put("char_filter", charFilters);
        analysis.put("analyzer", analyzers);

        // --- top-level settings ---
        Map<String, Object> totalFields = new HashMap<>();
        totalFields.put("limit", "5000");
        Map<String, Object> nestedObjects = new HashMap<>();
        nestedObjects.put("limit", "100000");
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("total_fields", totalFields);
        mapping.put("nested_objects", nestedObjects);

        Map<String, Object> defaultSimilarity = new HashMap<>();
        defaultSimilarity.put("type", "boolean");
        Map<String, Object> similarity = new HashMap<>();
        similarity.put("default", defaultSimilarity);

        Map<String, Object> settings = new HashMap<>();
        settings.put("analysis", analysis);
        settings.put("mapping", mapping);
        settings.put("similarity", similarity);
        return settings;
    }

    private boolean isElasticsearchHealthy() throws ElasticsearchException, IOException {
        RestHighLevelClient client = AtlasElasticsearchDatabase.getClient();
        ClusterHealthRequest request = new ClusterHealthRequest(Constants.INDEX_PREFIX + Constants.VERTEX_INDEX);
        ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
        RestStatus restStatus = response.status();
        if (restStatus.toString().equals(ELASTICSEARCH_REST_STATUS_OK)){
            ClusterHealthStatus status = response.getStatus();
            if (status.toString().equals(ELASTICSEARCH_CLUSTER_STATUS_GREEN) || status.toString().equals(ELASTICSEARCH_CLUSTER_STATUS_YELLOW)) {
                return true;
            }
        } else {
            LOG.error("isElasticsearchHealthy => ES health check request timed out!");
        }
        return false;
    }
}
