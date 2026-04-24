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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase;
import org.apache.atlas.service.redis.RedisService;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Lightweight bulk purge service that removes all asset vertices under a connection
 * directly from JanusGraph (Cassandra) and Elasticsearch, bypassing the standard
 * delete path (no preprocessors, no Kafka, no audits, no lineage recalc).
 *
 * Designed for "clean slate" scenarios where an entire connection's assets need
 * to be wiped efficiently.
 *
 * Usage: Instantiated manually and run in a background thread (not a Spring bean).
 */
public class ConnectionBulkPurgeService implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionBulkPurgeService.class);

    private static final int    BATCH_SIZE         = 300;
    private static final int    ES_PAGE_SIZE       = 5000;
    private static final int    MAX_COMMIT_RETRIES = 3;
    private static final long   SCROLL_KEEP_ALIVE  = 5L; // minutes
    public  static final String REDIS_KEY_PREFIX   = "bulk_purge:";

    private final AtlasGraph   graph;
    private final RedisService redisService;
    private final String       connectionQualifiedName;

    public ConnectionBulkPurgeService(AtlasGraph graph, RedisService redisService, String connectionQualifiedName) {
        this.graph                   = graph;
        this.redisService            = redisService;
        this.connectionQualifiedName = connectionQualifiedName;
    }

    @Override
    public void run() {
        String redisKey = REDIS_KEY_PREFIX + connectionQualifiedName;

        try {
            LOG.info("ConnectionBulkPurge: Starting purge for connectionQualifiedName={}", connectionQualifiedName);
            redisService.putValue(redisKey, "IN_PROGRESS");

            // Phase 1: Discover all vertex IDs via ES scroll
            List<String> vertexIds = discoverVertexIds();
            LOG.info("ConnectionBulkPurge: Discovered {} vertices for connectionQualifiedName={}", vertexIds.size(), connectionQualifiedName);

            if (vertexIds.isEmpty()) {
                LOG.info("ConnectionBulkPurge: No vertices found, nothing to purge");
                redisService.putValue(redisKey, "SUCCESSFUL:0");
                return;
            }

            // Phase 2: Batch delete vertices from JanusGraph
            int deletedCount = batchDeleteVertices(vertexIds);
            LOG.info("ConnectionBulkPurge: Deleted {} vertices from JanusGraph", deletedCount);

            // Phase 3: ES safety-net delete_by_query
            long esDeleted = esCleanup();
            LOG.info("ConnectionBulkPurge: ES safety-net cleanup removed {} additional documents", esDeleted);

            String summary = String.format("SUCCESSFUL:%d", deletedCount);
            redisService.putValue(redisKey, summary);
            LOG.info("ConnectionBulkPurge: Completed purge for connectionQualifiedName={}. Total vertices deleted: {}", connectionQualifiedName, deletedCount);

        } catch (Exception e) {
            LOG.error("ConnectionBulkPurge: Failed for connectionQualifiedName={}", connectionQualifiedName, e);
            redisService.putValue(redisKey, "FAILED:" + e.getMessage());
        }
    }

    /**
     * Phase 1: Use ES scroll API to discover all vertex document IDs
     * matching the connectionQualifiedName. Uses scroll for consistent
     * snapshot-based iteration unaffected by concurrent mutations.
     */
    private List<String> discoverVertexIds() throws Exception {
        RestHighLevelClient esClient = AtlasElasticsearchDatabase.getClient();
        List<String> vertexIds = new ArrayList<>();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery(Constants.CONNECTION_QUALIFIED_NAME, connectionQualifiedName));
        sourceBuilder.size(ES_PAGE_SIZE);
        sourceBuilder.sort("_doc", SortOrder.ASC);
        sourceBuilder.fetchSource(false); // We only need the _id

        SearchRequest searchRequest = new SearchRequest(Constants.VERTEX_INDEX_NAME);
        searchRequest.source(sourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(SCROLL_KEEP_ALIVE));

        String scrollId = null;

        try {
            SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);
            scrollId = response.getScrollId();

            while (response.getHits().getHits().length > 0) {
                for (SearchHit hit : response.getHits().getHits()) {
                    vertexIds.add(hit.getId());
                }

                LOG.info("ConnectionBulkPurge: Discovered {} vertex IDs so far...", vertexIds.size());

                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(TimeValue.timeValueMinutes(SCROLL_KEEP_ALIVE));
                response = esClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = response.getScrollId();
            }
        } finally {
            if (scrollId != null) {
                try {
                    ClearScrollRequest clearRequest = new ClearScrollRequest();
                    clearRequest.addScrollId(scrollId);
                    esClient.clearScroll(clearRequest, RequestOptions.DEFAULT);
                } catch (Exception e) {
                    LOG.warn("ConnectionBulkPurge: Failed to clear scroll context", e);
                }
            }
        }

        return vertexIds;
    }

    /**
     * Phase 2: Delete vertices from JanusGraph in batches.
     * For each vertex, removes all edges first (clean bilateral removal),
     * then removes the vertex. Commits after each batch with retry logic.
     */
    private int batchDeleteVertices(List<String> vertexIds) {
        int totalDeleted  = 0;
        int totalBatches  = (vertexIds.size() + BATCH_SIZE - 1) / BATCH_SIZE;
        int currentBatch  = 0;

        for (int i = 0; i < vertexIds.size(); i += BATCH_SIZE) {
            currentBatch++;
            int end = Math.min(i + BATCH_SIZE, vertexIds.size());
            List<String> batch = vertexIds.subList(i, end);
            int batchDeleted = 0;

            for (String vertexId : batch) {
                try {
                    AtlasVertex vertex = graph.getVertex(vertexId);

                    if (vertex == null) {
                        continue;
                    }

                    // Remove all edges first for clean bilateral cleanup
                    Iterable<AtlasEdge> edges = vertex.getEdges(AtlasEdgeDirection.BOTH);
                    Iterator<AtlasEdge> edgeIterator = edges.iterator();

                    while (edgeIterator.hasNext()) {
                        AtlasEdge edge = edgeIterator.next();

                        try {
                            graph.removeEdge(edge);
                        } catch (Exception e) {
                            LOG.debug("ConnectionBulkPurge: Failed to remove edge for vertex {}, skipping", vertexId, e);
                        }
                    }

                    graph.removeVertex(vertex);
                    batchDeleted++;
                } catch (Exception e) {
                    LOG.warn("ConnectionBulkPurge: Failed to remove vertex {}, skipping", vertexId, e);
                }
            }

            commitWithRetry(currentBatch, totalBatches);
            totalDeleted += batchDeleted;

            LOG.info("ConnectionBulkPurge: Committed batch {}/{} ({} vertices deleted, {} total)",
                    currentBatch, totalBatches, batchDeleted, totalDeleted);
        }

        return totalDeleted;
    }

    /**
     * Commit the current JanusGraph transaction with retry logic.
     * Pattern from ConcurrentPatchProcessor.
     */
    private void commitWithRetry(int currentBatch, int totalBatches) {
        for (int attempt = 1; attempt <= MAX_COMMIT_RETRIES; attempt++) {
            try {
                graph.commit();
                return;
            } catch (Exception e) {
                LOG.error("ConnectionBulkPurge: Commit failed (batch {}/{}, attempt {}/{})",
                        currentBatch, totalBatches, attempt, MAX_COMMIT_RETRIES, e);

                if (attempt < MAX_COMMIT_RETRIES) {
                    try {
                        Thread.sleep(300L * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.error("ConnectionBulkPurge: Interrupted during commit retry backoff");
                        return;
                    }
                }
            }
        }

        LOG.error("ConnectionBulkPurge: All {} commit retries exhausted for batch {}/{}",
                MAX_COMMIT_RETRIES, currentBatch, totalBatches);
    }

    /**
     * Phase 3: Safety-net ES delete_by_query to remove any documents
     * that may not have been cleaned up by JanusGraph commits.
     */
    private long esCleanup() throws Exception {
        RestHighLevelClient esClient = AtlasElasticsearchDatabase.getClient();

        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(Constants.VERTEX_INDEX_NAME);
        deleteRequest.setQuery(QueryBuilders.termQuery(Constants.CONNECTION_QUALIFIED_NAME, connectionQualifiedName));
        deleteRequest.setConflicts("proceed");
        deleteRequest.setRefresh(true);

        BulkByScrollResponse response = esClient.deleteByQuery(deleteRequest, RequestOptions.DEFAULT);

        return response.getDeleted();
    }
}
