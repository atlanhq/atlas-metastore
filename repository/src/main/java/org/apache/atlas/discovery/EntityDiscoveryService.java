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
package org.apache.atlas.discovery;

import org.apache.atlas.*;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasSearchResultScrubRequest;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.*;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.searchlog.SearchLogSearchParams;
import org.apache.atlas.model.searchlog.SearchLogSearchResult;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery.Result;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.util.AccessControlUtils;
import org.apache.atlas.searchlog.ESSearchLogger;
import org.apache.atlas.type.*;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.*;

@Component
public class EntityDiscoveryService implements AtlasDiscoveryService {
    private static final Logger LOG = LoggerFactory.getLogger(EntityDiscoveryService.class);

    private final AtlasGraph                      graph;
    private final EntityGraphRetriever            entityRetriever;
    private final AtlasTypeRegistry               typeRegistry;

    @Inject
    public EntityDiscoveryService(AtlasTypeRegistry typeRegistry,
                           AtlasGraph graph) throws AtlasException {
        this.graph                    = graph;
        this.entityRetriever          = new EntityGraphRetriever(this.graph, typeRegistry);
        this.typeRegistry             = typeRegistry;
    }

    private void scrubSearchResults(AtlasSearchResult result, boolean suppressLogs) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder scrubSearchResultsMetrics = RequestContext.get().startMetricRecord("scrubSearchResults");
        AtlasAuthorizationUtils.scrubSearchResults(new AtlasSearchResultScrubRequest(typeRegistry, result), suppressLogs);
        RequestContext.get().endMetricRecord(scrubSearchResultsMetrics);
    }


    @Override
    public AtlasSearchResult directIndexSearch(SearchParams searchParams) throws AtlasBaseException {
        IndexSearchParams params = (IndexSearchParams) searchParams;
        RequestContext.get().setRelationAttrsForSearch(params.getRelationAttributes());
        RequestContext.get().setAllowDeletedRelationsIndexsearch(params.isAllowDeletedRelations());
        RequestContext.get().setIncludeRelationshipAttributes(params.isIncludeRelationshipAttributes());

        AtlasSearchResult ret = new AtlasSearchResult();
        AtlasIndexQuery indexQuery;

        ret.setSearchParameters(searchParams);
        ret.setQueryType(AtlasQueryType.INDEX);

        Set<String> resultAttributes = new HashSet<>();
        if (CollectionUtils.isNotEmpty(searchParams.getAttributes())) {
            resultAttributes.addAll(searchParams.getAttributes());
        }

        try {
            if(LOG.isDebugEnabled()){
                LOG.debug("Performing ES search for the params ({})", searchParams);
            }

            String indexName = getIndexName(params);

            indexQuery = graph.elasticsearchQuery(indexName);
            AtlasPerfMetrics.MetricRecorder elasticSearchQueryMetric = RequestContext.get().startMetricRecord("elasticSearchQuery");
            DirectIndexQueryResult indexQueryResult = indexQuery.vertices(searchParams);
            if (indexQueryResult == null) {
                return null;
            }
            RequestContext.get().endMetricRecord(elasticSearchQueryMetric);
            prepareSearchResult(ret, indexQueryResult, resultAttributes, true);

            ret.setAggregations(indexQueryResult.getAggregationMap());
            ret.setApproximateCount(indexQuery.vertexTotals());
        } catch (Exception e) {
            LOG.error("Error while performing direct search for the params ({}), {}", searchParams, e.getMessage());
            throw e;
        }
        return ret;
    }

    @Override
    public AtlasSearchResult directRelationshipIndexSearch(SearchParams searchParams) throws AtlasBaseException {
        AtlasSearchResult ret = new AtlasSearchResult();
        AtlasIndexQuery indexQuery;

        ret.setSearchParameters(searchParams);
        ret.setQueryType(AtlasQueryType.INDEX);

        try {
            if(LOG.isDebugEnabled()){
                LOG.debug("Performing ES relationship search for the params ({})", searchParams);
            }

            indexQuery = graph.elasticsearchQuery(EDGE_INDEX_NAME);
            AtlasPerfMetrics.MetricRecorder elasticSearchQueryMetric = RequestContext.get().startMetricRecord("elasticSearchQueryEdge");
            DirectIndexQueryResult indexQueryResult = indexQuery.vertices(searchParams);
            if (indexQueryResult == null) {
                return null;
            }
            RequestContext.get().endMetricRecord(elasticSearchQueryMetric);

            //Note: AtlasSearchResult.entities are not supported yet

            ret.setAggregations(indexQueryResult.getAggregationMap());
            ret.setApproximateCount(indexQuery.vertexTotals());
        } catch (Exception e) {
            LOG.error("Error while performing direct relationship search for the params ({}), {}", searchParams, e.getMessage());
            throw e;
        }
        return ret;
    }

    @Override
    public SearchLogSearchResult searchLogs(SearchLogSearchParams searchParams) throws AtlasBaseException {
        SearchLogSearchResult ret = new SearchLogSearchResult();
        ret.setSearchParameters(searchParams);
        AtlasIndexQuery indexQuery = null;

        try {
            indexQuery = graph.elasticsearchQuery(ESSearchLogger.INDEX_NAME);
            Map<String, Object> result = indexQuery.directIndexQuery(searchParams.getQueryString());

            if (result.get("total") != null)
                ret.setApproximateCount( ((Integer) result.get("total")).longValue());

            List<LinkedHashMap> hits = (List<LinkedHashMap>) result.get("data");

            List<Map<String, Object>> logs = hits.stream().map(x -> (HashMap<String, Object>) x.get("_source")).collect(Collectors.toList());

            ret.setLogs(logs);
            ret.setAggregations((Map<String, Object>) result.get("aggregations"));

            return ret;
        } catch (AtlasBaseException be) {
            throw be;
        }
    }

    private void prepareSearchResult(AtlasSearchResult ret, DirectIndexQueryResult indexQueryResult, Set<String> resultAttributes, boolean fetchCollapsedResults) throws AtlasBaseException {
        SearchParams searchParams = ret.getSearchParameters();
        try {
            if(LOG.isDebugEnabled()){
                LOG.debug("Preparing search results for ({})", ret.getSearchParameters());
            }
            Iterator<Result> iterator = indexQueryResult.getIterator();
            boolean showSearchScore = searchParams.getShowSearchScore();
            if (iterator == null) {
                return;
            }

            while (iterator.hasNext()) {
                Result result = iterator.next();
                AtlasVertex vertex = result.getVertex();

                if (vertex == null) {
                    LOG.warn("vertex in null");
                    continue;
                }

                AtlasEntityHeader header = entityRetriever.toAtlasEntityHeader(vertex, resultAttributes);
                if(RequestContext.get().includeClassifications()){
                    header.setClassifications(entityRetriever.getAllClassifications(vertex));
                }
                if (showSearchScore) {
                    ret.addEntityScore(header.getGuid(), result.getScore());
                }
                if (fetchCollapsedResults) {
                    Map<String, AtlasSearchResult> collapse = new HashMap<>();

                    Set<String> collapseKeys = result.getCollapseKeys();
                    for (String collapseKey : collapseKeys) {
                        AtlasSearchResult collapseRet = new AtlasSearchResult();
                        collapseRet.setSearchParameters(ret.getSearchParameters());

                        Set<String> collapseResultAttributes = new HashSet<>();
                        if (searchParams.getCollapseAttributes() != null) {
                            collapseResultAttributes.addAll(searchParams.getCollapseAttributes());
                        } else {
                            collapseResultAttributes = resultAttributes;
                        }

                        if (searchParams.getCollapseRelationAttributes() != null) {
                            RequestContext.get().getRelationAttrsForSearch().clear();
                            RequestContext.get().setRelationAttrsForSearch(searchParams.getCollapseRelationAttributes());
                        }

                        DirectIndexQueryResult indexQueryCollapsedResult = result.getCollapseVertices(collapseKey);
                        collapseRet.setApproximateCount(indexQueryCollapsedResult.getApproximateCount());
                        prepareSearchResult(collapseRet, indexQueryCollapsedResult, collapseResultAttributes, false);

                        collapseRet.setSearchParameters(null);
                        collapse.put(collapseKey, collapseRet);
                    }
                    if (!collapse.isEmpty()) {
                        header.setCollapse(collapse);
                    }
                }
                if (searchParams.getShowSearchMetadata()) {
                    ret.addHighlights(header.getGuid(), result.getHighLights());
                    ret.addSort(header.getGuid(), result.getSort());
                } else if (searchParams.getShowHighlights()) {
                    ret.addHighlights(header.getGuid(), result.getHighLights());
                }

                ret.addEntity(header);
            }
        } catch (Exception e) {
                throw e;
        }
        scrubSearchResults(ret, searchParams.getSuppressLogs());
    }

    private Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    public List<AtlasEntityHeader> searchUsingTermQualifiedName(int from, int size, String termQName,
                                                        Set<String> attributes, Set<String>relationAttributes) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = getMap("from", from);
        dsl.put("size", size);
        dsl.put("query", getMap("term", getMap("__meanings", getMap("value",termQName))));

        indexSearchParams.setDsl(dsl);
        indexSearchParams.setAttributes(attributes);
        indexSearchParams.setRelationAttributes(relationAttributes);
        AtlasSearchResult searchResult = null;
        searchResult = directIndexSearch(indexSearchParams);
        List<AtlasEntityHeader> entityHeaders = searchResult.getEntities();
        return  entityHeaders;
    }

    private String getIndexName(IndexSearchParams params) throws AtlasBaseException {
        String vertexIndexName = getESIndex();

        if (StringUtils.isEmpty(params.getPersona()) && StringUtils.isEmpty(params.getPurpose())) {
            return vertexIndexName;
        }

        String qualifiedName = "";
        if (StringUtils.isNotEmpty(params.getPersona())) {
            qualifiedName = params.getPersona();
        } else {
            qualifiedName = params.getPurpose();
        }

        String aliasName = AccessControlUtils.getESAliasName(qualifiedName);

        if (StringUtils.isNotEmpty(aliasName)) {
            if(params.isAccessControlExclusive()) {
                accessControlExclusiveDsl(params, aliasName);
                aliasName = aliasName+","+vertexIndexName;
            }
            return aliasName;
        } else {
            throw new AtlasBaseException("ES alias not found for purpose/persona " + params.getPurpose());
        }
    }

    private void accessControlExclusiveDsl(IndexSearchParams params, String aliasName) {

        List<Map<String, Object>> mustClauses = new ArrayList<>();
        Map<String, Object> clientQuery = (Map<String, Object>) params.getDsl().get("query");

        mustClauses.add(clientQuery);

        List<Map<String, Object>>filterClauses = new ArrayList<>();
        filterClauses.add(getMap("terms", getMap("_index", Collections.singletonList(aliasName))));

        Map<String, Object> boolQuery = new HashMap<>();
        boolQuery.put("must", mustClauses);
        boolQuery.put("filter",filterClauses);

        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        shouldClauses.add(getMap("bool", boolQuery));
        shouldClauses.add(getStaticBoolQuery());

        Map<String, Object> topBoolQuery = getMap("bool", getMap("should", shouldClauses));

        Map copyOfDsl = new HashMap(params.getDsl());
        copyOfDsl.put("query", topBoolQuery);

        params.setDsl(copyOfDsl);
    }

    private Map<String, Object> getStaticBoolQuery() {
        List<Map<String, Object>> mustClauses = new ArrayList<>();
        Map<String, Object> mustClause = getMap("bool", getMap("should", Arrays.asList(
                getMap("term", getMap("daapVisibility", "Public")),
                getMap("term", getMap("daapVisibility", "Protected"))
        )));
        mustClauses.add(mustClause);

        List<Map<String, Object>>filterClauses = new ArrayList<>();
        filterClauses.add(getMap("terms", getMap("_index", Collections.singletonList(VERTEX_INDEX_NAME))));

        Map<String, Object> boolQuery = new HashMap<>();
        boolQuery.put("must", mustClauses);
        boolQuery.put("filter", filterClauses);

        return getMap("bool", boolQuery);
    }
}
