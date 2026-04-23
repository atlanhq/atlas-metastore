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

import org.apache.atlas.type.AtlasType;

import java.util.*;

/**
 * Lightweight wrapper around a raw Elasticsearch hit (the {@code LinkedHashMap}
 * from {@code ResultImplDirect.getRawHit()}).
 *
 * Provides typed accessors for standard ES hit fields (_id, _score, highlight, sort,
 * inner_hits) without coupling the pipeline stages to the ES response format.
 */
public class ESHitResult {

    private final LinkedHashMap<String, Object> hit;
    private Map<String, LinkedHashMap> innerHitsMap;

    public ESHitResult(LinkedHashMap<String, Object> hit) {
        this.hit = hit;
        if (hit != null && hit.get("inner_hits") != null) {
            this.innerHitsMap = AtlasType.fromJson(AtlasType.toJson(hit.get("inner_hits")), Map.class);
        }
    }

    /**
     * ES document _id. On ZeroGraph, this IS the Cassandra vertex_id.
     * On JanusGraph, this is a LongEncoding-encoded vertex ID.
     */
    public String getDocumentId() {
        return hit != null ? String.valueOf(hit.get("_id")) : null;
    }

    /**
     * ES relevance score. Returns -1 if not available (e.g., sort-based queries).
     */
    public double getScore() {
        if (hit == null) return -1;
        Object score = hit.get("_score");
        if (score == null) return -1;
        return Double.parseDouble(String.valueOf(score));
    }

    /**
     * ES highlight fragments keyed by field name.
     */
    @SuppressWarnings("unchecked")
    public Map<String, List<String>> getHighlights() {
        if (hit == null) return Collections.emptyMap();
        Object highlight = hit.get("highlight");
        if (highlight != null) {
            return (Map<String, List<String>>) highlight;
        }
        return Collections.emptyMap();
    }

    /**
     * ES sort values for search_after pagination.
     */
    @SuppressWarnings("unchecked")
    public ArrayList<Object> getSort() {
        if (hit == null) return new ArrayList<>();
        Object sort = hit.get("sort");
        if (sort instanceof List) {
            return (ArrayList<Object>) sort;
        }
        return new ArrayList<>();
    }

    /**
     * Inner hits map for collapse/field_collapse results.
     * Each key is a collapse field, value is the inner hits response.
     */
    public Map<String, LinkedHashMap> getInnerHits() {
        return innerHitsMap;
    }

    /**
     * Check if this hit has inner hits (collapse results).
     */
    public boolean hasInnerHits() {
        return innerHitsMap != null && !innerHitsMap.isEmpty();
    }

    /**
     * Access the raw ES hit map. Use sparingly — prefer typed accessors.
     */
    public LinkedHashMap<String, Object> getRawHit() {
        return hit;
    }
}
