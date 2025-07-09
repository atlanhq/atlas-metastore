package org.apache.atlas.model.instance;

import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchResponse;

/**
 * Wrapper class to handle different types of Elasticsearch responses.
 */
public class DirectSearchResponse {
    private final SearchResponse searchResponse;
    private final OpenPointInTimeResponse pitCreateResponse;
    private final ClosePointInTimeResponse pitDeleteResponse;
    private final String responseType;

    private DirectSearchResponse(SearchResponse searchResponse, OpenPointInTimeResponse pitCreateResponse, 
                               ClosePointInTimeResponse pitDeleteResponse, String responseType) {
        this.searchResponse = searchResponse;
        this.pitCreateResponse = pitCreateResponse;
        this.pitDeleteResponse = pitDeleteResponse;
        this.responseType = responseType;
    }

    public static DirectSearchResponse fromSearchResponse(SearchResponse response) {
        return new DirectSearchResponse(response, null, null, "SEARCH");
    }

    public static DirectSearchResponse fromPitCreateResponse(OpenPointInTimeResponse response) {
        return new DirectSearchResponse(null, response, null, "PIT_CREATE");
    }

    public static DirectSearchResponse fromPitDeleteResponse(ClosePointInTimeResponse response) {
        return new DirectSearchResponse(null, null, response, "PIT_DELETE");
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public OpenPointInTimeResponse getPitCreateResponse() {
        return pitCreateResponse;
    }

    public ClosePointInTimeResponse getPitDeleteResponse() {
        return pitDeleteResponse;
    }

    public String getResponseType() {
        return responseType;
    }

    @Override
    public String toString() {
        return switch (responseType) {
            case "SEARCH" -> searchResponse != null ? searchResponse.toString() : "null";
            case "PIT_CREATE" -> pitCreateResponse != null ? pitCreateResponse.toString() : "null";
            case "PIT_DELETE" -> pitDeleteResponse != null ? pitDeleteResponse.toString() : "null";
            default -> "Unknown response type";
        };
    }
} 