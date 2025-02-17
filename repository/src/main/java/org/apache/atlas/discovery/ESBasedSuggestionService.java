package org.apache.atlas.discovery;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.type.AtlasType;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ESBasedSuggestionService {
    private static final Logger LOG = LoggerFactory.getLogger(ESBasedSuggestionService.class);

    private RestClient esRestClient;

    public static final String INDEX_NAME = "suggest";

    public ESBasedSuggestionService(RestClient lowLevelClient) {
        this.esRestClient = lowLevelClient;
    }

    public SuggestionResponse searchSuggestions(String queryStr) throws IOException {
        Request queryRequest = new Request("POST", "/autocomplete/_search");
        queryRequest.setJsonEntity(queryStr);
        Response response = esRestClient.performRequest(queryRequest);

        SuggestionResponse suggestionResponse = new SuggestionResponse();
        String esResponseString =  EntityUtils.toString(response.getEntity());

        // Parse the response and return the suggestions
        Map<String, Object> responseMap = AtlasType.fromJson(esResponseString, Map.class);
        suggestionResponse.setResponseMap(responseMap);

        return suggestionResponse;
    }

    @JsonAutoDetect(getterVisibility=JsonAutoDetect.Visibility.PUBLIC_ONLY, setterVisibility=JsonAutoDetect.Visibility.PUBLIC_ONLY, fieldVisibility=JsonAutoDetect.Visibility.PUBLIC_ONLY)
    @JsonSerialize(include=JsonSerialize.Inclusion.ALWAYS)
    @JsonIgnoreProperties(ignoreUnknown=true)
    public class SuggestionResponse {

        public SuggestionResponse() { }
        private Map<String, Object> responseMap = new LinkedHashMap<>();

        public Map<String, Object> getResponseMap() {
            return responseMap;
        }

        public void setResponseMap(Map<String, Object> responseMap) {
            this.responseMap = responseMap;
        }

    }


}