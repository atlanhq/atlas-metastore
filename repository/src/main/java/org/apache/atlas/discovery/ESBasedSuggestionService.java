package org.apache.atlas.discovery;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.type.AtlasType;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
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
        // Build the suggestor query for ES
        String suggestorName = "suggest_keyword";
        Request queryRequest = new Request("POST", "/autocomplete/_search");
        queryRequest.setJsonEntity(queryStr);
        Response response = esRestClient.performRequest(queryRequest);

        SuggestionResponse suggestionResponse = new SuggestionResponse();
        String esResponseString =  EntityUtils.toString(response.getEntity());

        // Parse the response and return the suggestions
        Map<String, Object> responseMap = AtlasType.fromJson(esResponseString, Map.class);
        Map<String, Object> suggestMap = (Map<String, Object>) responseMap.get("suggest");
        ArrayList<LinkedHashMap> suggestionMap = (ArrayList<LinkedHashMap>) suggestMap.get(suggestorName);
        LinkedHashMap suggestions = suggestionMap.get(0);
        List<LinkedHashMap> options = (List<LinkedHashMap>) suggestions.get("options");
        for (LinkedHashMap option : options) {
            suggestionResponse.addSuggestion((String) option.get("text"));
        }

        return suggestionResponse;
    }

    @JsonAutoDetect(getterVisibility=JsonAutoDetect.Visibility.PUBLIC_ONLY, setterVisibility=JsonAutoDetect.Visibility.PUBLIC_ONLY, fieldVisibility=JsonAutoDetect.Visibility.PUBLIC_ONLY)
    @JsonSerialize(include=JsonSerialize.Inclusion.ALWAYS)
    @JsonIgnoreProperties(ignoreUnknown=true)
    public class SuggestionResponse {

        public SuggestionResponse() { }
        private List<String> suggestions = new ArrayList<>();

        public List<String> getSuggestions() {
            return suggestions;
        }

        public void addSuggestion(String suggestion) {
            this.suggestions.add(suggestion);
        }

        public void setSuggestions(List<String> suggestions) {
            this.suggestions = suggestions;
        }
    }


}