package org.apache.atlas.repository.search;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase;
import org.apache.atlas.service.Service;
import org.apache.atlas.type.AtlasType;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.xcontent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Elasticsearch implementation of DirectSearchRepository.
 */
@Singleton
@Component
@Order(8)
public class ESDirectSearchRepository implements DirectSearchRepository, Service {
    private static final Logger LOG = LoggerFactory.getLogger(ESDirectSearchRepository.class);

    @Inject
    public ESDirectSearchRepository() {
        // Constructor for dependency injection
    }

    @Override
    public void start() throws AtlasException {
        LOG.info("ESDirectSearchRepository - start! (Using shared ES client)");
        // Using centralized ES client from AtlasElasticsearchDatabase
    }

    @Override
    public void stop() throws AtlasException {
        LOG.info("ESDirectSearchRepository - stop! (Using shared ES client - no cleanup needed)");
        // No client cleanup needed - using shared AtlasElasticsearchDatabase client
    }



    @Override
    public Map<String, Object> searchWithRawJson(String indexName, String queryJson) throws AtlasBaseException {
        try {
            Request request = new Request("POST", indexName + "/_search");
            request.setJsonEntity(queryJson);

            Response response = AtlasElasticsearchDatabase.getLowLevelClient().performRequest(request);
            
            // Memory-efficient: Stream parse instead of loading entire response as string
            Map<String, Object> searchResponse;
            try (InputStream inputStream = response.getEntity().getContent()) {
                searchResponse = AtlasType.fromJson(inputStream, Map.class);
            }
            
            LOG.debug("<== ESDirectSearchRepository.searchWithRawJson() - found {} hits",
                    ((Map)((Map)searchResponse.get("hits")).get("total")).get("value"));
            return searchResponse;
        } catch (IOException e) {
            LOG.error("Error performing raw JSON search on index {}: query={}, error={}",
                    indexName, queryJson, e.getMessage(), e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED,
                    String.format("Search failed on index %s: %s", indexName, e.getMessage()));
        }
    }

    @Override
    public OpenPointInTimeResponse openPointInTime(OpenPointInTimeRequest pitRequest) throws AtlasBaseException {
        if (pitRequest == null || pitRequest.indices() == null || pitRequest.indices().length == 0) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "PIT request or indices cannot be null");
        }

        String indexName = pitRequest.indices()[0];
        LOG.debug("==> ESDirectSearchRepository.openPointInTime(index={}, keepAlive={})",
                indexName, pitRequest.keepAlive());

        try {
            String keepAliveParam = String.format("keep_alive=%s", pitRequest.keepAlive());
            String endpoint = indexName + "/_pit?" + keepAliveParam;

            Request request = new Request("POST", endpoint);
            request.setOptions(RequestOptions.DEFAULT.toBuilder()
                    .addHeader("Content-Type", ContentType.APPLICATION_JSON.getMimeType())
                    .build());
            Response response = AtlasElasticsearchDatabase.getLowLevelClient().performRequest(request);
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(NamedXContentRegistry.EMPTY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            response.getEntity().getContent())) {
                OpenPointInTimeResponse pitResponse = OpenPointInTimeResponse.fromXContent(parser);
                LOG.debug("<== ESDirectSearchRepository.openPointInTime() - created PIT: {}",
                        pitResponse.getPointInTimeId());
                return pitResponse;
            }
        } catch (IOException e) {
            LOG.error("Error opening PIT for index {}, keepAlive={}: {}",
                    indexName, pitRequest.keepAlive(), e.getMessage(), e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED,
                    String.format("Failed to open PIT for index %s: %s", indexName, e.getMessage()));
        }
    }

    @Override
    public ClosePointInTimeResponse closePointInTime(ClosePointInTimeRequest closeRequest) throws AtlasBaseException {
        if (closeRequest == null || closeRequest.getId() == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Close request or PIT ID cannot be null");
        }

        String pitId = closeRequest.getId();
        LOG.debug("==> ESDirectSearchRepository.closePointInTime(pitId={})", pitId);

        try {
            Request request = new Request("DELETE", "/_pit");

            // Create request body with PIT ID
            String requestBody = String.format("{\"id\": \"%s\"}", pitId);
            HttpEntity entity = new StringEntity(requestBody, ContentType.APPLICATION_JSON);
            request.setEntity(entity);

            Response response = AtlasElasticsearchDatabase.getLowLevelClient().performRequest(request);

            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(NamedXContentRegistry.EMPTY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            response.getEntity().getContent())) {
                ClosePointInTimeResponse closeResponse = ClosePointInTimeResponse.fromXContent(parser);
                LOG.debug("<== ESDirectSearchRepository.closePointInTime() - closed PIT: {}, succeeded: {}",
                        pitId, closeResponse.isSucceeded());
                return closeResponse;
            }
        } catch (IOException e) {
            LOG.error("Error closing PIT ID {}: {}", pitId, e.getMessage(), e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED,
                    String.format("Failed to close PIT %s: %s", pitId, e.getMessage()));
        }
    }
}