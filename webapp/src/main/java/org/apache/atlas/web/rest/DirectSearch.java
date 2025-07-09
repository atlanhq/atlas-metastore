package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.DirectSearchRequest;
import org.apache.atlas.model.instance.DirectSearchResponse;
import org.apache.atlas.repository.audit.ESBasedAuditRepository;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.elasticsearch.action.search.*;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import org.slf4j.Logger;

import static org.apache.atlas.repository.util.AccessControlUtils.ARGO_SERVICE_USER_NAME;

/**
 * REST endpoint for direct Elasticsearch operations in Atlas.
 * Provides functionality for simple search and Point-in-Time (PIT) operations.
 */
@Path("direct")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class DirectSearch {

    private static final long DEFAULT_KEEPALIVE = 60000L; // 60 seconds
    private static final long MIN_KEEPALIVE = 1000L; // 1 second
    private static final long MAX_KEEPALIVE = 300000L; // 5 minutes
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.DirectSearch");
    private static final Logger LOG = LoggerFactory.getLogger(DirectSearch.class);

    private final ESBasedAuditRepository es;

    @Inject
    public DirectSearch(ESBasedAuditRepository esBasedAuditRepository) {
        this.es = esBasedAuditRepository;
    }

    /**
     * Endpoint for performing direct Elasticsearch operations.
     * Supports simple search and Point-in-Time (PIT) operations.
     *
     * @param request The search request containing search type and parameters
     * @return DirectSearchResponse containing the appropriate response based on operation type
     * @throws AtlasBaseException if request validation fails or search operation fails
     * @throws IOException if there's an error processing the query
     */
    @Path("/search")
    @POST
    @Timed
    public DirectSearchResponse directSearch(DirectSearchRequest request) throws AtlasBaseException, IOException {
        
        // Ensure the current user is authorized to trigger this endpoint
        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Attribute update");
        }
                
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DirectSearch.directSearch()");
            }

            LOG.debug("==> DirectSearch.directSearch({})", request);
            
            validateRequest(request);

            DirectSearchResponse response = switch (request.getSearchType()) {
                case SIMPLE -> DirectSearchResponse.fromSearchResponse(handleSimpleSearch(request));
                case PIT_CREATE -> DirectSearchResponse.fromPitCreateResponse(handlePitCreate(request));
                case PIT_SEARCH -> DirectSearchResponse.fromSearchResponse(handlePitSearch(request));
                case PIT_DELETE -> DirectSearchResponse.fromPitDeleteResponse(handlePitDelete(request));
            };

            LOG.debug("<== DirectSearch.directSearch() - {}", response);
            return response;

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private void validateRequest(DirectSearchRequest request) throws AtlasBaseException {
        if (request == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Request cannot be null");
        }

        if (request.getSearchType() == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Search type is required");
        }

        if (request.getKeepAlive() != null) {
            if (request.getKeepAlive() < MIN_KEEPALIVE || request.getKeepAlive() > MAX_KEEPALIVE) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, 
                    String.format("KeepAlive must be between %d and %d milliseconds", MIN_KEEPALIVE, MAX_KEEPALIVE));
            }
        }

        switch (request.getSearchType()) {
            case SIMPLE:
                validateSimpleSearch(request);
                break;
            case PIT_CREATE:
                validatePitCreate(request);
                break;
            case PIT_SEARCH:
                validatePitSearch(request);
                break;
            case PIT_DELETE:
                validatePitDelete(request);
                break;
        }
    }

    private void validateSimpleSearch(DirectSearchRequest request) throws AtlasBaseException {
        if (request.getIndexName() == null || request.getIndexName().trim().isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Index name is required for simple search");
        }
        validateQuery(request);
    }

    private void validatePitCreate(DirectSearchRequest request) throws AtlasBaseException {
        if (request.getIndexName() == null || request.getIndexName().trim().isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Index name is required for PIT creation");
        }
    }

    private void validatePitSearch(DirectSearchRequest request) throws AtlasBaseException {
        if (request.getPitId() == null || request.getPitId().trim().isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "PIT ID is required for PIT search");
        }
        validateQuery(request);
    }

    private void validatePitDelete(DirectSearchRequest request) throws AtlasBaseException {
        if (request.getPitId() == null || request.getPitId().trim().isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "PIT ID is required for PIT deletion");
        }
    }

    private void validateQuery(DirectSearchRequest request) throws AtlasBaseException {
        if (request.getQuery() == null || request.getQuery().trim().isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Query is required");
        }
    }

    private SearchResponse handleSimpleSearch(DirectSearchRequest request) throws IOException, AtlasBaseException {
        try {
            LOG.debug("==> DirectSearch.handleSimpleSearch(indexName={}, query={})", request.getIndexName(), request.getQuery());
            
            SearchSourceBuilder srb = parseQuery(request.getQuery());
            SearchRequest searchRequest = new SearchRequest(request.getIndexName());
            searchRequest.source(srb);

            SearchResponse response = es.search(searchRequest);
            LOG.debug("<== DirectSearch.handleSimpleSearch() - {}", response);
            return response;
        } catch (IOException e) {
            LOG.error("Error in simple search", e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED, e.getMessage());
        }
    }

    private OpenPointInTimeResponse handlePitCreate(DirectSearchRequest request) throws AtlasBaseException {
        try {
            LOG.debug("==> DirectSearch.handlePitCreate(indexName={})", request.getIndexName());
            
            long keepAlive = request.getKeepAlive() != null ? request.getKeepAlive() : DEFAULT_KEEPALIVE;

            OpenPointInTimeRequest pitRequest = new OpenPointInTimeRequest(request.getIndexName())
                    .keepAlive(TimeValue.timeValueMillis(keepAlive));

            OpenPointInTimeResponse response = es.openPointInTime(pitRequest);
            LOG.debug("<== DirectSearch.handlePitCreate() - {}", response);
            return response;
        } catch (Exception e) {
            LOG.error("Error creating PIT", e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED, e.getMessage());
        }
    }

    private SearchResponse handlePitSearch(DirectSearchRequest request) throws IOException, AtlasBaseException {
        try {
            LOG.debug("==> DirectSearch.handlePitSearch(pitId={}, query={})", request.getPitId(), request.getQuery());
            
            SearchSourceBuilder srb = parseQuery(request.getQuery());
            long keepAlive = request.getKeepAlive() != null ? request.getKeepAlive() : DEFAULT_KEEPALIVE;

            srb.pointInTimeBuilder(
                    new PointInTimeBuilder(request.getPitId())
                            .setKeepAlive(TimeValue.timeValueMillis(keepAlive))
            );

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(srb);

            SearchResponse response = es.search(searchRequest);
            LOG.debug("<== DirectSearch.handlePitSearch() - {}", response);
            return response;
        } catch (IOException e) {
            LOG.error("Error in PIT search", e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED, e.getMessage());
        }
    }

    private ClosePointInTimeResponse handlePitDelete(DirectSearchRequest request) throws AtlasBaseException {
        try {
            LOG.debug("==> DirectSearch.handlePitDelete(pitId={})", request.getPitId());
            
            ClosePointInTimeRequest closeRequest = new ClosePointInTimeRequest(request.getPitId());
            ClosePointInTimeResponse response = es.closePointInTime(closeRequest);
            
            LOG.debug("<== DirectSearch.handlePitDelete() - {}", response);
            return response;
        } catch (Exception e) {
            LOG.error("Error deleting PIT", e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED, e.getMessage());
        }
    }

    private SearchSourceBuilder parseQuery(String query) throws IOException {
        try {
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(null, null, query);
            return SearchSourceBuilder.fromXContent(parser);
        } catch (IOException e) {
            LOG.error("Error parsing query: {}", query, e);
            throw new IOException("Failed to parse search query: " + e.getMessage(), e);
        }
    }
}
