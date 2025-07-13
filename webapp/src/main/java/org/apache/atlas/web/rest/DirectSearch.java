package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.DirectSearchRequest;
import org.apache.atlas.model.instance.DirectSearchResponse;
import org.apache.atlas.repository.audit.ESBasedAuditRepository;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.core.TimeValue;
import org.slf4j.Logger;
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
import java.util.Map;

import static org.apache.atlas.repository.util.AccessControlUtils.ARGO_SERVICE_USER_NAME;

@Path("direct")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class DirectSearch {
    private static final long DEFAULT_KEEPALIVE = 60000L; // 60 seconds
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.DirectSearch");
    private static final Logger LOG = LoggerFactory.getLogger(DirectSearch.class);

    private final ESBasedAuditRepository es;

    @Inject
    public DirectSearch(ESBasedAuditRepository esBasedAuditRepository) {
        this.es = esBasedAuditRepository;
    }

    @Path("/search")
    @POST
    @Timed
    public DirectSearchResponse directSearch(DirectSearchRequest request) throws AtlasBaseException, IOException {
        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Direct search");
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DirectSearch.directSearch()");
            }

            LOG.debug("==> DirectSearch.directSearch({})", request);
            validateRequest(request);

            DirectSearchResponse response;
            switch (request.getSearchType()) {
                case SIMPLE -> {
                    Map<String, Object> searchResponse = handleSimpleSearch(request);
                    response = DirectSearchResponse.fromSearchResponse(searchResponse);
                }
                case PIT_CREATE -> response = DirectSearchResponse.fromPitCreateResponse(handlePitCreate(request));
                case PIT_SEARCH -> {
                    Map<String, Object> searchResponse = handlePitSearch(request);
                    response = DirectSearchResponse.fromSearchResponse(searchResponse);
                }
                case PIT_DELETE -> response = DirectSearchResponse.fromPitDeleteResponse(handlePitDelete(request));
                default -> throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Invalid search type");
            }

            LOG.debug("<== DirectSearch.directSearch() - {}", response);
            return response;
        } catch (Exception e) {
            LOG.error("Error processing direct search request: {}", request, e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED, e.getMessage());
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

        switch (request.getSearchType()) {
            case SIMPLE -> {
                if (request.getIndexName() == null || request.getIndexName().trim().isEmpty()) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Index name is required for simple search");
                }
                if (request.getQuery() == null) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Query is required for simple search");
                }
            }
            case PIT_CREATE -> {
                if (request.getIndexName() == null || request.getIndexName().trim().isEmpty()) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Index name is required for PIT creation");
                }
            }
            case PIT_SEARCH -> {
                if (request.getQuery() == null) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Query is required for PIT search");
                }
                Map<String, Object> query = request.getQuery();
                if (!hasPitSection(query)) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "PIT section is required in query for PIT search");
                }
            }
            case PIT_DELETE -> {
                if (request.getPitId() == null || request.getPitId().trim().isEmpty()) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "PIT ID is required for PIT deletion");
                }
            }
        }
    }

    private boolean hasPitSection(Map<String, Object> query) {
        if (query == null || !query.containsKey("query")) return false;
        Map<String, Object> innerQuery = (Map<String, Object>) query.get("query");
        return innerQuery != null && innerQuery.containsKey("pit");
    }

    private Map<String, Object> handleSimpleSearch(DirectSearchRequest request) throws IOException, AtlasBaseException {
        try {
            LOG.debug("==> DirectSearch.handleSimpleSearch(indexName={}, query={})",
                    request.getIndexName(), request.getQuery());

            String queryJson = AtlasJson.toJson(request.getQuery());
            Map<String, Object> response = es.searchWithRawJson(request.getIndexName(), queryJson);
            
            LOG.debug("<== DirectSearch.handleSimpleSearch() - {}", response);
            return response;
        } catch (Exception e) {
            LOG.error("Error in simple search", e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED, e.getMessage());
        }
    }

    private Map<String, Object> handlePitSearch(DirectSearchRequest request) throws AtlasBaseException {
        try {
            LOG.debug("==> DirectSearch.handlePitSearch(query={})", request.getQuery());

            // Use empty index name for PIT search
            String queryJson = AtlasJson.toJson(request.getQuery());
            Map<String, Object> response = es.searchWithRawJson("", queryJson);

            LOG.debug("<== DirectSearch.handlePitSearch() - {}", response);
            return response;
        } catch (Exception e) {
            LOG.error("Error in PIT search", e);
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
}
