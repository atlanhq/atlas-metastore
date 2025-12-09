package org.apache.atlas.web.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.store.graph.v2.bulk.IngestionDAO;
import org.apache.atlas.repository.store.graph.v2.bulk.IngestionService;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.atlas.service.metrics.MetricUtils;

import static javax.ws.rs.core.Response.*;
import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;

@Path("/dump")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class IngestionDumpREST {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.IngestionDumpREST");
    private static final ObjectMapper mapper = new ObjectMapper();

    private final IngestionService ingestionService;

    @Inject
    public IngestionDumpREST(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @POST
    public Response submit(InputStream requestBodyStream, @Context HttpServletRequest httpServletRequest) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "IngestionDumpREST.submit()");
            }

            if (requestBodyStream == null) {
                throw new AtlasBaseException(BAD_REQUEST, "Request body cannot be empty");
            }

            Map<String, String> queryParams = getStringStringMap(httpServletRequest);

            String requestId = ingestionService.submit(requestBodyStream, queryParams);
            MetricUtils.getMeterRegistry().counter("ingestion.requests.submitted").increment();
            
            Map<String, String> response = Collections.singletonMap("requestId", requestId);
            return Response.status(Status.ACCEPTED).entity(response).build();

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private static Map<String, String> getStringStringMap(HttpServletRequest httpServletRequest) {
        Map<String, String[]> parameterMap = httpServletRequest.getParameterMap();
        Map<String, String> queryParams = new HashMap<>();
        for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
            if (entry.getValue() != null && entry.getValue().length > 0) {
                queryParams.put(entry.getKey(), entry.getValue()[0]);
            }
        }
        return queryParams;
    }

    @GET
    @Path("status/{requestId}")
    public Response getStatus(@PathParam("requestId") String requestId) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "IngestionDumpREST.getStatus(" + requestId + ")");
            }

            IngestionDAO.IngestionRequest request = ingestionService.getStatus(requestId);
            
            if (request == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, "Request ID " + requestId + " not found");
            }
            
            return ok(request).build();

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }
}
