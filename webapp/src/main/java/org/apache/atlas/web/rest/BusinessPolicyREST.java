package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.BusinessPolicyRequest;
import org.apache.atlas.model.instance.LinkBusinessPolicyRequest;
import org.apache.atlas.model.instance.UnlinkBusinessPolicyRequest;
import org.apache.atlas.model.instance.MoveBusinessPolicyRequest;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.util.Objects;

import static org.apache.atlas.repository.util.AccessControlUtils.ARGO_SERVICE_USER_NAME;

@Path("business-policy")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class BusinessPolicyREST {

    private static final Logger LOG = LoggerFactory.getLogger(BusinessPolicyREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.BusinessPolicyREST");

    private final AtlasEntityStore entitiesStore;

    @Inject
    public BusinessPolicyREST(AtlasEntityStore entitiesStore) {
        this.entitiesStore = entitiesStore;
    }

    /**
     * Links a business policy to entities.
     * @param request    the request containing the GUIDs of the assets to link the policy to
     * @throws AtlasBaseException if there is an error during the linking process
     */
    @POST
    @Path("/link-business-policy")
    @Timed
    public void linkBusinessPolicy(final BusinessPolicyRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("linkBusinessPolicy");

        // Ensure the current user is authorized to link policies
        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Policy linking");
        }


        // Set request context parameters
        RequestContext.get().setIncludeClassifications(false);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("x-atlan-route", "business-policy-rest");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "BusinessPolicyREST.linkBusinessPolicy()");
            }

            // Link the business policy to the specified entities

            entitiesStore.linkBusinessPolicy(request.getData());

        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
            RequestContext.get().endMetricRecord(metric);
        }
    }

    /**
     * Unlinks a business policy from entities.
     *
     * @param policyGuid the ID of the policy to be unlinked
     * @param request    the request containing the GUIDs of the assets to unlink the policy from
     * @throws AtlasBaseException if there is an error during the unlinking process
     */
    @POST
    @Path("/{policyId}/unlink-business-policy")
    @Timed
    public void unlinkBusinessPolicy(@PathParam("policyId") final String policyGuid, final LinkBusinessPolicyRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("unlinkBusinessPolicy");
        // Ensure the current user is authorized to unlink policies
        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Policy unlinking");
        }

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(false);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("x-atlan-route", "business-policy-rest");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "BusinessPolicyREST.unlinkBusinessPolicy(" + policyGuid + ")");
            }

            // Unlink the business policy from the specified entities
            entitiesStore.unlinkBusinessPolicy(policyGuid, request.getUnlinkGuids());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
            RequestContext.get().endMetricRecord(metric);
        }
    }

    @POST
    @Path("/unlink-business-policy/v2")
    @Timed
    public void unlinkBusinessPolicyV2(final UnlinkBusinessPolicyRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("unlinkBusinessPolicyV2");
        // Ensure the current user is authorized to unlink policies
        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Policy unlinking");
        }

        if(CollectionUtils.isEmpty(request.getAssetGuids()) || CollectionUtils.isEmpty(request.getUnlinkGuids())) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Asset GUIDs or Unlink GUIDs cannot be empty");
        }

        if(request.getAssetGuids().size() > 50 || request.getUnlinkGuids().size() > 50) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Asset GUIDs and Unlink GUIDs should not exceed 50");
        }

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(false);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("x-atlan-route", "business-policy-rest");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "BusinessPolicyREST.unlinkBusinessPolicyV2()");
            }

            // Unlink the business policy from the specified entities
            entitiesStore.unlinkBusinessPolicyV2(request.getAssetGuids(), request.getUnlinkGuids());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
            RequestContext.get().endMetricRecord(metric);
        }
    }



    private boolean isInvalidRequest(MoveBusinessPolicyRequest request, String assetId) {
        return Objects.isNull(request) ||
                CollectionUtils.isEmpty(request.getPolicyIds()) ||
                Objects.isNull(request.getType()) ||
                Objects.isNull(assetId);
    }

}

