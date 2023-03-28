/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.rest;

import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.policytransformer.CachePolicyTransformerImpl;
import org.apache.atlas.ranger.plugin.util.KeycloakUserStore;
import org.apache.atlas.ranger.plugin.util.RangerRoles;
import org.apache.atlas.ranger.plugin.util.RangerUserStore;
import org.apache.atlas.ranger.plugin.util.ServicePolicies;
import org.apache.atlas.tasks.TaskService;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

/**
 * REST interface for CRUD operations on tasks
 */
@Path("auth")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class AuthREST {
    private static final Logger LOG      = LoggerFactory.getLogger(AuthREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.AuthREST");

    private CachePolicyTransformerImpl policyTransformer;

    @Inject
    public AuthREST(CachePolicyTransformerImpl policyTransformer) {
        this.policyTransformer = policyTransformer;
    }

    @GET
    @Path("download/roles/{serviceName}")
    @Timed
    public RangerRoles downloadRoles(@PathParam("serviceName") final String serviceName,
                                          @QueryParam("pluginId") String pluginId,
                                          @DefaultValue("0") @QueryParam("lastUpdatedTime") Long lastUpdatedTime,
                                          @Context HttpServletResponse response) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "AuthREST.downloadUsers");
            }

            KeycloakUserStore keycloakUserStore = new KeycloakUserStore(serviceName);
            RangerRoles roles = keycloakUserStore.loadRolesIfUpdated(lastUpdatedTime);

            if (roles == null) {
                response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            }

            return roles;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @GET
    @Path("download/users/{serviceName}")
    @Timed
    public RangerUserStore downloadUserStore(@PathParam("serviceName") final String serviceName,
                                     @QueryParam("pluginId") String pluginId,
                                     @DefaultValue("0") @QueryParam("lastUpdatedTime") Long lastUpdatedTime,
                                     @Context HttpServletResponse response) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "AuthREST.downloadUserStore");
            }

            KeycloakUserStore keycloakUserStore = new KeycloakUserStore(serviceName);
            RangerUserStore userStore = keycloakUserStore.loadUserStoreIfUpdated(lastUpdatedTime);

            if (userStore == null) {
                response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            }

            return userStore;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @GET
    @Path("download/policies/{serviceName}")
    @Timed
    public ServicePolicies downloadPolicies(@PathParam("serviceName") final String serviceName,
                                     @QueryParam("pluginId") String pluginId,
                                     @DefaultValue("0") @QueryParam("lastUpdatedTime") Long lastUpdatedTime) {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "AuthREST.downloadPolicies");
            }

            ServicePolicies ret = policyTransformer.getPolicies(serviceName, pluginId, lastUpdatedTime);

            return ret;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }
}
