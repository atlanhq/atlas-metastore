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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.service.FeatureFlagStore;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;

/**
 * REST interface for CRUD operations on feature flags
 */
@Path("featureFlag")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class FeatureFlagREST {
    private static final Logger LOG      = LoggerFactory.getLogger(FeatureFlagREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.FeatureFlagREST");

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{flag}")
    public Map<String, String> getFeatureFlag(@PathParam("flag") String key) {
        AtlasPerfTracer perf = null;
        Map<String, String> ret = new HashMap<>();

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "FeatureFlagREST.getFeatureFlag(" + key + ")");
            }
            String value = FeatureFlagStore.getFlag(key);
            if (value != null)
                ret.put(key, value);
        } finally {
            AtlasPerfTracer.log(perf);
        }
        return ret;
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{flag}")
    public Map<String, String> setFeatureFlag(@PathParam("flag") String key, @QueryParam("value") String value) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        Map<String, String> ret = new HashMap<>();

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "FeatureFlagREST.setFeatureFlag(" + key + ", "+ value +")");
            }
            AtlasAuthorizationUtils.verifyAccess(new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_FEATURE_FLAG_CUD), "featureFlag");

            if (StringUtils.isEmpty(value)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Please specify value for the flag");
            }
            ret.put(key, FeatureFlagStore.setFlag(key, value));
        } finally {
            AtlasPerfTracer.log(perf);
        }
        return ret;
    }

    @DELETE
    @Path("/{flag}")
    @Produces(MediaType.APPLICATION_JSON)
    public void deleteFeatureFlag(@PathParam("flag") String key) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "FeatureFlagREST.deleteFeatureFlag(" + key + ")");
            }
            AtlasAuthorizationUtils.verifyAccess(new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_FEATURE_FLAG_CUD), "featureFlag");
            FeatureFlagStore.deleteFlag(key);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }
}
