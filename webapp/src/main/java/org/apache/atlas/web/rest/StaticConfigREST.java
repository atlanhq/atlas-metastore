/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.service.config.StaticConfigKey;
import org.apache.atlas.service.config.StaticConfigStore;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.rest.ConfigREST.ConfigInfo;
import org.apache.atlas.web.rest.ConfigREST.ConfigListResponse;
import org.apache.atlas.web.rest.ConfigREST.ConfigResponse;
import org.apache.atlas.web.rest.ConfigREST.ConfigUpdateRequest;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.*;

/**
 * REST interface for static configuration operations.
 *
 * Static configs are read from Cassandra once at startup and are immutable
 * at runtime. A restart is required for seeded values to take effect.
 *
 * Endpoints:
 * - GET  /api/atlas/v2/static-configs           - List all static configs
 * - GET  /api/atlas/v2/static-configs/{key}     - Get single static config
 * - PUT  /api/atlas/v2/static-configs/{key}     - Seed static config (restart required)
 */
@Path("static-configs")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class StaticConfigREST {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.StaticConfigREST");
    private static final Logger LOG = LoggerFactory.getLogger(StaticConfigREST.class);

    /**
     * Get all static configs currently loaded in memory.
     * These are the values read from Cassandra (or fallback) at startup.
     */
    @GET
    @Timed
    public ConfigListResponse getAllStaticConfigs(@Context HttpServletRequest request) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> StaticConfigREST.getAllStaticConfigs()");
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "StaticConfigREST.getAllStaticConfigs()");
            }

            ConfigListResponse response = new ConfigListResponse();
            List<ConfigInfo> configList = new ArrayList<>();

            Map<String, String> loadedConfigs = StaticConfigStore.getAllConfigs();

            for (StaticConfigKey staticKey : StaticConfigKey.values()) {
                String key = staticKey.getKey();
                ConfigInfo configInfo = new ConfigInfo();
                configInfo.setKey(key);
                configInfo.setDefaultValue(staticKey.getDefaultValue());
                configInfo.setCurrentValue(loadedConfigs.get(key));
                configList.add(configInfo);
            }

            response.setConfigs(configList);
            response.setTotalCount(configList.size());
            response.setTimestamp(new Date());
            response.setEnabled(StaticConfigStore.isReady());

            return response;

        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== StaticConfigREST.getAllStaticConfigs()");
            }
        }
    }

    /**
     * Get a specific static config by its key.
     * Returns the value currently loaded in memory (from last startup).
     */
    @GET
    @Path("{key}")
    @Timed
    public ConfigInfo getStaticConfig(@PathParam("key") String key,
                                      @Context HttpServletRequest request) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> StaticConfigREST.getStaticConfig({})", key);
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "StaticConfigREST.getStaticConfig(" + key + ")");
            }

            if (StringUtils.isBlank(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Config key cannot be empty");
            }

            if (!StaticConfigKey.isValidKey(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                        "Invalid static config key: " + key + ". Valid keys are: " + Arrays.toString(StaticConfigKey.getAllKeys()));
            }

            StaticConfigKey staticKey = StaticConfigKey.fromKey(key);

            ConfigInfo configInfo = new ConfigInfo();
            configInfo.setKey(key);
            configInfo.setDefaultValue(staticKey.getDefaultValue());
            configInfo.setCurrentValue(StaticConfigStore.getConfig(key));

            return configInfo;

        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== StaticConfigREST.getStaticConfig({})", key);
            }
        }
    }

    /**
     * Admin-only: Seed a static config value into Cassandra.
     * The value is persisted but does NOT take effect until Atlas is restarted.
     */
    @PUT
    @Path("{key}")
    @Timed
    public ConfigResponse seedStaticConfig(@PathParam("key") String key,
                                           ConfigUpdateRequest updateRequest,
                                           @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> StaticConfigREST.seedStaticConfig({}, {})", key,
                    updateRequest != null ? updateRequest.getValue() : "null");
        }

        AtlasAuthorizationUtils.verifyAccess(new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_FEATURE_FLAG_CUD), "seed static config is not allowed");

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "StaticConfigREST.seedStaticConfig(" + key + ")");
            }

            if (!StaticConfigStore.isReady()) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Static config store is not ready");
            }

            if (StringUtils.isBlank(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Config key cannot be empty");
            }

            if (updateRequest == null || StringUtils.isBlank(updateRequest.getValue())) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Config value cannot be empty");
            }

            if (!StaticConfigKey.isValidKey(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                        "Invalid static config key: " + key + ". Valid keys are: " + Arrays.toString(StaticConfigKey.getAllKeys()));
            }

            String updatedBy = servletRequest.getRemoteUser() != null ?
                    servletRequest.getRemoteUser() : "anonymous";

            StaticConfigStore.seedConfig(key, updateRequest.getValue(), updatedBy);

            LOG.info("Static config '{}' seeded to Cassandra with value: {} by user: {} (restart required)",
                    key, updateRequest.getValue(), updatedBy);

            ConfigResponse response = new ConfigResponse();
            response.setSuccess(true);
            response.setMessage("Static config '" + key + "' seeded in Cassandra with value: " +
                    updateRequest.getValue() + ". WARNING: Restart required to take effect.");
            response.setKey(key);
            response.setValue(updateRequest.getValue());
            response.setTimestamp(new Date());

            return response;

        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== StaticConfigREST.seedStaticConfig({}, {})", key,
                        updateRequest != null ? updateRequest.getValue() : "null");
            }
        }
    }
}
