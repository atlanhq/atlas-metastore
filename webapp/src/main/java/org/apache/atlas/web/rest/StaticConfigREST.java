package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.service.config.StaticConfigKey;
import org.apache.atlas.service.config.StaticConfigStore;
import org.apache.atlas.utils.AtlasPerfTracer;
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
 * Static configs are read once at startup from Cassandra. Changes made via
 * the admin PUT endpoint are persisted to Cassandra but require a restart
 * to take effect in-memory.
 *
 * Endpoints:
 * - GET  /api/atlas/v2/static-configs           - List all static configs
 * - GET  /api/atlas/v2/static-configs/{key}     - Get single config
 * - PUT  /api/atlas/v2/admin/static-configs/{key} - Admin: seed to Cassandra
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
     * Get all static configs with their current values.
     */
    @GET
    @Timed
    public StaticConfigListResponse getAllStaticConfigs(@Context HttpServletRequest request) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> StaticConfigREST.getAllStaticConfigs()");
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "StaticConfigREST.getAllStaticConfigs()");
            }

            if (!StaticConfigStore.isReady()) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                        "Static config store is not ready");
            }

            Map<String, String> allConfigs = StaticConfigStore.getAllConfigs();
            List<StaticConfigInfo> configList = new ArrayList<>();

            for (String key : StaticConfigKey.getAllKeys()) {
                StaticConfigKey configKey = StaticConfigKey.fromKey(key);

                StaticConfigInfo info = new StaticConfigInfo();
                info.setKey(key);
                info.setCurrentValue(allConfigs.get(key));
                info.setDefaultValue(configKey != null ? configKey.getDefaultValue() : null);
                configList.add(info);
            }

            StaticConfigListResponse response = new StaticConfigListResponse();
            response.setConfigs(configList);
            response.setTotalCount(configList.size());
            response.setTimestamp(new Date());

            return response;

        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== StaticConfigREST.getAllStaticConfigs()");
            }
        }
    }

    /**
     * Get a specific static config by key.
     */
    @GET
    @Path("{key}")
    @Timed
    public StaticConfigInfo getStaticConfig(@PathParam("key") String key,
                                            @Context HttpServletRequest request) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> StaticConfigREST.getStaticConfig({})", key);
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "StaticConfigREST.getStaticConfig(" + key + ")");
            }

            if (!StaticConfigStore.isReady()) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Static config store is not ready");
            }

            if (StringUtils.isBlank(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Config key cannot be empty");
            }

            if (!StaticConfigKey.isValidKey(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                        "Invalid static config key: " + key + ". Valid keys are: " + Arrays.toString(StaticConfigKey.getAllKeys()));
            }

            StaticConfigKey configKey = StaticConfigKey.fromKey(key);

            StaticConfigInfo info = new StaticConfigInfo();
            info.setKey(key);
            info.setCurrentValue(StaticConfigStore.getConfig(key));
            info.setDefaultValue(configKey != null ? configKey.getDefaultValue() : null);

            return info;

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
    @Path("/admin/{key}")
    @Timed
    public StaticConfigResponse seedStaticConfig(@PathParam("key") String key,
                                                  StaticConfigUpdateRequest updateRequest,
                                                  @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> StaticConfigREST.seedStaticConfig({}, {})", key,
                    updateRequest != null ? updateRequest.getValue() : "null");
        }

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

            // Write to Cassandra (does NOT update in-memory map)
            StaticConfigStore.seedConfig(key, updateRequest.getValue(), updatedBy);

            LOG.info("Static config '{}' seeded to Cassandra with value: {} by user: {} (restart required)",
                    key, updateRequest.getValue(), updatedBy);

            StaticConfigResponse response = new StaticConfigResponse();
            response.setSuccess(true);
            response.setMessage("Static config '" + key + "' seeded in Cassandra with value: " +
                    updateRequest.getValue() + ". WARNING: Restart required to take effect.");
            response.setKey(key);
            response.setValue(updateRequest.getValue());
            response.setTimestamp(new Date());
            response.setRestartRequired(true);

            return response;

        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== StaticConfigREST.seedStaticConfig({}, {})", key,
                        updateRequest != null ? updateRequest.getValue() : "null");
            }
        }
    }

    // ================== DTOs ==================

    public static class StaticConfigListResponse {
        private List<StaticConfigInfo> configs = new ArrayList<>();
        private int totalCount;
        private Date timestamp;

        public List<StaticConfigInfo> getConfigs() { return configs; }
        public void setConfigs(List<StaticConfigInfo> configs) { this.configs = configs; }

        public int getTotalCount() { return totalCount; }
        public void setTotalCount(int totalCount) { this.totalCount = totalCount; }

        public Date getTimestamp() { return timestamp; }
        public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
    }

    public static class StaticConfigInfo {
        private String key;
        private String currentValue;
        private String defaultValue;

        public String getKey() { return key; }
        public void setKey(String key) { this.key = key; }

        public String getCurrentValue() { return currentValue; }
        public void setCurrentValue(String currentValue) { this.currentValue = currentValue; }

        public String getDefaultValue() { return defaultValue; }
        public void setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }
    }

    public static class StaticConfigUpdateRequest {
        private String value;

        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
    }

    public static class StaticConfigResponse {
        private boolean success;
        private String message;
        private String key;
        private String value;
        private Date timestamp;
        private boolean restartRequired;

        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }

        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }

        public String getKey() { return key; }
        public void setKey(String key) { this.key = key; }

        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }

        public Date getTimestamp() { return timestamp; }
        public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }

        public boolean isRestartRequired() { return restartRequired; }
        public void setRestartRequired(boolean restartRequired) { this.restartRequired = restartRequired; }
    }
}
