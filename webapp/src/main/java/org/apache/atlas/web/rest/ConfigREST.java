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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.config.dynamic.ConfigKey;
import org.apache.atlas.config.dynamic.DynamicConfigCacheStore.ConfigEntry;
import org.apache.atlas.config.dynamic.DynamicConfigStore;
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
 * REST interface for dynamic configuration operations.
 *
 * Provides CRUD operations for managing dynamic configs stored in Cassandra:
 * - Get all configs with their current values
 * - Get individual config by key
 * - Update/set config value
 * - Delete config (reset to default)
 *
 * Endpoints:
 * - GET  /api/atlas/v2/configs         - List all configs
 * - GET  /api/atlas/v2/configs/{key}   - Get single config
 * - PUT  /api/atlas/v2/configs/{key}   - Update config
 * - DELETE /api/atlas/v2/configs/{key} - Delete config
 */
@Path("configs")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class ConfigREST {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.ConfigREST");
    private static final Logger LOG = LoggerFactory.getLogger(ConfigREST.class);

    /**
     * Get all dynamic configs with their current values and metadata.
     *
     * @param request HTTP servlet request
     * @return ConfigListResponse containing all configs
     * @throws AtlasBaseException if operation fails
     */
    @GET
    @Timed
    public ConfigListResponse getAllConfigs(@Context HttpServletRequest request) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> ConfigREST.getAllConfigs()");
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "ConfigREST.getAllConfigs()");
            }

            if (!DynamicConfigStore.isEnabled()) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                        "Dynamic config store is not enabled. Set atlas.config.store.cassandra.enabled=true");
            }

            ConfigListResponse response = new ConfigListResponse();
            List<ConfigInfo> configList = new ArrayList<>();

            // Get all configs from store
            Map<String, ConfigEntry> allConfigs = DynamicConfigStore.getAllConfigs();

            // Also include any predefined keys not in store with their defaults
            for (String key : ConfigKey.getAllKeys()) {
                ConfigEntry entry = allConfigs.get(key);
                ConfigKey configKey = ConfigKey.fromKey(key);

                ConfigInfo configInfo = new ConfigInfo();
                configInfo.setKey(key);
                configInfo.setDefaultValue(configKey != null ? configKey.getDefaultValue() : null);

                if (entry != null) {
                    configInfo.setCurrentValue(entry.getValue());
                    configInfo.setUpdatedBy(entry.getUpdatedBy());
                    configInfo.setLastUpdated(entry.getLastUpdated() != null ?
                            Date.from(entry.getLastUpdated()) : null);
                } else {
                    configInfo.setCurrentValue(configKey != null ? configKey.getDefaultValue() : null);
                }

                configList.add(configInfo);
            }

            response.setConfigs(configList);
            response.setTotalCount(configList.size());
            response.setTimestamp(new Date());
            response.setEnabled(true);

            return response;

        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== ConfigREST.getAllConfigs()");
            }
        }
    }

    /**
     * Get a specific config by its key.
     *
     * @param key Config key
     * @param request HTTP servlet request
     * @return ConfigInfo containing config details
     * @throws AtlasBaseException if config is invalid or operation fails
     */
    @GET
    @Path("{key}")
    @Timed
    public ConfigInfo getConfig(@PathParam("key") String key,
                                @Context HttpServletRequest request) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> ConfigREST.getConfig({})", key);
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "ConfigREST.getConfig(" + key + ")");
            }

            if (!DynamicConfigStore.isEnabled()) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                        "Dynamic config store is not enabled");
            }

            if (StringUtils.isBlank(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Config key cannot be empty");
            }

            if (!ConfigKey.isValidKey(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                        "Invalid config key: " + key + ". Valid keys are: " + Arrays.toString(ConfigKey.getAllKeys()));
            }

            ConfigKey configKey = ConfigKey.fromKey(key);
            String currentValue = DynamicConfigStore.getConfig(key);

            // Try to get metadata from cache
            Map<String, ConfigEntry> allConfigs = DynamicConfigStore.getAllConfigs();
            ConfigEntry entry = allConfigs.get(key);

            ConfigInfo configInfo = new ConfigInfo();
            configInfo.setKey(key);
            configInfo.setCurrentValue(currentValue);
            configInfo.setDefaultValue(configKey != null ? configKey.getDefaultValue() : null);

            if (entry != null) {
                configInfo.setUpdatedBy(entry.getUpdatedBy());
                configInfo.setLastUpdated(entry.getLastUpdated() != null ?
                        Date.from(entry.getLastUpdated()) : null);
            }

            return configInfo;

        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== ConfigREST.getConfig({})", key);
            }
        }
    }

    /**
     * Update or set a config value.
     *
     * @param key Config key
     * @param updateRequest Update request containing the new value
     * @param servletRequest HTTP servlet request
     * @return ConfigResponse indicating success
     * @throws AtlasBaseException if config is invalid or operation fails
     */
    @PUT
    @Path("{key}")
    @Timed
    public ConfigResponse updateConfig(@PathParam("key") String key,
                                       ConfigUpdateRequest updateRequest,
                                       @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> ConfigREST.updateConfig({}, {})", key, updateRequest != null ? updateRequest.getValue() : "null");
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "ConfigREST.updateConfig(" + key + ")");
            }

            if (!DynamicConfigStore.isEnabled()) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                        "Dynamic config store is not enabled");
            }

            if (StringUtils.isBlank(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Config key cannot be empty");
            }

            if (updateRequest == null || StringUtils.isBlank(updateRequest.getValue())) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Config value cannot be empty");
            }

            if (!ConfigKey.isValidKey(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                        "Invalid config key: " + key + ". Valid keys are: " + Arrays.toString(ConfigKey.getAllKeys()));
            }

            String updatedBy = servletRequest.getRemoteUser() != null ?
                    servletRequest.getRemoteUser() : "anonymous";

            // Set the config
            DynamicConfigStore.setConfig(key, updateRequest.getValue(), updatedBy);

            LOG.info("Config '{}' updated to value: {} by user: {}", key, updateRequest.getValue(), updatedBy);

            ConfigResponse response = new ConfigResponse();
            response.setSuccess(true);
            response.setMessage("Config '" + key + "' updated successfully to: " + updateRequest.getValue());
            response.setKey(key);
            response.setValue(updateRequest.getValue());
            response.setTimestamp(new Date());

            return response;

        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== ConfigREST.updateConfig({}, {})", key, updateRequest != null ? updateRequest.getValue() : "null");
            }
        }
    }

    /**
     * Delete a config (reset to default value).
     *
     * @param key Config key
     * @param request HTTP servlet request
     * @return ConfigResponse indicating success
     * @throws AtlasBaseException if config is invalid or operation fails
     */
    @DELETE
    @Path("{key}")
    @Timed
    public ConfigResponse deleteConfig(@PathParam("key") String key,
                                       @Context HttpServletRequest request) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> ConfigREST.deleteConfig({})", key);
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "ConfigREST.deleteConfig(" + key + ")");
            }

            if (!DynamicConfigStore.isEnabled()) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                        "Dynamic config store is not enabled");
            }

            if (StringUtils.isBlank(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Config key cannot be empty");
            }

            if (!ConfigKey.isValidKey(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                        "Invalid config key: " + key + ". Valid keys are: " + Arrays.toString(ConfigKey.getAllKeys()));
            }

            // Get current and default values for logging
            String previousValue = DynamicConfigStore.getConfig(key);
            ConfigKey configKey = ConfigKey.fromKey(key);
            String defaultValue = configKey != null ? configKey.getDefaultValue() : null;

            // Delete the config (resets to default)
            DynamicConfigStore.deleteConfig(key);

            String updatedBy = request.getRemoteUser() != null ?
                    request.getRemoteUser() : "anonymous";

            LOG.info("Config '{}' deleted (reset to default) by user: {}. Previous value: {}, Default value: {}",
                    key, updatedBy, previousValue, defaultValue);

            ConfigResponse response = new ConfigResponse();
            response.setSuccess(true);
            response.setMessage("Config '" + key + "' deleted successfully (reset to default: " + defaultValue + ")");
            response.setKey(key);
            response.setValue(defaultValue);
            response.setTimestamp(new Date());

            return response;

        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== ConfigREST.deleteConfig({})", key);
            }
        }
    }

    /**
     * Get JanusGraph configuration details.
     *
     * Returns the current JanusGraph config values from multiple layers:
     * - Dynamic config store (Cassandra cache)
     * - ApplicationProperties (what JanusGraph is actually using at runtime)
     * - Default values
     *
     * This is useful for debugging because JanusGraph config overrides only take
     * effect at graph initialization time (singleton). Changing them via the config
     * REST API will NOT affect an already-running graph instance -- a pod restart
     * is required.
     *
     * @param request HTTP servlet request
     * @return JanusConfigResponse with config details for each JanusGraph key
     * @throws AtlasBaseException if operation fails
     */
    @GET
    @Path("janus")
    @Timed
    public JanusConfigResponse getJanusConfigs(@Context HttpServletRequest request) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> ConfigREST.getJanusConfigs()");
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "ConfigREST.getJanusConfigs()");
            }

            JanusConfigResponse response = new JanusConfigResponse();
            response.setTimestamp(new Date());

            org.apache.commons.configuration.Configuration appConfig = null;
            try {
                appConfig = ApplicationProperties.get();
            } catch (Exception e) {
                LOG.warn("Failed to read ApplicationProperties for JanusGraph config", e);
            }

            // CQL Keyspace
            JanusConfigDetail cqlKeyspace = new JanusConfigDetail();
            cqlKeyspace.setConfigKey(ConfigKey.JANUS_CQL_KEYSPACE.getKey());
            cqlKeyspace.setApplicationPropertiesKey("atlas.graph.storage.cql.keyspace");
            cqlKeyspace.setDefaultValue(ConfigKey.JANUS_CQL_KEYSPACE.getDefaultValue());

            if (appConfig != null) {
                cqlKeyspace.setActiveValue(appConfig.getString("atlas.graph.storage.cql.keyspace",
                        ConfigKey.JANUS_CQL_KEYSPACE.getDefaultValue()));
            }

            if (DynamicConfigStore.isEnabled()) {
                Map<String, ConfigEntry> allConfigs = DynamicConfigStore.getAllConfigs();
                ConfigEntry cqlEntry = allConfigs.get(ConfigKey.JANUS_CQL_KEYSPACE.getKey());
                if (cqlEntry != null) {
                    cqlKeyspace.setDynamicConfigValue(cqlEntry.getValue());
                    cqlKeyspace.setUpdatedBy(cqlEntry.getUpdatedBy());
                    cqlKeyspace.setLastUpdated(cqlEntry.getLastUpdated() != null ?
                            Date.from(cqlEntry.getLastUpdated()) : null);
                }
            }

            cqlKeyspace.setEffectiveValue(DynamicConfigStore.getJanusCqlKeyspace());
            response.setCqlKeyspace(cqlKeyspace);

            // ES Index Name
            JanusConfigDetail indexName = new JanusConfigDetail();
            indexName.setConfigKey(ConfigKey.JANUS_INDEX_NAME.getKey());
            indexName.setApplicationPropertiesKey("atlas.graph.index.search.index-name");
            indexName.setDefaultValue(ConfigKey.JANUS_INDEX_NAME.getDefaultValue());

            if (appConfig != null) {
                indexName.setActiveValue(appConfig.getString("atlas.graph.index.search.index-name",
                        ConfigKey.JANUS_INDEX_NAME.getDefaultValue()));
            }

            if (DynamicConfigStore.isEnabled()) {
                Map<String, ConfigEntry> allConfigs = DynamicConfigStore.getAllConfigs();
                ConfigEntry indexEntry = allConfigs.get(ConfigKey.JANUS_INDEX_NAME.getKey());
                if (indexEntry != null) {
                    indexName.setDynamicConfigValue(indexEntry.getValue());
                    indexName.setUpdatedBy(indexEntry.getUpdatedBy());
                    indexName.setLastUpdated(indexEntry.getLastUpdated() != null ?
                            Date.from(indexEntry.getLastUpdated()) : null);
                }
            }

            indexName.setEffectiveValue(DynamicConfigStore.getJanusIndexName());
            response.setIndexName(indexName);

            // Store status
            response.setDynamicConfigStoreEnabled(DynamicConfigStore.isEnabled());
            response.setDynamicConfigStoreActivated(DynamicConfigStore.isActivated());
            response.setRestartRequiredNote(
                    "JanusGraph config overrides only take effect at graph initialization time. " +
                    "If activeValue differs from dynamicConfigValue, a pod restart is required.");

            return response;

        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== ConfigREST.getJanusConfigs()");
            }
        }
    }

    // ================== DTOs ==================

    /**
     * Response containing all configs
     */
    public static class ConfigListResponse {
        private List<ConfigInfo> configs = new ArrayList<>();
        private int totalCount;
        private Date timestamp;
        private boolean enabled;

        public List<ConfigInfo> getConfigs() { return configs; }
        public void setConfigs(List<ConfigInfo> configs) { this.configs = configs; }

        public int getTotalCount() { return totalCount; }
        public void setTotalCount(int totalCount) { this.totalCount = totalCount; }

        public Date getTimestamp() { return timestamp; }
        public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }

    /**
     * Information about a single config
     */
    public static class ConfigInfo {
        private String key;
        private String currentValue;
        private String defaultValue;
        private String updatedBy;
        private Date lastUpdated;

        public String getKey() { return key; }
        public void setKey(String key) { this.key = key; }

        public String getCurrentValue() { return currentValue; }
        public void setCurrentValue(String currentValue) { this.currentValue = currentValue; }

        public String getDefaultValue() { return defaultValue; }
        public void setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }

        public String getUpdatedBy() { return updatedBy; }
        public void setUpdatedBy(String updatedBy) { this.updatedBy = updatedBy; }

        public Date getLastUpdated() { return lastUpdated; }
        public void setLastUpdated(Date lastUpdated) { this.lastUpdated = lastUpdated; }
    }

    /**
     * Request for updating a config
     */
    public static class ConfigUpdateRequest {
        private String value;

        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
    }

    /**
     * Response for config operations
     */
    public static class ConfigResponse {
        private boolean success;
        private String message;
        private String key;
        private String value;
        private Date timestamp;

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
    }

    /**
     * Response containing JanusGraph configuration details.
     */
    public static class JanusConfigResponse {
        private JanusConfigDetail cqlKeyspace;
        private JanusConfigDetail indexName;
        private boolean dynamicConfigStoreEnabled;
        private boolean dynamicConfigStoreActivated;
        private String restartRequiredNote;
        private Date timestamp;

        public JanusConfigDetail getCqlKeyspace() { return cqlKeyspace; }
        public void setCqlKeyspace(JanusConfigDetail cqlKeyspace) { this.cqlKeyspace = cqlKeyspace; }

        public JanusConfigDetail getIndexName() { return indexName; }
        public void setIndexName(JanusConfigDetail indexName) { this.indexName = indexName; }

        public boolean isDynamicConfigStoreEnabled() { return dynamicConfigStoreEnabled; }
        public void setDynamicConfigStoreEnabled(boolean dynamicConfigStoreEnabled) { this.dynamicConfigStoreEnabled = dynamicConfigStoreEnabled; }

        public boolean isDynamicConfigStoreActivated() { return dynamicConfigStoreActivated; }
        public void setDynamicConfigStoreActivated(boolean dynamicConfigStoreActivated) { this.dynamicConfigStoreActivated = dynamicConfigStoreActivated; }

        public String getRestartRequiredNote() { return restartRequiredNote; }
        public void setRestartRequiredNote(String restartRequiredNote) { this.restartRequiredNote = restartRequiredNote; }

        public Date getTimestamp() { return timestamp; }
        public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
    }

    /**
     * Detail for a single JanusGraph config property showing values from all layers.
     */
    public static class JanusConfigDetail {
        /** The dynamic config key (e.g. janus_cql_keyspace) */
        private String configKey;
        /** The ApplicationProperties key (e.g. atlas.graph.storage.cql.keyspace) */
        private String applicationPropertiesKey;
        /** The hardcoded default value */
        private String defaultValue;
        /** Value currently in ApplicationProperties (what JanusGraph is actually using) */
        private String activeValue;
        /** Value stored in the dynamic config store (Cassandra cache) */
        private String dynamicConfigValue;
        /** The effective value returned by the helper method (considering fallback chain) */
        private String effectiveValue;
        /** Who last updated the dynamic config value */
        private String updatedBy;
        /** When the dynamic config value was last updated */
        private Date lastUpdated;

        public String getConfigKey() { return configKey; }
        public void setConfigKey(String configKey) { this.configKey = configKey; }

        public String getApplicationPropertiesKey() { return applicationPropertiesKey; }
        public void setApplicationPropertiesKey(String applicationPropertiesKey) { this.applicationPropertiesKey = applicationPropertiesKey; }

        public String getDefaultValue() { return defaultValue; }
        public void setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }

        public String getActiveValue() { return activeValue; }
        public void setActiveValue(String activeValue) { this.activeValue = activeValue; }

        public String getDynamicConfigValue() { return dynamicConfigValue; }
        public void setDynamicConfigValue(String dynamicConfigValue) { this.dynamicConfigValue = dynamicConfigValue; }

        public String getEffectiveValue() { return effectiveValue; }
        public void setEffectiveValue(String effectiveValue) { this.effectiveValue = effectiveValue; }

        public String getUpdatedBy() { return updatedBy; }
        public void setUpdatedBy(String updatedBy) { this.updatedBy = updatedBy; }

        public Date getLastUpdated() { return lastUpdated; }
        public void setLastUpdated(Date lastUpdated) { this.lastUpdated = lastUpdated; }
    }
}
