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
package org.apache.atlas.authorizer;

// ring-authz-deny-logging build trigger
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.authorize.AtlasAccessResult;
import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasRelationshipAccessRequest;
import org.apache.atlas.authorize.AtlasTypeAccessRequest;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.discovery.PurposeDiscoveryService;
import org.apache.atlas.discovery.PurposeDiscoveryServiceImpl;
import org.apache.atlas.model.discovery.PurposeUserRequest;
import org.apache.atlas.model.discovery.PurposeUserResponse;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Emits a single structured WARN log line whenever authorization denies an operation and
 * {@link org.apache.atlas.AtlasErrorCode#UNAUTHORIZED_ACCESS} (ATLAS-403-00-001) is about to be
 * thrown. Observability only: never alters control flow or the 403 payload.
 *
 * Field format (pipe-separated key=value, all values quoted):
 *   AUTHZ_DENY user="..." groups=<N> action="..." entity="<type>/<guid>" qn="..."
 *              cause="UNAUTHORIZED_ACCESS" policyId="<id|implicit>" enforcer="..."
 *              explicitDeny=<bool> priority=<int> purposes="[guid1, guid2]" msg="..."
 *
 * Logger name is {@value #LOGGER_NAME} so operators can grep and route independently of the
 * parent {@code org.apache.atlas.authorizer} logger.
 */
@Component
public class AuthzDenyLogger {

    static final String LOGGER_NAME = "org.apache.atlas.authz.deny";

    private static final Logger LOG      = LoggerFactory.getLogger(LOGGER_NAME);
    private static final Logger INTERNAL = LoggerFactory.getLogger(AuthzDenyLogger.class);

    private static final int MAX_QN_LEN         = 512;
    private static final int MAX_MESSAGE_LEN    = 512;
    private static final int MAX_PURPOSE_GUIDS  = 10;

    static final String CONFIG_ENABLED = "atlas.authz.deny-logging.enabled";
    static final String CONFIG_ENRICH  = "atlas.authz.deny-logging.enrich-personas-purposes";

    private static final boolean DENY_LOGGING_ENABLED;
    private static final boolean ENRICH_PERSONAS_PURPOSES;

    static {
        boolean enabled = true;
        boolean enrich  = true;
        try {
            org.apache.commons.configuration.Configuration conf = ApplicationProperties.get();
            enabled = conf.getBoolean(CONFIG_ENABLED, true);
            enrich  = conf.getBoolean(CONFIG_ENRICH, true);
        } catch (AtlasException e) {
            INTERNAL.warn("AuthzDenyLogger: failed to read config, defaulting enabled=true enrich=true", e);
        }
        DENY_LOGGING_ENABLED     = enabled;
        ENRICH_PERSONAS_PURPOSES = enrich;
    }

    private static volatile AtlasDiscoveryService   discoveryService;
    private static volatile PurposeDiscoveryService purposeServiceOverride;
    private static volatile PurposeDiscoveryService purposeServiceLazy;

    @Inject
    public AuthzDenyLogger(AtlasDiscoveryService discoveryService) {
        AuthzDenyLogger.discoveryService = discoveryService;
    }

    // Package-private seam for unit tests.
    static void setPurposeDiscoveryServiceForTesting(PurposeDiscoveryService svc) {
        purposeServiceOverride = svc;
        purposeServiceLazy     = null;
    }

    // ---------------------------------------------------------------------
    // Public entry points — one per AtlasAccessRequest subtype.
    // ---------------------------------------------------------------------

    static void logEntityDeny(AtlasEntityAccessRequest req, AtlasAccessResult result, String message) {
        if (!DENY_LOGGING_ENABLED || !LOG.isWarnEnabled()) {
            return;
        }
        try {
            String            user   = req != null ? req.getUser() : null;
            Set<String>       groups = req != null ? req.getUserGroups() : null;
            AtlasPrivilege    action = req != null ? req.getAction() : null;
            AtlasEntityHeader entity = req != null ? req.getEntity() : null;

            LOG.warn("AUTHZ_DENY user=\"{}\" groups={} action=\"{}\" entity=\"{}/{}\" qn=\"{}\""
                            + " cause=\"UNAUTHORIZED_ACCESS\" policyId=\"{}\" policyName=\"{}\" enforcer=\"{}\""
                            + " explicitDeny={} priority={} purposes=\"{}\" msg=\"{}\"",
                    nullSafe(user),
                    groupCount(groups),
                    renderAction(action),
                    entity != null ? nullSafe(entity.getTypeName()) : "null",
                    entity != null ? nullSafe(entity.getGuid())     : "null",
                    renderQualifiedName(entity),
                    renderPolicyId(result),
                    renderPolicyName(result),
                    renderEnforcer(result),
                    result != null && result.isExplicitDeny(),
                    renderPriority(result),
                    renderPurposes(user, groups),
                    renderMessage(message));
        } catch (Throwable t) {
            INTERNAL.debug("AuthzDenyLogger.logEntityDeny suppressed", t);
        }
    }

    static void logRelationshipDeny(AtlasRelationshipAccessRequest req, AtlasAccessResult result, String message) {
        if (!DENY_LOGGING_ENABLED || !LOG.isWarnEnabled()) {
            return;
        }
        try {
            String            user   = req != null ? req.getUser() : null;
            Set<String>       groups = req != null ? req.getUserGroups() : null;
            AtlasPrivilege    action = req != null ? req.getAction() : null;
            AtlasEntityHeader end1   = req != null ? req.getEnd1Entity() : null;
            AtlasEntityHeader end2   = req != null ? req.getEnd2Entity() : null;
            String            relType = req != null ? req.getRelationshipType() : null;

            LOG.warn("AUTHZ_DENY user=\"{}\" groups={} action=\"{}\" relationshipType=\"{}\""
                            + " end1=\"{}/{}\" end1Qn=\"{}\" end2=\"{}/{}\" end2Qn=\"{}\""
                            + " cause=\"UNAUTHORIZED_ACCESS\" policyId=\"{}\" policyName=\"{}\" enforcer=\"{}\""
                            + " explicitDeny={} priority={} purposes=\"{}\" msg=\"{}\"",
                    nullSafe(user),
                    groupCount(groups),
                    renderAction(action),
                    nullSafe(relType),
                    end1 != null ? nullSafe(end1.getTypeName()) : "null",
                    end1 != null ? nullSafe(end1.getGuid())     : "null",
                    renderQualifiedName(end1),
                    end2 != null ? nullSafe(end2.getTypeName()) : "null",
                    end2 != null ? nullSafe(end2.getGuid())     : "null",
                    renderQualifiedName(end2),
                    renderPolicyId(result),
                    renderPolicyName(result),
                    renderEnforcer(result),
                    result != null && result.isExplicitDeny(),
                    renderPriority(result),
                    renderPurposes(user, groups),
                    renderMessage(message));
        } catch (Throwable t) {
            INTERNAL.debug("AuthzDenyLogger.logRelationshipDeny suppressed", t);
        }
    }

    static void logTypeDeny(AtlasTypeAccessRequest req, AtlasAccessResult result, String message) {
        if (!DENY_LOGGING_ENABLED || !LOG.isWarnEnabled()) {
            return;
        }
        try {
            String           user    = req != null ? req.getUser() : null;
            Set<String>      groups  = req != null ? req.getUserGroups() : null;
            AtlasPrivilege   action  = req != null ? req.getAction() : null;
            AtlasBaseTypeDef typeDef = req != null ? req.getTypeDef() : null;

            LOG.warn("AUTHZ_DENY user=\"{}\" groups={} action=\"{}\" typeDef=\"{}\""
                            + " cause=\"UNAUTHORIZED_ACCESS\" policyId=\"{}\" policyName=\"{}\" enforcer=\"{}\""
                            + " explicitDeny={} priority={} msg=\"{}\"",
                    nullSafe(user),
                    groupCount(groups),
                    renderAction(action),
                    typeDef != null ? nullSafe(typeDef.getName()) : "null",
                    renderPolicyId(result),
                    renderPolicyName(result),
                    renderEnforcer(result),
                    result != null && result.isExplicitDeny(),
                    renderPriority(result),
                    renderMessage(message));
        } catch (Throwable t) {
            INTERNAL.debug("AuthzDenyLogger.logTypeDeny suppressed", t);
        }
    }

    static void logAdminDeny(AtlasAdminAccessRequest req, AtlasAccessResult result, String message) {
        if (!DENY_LOGGING_ENABLED || !LOG.isWarnEnabled()) {
            return;
        }
        try {
            String         user   = req != null ? req.getUser() : null;
            Set<String>    groups = req != null ? req.getUserGroups() : null;
            AtlasPrivilege action = req != null ? req.getAction() : null;

            LOG.warn("AUTHZ_DENY user=\"{}\" groups={} action=\"{}\" scope=\"admin\""
                            + " cause=\"UNAUTHORIZED_ACCESS\" policyId=\"{}\" policyName=\"{}\" enforcer=\"{}\""
                            + " explicitDeny={} priority={} msg=\"{}\"",
                    nullSafe(user),
                    groupCount(groups),
                    renderAction(action),
                    renderPolicyId(result),
                    renderPolicyName(result),
                    renderEnforcer(result),
                    result != null && result.isExplicitDeny(),
                    renderPriority(result),
                    renderMessage(message));
        } catch (Throwable t) {
            INTERNAL.debug("AuthzDenyLogger.logAdminDeny suppressed", t);
        }
    }

    // ---------------------------------------------------------------------
    // Rendering helpers — all NPE-safe, all bounded.
    // ---------------------------------------------------------------------

    private static String nullSafe(String s) {
        return s == null ? "null" : s;
    }

    private static int groupCount(Set<String> groups) {
        return groups == null ? 0 : groups.size();
    }

    private static String renderAction(AtlasPrivilege action) {
        if (action == null) {
            return "null";
        }
        String t = action.getType();
        return t != null ? t : action.name();
    }

    private static String renderQualifiedName(AtlasEntityHeader entity) {
        if (entity == null) {
            return "null";
        }
        Object qn;
        try {
            qn = entity.getAttribute("qualifiedName");
        } catch (Throwable t) {
            return "null";
        }
        if (qn == null) {
            return "null";
        }
        String s = String.valueOf(qn);
        if (s.length() > MAX_QN_LEN) {
            return s.substring(0, MAX_QN_LEN) + "...";
        }
        return s;
    }

    private static String renderMessage(String message) {
        if (message == null) {
            return "";
        }
        if (message.length() > MAX_MESSAGE_LEN) {
            return message.substring(0, MAX_MESSAGE_LEN) + "...";
        }
        return message;
    }

    private static String renderPolicyId(AtlasAccessResult result) {
        if (result == null) {
            return "null";
        }
        String pid = result.getPolicyId();
        if (pid == null || "-1".equals(pid)) {
            return "implicit";
        }
        return pid;
    }

    private static String renderPolicyName(AtlasAccessResult result) {
        if (result == null) {
            return "null";
        }
        String name = result.getPolicyName();
        if (name == null || name.isEmpty()) {
            return "null";
        }
        return name;
    }

    private static String renderEnforcer(AtlasAccessResult result) {
        if (result == null) {
            return "null";
        }
        String e = result.getEnforcer();
        return e != null ? e : "null";
    }

    private static int renderPriority(AtlasAccessResult result) {
        return result == null ? -1 : result.getPolicyPriority();
    }

    private static String renderPurposes(String user, Set<String> groups) {
        if (!ENRICH_PERSONAS_PURPOSES) {
            return "";
        }
        if (user == null) {
            return "";
        }
        PurposeDiscoveryService svc = resolvePurposeDiscoveryService();
        if (svc == null) {
            return "";
        }
        try {
            PurposeUserRequest preq = new PurposeUserRequest();
            preq.setUsername(user);
            preq.setGroups(groups != null ? new ArrayList<>(groups) : Collections.emptyList());
            preq.setLimit(MAX_PURPOSE_GUIDS);
            preq.setOffset(0);

            PurposeUserResponse resp = svc.discoverPurposesForUser(preq);
            List<AtlasEntityHeader> purposes = resp != null ? resp.getPurposes() : null;
            if (purposes == null || purposes.isEmpty()) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < purposes.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                AtlasEntityHeader p = purposes.get(i);
                sb.append(p != null ? nullSafe(p.getGuid()) : "null");
            }
            return sb.toString();
        } catch (Throwable t) {
            INTERNAL.debug("AuthzDenyLogger: purpose enrichment failed for user={}", user, t);
            return "";
        }
    }

    private static PurposeDiscoveryService resolvePurposeDiscoveryService() {
        PurposeDiscoveryService override = purposeServiceOverride;
        if (override != null) {
            return override;
        }
        PurposeDiscoveryService local = purposeServiceLazy;
        if (local != null) {
            return local;
        }
        AtlasDiscoveryService ds = discoveryService;
        if (ds == null) {
            return null;
        }
        synchronized (AuthzDenyLogger.class) {
            local = purposeServiceLazy;
            if (local == null) {
                local = new PurposeDiscoveryServiceImpl(ds);
                purposeServiceLazy = local;
            }
        }
        return local;
    }
}
