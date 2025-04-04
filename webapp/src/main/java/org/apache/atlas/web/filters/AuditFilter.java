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

package org.apache.atlas.web.filters;

// --- OTel API Imports ---
// Add these dependencies to your pom.xml or build.gradle:
// - io.opentelemetry:opentelemetry-api
// - io.opentelemetry:opentelemetry-context
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
// --- End OTel API Imports ---

import org.apache.atlas.*;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.service.metrics.MetricUtils;
import org.apache.atlas.service.metrics.MetricsRegistry;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.atlas.web.util.DateTimeHelper;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.context.support.SpringBeanAutowiringSupport;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom; // For generating spanId

import static java.util.Optional.ofNullable;
import static org.apache.atlas.AtlasConfiguration.*;
import static org.apache.commons.lang.StringUtils.EMPTY;

/**
 * This records audit information as part of the filter after processing the request
 * and also introduces a UUID into request and response for tracing requests in logs.
 * Integrates with OpenTelemetry to allow custom trace IDs via X-Atlan-Request-Id header.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE) // Ensure this runs very early
public class AuditFilter implements Filter {
    private static final Logger LOG                   = LoggerFactory.getLogger(AuditFilter.class);
    private static final Logger AUDIT_LOG             = LoggerFactory.getLogger("AUDIT");
    public static final String  TRACE_ID_MDC_KEY      = "trace_id"; // Original MDC key, will hold OTel Trace ID
    public static final String  SPAN_ID_MDC_KEY       = "span_id";  // Add OTel Span ID to MDC
    public static final String  X_ATLAN_REQUEST_ID    = "X-Atlan-Request-Id"; // Header for custom Trace ID
    public static final String  X_ATLAN_CLIENT_ORIGIN = "X-Atlan-Client-Origin";
    private boolean deleteTypeOverrideEnabled                = false;
    private boolean createShellEntityForNonExistingReference = false;

    @Inject
    private MetricsRegistry metricsRegistry;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("AuditFilter initialization started");
        deleteTypeOverrideEnabled                = REST_API_ENABLE_DELETE_TYPE_OVERRIDE.getBoolean();
        createShellEntityForNonExistingReference = REST_API_CREATE_SHELL_ENTITY_FOR_NON_EXISTING_REF.getBoolean();
        SpringBeanAutowiringSupport.processInjectionBasedOnCurrentContext(this);
        LOG.info("REST_API_ENABLE_DELETE_TYPE_OVERRIDE={}", deleteTypeOverrideEnabled);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
            throws IOException, ServletException {

        final HttpServletRequest httpRequest = (HttpServletRequest) request;
        final HttpServletResponse httpResponse = (HttpServletResponse) response;
        final long startTime = System.currentTimeMillis();
        final Date requestTime = new Date();
        final Thread currentThread = Thread.currentThread();
        final String oldName = currentThread.getName();
        final String user = AtlasAuthorizationUtils.getCurrentUserName();
        final Set<String> userGroups = AtlasAuthorizationUtils.getCurrentUserGroups();
        final String deleteType = httpRequest.getParameter("deleteType");
        final boolean skipFailedEntities = Boolean.parseBoolean(httpRequest.getParameter("skipFailedEntities"));

        // --- OTel Context Handling ---
        Context currentOtelContext = Context.current(); // Get context possibly populated by upstream or agent propagator
        boolean existingContextIsValid = Span.fromContext(currentOtelContext).getSpanContext().isValid();
        boolean contextSetManually = false;

        if (!existingContextIsValid) {
            String customTraceIdHeader = httpRequest.getHeader(X_ATLAN_REQUEST_ID);

            // Validate custom trace ID (must be 32 hex characters)
            if (customTraceIdHeader != null && customTraceIdHeader.matches("[a-fA-F0-9]{32}")) {
                try {
                    String newSpanId = generateRandomOtelSpanId();
                    TraceFlags traceFlags = TraceFlags.getSampled(); // Default to sampled, adjust if needed
                    TraceState traceState = TraceState.getDefault();

                    // Create the custom SpanContext
                    SpanContext customSpanContext = SpanContext.create(
                            customTraceIdHeader,
                            newSpanId,
                            traceFlags,
                            traceState);

                    // Prepare the new context with the wrapped SpanContext
                    currentOtelContext = currentOtelContext.with(Span.wrap(customSpanContext));
                    contextSetManually = true;
                    LOG.debug("Using custom Trace ID from {} header: {}", X_ATLAN_REQUEST_ID, customTraceIdHeader);

                } catch (IllegalArgumentException e) {
                    LOG.warn("Failed to create SpanContext with custom Trace ID '{}': {}", customTraceIdHeader, e.getMessage());
                    // Let agent generate ID if creation fails
                }
            } else if (customTraceIdHeader != null) {
                LOG.warn("Invalid format or length for custom trace ID header {}: '{}'. Expected 32 hex chars. Letting OTel Agent generate ID.", X_ATLAN_REQUEST_ID, customTraceIdHeader);
                // Let agent generate ID
            } else {
                // No custom header, let agent generate ID or use upstream propagation if detected later by agent
                LOG.debug("No valid custom Trace ID header found. Letting OTel Agent handle Trace ID.");
            }
        } else {
            LOG.debug("Valid OpenTelemetry SpanContext already exists. Using propagated context.");
        }
        // --- End OTel Context Handling ---


        Scope otelScope = null; // Define scope variable outside try-finally
        String effectiveOtelTraceId = "unset-trace-id"; // Default value
        String effectiveOtelSpanId = "unset-span-id";   // Default value

        try {
            // --- Activate OTel Scope ---
            // This makes the context (either original, manually created, or root) current for this thread
            otelScope = currentOtelContext.makeCurrent();

            // --- Get Effective OTel IDs ---
            // Retrieve the trace/span IDs *after* the scope is activated.
            // This will be the custom one, propagated one, or agent-generated one.
            final Span currentSpan = Span.current(); // Gets the span associated with the now-current context
            final SpanContext currentSpanContext = currentSpan.getSpanContext();

            if (currentSpanContext.isValid()) {
                effectiveOtelTraceId = currentSpanContext.getTraceId();
                effectiveOtelSpanId = currentSpanContext.getSpanId();
            } else {
                // Should ideally not happen if agent is running, but have fallback. Agent might create span later.
                LOG.warn("No valid OTel SpanContext found after activating scope. MDC/RequestContext might have default IDs.");
                // Generate internal ID as fallback for Atlas context if needed? Or leave unset?
                // Let's use a placeholder for now.
                effectiveOtelTraceId = "agent-did-not-create-span-early";
            }

            // --- Setup MDC ---
            // Use OTel IDs for logging context
            MDC.put(TRACE_ID_MDC_KEY, effectiveOtelTraceId); // Overwrite original trace_id key
            MDC.put(SPAN_ID_MDC_KEY, effectiveOtelSpanId);
            // Keep original request ID header value in MDC if present
            MDC.put(X_ATLAN_REQUEST_ID, ofNullable(httpRequest.getHeader(X_ATLAN_REQUEST_ID)).orElse(EMPTY));
            MDC.put(X_ATLAN_CLIENT_ORIGIN, ofNullable(httpRequest.getHeader(X_ATLAN_CLIENT_ORIGIN)).orElse(EMPTY));

            // --- Update Thread Name ---
            currentThread.setName(formatName(oldName, effectiveOtelTraceId)); // Use OTel Trace ID in thread name

            // --- Setup Atlas RequestContext ---
            RequestContext.clear(); // Clear first
            RequestContext requestContext = RequestContext.get();
            requestContext.setUri(MetricUtils.matchCanonicalPattern(httpRequest.getRequestURI()).orElse(EMPTY));
            requestContext.setTraceId(effectiveOtelTraceId); // Use OTel Trace ID here
            requestContext.setUser(user, userGroups);
            requestContext.setClientIPAddress(AtlasAuthorizationUtils.getRequestIpAddress(httpRequest));
            requestContext.setCreateShellEntityForNonExistingReference(createShellEntityForNonExistingReference);
            requestContext.setForwardedAddresses(AtlasAuthorizationUtils.getForwardedAddressesFromRequest(httpRequest));
            requestContext.setSkipFailedEntities(skipFailedEntities);
            requestContext.setClientOrigin(httpRequest.getHeader(X_ATLAN_CLIENT_ORIGIN));
            requestContext.setMetricRegistry(metricsRegistry);

            // --- Existing Filter Logic ---
            if (StringUtils.isNotEmpty(deleteType)) {
                if (deleteTypeOverrideEnabled) {
                    if(DeleteType.PURGE.name().equals(deleteType)) {
                        requestContext.setPurgeRequested(true);
                    }
                    requestContext.setDeleteType(DeleteType.from(deleteType));
                } else {
                    LOG.warn("Override of deleteType is not enabled. Ignoring parameter deleteType={}, in request from user={}", deleteType, user);
                }
            }
            HeadersUtil.setRequestContextHeaders(httpRequest);
            // --- End Existing Filter Logic ---


            // <<< Execute rest of the filter chain WITHIN the OTel Scope >>>
            filterChain.doFilter(request, response);

        } finally {
            // --- OTel Scope Closure ---
            if (otelScope != null) {
                otelScope.close(); // IMPORTANT: Closes the scope, restores previous context
            }

            // --- Auditing and Final Response Headers ---
            long timeTaken = System.currentTimeMillis() - startTime;
            recordAudit(httpRequest, requestTime, user, httpResponse.getStatus(), timeTaken);

            // Put effective OTel trace id into response header
            // Use the value captured earlier, as context might have changed after scope close
            httpResponse.setHeader(TRACE_ID_MDC_KEY, effectiveOtelTraceId);

            // Also return the original request ID header if it was present
            String originalRequestIdHeader = MDC.get(X_ATLAN_REQUEST_ID); // Get from MDC populated earlier
            if (originalRequestIdHeader != null && !originalRequestIdHeader.isEmpty()) {
                httpResponse.setHeader(X_ATLAN_REQUEST_ID, originalRequestIdHeader);
            }

            // --- Cleanup ---
            currentThread.setName(oldName); // Restore original thread name
            RequestContext.clear(); // Clear Atlas context
            MDC.clear(); // Clear MDC
        }
    }

    // Helper method to generate OTel compliant 16-char hex Span ID
    private String generateRandomOtelSpanId() {
        long randomLong = ThreadLocalRandom.current().nextLong();
        // Ensure positive value for hex conversion, format as 16 hex chars
        if (randomLong < 0) {
            // Avoid Long.MIN_VALUE which has no positive equivalent by adding 1
            randomLong = -(randomLong + 1);
        }
        return String.format("%016x", randomLong);
    }

    private String formatName(String oldName, String requestId) {
        // Shorten request ID for thread name if it's the long OTel ID
        String displayId = requestId;
        if (requestId != null && requestId.length() > 12) {
            displayId = requestId.substring(0, 12) + "...";
        }
        return oldName + " - OTel[" + displayId + "]";
    }

    private void recordAudit(HttpServletRequest httpRequest, Date when, String who, int httpStatus, long timeTaken) {
        final String fromAddress = httpRequest.getRemoteAddr();
        final String whatRequest = httpRequest.getMethod();
        final String whatURL     = Servlets.getRequestURL(httpRequest);
        final String whatUrlPath = httpRequest.getRequestURL().toString(); //url path without query string

        if (!isOperationExcludedFromAudit(whatRequest, whatUrlPath.toLowerCase(), null)) {
            audit(new AuditLog(who, fromAddress, whatRequest, whatURL, when, httpStatus, timeTaken));
        } else {
            if(LOG.isDebugEnabled()) {
                LOG.debug(" Skipping Audit for {} ", whatURL);
            }
        }
    }

    public static void audit(AuditLog auditLog) {
        if (AUDIT_LOG.isInfoEnabled() && auditLog != null) {
            // Use MDC values already set within the filter's scope if possible,
            // otherwise use the passed AuditLog object values.
            // Note: MDC might be cleared by the time this is called if not careful,
            // relying on the passed object is safer. Let's stick to the object.
            MDC.put("audit_requestTime", DateTimeHelper.formatDateUTC(auditLog.requestTime));
            MDC.put("audit_user", auditLog.userName);
            MDC.put("audit_from", auditLog.fromAddress);
            MDC.put("audit_requestMethod", auditLog.requestMethod);
            MDC.put("audit_requestUrl", auditLog.requestUrl);
            MDC.put("audit_httpStatus", String.valueOf(auditLog.httpStatus));
            MDC.put("audit_timeTaken", String.valueOf(auditLog.timeTaken));
            AUDIT_LOG.info("ATLAS_AUDIT - {} {} {} {}", auditLog.requestMethod, auditLog.requestUrl, auditLog.httpStatus, auditLog.timeTaken);
            // Clean up audit-specific MDC keys if desired, or rely on final MDC.clear()
        }
    }

    boolean isOperationExcludedFromAudit(String requestHttpMethod, String requestOperation, Configuration config) {
        try {
            return AtlasRepositoryConfiguration.isExcludedFromAudit(config, requestHttpMethod, requestOperation);
        } catch (AtlasException e) {
            LOG.error("Error checking if operation is excluded from audit", e);
            return false; // Default to auditing if config check fails
        }
    }

    @Override
    public void destroy() {
        // do nothing
    }

    // --- AuditLog Inner Class (Unchanged) ---
    public static class AuditLog {
        // ... (content as provided previously) ...
        private static final char FIELD_SEP = '|';

        private final String userName;
        private final String fromAddress;
        private final String requestMethod;
        private final String requestUrl;
        private final Date   requestTime;
        private       int    httpStatus;
        private       long   timeTaken;

        public AuditLog(String userName, String fromAddress, String requestMethod, String requestUrl) {
            this(userName, fromAddress, requestMethod, requestUrl, new Date());
        }

        public AuditLog(String userName, String fromAddress, String requestMethod, String requestUrl, Date requestTime) {
            this(userName, fromAddress, requestMethod, requestUrl, requestTime, HttpServletResponse.SC_OK, 0);
        }

        public AuditLog(String userName, String fromAddress, String requestMethod, String requestUrl, Date requestTime, int httpStatus, long timeTaken) {
            this.userName      = userName;
            this.fromAddress   = fromAddress;
            this.requestMethod = requestMethod;
            this.requestUrl    = requestUrl;
            this.requestTime   = requestTime;
            this.httpStatus    = httpStatus;
            this.timeTaken     = timeTaken;
        }

        public void setHttpStatus(int httpStatus) { this.httpStatus = httpStatus; }

        public void setTimeTaken(long timeTaken) { this.timeTaken = timeTaken; }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append(DateTimeHelper.formatDateUTC(requestTime))
                    .append(FIELD_SEP).append(userName)
                    .append(FIELD_SEP).append(fromAddress)
                    .append(FIELD_SEP).append(requestMethod)
                    .append(FIELD_SEP).append(requestUrl)
                    .append(FIELD_SEP).append(httpStatus)
                    .append(FIELD_SEP).append(timeTaken);

            return sb.toString();
        }
    }
    // --- End AuditLog Inner Class ---
}