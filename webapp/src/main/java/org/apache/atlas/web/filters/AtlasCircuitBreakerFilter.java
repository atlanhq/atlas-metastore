package org.apache.atlas.web.filters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.web.service.AtlasCircuitBreakerService;
import org.apache.atlas.web.service.AtlasCircuitBreakerService.CircuitBreakerResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * CIRCUIT BREAKER FILTER: Protects system from overload by rejecting requests when CPU/heap usage is too high
 * 
 * This filter runs early in the request processing chain to:
 * 1. Check system resource usage via AtlasCircuitBreakerService
 * 2. Reject requests with 429 (Too Many Requests) when overloaded
 * 3. Provide detailed system metrics in response and Retry-After header
 * 4. Allow health check endpoints to pass through even when circuit is open
 * 5. Log circuit breaker decisions for monitoring
 * 
 * Filter Order: Runs before AuditFilter to minimize resource usage during overload
 */
@Component
@Order(1) // Run before AuditFilter (Order 2) to minimize overhead
public class AtlasCircuitBreakerFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasCircuitBreakerFilter.class);
    
    @Autowired
    private AtlasCircuitBreakerService circuitBreakerService;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // Endpoints that should always pass through even when circuit is open
    private static final Set<String> BYPASS_ENDPOINTS = new HashSet<>(Arrays.asList(
        "/admin/health",
        "/admin/memory/status", 
        "/admin/memory/cleanup",
        "/admin/health/cassandra",
        "/admin/health/cassandra/ping",
        "/metrics"
    ));
    
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("AtlasCircuitBreakerFilter initialized");
    }
    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        
        // Check if this endpoint should bypass circuit breaker
        if (shouldBypassCircuitBreaker(httpRequest)) {
            chain.doFilter(request, response);
            return;
        }
        
        // Check circuit breaker status
        CircuitBreakerResult result = circuitBreakerService.checkRequest();
        
        if (!result.isAllowed()) {
            // Circuit breaker is open - reject the request
            handleRejectedRequest(httpRequest, httpResponse, result);
            return;
        }
        
        // Request allowed - continue processing
        chain.doFilter(request, response);
    }
    
    @Override
    public void destroy() {
        LOG.info("AtlasCircuitBreakerFilter destroyed");
    }
    
    /**
     * Check if the request should bypass the circuit breaker
     * Health checks and admin endpoints should always be accessible for monitoring
     */
    private boolean shouldBypassCircuitBreaker(HttpServletRequest request) {
        String requestURI = request.getRequestURI();
        String contextPath = request.getContextPath();
        
        // Remove context path for comparison
        if (contextPath != null && !contextPath.isEmpty() && requestURI.startsWith(contextPath)) {
            requestURI = requestURI.substring(contextPath.length());
        }
        
        // Check exact matches
        if (BYPASS_ENDPOINTS.contains(requestURI)) {
            return true;
        }
        
        // Check path prefixes
        for (String bypassPath : BYPASS_ENDPOINTS) {
            if (requestURI.startsWith(bypassPath)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Handle rejected request by sending 429 response with detailed metrics
     */
    private void handleRejectedRequest(HttpServletRequest request, HttpServletResponse response,
                                       CircuitBreakerResult result) throws IOException {
        
        String requestURI = request.getRequestURI();
        String method = request.getMethod();
        String remoteAddr = getClientIpAddress(request);
        
        // Log the rejection for monitoring
        LOG.warn("Circuit breaker REJECTED request: {} {} from {} - CPU: {}%, Heap: {}%, RetryAfter: {}s",
                method, requestURI, remoteAddr,
                String.format("%.1f", circuitBreakerService.getCurrentCpuUsage()),
                String.format("%.1f", circuitBreakerService.getCurrentHeapUsage()),
                result.getRetryAfterSeconds());
        
        // Set response headers
        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        response.setHeader("Retry-After", String.valueOf(result.getRetryAfterSeconds()));
        response.setHeader("X-RateLimit-Limit", "system-capacity");
        response.setHeader("X-RateLimit-Remaining", "0");
        response.setHeader("X-RateLimit-Reset", String.valueOf(System.currentTimeMillis() / 1000 + result.getRetryAfterSeconds()));
        response.setContentType("application/json");
        
        // Create detailed error response
        Map<String, Object> errorResponse = createErrorResponse(result);
        
        // Write response
        String jsonResponse = objectMapper.writeValueAsString(errorResponse);
        response.getWriter().write(jsonResponse);
        response.getWriter().flush();
    }
    
    /**
     * Create comprehensive error response with system metrics and retry information
     */
    private Map<String, Object> createErrorResponse(CircuitBreakerResult result) {
        Map<String, Object> response = new HashMap<>();
        
        // Error details
        response.put("error", "TOO_MANY_REQUESTS");
        response.put("message", "System is currently overloaded. Please retry after the specified time.");
        response.put("reason", result.getReason());
        response.put("timestamp", System.currentTimeMillis());
        
        // Retry information
        Map<String, Object> retryInfo = new HashMap<>();
        retryInfo.put("retryAfterSeconds", result.getRetryAfterSeconds());
        retryInfo.put("retryAfterTimestamp", System.currentTimeMillis() + (result.getRetryAfterSeconds() * 1000L));
        retryInfo.put("recommendation", "Implement exponential backoff and check system metrics before retrying");
        response.put("retry", retryInfo);
        
        // Current system metrics
        Map<String, Object> systemMetrics = new HashMap<>();
        systemMetrics.put("cpuUsagePercent", Math.round(circuitBreakerService.getCurrentCpuUsage() * 10.0) / 10.0);
        systemMetrics.put("heapUsagePercent", Math.round(circuitBreakerService.getCurrentHeapUsage() * 10.0) / 10.0);
        systemMetrics.put("circuitBreakerState", circuitBreakerService.getState().name());
        systemMetrics.put("rejectedRequestCount", circuitBreakerService.getRejectedRequestCount());
        systemMetrics.put("totalRequestCount", circuitBreakerService.getTotalRequestCount());
        response.put("systemMetrics", systemMetrics);
        
        // Suggested actions
        response.put("suggestedActions", Arrays.asList(
            "Reduce request rate and implement backoff",
            "Check system metrics at /admin/memory/status",
            "Monitor circuit breaker state via /metrics endpoint",
            "Consider scaling resources if overload persists"
        ));
        
        return response;
    }
    
    /**
     * Get client IP address handling proxy headers
     */
    private String getClientIpAddress(HttpServletRequest request) {
        String[] headerNames = {
            "X-Forwarded-For",
            "X-Real-IP", 
            "Proxy-Client-IP",
            "WL-Proxy-Client-IP",
            "HTTP_X_FORWARDED_FOR",
            "HTTP_X_FORWARDED",
            "HTTP_X_CLUSTER_CLIENT_IP",
            "HTTP_CLIENT_IP",
            "HTTP_FORWARDED_FOR",
            "HTTP_FORWARDED",
            "HTTP_VIA",
            "REMOTE_ADDR"
        };
        
        for (String headerName : headerNames) {
            String ip = request.getHeader(headerName);
            if (ip != null && !ip.isEmpty() && !"unknown".equalsIgnoreCase(ip)) {
                // Take the first IP if multiple are present
                if (ip.contains(",")) {
                    ip = ip.split(",")[0].trim();
                }
                return ip;
            }
        }
        
        return request.getRemoteAddr();
    }
} 