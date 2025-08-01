package org.apache.atlas.web.filters;

import io.micrometer.core.instrument.Timer;
import org.apache.atlas.service.metrics.MetricUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.context.support.SpringBeanAutowiringSupport;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.inject.Inject;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotNull;
import java.io.IOException;

import static org.apache.atlas.web.filters.AuditFilter.X_ATLAN_CLIENT_ORIGIN;
import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * Filter used to record HTTP request & response metrics
 */
@Component
public class MetricsFilter extends OncePerRequestFilter {

    @Inject
    private MetricUtils metricUtils;

    public MetricsFilter() {
        SpringBeanAutowiringSupport.processInjectionBasedOnCurrentContext(this);
    }

    @Override
    protected void doFilterInternal(@NotNull HttpServletRequest request, @NotNull HttpServletResponse response, @NotNull FilterChain filterChain) throws ServletException, IOException {
        Timer.Sample timerSample = null;
        try {
            timerSample = metricUtils.start(request.getRequestURI());
            filterChain.doFilter(request, response);
        } finally {
            String clientOrigin = request.getHeader(X_ATLAN_CLIENT_ORIGIN);
            if (isEmpty(clientOrigin)) {
                clientOrigin = "other";
            }
            metricUtils.recordHttpTimer(timerSample, request.getMethod(), request.getRequestURI(), response.getStatus(), 
                                      "origin", clientOrigin);
        }
    }
}
