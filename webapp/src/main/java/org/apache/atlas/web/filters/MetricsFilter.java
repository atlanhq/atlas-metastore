package org.apache.atlas.web.filters;

import io.micrometer.core.instrument.Timer;
import org.apache.atlas.service.metrics.MetricUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.context.support.SpringBeanAutowiringSupport;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.inject.Inject;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

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
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        Timer.Sample timerSample = null;
        try {
            timerSample = metricUtils.start(request.getRequestURI());
            filterChain.doFilter(request, response);
        } finally {
            metricUtils.recordHttpTimer(timerSample, request.getMethod(), request.getRequestURI(), response.getStatus());
        }
    }
}
