package org.apache.atlas.web.filters;

import io.micrometer.core.instrument.Timer;
import org.apache.atlas.service.metrics.MetricUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.inject.Inject;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;

/**
 * Filter used to record HTTP request & response metrics
 */
@Component
public class MetricsFilter extends OncePerRequestFilter {

    @Inject
    private MetricUtils metricUtils;

    @Override
    protected void initFilterBean() throws ServletException {
        WebApplicationContext ctx = WebApplicationContextUtils.findWebApplicationContext(getServletContext());
        if (ctx != null) {
            ctx.getAutowireCapableBeanFactory().autowireBean(this);
        }
    }

    @Override
    protected void doFilterInternal(@NotNull HttpServletRequest request, @NotNull HttpServletResponse response, @NotNull FilterChain filterChain) throws ServletException, IOException {
        Timer.Sample timerSample = null;
        try {
            timerSample = metricUtils.start(request.getRequestURI());
            filterChain.doFilter(request, response);
        } finally {
            metricUtils.recordHttpTimer(timerSample, request.getMethod(), request.getRequestURI(), response.getStatus());
        }
    }
}
