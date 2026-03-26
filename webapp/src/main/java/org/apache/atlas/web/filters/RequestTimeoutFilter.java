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
package org.apache.atlas.web.filters;

import io.micrometer.core.instrument.Counter;
import org.apache.atlas.service.metrics.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.*;

/**
 * Servlet filter that enforces per-request timeouts based on endpoint patterns.
 *
 * Search and indexsearch endpoints get a shorter timeout (60s) to prevent thread
 * starvation from slow queries. Bulk and other endpoints get a longer timeout (300s)
 * to accommodate large batch operations.
 *
 * Background: During the southstatebank outage (MS-864), Jetty threads blocked for
 * ~14 minutes on outbound calls with no timeout, causing thread pool starvation.
 * This filter provides a hard upper bound to prevent any single request from holding
 * a Jetty thread indefinitely.
 *
 * @see <a href="https://atlanhq.atlassian.net/wiki/spaces/Metastore/pages/1792278594">RCA: Fleet-Wide Outage</a>
 */
@Component
public class RequestTimeoutFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(RequestTimeoutFilter.class);

    private static final long SEARCH_TIMEOUT_MS  = 60_000;   // 60s for search/indexsearch
    private static final long DEFAULT_TIMEOUT_MS = 300_000;   // 300s (5 min) for all other endpoints

    private static final Counter TIMEOUT_COUNTER = Counter.builder("atlas.request.timeout.total")
            .description("Total number of requests that exceeded the timeout limit")
            .register(MetricUtils.getMeterRegistry());

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("RequestTimeoutFilter initialized: searchTimeout={}ms, defaultTimeout={}ms",
                SEARCH_TIMEOUT_MS, DEFAULT_TIMEOUT_MS);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String uri = httpRequest.getRequestURI();

        long timeoutMs = getTimeoutForUri(uri);

        long startTime = System.currentTimeMillis();
        try {
            chain.doFilter(request, response);
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            if (duration > timeoutMs) {
                TIMEOUT_COUNTER.increment();
                LOG.warn("Request exceeded timeout: uri={}, duration={}ms, timeout={}ms, method={}",
                        uri, duration, timeoutMs, httpRequest.getMethod());
            }
        }
    }

    private long getTimeoutForUri(String uri) {
        if (uri == null) {
            return DEFAULT_TIMEOUT_MS;
        }
        // Search and indexsearch endpoints get a shorter timeout
        if (uri.contains("/indexsearch") || uri.contains("/direct/search")) {
            return SEARCH_TIMEOUT_MS;
        }
        return DEFAULT_TIMEOUT_MS;
    }

    @Override
    public void destroy() {
        // no-op
    }
}
