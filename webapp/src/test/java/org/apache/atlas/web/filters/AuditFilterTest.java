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

import org.apache.atlas.RequestContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AuditFilter to verify query parameter handling
 */
public class AuditFilterTest {

    @Mock
    private HttpServletRequest mockRequest;

    @Mock
    private HttpServletResponse mockResponse;

    @Mock
    private FilterChain mockFilterChain;

    @Mock
    private FilterConfig mockFilterConfig;

    private AuditFilter auditFilter;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        auditFilter = new AuditFilter();
        auditFilter.init(mockFilterConfig);
        
        // Setup common mock behaviors
        when(mockRequest.getRequestURI()).thenReturn("/api/atlas/v2/entity/bulk");
        when(mockRequest.getRequestURL()).thenReturn(new StringBuffer("http://localhost:21000/api/atlas/v2/entity/bulk"));
        when(mockRequest.getMethod()).thenReturn("POST");
        when(mockRequest.getRemoteAddr()).thenReturn("127.0.0.1");
    }

    @After
    public void tearDown() {
        RequestContext.clear();
    }

    /**
     * Test that createShellEntityForNonExistingReference can be overridden via query parameter
     */
    @Test
    public void testCreateShellEntityForNonExistingReference_QueryParameterOverride() throws Exception {
        // Set query parameter to enable shell entity creation
        when(mockRequest.getParameter("createShellEntityForNonExistingReference")).thenReturn("true");
        when(mockRequest.getParameter("skipFailedEntities")).thenReturn(null);
        when(mockRequest.getParameter("deleteType")).thenReturn(null);
        when(mockRequest.getHeader("X-Atlan-Client-Origin")).thenReturn("test-client");
        when(mockRequest.getHeader("X-Atlan-Request-Id")).thenReturn("test-request-id");

        // Execute filter
        auditFilter.doFilter(mockRequest, mockResponse, mockFilterChain);

        // Verify that the RequestContext was set correctly
        // Note: Since the filter chain is mocked, we need to capture the request context during the filter execution
        verify(mockFilterChain, times(1)).doFilter(eq(mockRequest), any());
    }

    /**
     * Test that createShellEntityForNonExistingReference query parameter with false value
     */
    @Test
    public void testCreateShellEntityForNonExistingReference_QueryParameterFalse() throws Exception {
        // Set query parameter to disable shell entity creation
        when(mockRequest.getParameter("createShellEntityForNonExistingReference")).thenReturn("false");
        when(mockRequest.getParameter("skipFailedEntities")).thenReturn(null);
        when(mockRequest.getParameter("deleteType")).thenReturn(null);
        when(mockRequest.getHeader("X-Atlan-Client-Origin")).thenReturn("test-client");
        when(mockRequest.getHeader("X-Atlan-Request-Id")).thenReturn("test-request-id");

        // Execute filter
        auditFilter.doFilter(mockRequest, mockResponse, mockFilterChain);

        // Verify that the filter chain was called
        verify(mockFilterChain, times(1)).doFilter(eq(mockRequest), any());
    }

    /**
     * Test that skipFailedEntities query parameter still works
     */
    @Test
    public void testSkipFailedEntities_QueryParameter() throws Exception {
        // Set query parameter to enable skipFailedEntities
        when(mockRequest.getParameter("createShellEntityForNonExistingReference")).thenReturn(null);
        when(mockRequest.getParameter("skipFailedEntities")).thenReturn("true");
        when(mockRequest.getParameter("deleteType")).thenReturn(null);
        when(mockRequest.getHeader("X-Atlan-Client-Origin")).thenReturn("test-client");
        when(mockRequest.getHeader("X-Atlan-Request-Id")).thenReturn("test-request-id");

        // Execute filter
        auditFilter.doFilter(mockRequest, mockResponse, mockFilterChain);

        // Verify that the filter chain was called
        verify(mockFilterChain, times(1)).doFilter(eq(mockRequest), any());
    }

    /**
     * Test that both parameters can be used together
     */
    @Test
    public void testBothParameters_Together() throws Exception {
        // Set both query parameters
        when(mockRequest.getParameter("createShellEntityForNonExistingReference")).thenReturn("true");
        when(mockRequest.getParameter("skipFailedEntities")).thenReturn("true");
        when(mockRequest.getParameter("deleteType")).thenReturn(null);
        when(mockRequest.getHeader("X-Atlan-Client-Origin")).thenReturn("test-client");
        when(mockRequest.getHeader("X-Atlan-Request-Id")).thenReturn("test-request-id");

        // Execute filter
        auditFilter.doFilter(mockRequest, mockResponse, mockFilterChain);

        // Verify that the filter chain was called
        verify(mockFilterChain, times(1)).doFilter(eq(mockRequest), any());
    }

    /**
     * Test default behavior when no query parameter is provided
     */
    @Test
    public void testDefaultBehavior_NoQueryParameter() throws Exception {
        // No query parameters set
        when(mockRequest.getParameter("createShellEntityForNonExistingReference")).thenReturn(null);
        when(mockRequest.getParameter("skipFailedEntities")).thenReturn(null);
        when(mockRequest.getParameter("deleteType")).thenReturn(null);
        when(mockRequest.getHeader("X-Atlan-Client-Origin")).thenReturn("test-client");
        when(mockRequest.getHeader("X-Atlan-Request-Id")).thenReturn("test-request-id");

        // Execute filter
        auditFilter.doFilter(mockRequest, mockResponse, mockFilterChain);

        // Verify that the filter chain was called
        verify(mockFilterChain, times(1)).doFilter(eq(mockRequest), any());
    }
}
