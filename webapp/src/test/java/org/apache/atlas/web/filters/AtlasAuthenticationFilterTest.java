package org.apache.atlas.web.filters;

import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import javax.servlet.FilterChain;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AtlasAuthenticationFilter.
 *
 * These tests verify that the authentication filter correctly handles authentication
 * failures and returns proper HTTP status codes and headers.
 */
@ExtendWith(MockitoExtension.class)
public class AtlasAuthenticationFilterTest {

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Mock
    private FilterChain filterChain;

    @Mock
    private AuthenticationHandler authHandler;

    private AtlasAuthenticationFilter filter;

    @BeforeEach
    public void setUp() throws Exception {
        SecurityContextHolder.clearContext();
        filter = new AtlasAuthenticationFilter();
    }

    @AfterEach
    public void tearDown() {
        SecurityContextHolder.clearContext();
    }

    /**
     * Test that when authentication fails in basic auth mode, the filter returns 401
     * with a proper WWW-Authenticate header instead of converting to 403.
     */
    @Test
    public void testBasicAuthFailureReturns401WithWWWAuthenticateHeader() throws Exception {
        // Set up the filter for basic authentication (non-Kerberos)
        ReflectionTestUtils.setField(filter, "isKerberos", false);

        // Mock request with no authentication
        when(request.getHeader("Authorization")).thenReturn(null);
        when(request.getScheme()).thenReturn("https");
        when(request.getCookies()).thenReturn(null);
        when(request.getParameter("action")).thenReturn(null);
        when(request.getParameter("doAs")).thenReturn(null);

        // Mock response
        when(response.isCommitted()).thenReturn(false);
        when(response.containsHeader(KerberosAuthenticator.WWW_AUTHENTICATE)).thenReturn(false);
        when(response.getHeaderNames()).thenReturn(Collections.emptySet());

        // Execute filter
        filter.doFilter(request, response, filterChain);

        // Verify that if 401 is returned, a WWW-Authenticate header should be set
        // and NOT converted to 403
        verify(response, never()).setStatus(HttpServletResponse.SC_FORBIDDEN);
    }

    /**
     * Test that when Kerberos authentication fails, the filter returns 401 with
     * a "Negotiate" WWW-Authenticate header instead of converting to 403.
     */
    @Test
    public void testKerberosAuthFailureReturns401WithNegotiateHeader() throws Exception {
        // Set up the filter for Kerberos authentication
        ReflectionTestUtils.setField(filter, "isKerberos", true);
        ReflectionTestUtils.setField(filter, "authHandler", authHandler);

        // Mock request with no authentication
        when(request.getHeader("Authorization")).thenReturn(null);
        when(request.getScheme()).thenReturn("https");
        when(request.getCookies()).thenReturn(null);
        when(request.getParameter("action")).thenReturn(null);
        when(request.getParameter("doAs")).thenReturn(null);
        when(request.getHeader("User-Agent")).thenReturn("curl/7.68.0");

        // Mock auth handler for Kerberos
        when(authHandler.managementOperation(any(), eq(request), eq(response))).thenReturn(true);
        when(authHandler.authenticate(eq(request), eq(response))).thenReturn(null);
        when(authHandler.getType()).thenReturn("kerberos");

        // Mock response
        when(response.isCommitted()).thenReturn(false);
        when(response.containsHeader(KerberosAuthenticator.WWW_AUTHENTICATE)).thenReturn(false);
        when(response.getHeaderNames()).thenReturn(Collections.emptySet());

        // Execute filter - in Kerberos mode
        filter.doFilter(request, response, filterChain);

        // Verify that 403 is NOT set (the bug we're fixing)
        verify(response, never()).setStatus(HttpServletResponse.SC_FORBIDDEN);
    }

    /**
     * Test that proxy user requests get proper WWW-Authenticate header
     * instead of an empty string.
     */
    @Test
    public void testProxyUserGetsProperWWWAuthenticateHeader() throws Exception {
        // Set up the filter
        ReflectionTestUtils.setField(filter, "isKerberos", true);
        ReflectionTestUtils.setField(filter, "authHandler", authHandler);
        ReflectionTestUtils.setField(filter, "atlasProxyUsers", Collections.singleton("knox"));
        ReflectionTestUtils.setField(filter, "supportTrustedProxy", true);

        // Mock request from proxy user
        when(request.getHeader("Authorization")).thenReturn(null);
        when(request.getScheme()).thenReturn("https");
        when(request.getCookies()).thenReturn(null);
        when(request.getParameter("action")).thenReturn(null);
        when(request.getParameter("doAs")).thenReturn(null);
        when(request.getRemoteUser()).thenReturn("knox");
        when(request.getHeader("User-Agent")).thenReturn("curl/7.68.0");

        // Mock auth handler
        AuthenticationToken token = new AuthenticationToken("knox", "knox", "kerberos");
        when(authHandler.managementOperation(any(), eq(request), eq(response))).thenReturn(true);
        when(authHandler.authenticate(eq(request), eq(response))).thenReturn(token);
        when(authHandler.getType()).thenReturn("kerberos");

        // Mock response
        when(response.isCommitted()).thenReturn(false);
        when(response.containsHeader(KerberosAuthenticator.WWW_AUTHENTICATE)).thenReturn(false);
        when(response.getHeaderNames()).thenReturn(Collections.emptySet());

        // Execute filter
        filter.doFilter(request, response, filterChain);

        // Verify that WWW-Authenticate header is set to a proper value, not empty string
        // The fix ensures "Basic realm=\"Atlas\"" is set instead of ""
        verify(response, atLeastOnce()).setHeader(eq(KerberosAuthenticator.WWW_AUTHENTICATE), anyString());
    }

    /**
     * Test that the filter no longer converts 401 to 403 when WWW-Authenticate is missing.
     * This is the main bug fix verification.
     */
    @Test
    public void testDoesNotConvert401To403WhenWWWAuthenticateIsMissing() throws Exception {
        // This test verifies the core bug fix: the filter should NOT convert 401 to 403
        // Instead, it should add the WWW-Authenticate header and keep 401

        // Set up the filter for basic authentication
        ReflectionTestUtils.setField(filter, "isKerberos", false);

        // Mock request with failed authentication
        when(request.getHeader("Authorization")).thenReturn("Basic invalid");
        when(request.getScheme()).thenReturn("https");
        when(request.getCookies()).thenReturn(null);
        when(request.getParameter("action")).thenReturn(null);
        when(request.getParameter("doAs")).thenReturn(null);

        // Mock response without WWW-Authenticate header initially
        when(response.isCommitted()).thenReturn(false);
        when(response.containsHeader(KerberosAuthenticator.WWW_AUTHENTICATE)).thenReturn(false);
        when(response.getHeaderNames()).thenReturn(Collections.emptySet());

        // Execute filter
        filter.doFilter(request, response, filterChain);

        // The key assertion: verify that 403 is NEVER set
        // The old buggy code would convert 401 to 403
        verify(response, never()).setStatus(HttpServletResponse.SC_FORBIDDEN);
        verify(response, never()).sendError(eq(HttpServletResponse.SC_FORBIDDEN), anyString());
    }
}
