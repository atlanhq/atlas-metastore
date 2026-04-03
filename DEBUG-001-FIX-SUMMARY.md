# DEBUG-001: Fix 403 Authentication Error Bug

## Problem Statement

The AtlasAuthenticationFilter incorrectly converts HTTP 401 (Unauthorized) responses to HTTP 403 (Forbidden) responses when the WWW-Authenticate header is missing. This masks authentication failures as authorization failures, making debugging difficult for API clients.

## Root Cause Analysis

### Issue #1: 401 to 403 Conversion (Lines 562-566)

The authentication filter contains logic that converts 401 to 403:

```java
// If response code is 401. Then WWW-Authenticate Header should be
// present.. reset to 403 if not found..
if (errCode == HttpServletResponse.SC_UNAUTHORIZED && !httpResponse.containsHeader(KerberosAuthenticator.WWW_AUTHENTICATE)) {
    errCode = HttpServletResponse.SC_FORBIDDEN;
}
```

**Why this is wrong:**
- **HTTP 401** indicates authentication credentials are missing or invalid - the client should retry with proper credentials
- **HTTP 403** indicates the server refuses to authorize the request even with valid credentials - retrying with different credentials won't help
- Converting 401 to 403 misleads clients into thinking they have a permission problem rather than an authentication problem

### Issue #2: Empty WWW-Authenticate Header (Line 532)

For proxy users, the filter sets an empty WWW-Authenticate header:

```java
httpResponse.setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, "");
```

**Why this is wrong:**
- An empty WWW-Authenticate header doesn't provide the client with information about how to authenticate
- The HTTP specification requires that 401 responses include a WWW-Authenticate header with a proper authentication challenge

## Solution

### Fix #1: Proper WWW-Authenticate Headers Instead of Conversion

Instead of converting 401 to 403, the fix ensures a proper WWW-Authenticate header is always set:

```java
// Ensure WWW-Authenticate header is set for 401 responses
if (errCode == HttpServletResponse.SC_UNAUTHORIZED && !httpResponse.containsHeader(KerberosAuthenticator.WWW_AUTHENTICATE)) {
    // Set appropriate authentication challenge based on auth method
    if (isKerberos) {
        httpResponse.setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, "Negotiate");
    } else {
        httpResponse.setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, "Basic realm=\"Atlas\"");
    }
}
```

### Fix #2: Proper WWW-Authenticate for Proxy Users

For proxy users, the fix sets a proper authentication challenge:

```java
httpResponse.setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, "Basic realm=\"Atlas\"");
```

## Impact

### Before the Fix
- Authentication failures return 403 Forbidden
- Clients think they have insufficient permissions
- Debugging is difficult because the error code doesn't match the actual problem
- Empty WWW-Authenticate headers don't guide clients on how to authenticate

### After the Fix
- Authentication failures correctly return 401 Unauthorized with proper WWW-Authenticate headers
- Clients understand they need to provide or correct authentication credentials
- Debugging is straightforward: 401 = authentication issue, 403 = authorization issue
- WWW-Authenticate headers guide clients on the authentication method ("Negotiate" for Kerberos, "Basic realm=\"Atlas\"" for basic auth)

## Testing

Created comprehensive unit tests in `AtlasAuthenticationFilterTest.java` that verify:

1. **testBasicAuthFailureReturns401WithWWWAuthenticateHeader**: Basic auth failures return 401 with proper header
2. **testKerberosAuthFailureReturns401WithNegotiateHeader**: Kerberos auth failures return 401 with "Negotiate" header
3. **testProxyUserGetsProperWWWAuthenticateHeader**: Proxy users get proper headers instead of empty strings
4. **testDoesNotConvert401To403WhenWWWAuthenticateIsMissing**: Verifies the core bug fix - no 401 to 403 conversion

## Files Modified

1. `webapp/src/main/java/org/apache/atlas/web/filters/AtlasAuthenticationFilter.java`
   - Line 532: Changed empty WWW-Authenticate header to "Basic realm=\"Atlas\""
   - Lines 562-573: Changed 401-to-403 conversion logic to proper WWW-Authenticate header setting

2. `webapp/src/test/java/org/apache/atlas/web/filters/AtlasAuthenticationFilterTest.java` (NEW)
   - Added comprehensive unit tests for the authentication filter

## HTTP Status Code Reference

For clarity on the correct usage of HTTP status codes:

- **401 Unauthorized**: The request requires user authentication. The response MUST include a WWW-Authenticate header containing a challenge applicable to the requested resource.
- **403 Forbidden**: The server understood the request, but refuses to authorize it. Authorization will not help and the request SHOULD NOT be repeated.

## Related Tickets

This fix addresses the root cause of authentication-related 403 errors. Previous fixes addressed authorization logic (DG-1235, DG-996) but this fix addresses the authentication layer that was masking authentication failures as authorization failures.
