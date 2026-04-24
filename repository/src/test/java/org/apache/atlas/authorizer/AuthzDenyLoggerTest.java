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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.atlas.authorize.AtlasAccessResult;
import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasRelationshipAccessRequest;
import org.apache.atlas.authorize.AtlasTypeAccessRequest;
import org.apache.atlas.discovery.PurposeDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.PurposeUserRequest;
import org.apache.atlas.model.discovery.PurposeUserResponse;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AuthzDenyLogger}. Uses a Logback {@link ListAppender} to capture
 * the WARN-level log lines and asserts on their formatted content.
 *
 * <p>These tests focus on two properties:
 * <ol>
 *   <li>The log line contains the expected fields in the expected format.</li>
 *   <li>The logger never throws under adversarial inputs (null everything, service failure, etc.).</li>
 * </ol>
 */
public class AuthzDenyLoggerTest {

    private Logger                   denyLogger;
    private ListAppender<ILoggingEvent> appender;
    private AtlasTypeRegistry        typeRegistry;

    @Before
    public void setUp() {
        denyLogger = (Logger) LoggerFactory.getLogger(AuthzDenyLogger.LOGGER_NAME);
        denyLogger.setLevel(Level.WARN);

        appender = new ListAppender<>();
        appender.start();
        denyLogger.addAppender(appender);

        typeRegistry = mock(AtlasTypeRegistry.class);

        AuthzDenyLogger.setPurposeDiscoveryServiceForTesting(null);
    }

    @After
    public void tearDown() {
        denyLogger.detachAppender(appender);
        AuthzDenyLogger.setPurposeDiscoveryServiceForTesting(null);
    }

    // ------------------------------------------------------------
    // Entity deny
    // ------------------------------------------------------------

    @Test
    public void logEntityDeny_happyPath_emitsOneWarnWithAllFields() throws Exception {
        AtlasEntityHeader entity = entity("hive_table", "guid-abc", "default.db.tbl@cluster");
        AtlasEntityAccessRequest req = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_DELETE, entity);
        req.setUser("alice", set("g1", "g2"));

        AtlasAccessResult result = explicitDenyResult("policy-123", "merged_auth", 100);

        AuthzDenyLogger.setPurposeDiscoveryServiceForTesting(mockPurposeService("purpose-guid-1", "purpose-guid-2"));

        AuthzDenyLogger.logEntityDeny(req, result, "delete entity");

        String line = singleWarnLine();
        assertContains(line, "AUTHZ_DENY");
        assertContains(line, "user=\"alice\"");
        assertContains(line, "groups=2");
        assertContains(line, "action=\"entity-delete\"");
        assertContains(line, "entity=\"hive_table/guid-abc\"");
        assertContains(line, "qn=\"default.db.tbl@cluster\"");
        assertContains(line, "cause=\"UNAUTHORIZED_ACCESS\"");
        assertContains(line, "policyId=\"policy-123\"");
        assertContains(line, "enforcer=\"merged_auth\"");
        assertContains(line, "explicitDeny=true");
        assertContains(line, "priority=100");
        assertContains(line, "purposes=\"purpose-guid-1,purpose-guid-2\"");
        assertContains(line, "msg=\"delete entity\"");
    }

    @Test
    public void logEntityDeny_withPolicyName_includesBothPolicyIdAndName() throws Exception {
        AtlasEntityHeader entity = entity("Table", "g1", "qn-with-name");
        AtlasEntityAccessRequest req = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_DELETE, entity);
        req.setUser("juno", Collections.emptySet());

        AtlasAccessResult result = explicitDenyResult("pol-9f8e2", "deny-prod-tables", "abac_auth", 100);

        AuthzDenyLogger.logEntityDeny(req, result, "delete entity");

        String line = singleWarnLine();
        assertContains(line, "policyId=\"pol-9f8e2\"");
        assertContains(line, "policyName=\"deny-prod-tables\"");
    }

    @Test
    public void logEntityDeny_emptyPolicyName_rendersNullToken() throws Exception {
        AtlasEntityHeader entity = entity("Table", "g1", "qn-empty-name");
        AtlasEntityAccessRequest req = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, entity);
        req.setUser("kai", Collections.emptySet());

        // policyName explicitly empty — helper maps to null; logger should render "null".
        AtlasAccessResult result = explicitDenyResult("pol-blank", "", "abac_auth", 0);
        result.setPolicyName("");
        AuthzDenyLogger.logEntityDeny(req, result, "");

        String line = singleWarnLine();
        assertContains(line, "policyId=\"pol-blank\"");
        assertContains(line, "policyName=\"null\"");
    }

    @Test
    public void logEntityDeny_nullPolicyName_doesNotThrow() throws Exception {
        AtlasEntityHeader entity = entity("Table", "g1", "qn-null-name");
        AtlasEntityAccessRequest req = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, entity);
        req.setUser("lea", Collections.emptySet());

        AtlasAccessResult result = new AtlasAccessResult(false, "pol-anon", 0);
        // policyName remains null by default.

        AuthzDenyLogger.logEntityDeny(req, result, "read");

        String line = singleWarnLine();
        assertContains(line, "policyId=\"pol-anon\"");
        assertContains(line, "policyName=\"null\"");
    }

    @Test
    public void logEntityDeny_implicitDeny_rendersPolicyIdAsImplicit() throws Exception {
        AtlasEntityHeader entity = entity("Table", "g1", "qn-1");
        AtlasEntityAccessRequest req = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, entity);
        req.setUser("bob", Collections.emptySet());

        AtlasAccessResult implicit = new AtlasAccessResult(false);   // policyId defaults to "-1"

        AuthzDenyLogger.logEntityDeny(req, implicit, "update entity");

        String line = singleWarnLine();
        assertContains(line, "policyId=\"implicit\"");
        assertContains(line, "explicitDeny=false");
    }

    @Test
    public void logEntityDeny_nullQualifiedName_rendersNullNoNpe() throws Exception {
        AtlasEntityHeader entity = new AtlasEntityHeader();
        entity.setTypeName("Table");
        entity.setGuid("g1");
        // no attributes map → qn lookup returns null
        AtlasEntityAccessRequest req = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, entity);
        req.setUser("carol", Collections.emptySet());

        AuthzDenyLogger.logEntityDeny(req, new AtlasAccessResult(false), "");

        String line = singleWarnLine();
        assertContains(line, "qn=\"null\"");
    }

    @Test
    public void logEntityDeny_longQualifiedName_truncates() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append('x');
        }
        AtlasEntityHeader entity = entity("Table", "g1", sb.toString());
        AtlasEntityAccessRequest req = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, entity);
        req.setUser("dave", Collections.emptySet());

        AuthzDenyLogger.logEntityDeny(req, new AtlasAccessResult(false), "");

        String line = singleWarnLine();
        assertContains(line, "...");
        assertTrue("truncated qn should appear near the max length",
                line.length() < sb.length() + 200);
    }

    @Test
    public void logEntityDeny_nullEverything_doesNotThrow() {
        AuthzDenyLogger.logEntityDeny(null, null, null);
        // Intentionally no message assertions — we only care that it did not throw.
        assertTrue(appender.list.size() <= 1);
    }

    @Test
    public void logEntityDeny_purposeServiceThrows_logsBaseLine() throws Exception {
        PurposeDiscoveryService svc = mock(PurposeDiscoveryService.class);
        when(svc.discoverPurposesForUser(any(PurposeUserRequest.class)))
                .thenThrow(new RuntimeException("ES unavailable"));
        AuthzDenyLogger.setPurposeDiscoveryServiceForTesting(svc);

        AtlasEntityHeader entity = entity("Table", "g1", "qn-1");
        AtlasEntityAccessRequest req = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, entity);
        req.setUser("erin", Collections.emptySet());

        AuthzDenyLogger.logEntityDeny(req, new AtlasAccessResult(false), "read");

        String line = singleWarnLine();
        assertContains(line, "purposes=\"\"");
        verify(svc).discoverPurposesForUser(any(PurposeUserRequest.class));
    }

    @Test
    public void logEntityDeny_enrichmentPassesUserAndGroups() throws Exception {
        PurposeDiscoveryService svc = mockPurposeService("p1");
        AuthzDenyLogger.setPurposeDiscoveryServiceForTesting(svc);

        AtlasEntityHeader entity = entity("Table", "g1", "qn-1");
        AtlasEntityAccessRequest req = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, entity);
        req.setUser("frank", set("team-a", "team-b"));

        AuthzDenyLogger.logEntityDeny(req, new AtlasAccessResult(false), "");

        org.mockito.ArgumentCaptor<PurposeUserRequest> captor =
                org.mockito.ArgumentCaptor.forClass(PurposeUserRequest.class);
        verify(svc).discoverPurposesForUser(captor.capture());
        assertEquals("frank", captor.getValue().getUsername());
        assertNotNull(captor.getValue().getGroups());
        assertEquals(2, captor.getValue().getGroups().size());
    }

    // ------------------------------------------------------------
    // Relationship deny
    // ------------------------------------------------------------

    @Test
    public void logRelationshipDeny_rendersBothEnds() throws Exception {
        AtlasEntityHeader end1 = entity("AtlasGlossaryTerm", "term-1", "term@glossary");
        AtlasEntityHeader end2 = entity("Table", "tbl-1", "default.db.tbl@cluster");

        AtlasRelationshipAccessRequest req = new AtlasRelationshipAccessRequest(
                typeRegistry, AtlasPrivilege.RELATIONSHIP_ADD, "atlas_glossary_semantic_assignment", end1, end2);
        req.setUser("grace", Collections.emptySet());

        AtlasAccessResult result = explicitDenyResult("rel-policy", "term-to-asset-deny", "atlas", 0);

        AuthzDenyLogger.logRelationshipDeny(req, result, "add relationship");

        String line = singleWarnLine();
        assertContains(line, "action=\"add-relationship\"");
        assertContains(line, "relationshipType=\"atlas_glossary_semantic_assignment\"");
        assertContains(line, "end1=\"AtlasGlossaryTerm/term-1\"");
        assertContains(line, "end1Qn=\"term@glossary\"");
        assertContains(line, "end2=\"Table/tbl-1\"");
        assertContains(line, "end2Qn=\"default.db.tbl@cluster\"");
        assertContains(line, "policyId=\"rel-policy\"");
        assertContains(line, "policyName=\"term-to-asset-deny\"");
    }

    // ------------------------------------------------------------
    // Type + admin deny
    // ------------------------------------------------------------

    @Test
    public void logTypeDeny_includesTypeDefName() throws Exception {
        AtlasBaseTypeDef def = new AtlasEntityDef("MyCustomType");
        AtlasTypeAccessRequest req = new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_CREATE, def);
        req.setUser("heidi", Collections.emptySet());

        AuthzDenyLogger.logTypeDeny(req, new AtlasAccessResult(false, "policy-t1"), "");

        String line = singleWarnLine();
        assertContains(line, "typeDef=\"MyCustomType\"");
        assertContains(line, "action=\"type-create\"");
        assertContains(line, "policyId=\"policy-t1\"");
        assertContains(line, "explicitDeny=true");
    }

    @Test
    public void logAdminDeny_scopeAdmin() throws Exception {
        AtlasAdminAccessRequest req = new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_EXPORT);
        req.setUser("ivan", Collections.emptySet());

        AuthzDenyLogger.logAdminDeny(req, new AtlasAccessResult(false), "export");

        String line = singleWarnLine();
        assertContains(line, "scope=\"admin\"");
        assertContains(line, "action=\"admin-export\"");
        assertContains(line, "user=\"ivan\"");
    }

    // ------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------

    private AtlasEntityHeader entity(String typeName, String guid, String qualifiedName) {
        AtlasEntityHeader h = new AtlasEntityHeader();
        h.setTypeName(typeName);
        h.setGuid(guid);
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("qualifiedName", qualifiedName);
        h.setAttributes(attrs);
        return h;
    }

    private Set<String> set(String... values) {
        return new HashSet<>(Arrays.asList(values));
    }

    private AtlasAccessResult explicitDenyResult(String policyId, String enforcer, int priority) {
        return explicitDenyResult(policyId, null, enforcer, priority);
    }

    private AtlasAccessResult explicitDenyResult(String policyId, String policyName, String enforcer, int priority) {
        AtlasAccessResult r = new AtlasAccessResult(false, policyId, priority);
        r.setPolicyName(policyName);
        r.setEnforcer(enforcer);
        return r;
    }

    private PurposeDiscoveryService mockPurposeService(String... purposeGuids) throws AtlasBaseException {
        PurposeDiscoveryService svc = mock(PurposeDiscoveryService.class);
        List<AtlasEntityHeader> purposes = new java.util.ArrayList<>();
        for (String g : purposeGuids) {
            AtlasEntityHeader h = new AtlasEntityHeader();
            h.setTypeName("Purpose");
            h.setGuid(g);
            purposes.add(h);
        }
        PurposeUserResponse resp = new PurposeUserResponse();
        resp.setPurposes(purposes);
        when(svc.discoverPurposesForUser(any(PurposeUserRequest.class))).thenReturn(resp);
        return svc;
    }

    private String singleWarnLine() {
        List<ILoggingEvent> events = appender.list;
        assertFalse("expected at least one log event", events.isEmpty());
        ILoggingEvent evt = events.get(events.size() - 1);
        assertEquals(Level.WARN, evt.getLevel());
        return evt.getFormattedMessage();
    }

    private static void assertContains(String haystack, String needle) {
        assertTrue("expected log line to contain: " + needle + "\nactual: " + haystack,
                haystack.contains(needle));
    }
}
