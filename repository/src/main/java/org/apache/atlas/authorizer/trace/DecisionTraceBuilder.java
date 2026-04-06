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
package org.apache.atlas.authorizer.trace;

import org.apache.atlas.authorize.*;

import java.util.*;

/**
 * Builds access decision traces from collected policy information.
 * Applies 5-level precedence model to determine final decisions.
 */
public class DecisionTraceBuilder {

    /**
     * Builds access decisions for all principals in the response.
     * @param response The accessor response with users/groups/roles
     * @param rangerPolicies Map of Ranger policy traces
     * @param abacPolicies Map of ABAC policy traces
     * @return List of access decisions explaining why each principal has access
     */
    public static List<AtlasAccessDecision> buildDecisions(
        AtlasAccessorResponse response,
        Map<String, PolicyTrace> rangerPolicies,
        Map<String, PolicyTrace> abacPolicies
    ) {
        if (response == null) {
            return Collections.emptyList();
        }

        Set<String> allPrincipals = collectAllPrincipals(response);
        List<AtlasAccessDecision> decisions = new ArrayList<>();

        for (String principal : allPrincipals) {
            decisions.add(buildDecisionForPrincipal(principal, response, rangerPolicies, abacPolicies));
        }

        return decisions;
    }

    /**
     * Collects all unique principals (users, groups, roles) from the response.
     */
    private static Set<String> collectAllPrincipals(AtlasAccessorResponse response) {
        Set<String> principals = new HashSet<>();

        if (response.getUsers() != null) {
            principals.addAll(response.getUsers());
        }
        if (response.getGroups() != null) {
            principals.addAll(response.getGroups());
        }
        if (response.getRoles() != null) {
            principals.addAll(response.getRoles());
        }
        if (response.getDenyUsers() != null) {
            principals.addAll(response.getDenyUsers());
        }
        if (response.getDenyGroups() != null) {
            principals.addAll(response.getDenyGroups());
        }
        if (response.getDenyRoles() != null) {
            principals.addAll(response.getDenyRoles());
        }

        return principals;
    }

    /**
     * Builds an access decision for a single principal.
     */
    private static AtlasAccessDecision buildDecisionForPrincipal(
        String principal,
        AtlasAccessorResponse response,
        Map<String, PolicyTrace> rangerPolicies,
        Map<String, PolicyTrace> abacPolicies
    ) {
        AtlasAccessDecision decision = new AtlasAccessDecision();
        decision.setPrincipal(principal);
        decision.setPrincipalType(inferPrincipalType(principal, response));

        // Collect all policies (both allow and deny)
        List<PolicyTrace> allPolicies = new ArrayList<>();
        if (rangerPolicies != null) {
            allPolicies.addAll(rangerPolicies.values());
        }
        if (abacPolicies != null) {
            allPolicies.addAll(abacPolicies.values());
        }

        // Sort by precedence: override deny > override allow > explicit deny > allow
        allPolicies.sort((p1, p2) -> {
            int level1 = getPrecedenceLevel(p1);
            int level2 = getPrecedenceLevel(p2);
            return Integer.compare(level1, level2);
        });

        decision.setPolicies(allPolicies);

        // Determine final decision based on which list the principal is in
        boolean isDenied = (response.getDenyUsers() != null && response.getDenyUsers().contains(principal)) ||
                          (response.getDenyGroups() != null && response.getDenyGroups().contains(principal)) ||
                          (response.getDenyRoles() != null && response.getDenyRoles().contains(principal));

        decision.setDecision(isDenied ? AccessDecision.DENY : AccessDecision.ALLOW);
        decision.setFinalReason(explainPrecedenceRule(allPolicies, decision.getDecision()));

        return decision;
    }

    /**
     * Infers the principal type based on which list it appears in.
     */
    private static PrincipalType inferPrincipalType(String principal, AtlasAccessorResponse response) {
        if ((response.getUsers() != null && response.getUsers().contains(principal)) ||
            (response.getDenyUsers() != null && response.getDenyUsers().contains(principal))) {
            return PrincipalType.USER;
        } else if ((response.getGroups() != null && response.getGroups().contains(principal)) ||
                   (response.getDenyGroups() != null && response.getDenyGroups().contains(principal))) {
            return PrincipalType.GROUP;
        } else {
            return PrincipalType.ROLE;
        }
    }

    /**
     * Gets the precedence level for a policy.
     * Lower number = higher precedence.
     *
     * Precedence model:
     * 1. Override DENY (priority=100, deny)
     * 2. Override ALLOW (priority=100, allow)
     * 3. Explicit DENY (priority=0, deny)
     * 4. Normal ALLOW (priority=0, allow)
     * 5. Implicit DENY (no policies)
     */
    private static int getPrecedenceLevel(PolicyTrace policy) {
        if (policy.getPolicyPriority() == 100 && !policy.isAllowPolicy()) {
            return 1; // Override DENY
        } else if (policy.getPolicyPriority() == 100 && policy.isAllowPolicy()) {
            return 2; // Override ALLOW
        } else if (policy.getPolicyPriority() == 0 && !policy.isAllowPolicy()) {
            return 3; // Explicit DENY
        } else {
            return 4; // Normal ALLOW
        }
    }

    /**
     * Generates a human-readable explanation of the final decision.
     */
    private static String explainPrecedenceRule(List<PolicyTrace> policies, AccessDecision finalDecision) {
        if (policies == null || policies.isEmpty()) {
            return "Implicit DENY (no matching policies)";
        }

        PolicyTrace highestPrecedence = policies.get(0);
        String policyName = highestPrecedence.getPolicyName() != null ? highestPrecedence.getPolicyName() : "unknown";
        String enforcer = highestPrecedence.getEnforcer() != null ? highestPrecedence.getEnforcer() : "unknown";

        if (highestPrecedence.getPolicyPriority() == 100 && !highestPrecedence.isAllowPolicy()) {
            return "Override DENY from " + policyName;
        } else if (highestPrecedence.getPolicyPriority() == 100 && highestPrecedence.isAllowPolicy()) {
            return "Override ALLOW from " + policyName;
        } else if (!highestPrecedence.isAllowPolicy()) {
            return "Explicit DENY from " + policyName;
        } else {
            return "Normal ALLOW from " + enforcer + " " + policyName;
        }
    }
}
