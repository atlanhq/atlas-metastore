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

        List<AtlasAccessDecision> decisions = new ArrayList<>();

        // Process each principal type separately to avoid name collisions
        if (response.getUsers() != null) {
            for (String user : response.getUsers()) {
                decisions.add(buildDecisionForPrincipal(user, PrincipalType.USER, false, response, rangerPolicies, abacPolicies));
            }
        }
        if (response.getGroups() != null) {
            for (String group : response.getGroups()) {
                decisions.add(buildDecisionForPrincipal(group, PrincipalType.GROUP, false, response, rangerPolicies, abacPolicies));
            }
        }
        if (response.getRoles() != null) {
            for (String role : response.getRoles()) {
                decisions.add(buildDecisionForPrincipal(role, PrincipalType.ROLE, false, response, rangerPolicies, abacPolicies));
            }
        }
        if (response.getDenyUsers() != null) {
            for (String user : response.getDenyUsers()) {
                decisions.add(buildDecisionForPrincipal(user, PrincipalType.USER, true, response, rangerPolicies, abacPolicies));
            }
        }
        if (response.getDenyGroups() != null) {
            for (String group : response.getDenyGroups()) {
                decisions.add(buildDecisionForPrincipal(group, PrincipalType.GROUP, true, response, rangerPolicies, abacPolicies));
            }
        }
        if (response.getDenyRoles() != null) {
            for (String role : response.getDenyRoles()) {
                decisions.add(buildDecisionForPrincipal(role, PrincipalType.ROLE, true, response, rangerPolicies, abacPolicies));
            }
        }

        return decisions;
    }

    /**
     * Builds an access decision for a single principal with known type and decision.
     * @param principal The principal name
     * @param principalType The type (USER, GROUP, or ROLE)
     * @param isDenied True if this principal is denied access
     * @param response The accessor response
     * @param rangerPolicies Map of Ranger policy traces
     * @param abacPolicies Map of ABAC policy traces
     */
    private static AtlasAccessDecision buildDecisionForPrincipal(
        String principal,
        PrincipalType principalType,
        boolean isDenied,
        AtlasAccessorResponse response,
        Map<String, PolicyTrace> rangerPolicies,
        Map<String, PolicyTrace> abacPolicies
    ) {
        AtlasAccessDecision decision = new AtlasAccessDecision();
        decision.setPrincipal(principal);
        decision.setPrincipalType(principalType);

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

        AccessDecision finalDecision = isDenied ? AccessDecision.DENY : AccessDecision.ALLOW;
        decision.setDecision(finalDecision);
        decision.setFinalReason(explainPrecedenceRule(allPolicies, finalDecision));

        return decision;
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
     * @param policies List of policies sorted by precedence
     * @param finalDecision The actual final decision (ALLOW or DENY)
     */
    private static String explainPrecedenceRule(List<PolicyTrace> policies, AccessDecision finalDecision) {
        if (policies == null || policies.isEmpty()) {
            if (finalDecision == AccessDecision.DENY) {
                return "Implicit DENY (no matching policies)";
            } else {
                return "ALLOW (no deny policies matched)";
            }
        }

        PolicyTrace highestPrecedence = policies.get(0);
        String policyName = highestPrecedence.getPolicyName() != null ? highestPrecedence.getPolicyName() : "unknown";
        String enforcer = highestPrecedence.getEnforcer() != null ? highestPrecedence.getEnforcer() : "unknown";

        // Use the actual final decision to determine the explanation
        if (finalDecision == AccessDecision.DENY) {
            // Find the highest precedence deny policy
            for (PolicyTrace policy : policies) {
                if (!policy.isAllowPolicy()) {
                    String denyPolicyName = policy.getPolicyName() != null ? policy.getPolicyName() : "unknown";
                    if (policy.getPolicyPriority() == 100) {
                        return "Override DENY from " + denyPolicyName;
                    } else {
                        return "Explicit DENY from " + denyPolicyName;
                    }
                }
            }
            return "DENY (policy evaluation)";
        } else {
            // ALLOW decision
            if (highestPrecedence.getPolicyPriority() == 100 && highestPrecedence.isAllowPolicy()) {
                return "Override ALLOW from " + policyName;
            } else if (highestPrecedence.isAllowPolicy()) {
                return "Normal ALLOW from " + enforcer + " " + policyName;
            } else {
                return "ALLOW (no deny policies matched)";
            }
        }
    }
}
