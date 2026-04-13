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
import org.apache.atlas.authorizer.store.UsersStore;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.apache.atlas.plugin.util.RangerRoles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Builds access decision traces from collected policy information.
 * Applies 5-level precedence model to determine final decisions.
 */
public class DecisionTraceBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(DecisionTraceBuilder.class);

    /**
     * Builds access decisions for a specific user showing how they get access.
     *
     * SECURITY: This method REQUIRES forUser parameter to prevent unauthorized enumeration
     * of all principals with access to an asset. Returns empty list if forUser is not provided.
     *
     * @param response The accessor response with users/groups/roles
     * @param rangerPolicies Map of Ranger policy traces
     * @param abacPolicies Map of ABAC policy traces
     * @param forUser REQUIRED username to trace access decisions for
     * @return List of access decisions explaining how the user gets access (direct, via groups, via roles)
     *         Returns empty list if forUser is null/empty
     */
    public static List<AtlasAccessDecision> buildDecisions(
        AtlasAccessorResponse response,
        Map<String, PolicyTrace> rangerPolicies,
        Map<String, PolicyTrace> abacPolicies,
        String forUser
    ) {
        if (response == null) {
            return Collections.emptyList();
        }

        List<AtlasAccessDecision> decisions = new ArrayList<>();

        // This prevents unauthorized users from enumerating all principals with access
        if (forUser == null || forUser.isEmpty()) {
            LOG.warn("Decision trace requested without forUser parameter - access denied for security reasons");
            return Collections.emptyList();
        }

        boolean hasAnyAccess = false;

        // Resolve user's groups and roles
        ResolvedPrincipals resolved = resolveUserPrincipals(forUser);

        // 1. Check direct user access
        if (response.getUsers() != null && response.getUsers().contains(forUser)) {
            decisions.add(buildDecisionForPrincipal(forUser, PrincipalType.USER, false, response, rangerPolicies, abacPolicies));
            hasAnyAccess = true;
        } else if (response.getDenyUsers() != null && response.getDenyUsers().contains(forUser)) {
            decisions.add(buildDecisionForPrincipal(forUser, PrincipalType.USER, true, response, rangerPolicies, abacPolicies));
            hasAnyAccess = true;
        }

        // 2. Check group-based access
        for (String group : resolved.groups) {
            if (response.getGroups() != null && response.getGroups().contains(group)) {
                decisions.add(buildDecisionForPrincipal(group, PrincipalType.GROUP, false, response, rangerPolicies, abacPolicies));
                hasAnyAccess = true;
            } else if (response.getDenyGroups() != null && response.getDenyGroups().contains(group)) {
                decisions.add(buildDecisionForPrincipal(group, PrincipalType.GROUP, true, response, rangerPolicies, abacPolicies));
                hasAnyAccess = true;
            }
        }

        // 3. Check role-based access (includes persona_* and connection_admins_*)
        for (String role : resolved.roles) {
            if (response.getRoles() != null && response.getRoles().contains(role)) {
                decisions.add(buildDecisionForPrincipal(role, PrincipalType.ROLE, false, response, rangerPolicies, abacPolicies));
                hasAnyAccess = true;
            } else if (response.getDenyRoles() != null && response.getDenyRoles().contains(role)) {
                decisions.add(buildDecisionForPrincipal(role, PrincipalType.ROLE, true, response, rangerPolicies, abacPolicies));
                hasAnyAccess = true;
            }
        }

        // 4. If no access found, return implicit deny
        if (!hasAnyAccess) {
            decisions.add(buildImplicitDenyDecision(forUser, PrincipalType.USER));
        }

        return decisions;
    }

    /**
     * Resolves a user's complete set of groups and roles.
     * @param username The username to resolve
     * @return ResolvedPrincipals containing groups and roles
     */
    private static ResolvedPrincipals resolveUserPrincipals(String username) {
        ResolvedPrincipals result = new ResolvedPrincipals();

        try {
            UsersStore usersStore = UsersStore.getInstance();
            if (usersStore == null) {
                LOG.warn("UsersStore not available, cannot resolve groups/roles for user: {}", username);
                return result;
            }

            // Get user's groups
            RangerUserStore userStore = usersStore.getUserStore();
            List<String> groups = usersStore.getGroupsForUser(username, userStore);
            result.groups = groups != null ? groups : Collections.emptyList();

            // Get user's roles (direct + from groups)
            RangerRoles allRoles = usersStore.getAllRoles();
            List<String> roles = usersStore.getRolesForUser(username, groups, allRoles);
            if (roles == null) {
                roles = new ArrayList<>();
            }

            // Get nested/inherited roles
            List<String> nestedRoles = usersStore.getNestedRolesForUser(roles, allRoles);
            if (nestedRoles != null) {
                roles.addAll(nestedRoles);
            }

            result.roles = roles;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Resolved principals for user '{}': groups={}, roles={}", username, result.groups, result.roles);
            }

        } catch (Exception e) {
            LOG.error("Error resolving principals for user: {}", username, e);
        }

        return result;
    }

    /**
     * Helper class to hold resolved principals for a user.
     */
    private static class ResolvedPrincipals {
        List<String> groups = Collections.emptyList();
        List<String> roles = Collections.emptyList();
    }

    /**
     * Builds an implicit deny decision for a principal not found in access lists.
     * @param principal The principal name
     * @param principalType The principal type
     * @return An access decision with implicit deny
     */
    private static AtlasAccessDecision buildImplicitDenyDecision(String principal, PrincipalType principalType) {
        AtlasAccessDecision decision = new AtlasAccessDecision();
        decision.setPrincipal(principal);
        decision.setPrincipalType(principalType);
        decision.setDecision(AccessDecision.DENY);
        decision.setPolicies(Collections.emptyList());
        decision.setFinalReason("Implicit DENY (no matching policies grant access to this principal)");
        return decision;
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

        // Collect only policies that apply to THIS specific principal
        List<PolicyTrace> applicablePolicies = new ArrayList<>();

        if (rangerPolicies != null) {
            for (PolicyTrace policy : rangerPolicies.values()) {
                if (policy.appliesToPrincipal(principal, principalType)) {
                    applicablePolicies.add(policy);
                }
            }
        }

        if (abacPolicies != null) {
            for (PolicyTrace policy : abacPolicies.values()) {
                if (policy.appliesToPrincipal(principal, principalType)) {
                    applicablePolicies.add(policy);
                }
            }
        }

        // Sort by precedence: override deny > override allow > explicit deny > allow
        applicablePolicies.sort((p1, p2) -> {
            int level1 = getPrecedenceLevel(p1);
            int level2 = getPrecedenceLevel(p2);
            return Integer.compare(level1, level2);
        });

        decision.setPolicies(applicablePolicies);

        AccessDecision finalDecision = isDenied ? AccessDecision.DENY : AccessDecision.ALLOW;
        decision.setDecision(finalDecision);
        decision.setFinalReason(explainPrecedenceRule(applicablePolicies, finalDecision));

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
