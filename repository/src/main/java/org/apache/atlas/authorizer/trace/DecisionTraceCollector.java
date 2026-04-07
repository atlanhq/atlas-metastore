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

import org.apache.atlas.authorize.PolicyTrace;
import org.apache.atlas.plugin.model.RangerPolicy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collects policy match information during authorization evaluation.
 * Uses caching to minimize overhead and ensure <20% performance impact.
 */
public class DecisionTraceCollector {
    // Caches to prevent duplicate policy object creation
    private final Map<String, PolicyTrace> rangerPolicyCache = new ConcurrentHashMap<>();
    private final Map<String, PolicyTrace> abacPolicyCache = new ConcurrentHashMap<>();

    /**
     * Records a Ranger policy match from RangerAccessResult.
     * Used when we don't have access to the full RangerPolicy object.
     * @param policyId The policy ID
     * @param policyPriority The policy priority
     * @param isAllow True if this is an allow policy, false if deny
     * @param users List of users this policy applies to
     * @param groups List of groups this policy applies to
     * @param roles List of roles this policy applies to
     */
    public void recordRangerMatchFromResult(String policyId, int policyPriority, boolean isAllow,
                                           java.util.List<String> users,
                                           java.util.List<String> groups,
                                           java.util.List<String> roles) {
        if (policyId == null || policyId.equals("-1")) {
            return;
        }

        // Use cache to avoid duplicate processing, but merge principals if already exists
        PolicyTrace trace = rangerPolicyCache.get(policyId);
        if (trace == null) {
            trace = createRangerPolicyTraceFromResult(policyId, policyPriority, isAllow);
            rangerPolicyCache.put(policyId, trace);
        }

        // Add principals from this policy item
        if (users != null) {
            trace.getApplicableUsers().addAll(users);
        }
        if (groups != null) {
            trace.getApplicableGroups().addAll(groups);
        }
        if (roles != null) {
            trace.getApplicableRoles().addAll(roles);
        }
    }

    /**
     * Records an ABAC policy match (persona or purpose).
     * @param policy The matched ABAC policy
     * @param isAllow True if this is an allow policy, false if deny
     */
    public void recordAbacMatch(RangerPolicy policy, boolean isAllow) {
        if (policy == null) {
            return;
        }

        String policyId = policy.getGuid();
        if (policyId == null) {
            return;
        }

        // Use cache to avoid duplicate processing, but merge principals if already exists
        PolicyTrace trace = abacPolicyCache.get(policyId);
        if (trace == null) {
            trace = createAbacPolicyTrace(policy, isAllow);
            abacPolicyCache.put(policyId, trace);
        }

        // Extract principals from policy items
        java.util.List<RangerPolicy.RangerPolicyItem> policyItems = isAllow ? policy.getPolicyItems() : policy.getDenyPolicyItems();
        if (policyItems != null) {
            for (RangerPolicy.RangerPolicyItem item : policyItems) {
                if (item.getUsers() != null) {
                    trace.getApplicableUsers().addAll(item.getUsers());
                }
                if (item.getGroups() != null) {
                    trace.getApplicableGroups().addAll(item.getGroups());
                }
                if (item.getRoles() != null) {
                    trace.getApplicableRoles().addAll(item.getRoles());
                }
            }
        }
    }

    /**
     * Gets all recorded Ranger policies.
     * @return Map of policy ID to PolicyTrace
     */
    public Map<String, PolicyTrace> getRangerPolicies() {
        return rangerPolicyCache;
    }

    /**
     * Gets all recorded ABAC policies.
     * @return Map of policy ID to PolicyTrace
     */
    public Map<String, PolicyTrace> getAbacPolicies() {
        return abacPolicyCache;
    }

    /**
     * Creates a PolicyTrace from RangerAccessResult information.
     * Used when we don't have access to the full RangerPolicy object.
     */
    private PolicyTrace createRangerPolicyTraceFromResult(String policyId, int policyPriority, boolean isAllow) {
        PolicyTrace trace = new PolicyTrace();
        trace.setPolicyId(policyId);
        trace.setPolicyName("ranger-policy-" + policyId);  // Use ID as name since we don't have the actual name
        trace.setPolicyType("ranger");
        trace.setPolicyPriority(policyPriority);
        trace.setAllowPolicy(isAllow);
        trace.setEnforcer("ranger");

        return trace;
    }

    /**
     * Creates a PolicyTrace from an ABAC policy.
     */
    private PolicyTrace createAbacPolicyTrace(RangerPolicy policy, boolean isAllow) {
        PolicyTrace trace = new PolicyTrace();
        trace.setPolicyId(policy.getGuid());
        trace.setPolicyName(policy.getName());

        // Infer policy type from service type or policy name
        String policyName = policy.getName();
        String serviceType = policy.getServiceType();

        // Try to use service type first (more reliable)
        if (serviceType != null && serviceType.contains("persona")) {
            trace.setPolicyType("persona");
        } else if (serviceType != null && serviceType.contains("purpose")) {
            trace.setPolicyType("purpose");
        } else if (policyName != null) {
            // Fall back to name-based inference
            if (policyName.contains("persona")) {
                trace.setPolicyType("persona");
            } else if (policyName.contains("purpose")) {
                trace.setPolicyType("purpose");
            } else {
                trace.setPolicyType("abac");
            }
        } else {
            trace.setPolicyType("abac");
        }

        trace.setPolicyPriority(policy.getPolicyPriority() != null ? policy.getPolicyPriority() : 0);
        trace.setAllowPolicy(isAllow);
        trace.setEnforcer("abac");

        return trace;
    }
}
