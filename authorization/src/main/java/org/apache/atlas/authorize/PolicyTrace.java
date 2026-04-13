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
package org.apache.atlas.authorize;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashSet;
import java.util.Set;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PolicyTrace {
    private String policyId;
    private String policyName;
    private String policyType;      // "ranger", "persona", "purpose"
    private int policyPriority;     // 0=normal, 100=override
    private boolean isAllowPolicy;  // true=allow, false=deny
    private String enforcer;        // "ranger" or "abac"

    // Atlas policy details (populated for ABAC persona/purpose policies)
    private String atlasPolicyGuid;        // Original Atlas policy GUID
    private String atlasPolicyName;        // Human-readable policy name from Atlas
    private String atlasPolicyCategory;    // "persona", "purpose", "datamesh"
    private String atlasPolicySubCategory; // "metadata", "data", "glossary", etc.
    private java.util.List<String> atlasPolicyActions;    // Original Atlas actions
    private Object atlasPolicyConditions;  // Original Atlas policy conditions

    // Internal tracking of which principals this policy applies to
    // Not serialized to JSON
    @JsonIgnore
    private Set<String> applicableUsers = new HashSet<>();
    @JsonIgnore
    private Set<String> applicableGroups = new HashSet<>();
    @JsonIgnore
    private Set<String> applicableRoles = new HashSet<>();

    public PolicyTrace() {
    }

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    public String getPolicyName() {
        return policyName;
    }

    public void setPolicyName(String policyName) {
        this.policyName = policyName;
    }

    public String getPolicyType() {
        return policyType;
    }

    public void setPolicyType(String policyType) {
        this.policyType = policyType;
    }

    public int getPolicyPriority() {
        return policyPriority;
    }

    public void setPolicyPriority(int policyPriority) {
        this.policyPriority = policyPriority;
    }

    public boolean isAllowPolicy() {
        return isAllowPolicy;
    }

    public void setAllowPolicy(boolean allowPolicy) {
        isAllowPolicy = allowPolicy;
    }

    public String getEnforcer() {
        return enforcer;
    }

    public void setEnforcer(String enforcer) {
        this.enforcer = enforcer;
    }

    @JsonIgnore
    public Set<String> getApplicableUsers() {
        return applicableUsers;
    }

    @JsonIgnore
    public void setApplicableUsers(Set<String> applicableUsers) {
        this.applicableUsers = applicableUsers;
    }

    @JsonIgnore
    public Set<String> getApplicableGroups() {
        return applicableGroups;
    }

    @JsonIgnore
    public void setApplicableGroups(Set<String> applicableGroups) {
        this.applicableGroups = applicableGroups;
    }

    @JsonIgnore
    public Set<String> getApplicableRoles() {
        return applicableRoles;
    }

    @JsonIgnore
    public void setApplicableRoles(Set<String> applicableRoles) {
        this.applicableRoles = applicableRoles;
    }

    public String getAtlasPolicyGuid() {
        return atlasPolicyGuid;
    }

    public void setAtlasPolicyGuid(String atlasPolicyGuid) {
        this.atlasPolicyGuid = atlasPolicyGuid;
    }

    public String getAtlasPolicyName() {
        return atlasPolicyName;
    }

    public void setAtlasPolicyName(String atlasPolicyName) {
        this.atlasPolicyName = atlasPolicyName;
    }

    public String getAtlasPolicyCategory() {
        return atlasPolicyCategory;
    }

    public void setAtlasPolicyCategory(String atlasPolicyCategory) {
        this.atlasPolicyCategory = atlasPolicyCategory;
    }

    public String getAtlasPolicySubCategory() {
        return atlasPolicySubCategory;
    }

    public void setAtlasPolicySubCategory(String atlasPolicySubCategory) {
        this.atlasPolicySubCategory = atlasPolicySubCategory;
    }

    public java.util.List<String> getAtlasPolicyActions() {
        return atlasPolicyActions;
    }

    public void setAtlasPolicyActions(java.util.List<String> atlasPolicyActions) {
        this.atlasPolicyActions = atlasPolicyActions;
    }

    public Object getAtlasPolicyConditions() {
        return atlasPolicyConditions;
    }

    public void setAtlasPolicyConditions(Object atlasPolicyConditions) {
        this.atlasPolicyConditions = atlasPolicyConditions;
    }

    /**
     * Checks if this policy applies to a specific principal.
     * @param principal The principal name
     * @param principalType The principal type (USER, GROUP, ROLE)
     * @return true if this policy applies to the principal
     */
    @JsonIgnore
    public boolean appliesToPrincipal(String principal, PrincipalType principalType) {
        if (principal == null || principalType == null) {
            return false;
        }

        switch (principalType) {
            case USER:
                return applicableUsers.contains(principal);
            case GROUP:
                return applicableGroups.contains(principal);
            case ROLE:
                return applicableRoles.contains(principal);
            default:
                return false;
        }
    }

    @Override
    public String toString() {
        return "PolicyTrace{" +
                "policyId='" + policyId + '\'' +
                ", policyName='" + policyName + '\'' +
                ", policyType='" + policyType + '\'' +
                ", policyPriority=" + policyPriority +
                ", isAllowPolicy=" + isAllowPolicy +
                ", enforcer='" + enforcer + '\'' +
                '}';
    }
}
