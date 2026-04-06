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

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PolicyTrace {
    private String policyId;
    private String policyName;
    private String policyType;      // "ranger", "persona", "purpose"
    private int policyPriority;     // 0=normal, 100=override
    private boolean isAllowPolicy;  // true=allow, false=deny
    private String enforcer;        // "ranger" or "abac"

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
