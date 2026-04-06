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

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AtlasAccessDecision {
    private String principal;
    private PrincipalType principalType;
    private AccessDecision decision;
    private List<PolicyTrace> policies;
    private String finalReason;

    public AtlasAccessDecision() {
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public PrincipalType getPrincipalType() {
        return principalType;
    }

    public void setPrincipalType(PrincipalType principalType) {
        this.principalType = principalType;
    }

    public AccessDecision getDecision() {
        return decision;
    }

    public void setDecision(AccessDecision decision) {
        this.decision = decision;
    }

    public List<PolicyTrace> getPolicies() {
        return policies;
    }

    public void setPolicies(List<PolicyTrace> policies) {
        this.policies = policies;
    }

    public String getFinalReason() {
        return finalReason;
    }

    public void setFinalReason(String finalReason) {
        this.finalReason = finalReason;
    }

    @Override
    public String toString() {
        return "AtlasAccessDecision{" +
                "principal='" + principal + '\'' +
                ", principalType=" + principalType +
                ", decision=" + decision +
                ", policies=" + policies +
                ", finalReason='" + finalReason + '\'' +
                '}';
    }
}
