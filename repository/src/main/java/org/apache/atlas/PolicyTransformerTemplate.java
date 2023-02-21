/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas;

import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PolicyTransformerTemplate {
    private static final Logger LOG = LoggerFactory.getLogger(PolicyTransformerTemplate.class);

    private Map<String, List<TemplatePolicy>> actionToPoliciesMap = new HashMap<>();

    public PolicyTransformerTemplate() {
    }

    public List<TemplatePolicy> getTemplate(String action) {
        return actionToPoliciesMap.get(action);
    }

    public void fromJsonString(String json) {

        Map<String, List<Map>> templates = AtlasType.fromJson(json, Map.class);

        for (String customAction : templates.keySet()) {
            List<Map> templatePolicies = templates.get(customAction);
            List<TemplatePolicy> policies = new ArrayList<>();

            for (Map policy: templatePolicies) {
                TemplatePolicy templatePolicy = new TemplatePolicy();

                templatePolicy.setActions((List<String>) policy.get("actions"));
                templatePolicy.setResources((List<String>) policy.get("resources"));
                templatePolicy.setCategory((String) policy.get("category"));

                policies.add(templatePolicy);
            }

            this.actionToPoliciesMap.put(customAction, policies);
        }
    }

    class TemplatePolicy {
        private String category;
        private List<String> resources;
        private List<String> actions;

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public List<String> getResources() {
            return resources;
        }

        public void setResources(List<String> resources) {
            this.resources = resources;
        }

        public List<String> getActions() {
            return actions;
        }

        public void setActions(List<String> actions) {
            this.actions = actions;
        }
    }
}
