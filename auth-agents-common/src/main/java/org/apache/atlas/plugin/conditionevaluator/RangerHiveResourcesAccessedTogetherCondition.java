/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.plugin.conditionevaluator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.policyengine.RangerAccessRequest;
import org.apache.atlas.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.atlas.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.atlas.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.atlas.plugin.util.RangerAccessRequestUtil;
import org.apache.atlas.plugin.util.RangerRequestedResources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerHiveResourcesAccessedTogetherCondition extends RangerAbstractConditionEvaluator {
	private static final Log LOG = LogFactory.getLog(RangerHiveResourcesAccessedTogetherCondition.class);

	private List<RangerPolicyResourceMatcher> matchers = new ArrayList<>();
	private boolean isInitialized;

	@Override
	public void init() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHiveResourcesAccessedTogetherCondition.init(" + condition + ")");
		}

		super.init();

		if (serviceDef != null) {
			doInitialize();
		} else {
			LOG.error("RangerHiveResourcesAccessedTogetherCondition.init() - ServiceDef not set ... ERROR ..");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHiveResourcesAccessedTogetherCondition.init(" + condition + ")");
		}
	}

	@Override
	public boolean isMatched(final RangerAccessRequest request) {
		boolean ret = true;

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHiveResourcesAccessedTogetherCondition.isMatched(" + request + ")");
		}

		if (isInitialized && CollectionUtils.isNotEmpty(matchers)) {
			RangerRequestedResources resources = RangerAccessRequestUtil.getRequestedResourcesFromContext(request.getContext());

			ret = resources != null && !resources.isMutuallyExcluded(matchers, request.getContext());
		} else {
			LOG.error("RangerHiveResourcesAccessedTogetherCondition.isMatched() - condition is not initialized correctly and will NOT be enforced");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHiveResourcesAccessedTogetherCondition.isMatched(" + request + ")" + ", result=" + ret);
		}

		return ret;
	}

	private void doInitialize() {
		List<String> mutuallyExclusiveResources = condition.getValues();

		if (CollectionUtils.isNotEmpty(mutuallyExclusiveResources)) {
			initializeMatchers(mutuallyExclusiveResources);

			if (CollectionUtils.isEmpty(matchers)) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("RangerHiveResourcesAccessedTogetherCondition.doInitialize() - Cannot create matchers from values in MutualExclustionEnforcer");
				}
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("RangerHiveResourcesAccessedTogetherCondition.doInitialize() - Created " + matchers.size() + " matchers from values in MutualExclustionEnforcer");
				}
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("RangerHiveResourcesAccessedTogetherCondition.doInitialize() - No values in MutualExclustionEnforcer");
			}
		}

		isInitialized = true;
	}

	private void initializeMatchers(List<String> mutuallyExclusiveResources) {

		for (String s : mutuallyExclusiveResources) {

			String policyResourceSpec = s.trim();

			RangerPolicyResourceMatcher matcher = buildMatcher(policyResourceSpec);

			if (matcher != null) {
				matchers.add(matcher);
			}
		}
	}

	private RangerPolicyResourceMatcher buildMatcher(String policyResourceSpec) {

		RangerPolicyResourceMatcher matcher = null;

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHiveResourcesAccessedTogetherCondition.buildMatcher(" + policyResourceSpec + ")");
		}

		// Works only for Hive serviceDef for now
		if (serviceDef != null && EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_HIVE_NAME.equals(serviceDef.getName())) {
			//Parse policyResourceSpec
			char separator = '.';
			String any = "*";

			Map<String, RangerPolicy.RangerPolicyResource> policyResources = new HashMap<>();

			String[] elements = StringUtils.split(policyResourceSpec, separator);

			RangerPolicy.RangerPolicyResource policyResource;

			if (elements.length > 0 && elements.length < 4) {
				if (elements.length == 3) {
					policyResource = new RangerPolicy.RangerPolicyResource(elements[2]);
				} else {
					policyResource = new RangerPolicy.RangerPolicyResource(any);
				}
				policyResources.put("column", policyResource);

				if (elements.length >= 2) {
					policyResource = new RangerPolicy.RangerPolicyResource(elements[1]);
				} else {
					policyResource = new RangerPolicy.RangerPolicyResource(any);
				}
				policyResources.put("table", policyResource);

				policyResource = new RangerPolicy.RangerPolicyResource(elements[0]);
				policyResources.put("database", policyResource);

				matcher = new RangerDefaultPolicyResourceMatcher();
				matcher.setPolicyResources(policyResources);
				matcher.setServiceDef(serviceDef);
				matcher.init();

			} else {
				LOG.error("RangerHiveResourcesAccessedTogetherCondition.buildMatcher() - Incorrect elements in the hierarchy specified ("
						+ elements.length + ")");
			}
		} else {
			LOG.error("RangerHiveResourcesAccessedTogetherCondition.buildMatcher() - ServiceDef not set or ServiceDef is not for Hive");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHiveResourcesAccessedTogetherCondition.buildMatcher(" + policyResourceSpec + ")" + ", matcher=" + matcher);
		}

		return matcher;
	}
}
