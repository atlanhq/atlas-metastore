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

package org.apache.atlas.plugin.util;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.authorizer.store.PoliciesStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.model.RangerPolicyDelta;
import org.apache.atlas.plugin.model.RangerServiceDef;
import org.apache.atlas.plugin.policyengine.RangerPolicyEngine;
import org.apache.atlas.plugin.policyengine.RangerPolicyEngineImpl;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ServicePolicies implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(ServicePolicies.class);
	private static final PoliciesStore policiesStore = PoliciesStore.getInstance();

	private String             serviceName;
	private String             serviceId;
	private Long               policyVersion;
	private Date               policyUpdateTime;
	private List<RangerPolicy> policies;
	private RangerServiceDef   serviceDef;
	private String             auditMode = RangerPolicyEngine.AUDIT_DEFAULT;
	private TagPolicies        tagPolicies;
	private ABACPolicies abacPolicies;
	private Map<String, SecurityZoneInfo> securityZones;
	private List<RangerPolicyDelta> policyDeltas;
	private Map<String, String> serviceConfig;

	/**
	 * @return the serviceName
	 */
	public String getServiceName() {
		return serviceName;
	}
	/**
	 * @param serviceName the serviceName to set
	 */
	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}
	/**
	 * @return the serviceId
	 */
	public String getServiceId() {
		return serviceId;
	}
	/**
	 * @param serviceId the serviceId to set
	 */
	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}
	/**
	 * @return the policyVersion
	 */
	public Long getPolicyVersion() {
		return policyVersion;
	}
	/**
	 * @param policyVersion the policyVersion to set
	 */
	public void setPolicyVersion(Long policyVersion) {
		this.policyVersion = policyVersion;
	}
	/**
	 * @return the policyUpdateTime
	 */
	public Date getPolicyUpdateTime() {
		return policyUpdateTime;
	}
	/**
	 * @param policyUpdateTime the policyUpdateTime to set
	 */
	public void setPolicyUpdateTime(Date policyUpdateTime) {
		this.policyUpdateTime = policyUpdateTime;
	}

	public Map<String, String> getServiceConfig() {
		return serviceConfig;
	}
	public void setServiceConfig(Map<String, String> serviceConfig) {
		this.serviceConfig = serviceConfig;
	}

	/**
	 * @return the policies
	 */
	public List<RangerPolicy> getPolicies() {
		return policies;
	}
	/**
	 * @param policies the policies to set
	 */
	public void setPolicies(List<RangerPolicy> policies) {
		this.policies = policies;
	}
	/**
	 * @return the serviceDef
	 */
	public RangerServiceDef getServiceDef() {
		return serviceDef;
	}
	/**
	 * @param serviceDef the serviceDef to set
	 */
	public void setServiceDef(RangerServiceDef serviceDef) {
		this.serviceDef = serviceDef;
	}

	public String getAuditMode() {
		return auditMode;
	}

	public void setAuditMode(String auditMode) {
		this.auditMode = auditMode;
	}
	/**
	 * @return the tagPolicies
	 */
	public TagPolicies getTagPolicies() {
		return tagPolicies;
	}
	/**
	 * @param tagPolicies the tagPolicies to set
	 */
	public void setTagPolicies(TagPolicies tagPolicies) {
		this.tagPolicies = tagPolicies;
	}

	public ABACPolicies getAbacPolicies() {
		return abacPolicies;
	}

	public void setAbacPolicies(ABACPolicies abacPolicies) {
		this.abacPolicies = abacPolicies;
	}

	public Map<String, SecurityZoneInfo> getSecurityZones() { return securityZones; }

	public void setSecurityZones(Map<String, SecurityZoneInfo> securityZones) {
		this.securityZones = securityZones;
	}

	@Override
	public String toString() {
		return "serviceName=" + serviceName + ", "
				+ "serviceId=" + serviceId + ", "
			 	+ "policyVersion=" + policyVersion + ", "
			 	+ "policyUpdateTime=" + policyUpdateTime + ", "
			 	+ "policies=" + policies + ", "
			 	+ "tagPolicies=" + tagPolicies + ", "
			 	+ "policyDeltas=" + policyDeltas + ", "
			 	+ "serviceDef=" + serviceDef + ", "
			 	+ "auditMode=" + auditMode + ", "
				+ "securityZones=" + securityZones
				;
	}

	public List<RangerPolicyDelta> getPolicyDeltas() { return this.policyDeltas; }

	public void setPolicyDeltas(List<RangerPolicyDelta> policyDeltas) { this.policyDeltas = policyDeltas; }

	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
	@XmlRootElement
	@XmlAccessorType(XmlAccessType.FIELD)
	public static class TagPolicies implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private String             serviceName;
		private String             serviceId;
		private Long               policyVersion;
		private Date               policyUpdateTime;
		private List<RangerPolicy> policies;
		private RangerServiceDef   serviceDef;
		private String             auditMode = RangerPolicyEngine.AUDIT_DEFAULT;
		private Map<String, String> serviceConfig;

		/**
		 * @return the serviceName
		 */
		public String getServiceName() {
			return serviceName;
		}
		/**
		 * @param serviceName the serviceName to set
		 */
		public void setServiceName(String serviceName) {
			this.serviceName = serviceName;
		}
		/**
		 * @return the serviceId
		 */
		public String getServiceId() {
			return serviceId;
		}
		/**
		 * @param serviceId the serviceId to set
		 */
		public void setServiceId(String serviceId) {
			this.serviceId = serviceId;
		}
		/**
		 * @return the policyVersion
		 */
		public Long getPolicyVersion() {
			return policyVersion;
		}
		/**
		 * @param policyVersion the policyVersion to set
		 */
		public void setPolicyVersion(Long policyVersion) {
			this.policyVersion = policyVersion;
		}
		/**
		 * @return the policyUpdateTime
		 */
		public Date getPolicyUpdateTime() {
			return policyUpdateTime;
		}
		/**
		 * @param policyUpdateTime the policyUpdateTime to set
		 */
		public void setPolicyUpdateTime(Date policyUpdateTime) {
			this.policyUpdateTime = policyUpdateTime;
		}
		/**
		 * @return the policies
		 */
		public List<RangerPolicy> getPolicies() {
			return policies;
		}
		/**
		 * @param policies the policies to set
		 */
		public void setPolicies(List<RangerPolicy> policies) {
			this.policies = policies;
		}
		/**
		 * @return the serviceDef
		 */
		public RangerServiceDef getServiceDef() {
			return serviceDef;
		}
		/**
		 * @param serviceDef the serviceDef to set
		 */
		public void setServiceDef(RangerServiceDef serviceDef) {
			this.serviceDef = serviceDef;
		}

		public String getAuditMode() {
			return auditMode;
		}

		public void setAuditMode(String auditMode) {
			this.auditMode = auditMode;
		}

		public Map<String, String> getServiceConfig() {
			return serviceConfig;
		}

		public void setServiceConfig(Map<String, String> serviceConfig) {
			this.serviceConfig = serviceConfig;
		}

		@Override
		public String toString() {
			return "serviceName=" + serviceName + ", "
					+ "serviceId=" + serviceId + ", "
					+ "policyVersion=" + policyVersion + ", "
					+ "policyUpdateTime=" + policyUpdateTime + ", "
					+ "policies=" + policies + ", "
					+ "serviceDef=" + serviceDef + ", "
					+ "auditMode=" + auditMode
					+ "serviceConfig=" + serviceConfig
					;
		}
	}

	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
	@XmlRootElement
	@XmlAccessorType(XmlAccessType.FIELD)
	public static class ABACPolicies implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private String serviceName;
		private String serviceId;
		private Long policyVersion;
		private Date policyUpdateTime;
		private List<RangerPolicy> policies;
		private RangerServiceDef serviceDef;
		private String auditMode = RangerPolicyEngine.AUDIT_DEFAULT;
		private Map<String, String> serviceConfig;

		public ABACPolicies(String abacServiceName, String serviceId) {
			this.setServiceName(abacServiceName);
			this.setPolicyUpdateTime(new Date());
			this.setPolicyVersion(-1L);
			this.setServiceId(serviceId);
		}

		public ABACPolicies() {}

		public String getServiceName() {
			return serviceName;
		}

		public void setServiceName(String serviceName) {
			this.serviceName = serviceName;
		}

		public String getServiceId() {
			return serviceId;
		}

		public void setServiceId(String serviceId) {
			this.serviceId = serviceId;
		}

		public Long getPolicyVersion() {
			return policyVersion;
		}

		public void setPolicyVersion(Long policyVersion) {
			this.policyVersion = policyVersion;
		}

		public Date getPolicyUpdateTime() {
			return policyUpdateTime;
		}

		public void setPolicyUpdateTime(Date policyUpdateTime) {
			this.policyUpdateTime = policyUpdateTime;
		}

		public List<RangerPolicy> getPolicies() {
			return policies == null ? new ArrayList<>() : policies;
		}

		public void setPolicies(List<RangerPolicy> policies) {
			this.policies = policies;
		}

		public RangerServiceDef getServiceDef() {
			return serviceDef;
		}

		public void setServiceDef(RangerServiceDef serviceDef) {
			this.serviceDef = serviceDef;
		}

		public String getAuditMode() {
			return auditMode;
		}

		public void setAuditMode(String auditMode) {
			this.auditMode = auditMode;
		}

		public Map<String, String> getServiceConfig() {
			return serviceConfig;
		}

		public void setServiceConfig(Map<String, String> serviceConfig) {
			this.serviceConfig = serviceConfig;
		}

		@Override
		public String toString() {
			return "serviceName=" + serviceName + ", "
					+ "serviceId=" + serviceId + ", "
					+ "policyVersion=" + policyVersion + ", "
					+ "policyUpdateTime=" + policyUpdateTime + ", "
					+ "policies=" + policies + ", "
					+ "serviceDef=" + serviceDef + ", "
					+ "auditMode=" + auditMode
					+ "serviceConfig=" + serviceConfig
					;
		}
	}

	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
	@XmlRootElement
	@XmlAccessorType(XmlAccessType.FIELD)
	public static class SecurityZoneInfo implements java.io.Serializable {
		private static final long serialVersionUID = 1L;
		private String                          zoneName;
		private List<HashMap<String, List<String>>> resources;
		private List<RangerPolicy>              policies;
		private List<RangerPolicyDelta>         policyDeltas;
		private Boolean                         containsAssociatedTagService;

		public String getZoneName() {
			return zoneName;
		}

		public List<HashMap<String, List<String>>> getResources() {
			return resources;
		}

		public List<RangerPolicy> getPolicies() {
			return policies;
		}

		public List<RangerPolicyDelta> getPolicyDeltas() { return policyDeltas; }

		public Boolean getContainsAssociatedTagService() { return containsAssociatedTagService; }

		public void setZoneName(String zoneName) {
			this.zoneName = zoneName;
		}

		public void setResources(List<HashMap<String, List<String>>> resources) {
			this.resources = resources;
		}

		public void setPolicies(List<RangerPolicy> policies) {
			this.policies = policies;
		}

		public void setPolicyDeltas(List<RangerPolicyDelta> policyDeltas) { this.policyDeltas = policyDeltas; }

		public void setContainsAssociatedTagService(Boolean containsAssociatedTagService) { this.containsAssociatedTagService = containsAssociatedTagService; }

		@Override
		public String toString() {
			return "zoneName=" + zoneName + ", "
					+ "resources=" + resources + ", "
					+ "policies=" + policies + ", "
					+ "policyDeltas=" + policyDeltas + ", "
					+ "containsAssociatedTagService=" + containsAssociatedTagService
					;
		}
	}

	static public ServicePolicies copyHeader(ServicePolicies source) {
		ServicePolicies ret = new ServicePolicies();

		ret.setServiceName(source.getServiceName());
		ret.setServiceId(source.getServiceId());
		ret.setPolicyVersion(source.getPolicyVersion());
		ret.setAuditMode(source.getAuditMode());
		ret.setServiceDef(source.getServiceDef());
		ret.setPolicyUpdateTime(source.getPolicyUpdateTime());
		ret.setSecurityZones(source.getSecurityZones());
		ret.setPolicies(Collections.emptyList());
		ret.setPolicyDeltas(null);
		if (source.getTagPolicies() != null) {
			TagPolicies tagPolicies = copyHeader(source.getTagPolicies(), source.getServiceDef().getName());
			ret.setTagPolicies(tagPolicies);
		}

		if (source.getAbacPolicies() != null) {
			ABACPolicies abacPolicies = copyHeader(source.getAbacPolicies(), null);
			ret.setAbacPolicies(abacPolicies);
		}

		return ret;
	}

	static public ABACPolicies copyHeader(ABACPolicies source, String componentServiceName) {
		ABACPolicies ret = new ABACPolicies();

		ret.setServiceName(source.getServiceName());
		ret.setServiceId(source.getServiceId());
		ret.setPolicyVersion(source.getPolicyVersion());
		ret.setAuditMode(source.getAuditMode());
		ret.setPolicyUpdateTime(source.getPolicyUpdateTime());
		ret.setPolicies(new ArrayList<>());

		if (componentServiceName != null) {
			ret.setServiceDef(ServiceDefUtil.normalizeAccessTypeDefs(source.getServiceDef(), componentServiceName));
		}

		return ret;
	}

	static public TagPolicies copyHeader(TagPolicies source, String componentServiceName) {
		TagPolicies ret = new TagPolicies();

		ret.setServiceName(source.getServiceName());
		ret.setServiceId(source.getServiceId());
		ret.setPolicyVersion(source.getPolicyVersion());
		ret.setAuditMode(source.getAuditMode());
		ret.setServiceDef(ServiceDefUtil.normalizeAccessTypeDefs(source.getServiceDef(), componentServiceName));
		ret.setPolicyUpdateTime(source.getPolicyUpdateTime());
		ret.setPolicies(Collections.emptyList());

		return ret;
	}

	private static Map<String, RangerPolicyDelta> fetchDeletedDeltaMap(List<RangerPolicyDelta> deltas) {
		Map<String, RangerPolicyDelta> ret = new HashMap<>();
		for (RangerPolicyDelta delta : deltas) {
			if (delta.getChangeType() == RangerPolicyDelta.CHANGE_TYPE_POLICY_DELETE || delta.getChangeType() == RangerPolicyDelta.CHANGE_TYPE_POLICY_UPDATE) {
				ret.put(delta.getPolicyAtlasGuid(), delta);
			}
		}
		return ret;
	}

	public static ServicePolicies applyDelta(final ServicePolicies servicePolicies, RangerPolicyEngineImpl policyEngine) {
		ServicePolicies ret = copyHeader(servicePolicies);

		List<RangerPolicy> oldResourcePolicies = policyEngine.getResourcePolicies();
		List<RangerPolicy> oldTagPolicies      = policyEngine.getTagPolicies();
		Map<String, RangerPolicyDelta> deletedDeltaMap = fetchDeletedDeltaMap(servicePolicies.getPolicyDeltas());

		List<RangerPolicy> resourcePoliciesAfterDelete =
				RangerPolicyDeltaUtil.deletePoliciesByDelta(oldResourcePolicies, deletedDeltaMap);
		List<RangerPolicy> newResourcePolicies =
				RangerPolicyDeltaUtil.applyDeltas(resourcePoliciesAfterDelete, servicePolicies.getPolicyDeltas(), servicePolicies.getServiceDef().getName(), servicePolicies.getServiceName());

		ret.setPolicies(newResourcePolicies);

		List<RangerPolicy> newTagPolicies;
		if (servicePolicies.getTagPolicies() != null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("applyingDeltas for tag policies");
			}
			List<RangerPolicy> tagPoliciesAfterDelete =
					RangerPolicyDeltaUtil.deletePoliciesByDelta(oldTagPolicies, deletedDeltaMap);
			newTagPolicies = RangerPolicyDeltaUtil.applyDeltas(tagPoliciesAfterDelete, servicePolicies.getPolicyDeltas(), servicePolicies.getTagPolicies().getServiceDef().getName(), servicePolicies.getTagPolicies().getServiceName());
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No need to apply deltas for tag policies");
			}
			newTagPolicies = oldTagPolicies;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("New tag policies: " + newTagPolicies);
		}

		if (ret.getTagPolicies() != null) {
			ret.getTagPolicies().setPolicies(newTagPolicies);
		}

		if (servicePolicies.getAbacPolicies() != null ) {
			List<RangerPolicy> oldAbacPolicies = policiesStore.getAbacPolicies() != null ? policiesStore.getAbacPolicies() : new ArrayList<>();;
			List<RangerPolicy> abacPoliciesAfterDelete =
				RangerPolicyDeltaUtil.deletePoliciesByDelta(oldAbacPolicies, deletedDeltaMap);
			List<RangerPolicy> newAbacPolicies =
					RangerPolicyDeltaUtil.applyDeltas(abacPoliciesAfterDelete, servicePolicies.getPolicyDeltas(), servicePolicies.getAbacPolicies().getServiceName(), servicePolicies.getAbacPolicies().getServiceName());
			ret.getAbacPolicies().setPolicies(newAbacPolicies);
		}

		return ret;
	}
}
