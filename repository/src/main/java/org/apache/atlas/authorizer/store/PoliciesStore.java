package org.apache.atlas.authorizer.store;

import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.authorizers.AuthorizerCommon;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.atlas.authorizer.AuthorizerUtils.POLICY_TYPE_ALLOW;
import static org.apache.atlas.authorizer.AuthorizerUtils.POLICY_TYPE_DENY;

public class PoliciesStore {

    private static final Logger LOG = LoggerFactory.getLogger(PoliciesStore.class);

    private static List<RangerPolicy> resourcePolicies;
    private static List<RangerPolicy> tagPolicies;
    private static List<RangerPolicy> abacPolicies;

    private static PoliciesStore policiesStore;

    public static PoliciesStore getInstance() {
        synchronized (PoliciesStore.class) {
            if (policiesStore == null) {
                policiesStore = new PoliciesStore();
            }
            return policiesStore;
        }
    }

    public void setResourcePolicies(List<RangerPolicy> resourcePolicies) {
        this.resourcePolicies = resourcePolicies;
    }

    private static List<RangerPolicy> getResourcePolicies() {
        return resourcePolicies;
    }

    public void setTagPolicies(List<RangerPolicy> tagPolicies) {
        this.tagPolicies = tagPolicies;
    }

    private static List<RangerPolicy> getTagPolicies() {
        return tagPolicies;
    }

    public void setAbacPolicies(List<RangerPolicy> abacPolicies) {
        this.abacPolicies = abacPolicies;
    }

    private static List<RangerPolicy> getAbacPolicies() {
        return abacPolicies;
    }

    public static List<RangerPolicy> getRelevantPolicies(String persona, String purpose, String serviceName, List<String> actions, String policyType) {
        return getRelevantPolicies(null, null, serviceName, actions, policyType, false);
    }

    public static List<RangerPolicy> getRelevantPolicies(String persona, String purpose, String serviceName, List<String> actions, String policyType, boolean ignoreUser) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getRelevantPolicies");
        String policyQualifiedNamePrefix = null;
        if (persona != null && !persona.isEmpty()) {
            policyQualifiedNamePrefix = persona;
        } else if (purpose != null && !purpose.isEmpty()) {
            policyQualifiedNamePrefix = purpose;
        }

        List<RangerPolicy> policies = new ArrayList<>();
        if ("atlas".equals(serviceName)) {
            policies = getResourcePolicies();
        } else if ("atlas_tag".equals(serviceName)) {
            policies = getTagPolicies();
            LOG.info("getTagPolicies {}", policies.size());
            LOG.info("getTagPolicies {}", policies.stream().map(x -> x.getGuid()).collect(Collectors.toList()));
        } else if ("atlas_abac".equals(serviceName)) {
            policies = getAbacPolicies();
        }


        if (CollectionUtils.isNotEmpty(policies)) {
            policies = getFilteredPoliciesForQualifiedName(policies, policyQualifiedNamePrefix);
            policies = getFilteredPoliciesForActions(policies, actions, policyType);
            LOG.info("getFilteredPoliciesForActions {}", policies.stream().map(x -> x.getGuid()).collect(Collectors.toList()));

            if (!ignoreUser) {
                String user = AuthorizerCommon.getCurrentUserName();
                LOG.info("Getting relevant policies for user: {}", user);

                RangerUserStore userStore = UsersStore.getUserStore();
                List<String> groups = UsersStore.getGroupsForUser(user, userStore);

                RangerRoles allRoles = UsersStore.getAllRoles();
                List<String> roles = UsersStore.getRolesForUser(user, allRoles);
                roles.addAll(UsersStore.getNestedRolesForUser(roles, allRoles));

                policies = getFilteredPoliciesForUser(policies, user, groups, roles, policyType);
                LOG.info("getFilteredPoliciesForUser {}", policies.stream().map(x -> x.getGuid()).collect(Collectors.toList()));
            }
        }

        RequestContext.get().endMetricRecord(recorder);
        LOG.info("final filtered policies {}: {}", serviceName, policies.size());
        return policies;
    }

    static List<RangerPolicy> getFilteredPoliciesForQualifiedName(List<RangerPolicy> policies, String qualifiedNamePrefix) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getFilteredPoliciesForQualifiedName");
        if (qualifiedNamePrefix != null && !qualifiedNamePrefix.isEmpty()) {
            List<RangerPolicy> filteredPolicies = new ArrayList<>();
            for(RangerPolicy policy : policies) {
                if (policy.getName().startsWith(qualifiedNamePrefix)) {
                    filteredPolicies.add(policy);
                }
            }
            return filteredPolicies;
        }

        RequestContext.get().endMetricRecord(recorder);
        return policies;
    }

    private static List<RangerPolicy> getFilteredPoliciesForActions(List<RangerPolicy> policies, List<String> actions, String type) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getFilteredPoliciesForActions");
        List<RangerPolicy> filteredPolicies = new ArrayList<>();


        for(RangerPolicy policy : policies) {
            RangerPolicy.RangerPolicyItem policyItem = null;

            if (StringUtils.isNotEmpty(type)) {
                if (POLICY_TYPE_ALLOW.equals(type) && !policy.getPolicyItems().isEmpty()) {
                    policyItem = policy.getPolicyItems().get(0);
                } else if (POLICY_TYPE_DENY.equals(type) && !policy.getDenyPolicyItems().isEmpty()) {
                    policyItem = policy.getDenyPolicyItems().get(0);
                }
            } else {
                if (!policy.getPolicyItems().isEmpty()) {
                    policyItem = policy.getPolicyItems().get(0);
                } else if (!policy.getDenyPolicyItems().isEmpty()) {
                    policyItem = policy.getDenyPolicyItems().get(0);
                }
            }

            if (policyItem != null) {
                List<String> policyActions = new ArrayList<>();
                if (!policyItem.getAccesses().isEmpty()) {
                    policyActions = policyItem.getAccesses().stream().map(x -> x.getType()).collect(Collectors.toList());
                }
                if (AuthorizerCommon.arrayListContains(policyActions, actions)) {
                    filteredPolicies.add(policy);
                }
            }
        }

        RequestContext.get().endMetricRecord(recorder);
        return filteredPolicies;
    }

    private static List<RangerPolicy> getFilteredPoliciesForUser(List<RangerPolicy> policies, String user, List<String> groups, List<String> roles, String type) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getFilteredPoliciesForUser");

        List<RangerPolicy> filterPolicies = new ArrayList<>();
        for(RangerPolicy policy : policies) {
            RangerPolicy.RangerPolicyItem policyItem = null;

            if (StringUtils.isNotEmpty(type)) {
                if (POLICY_TYPE_ALLOW.equals(type) && !policy.getPolicyItems().isEmpty()) {
                    policyItem = policy.getPolicyItems().get(0);
                } else if (POLICY_TYPE_DENY.equals(type) && !policy.getDenyPolicyItems().isEmpty()) {
                    policyItem = policy.getDenyPolicyItems().get(0);
                }
            } else {
                if (!policy.getPolicyItems().isEmpty()) {
                    policyItem = policy.getPolicyItems().get(0);
                } else if (!policy.getDenyPolicyItems().isEmpty()) {
                    policyItem = policy.getDenyPolicyItems().get(0);
                }
            }

            if (policyItem != null) {
                List<String> policyUsers = policyItem.getUsers();
                List<String> policyGroups = policyItem.getGroups();
                List<String> policyRoles = policyItem.getRoles();
                if (policyUsers.contains(user)
                        || policyGroups.contains("public")
                        || AuthorizerCommon.arrayListContains(policyGroups, groups)
                        || AuthorizerCommon.arrayListContains(policyRoles, roles)) {
                    filterPolicies.add(policy);
                }
            }
        }

        RequestContext.get().endMetricRecord(recorder);
        return filterPolicies;
    }
}