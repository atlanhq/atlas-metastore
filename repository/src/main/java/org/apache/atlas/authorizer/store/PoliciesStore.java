package org.apache.atlas.authorizer.store;

import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.authorizers.AuthorizerCommonUtil;
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

import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_ALLOW;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_DENY;

public class PoliciesStore {

    private static final Logger LOG = LoggerFactory.getLogger(PoliciesStore.class);
    private static final PoliciesStore INSTANCE = new PoliciesStore();

    private List<RangerPolicy> resourcePolicies;
    private List<RangerPolicy> tagPolicies;
    private List<RangerPolicy> abacPolicies;

    private PoliciesStore() {} // private constructor

    public static PoliciesStore getInstance() {
        return INSTANCE;
    }

    public void setResourcePolicies(List<RangerPolicy> resourcePolicies) {
        this.resourcePolicies = resourcePolicies;
    }

    private List<RangerPolicy> getResourcePolicies() {
        return resourcePolicies;
    }

    public void setTagPolicies(List<RangerPolicy> tagPolicies) {
        this.tagPolicies = tagPolicies;
    }

    private List<RangerPolicy> getTagPolicies() {
        return tagPolicies;
    }

    public void setAbacPolicies(List<RangerPolicy> abacPolicies) {
        this.abacPolicies = abacPolicies;
    }

    public List<RangerPolicy> getAbacPolicies() {
        return abacPolicies;
    }

    public List<RangerPolicy> getRelevantPolicies(String persona, String purpose, String serviceName, List<String> actions, String policyType) {
        return getRelevantPolicies(persona, purpose, serviceName, actions, policyType, false);
    }

    public List<RangerPolicy> getRelevantPolicies(String persona, String purpose, String serviceName, List<String> actions, String policyType, boolean ignoreUser) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getRelevantPolicies");
        LOG.debug("ABAC_DEBUG: getRelevantPolicies - Entry: persona={}, purpose={}, serviceName={}, actions={}, policyType={}, ignoreUser={}",
                persona, purpose, serviceName, actions, policyType, ignoreUser);
        
        String policyQualifiedNamePrefix = null;
        if (persona != null && !persona.isEmpty()) {
            policyQualifiedNamePrefix = persona;
            LOG.debug("ABAC_DEBUG: Using persona as qualified name prefix: {}", policyQualifiedNamePrefix);
        } else if (purpose != null && !purpose.isEmpty()) {
            policyQualifiedNamePrefix = purpose;
            LOG.debug("ABAC_DEBUG: Using purpose as qualified name prefix: {}", policyQualifiedNamePrefix);
        } else {
            LOG.debug("ABAC_DEBUG: No persona or purpose specified, will not filter by qualified name prefix");
        }

        List<RangerPolicy> policies = new ArrayList<>();
        if ("atlas".equals(serviceName)) {
            policies = getResourcePolicies();
            LOG.debug("ABAC_DEBUG: Retrieved {} resource policies", policies != null ? policies.size() : 0);
        } else if ("atlas_tag".equals(serviceName)) {
            policies = getTagPolicies();
            LOG.debug("ABAC_DEBUG: Retrieved {} tag policies", policies != null ? policies.size() : 0);
        } else if ("atlas_abac".equals(serviceName)) {
            policies = getAbacPolicies();
            LOG.debug("ABAC_DEBUG: Retrieved {} ABAC policies", policies != null ? policies.size() : 0);
        } else {
            LOG.warn("ABAC_DEBUG: Unknown service name: {}", serviceName);
        }

        List<RangerPolicy> filteredPolicies = null;
        if (CollectionUtils.isNotEmpty(policies)) {
            filteredPolicies = new ArrayList<>(policies);
            LOG.debug("ABAC_DEBUG: Starting with {} policies before filtering", filteredPolicies.size());
            
            filteredPolicies = getFilteredPoliciesForQualifiedName(filteredPolicies, policyQualifiedNamePrefix);
            LOG.debug("ABAC_DEBUG: After qualified name filtering: {} policies", filteredPolicies.size());
            
            filteredPolicies = getFilteredPoliciesForActions(filteredPolicies, actions, policyType);
            LOG.debug("ABAC_DEBUG: After action filtering: {} policies", filteredPolicies.size());

            if (!ignoreUser) {
                String user = AuthorizerCommonUtil.getCurrentUserName();
                LOG.debug("ABAC_DEBUG: Current user: {}", user);

                UsersStore usersStore = UsersStore.getInstance();
                RangerUserStore userStore = usersStore.getUserStore();
                List<String> groups = usersStore.getGroupsForUser(user, userStore);
                LOG.debug("ABAC_DEBUG: User groups: {}", groups);

                RangerRoles allRoles = usersStore.getAllRoles();
                List<String> roles = usersStore.getRolesForUser(user, allRoles);
                roles.addAll(usersStore.getNestedRolesForUser(roles, allRoles));
                LOG.debug("ABAC_DEBUG: User roles: {}", roles);

                filteredPolicies = getFilteredPoliciesForUser(filteredPolicies, user, groups, roles, policyType);
                LOG.debug("ABAC_DEBUG: After user filtering: {} policies", filteredPolicies.size());
            } else {
                LOG.debug("ABAC_DEBUG: Skipping user filtering (ignoreUser=true)");
            }
        } else {
            filteredPolicies = new ArrayList<>(0);
            LOG.warn("ABAC_DEBUG: No policies found for serviceName={}, returning empty list", serviceName);
        }

        LOG.debug("ABAC_DEBUG: getRelevantPolicies - Final result: {} policies", filteredPolicies.size());
        RequestContext.get().endMetricRecord(recorder);
        return filteredPolicies;
    }

    private List<RangerPolicy> getFilteredPoliciesForQualifiedName(List<RangerPolicy> policies, String qualifiedNamePrefix) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getFilteredPoliciesForQualifiedName");
        LOG.debug("ABAC_DEBUG: getFilteredPoliciesForQualifiedName - Entry: policiesCount={}, prefix={}", 
                policies.size(), qualifiedNamePrefix);
        
        if (qualifiedNamePrefix != null && !qualifiedNamePrefix.isEmpty()) {
            List<RangerPolicy> filteredPolicies = new ArrayList<>();
            for(RangerPolicy policy : policies) {
                boolean matches = policy.getName().startsWith(qualifiedNamePrefix);
                LOG.debug("ABAC_DEBUG: Policy name={}, prefix={}, matches={}", 
                        policy.getName(), qualifiedNamePrefix, matches);
                if (matches) {
                    filteredPolicies.add(policy);
                }
            }
            LOG.debug("ABAC_DEBUG: getFilteredPoliciesForQualifiedName - Result: {} policies matched", filteredPolicies.size());
            RequestContext.get().endMetricRecord(recorder);
            return filteredPolicies;
        }

        LOG.debug("ABAC_DEBUG: getFilteredPoliciesForQualifiedName - No prefix filter, returning all {} policies", policies.size());
        RequestContext.get().endMetricRecord(recorder);
        return policies;
    }

    private static List<RangerPolicy> getFilteredPoliciesForActions(List<RangerPolicy> policies, List<String> actions, String type) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getFilteredPoliciesForActions");
        LOG.debug("ABAC_DEBUG: getFilteredPoliciesForActions - Entry: policiesCount={}, actions={}, type={}", 
                policies.size(), actions, type);
        
        List<RangerPolicy> filteredPolicies = new ArrayList<>();

        int policyIndex = 0;
        for(RangerPolicy policy : policies) {
            policyIndex++;
            RangerPolicy.RangerPolicyItem policyItem = null;

            if (StringUtils.isNotEmpty(type)) {
                if (POLICY_TYPE_ALLOW.equals(type) && !policy.getPolicyItems().isEmpty()) {
                    policyItem = policy.getPolicyItems().get(0);
                    LOG.debug("ABAC_DEBUG: Policy[{}] using allow policy item", policyIndex);
                } else if (POLICY_TYPE_DENY.equals(type) && !policy.getDenyPolicyItems().isEmpty()) {
                    policyItem = policy.getDenyPolicyItems().get(0);
                    LOG.debug("ABAC_DEBUG: Policy[{}] using deny policy item", policyIndex);
                }
            } else {
                if (!policy.getPolicyItems().isEmpty()) {
                    policyItem = policy.getPolicyItems().get(0);
                    LOG.debug("ABAC_DEBUG: Policy[{}] using allow policy item (no type specified)", policyIndex);
                } else if (!policy.getDenyPolicyItems().isEmpty()) {
                    policyItem = policy.getDenyPolicyItems().get(0);
                    LOG.debug("ABAC_DEBUG: Policy[{}] using deny policy item (no type specified)", policyIndex);
                }
            }

            if (policyItem != null) {
                List<String> policyActions = new ArrayList<>();
                if (!policyItem.getAccesses().isEmpty()) {
                    policyActions = policyItem.getAccesses().stream().map(x -> x.getType()).collect(Collectors.toList());
                }
                LOG.debug("ABAC_DEBUG: Policy[{}] actions: {}, requested actions: {}", 
                        policyIndex, policyActions, actions);
                
                boolean actionMatches = AuthorizerCommonUtil.arrayListContains(policyActions, actions);
                LOG.debug("ABAC_DEBUG: Policy[{}] action match: {}", policyIndex, actionMatches);
                
                if (actionMatches) {
                    filteredPolicies.add(policy);
                    LOG.debug("ABAC_DEBUG: Policy[{}] added to filtered list", policyIndex);
                } else {
                    LOG.debug("ABAC_DEBUG: Policy[{}] action mismatch, not added", policyIndex);
                }
            } else {
                LOG.warn("ABAC_DEBUG: Policy[{}] has no policy items (allow or deny)", policyIndex);
            }
        }

        LOG.debug("ABAC_DEBUG: getFilteredPoliciesForActions - Result: {} policies matched actions", filteredPolicies.size());
        RequestContext.get().endMetricRecord(recorder);
        return filteredPolicies;
    }

    private static List<RangerPolicy> getFilteredPoliciesForUser(List<RangerPolicy> policies, String user, List<String> groups, List<String> roles, String type) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getFilteredPoliciesForUser");
        LOG.debug("ABAC_DEBUG: getFilteredPoliciesForUser - Entry: policiesCount={}, user={}, groups={}, roles={}, type={}", 
                policies.size(), user, groups, roles, type);

        List<RangerPolicy> filterPolicies = new ArrayList<>();
        int policyIndex = 0;
        for(RangerPolicy policy : policies) {
            policyIndex++;
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
                
                LOG.debug("ABAC_DEBUG: Policy[{}] user matching - policyUsers={}, policyGroups={}, policyRoles={}", 
                        policyIndex, policyUsers, policyGroups, policyRoles);
                
                boolean userMatch = policyUsers.contains(user);
                boolean publicGroupMatch = policyGroups.contains("public");
                boolean groupMatch = AuthorizerCommonUtil.arrayListContains(policyGroups, groups);
                boolean roleMatch = AuthorizerCommonUtil.arrayListContains(policyRoles, roles);
                
                LOG.debug("ABAC_DEBUG: Policy[{}] match results - userMatch={}, publicGroupMatch={}, groupMatch={}, roleMatch={}", 
                        policyIndex, userMatch, publicGroupMatch, groupMatch, roleMatch);
                
                if (userMatch || publicGroupMatch || groupMatch || roleMatch) {
                    filterPolicies.add(policy);
                    LOG.debug("ABAC_DEBUG: Policy[{}] matched user/groups/roles, added to filtered list", policyIndex);
                } else {
                    LOG.debug("ABAC_DEBUG: Policy[{}] did not match user/groups/roles, not added", policyIndex);
                }
            } else {
                LOG.warn("ABAC_DEBUG: Policy[{}] has no policy items for type={}", policyIndex, type);
            }
        }

        LOG.debug("ABAC_DEBUG: getFilteredPoliciesForUser - Result: {} policies matched user/groups/roles", filterPolicies.size());
        RequestContext.get().endMetricRecord(recorder);
        return filterPolicies;
    }
}
