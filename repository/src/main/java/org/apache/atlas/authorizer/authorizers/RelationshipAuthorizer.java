package org.apache.atlas.authorizer.authorizers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAccessResult;
import org.apache.atlas.authorizer.store.PoliciesStore;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_ALLOW;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_DENY;
import static org.apache.atlas.authorizer.authorizers.EntityAuthorizer.validateEntityFilterCriteria;

public class RelationshipAuthorizer {

    private static final Logger LOG = LoggerFactory.getLogger(RelationshipAuthorizer.class);
    private static final PoliciesStore policiesStore = PoliciesStore.getInstance();

    private static List<String> RELATIONSHIP_ENDS = new ArrayList<String>() {{
        add("end-one");
        add("end-two");
    }};

    public static AtlasAccessResult isAccessAllowedInMemory(String action, String relationshipType, AtlasEntityHeader endOneEntity, AtlasEntityHeader endTwoEntity) throws AtlasBaseException {
        AtlasAccessResult denyResult = checkRelationshipAccessAllowedInMemory(action, relationshipType, endOneEntity, endTwoEntity, POLICY_TYPE_DENY);
        if (denyResult.isAllowed() && denyResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
            return new AtlasAccessResult(false, denyResult.getPolicyId(), denyResult.getPolicyPriority());
        }

        AtlasAccessResult allowResult = checkRelationshipAccessAllowedInMemory(action, relationshipType, endOneEntity, endTwoEntity, POLICY_TYPE_ALLOW);
        if (allowResult.isAllowed() && allowResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
            return allowResult;
        }

        if (denyResult.isAllowed() && !"-1".equals(denyResult.getPolicyId())) {
            // explicit deny
            return new AtlasAccessResult(false, denyResult.getPolicyId(), denyResult.getPolicyPriority());
        } else {
            return allowResult;
        }
    }

    private static AtlasAccessResult checkRelationshipAccessAllowedInMemory(String action, String relationshipType, AtlasEntityHeader endOneEntity,
                                                         AtlasEntityHeader endTwoEntity, String policyType) throws AtlasBaseException {
        // Relationship add, update, remove access check in memory
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("checkRelationshipAccessAllowedInMemory."+policyType);
        AtlasAccessResult result = new AtlasAccessResult();

        try {
            List<RangerPolicy> policies = policiesStore.getRelevantPolicies(null, null, "atlas_abac", Arrays.asList(action), policyType);
            if (!policies.isEmpty()) {
                AtlasVertex oneVertex = endOneEntity.getGuid() != null ? AtlasGraphUtilsV2.findByGuid(endOneEntity.getGuid()) : null;
                AtlasVertex twoVertex = endTwoEntity.getGuid() != null ? AtlasGraphUtilsV2.findByGuid(endTwoEntity.getGuid()) : null;

                for (RangerPolicy policy : policies) {
                    boolean eval = false;
                    JsonNode entityFilterCriteriaNode = policy.getPolicyParsedFilterCriteria("endOneEntity");
                    if (entityFilterCriteriaNode != null) {
                        eval = validateEntityFilterCriteria(entityFilterCriteriaNode, endOneEntity, oneVertex);
                        if (eval) {
                            entityFilterCriteriaNode = policy.getPolicyParsedFilterCriteria("endTwoEntity");
                            eval = validateEntityFilterCriteria(entityFilterCriteriaNode, endTwoEntity, twoVertex);
                        }
                    }
                    //ret = ret || eval;
                    if (eval) {
                        result = new AtlasAccessResult(true, policy.getGuid(), policy.getPolicyPriority());
                        if (policy.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
                            return result;
                        }
                    }
                }
            }

            return result;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }
}
