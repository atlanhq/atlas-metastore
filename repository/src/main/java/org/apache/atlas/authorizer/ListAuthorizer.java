package org.apache.atlas.authorizer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.authorizer.AuthorizerCommon.*;

public class ListAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthorizationUtils.class);

    public static Map<String, Object> getElasticsearchDSL(String persona, String purpose, List<String> actions) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("ListAuthorizer.getElasticsearchDSL");
        Map<String, Object> allowDsl = getElasticsearchDSLForPolicyType(persona, purpose, actions, POLICY_TYPE_ALLOW);
        Map<String, Object> denyDsl = getElasticsearchDSLForPolicyType(persona, purpose, actions, POLICY_TYPE_DENY);
        Map<String, Object> finaDsl = new HashMap<>();
        if (allowDsl != null) {
            finaDsl.put("filter", allowDsl);
        }
        if (denyDsl != null) {
            finaDsl.put("must_not", denyDsl);
        }

        RequestContext.get().endMetricRecord(recorder);
        return getMap("bool", finaDsl);
    }

    public static Map<String, Object> getElasticsearchDSLForPolicyType(String persona, String purpose, List<String> actions, String policyType) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("ListAuthorizer.getElasticsearchDSLForPolicyType."+ policyType);
        List<RangerPolicy> resourcePolicies = PoliciesStore.getRelevantPolicies(persona, purpose, "atlas", actions, policyType);
        List<Map<String, Object>> resourcePoliciesClauses = getDSLForResourcePolicies(resourcePolicies);

        List<RangerPolicy> tagPolicies = PoliciesStore.getRelevantPolicies(persona, purpose, "atlas_tag", actions, policyType);
        Map<String, Object> tagPoliciesClause = getDSLForTagPolicies(tagPolicies);

        List<RangerPolicy> abacPolicies = PoliciesStore.getRelevantPolicies(persona, purpose, "atlas_abac", actions, policyType);
        List<Map<String, Object>> abacPoliciesClauses = getDSLForAbacPolicies(abacPolicies);

        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        shouldClauses.addAll(resourcePoliciesClauses);
        if (tagPoliciesClause != null) {
            shouldClauses.add(tagPoliciesClause);
        }
        shouldClauses.addAll(abacPoliciesClauses);

        Map<String, Object> boolClause = new HashMap<>();
        if (shouldClauses.isEmpty()) {
            if (POLICY_TYPE_ALLOW.equals(policyType)) {
                boolClause.put("must_not", getMap("match_all", new HashMap<>()));
            } else {
                return null;
            }

        } else {
            boolClause.put("should", shouldClauses);
            boolClause.put("minimum_should_match", 1);
        }

        RequestContext.get().endMetricRecord(recorder);
        return getMap("bool", boolClause);
    }

    public static List<Map<String, Object>> getDSLForResourcePolicies(List<RangerPolicy> policies) {

        // To reduce the number of clauses
        List<String> combinedEntities = new ArrayList<>();
        Set<String> combinedEntityTypes = new HashSet<>();
        List<Map<String, Object>> shouldClauses = new ArrayList<>();

        for (RangerPolicy policy : policies) {
            if (!policy.getResources().isEmpty() && "ENTITY".equals(policy.getPolicyResourceCategory())) {
                List<String> entities = policy.getResources().get("entity").getValues();
                List<String> entityTypesRaw = policy.getResources().get("entity-type").getValues();

                if (entities.contains("*") && entityTypesRaw.contains("*")) {
                    Map<String, String> emptyMap = new HashMap<>();
                    shouldClauses.removeAll(shouldClauses);
                    shouldClauses.add(getMap("match_all",emptyMap));
                    break;
                }

                entities.remove("*");
                entityTypesRaw.remove("*");

                //Set<String> entityTypes = new HashSet<>();
                //entityTypesRaw.forEach(x -> entityTypes.addAll(AuthorizerCommon.getTypeAndSupertypesList(x)));

                if (!entities.isEmpty() && entityTypesRaw.isEmpty()) {
                    combinedEntities.addAll(entities);
                } else if (entities.isEmpty() && !entityTypesRaw.isEmpty()) {
                    combinedEntityTypes.addAll(entityTypesRaw);
                } else if (!entities.isEmpty() && !entityTypesRaw.isEmpty()) {
                    Map<String, Object> dslForPolicyResources = getDSLForResources(entities, new HashSet<>(entityTypesRaw), null, null);
                    shouldClauses.add(dslForPolicyResources);
                }
            }
        }
        if (!combinedEntities.isEmpty()) {
            shouldClauses.add(getDSLForResources(combinedEntities, new HashSet<>(), null, null));
        }
        if (!combinedEntityTypes.isEmpty()) {
            shouldClauses.add(getDSLForResources(new ArrayList<>(), combinedEntityTypes, null, null));
        }
        return shouldClauses;
    }

    public static Map<String, Object> getDSLForResources(List<String> entities, Set<String> typeNames, List<String> classifications, String clauseName){
        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        List<String> termsQualifiedNames = new ArrayList<>();
        for (String entity: entities) {
            if (!entity.equals("*")) {
                if (entity.contains("*")) {
                    shouldClauses.add(getMap("wildcard", getMap("qualifiedName", entity)));
                } else {
                    termsQualifiedNames.add(entity);
                }
            }
        }
        if (!termsQualifiedNames.isEmpty()) {
            shouldClauses.add(getMap("terms", getMap("qualifiedName", termsQualifiedNames)));
        }

        Map<String, Object> boolClause = new HashMap<>();

        if (!shouldClauses.isEmpty()) {
            boolClause.put("should", shouldClauses);
            boolClause.put("minimum_should_match", 1);
        }

        List<Map<String, Object>> filterClauses = new ArrayList<>();

        if (!typeNames.isEmpty() && !typeNames.contains("*")) {
            List<Map<String, Object>> typeClauses = new ArrayList<>();
            typeClauses.add(getMap("terms", getMap("__typeName.keyword", typeNames)));
            typeClauses.add(getMap("terms", getMap("__superTypeNames.keyword", typeNames)));

            filterClauses.add(getMap("bool", getMap("should", typeClauses)));
        }

        if (classifications != null && !classifications.isEmpty() && !classifications.contains("*")) {
            List<Map<String, Object>> classificationClauses = new ArrayList<>();

            classificationClauses.add(getMap("terms", getMap("__traitNames", classifications)));
            classificationClauses.add(getMap("terms", getMap("__propagatedTraitNames", classifications)));

            filterClauses.add(getMap("bool", getMap("should", classificationClauses)));
        }

        if (!filterClauses.isEmpty()) {
            boolClause.put("filter", filterClauses);
        }

        if (clauseName != null) {
            boolClause.put("_name", clauseName);
        }

        return getMap("bool", boolClause);
    }

    public static Map<String, Object> getDSLForTagPolicies(List<RangerPolicy> policies) {
        // To reduce the number of clauses
        Set<String> allTags = new HashSet<>();
        LOG.info("Found {} tag policies", policies.size());

        for (RangerPolicy policy : policies) {
            if (!policy.getResources().isEmpty()) {
                LOG.info("policy {}", AtlasType.toJson(policy));
                List<String> tags = policy.getResources().get("tag").getValues();
                if (!tags.isEmpty()) {
                    allTags.addAll(tags);
                }
            }
        }
        if (!allTags.isEmpty()) {
            return getDSLForTags(allTags);
        }
        return null;
    }

    private static Map<String, Object> getDSLForTags(Set<String> tags){
        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        shouldClauses.add(getMap("terms", getMap("__traitNames", tags)));
        shouldClauses.add(getMap("terms", getMap("__propagatedTraitNames", tags)));

        Map<String, Object> boolClause = new HashMap<>();
        boolClause.put("should", shouldClauses);
        boolClause.put("minimum_should_match", 1);

        return getMap("bool", boolClause);
    }

    public static List<Map<String, Object>> getDSLForAbacPolicies(List<RangerPolicy> policies) {
        List<String> filterCriteriaList = new ArrayList<>();
        for (RangerPolicy policy : policies) {
            String filterCriteria = policy.getPolicyFilterCriteria();
            if (filterCriteria != null && !filterCriteria.isEmpty() ) {
                filterCriteriaList.add(filterCriteria);
            }
        }
        List<String> dslList = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        for (String filterCriteria: filterCriteriaList) {
            JsonNode filterCriteriaNode = null;
            try {
                filterCriteriaNode = mapper.readTree(filterCriteria);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            if (filterCriteriaNode != null && filterCriteriaNode.get("entity") != null) {
                JsonNode entityFilterCriteriaNode = filterCriteriaNode.get("entity");
                JsonNode dsl = JsonToElasticsearchQuery.convertJsonToQuery(entityFilterCriteriaNode, mapper);
                dslList.add(dsl.toString());
            }
        }
        List<Map<String, Object>> clauses = new ArrayList<>();
        for (String dsl: dslList) {
            String policyDSLBase64 = Base64.getEncoder().encodeToString(dsl.getBytes());;
            clauses.add(getMap("wrapper", getMap("query", policyDSLBase64)));
        }
        return clauses;
    }
}
