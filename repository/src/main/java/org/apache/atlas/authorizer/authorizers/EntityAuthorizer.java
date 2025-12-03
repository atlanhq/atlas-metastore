package org.apache.atlas.authorizer.authorizers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAccessResult;
import org.apache.atlas.authorizer.store.PoliciesStore;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_ALLOW;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_DENY;
import static org.apache.atlas.repository.Constants.ACTION_READ;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_TAGS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_EQUALS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_ENDS_WITH;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_NOT_EQUALS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_STARTS_WITH;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_IN;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_NOT_IN;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_QUALIFIED_NAME;

public class EntityAuthorizer {

    private static final Logger LOG = LoggerFactory.getLogger(EntityAuthorizer.class);
    private static final PoliciesStore policiesStore = PoliciesStore.getInstance();
    private static final List<Set<String>> relatedAttributeSets = Arrays.asList(
            Set.of("ownerUsers", "ownerGroups"),
            Set.of("__traitNames", "__propagatedTraitNames")
    );

    public static AtlasAccessResult isAccessAllowedInMemory(AtlasEntityHeader entity, String action) {
        LOG.debug("ABAC_DEBUG: isAccessAllowedInMemory - Entry: action={}, entityGuid={}, entityType={}, qualifiedName={}",
                action, entity.getGuid(), entity.getTypeName(), entity.getAttribute(ATTR_QUALIFIED_NAME));

        AtlasAccessResult denyResult = isAccessAllowedInMemory(entity, action, POLICY_TYPE_DENY);
        LOG.debug("ABAC_DEBUG: Deny policy evaluation result: isAllowed={}, policyId={}, priority={}",
                denyResult.isAllowed(), denyResult.getPolicyId(), denyResult.getPolicyPriority());
        
        if (denyResult.isAllowed() && denyResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
            LOG.debug("ABAC_DEBUG: Override deny policy found, returning deny");
            return new AtlasAccessResult(false, denyResult.getPolicyId(), denyResult.getPolicyPriority());
        }

        AtlasAccessResult allowResult = isAccessAllowedInMemory(entity, action, POLICY_TYPE_ALLOW);
        LOG.debug("ABAC_DEBUG: Allow policy evaluation result: isAllowed={}, policyId={}, priority={}",
                allowResult.isAllowed(), allowResult.getPolicyId(), allowResult.getPolicyPriority());
        
        if (allowResult.isAllowed() && allowResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
            LOG.debug("ABAC_DEBUG: Override allow policy found, returning allow");
            return allowResult;
        }

        if (denyResult.isAllowed() && !"-1".equals(denyResult.getPolicyId())) {
            // explicit deny
            LOG.debug("ABAC_DEBUG: Explicit deny policy found, returning deny");
            return new AtlasAccessResult(false, denyResult.getPolicyId(), denyResult.getPolicyPriority());
        } else {
            LOG.debug("ABAC_DEBUG: Final result: isAllowed={}, policyId={}", allowResult.isAllowed(), allowResult.getPolicyId());
            return allowResult;
        }
    }

    private static AtlasAccessResult isAccessAllowedInMemory(AtlasEntityHeader entity, String action, String policyType) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("isAccessAllowedInMemory."+policyType);
        AtlasAccessResult result;

        LOG.debug("ABAC_DEBUG: Getting relevant policies - action={}, policyType={}, entityGuid={}", action, policyType, entity.getGuid());
        List<RangerPolicy> policies = policiesStore.getRelevantPolicies(null, null, "atlas_abac", Arrays.asList(action), policyType);
        LOG.debug("ABAC_DEBUG: Retrieved {} ABAC policies for action={}, policyType={}", policies.size(), action, policyType);
        
        if (policies.isEmpty()) {
            LOG.warn("ABAC_DEBUG: No ABAC policies found for action={}, policyType={}, entityGuid={}", action, policyType, entity.getGuid());
        } else {
            for (int i = 0; i < policies.size(); i++) {
                RangerPolicy policy = policies.get(i);
                LOG.debug("ABAC_DEBUG: Policy[{}]: id={}, name={}, guid={}, priority={}", 
                        i, policy.getId(), policy.getName(), policy.getGuid(), policy.getPolicyPriority());
            }
        }
        
        result = evaluateABACPoliciesInMemory(policies, entity, action);
        LOG.debug("ABAC_DEBUG: Evaluation result for policyType={}: isAllowed={}, policyId={}, priority={}",
                policyType, result.isAllowed(), result.getPolicyId(), result.getPolicyPriority());

        RequestContext.get().endMetricRecord(recorder);
        return result;
    }

    private static AtlasAccessResult evaluateABACPoliciesInMemory(List<RangerPolicy> abacPolicies, AtlasEntityHeader entity, String action) {
        AtlasAccessResult result = new AtlasAccessResult(false);
        LOG.debug("ABAC_DEBUG: evaluateABACPoliciesInMemory - Entry: entityGuid={}, entityType={}, policiesCount={}, action={}",
                entity.getGuid(), entity.getTypeName(), abacPolicies.size(), action);

        // don't need to fetch vertex for indexsearch response scrubbing as it already has the required attributes
        // setting vertex to null here as usage is already with a check for null possibility
        AtlasVertex vertex =  entity.getDocId() == null || !ACTION_READ.equals(action) ? AtlasGraphUtilsV2.findByGuid(entity.getGuid()) : null;
        LOG.debug("ABAC_DEBUG: Vertex lookup - docId={}, action={}, vertex={}", entity.getDocId(), action, vertex != null ? "found" : "null");

        int policyIndex = 0;
        for (RangerPolicy policy : abacPolicies) {
            policyIndex++;
            LOG.debug("ABAC_DEBUG: Evaluating policy[{}]: id={}, name={}, guid={}, priority={}",
                    policyIndex, policy.getId(), policy.getName(), policy.getGuid(), policy.getPolicyPriority());
            
            boolean matched = false;
            JsonNode entityFilterCriteriaNode = policy.getPolicyParsedFilterCriteria("entity");
            LOG.debug("ABAC_DEBUG: Policy[{}] filter criteria node: {}", policyIndex, entityFilterCriteriaNode != null ? "exists" : "null");
            
            if (entityFilterCriteriaNode != null) {
                LOG.debug("ABAC_DEBUG: Policy[{}] filter criteria: {}", policyIndex, entityFilterCriteriaNode.toString());
                matched = validateEntityFilterCriteria(entityFilterCriteriaNode, entity, vertex);
                LOG.debug("ABAC_DEBUG: Policy[{}] filter criteria match result: {}", policyIndex, matched);
            } else {
                LOG.warn("ABAC_DEBUG: Policy[{}] has no entity filter criteria, skipping", policyIndex);
            }
            
            if (matched) {
                // result here only means that a matching policy is found, allow and deny needs to be handled by caller
                LOG.debug("ABAC_DEBUG: Policy[{}] MATCHED! Setting result: policyGuid={}, priority={}",
                        policyIndex, policy.getGuid(), policy.getPolicyPriority());
                result = new AtlasAccessResult(true, policy.getGuid(), policy.getPolicyPriority());
                if (policy.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
                    LOG.debug("ABAC_DEBUG: Policy[{}] has override priority, returning immediately", policyIndex);
                    return result;
                }
            } else {
                LOG.debug("ABAC_DEBUG: Policy[{}] did not match filter criteria", policyIndex);
            }
        }
        
        LOG.debug("ABAC_DEBUG: Final evaluation result: isAllowed={}, policyId={}, priority={}",
                result.isAllowed(), result.getPolicyId(), result.getPolicyPriority());
        return result;
    }

    public static boolean validateEntityFilterCriteria(JsonNode data, AtlasEntityHeader entity, AtlasVertex vertex) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("validateEntityFilterCriteria");
        String condition = data.get("condition").asText();
        JsonNode criterion = data.get("criterion");

        LOG.debug("ABAC_DEBUG: validateEntityFilterCriteria - Entry: condition={}, criterionCount={}, entityGuid={}",
                condition, criterion != null && criterion.isArray() ? criterion.size() : 0, entity.getGuid());

        if (criterion == null || !criterion.isArray() || criterion.isEmpty() ) {
            LOG.warn("ABAC_DEBUG: validateEntityFilterCriteria - Invalid criterion: null={}, isArray={}, isEmpty={}",
                    criterion == null, criterion != null && criterion.isArray(), criterion != null && criterion.isEmpty());
            return false;
        }
        boolean result = true;

        int critIndex = 0;
        for (JsonNode crit : criterion) {
            critIndex++;
            result = !condition.equals("OR");

            boolean evaluation = false;

            if (crit.has("condition")) {
                LOG.debug("ABAC_DEBUG: Criterion[{}] has nested condition, recursing", critIndex);
                evaluation = validateEntityFilterCriteria(crit, entity, vertex);
                LOG.debug("ABAC_DEBUG: Criterion[{}] nested condition result: {}", critIndex, evaluation);
            } else {
                LOG.debug("ABAC_DEBUG: Criterion[{}] evaluating filter: {}", critIndex, crit.toString());
                evaluation = evaluateFilterCriteriaInMemory(crit, entity, vertex);
                LOG.debug("ABAC_DEBUG: Criterion[{}] evaluation result: {}", critIndex, evaluation);
            }

            if (condition.equals("AND")) {
                if (!evaluation) {
                    // One of the condition in AND is false, return false
                    LOG.debug("ABAC_DEBUG: AND condition - Criterion[{}] failed, returning false", critIndex);
                    RequestContext.get().endMetricRecord(recorder);
                    return false;
                }
                result = true;
                LOG.debug("ABAC_DEBUG: AND condition - Criterion[{}] passed, continuing", critIndex);
            } else {
                if (evaluation) {
                    // One of the condition in OR is true, return true
                    LOG.debug("ABAC_DEBUG: OR condition - Criterion[{}] passed, returning true", critIndex);
                    RequestContext.get().endMetricRecord(recorder);
                    return true;
                }
                result = result || evaluation;
                LOG.debug("ABAC_DEBUG: OR condition - Criterion[{}] failed, result so far: {}", critIndex, result);
            }
        }

        LOG.debug("ABAC_DEBUG: validateEntityFilterCriteria - Final result: {}", result);
        RequestContext.get().endMetricRecord(recorder);
        return result;
    }

    private static boolean evaluateFilterCriteriaInMemory(JsonNode crit, AtlasEntityHeader entity, AtlasVertex vertex) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("evaluateFilterCriteria");

        String attributeName = crit.get("attributeName").asText();
        LOG.debug("ABAC_DEBUG: evaluateFilterCriteria - Entry: attributeName={}, entityGuid={}", attributeName, entity.getGuid());

        if (attributeName.endsWith(".text")) {
            attributeName = attributeName.replace(".text", "");
            LOG.debug("ABAC_DEBUG: Removed .text suffix, attributeName={}", attributeName);
        } else if (attributeName.endsWith(".keyword")) {
            attributeName = attributeName.replace(".keyword", "");
            LOG.debug("ABAC_DEBUG: Removed .keyword suffix, attributeName={}", attributeName);
        }

        List<String> entityAttributeValues = getAttributeValue(entity, attributeName, vertex);
        LOG.debug("ABAC_DEBUG: Entity attribute values from getAttributeValue: attributeName={}, values={}", 
                attributeName, entityAttributeValues);
        
        List<String> specialAttributeValues = handleSpecialAttributes(entity, attributeName);
        LOG.debug("ABAC_DEBUG: Special attribute values: attributeName={}, values={}", attributeName, specialAttributeValues);
        
        entityAttributeValues.addAll(specialAttributeValues);
        LOG.debug("ABAC_DEBUG: Combined attribute values: attributeName={}, totalValues={}, values={}", 
                attributeName, entityAttributeValues.size(), entityAttributeValues);
        
        if (entityAttributeValues.isEmpty()) {
            LOG.warn("ABAC_DEBUG: Value for attribute {} not found for {}:{}", attributeName, entity.getTypeName(), entity.getAttribute(ATTR_QUALIFIED_NAME));
        }

        JsonNode attributeValueNode = crit.get("attributeValue");
        String operator = crit.get("operator").asText();
        LOG.debug("ABAC_DEBUG: Filter criteria - operator={}, attributeValueNode={}", operator, attributeValueNode);

        if (ATTR_TAGS.contains(attributeName)) { // handling tag values separately to incorporate multiple values requirement
            LOG.debug("ABAC_DEBUG: Evaluating tag filter criteria for attributeName={}", attributeName);
            boolean tagResult = evaluateTagFilterCriteria(attributeName, attributeValueNode, operator, entityAttributeValues);
            LOG.debug("ABAC_DEBUG: Tag filter criteria result: {}", tagResult);
            RequestContext.get().endMetricRecord(recorder);
            return tagResult;
        }

        List<String> attributeValues = new ArrayList<>();
        if (attributeValueNode.isArray()) {
            attributeValueNode.elements().forEachRemaining(node -> attributeValues.add(node.asText()));
            LOG.debug("ABAC_DEBUG: Attribute value is array, extracted {} values", attributeValues.size());
        } else {
            attributeValues.add(attributeValueNode.asText());
            LOG.debug("ABAC_DEBUG: Attribute value is single value: {}", attributeValues.get(0));
        }
        LOG.debug("ABAC_DEBUG: Policy attribute values: {}", attributeValues);

        boolean evaluationResult = false;
        switch (operator) {
            case POLICY_FILTER_CRITERIA_EQUALS -> {
                evaluationResult = new HashSet<>(entityAttributeValues).containsAll(attributeValues);
                LOG.debug("ABAC_DEBUG: EQUALS operator - entityValues={}, policyValues={}, result={}", 
                        entityAttributeValues, attributeValues, evaluationResult);
            }
            case POLICY_FILTER_CRITERIA_STARTS_WITH -> {
                for (String value : attributeValues) {
                    if (AuthorizerCommonUtil.listStartsWith(value, entityAttributeValues)) {
                        evaluationResult = true;
                        LOG.debug("ABAC_DEBUG: STARTS_WITH operator - matched value={}, result=true", value);
                        break;
                    }
                }
                if (!evaluationResult) {
                    LOG.debug("ABAC_DEBUG: STARTS_WITH operator - no match found, result=false");
                }
            }
            case POLICY_FILTER_CRITERIA_ENDS_WITH -> {
                for (String value : attributeValues) {
                    if (AuthorizerCommonUtil.listEndsWith(value, entityAttributeValues)) {
                        evaluationResult = true;
                        LOG.debug("ABAC_DEBUG: ENDS_WITH operator - matched value={}, result=true", value);
                        break;
                    }
                }
                if (!evaluationResult) {
                    LOG.debug("ABAC_DEBUG: ENDS_WITH operator - no match found, result=false");
                }
            }
            case POLICY_FILTER_CRITERIA_NOT_EQUALS -> {
                evaluationResult = Collections.disjoint(entityAttributeValues, attributeValues);
                LOG.debug("ABAC_DEBUG: NOT_EQUALS operator - entityValues={}, policyValues={}, result={}", 
                        entityAttributeValues, attributeValues, evaluationResult);
            }
            case POLICY_FILTER_CRITERIA_IN -> {
                if (AuthorizerCommonUtil.arrayListContains(attributeValues, entityAttributeValues)) {
                    evaluationResult = true;
                    LOG.debug("ABAC_DEBUG: IN operator - match found, result=true");
                } else {
                    LOG.debug("ABAC_DEBUG: IN operator - no match, result=false");
                }
            }
            case POLICY_FILTER_CRITERIA_NOT_IN -> {
                if (!AuthorizerCommonUtil.arrayListContains(attributeValues, entityAttributeValues)) {
                    evaluationResult = true;
                    LOG.debug("ABAC_DEBUG: NOT_IN operator - match found, result=true");
                } else {
                    LOG.debug("ABAC_DEBUG: NOT_IN operator - no match, result=false");
                }
            }
            default -> {
                LOG.warn("ABAC_DEBUG: Found unknown operator {}", operator);
            }
        }

        LOG.debug("ABAC_DEBUG: evaluateFilterCriteria - Final result: attributeName={}, operator={}, result={}", 
                attributeName, operator, evaluationResult);
        RequestContext.get().endMetricRecord(recorder);
        return evaluationResult;
    }

    private static List<String> handleSpecialAttributes(AtlasEntityHeader entity, String attributeName) {
        List<String> entityAttributeValues = new ArrayList<>();

        switch (attributeName) {
            case "__traitNames" -> {
                List<AtlasClassification> tags = entity.getClassifications();
                if (tags != null) {
                    for (AtlasClassification tag : tags) {
                        if (StringUtils.isEmpty(tag.getEntityGuid()) || tag.getEntityGuid().equals(entity.getGuid())) {
                            entityAttributeValues.add(tag.getTypeName());
                            entityAttributeValues.addAll(extractTagAttachmentValues(tag));
                        }
                    }
                }
            }
            case "__propagatedTraitNames" -> {
                List<AtlasClassification> tags = entity.getClassifications();
                if (tags != null) {
                    for (AtlasClassification tag : tags) {
                        if (StringUtils.isNotEmpty(tag.getEntityGuid()) && !tag.getEntityGuid().equals(entity.getGuid())) {
                            entityAttributeValues.add(tag.getTypeName());
                            entityAttributeValues.addAll(extractTagAttachmentValues(tag));
                        }
                    }
                }
            }
            case "__typeName" -> {
                String typeName = entity.getTypeName();
                Set<String> allValidTypes = AuthorizerCommonUtil.getTypeAndSupertypesList(typeName);
                entityAttributeValues.addAll(allValidTypes);
            }
        }

        return entityAttributeValues;
    }

    private static List<String> extractTagAttachmentValues(AtlasClassification tag) {
        String tagTypeName = tag.getTypeName();
        List<String> tagAttachmentValues = new ArrayList<>();

        if (tag.getAttributes() == null || tag.getAttributes().isEmpty()) {
            LOG.warn("ABAC_AUTH: Tag attributes are null or empty, tag={}", tagTypeName);
            return tagAttachmentValues;
        }

        for (String attrName : tag.getAttributes().keySet()) {
            try {
                Collection<AtlasStruct> attrValues = (Collection<AtlasStruct>) tag.getAttribute(attrName);
                for (AtlasStruct attrValue : attrValues) {
                    Map<String, Object> attrValueAttributes = attrValue.getAttributes();
                    if (attrValueAttributes == null || attrValueAttributes.isEmpty()) {
                        LOG.warn("ABAC_AUTH: Tag attribute value is null, tag={}, attribute={}", tagTypeName, attrName);
                        continue;
                    }
                    
                    List<AtlasStruct> sourceTagValue = (List<AtlasStruct>) attrValueAttributes.get("sourceTagValue");
                    if (sourceTagValue == null || sourceTagValue.isEmpty()) {
                        LOG.warn("ABAC_AUTH: Tag attribute's sourceTagValue attribute is empty, tag={}, attribute={}.sourceTagValue", tagTypeName, attrName);
                        continue;
                    }

                    for (AtlasStruct item : sourceTagValue) {
                        String key = item.getAttribute("tagAttachmentKey") == null ? "" : item.getAttribute("tagAttachmentKey").toString();
                        String value = item.getAttribute("tagAttachmentValue") == null ? "" : item.getAttribute("tagAttachmentValue").toString();
                        tagAttachmentValues.add(AuthorizerCommonUtil.tagKeyValueRepr(tagTypeName, key, value));
                    }
                }
            } catch (ClassCastException | NullPointerException e) {
                LOG.warn("ABAC_AUTH: Unexpected exception in tag attribute processing, tag={}, attribute={}, error={}", tagTypeName, attrName, e.getMessage());
            }
        }

        if (tagAttachmentValues.isEmpty()) {
            tagAttachmentValues.add(tagTypeName + ".="); // to support tag with no attachment values
        }
        LOG.info("ABAC_AUTH: Tag attachment values for tag={} value={}", tagTypeName, tagAttachmentValues);

        return tagAttachmentValues;
    }

    private static List<String> getAttributeValue(AtlasEntityHeader entity, String attributeName, AtlasVertex vertex) {
        List<String> entityAttributeValues = new ArrayList<>();
        LOG.debug("ABAC_DEBUG: getAttributeValue - Entry: attributeName={}, entityGuid={}, vertex={}", 
                attributeName, entity.getGuid(), vertex != null ? "exists" : "null");

        List<String> relatedAttributes = getRelatedAttributes(attributeName);
        LOG.debug("ABAC_DEBUG: Related attributes for {}: {}", attributeName, relatedAttributes);
        
        for (String relatedAttribute : relatedAttributes) {
            Object attrValue = entity.getAttribute(relatedAttribute);
            LOG.debug("ABAC_DEBUG: Checking attribute {} in entity header: value={}", relatedAttribute, attrValue != null ? "exists" : "null");
            
            if (attrValue != null) {
                if (attrValue instanceof Collection) {
                    Collection<? extends String> collection = (Collection<? extends String>) attrValue;
                    entityAttributeValues.addAll(collection);
                    LOG.debug("ABAC_DEBUG: Added collection values from entity header: attribute={}, count={}, values={}", 
                            relatedAttribute, collection.size(), collection);
                } else {
                    String strValue = String.valueOf(attrValue);
                    entityAttributeValues.add(strValue);
                    LOG.debug("ABAC_DEBUG: Added single value from entity header: attribute={}, value={}", 
                            relatedAttribute, strValue);
                }
            } else if (vertex != null) {
                // try fetching from vertex
                LOG.debug("ABAC_DEBUG: Attribute not in entity header, trying vertex: attribute={}", relatedAttribute);
                Collection<?> values = vertex.getPropertyValues(relatedAttribute, String.class);
                for (Object value : values) {
                    String strValue = String.valueOf(value);
                    entityAttributeValues.add(strValue);
                    LOG.debug("ABAC_DEBUG: Added value from vertex: attribute={}, value={}", relatedAttribute, strValue);
                }
                if (values.isEmpty()) {
                    LOG.debug("ABAC_DEBUG: No values found in vertex for attribute: {}", relatedAttribute);
                }
            } else {
                LOG.debug("ABAC_DEBUG: Attribute not found in entity header and vertex is null: attribute={}", relatedAttribute);
            }
        }
        
        LOG.debug("ABAC_DEBUG: getAttributeValue - Final result: attributeName={}, valuesCount={}, values={}", 
                attributeName, entityAttributeValues.size(), entityAttributeValues);
        return entityAttributeValues;
    }

    public static List<String> getRelatedAttributes(String attributeName) {
        List<String> relatedAttributes = new ArrayList<>();

        // Check if attributeName exists in any set and add the entire set
        for (Set<String> attributeSet : relatedAttributeSets) {
            if (attributeSet.contains(attributeName)) {
                relatedAttributes.addAll(attributeSet);
                break;
            }
        }
        if (relatedAttributes.isEmpty()) {
            relatedAttributes.add(attributeName);
        }

        return relatedAttributes;
    }

    private static List<String> getRequiredTagValues(JsonNode tagValueNode) {
        List<String> requiredTagValues = new ArrayList<>();
        if (AuthorizerCommonUtil.isTagKeyValueFormat(tagValueNode)) {
            String tagName = tagValueNode.get("name").asText();

            JsonNode valuesNode = tagValueNode.get("tagValues");
            if (valuesNode != null && valuesNode.isArray()) {
                for (JsonNode valueNode : valuesNode) {
                    String key = valueNode.get("key") == null ? null : valueNode.get("key").asText();
                    String value = valueNode.get("consolidatedValue") == null ? null : valueNode.get("consolidatedValue").asText();
                    requiredTagValues.add(AuthorizerCommonUtil.tagKeyValueRepr(tagName, key, value));
                }
            } else {
                LOG.warn("Invalid tag values format for tag: {}", tagName);
            }

            if (requiredTagValues.isEmpty()) {
                requiredTagValues.add(tagName);
            }
        } else {
            requiredTagValues.add(tagValueNode.asText());
        }
        return requiredTagValues;
    }

    private static boolean evaluateTagFilterCriteria(String attributeName, JsonNode attributeValueNode, String operator, List<String> entityAttributeValues) {
        List<List<String>> attributeValues = new ArrayList<>();
        if (attributeValueNode.isArray()) {
            for (JsonNode node : attributeValueNode) {
                attributeValues.add(getRequiredTagValues(node));
            }
        } else {
            attributeValues.add(getRequiredTagValues(attributeValueNode));
        }

        // no support required for starts_with and ends_with for tags
        boolean result = false;
        switch(operator) {
            case POLICY_FILTER_CRITERIA_EQUALS -> {
                for (List<String> tagValues : attributeValues) {
                    if (!AuthorizerCommonUtil.arrayListContains(entityAttributeValues, tagValues)) {
                        return false;
                    }
                }
                result = true;
            }
            case POLICY_FILTER_CRITERIA_NOT_EQUALS, POLICY_FILTER_CRITERIA_NOT_IN -> {
                for (List<String> tagValues : attributeValues) {
                    if (AuthorizerCommonUtil.arrayListContains(entityAttributeValues, tagValues)) {
                        return false;
                    }
                }
                result = true;
            }
            case POLICY_FILTER_CRITERIA_IN -> {
                for (List<String> tagValues : attributeValues) {
                    if (AuthorizerCommonUtil.arrayListContains(entityAttributeValues, tagValues)) {
                        return true;
                    }
                }
            }
        }
        return result;
    }
}

