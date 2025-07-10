package org.apache.atlas.authorizer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.authorizers.EntityAuthorizer;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_AND;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_OR;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_EQUALS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_NOT_EQUALS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_STARTS_WITH;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_ENDS_WITH;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_IN;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_NOT_IN;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_NEGATIVE_OPS;


public class JsonToElasticsearchQuery {
    private static final Logger LOG = LoggerFactory.getLogger(JsonToElasticsearchQuery.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String FILTER_CRITERIA_CONDITION = "condition";

    private static JsonNode convertConditionToQuery(String condition) {
        if (condition.equals(POLICY_FILTER_CRITERIA_AND)) {
            return mapper.createObjectNode().set("bool", mapper.createObjectNode().set("filter", mapper.createArrayNode()));
        } else if (condition.equals(POLICY_FILTER_CRITERIA_OR)) {
            return mapper.createObjectNode()
                    .set("bool", mapper.createObjectNode()
                    .set("should", mapper.createArrayNode()));
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition);
        }
    }

    public static JsonNode convertJsonToQuery(JsonNode data) {
        AtlasPerfMetrics.MetricRecorder convertJsonToQueryMetrics = RequestContext.get().startMetricRecord("convertJsonToQuery");
        String condition = data.get("condition").asText();
        JsonNode criterion = data.get("criterion");

        JsonNode query = convertConditionToQuery(condition);

        for (JsonNode crit : criterion) {
            if (crit.has("condition")) {
                JsonNode nestedQuery = convertJsonToQuery(crit);
                if (condition.equals("AND")) {
                    ((ArrayNode) query.get("bool").get("filter")).add(nestedQuery);
                } else {
                    ((ArrayNode) query.get("bool").get("should")).add(nestedQuery);
                }
            } else {
                String operator = crit.get("operator").asText();
                String attributeName = crit.get("attributeName").asText();
                JsonNode attributeValueNode = crit.get("attributeValue");
                
                List<String> relatedAttributes = EntityAuthorizer.getRelatedAttributes(attributeName);

                ArrayNode queryArray = ((ArrayNode) query.get("bool").get(getConditionClause(condition)));
                JsonNode attributeQuery;
                if (relatedAttributes.size() > 1) { // handle attributes with multiple related attributes
                    String relatedAttrCondition = POLICY_FILTER_CRITERIA_NEGATIVE_OPS.contains(operator)
                            ? POLICY_FILTER_CRITERIA_AND
                            : POLICY_FILTER_CRITERIA_OR;
                    attributeQuery = convertConditionToQuery(relatedAttrCondition);
                    for (String relatedAttribute : relatedAttributes) {
                        JsonNode relatedAttributeQuery = createAttributeQuery(operator, relatedAttribute, attributeValueNode);
                        ((ArrayNode) attributeQuery.get("bool").get(getConditionClause(relatedAttrCondition))).add(relatedAttributeQuery);
                    }
                } else {
                    attributeQuery = createAttributeQuery(operator, attributeName, attributeValueNode);
                }

                if (attributeQuery != null) queryArray.add(attributeQuery);
            }
        }
        RequestContext.get().endMetricRecord(convertJsonToQueryMetrics);
        return query;
    }

    private static boolean hasNullValue(ArrayNode arrayNode) {
        for (JsonNode valueNode : arrayNode) {
            if (valueNode.isNull()) {
                return true;
            }
        }
        return false;
    }

    private static ArrayNode removeNullValues(ArrayNode arrayNode) {
        ArrayNode nonNullValues = mapper.createArrayNode();
        for (JsonNode valueNode : arrayNode) {
            if (!valueNode.isNull()) {
                nonNullValues.add(valueNode);
            }
        }
        return nonNullValues;
    }

    private static JsonNode createAttributeQuery(String operator, String attributeName, JsonNode attributeValueNode) {
        ObjectNode queryNode = mapper.createObjectNode();
        String attributeValue = attributeValueNode.asText();


        // handle array values for attributeValueNode
        ArrayNode arrayNode = null;
        ArrayNode nonNullValues = null;
        boolean hasNull = false;
        boolean hasNonNull = false;
        if (attributeValueNode.isArray()) {
            arrayNode = (ArrayNode) attributeValueNode;
            nonNullValues = removeNullValues(arrayNode);
            hasNull = hasNullValue(arrayNode);
            hasNonNull = nonNullValues.size() > 0;
        }
        
        switch (operator) {
            case POLICY_FILTER_CRITERIA_EQUALS:
                if (arrayNode != null) {
                    ArrayNode filterArray = queryNode.putObject("bool").putArray("filter");
                    for (JsonNode valueNode : nonNullValues) {
                        filterArray.addObject().putObject("term").put(attributeName, valueNode.asText());
                    }
                    if (hasNull) {
                        filterArray.addObject().putObject("bool").putObject("must_not").putObject("exists").put("field", attributeName);
                    }
                } else if (attributeValueNode.isNull()) {
                    queryNode.putObject("bool").putObject("must_not").putObject("exists").put("field", attributeName);
                } else {
                    queryNode.putObject("term").put(attributeName, attributeValue);
                }
                break;

            case POLICY_FILTER_CRITERIA_NOT_EQUALS:
                if (arrayNode != null) {
                    ObjectNode boolNode = queryNode.putObject("bool");
                    if (hasNull) {
                        boolNode.putArray("filter").addObject().putObject("exists").put("field", attributeName);
                    }
                    if (nonNullValues.size() > 0) {
                        boolNode.putObject("must_not").putObject("terms").set(attributeName, nonNullValues);
                    }
                } else if (attributeValueNode.isNull()) {
                    queryNode.putObject("exists").put("field", attributeName);
                } else {
                    queryNode.putObject("bool").putObject("must_not").putObject("term").put(attributeName, attributeValue);
                }
                break;

            case POLICY_FILTER_CRITERIA_STARTS_WITH:
                if (attributeValueNode.isNull()) { // Cannot do prefix search on null values
                    return null;
                } else {
                    queryNode.putObject("prefix").put(attributeName, attributeValue);
                }
                break;

            case POLICY_FILTER_CRITERIA_ENDS_WITH:
                if (attributeValueNode.isNull()) {
                    return null;
                } else {
                    queryNode.putObject("wildcard").put(attributeName, "*" + attributeValue);
                }
                break;

            case POLICY_FILTER_CRITERIA_IN:
                if (arrayNode != null) {
                    if (hasNull && hasNonNull) {
                        ArrayNode shouldArray = queryNode.putObject("bool").putArray("should");
                        shouldArray.addObject().putObject("bool").putObject("must_not").putObject("exists").put("field", attributeName);
                        shouldArray.addObject().putObject("terms").set(attributeName, nonNullValues);
                    } else if (hasNull) {
                        queryNode.putObject("bool").putObject("must_not").putObject("exists").put("field", attributeName);
                    } else if (hasNonNull) {
                        queryNode.putObject("terms").set(attributeName, nonNullValues);
                    } else {
                        return null;
                    }
                } else if (attributeValueNode.isNull()) {
                    queryNode.putObject("bool").putObject("must_not").putObject("exists").put("field", attributeName);
                } else {
                    queryNode.putObject("term").put(attributeName, attributeValue);
                }
                break;

            case POLICY_FILTER_CRITERIA_NOT_IN:
                if (arrayNode != null) {
                    if (hasNull && hasNonNull) {
                        ObjectNode boolNode = queryNode.putObject("bool");
                        boolNode.putArray("filter").addObject().putObject("exists").put("field", attributeName);
                        boolNode.putObject("must_not").putObject("terms").set(attributeName, nonNullValues);
                    } else if (hasNull) {
                        queryNode.putObject("exists").put("field", attributeName);
                    } else if (hasNonNull) {
                        queryNode.putObject("bool").putObject("must_not").putObject("terms").set(attributeName, nonNullValues);
                    } else {
                        return null;
                    }
                } else if (attributeValueNode.isNull()) {
                    queryNode.putObject("exists").put("field", attributeName);
                } else {
                    queryNode.putObject("bool").putObject("must_not").putObject("term").put(attributeName, attributeValue);
                }
                break;

            default: LOG.warn("Found unknown operator {}", operator);
        }
        return queryNode;
    }


    public static JsonNode parseFilterJSON(String policyFilterCriteria, String rootKey) {
        JsonNode filterCriteriaNode = null;
        if (!StringUtils.isEmpty(policyFilterCriteria)) {
            try {
                filterCriteriaNode = mapper.readTree(policyFilterCriteria);
            } catch (JsonProcessingException e) {
                LOG.error("ABAC_AUTH: parsing filterCriteria failed, filterCriteria={}", policyFilterCriteria);
            }
        }

        if (filterCriteriaNode != null) {
            return StringUtils.isNotEmpty(rootKey) ? filterCriteriaNode.get(rootKey) : filterCriteriaNode;
        }
        return null;
    }

    private static String getConditionClause(String condition) {
        return POLICY_FILTER_CRITERIA_AND.equals(condition) ? "filter" : "should";
    }
}
