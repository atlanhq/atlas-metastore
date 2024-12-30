package org.apache.atlas.web.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.rest.DiscoveryREST;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;

public class ModelUtil {

    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.ModelUtil");
    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryREST.class);

    private static final String BUSINESS_DATE = Constants.MODEL_BUSINESS_DATE;
    private static final String EXPIRED_BUSINESS_DATE =  Constants.MODEL_EXPIRED_AT_BUSINESS_DATE;
    private static final String LESSER_THAN_EQUAL_TO = "lte";
    private static final String SYSTEM_DATE = Constants.MODEL_SYSTEM_DATE;
    private static final String EXPIRED_SYSTEM_DATE = Constants.MODEL_EXPIRED_AT_SYSTEM_DATE;
    private static final String NAMESPACE = Constants.MODEL_NAMESPACE;

    /***
     * combines user query/dsl along with business parameters
     *
     * creates query as following :
     * {"query":{"bool":{"must":[{"bool":{"filter":[{"match":{"namespace":"{namespace}"}},{"bool":{"must":[{"range":{"businessDate":{"lte":"businessDate"}}},{"bool":{"should":[{"range":{"expiredAtBusinessDate":{"gt":"{businessDate}"}}},{"bool":{"must_not":[{"exists":{"field":"expiredAtBusiness"}}]}}],"minimum_should_match":1}}]}}]}},{"wrapper":{"query":"user query"}}]}}}
     * @param namespace
     * @param businessDate
     * @param dslString
     * @return
     */
    public static String createQueryStringUsingFiltersAndUserDSL(final String namespace,
                                                           final String businessDate,
                                                           final String systemDate,
                                                           final String dslString) {
        try {
            AtlasPerfMetrics.MetricRecorder addBusinessFiltersToSearchQueryMetric = RequestContext.get().startMetricRecord("createQueryStringUsingFiltersAndUserDSL");
            // Create an ObjectMapper instance
            ObjectMapper objectMapper = new ObjectMapper();

            // Create the root 'query' node
            ObjectNode rootNode = objectMapper.createObjectNode();
            ObjectNode queryNode = objectMapper.createObjectNode();
            ObjectNode boolNode = objectMapper.createObjectNode();
            ArrayNode mustArray = objectMapper.createArrayNode();

            // Create the first 'bool' object inside 'must'
            ObjectNode firstBoolNode = objectMapper.createObjectNode();
            ObjectNode filterBoolNode = objectMapper.createObjectNode();
            ArrayNode filterArray = objectMapper.createArrayNode();

            // Create 'match' object
            ObjectNode matchNode = objectMapper.createObjectNode();
            matchNode.put(NAMESPACE.concat(".keyword"), namespace);

            // Add 'match' object to filter
            ObjectNode matchWrapper = objectMapper.createObjectNode();
            matchWrapper.set("term", matchNode);
            filterArray.add(matchWrapper);

            // add 'businessDateValidation'
            ObjectNode businessDateWrapper = dateValidation(businessDate, true, objectMapper);
            filterArray.add(businessDateWrapper);

            // add 'systemDateValidation'
            if (!StringUtils.isEmpty(systemDate)) {
                ObjectNode systemDateWrapper = dateValidation(systemDate, false, objectMapper);
                filterArray.add(systemDateWrapper);
            }

            // Add filter to firstBool
            filterBoolNode.set("filter", filterArray);
            firstBoolNode.set("bool", filterBoolNode);

            // Add firstBool to must array
            mustArray.add(firstBoolNode);

            // process user query
            if (!StringUtils.isEmpty(dslString)) {
                JsonNode node = new ObjectMapper().readTree(dslString);
                JsonNode userQueryNode = node.get("query");
                ObjectNode wrapperNode = objectMapper.createObjectNode();
                String userQueryString = userQueryNode.toString();
                String userQueryBase64 = Base64.getEncoder().encodeToString(userQueryString.getBytes());
                wrapperNode.put("query", userQueryBase64);
                // Add wrapper to must array
                ObjectNode wrapperWrapper = objectMapper.createObjectNode();
                wrapperWrapper.set("wrapper", wrapperNode);
                mustArray.add(wrapperWrapper);
            }


            // Add must array to bool node
            boolNode.set("must", mustArray);

            // Add bool to query
            queryNode.set("bool", boolNode);

            rootNode.set("query", queryNode);

            // Print the JSON representation of the query
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
        } catch (Exception e) {
            LOG.error("Error -> createQueryStringUsingFiltersAndUserDSL!", e);
        }
        return "";
    }

    private static ObjectNode dateValidation(final String date, final boolean isBusinessDate, ObjectMapper objectMapper) {

        String condition = LESSER_THAN_EQUAL_TO, dateType = BUSINESS_DATE, expiredDateType = EXPIRED_BUSINESS_DATE;

        if (!isBusinessDate) {
            dateType = SYSTEM_DATE;
            expiredDateType = EXPIRED_SYSTEM_DATE;
        }
        // Create the nested 'bool' object inside filter
        ObjectNode nestedBoolNode = objectMapper.createObjectNode();
        ArrayNode nestedMustArray = objectMapper.createArrayNode();
        ObjectNode rangeBusinessDateNode = objectMapper.createObjectNode();
        rangeBusinessDateNode.put(condition, date);

        // Add 'range' object to nestedMust
        ObjectNode rangeBusinessDateWrapper = objectMapper.createObjectNode();
        rangeBusinessDateWrapper.set("range", objectMapper.createObjectNode().set(dateType, rangeBusinessDateNode));
        nestedMustArray.add(rangeBusinessDateWrapper);


        // Create 'bool' object for 'should'
        ObjectNode shouldBoolNodeWrapper = objectMapper.createObjectNode();
        ObjectNode shouldBoolNode = objectMapper.createObjectNode();
        ArrayNode shouldArray = objectMapper.createArrayNode();

        // Create 'range' object for 'expiredAtBusinessDate'
        ObjectNode rangeExpiredAtNode = objectMapper.createObjectNode();
        rangeExpiredAtNode.put("gt", date);

        // Add 'range' object to should array
        ObjectNode rangeExpiredAtWrapper = objectMapper.createObjectNode();
        rangeExpiredAtWrapper.set("range", objectMapper.createObjectNode().set(expiredDateType, rangeExpiredAtNode));
        shouldArray.add(rangeExpiredAtWrapper);

        // add 'term' object to should array
        ObjectNode termNode = objectMapper.createObjectNode();
        termNode.put(expiredDateType, 0);
        ObjectNode termNodeWrapper = objectMapper.createObjectNode();
        termNodeWrapper.set("term", termNode);
        shouldArray.add(termNodeWrapper);

        // Add 'should' to should array
        shouldBoolNode.set("should", shouldArray);
        shouldBoolNode.put("minimum_should_match", 1);
        shouldBoolNodeWrapper.set("bool", shouldBoolNode);

        // Add shouldBoolNodeWrapper to nestedMust
        nestedMustArray.add(shouldBoolNodeWrapper);

        // Add nestedMust to nestedBool
        nestedBoolNode.set("must", nestedMustArray);

        // Add nestedBool to filter
        ObjectNode nestedBoolWrapper = objectMapper.createObjectNode();
        nestedBoolWrapper.set("bool", nestedBoolNode);
        return nestedBoolWrapper;
    }
}
