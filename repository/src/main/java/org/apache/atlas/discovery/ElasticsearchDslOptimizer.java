package org.apache.atlas.discovery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Generic Elasticsearch DSL Query Optimizer
 *
 * Optimizes queries through multiple optimization pipelines:
 * 1. Structure simplification (flatten nested bools)
 * 2. Empty bool elimination
 * 3. Multiple terms consolidation
 * 4. Array deduplication
 * 5. Multi-match consolidation
 * 6. Regexp simplification
 * 7. Aggregation optimization
 * 8. Wildcard consolidation
 * 9. QualifiedName hierarchy optimization
 * 10. Duplicate removal and consolidation
 * 11. Filter context optimization
 * 12. Function score optimization
 */
public class ElasticsearchDslOptimizer {

    private final ObjectMapper objectMapper;
    private final List<OptimizationRule> optimizationRules;
    private final OptimizationMetrics metrics;
    private static final ElasticsearchDslOptimizer INSTANCE = new ElasticsearchDslOptimizer();

    public ElasticsearchDslOptimizer() {
        this.objectMapper = new ObjectMapper();
        this.optimizationRules = Arrays.asList(
                new StructureSimplificationRule(),
                new EmptyBoolEliminationRule(),
                new MultipleTermsConsolidationRule(),
                new ArrayDeduplicationRule(),
                new MultiMatchConsolidationRule(),
                new RegexpSimplificationRule(),
                new AggregationOptimizationRule(),
                new QualifiedNameHierarchyRule(), // Move this before wildcard consolidation
                new WildcardConsolidationRule(),
                new FilterStructureOptimizationRule(), // Add new rule
                new DuplicateRemovalRule(),
                new FilterContextRule(),
                new FunctionScoreOptimizationRule(),
                new DuplicateFilterRemovalRule()
        );
        this.metrics = new OptimizationMetrics();
    }

    public static ElasticsearchDslOptimizer getInstance() {
        return INSTANCE;
    }


    /**
     * Rule 10: Filter Structure Optimization
     * Reorganizes nested filter structures into cleaner arrays
     */
    private class FilterStructureOptimizationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "FilterStructureOptimization";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::optimizeFilterStructure);
        }

        private JsonNode optimizeFilterStructure(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("bool")) {
                ObjectNode boolNode = (ObjectNode) objectNode.get("bool");

                // Convert nested filter bool structure to flat filter array
                if (boolNode.has("filter") && boolNode.get("filter").isObject()) {
                    JsonNode filterObj = boolNode.get("filter");

                    if (filterObj.has("bool")) {
                        ArrayNode newFilterArray = extractFiltersFromNestedBool(filterObj.get("bool"));

                        // Also add other clauses from the main bool as filters
                        if (boolNode.has("should")) {
                            newFilterArray.add(createBoolShouldWrapper(boolNode.get("should")));
                            boolNode.remove("should");
                        }
                        if (boolNode.has("must_not")) {
                            boolNode.set("must_not", boolNode.get("must_not"));
                        }
                        if (boolNode.has("minimum_should_match")) {
                            boolNode.set("minimum_should_match", boolNode.get("minimum_should_match"));
                        }

                        boolNode.set("filter", newFilterArray);
                    }
                }
            }

            return objectNode;
        }

        private ArrayNode extractFiltersFromNestedBool(JsonNode boolNode) {
            ArrayNode filters = objectMapper.createArrayNode();

            if (boolNode.has("must") && boolNode.get("must").isArray()) {
                for (JsonNode mustItem : boolNode.get("must")) {
                    extractFiltersRecursively(mustItem, filters);
                }
            }

            return filters;
        }

        private void extractFiltersRecursively(JsonNode node, ArrayNode filters) {
            if (node.has("bool")) {
                JsonNode boolContent = node.get("bool");
                if (boolContent.has("must") && boolContent.get("must").isArray()) {
                    for (JsonNode mustItem : boolContent.get("must")) {
                        extractFiltersRecursively(mustItem, filters);
                    }
                } else if (boolContent.has("should")) {
                    // Keep bool should structure intact
                    filters.add(node);
                } else {
                    filters.add(node);
                }
            } else if (node.has("term") || node.has("terms") || node.has("wildcard") || node.has("regexp")) {
                filters.add(node);
            } else {
                filters.add(node);
            }
        }

        private ObjectNode createBoolShouldWrapper(JsonNode shouldClause) {
            ObjectNode wrapper = objectMapper.createObjectNode();
            ObjectNode boolWrapper = objectMapper.createObjectNode();
            boolWrapper.set("should", shouldClause);
            wrapper.set("bool", boolWrapper);
            return wrapper;
        }
    }

    /**
     * Main optimization method - applies all optimization rules
     */
    public OptimizationResult optimizeQuery(String queryJson) {
        try {
            JsonNode query = objectMapper.readTree(queryJson);
            JsonNode originalQuery = query.deepCopy();

            metrics.startOptimization(query);

            // Apply optimization rules in sequence
            for (OptimizationRule rule : optimizationRules) {
                query = rule.apply(query);
                metrics.recordRuleApplication(rule.getName());
            }

            String optimizedQueryJson = objectMapper.writeValueAsString(query);
            OptimizationMetrics.Result result = metrics.finishOptimization(originalQuery, query);

            return new OptimizationResult(optimizedQueryJson, result);

        } catch (Exception e) {
            throw new RuntimeException("Failed to optimize query", e);
        }
    }

    /**
     * Rule 1: Structure Simplification
     * Flattens unnecessary nested bool queries and removes single-item wrappers
     */
    private class StructureSimplificationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "StructureSimplification";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::optimizeBoolStructure);
        }

        private JsonNode optimizeBoolStructure(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            // Optimize bool queries
            if (objectNode.has("bool")) {
                ObjectNode boolNode = (ObjectNode) objectNode.get("bool");

                // Flatten nested bool structures
                for (String clause : Arrays.asList("must", "should", "filter", "must_not")) {
                    if (boolNode.has(clause)) {
                        JsonNode clauseNode = boolNode.get(clause);

                        // Handle single objects that are not arrays
                        if (clauseNode.isObject() && !clauseNode.isArray()) {
                            // Convert single object to array for consistent processing
                            ArrayNode arrayNode = objectMapper.createArrayNode();
                            arrayNode.add(clauseNode);
                            boolNode.set(clause, arrayNode);
                            clauseNode = arrayNode;
                        }

                        if (clauseNode.isArray()) {
                            ArrayNode array = (ArrayNode) clauseNode;

                            // Remove empty arrays
                            if (array.size() == 0) {
                                boolNode.remove(clause);
                                continue;
                            }

                            // Flatten deeply nested bool structures
                            ArrayNode flattenedArray = flattenNestedBoolArray(array, clause);
                            if (flattenedArray.size() != array.size() || !flattenedArray.equals(array)) {
                                boolNode.set(clause, flattenedArray);
                            }
                        }
                    }
                }

                // Convert filter object to filter array if needed
                if (boolNode.has("filter") && boolNode.get("filter").isObject() && !boolNode.get("filter").isArray()) {
                    JsonNode filterObj = boolNode.get("filter");
                    if (filterObj.has("bool")) {
                        // Extract content from nested bool and flatten
                        JsonNode innerBool = filterObj.get("bool");
                        if (innerBool.has("must") && innerBool.get("must").isArray()) {
                            ArrayNode mustArray = (ArrayNode) innerBool.get("must");
                            ArrayNode flattenedFilters = flattenFilterStructure(mustArray);
                            boolNode.set("filter", flattenedFilters);
                        }
                    }
                }
            }

            return objectNode;
        }

        private ArrayNode flattenNestedBoolArray(ArrayNode array, String clauseType) {
            ArrayNode result = objectMapper.createArrayNode();

            for (JsonNode item : array) {
                if (item.has("bool")) {
                    JsonNode boolContent = item.get("bool");

                    // If this bool only has the same clause type, flatten it
                    if (boolContent.has(clauseType) && boolContent.size() == 1) {
                        JsonNode innerClause = boolContent.get(clauseType);
                        if (innerClause.isArray()) {
                            for (JsonNode innerItem : innerClause) {
                                result.add(innerItem);
                            }
                        } else {
                            result.add(innerClause);
                        }
                    } else {
                        result.add(item);
                    }
                } else {
                    result.add(item);
                }
            }

            return result;
        }

        private ArrayNode flattenFilterStructure(ArrayNode mustArray) {
            ArrayNode result = objectMapper.createArrayNode();

            for (JsonNode mustItem : mustArray) {
                if (mustItem.has("bool") && mustItem.get("bool").has("must")) {
                    JsonNode innerMust = mustItem.get("bool").get("must");
                    if (innerMust.isArray()) {
                        for (JsonNode innerItem : innerMust) {
                            result.add(innerItem);
                        }
                    } else {
                        result.add(innerMust);
                    }
                } else {
                    result.add(mustItem);
                }
            }

            return result;
        }
    }

    /**
     * Rule 2: Empty Bool Elimination
     * Removes empty bool query objects that provide no filtering logic
     */
    private class EmptyBoolEliminationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "EmptyBoolElimination";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::removeEmptyBoolQueries);
        }

        private JsonNode removeEmptyBoolQueries(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            // Check for empty bool objects
            if (objectNode.has("bool")) {
                JsonNode boolNode = objectNode.get("bool");

                if (boolNode.isObject() && boolNode.size() == 0) {
                    // Return match_all query instead of empty object
                    ObjectNode matchAll = objectMapper.createObjectNode();
                    matchAll.set("match_all", objectMapper.createObjectNode());
                    return matchAll;
                }
            }

            // Remove empty bool objects from arrays
            if (objectNode.has("bool")) {
                ObjectNode boolObject = (ObjectNode) objectNode.get("bool");

                for (String clause : Arrays.asList("must", "should", "filter", "must_not")) {
                    if (boolObject.has(clause) && boolObject.get(clause).isArray()) {
                        ArrayNode array = (ArrayNode) boolObject.get(clause);
                        ArrayNode filteredArray = objectMapper.createArrayNode();

                        for (JsonNode item : array) {
                            // Skip empty bool objects
                            if (!(item.has("bool") && item.get("bool").size() == 0)) {
                                filteredArray.add(item);
                            }
                        }

                        if (filteredArray.size() != array.size()) {
                            boolObject.set(clause, filteredArray);
                        }
                    }
                }
            }

            return objectNode;
        }
    }

    /**
     * Rule 3: Multiple Terms Consolidation
     * Merges multiple terms queries on the same field into a single terms query
     */
    private class MultipleTermsConsolidationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "MultipleTermsConsolidation";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::consolidateMultipleTermsQueries);
        }

        private JsonNode consolidateMultipleTermsQueries(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("bool")) {
                ObjectNode boolNode = (ObjectNode) objectNode.get("bool");

                // Consolidate terms queries in must_not, must, should, filter clauses
                for (String clause : Arrays.asList("must", "should", "filter", "must_not")) {
                    if (boolNode.has(clause) && boolNode.get(clause).isArray()) {
                        ArrayNode array = (ArrayNode) boolNode.get(clause);
                        ArrayNode consolidatedArray = consolidateTermsInArray(array);

                        if (consolidatedArray.size() != array.size()) {
                            boolNode.set(clause, consolidatedArray);
                        }
                    }
                }
            }

            return objectNode;
        }

        private ArrayNode consolidateTermsInArray(ArrayNode array) {
            Map<String, Set<String>> termsByField = new HashMap<>();
            ArrayNode nonTermsQueries = objectMapper.createArrayNode();

            // Group terms queries by field
            for (JsonNode item : array) {
                if (item.has("terms")) {
                    JsonNode termsNode = item.get("terms");
                    Iterator<String> fieldNames = termsNode.fieldNames();
                    if (fieldNames.hasNext()) {
                        String field = fieldNames.next();
                        JsonNode valuesNode = termsNode.get(field);

                        if (valuesNode.isArray()) {
                            Set<String> values = new LinkedHashSet<>();
                            for (JsonNode value : valuesNode) {
                                values.add(value.asText());
                            }
                            termsByField.computeIfAbsent(field, k -> new LinkedHashSet<>()).addAll(values);
                        }
                    }
                } else if (item.has("term")) {
                    // Convert single term to terms
                    JsonNode termNode = item.get("term");
                    Iterator<String> fieldNames = termNode.fieldNames();
                    if (fieldNames.hasNext()) {
                        String field = fieldNames.next();
                        String value = termNode.get(field).asText();
                        termsByField.computeIfAbsent(field, k -> new LinkedHashSet<>()).add(value);
                    }
                } else {
                    nonTermsQueries.add(item);
                }
            }

            // Create consolidated terms queries
            ArrayNode result = objectMapper.createArrayNode();

            // Add consolidated terms queries
            for (Map.Entry<String, Set<String>> entry : termsByField.entrySet()) {
                String field = entry.getKey();
                Set<String> allValues = entry.getValue();

                ObjectNode termsQuery = objectMapper.createObjectNode();
                ObjectNode termsObject = objectMapper.createObjectNode();
                ArrayNode valuesArray = objectMapper.createArrayNode();
                allValues.forEach(valuesArray::add);
                termsObject.set(field, valuesArray);
                termsQuery.set("terms", termsObject);

                result.add(termsQuery);
            }

            // Add non-terms queries back
            for (JsonNode nonTerms : nonTermsQueries) {
                result.add(nonTerms);
            }

            return result;
        }
    }

    /**
     * Rule 4: Array Deduplication
     * Removes duplicate values from terms arrays and other array fields
     */
    private class ArrayDeduplicationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "ArrayDeduplication";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::deduplicateArrays);
        }

        private JsonNode deduplicateArrays(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            // Deduplicate terms arrays
            if (objectNode.has("terms")) {
                JsonNode termsNode = objectNode.get("terms");
                if (termsNode.isObject()) {
                    ObjectNode termsObject = (ObjectNode) termsNode;
                    Iterator<String> fieldNames = termsObject.fieldNames();
                    List<String> fieldsToUpdate = new ArrayList<>();
                    Map<String, ArrayNode> newArrays = new HashMap<>();

                    fieldNames.forEachRemaining(fieldName -> {
                        JsonNode arrayNode = termsObject.get(fieldName);
                        if (arrayNode.isArray()) {
                            ArrayNode deduplicatedArray = deduplicateArray((ArrayNode) arrayNode);
                            if (deduplicatedArray.size() != arrayNode.size()) {
                                fieldsToUpdate.add(fieldName);
                                newArrays.put(fieldName, deduplicatedArray);
                            }
                        }
                    });

                    // Update arrays that had duplicates
                    for (String field : fieldsToUpdate) {
                        termsObject.set(field, newArrays.get(field));
                    }
                }
            }

            return objectNode;
        }

        private ArrayNode deduplicateArray(ArrayNode arrayNode) {
            Set<String> seen = new LinkedHashSet<>(); // Preserve order
            ArrayNode result = objectMapper.createArrayNode();

            for (JsonNode item : arrayNode) {
                String value = item.asText();
                if (!seen.contains(value)) {
                    seen.add(value);
                    result.add(item);
                }
            }

            return result;
        }
    }

    /**
     * Rule 5: Multi-Match Consolidation
     * Consolidates multiple multi_match queries with same search term
     */
    private class MultiMatchConsolidationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "MultiMatchConsolidation";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::consolidateMultiMatch);
        }

        private JsonNode consolidateMultiMatch(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            // Look for bool should clauses with multiple multi_match queries
            if (objectNode.has("bool") && objectNode.get("bool").has("should")) {
                JsonNode shouldArray = objectNode.get("bool").get("should");

                if (shouldArray.isArray() && shouldArray.size() > 1) {
                    Map<String, List<MultiMatchQuery>> multiMatchGroups = extractMultiMatchQueries((ArrayNode) shouldArray);

                    if (!multiMatchGroups.isEmpty()) {
                        ArrayNode optimizedShouldArray = optimizeMultiMatchQueries((ArrayNode) shouldArray, multiMatchGroups);
                        ((ObjectNode) objectNode.get("bool")).set("should", optimizedShouldArray);
                    }
                }
            }

            return objectNode;
        }

        private Map<String, List<MultiMatchQuery>> extractMultiMatchQueries(ArrayNode shouldArray) {
            Map<String, List<MultiMatchQuery>> groups = new HashMap<>();

            for (JsonNode clause : shouldArray) {
                if (clause.has("multi_match")) {
                    JsonNode multiMatchNode = clause.get("multi_match");
                    if (multiMatchNode.has("query")) {
                        String queryText = multiMatchNode.get("query").asText();
                        MultiMatchQuery mmq = new MultiMatchQuery(clause, queryText);
                        groups.computeIfAbsent(queryText, k -> new ArrayList<>()).add(mmq);
                    }
                }
            }

            // Only return groups with multiple queries
            return groups.entrySet().stream()
                    .filter(entry -> entry.getValue().size() > 1)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        private ArrayNode optimizeMultiMatchQueries(ArrayNode originalArray, Map<String, List<MultiMatchQuery>> groups) {
            ArrayNode result = objectMapper.createArrayNode();
            Set<JsonNode> processedQueries = new HashSet<>();

            // Add non-multi_match queries and ungrouped multi_match queries
            for (JsonNode clause : originalArray) {
                if (clause.has("multi_match")) {
                    JsonNode multiMatchNode = clause.get("multi_match");
                    String queryText = multiMatchNode.has("query") ? multiMatchNode.get("query").asText() : "";

                    if (!groups.containsKey(queryText) && !processedQueries.contains(clause)) {
                        result.add(clause);
                        processedQueries.add(clause);
                    }
                } else {
                    result.add(clause);
                }
            }

            // Add consolidated multi_match queries
            for (Map.Entry<String, List<MultiMatchQuery>> entry : groups.entrySet()) {
                String queryText = entry.getKey();
                List<MultiMatchQuery> queries = entry.getValue();

                // Mark all original queries as processed
                queries.forEach(q -> processedQueries.add(q.originalNode));

                // Create consolidated query
                MultiMatchQuery consolidated = consolidateMultiMatchQueries(queries, queryText);
                result.add(consolidated.originalNode);
            }

            return result;
        }

        private MultiMatchQuery consolidateMultiMatchQueries(List<MultiMatchQuery> queries, String queryText) {
            // Combine all fields and find highest boost
            Set<String> allFields = new LinkedHashSet<>();
            double maxBoost = 1.0;
            String type = "best_fields"; // default

            for (MultiMatchQuery query : queries) {
                allFields.addAll(query.fields);
                maxBoost = Math.max(maxBoost, query.boost);

                // Extract type if available
                JsonNode multiMatch = query.originalNode.get("multi_match");
                if (multiMatch.has("type")) {
                    type = multiMatch.get("type").asText();
                }
            }

            // Create consolidated multi_match query
            ObjectNode consolidated = objectMapper.createObjectNode();
            ObjectNode multiMatch = objectMapper.createObjectNode();

            multiMatch.put("query", queryText);
            multiMatch.put("type", type);
            if (maxBoost != 1.0) {
                multiMatch.put("boost", maxBoost);
            }

            ArrayNode fieldsArray = objectMapper.createArrayNode();
            allFields.forEach(fieldsArray::add);
            multiMatch.set("fields", fieldsArray);

            consolidated.set("multi_match", multiMatch);

            return new MultiMatchQuery(consolidated, queryText);
        }
    }

    /**
     * Rule 6: Regexp Simplification
     * Simplifies overly complex regexp patterns
     */
    private class RegexpSimplificationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "RegexpSimplification";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::simplifyRegexp);
        }

        private JsonNode simplifyRegexp(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("regexp")) {
                JsonNode regexpNode = objectNode.get("regexp");
                if (regexpNode.isObject()) {
                    ObjectNode regexpObject = (ObjectNode) regexpNode;
                    Iterator<String> fieldNames = regexpObject.fieldNames();
                    List<String> fieldsToUpdate = new ArrayList<>();
                    Map<String, String> newPatterns = new HashMap<>();

                    fieldNames.forEachRemaining(fieldName -> {
                        JsonNode valueNode = regexpObject.get(fieldName);
                        String pattern;

                        if (valueNode.isObject() && valueNode.has("value")) {
                            pattern = valueNode.get("value").asText();
                        } else if (valueNode.isTextual()) {
                            pattern = valueNode.asText();
                        } else {
                            return;
                        }

                        String simplifiedPattern = simplifyRegexpPattern(pattern);
                        if (!simplifiedPattern.equals(pattern)) {
                            fieldsToUpdate.add(fieldName);
                            newPatterns.put(fieldName, simplifiedPattern);
                        }
                    });

                    // Update patterns that were simplified
                    for (String field : fieldsToUpdate) {
                        JsonNode valueNode = regexpObject.get(field);
                        String newPattern = newPatterns.get(field);

                        if (valueNode.isObject()) {
                            ((ObjectNode) valueNode).put("value", newPattern);
                        } else {
                            regexpObject.put(field, newPattern);
                        }
                    }
                }
            }

            return objectNode;
        }

        private String simplifyRegexpPattern(String pattern) {
            // Simplify overly complex character classes
            if (pattern.contains("[a-zA-Z0-9")) {
                pattern = pattern.replaceAll("\\[a-zA-Z0-9[^\\]]*\\]\\*", "[\\\\w\\\\s\\\\-._\"#%()\\\\[\\\\]]*");
            }

            // Remove redundant .* patterns
            if (pattern.startsWith("[\\w\\s\\-._\"#%()\\[\\]]*") && pattern.contains(".*")) {
                String suffix = pattern.substring(pattern.indexOf(".*") + 2);
                if (!suffix.isEmpty()) {
                    pattern = ".*" + suffix;
                }
            }

            return pattern;
        }
    }

    /**
     * Rule 7: Aggregation Optimization
     * Optimizes repetitive aggregation patterns
     */
    private class AggregationOptimizationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "AggregationOptimization";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::optimizeAggregations);
        }

        private JsonNode optimizeAggregations(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("aggs")) {
                JsonNode aggsNode = objectNode.get("aggs");
                if (aggsNode.isObject() && aggsNode.size() > 10) {
                    ObjectNode optimizedAggs = optimizeRepetitiveAggregations((ObjectNode) aggsNode);
                    objectNode.set("aggs", optimizedAggs);
                }
            }

            return objectNode;
        }

        private ObjectNode optimizeRepetitiveAggregations(ObjectNode aggsNode) {
            ObjectNode result = objectMapper.createObjectNode();

            Iterator<String> fieldNames = aggsNode.fieldNames();
            fieldNames.forEachRemaining(aggName -> {
                JsonNode aggConfig = aggsNode.get(aggName);
                JsonNode optimizedAgg = optimizeSingleAggregation(aggConfig);
                result.set(aggName, optimizedAgg);
            });

            return result;
        }

        private JsonNode optimizeSingleAggregation(JsonNode aggConfig) {
            if (aggConfig.has("filter") && aggConfig.get("filter").has("bool")) {
                ObjectNode optimized = (ObjectNode) aggConfig.deepCopy();
                JsonNode filterBool = optimized.get("filter").get("bool");

                if (filterBool.has("should") && filterBool.get("should").isArray()) {
                    ArrayNode shouldArray = (ArrayNode) filterBool.get("should");
                    ArrayNode optimizedShould = objectMapper.createArrayNode();

                    for (JsonNode shouldClause : shouldArray) {
                        optimizedShould.add(shouldClause);
                    }

                    ((ObjectNode) filterBool).set("should", optimizedShould);
                }

                return optimized;
            }

            return aggConfig;
        }
    }

    /**
     * Rule 8: Wildcard Consolidation
     * Groups multiple wildcard queries into regexp patterns
     */
    private class WildcardConsolidationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "WildcardConsolidation";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::consolidateWildcards);
        }

        private JsonNode consolidateWildcards(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("bool") && objectNode.get("bool").has("should")) {
                JsonNode shouldArray = objectNode.get("bool").get("should");

                if (shouldArray.isArray() && shouldArray.size() > 3) {
                    List<WildcardPattern> wildcards = extractWildcardPatterns((ArrayNode) shouldArray);

                    if (wildcards.size() > 3) {
                        Map<String, List<WildcardPattern>> groupedByField = wildcards.stream()
                                .collect(Collectors.groupingBy(w -> w.field));

                        boolean modified = false;
                        ArrayNode newShouldArray = objectMapper.createArrayNode();

                        // Add non-wildcard clauses
                        for (JsonNode clause : shouldArray) {
                            if (!clause.has("wildcard")) {
                                newShouldArray.add(clause);
                            }
                        }

                        for (Map.Entry<String, List<WildcardPattern>> entry : groupedByField.entrySet()) {
                            String field = entry.getKey();
                            List<WildcardPattern> patterns = entry.getValue();

                            if (patterns.size() > 3) {
                                // Create regexp consolidation
                                List<String> patternStrings = patterns.stream()
                                        .map(WildcardPattern::extractPattern)
                                        .collect(Collectors.toList());

                                String regexpPattern = createRegexpPattern(patternStrings);

                                ObjectNode regexpNode = objectMapper.createObjectNode();
                                ObjectNode regexpQuery = objectMapper.createObjectNode();
                                regexpQuery.put(field, regexpPattern);
                                regexpNode.set("regexp", regexpQuery);
                                newShouldArray.add(regexpNode);
                                modified = true;
                            } else {
                                // Keep original wildcards if not enough to consolidate
                                for (WildcardPattern pattern : patterns) {
                                    ObjectNode wildcardNode = objectMapper.createObjectNode();
                                    ObjectNode wildcardQuery = objectMapper.createObjectNode();
                                    wildcardQuery.put(pattern.field, pattern.pattern);
                                    wildcardNode.set("wildcard", wildcardQuery);
                                    newShouldArray.add(wildcardNode);
                                }
                            }
                        }

                        if (modified) {
                            ((ObjectNode) objectNode.get("bool")).set("should", newShouldArray);
                        }
                    }
                }
            }

            return objectNode;
        }

        private List<WildcardPattern> extractWildcardPatterns(ArrayNode shouldArray) {
            List<WildcardPattern> patterns = new ArrayList<>();

            for (JsonNode clause : shouldArray) {
                if (clause.has("wildcard")) {
                    JsonNode wildcardNode = clause.get("wildcard");
                    Iterator<String> fieldNames = wildcardNode.fieldNames();
                    if (fieldNames.hasNext()) {
                        String field = fieldNames.next();
                        String pattern = wildcardNode.get(field).asText();
                        patterns.add(new WildcardPattern(field, pattern));
                    }
                }
            }

            return patterns;
        }

        private String createRegexpPattern(List<String> patterns) {
            if (patterns.isEmpty()) return ".*";

            String commonPrefix = findCommonPrefix(patterns);

            if (commonPrefix.length() > 5) {
                List<String> variableParts = patterns.stream()
                        .map(p -> p.substring(commonPrefix.length()))
                        .filter(p -> !p.isEmpty())
                        .collect(Collectors.toList());

                if (!variableParts.isEmpty()) {
                    String alternatives = variableParts.stream()
                            .map(this::escapeRegexSpecialChars)
                            .collect(Collectors.joining("|"));
                    return escapeRegexSpecialChars(commonPrefix) + "(" + alternatives + ").*";
                }
            }

            String alternatives = patterns.stream()
                    .map(this::escapeRegexSpecialChars)
                    .collect(Collectors.joining("|"));
            return ".*(" + alternatives + ").*";
        }

        private String escapeRegexSpecialChars(String input) {
            return input.replaceAll("([\\[\\]\\(\\)\\{\\}\\*\\+\\?\\|\\^\\$\\\\\\.])", "\\\\$1");
        }

        private String findCommonPrefix(List<String> patterns) {
            if (patterns.isEmpty()) return "";

            String first = patterns.get(0);
            int prefixLength = 0;

            for (int i = 0; i < first.length(); i++) {
                char c = first.charAt(i);
                int finalI = i;
                boolean allMatch = patterns.stream().allMatch(p -> finalI < p.length() && p.charAt(finalI) == c);

                if (allMatch) {
                    prefixLength = i + 1;
                } else {
                    break;
                }
            }

            return first.substring(0, prefixLength);
        }
    }

    /**
     * Rule 9: QualifiedName Hierarchy Optimization
     * Converts suffix wildcards to qualifiedNameHierarchy term queries and database term queries
     */
    private class QualifiedNameHierarchyRule implements OptimizationRule {

        @Override
        public String getName() {
            return "QualifiedNameHierarchy";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::optimizeQualifiedNameWildcards);
        }

        private JsonNode optimizeQualifiedNameWildcards(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("wildcard") && objectNode.get("wildcard").has("qualifiedName")) {
                String pattern = objectNode.get("wildcard").get("qualifiedName").asText();

                // Check for database prefix pattern like "default/athena/1731597928/AwsDataCatalog*"
                if (pattern.endsWith("*") && !pattern.startsWith("*") && pattern.length() > 1) {
                    String prefix = pattern.substring(0, pattern.length() - 1);
                    // Standard hierarchy optimization
                    ObjectNode termsNode = objectMapper.createObjectNode();
                    ObjectNode termsQuery = objectMapper.createObjectNode();
                    ArrayNode valuesArray = objectMapper.createArrayNode();
                    valuesArray.add(prefix);
                    termsQuery.set("__qualifiedNameHierarchy", valuesArray);
                    termsNode.set("terms", termsQuery);
                    return termsNode;
                }
            }

            return objectNode;
        }
    }

    /**
     * Rule 11: Duplicate Removal
     * Removes duplicate filters and consolidates similar clauses
     */
    private class DuplicateRemovalRule implements OptimizationRule {

        @Override
        public String getName() {
            return "DuplicateRemoval";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::removeDuplicates);
        }

        private JsonNode removeDuplicates(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("bool")) {
                ObjectNode boolNode = (ObjectNode) objectNode.get("bool");

                for (String clause : Arrays.asList("must", "should", "filter", "must_not")) {
                    if (boolNode.has(clause) && boolNode.get(clause).isArray()) {
                        ArrayNode array = (ArrayNode) boolNode.get(clause);
                        ArrayNode deduplicated = removeDuplicateFromArray(array);
                        if (deduplicated.size() != array.size()) {
                            boolNode.set(clause, deduplicated);
                        }
                    }
                }
            }

            return objectNode;
        }

        private ArrayNode removeDuplicateFromArray(ArrayNode array) {
            Set<String> seen = new LinkedHashSet<>();
            ArrayNode result = objectMapper.createArrayNode();

            for (JsonNode item : array) {
                String itemString = item.toString();
                if (!seen.contains(itemString)) {
                    seen.add(itemString);
                    result.add(item);
                }
            }

            return result;
        }
    }

    /**
     * Rule 12: Filter Context Optimization
     * Moves non-scoring queries to filter context
     */
    private class FilterContextRule implements OptimizationRule {

        @Override
        public String getName() {
            return "FilterContext";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::optimizeFilterContext);
        }

        private JsonNode optimizeFilterContext(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("bool")) {
                ObjectNode boolNode = (ObjectNode) objectNode.get("bool");

                if (boolNode.has("must") && boolNode.get("must").isArray()) {
                    ArrayNode mustArray = (ArrayNode) boolNode.get("must");
                    ArrayNode newMustArray = objectMapper.createArrayNode();
                    ArrayNode filterArray = boolNode.has("filter") && boolNode.get("filter").isArray()
                            ? (ArrayNode) boolNode.get("filter").deepCopy()
                            : objectMapper.createArrayNode();

                    for (JsonNode clause : mustArray) {
                        if (isFilterCandidate(clause)) {
                            filterArray.add(clause);
                        } else {
                            newMustArray.add(clause);
                        }
                    }

                    if (newMustArray.size() > 0) {
                        boolNode.set("must", newMustArray);
                    } else {
                        boolNode.remove("must");
                    }

                    if (filterArray.size() > 0) {
                        boolNode.set("filter", filterArray);
                    }
                }
            }

            return objectNode;
        }

        private boolean isFilterCandidate(JsonNode clause) {
            return clause.has("term") || clause.has("terms") || clause.has("range") ||
                    clause.has("exists") || clause.has("regexp") || clause.has("wildcard");
        }
    }

    /**
     * Rule 13: Function Score Optimization
     * Optimizes function_score queries by removing duplicates and reordering
     */
    private class FunctionScoreOptimizationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "FunctionScoreOptimization";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::optimizeFunctionScore);
        }

        private JsonNode optimizeFunctionScore(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("function_score") && objectNode.get("function_score").has("functions")) {
                ArrayNode functions = (ArrayNode) objectNode.get("function_score").get("functions");

                List<JsonNode> uniqueFunctions = new ArrayList<>();
                Set<String> seenFilters = new LinkedHashSet<>();

                for (JsonNode function : functions) {
                    String filterString = function.has("filter") ? function.get("filter").toString() : "no_filter";
                    if (!seenFilters.contains(filterString)) {
                        seenFilters.add(filterString);
                        uniqueFunctions.add(function);
                    }
                }

                // Sort by weight (descending)
                uniqueFunctions.sort((a, b) -> {
                    double weightA = a.has("weight") ? a.get("weight").asDouble(1.0) : 1.0;
                    double weightB = b.has("weight") ? b.get("weight").asDouble(1.0) : 1.0;
                    return Double.compare(weightB, weightA);
                });

                if (uniqueFunctions.size() != functions.size()) {
                    ArrayNode optimizedFunctions = objectMapper.createArrayNode();
                    uniqueFunctions.forEach(optimizedFunctions::add);
                    ((ObjectNode) objectNode.get("function_score")).set("functions", optimizedFunctions);
                }
            }

            return objectNode;
        }
    }

    /**
     * Rule 14: Duplicate Filter Removal
     * Removes duplicate filters between main query and function_score
     */
    private class DuplicateFilterRemovalRule implements OptimizationRule {

        @Override
        public String getName() {
            return "DuplicateFilterRemoval";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::removeDuplicateFilters);
        }

        private JsonNode removeDuplicateFilters(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("function_score")) {
                JsonNode functionScore = objectNode.get("function_score");

                if (functionScore.has("query") && functionScore.has("functions")) {
                    Set<String> mainQueryFilters = extractMainQueryFilters(functionScore.get("query"));
                    ArrayNode functions = (ArrayNode) functionScore.get("functions");

                    ArrayNode optimizedFunctions = removeDuplicateFunctionFilters(functions, mainQueryFilters);

                    if (optimizedFunctions.size() != functions.size()) {
                        ((ObjectNode) functionScore).set("functions", optimizedFunctions);
                    }
                }
            }

            return objectNode;
        }

        private Set<String> extractMainQueryFilters(JsonNode mainQuery) {
            Set<String> filters = new HashSet<>();

            if (mainQuery.has("bool")) {
                JsonNode boolQuery = mainQuery.get("bool");

                if (boolQuery.has("filter")) {
                    addFiltersFromNode(boolQuery.get("filter"), filters);
                }
                if (boolQuery.has("must")) {
                    addFiltersFromNode(boolQuery.get("must"), filters);
                }
            }

            return filters;
        }

        private void addFiltersFromNode(JsonNode node, Set<String> filters) {
            if (node.isArray()) {
                for (JsonNode item : node) {
                    addFiltersFromNode(item, filters);
                }
            } else if (node.isObject()) {
                if (node.has("bool")) {
                    JsonNode boolNode = node.get("bool");
                    if (boolNode.has("must")) {
                        addFiltersFromNode(boolNode.get("must"), filters);
                    }
                    if (boolNode.has("filter")) {
                        addFiltersFromNode(boolNode.get("filter"), filters);
                    }
                } else if (node.has("term")) {
                    JsonNode termNode = node.get("term");
                    Iterator<String> fieldNames = termNode.fieldNames();
                    fieldNames.forEachRemaining(field -> {
                        String value = termNode.get(field).asText();
                        filters.add("term:" + field + ":" + value);
                    });
                } else if (node.has("terms")) {
                    JsonNode termsNode = node.get("terms");
                    Iterator<String> fieldNames = termsNode.fieldNames();
                    fieldNames.forEachRemaining(field -> {
                        String values = termsNode.get(field).toString();
                        filters.add("terms:" + field + ":" + values);
                    });
                }
            }
        }

        private ArrayNode removeDuplicateFunctionFilters(ArrayNode functions, Set<String> mainQueryFilters) {
            ArrayNode result = objectMapper.createArrayNode();

            for (JsonNode function : functions) {
                if (function.has("filter")) {
                    String functionFilterKey = extractFilterKey(function.get("filter"));

                    if (!mainQueryFilters.contains(functionFilterKey)) {
                        result.add(function);
                    }
                } else {
                    result.add(function);
                }
            }

            return result;
        }

        private String extractFilterKey(JsonNode filter) {
            if (filter.has("match")) {
                JsonNode matchNode = filter.get("match");
                Iterator<String> fieldNames = matchNode.fieldNames();
                if (fieldNames.hasNext()) {
                    String field = fieldNames.next();
                    String value = matchNode.get(field).asText();
                    return "term:" + field + ":" + value;
                }
            } else if (filter.has("term")) {
                JsonNode termNode = filter.get("term");
                Iterator<String> fieldNames = termNode.fieldNames();
                if (fieldNames.hasNext()) {
                    String field = fieldNames.next();
                    String value = termNode.get(field).asText();
                    return "term:" + field + ":" + value;
                }
            } else if (filter.has("terms")) {
                JsonNode termsNode = filter.get("terms");
                Iterator<String> fieldNames = termsNode.fieldNames();
                if (fieldNames.hasNext()) {
                    String field = fieldNames.next();
                    String values = termsNode.get(field).toString();
                    return "terms:" + field + ":"+ values;
                }
            }

            return filter.toString();
        }
    }

    // Helper classes and interfaces

    private interface OptimizationRule {
        String getName();
        JsonNode apply(JsonNode query);
    }

    private static class WildcardPattern {
        final String field;
        final String pattern;

        WildcardPattern(String field, String pattern) {
            this.field = field;
            this.pattern = pattern;
        }

        String extractPattern() {
            return pattern.replaceAll("\\*", "");
        }
    }

    // Helper class for multi-match optimization
    private static class MultiMatchQuery {
        final JsonNode originalNode;
        final String queryText;
        final List<String> fields;
        final double boost;

        MultiMatchQuery(JsonNode node, String queryText) {
            this.originalNode = node;
            this.queryText = queryText;
            this.fields = extractFields(node);
            this.boost = extractBoost(node);
        }

        private List<String> extractFields(JsonNode node) {
            List<String> fields = new ArrayList<>();
            JsonNode multiMatch = node.get("multi_match");
            if (multiMatch.has("fields") && multiMatch.get("fields").isArray()) {
                for (JsonNode field : multiMatch.get("fields")) {
                    fields.add(field.asText());
                }
            }
            return fields;
        }

        private double extractBoost(JsonNode node) {
            JsonNode multiMatch = node.get("multi_match");
            if (multiMatch.has("boost")) {
                return multiMatch.get("boost").asDouble();
            }
            return 1.0;
        }

        double getTotalBoost() {
            return boost * fields.size();
        }

        int getFieldCount() {
            return fields.size();
        }
    }

    private JsonNode traverseAndOptimize(JsonNode node, java.util.function.Function<JsonNode, JsonNode> optimizer) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;

            // First optimize children
            Iterator<String> fieldNames = objectNode.fieldNames();
            List<String> fieldsToUpdate = new ArrayList<>();
            Map<String, JsonNode> newValues = new HashMap<>();

            fieldNames.forEachRemaining(fieldName -> {
                JsonNode childNode = objectNode.get(fieldName);
                JsonNode optimizedChild = traverseAndOptimize(childNode, optimizer);
                if (optimizedChild != childNode) {
                    fieldsToUpdate.add(fieldName);
                    newValues.put(fieldName, optimizedChild);
                }
            });

            // Update modified fields
            for (String field : fieldsToUpdate) {
                objectNode.set(field, newValues.get(field));
            }

            // Then optimize this node
            return optimizer.apply(objectNode);
        } else if (node.isArray()) {
            ArrayNode arrayNode = (ArrayNode) node;
            ArrayNode optimizedArray = objectMapper.createArrayNode();

            for (JsonNode child : arrayNode) {
                optimizedArray.add(traverseAndOptimize(child, optimizer));
            }

            return optimizedArray;
        }

        return node;
    }

    /**
     * Optimization metrics and results
     */
    public static class OptimizationMetrics {
        private long startTime;
        private int originalSize;
        private int originalNesting;
        private final List<String> appliedRules = new ArrayList<>();

        void startOptimization(JsonNode query) {
            startTime = System.currentTimeMillis();
            originalSize = query.toString().length();
            originalNesting = calculateNestingDepth(query);
            appliedRules.clear();
        }

        void recordRuleApplication(String ruleName) {
            appliedRules.add(ruleName);
        }

        Result finishOptimization(JsonNode original, JsonNode optimized) {
            long duration = System.currentTimeMillis() - startTime;
            int optimizedSize = optimized.toString().length();
            int optimizedNesting = calculateNestingDepth(optimized);

            return new Result(
                    originalSize, optimizedSize,
                    originalNesting, optimizedNesting,
                    duration, new ArrayList<>(appliedRules)
            );
        }

        private int calculateNestingDepth(JsonNode node) {
            if (!node.isContainerNode()) return 0;

            int maxDepth = 0;
            if (node.isObject()) {
                for (JsonNode child : node) {
                    maxDepth = Math.max(maxDepth, calculateNestingDepth(child));
                }
            } else if (node.isArray()) {
                for (JsonNode child : node) {
                    maxDepth = Math.max(maxDepth, calculateNestingDepth(child));
                }
            }

            return maxDepth + 1;
        }

        public static class Result {
            public final int originalSize;
            public final int optimizedSize;
            public final int originalNesting;
            public final int optimizedNesting;
            public final long optimizationTime;
            public final List<String> appliedRules;

            Result(int originalSize, int optimizedSize, int originalNesting,
                   int optimizedNesting, long optimizationTime, List<String> appliedRules) {
                this.originalSize = originalSize;
                this.optimizedSize = optimizedSize;
                this.originalNesting = originalNesting;
                this.optimizedNesting = optimizedNesting;
                this.optimizationTime = optimizationTime;
                this.appliedRules = new ArrayList<>(appliedRules);
            }

            public double getSizeReduction() {
                if (originalSize == 0) return 0.0;
                return ((double) (originalSize - optimizedSize) / originalSize) * 100;
            }

            public double getNestingReduction() {
                if (originalNesting == 0) return 0.0;
                return ((double) (originalNesting - optimizedNesting) / originalNesting) * 100;
            }
        }
    }

    public static class OptimizationResult {
        public final String optimizedQuery;
        public final OptimizationMetrics.Result metrics;

        OptimizationResult(String optimizedQuery, OptimizationMetrics.Result metrics) {
            this.optimizedQuery = optimizedQuery;
            this.metrics = metrics;
        }

        public String getOptimizedQuery() {
            return optimizedQuery;
        }

        public void printOptimizationSummary() {
            System.out.println("=== Optimization Summary ===");
            System.out.println("Size reduction: " + String.format("%.1f", metrics.getSizeReduction()) + "%");
            System.out.println("Nesting reduction: " + String.format("%.1f", metrics.getNestingReduction()) + "%");
            System.out.println("Optimization time: " + metrics.optimizationTime + "ms");
            System.out.println("Applied rules: " + String.join(", ", metrics.appliedRules));
        }
    }

    public static void main(String[] args) {
        ElasticsearchDslOptimizer elasticsearchDslOptimizer = new ElasticsearchDslOptimizer();
        // get all json files from a directory
        File dir = new File("/Users/sriram.aravamuthan/Documents/Notes/TestDSLRewrite/");
        File[] files = dir.listFiles((d, name) -> name.endsWith(".json"));
        if (files != null) {
            for (File file : files) {
                try {
                    BufferedInputStream bufferedInputStream = IOUtils.buffer(new FileInputStream(file));
                    //convert to bufferedInputStream to string
                    String jsonString = IOUtils.toString(bufferedInputStream, StandardCharsets.UTF_8);
                    OptimizationResult result = elasticsearchDslOptimizer.optimizeQuery(jsonString);
                    result.printOptimizationSummary();
                    //write the output to another file in same directory with file name as <original_file_name>_optimized.json
                    String outputFileName = file.getName().replace(".json", "_optimized.json");
                    File outputFile = new File(dir, outputFileName);
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
                        writer.write(result.getOptimizedQuery());
                    }
                    System.out.println("Optimized query written to: " + outputFile.getAbsolutePath());
                } catch (IOException e) {
                    System.err.println("Failed to read file " + file.getName() + ": " + e.getMessage());
                }
            }
        } else {
            System.out.println("No JSON files found in the specified directory.");
        }
    }
}