package org.apache.atlas.web.integration;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.TestcontainersExtension;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for validating large payload handling in the bulk API.
 * 
 * Tests cover:
 * 1. Large text fields (description, userDescription) on Table entity - ~95KB each
 * 2. Large arrays of PopularityInsights structs - 100 structs with ~1KB queries each
 * 3. Large SQL definition on View entity - ~95KB SQL text
 * 
 * Related to Linear ticket MS-513: Leangraph - Test with large asset size
 *
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestcontainersExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LargePayloadIntegrationTest extends AtlasDockerIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(LargePayloadIntegrationTest.class);

    // Configuration constants
    private static final int TEXT_SIZE_KB = 95;  // Just under the 100K character limit
    private static final int STRUCT_COUNT = 100;
    private static final int QUERY_SIZE_KB = 95;

    // Paragraphs for generating realistic text content
    private static final String[] TEXT_PARAGRAPHS = {
        "This is a test description for validating large text field storage in Atlas metastore. ",
        "The purpose of this test is to ensure that the system can handle substantial text payloads. ",
        "Data governance and metadata management require robust handling of various content sizes. ",
        "Asset descriptions may contain detailed documentation, usage guidelines, and technical specifications. ",
        "Quality assurance testing validates system behavior under different load conditions. "
    };

    private String encodedAuth;

    private String getEncodedAuth() {
        if (encodedAuth == null) {
            String auth = "admin:admin";
            encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
        }
        return encodedAuth;
    }

    /**
     * Test Case 1: Create Table entity with large description and userDescription fields.
     * Verifies that ~95KB text fields are stored and retrieved correctly.
     */
    @Test
    @Order(1)
    void testLargeTextFields() throws Exception {
        LOG.info(">> testLargeTextFields - Starting test with {}KB text fields", TEXT_SIZE_KB);

        // Generate large text content
        String description = generateLargeText(TEXT_SIZE_KB, "[DESCRIPTION] ");
        String userDescription = generateLargeText(TEXT_SIZE_KB, "[USER_DESCRIPTION] ");
        String qualifiedName = "large_text_table_" + UUID.randomUUID().toString();

        LOG.info("Generated description length: {} chars", description.length());
        LOG.info("Generated userDescription length: {} chars", userDescription.length());

        // Create JSON payload - escape the text properly
        String payload = String.format("""
            {
                "entities": [
                    {
                        "typeName": "Table",
                        "attributes": {
                            "qualifiedName": "%s",
                            "name": "LargeTextTestTable",
                            "description": "%s",
                            "userDescription": "%s"
                        }
                    }
                ]
            }""", qualifiedName, escapeJson(description), escapeJson(userDescription));

        LOG.info("Payload size: {} KB", payload.length() / 1024);

        // Create the entity
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL + "/entity/bulk"))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", "Basic " + getEncodedAuth())
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(60))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        LOG.info("Create response status: {}", response.statusCode());
        assertEquals(200, response.statusCode(), "Entity creation should succeed");

        // Parse response to get GUID
        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);
        assertNotNull(result.get("mutatedEntities"), "Should have mutatedEntities");
        assertTrue(result.get("mutatedEntities").has("CREATE"), "Should have CREATE");
        
        String createdGuid = result.get("mutatedEntities").get("CREATE").get(0).get("guid").asText();
        LOG.info("Created entity GUID: {}", createdGuid);

        // Wait for sync
        Thread.sleep(2000);

        // Retrieve and verify
        HttpRequest getRequest = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL + "/entity/guid/" + createdGuid))
                .header("Accept", "application/json")
                .header("Authorization", "Basic " + getEncodedAuth())
                .GET()
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> getResponse = httpClient.send(getRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, getResponse.statusCode(), "GET should succeed");

        ObjectNode entityResult = mapper.readValue(getResponse.body(), ObjectNode.class);
        ObjectNode attributes = (ObjectNode) entityResult.get("entity").get("attributes");
        
        String storedDescription = attributes.get("description").asText();
        String storedUserDescription = attributes.get("userDescription").asText();

        LOG.info("Stored description length: {} chars", storedDescription.length());
        LOG.info("Stored userDescription length: {} chars", storedUserDescription.length());

        assertEquals(description.length(), storedDescription.length(), "Description length should match");
        assertEquals(userDescription.length(), storedUserDescription.length(), "UserDescription length should match");
        assertEquals(description, storedDescription, "Description content should match");
        assertEquals(userDescription, storedUserDescription, "UserDescription content should match");

        LOG.info("<< testLargeTextFields - PASSED");
    }

    /**
     * Test Case 2: Create Table entity with large array of PopularityInsights structs.
     * Verifies that 100+ structs with ~1KB recordQuery each are stored correctly.
     */
    @Test
    @Order(2)
    void testLargeStructArray() throws Exception {
        LOG.info(">> testLargeStructArray - Starting test with {} structs", STRUCT_COUNT);

        String qualifiedName = "large_struct_table_" + UUID.randomUUID().toString();

        // Build struct array JSON
        StringBuilder structsJson = new StringBuilder("[");
        for (int i = 0; i < STRUCT_COUNT; i++) {
            if (i > 0) structsJson.append(",");
            String recordQuery = generateLargeSqlQuery(1); // 1KB per query
            structsJson.append(String.format("""
                {
                    "typeName": "PopularityInsights",
                    "attributes": {
                        "recordUser": "user_%d@example.com",
                        "recordQuery": "%s",
                        "recordQueryDuration": %d,
                        "recordQueryCount": %d,
                        "recordTotalUserCount": %d,
                        "recordComputeCost": %.2f,
                        "recordMaxComputeCost": %.2f,
                        "recordComputeCostUnit": "%s",
                        "recordLastTimestamp": %d,
                        "recordWarehouse": "warehouse_%d"
                    }
                }""", 
                i, escapeJson(recordQuery), i * 100L, (long)(i + 1), (long)((i % 10) + 1),
                i * 0.5f, i * 1.5f, (i % 2 == 0 ? "CREDITS" : "BYTES"),
                System.currentTimeMillis() - (i * 1000L), i % 5));
        }
        structsJson.append("]");

        String payload = String.format("""
            {
                "entities": [
                    {
                        "typeName": "Table",
                        "attributes": {
                            "qualifiedName": "%s",
                            "name": "LargeStructTestTable",
                            "sourceReadPopularQueryRecordList": %s
                        }
                    }
                ]
            }""", qualifiedName, structsJson.toString());

        LOG.info("Payload size: {} KB", payload.length() / 1024);

        // Create the entity
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL + "/entity/bulk"))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", "Basic " + getEncodedAuth())
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(120))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        LOG.info("Create response status: {}", response.statusCode());
        assertEquals(200, response.statusCode(), "Entity creation should succeed");

        // Parse response to get GUID
        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);
        String createdGuid = result.get("mutatedEntities").get("CREATE").get(0).get("guid").asText();
        LOG.info("Created entity GUID: {}", createdGuid);

        // Wait for sync
        Thread.sleep(2000);

        // Retrieve and verify
        HttpRequest getRequest = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL + "/entity/guid/" + createdGuid))
                .header("Accept", "application/json")
                .header("Authorization", "Basic " + getEncodedAuth())
                .GET()
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> getResponse = httpClient.send(getRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, getResponse.statusCode(), "GET should succeed");

        ObjectNode entityResult = mapper.readValue(getResponse.body(), ObjectNode.class);
        ArrayNode storedStructs = (ArrayNode) entityResult.get("entity").get("attributes")
                .get("sourceReadPopularQueryRecordList");

        assertNotNull(storedStructs, "Stored structs should not be null");
        assertEquals(STRUCT_COUNT, storedStructs.size(), "Struct count should match");

        // Verify sample struct content
        ObjectNode firstStruct = (ObjectNode) storedStructs.get(0);
        String firstUser = firstStruct.get("attributes").get("recordUser").asText();
        assertTrue(firstUser.startsWith("user_"), "First user should start with 'user_'");

        LOG.info("<< testLargeStructArray - PASSED with {} structs verified", storedStructs.size());
    }

    /**
     * Test Case 3: Create View entity with large SQL definition field.
     * Verifies that ~95KB SQL definition is stored and retrieved correctly.
     */
    @Test
    @Order(3)
    void testLargeSqlDefinition() throws Exception {
        LOG.info(">> testLargeSqlDefinition - Starting test with {}KB SQL definition", QUERY_SIZE_KB);

        String sqlDefinition = generateLargeSqlQuery(QUERY_SIZE_KB);
        String qualifiedName = "large_definition_view_" + UUID.randomUUID().toString();

        LOG.info("Generated SQL definition length: {} chars", sqlDefinition.length());

        String payload = String.format("""
            {
                "entities": [
                    {
                        "typeName": "View",
                        "attributes": {
                            "qualifiedName": "%s",
                            "name": "LargeDefinitionView",
                            "definition": "%s"
                        }
                    }
                ]
            }""", qualifiedName, escapeJson(sqlDefinition));

        LOG.info("Payload size: {} KB", payload.length() / 1024);

        // Create the entity
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL + "/entity/bulk"))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", "Basic " + getEncodedAuth())
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(60))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        LOG.info("Create response status: {}", response.statusCode());
        assertEquals(200, response.statusCode(), "Entity creation should succeed");

        // Parse response to get GUID
        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);
        String createdGuid = result.get("mutatedEntities").get("CREATE").get(0).get("guid").asText();
        LOG.info("Created entity GUID: {}", createdGuid);

        // Wait for sync
        Thread.sleep(2000);

        // Retrieve and verify
        HttpRequest getRequest = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL + "/entity/guid/" + createdGuid))
                .header("Accept", "application/json")
                .header("Authorization", "Basic " + getEncodedAuth())
                .GET()
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> getResponse = httpClient.send(getRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, getResponse.statusCode(), "GET should succeed");

        ObjectNode entityResult = mapper.readValue(getResponse.body(), ObjectNode.class);
        String storedDefinition = entityResult.get("entity").get("attributes").get("definition").asText();

        LOG.info("Stored definition length: {} chars", storedDefinition.length());
        assertEquals(sqlDefinition.length(), storedDefinition.length(), "Definition length should match");
        assertEquals(sqlDefinition, storedDefinition, "Definition content should match");

        LOG.info("<< testLargeSqlDefinition - PASSED");
    }

    // ==================== Helper Methods ====================

    /**
     * Generate a large text string of approximately the specified size in KB.
     */
    private String generateLargeText(int sizeKb, String prefix) {
        int targetSize = sizeKb * 1024;
        StringBuilder sb = new StringBuilder(targetSize + 100);
        sb.append(prefix);

        int paragraphIndex = 0;
        while (sb.length() < targetSize) {
            sb.append(TEXT_PARAGRAPHS[paragraphIndex % TEXT_PARAGRAPHS.length]);
            sb.append("[Block ").append(sb.length() / 1024).append("KB] ");
            paragraphIndex++;
        }

        return sb.substring(0, targetSize);
    }

    /**
     * Generate a large SQL query string of approximately the specified size in KB.
     */
    private String generateLargeSqlQuery(int sizeKb) {
        int targetSize = sizeKb * 1024;
        StringBuilder sb = new StringBuilder(targetSize + 100);

        sb.append("SELECT t1.id, t1.name, t1.description FROM main_table t1 ");
        sb.append("LEFT JOIN category_table t2 ON t1.category_id = t2.id ");
        sb.append("WHERE t1.status = 'active' ");

        int conditionNum = 0;
        while (sb.length() < targetSize) {
            sb.append(String.format("AND t1.col_%d = 'val_%d' ", conditionNum, conditionNum));
            conditionNum++;
        }

        return sb.substring(0, targetSize);
    }

    /**
     * Escape special characters for JSON string.
     */
    private String escapeJson(String text) {
        if (text == null) return null;
        return text
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
    }
}
