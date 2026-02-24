/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.util;

import org.apache.atlas.AtlasConfiguration;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Utility class for generating deterministic IDs based on entity attributes.
 * Ensures two Atlas instances with same input will generate identical IDs.
 *
 * This enables running multiple Atlas instances that can generate consistent
 * GUIDs and QualifiedNames for the same entities, allowing for failover
 * scenarios without ID mismatches.
 *
 * Deterministic ID generation can be enabled/disabled via configuration:
 * atlas.deterministic.id.generation.enabled=true/false (default: false)
 *
 * Note: Access control entities (AuthPolicy, Persona, Purpose, Stakeholder) use
 * 22-character NanoIds for backward compatibility with existing data.
 */
public final class DeterministicIdUtils {

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
    private static final char[] NANOID_ALPHABET =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

    /** Default NanoId size for most entities */
    public static final int NANOID_SIZE_DEFAULT = 21;

    /** NanoId size for access control entities (AuthPolicy, Persona, Purpose, Stakeholder) for backward compatibility */
    public static final int NANOID_SIZE_ACCESS_CONTROL = 22;

    /**
     * Time bucket size in seconds for deterministic ID generation when duplicates are allowed.
     *
     * This value is chosen to balance two requirements:
     * 1. Small enough that two separate duplicate creation requests will fall into different buckets
     *    (humans cannot create two entities in under 5 seconds)
     * 2. Large enough to account for clock skew between mirrored Atlas instances
     *    (cloud NTP keeps clocks within milliseconds, so 5 seconds is more than sufficient)
     *
     * When two Atlas instances receive the same mirrored request, they will process it within
     * the same 5-second bucket, generating identical IDs. When a user creates two entities
     * with duplicate names, they will be in different time buckets, generating unique IDs.
     */
    public static final int TIME_BUCKET_SIZE_SECONDS = 5;

    private DeterministicIdUtils() {
        // Utility class - prevent instantiation
    }

    /**
     * Check if deterministic ID generation is enabled via configuration.
     *
     * @return true if deterministic IDs should be generated, false otherwise
     */
    public static boolean isDeterministicIdGenerationEnabled() {
        return AtlasConfiguration.DETERMINISTIC_ID_GENERATION_ENABLED.getBoolean();
    }

    /**
     * Generate a deterministic UUID-formatted GUID from input components.
     * Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
     *
     * @param components Variable number of string components to hash
     * @return A deterministic UUID-formatted string
     */
    static String generateGuid(String... components) {
        String hash = sha256Hash(components);
        // Format as UUID: 8-4-4-4-12
        return String.format("%s-%s-%s-%s-%s",
            hash.substring(0, 8),
            hash.substring(8, 12),
            hash.substring(12, 16),
            hash.substring(16, 20),
            hash.substring(20, 32));
    }

    /**
     * Generate a GUID, using deterministic generation if enabled, or random UUID if disabled.
     *
     * @param components Variable number of string components to hash (used only if deterministic generation is enabled)
     * @return A UUID-formatted string (deterministic or random based on configuration)
     */
    public static String getGuid(String... components) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateGuid(components);
        }
        return UUID.randomUUID().toString();
    }

    /**
     * Generate a deterministic NanoId-like string from input components.
     * Length: 21 characters (matching NanoId default)
     *
     * @param components Variable number of string components to hash
     * @return A deterministic 21-character alphanumeric string
     */
    static String generateNanoId(String... components) {
        return generateNanoId(NANOID_SIZE_DEFAULT, components);
    }

    /**
     * Generate a deterministic NanoId-like string from input components with specified size.
     *
     * @param size The length of the NanoId to generate
     * @param components Variable number of string components to hash
     * @return A deterministic alphanumeric string of the specified length
     */
    static String generateNanoId(int size, String... components) {
        byte[] hashBytes = sha256HashBytes(components);
        StringBuilder result = new StringBuilder(size);

        for (int i = 0; i < size; i++) {
            int index = (hashBytes[i % hashBytes.length] & 0xFF) % NANOID_ALPHABET.length;
            result.append(NANOID_ALPHABET[index]);
        }
        return result.toString();
    }

    /**
     * Generate a NanoId, using deterministic generation if enabled, or random NanoId if disabled.
     *
     * @param components Variable number of string components to hash (used only if deterministic generation is enabled)
     * @return A 21-character alphanumeric string (deterministic or random based on configuration)
     */
    public static String getNanoId(String... components) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateNanoId(components);
        }
        return NanoIdUtils.randomNanoId();
    }

    /**
     * Generate a NanoId with specified size, using deterministic generation if enabled, or random NanoId if disabled.
     *
     * @param size The length of the NanoId to generate
     * @param components Variable number of string components to hash (used only if deterministic generation is enabled)
     * @return An alphanumeric string of the specified length (deterministic or random based on configuration)
     */
    public static String getNanoId(int size, String... components) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateNanoId(size, components);
        }
        return NanoIdUtils.randomNanoId(size);
    }

    // ==================== TIME BUCKET METHODS ====================
    // These methods are used for entities that allow duplicate names.
    // The time bucket ensures:
    // 1. Same request on mirrored instances -> same ID (within bucket window)
    // 2. Different requests (even with duplicate names) -> different IDs (different time buckets)

    /**
     * Get the current time bucket value.
     * The bucket changes every TIME_BUCKET_SIZE_SECONDS seconds.
     *
     * @return The current time bucket as a string
     */
    static String getTimeBucket() {
        long timeBucket = Instant.now().getEpochSecond() / TIME_BUCKET_SIZE_SECONDS;
        return String.valueOf(timeBucket);
    }

    /**
     * Generate a deterministic NanoId with time bucket for entities that allow duplicate names.
     * The time bucket ensures uniqueness across separate creation requests while maintaining
     * determinism for the same request processed by mirrored Atlas instances.
     *
     * @param components Variable number of string components to hash
     * @return A deterministic 21-character alphanumeric string
     */
    static String generateNanoIdWithTimeBucket(String... components) {
        return generateNanoIdWithTimeBucket(NANOID_SIZE_DEFAULT, components);
    }

    /**
     * Generate a deterministic NanoId with time bucket and specified size.
     *
     * @param size The length of the NanoId to generate
     * @param components Variable number of string components to hash
     * @return A deterministic alphanumeric string of the specified length
     */
    static String generateNanoIdWithTimeBucket(int size, String... components) {
        String[] allComponents = Arrays.copyOf(components, components.length + 1);
        allComponents[components.length] = getTimeBucket();
        return generateNanoId(size, allComponents);
    }

    /**
     * Get a NanoId with time bucket, using deterministic generation if enabled, or random NanoId if disabled.
     * Use this for entities that allow duplicate names (e.g., Persona, Purpose, DataDomain, Collection).
     *
     * @param components Variable number of string components to hash (used only if deterministic generation is enabled)
     * @return A 21-character alphanumeric string (deterministic with time bucket, or random based on configuration)
     */
    public static String getNanoIdWithTimeBucket(String... components) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateNanoIdWithTimeBucket(components);
        }
        return NanoIdUtils.randomNanoId();
    }

    /**
     * Get a NanoId with time bucket and specified size, using deterministic generation if enabled.
     * Use this for entities that allow duplicate names.
     *
     * @param size The length of the NanoId to generate
     * @param components Variable number of string components to hash (used only if deterministic generation is enabled)
     * @return An alphanumeric string of the specified length (deterministic with time bucket, or random based on configuration)
     */
    public static String getNanoIdWithTimeBucket(int size, String... components) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateNanoIdWithTimeBucket(size, components);
        }
        return NanoIdUtils.randomNanoId(size);
    }

    // ==================== END TIME BUCKET METHODS ====================

    /**
     * Generate entity GUID from type name and qualified name.
     *
     * @param typeName The entity type name
     * @param qualifiedName The entity's qualified name
     * @return A deterministic UUID-formatted GUID
     */
    static String generateEntityGuid(String typeName, String qualifiedName) {
        return generateGuid("entity", typeName, qualifiedName);
    }

    /**
     * Generate entity GUID from type name and unique attributes (for shell entities).
     *
     * @param typeName The entity type name
     * @param uniqueAttributes Map of unique attribute names to values
     * @return A deterministic UUID-formatted GUID
     */
    static String generateEntityGuid(String typeName, Map<String, Object> uniqueAttributes) {
        List<String> components = new ArrayList<>();
        components.add("entity");
        components.add(typeName);

        if (uniqueAttributes != null && !uniqueAttributes.isEmpty()) {
            // Sort keys to ensure deterministic ordering
            List<String> sortedKeys = new ArrayList<>(uniqueAttributes.keySet());
            Collections.sort(sortedKeys);

            for (String key : sortedKeys) {
                Object value = uniqueAttributes.get(key);
                components.add(key);
                components.add(value != null ? value.toString() : "");
            }
        }

        return generateGuid(components.toArray(new String[0]));
    }

    /**
     * Generate TypeDef GUID from type name and optional service type.
     *
     * @param typeName The type definition name
     * @param serviceType The service type (optional)
     * @return A deterministic UUID-formatted GUID
     */
    static String generateTypeDefGuid(String typeName, String serviceType) {
        return generateGuid("typedef", typeName,
            StringUtils.isNotEmpty(serviceType) ? serviceType : "atlas");
    }

    /**
     * Generate relationship GUID from relationship type and end entity GUIDs.
     * End GUIDs are sorted to ensure consistency regardless of direction.
     *
     * @param relationshipType The relationship type name
     * @param end1Guid GUID of the first end entity
     * @param end2Guid GUID of the second end entity
     * @return A deterministic UUID-formatted GUID
     */
    static String generateRelationshipGuid(String relationshipType, String end1Guid, String end2Guid) {
        // Sort end GUIDs to ensure consistency regardless of direction
        String first = end1Guid.compareTo(end2Guid) <= 0 ? end1Guid : end2Guid;
        String second = end1Guid.compareTo(end2Guid) <= 0 ? end2Guid : end1Guid;
        return generateGuid("relationship", relationshipType, first, second);
    }

    /**
     * Get entity GUID, using deterministic generation if enabled, or random UUID if disabled.
     *
     * @param typeName The entity type name
     * @param qualifiedName The entity's qualified name
     * @return A UUID-formatted GUID (deterministic or random based on configuration)
     */
    public static String getEntityGuid(String typeName, String qualifiedName) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateEntityGuid(typeName, qualifiedName);
        }
        return UUID.randomUUID().toString();
    }

    /**
     * Get entity GUID from unique attributes, using deterministic generation if enabled.
     *
     * @param typeName The entity type name
     * @param uniqueAttributes Map of unique attribute names to values
     * @return A UUID-formatted GUID (deterministic or random based on configuration)
     */
    public static String getEntityGuid(String typeName, Map<String, Object> uniqueAttributes) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateEntityGuid(typeName, uniqueAttributes);
        }
        return UUID.randomUUID().toString();
    }

    /**
     * Get TypeDef GUID, using deterministic generation if enabled, or random UUID if disabled.
     *
     * @param typeName The type definition name
     * @param serviceType The service type (optional)
     * @return A UUID-formatted GUID (deterministic or random based on configuration)
     */
    public static String getTypeDefGuid(String typeName, String serviceType) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateTypeDefGuid(typeName, serviceType);
        }
        return UUID.randomUUID().toString();
    }

    /**
     * Get relationship GUID, using deterministic generation if enabled, or random UUID if disabled.
     *
     * @param relationshipType The relationship type name
     * @param end1Guid GUID of the first end entity
     * @param end2Guid GUID of the second end entity
     * @return A UUID-formatted GUID (deterministic or random based on configuration)
     */
    public static String getRelationshipGuid(String relationshipType, String end1Guid, String end2Guid) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateRelationshipGuid(relationshipType, end1Guid, end2Guid);
        }
        return UUID.randomUUID().toString();
    }

    /**
     * Generate Glossary QualifiedName from glossary name.
     *
     * @param glossaryName The glossary name
     * @return A deterministic 21-character NanoId
     */
    static String generateGlossaryQN(String glossaryName) {
        return generateNanoId("glossary", glossaryName);
    }

    /**
     * Get Glossary QualifiedName, using deterministic generation if enabled.
     *
     * @param glossaryName The glossary name
     * @return A 21-character NanoId (deterministic or random based on configuration)
     */
    public static String getGlossaryQN(String glossaryName) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateGlossaryQN(glossaryName);
        }
        return NanoIdUtils.randomNanoId();
    }

    /**
     * Generate Term QualifiedName (prefix) from term name, parent category QN, and anchor glossary QN.
     *
     * @param termName The term name
     * @param parentCategoryQN The parent category's qualified name (can be null if term is not in a category)
     * @param anchorGlossaryQN The anchor glossary's qualified name
     * @return A deterministic 21-character NanoId
     */
    static String generateTermQN(String termName, String parentCategoryQN, String anchorGlossaryQN) {
        return generateNanoId("term", termName, StringUtils.defaultString(parentCategoryQN), anchorGlossaryQN);
    }

    /**
     * Get Term QualifiedName prefix, using deterministic generation if enabled.
     *
     * @param termName The term name
     * @param parentCategoryQN The parent category's qualified name (can be null if term is not in a category)
     * @param anchorGlossaryQN The anchor glossary's qualified name
     * @return A 21-character NanoId (deterministic or random based on configuration)
     */
    public static String getTermQN(String termName, String parentCategoryQN, String anchorGlossaryQN) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateTermQN(termName, parentCategoryQN, anchorGlossaryQN);
        }
        return NanoIdUtils.randomNanoId();
    }

    /**
     * Generate Category QualifiedName (prefix) from category name, parent hierarchy, and anchor glossary QN.
     *
     * @param categoryName The category name
     * @param parentCategoryQN The parent category's qualified name (can be null)
     * @param anchorGlossaryQN The anchor glossary's qualified name
     * @return A deterministic 21-character NanoId
     */
    static String generateCategoryQN(String categoryName, String parentCategoryQN, String anchorGlossaryQN) {
        return generateNanoId("category", categoryName,
            StringUtils.defaultString(parentCategoryQN), anchorGlossaryQN);
    }

    /**
     * Get Category QualifiedName prefix, using deterministic generation if enabled.
     *
     * @param categoryName The category name
     * @param parentCategoryQN The parent category's qualified name (can be null)
     * @param anchorGlossaryQN The anchor glossary's qualified name
     * @return A 21-character NanoId (deterministic or random based on configuration)
     */
    public static String getCategoryQN(String categoryName, String parentCategoryQN, String anchorGlossaryQN) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateCategoryQN(categoryName, parentCategoryQN, anchorGlossaryQN);
        }
        return NanoIdUtils.randomNanoId();
    }

    /**
     * Generate DataDomain QualifiedName suffix from domain name and parent domain QN.
     * Uses time bucket because domain names can be duplicated.
     *
     * @param domainName The domain name
     * @param parentDomainQN The parent domain's qualified name (can be empty for root)
     * @return A deterministic 21-character NanoId
     */
    static String generateDomainQN(String domainName, String parentDomainQN) {
        return generateNanoIdWithTimeBucket("domain", domainName, StringUtils.defaultString(parentDomainQN));
    }

    /**
     * Get DataDomain QualifiedName suffix, using deterministic generation if enabled.
     * Uses time bucket because domain names can be duplicated.
     *
     * @param domainName The domain name
     * @param parentDomainQN The parent domain's qualified name (can be empty for root)
     * @return A 21-character NanoId (deterministic or random based on configuration)
     */
    public static String getDomainQN(String domainName, String parentDomainQN) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateDomainQN(domainName, parentDomainQN);
        }
        return NanoIdUtils.randomNanoId();
    }

    /**
     * Generate DataProduct QualifiedName suffix from product name and parent domain QN.
     *
     * @param productName The product name
     * @param parentDomainQN The parent domain's qualified name
     * @return A deterministic 21-character NanoId
     */
    static String generateProductQN(String productName, String parentDomainQN) {
        return generateNanoId("product", productName, parentDomainQN);
    }

    /**
     * Get DataProduct QualifiedName suffix, using deterministic generation if enabled.
     *
     * @param productName The product name
     * @param parentDomainQN The parent domain's qualified name
     * @return A 21-character NanoId (deterministic or random based on configuration)
     */
    public static String getProductQN(String productName, String parentDomainQN) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateProductQN(productName, parentDomainQN);
        }
        return NanoIdUtils.randomNanoId();
    }

    /**
     * Generate Persona/Purpose/Stakeholder QualifiedName suffix from name and context.
     * Uses 22-character NanoId for backward compatibility with existing access control entities.
     * Uses time bucket because these entity types allow duplicate names.
     *
     * @param type The access control type (persona, purpose, stakeholder, stakeholdertitle)
     * @param name The entity name
     * @param contextQN The context qualified name (tenantId or domainQN)
     * @return A deterministic 22-character NanoId
     */
    static String generateAccessControlQN(String type, String name, String contextQN) {
        return generateNanoIdWithTimeBucket(NANOID_SIZE_ACCESS_CONTROL, type, name, contextQN);
    }

    /**
     * Get Persona/Purpose/Stakeholder QualifiedName suffix, using deterministic generation if enabled.
     * Uses 22-character NanoId for backward compatibility with existing access control entities.
     * Uses time bucket because these entity types allow duplicate names.
     *
     * @param type The access control type (persona, purpose, stakeholder, stakeholdertitle)
     * @param name The entity name
     * @param contextQN The context qualified name (tenantId or domainQN)
     * @return A 22-character NanoId (deterministic or random based on configuration)
     */
    public static String getAccessControlQN(String type, String name, String contextQN) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateAccessControlQN(type, name, contextQN);
        }
        return NanoIdUtils.randomNanoId(NANOID_SIZE_ACCESS_CONTROL);
    }

    /**
     * Generate Query/Folder/Collection QualifiedName suffix.
     * Uses time bucket because these entity types allow duplicate names.
     *
     * @param type The resource type (collection, folder, query)
     * @param name The resource name
     * @param parentQN The parent qualified name (can be empty for collections)
     * @param userName The user name
     * @return A deterministic 21-character NanoId
     */
    static String generateQueryResourceQN(String type, String name, String parentQN, String userName) {
        return generateNanoIdWithTimeBucket(type, name, StringUtils.defaultString(parentQN), userName);
    }

    /**
     * Get Query/Folder/Collection QualifiedName suffix, using deterministic generation if enabled.
     * Uses time bucket because these entity types allow duplicate names.
     *
     * @param type The resource type (collection, folder, query)
     * @param name The resource name
     * @param parentQN The parent qualified name (can be empty for collections)
     * @param userName The user name
     * @return A 21-character NanoId (deterministic or random based on configuration)
     */
    public static String getQueryResourceQN(String type, String name, String parentQN, String userName) {
        if (isDeterministicIdGenerationEnabled()) {
            return generateQueryResourceQN(type, name, parentQN, userName);
        }
        return NanoIdUtils.randomNanoId();
    }

    /**
     * Generate Policy QualifiedName suffix from policy name and parent entity QN.
     * Uses 22-character NanoId for backward compatibility with existing policy entities.
     * Uses time bucket because policy names can be duplicated.
     *
     * @param policyName The policy name
     * @param parentEntityQN The parent entity's qualified name
     * @return A deterministic 22-character NanoId
     */
    static String generatePolicyQN(String policyName, String parentEntityQN) {
        return generateNanoIdWithTimeBucket(NANOID_SIZE_ACCESS_CONTROL, "policy", policyName, parentEntityQN);
    }

    /**
     * Get Policy QualifiedName suffix, using deterministic generation if enabled.
     * Uses 22-character NanoId for backward compatibility with existing policy entities.
     * Uses time bucket because policy names can be duplicated.
     *
     * @param policyName The policy name
     * @param parentEntityQN The parent entity's qualified name
     * @return A 22-character NanoId (deterministic or random based on configuration)
     */
    public static String getPolicyQN(String policyName, String parentEntityQN) {
        if (isDeterministicIdGenerationEnabled()) {
            return generatePolicyQN(policyName, parentEntityQN);
        }
        return NanoIdUtils.randomNanoId(NANOID_SIZE_ACCESS_CONTROL);
    }

    private static String sha256Hash(String... components) {
        return bytesToHex(sha256HashBytes(components));
    }

    private static byte[] sha256HashBytes(String... components) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String component : components) {
                if (component != null) {
                    digest.update(component.getBytes(StandardCharsets.UTF_8));
                    digest.update((byte) 0); // Separator to avoid concatenation collisions
                }
            }
            return digest.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xFF;
            hexChars[i * 2] = HEX_CHARS[v >>> 4];
            hexChars[i * 2 + 1] = HEX_CHARS[v & 0x0F];
        }
        return new String(hexChars);
    }
}
