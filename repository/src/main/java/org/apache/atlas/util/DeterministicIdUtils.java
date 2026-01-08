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

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Utility class for generating deterministic IDs based on entity attributes.
 * Ensures two Atlas instances with same input will generate identical IDs.
 *
 * This enables running multiple Atlas instances that can generate consistent
 * GUIDs and QualifiedNames for the same entities, allowing for failover
 * scenarios without ID mismatches.
 */
public final class DeterministicIdUtils {

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
    private static final char[] NANOID_ALPHABET =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    private static final int NANOID_SIZE = 21;

    private DeterministicIdUtils() {
        // Utility class - prevent instantiation
    }

    /**
     * Generate a deterministic UUID-formatted GUID from input components.
     * Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
     *
     * @param components Variable number of string components to hash
     * @return A deterministic UUID-formatted string
     */
    public static String generateGuid(String... components) {
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
     * Generate a deterministic NanoId-like string from input components.
     * Length: 21 characters (matching NanoId default)
     *
     * @param components Variable number of string components to hash
     * @return A deterministic 21-character alphanumeric string
     */
    public static String generateNanoId(String... components) {
        byte[] hashBytes = sha256HashBytes(components);
        StringBuilder result = new StringBuilder(NANOID_SIZE);

        for (int i = 0; i < NANOID_SIZE; i++) {
            int index = (hashBytes[i % hashBytes.length] & 0xFF) % NANOID_ALPHABET.length;
            result.append(NANOID_ALPHABET[index]);
        }
        return result.toString();
    }

    /**
     * Generate entity GUID from type name and qualified name.
     *
     * @param typeName The entity type name
     * @param qualifiedName The entity's qualified name
     * @return A deterministic UUID-formatted GUID
     */
    public static String generateEntityGuid(String typeName, String qualifiedName) {
        return generateGuid("entity", typeName, qualifiedName);
    }

    /**
     * Generate entity GUID from type name and unique attributes (for shell entities).
     *
     * @param typeName The entity type name
     * @param uniqueAttributes Map of unique attribute names to values
     * @return A deterministic UUID-formatted GUID
     */
    public static String generateEntityGuid(String typeName, Map<String, Object> uniqueAttributes) {
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
    public static String generateTypeDefGuid(String typeName, String serviceType) {
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
    public static String generateRelationshipGuid(String relationshipType, String end1Guid, String end2Guid) {
        // Sort end GUIDs to ensure consistency regardless of direction
        String first = end1Guid.compareTo(end2Guid) <= 0 ? end1Guid : end2Guid;
        String second = end1Guid.compareTo(end2Guid) <= 0 ? end2Guid : end1Guid;
        return generateGuid("relationship", relationshipType, first, second);
    }

    /**
     * Generate Glossary QualifiedName from glossary name.
     *
     * @param glossaryName The glossary name
     * @return A deterministic 21-character NanoId
     */
    public static String generateGlossaryQN(String glossaryName) {
        return generateNanoId("glossary", glossaryName);
    }

    /**
     * Generate Term QualifiedName (prefix) from term name and anchor glossary QN.
     *
     * @param termName The term name
     * @param anchorGlossaryQN The anchor glossary's qualified name
     * @return A deterministic 21-character NanoId
     */
    public static String generateTermQN(String termName, String anchorGlossaryQN) {
        return generateNanoId("term", termName, anchorGlossaryQN);
    }

    /**
     * Generate Category QualifiedName (prefix) from category name, parent hierarchy, and anchor glossary QN.
     *
     * @param categoryName The category name
     * @param parentCategoryQN The parent category's qualified name (can be null)
     * @param anchorGlossaryQN The anchor glossary's qualified name
     * @return A deterministic 21-character NanoId
     */
    public static String generateCategoryQN(String categoryName, String parentCategoryQN, String anchorGlossaryQN) {
        return generateNanoId("category", categoryName,
            StringUtils.defaultString(parentCategoryQN), anchorGlossaryQN);
    }

    /**
     * Generate DataDomain QualifiedName suffix from domain name and parent domain QN.
     *
     * @param domainName The domain name
     * @param parentDomainQN The parent domain's qualified name (can be empty for root)
     * @return A deterministic 21-character NanoId
     */
    public static String generateDomainQN(String domainName, String parentDomainQN) {
        return generateNanoId("domain", domainName, StringUtils.defaultString(parentDomainQN));
    }

    /**
     * Generate DataProduct QualifiedName suffix from product name and parent domain QN.
     *
     * @param productName The product name
     * @param parentDomainQN The parent domain's qualified name
     * @return A deterministic 21-character NanoId
     */
    public static String generateProductQN(String productName, String parentDomainQN) {
        return generateNanoId("product", productName, parentDomainQN);
    }

    /**
     * Generate Persona/Purpose/Stakeholder QualifiedName suffix from name and context.
     *
     * @param type The access control type (persona, purpose, stakeholder, stakeholdertitle)
     * @param name The entity name
     * @param contextQN The context qualified name (tenantId or domainQN)
     * @return A deterministic 21-character NanoId
     */
    public static String generateAccessControlQN(String type, String name, String contextQN) {
        return generateNanoId(type, name, contextQN);
    }

    /**
     * Generate Query/Folder/Collection QualifiedName suffix.
     *
     * @param type The resource type (collection, folder, query)
     * @param name The resource name
     * @param parentQN The parent qualified name (can be empty for collections)
     * @param userName The user name
     * @return A deterministic 21-character NanoId
     */
    public static String generateQueryResourceQN(String type, String name, String parentQN, String userName) {
        return generateNanoId(type, name, StringUtils.defaultString(parentQN), userName);
    }

    /**
     * Generate Policy QualifiedName suffix from policy name and parent entity QN.
     *
     * @param policyName The policy name
     * @param parentEntityQN The parent entity's qualified name
     * @return A deterministic 21-character NanoId
     */
    public static String generatePolicyQN(String policyName, String parentEntityQN) {
        return generateNanoId("policy", policyName, parentEntityQN);
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
