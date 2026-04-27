/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.ApplicationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone replacement for org.janusgraph.util.encoding.LongEncoding.
 * Encodes/decodes long values to/from compact string representation
 * compatible with JanusGraph's Elasticsearch document ID format.
 *
 * The encoding uses base-36 (digits 0-9 then lowercase a-z) matching
 * JanusGraph's actual LongEncoding implementation (janusgraph-driver).
 * This is a direct reimplementation to remove the JanusGraph dependency
 * while maintaining backward compatibility with existing ES document IDs.
 */
public final class LongEncodingUtil {

    private static final Logger LOG = LoggerFactory.getLogger(LongEncodingUtil.class);

    private static final String BASE_SYMBOLS = "0123456789abcdefghijklmnopqrstuvwxyz";
    private static final int    BASE         = BASE_SYMBOLS.length(); // 36

    private LongEncodingUtil() {
        // utility class
    }

    /**
     * Resolved on every call so the value tracks the runtime graphdb backend
     * (StaticConfigStore overlays {@code atlas.graphdb.backend} into
     * {@link ApplicationProperties} after Cassandra reads complete; caching this
     * in a {@code static {}} block snapshots the file value too early and silently
     * encodes vertex IDs against the wrong scheme after a JG↔ZG rollback).
     */
    private static boolean isCassandraBackend() {
        try {
            String backend = ApplicationProperties.get().getString(
                    ApplicationProperties.GRAPHDB_BACKEND_CONF,
                    ApplicationProperties.DEFAULT_GRAPHDB_BACKEND);
            return ApplicationProperties.GRAPHDB_BACKEND_CASSANDRA.equalsIgnoreCase(backend);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Encode a long value to a compact string representation.
     * Matches JanusGraph's LongEncoding.encode() output exactly.
     */
    public static String encode(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Expected non-negative value: " + value);
        }
        if (value == 0) {
            return String.valueOf(BASE_SYMBOLS.charAt(0));
        }

        StringBuilder sb = new StringBuilder();
        while (value > 0) {
            sb.append(BASE_SYMBOLS.charAt((int) (value % BASE)));
            value = value / BASE;
        }
        return sb.reverse().toString();
    }

    /**
     * Decode a string back to a long value.
     * Matches JanusGraph's LongEncoding.decode() output exactly.
     */
    public static long decode(String encoded) {
        long value = 0;
        for (int i = 0; i < encoded.length(); i++) {
            int digit = BASE_SYMBOLS.indexOf(encoded.charAt(i));
            if (digit < 0) {
                throw new IllegalArgumentException("Invalid character in encoded string: " + encoded.charAt(i));
            }
            value = value * BASE + digit;
        }
        return value;
    }

    /**
     * Compute the ES document ID from a vertex ID string.
     *
     * The mapping depends on the graph backend:
     *
     * <b>JanusGraph mode:</b> vertex IDs are numeric longs (e.g., "4096").
     * JanusGraph internally base-36 encodes them for ES doc IDs (e.g., "38g").
     * We replicate that encoding here.
     *
     * <b>CassandraGraph mode:</b> CassandraGraph.syncVerticesToElasticsearch()
     * always uses the vertex ID string as-is for the ES doc _id, regardless of
     * whether the vertex ID is a migrated JanusGraph numeric long (e.g., "41996504")
     * or a new deterministic SHA-256 hex / UUID. So we return as-is.
     */
    public static String vertexIdToDocId(String vertexId) {
        if (isCassandraBackend()) {
            return vertexId;
        }

        // JanusGraph mode: base-36 encode numeric vertex IDs
        try {
            return encode(Long.parseLong(vertexId));
        } catch (NumberFormatException e) {
            // Non-numeric vertex ID — return as-is
            return vertexId;
        }
    }
}
