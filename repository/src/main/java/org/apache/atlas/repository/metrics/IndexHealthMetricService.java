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
package org.apache.atlas.repository.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasBusinessMetadataType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;

/**
 * Audits JanusGraph mixed index health on startup and exposes Prometheus metrics.
 *
 * For every primitive attribute across all entity types and business metadata types,
 * checks whether a property key is registered in the mixed index. Missing property keys
 * mean JanusGraph will never sync that attribute to Elasticsearch — the attribute exists
 * in Cassandra but is invisible to search.
 *
 * Metrics are set once on startup and persist for the lifetime of the pod.
 */
@Service
public class IndexHealthMetricService {
    private static final Logger LOG = LoggerFactory.getLogger(IndexHealthMetricService.class);

    private static final String METRIC_PREFIX = "atlas_index_health";

    // Core types from addons/models — if these are missing, ALL tenants are affected
    private static final Set<String> CORE_TYPE_NAMES = Set.of(
            "Referenceable", "Asset", "DataSet", "Process", "Infrastructure",
            "DataDomain", "DataProduct", "Connection",
            "AuthPolicy", "Persona", "Purpose", "AccessControl", "StakeholderTitle",
            "AtlasGlossary", "AtlasGlossaryTerm", "AtlasGlossaryCategory",
            "AuthService", "AtlasServer",
            // Common catalog types (from Cedar/models repo)
            "Table", "Column", "Schema", "Database", "View", "MaterialisedView",
            "Query", "Folder", "Collection"
    );

    private final MeterRegistry meterRegistry;
    private final AtlasTypeRegistry typeRegistry;

    // Atomic integers backing the gauges
    private final AtomicInteger entityExpected = new AtomicInteger(0);
    private final AtomicInteger entityIndexed = new AtomicInteger(0);
    private final AtomicInteger entityMissing = new AtomicInteger(0);
    private final AtomicInteger entityCoreMissing = new AtomicInteger(0);
    private final AtomicInteger entityCustomMissing = new AtomicInteger(0);
    private final AtomicInteger bmExpected = new AtomicInteger(0);
    private final AtomicInteger bmIndexed = new AtomicInteger(0);
    private final AtomicInteger bmMissing = new AtomicInteger(0);
    private final AtomicInteger coreMissingTotal = new AtomicInteger(0);
    private final AtomicInteger healthStatus = new AtomicInteger(1); // 1 = healthy, 0 = unhealthy
    private final Map<String, AtomicInteger> perTypeMissing = new ConcurrentHashMap<>();

    @Inject
    public IndexHealthMetricService(AtlasTypeRegistry typeRegistry) {
        this(typeRegistry, getMeterRegistry());
    }

    // Constructor for testing
    IndexHealthMetricService(AtlasTypeRegistry typeRegistry, MeterRegistry meterRegistry) {
        this.typeRegistry = typeRegistry;
        this.meterRegistry = meterRegistry;
        registerGauges();
    }

    private void registerGauges() {
        // Entity type gauges
        Gauge.builder(METRIC_PREFIX + "_expected_total", entityExpected, AtomicInteger::get)
                .description("Total primitive attributes expected in mixed index")
                .tag("category", "entity")
                .register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_indexed_total", entityIndexed, AtomicInteger::get)
                .description("Primitive attributes registered in mixed index")
                .tag("category", "entity")
                .register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_missing_total", entityMissing, AtomicInteger::get)
                .description("Primitive attributes missing from mixed index")
                .tag("category", "entity")
                .tag("source", "all")
                .register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_missing_total", entityCoreMissing, AtomicInteger::get)
                .description("Core type attributes missing from mixed index")
                .tag("category", "entity")
                .tag("source", "core")
                .register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_missing_total", entityCustomMissing, AtomicInteger::get)
                .description("Custom type attributes missing from mixed index")
                .tag("category", "entity")
                .tag("source", "custom")
                .register(meterRegistry);

        // Business metadata gauges
        Gauge.builder(METRIC_PREFIX + "_expected_total", bmExpected, AtomicInteger::get)
                .description("Total BM attributes expected in mixed index")
                .tag("category", "business_metadata")
                .register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_indexed_total", bmIndexed, AtomicInteger::get)
                .description("BM attributes registered in mixed index")
                .tag("category", "business_metadata")
                .register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_missing_total", bmMissing, AtomicInteger::get)
                .description("BM attributes missing from mixed index")
                .tag("category", "business_metadata")
                .tag("source", "all")
                .register(meterRegistry);

        // Cross-category summary gauges
        Gauge.builder(METRIC_PREFIX + "_core_missing_total", coreMissingTotal, AtomicInteger::get)
                .description("Core type attributes missing — affects all tenants if > 0")
                .register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_status", healthStatus, AtomicInteger::get)
                .description("Overall index health: 1 = healthy, 0 = unhealthy")
                .register(meterRegistry);
    }

    /**
     * Audits the mixed index schema against the type registry.
     * Called after onLoadCompletion() when the management system is available.
     *
     * This is READ-ONLY — the management transaction should be rolled back after the audit.
     *
     * @param managementSystem a read-only JanusGraph management system
     */
    public void auditIndexHealth(AtlasGraphManagement managementSystem) {
        LOG.info("Starting index health audit...");

        int totalExpected = 0;
        int totalIndexed = 0;
        int totalMissing = 0;
        int totalCoreMissing = 0;
        int totalCustomMissing = 0;
        int totalBmExpected = 0;
        int totalBmIndexed = 0;
        int totalBmMissing = 0;
        List<String> missingAttributes = new ArrayList<>();

        // Audit entity types
        for (AtlasEntityType entityType : typeRegistry.getAllEntityDefs()
                .stream()
                .map(def -> typeRegistry.getEntityTypeByName(def.getName()))
                .filter(Objects::nonNull)
                .toList()) {

            String typeName = entityType.getTypeName();
            boolean isCoreType = CORE_TYPE_NAMES.contains(typeName);
            int typeMissingCount = 0;

            for (AtlasAttribute attribute : entityType.getAllAttributes().values()) {
                if (!TypeCategory.PRIMITIVE.equals(attribute.getAttributeType().getTypeCategory())) {
                    continue;
                }
                if (!attribute.getAttributeDef().getIsIndexable()) {
                    continue;
                }

                totalExpected++;

                AtlasPropertyKey propertyKey = managementSystem.getPropertyKey(attribute.getVertexPropertyName());
                if (propertyKey != null) {
                    totalIndexed++;
                } else {
                    totalMissing++;
                    typeMissingCount++;
                    if (isCoreType) {
                        totalCoreMissing++;
                    } else {
                        totalCustomMissing++;
                    }
                    missingAttributes.add(typeName + "." + attribute.getName());
                }
            }

            if (typeMissingCount > 0) {
                getOrCreatePerTypeGauge(typeName).set(typeMissingCount);
            }
        }

        // Audit business metadata types
        for (AtlasBusinessMetadataType bmType : typeRegistry.getAllBusinessMetadataDefs()
                .stream()
                .map(def -> typeRegistry.getBusinessMetadataTypeByName(def.getName()))
                .filter(Objects::nonNull)
                .toList()) {

            for (AtlasAttribute attribute : bmType.getAllAttributes().values()) {
                if (!TypeCategory.PRIMITIVE.equals(attribute.getAttributeType().getTypeCategory())) {
                    continue;
                }
                if (!attribute.getAttributeDef().getIsIndexable()) {
                    continue;
                }

                totalBmExpected++;

                AtlasPropertyKey propertyKey = managementSystem.getPropertyKey(attribute.getVertexPropertyName());
                if (propertyKey != null) {
                    totalBmIndexed++;
                } else {
                    totalBmMissing++;
                    missingAttributes.add(bmType.getTypeName() + "." + attribute.getName());
                }
            }
        }

        // Set gauge values
        entityExpected.set(totalExpected);
        entityIndexed.set(totalIndexed);
        entityMissing.set(totalMissing);
        entityCoreMissing.set(totalCoreMissing);
        entityCustomMissing.set(totalCustomMissing);
        bmExpected.set(totalBmExpected);
        bmIndexed.set(totalBmIndexed);
        bmMissing.set(totalBmMissing);
        coreMissingTotal.set(totalCoreMissing);

        boolean isHealthy = (totalMissing + totalBmMissing) == 0;
        healthStatus.set(isHealthy ? 1 : 0);

        // Log results
        if (isHealthy) {
            LOG.info("Index health audit PASSED: {}/{} entity attrs indexed, {}/{} BM attrs indexed",
                    totalIndexed, totalExpected, totalBmIndexed, totalBmExpected);
        } else {
            LOG.error("INDEX HEALTH AUDIT FAILED: {} entity attrs missing (core={}, custom={}), {} BM attrs missing. Missing: {}",
                    totalMissing, totalCoreMissing, totalCustomMissing, totalBmMissing, missingAttributes);
        }
    }

    private AtomicInteger getOrCreatePerTypeGauge(String typeName) {
        return perTypeMissing.computeIfAbsent(typeName, name -> {
            AtomicInteger counter = new AtomicInteger(0);
            Gauge.builder(METRIC_PREFIX + "_type_missing", counter, AtomicInteger::get)
                    .description("Missing attributes for entity type")
                    .tag("typeName", name)
                    .register(meterRegistry);
            return counter;
        });
    }
}
