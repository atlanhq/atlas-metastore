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

package org.apache.atlas.repository.util;

import org.apache.atlas.RequestContext;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.repository.store.graph.v2.ClassificationAssociator;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_ADD;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_DELETE;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_NOOP;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_UPDATE;

public final class AtlasEntityUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityUtils.class);

    private AtlasEntityUtils() {
    }

    public static String getQualifiedName(AtlasEntity entity) {
        return getStringAttribute(entity, QUALIFIED_NAME);
    }

    public static String getName(AtlasEntity entity) {
        return getStringAttribute(entity, NAME);
    }

    public static List<String> getListAttribute(AtlasStruct entity, String attrName) {
        List<String> ret = new ArrayList<>();

        Object valueObj = entity.getAttribute(attrName);
        if (valueObj != null) {
            ret = (List<String>) valueObj;
        }

        return ret;
    }

    public static String getStringAttribute(AtlasEntity entity, String attrName) {
        Object obj = entity.getAttribute(attrName);
        return obj == null ? null : (String) obj;
    }

    public static String getStringAttribute(AtlasEntityHeader entity, String attrName) {
        Object obj = entity.getAttribute(attrName);
        return obj == null ? null : (String) obj;
    }

    public static Map<String, Object> mapOf(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

/*    public static Map<String, List<AtlasClassification>> validateAndGetTagsDiff(String entityGuid,
                                                                                List<AtlasClassification> newTags,
                                                                                List<AtlasClassification> currentTags) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("validateAndGetTagsDiffReplace");

        try {
            Map<String, List<AtlasClassification>> operationListMap = new HashMap<>();

            if (CollectionUtils.isEmpty(newTags)) {
                if (!CollectionUtils.isEmpty(currentTags)) {
                    // Remove all existing tags
                    bucket(PROCESS_DELETE, operationListMap, currentTags);
                }
                return operationListMap;
            }

            List<AtlasClassification> toUpdate = new ArrayList<>();

            Predicate<AtlasClassification> existsInCurrent = newCar -> currentTags.stream().anyMatch(currentCar -> currentCar.checkForUpdate(newCar));
            Predicate<AtlasClassification> existsInNew = currentCar -> newTags.stream().anyMatch(newCar -> newCar.checkForUpdate(currentCar));


            List<AtlasClassification> toPreserve = newTags.stream().filter(existsInCurrent).collect(Collectors.toList());
            List<AtlasClassification> toAdd = newTags.stream().filter(existsInCurrent.negate()).collect(Collectors.toList());
            List<AtlasClassification> toRemove = currentTags.stream().filter(existsInNew.negate()).collect(Collectors.toList());

            bucket(PROCESS_DELETE, operationListMap, toRemove);
            bucket(PROCESS_UPDATE, operationListMap, toUpdate);
            bucket(PROCESS_ADD, operationListMap, toAdd);
            bucket("NOOP", operationListMap, toPreserve);

            return operationListMap;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }*/

    public static Map<String, List<AtlasClassification>> validateAndGetTagsDiff(String entityGuid,
                                                                                List<AtlasClassification> newTags,
                                                                                List<AtlasClassification> currentTags) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("validateAndGetTagsDiffReplace");

        try {
            Map<String, List<AtlasClassification>> operationListMap = new HashMap<>();

            if (CollectionUtils.isEmpty(newTags)) {
                if (!CollectionUtils.isEmpty(currentTags)) {
                    // Remove all existing tags
                    bucket(PROCESS_DELETE, operationListMap, currentTags);
                }
                return operationListMap;
            }

            List<AtlasClassification> toAdd = new ArrayList<>();
            List<AtlasClassification> toUpdate = new ArrayList<>();
            List<AtlasClassification> toRemove = new ArrayList<>();
            List<AtlasClassification> toPreserve = new ArrayList<>();

            Map<String, AtlasClassification> currentTagWithKeys = new HashMap<>();
            Optional.ofNullable(currentTags).orElse(Collections.emptyList()).forEach(x -> currentTagWithKeys.put(generateClassificationComparisonKey(entityGuid, x), x));

            Map<String, AtlasClassification> newTagWithKeys = new HashMap<>();
            newTags.forEach(x -> {
                if (StringUtils.isEmpty(x.getEntityGuid())) {
                    x.setEntityGuid(entityGuid);
                }
                newTagWithKeys.put(generateClassificationComparisonKey(entityGuid, x), x);
            });

            List<String> keysToAdd = (List<String>) CollectionUtils.subtract(newTagWithKeys.keySet(), currentTagWithKeys.keySet());
            List<String> keysToRemove = (List<String>) CollectionUtils.subtract(currentTagWithKeys.keySet(), newTagWithKeys.keySet());
            List<String> keysCommon = (List<String>) CollectionUtils.intersection(currentTagWithKeys.keySet(), newTagWithKeys.keySet());

            List<String> keysToUpdate = keysCommon.stream().filter(key -> !newTagWithKeys.get(key).checkForUpdate(currentTagWithKeys.get(key))).collect(Collectors.toList());
            List<String> keysUnChanged = keysCommon.stream().filter(key -> newTagWithKeys.get(key).checkForUpdate(currentTagWithKeys.get(key))).collect(Collectors.toList());

            keysToAdd.forEach(key -> toAdd.add(newTagWithKeys.get(key)));
            keysToRemove.forEach(key -> toRemove.add(currentTagWithKeys.get(key)));
            keysToUpdate.forEach(key -> toUpdate.add(newTagWithKeys.get(key)));
            keysUnChanged.forEach(key -> toPreserve.add(currentTagWithKeys.get(key)));


            bucket(PROCESS_DELETE, operationListMap, toRemove);
            bucket(PROCESS_UPDATE, operationListMap, toUpdate);
            bucket(PROCESS_ADD, operationListMap, toAdd);
            bucket(PROCESS_NOOP, operationListMap, toPreserve);

            return operationListMap;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public static Map<String, List<AtlasClassification>> validateAndGetTagsDiff(String entityGuid,
                                                                                List<AtlasClassification> newTags,
                                                                                List<AtlasClassification> currentTags,
                                                                                List<AtlasClassification> tagsToRemove) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("validateAndGetTagsDiffAppend");

        try {
            Map<String, List<AtlasClassification>> operationListMap = new HashMap<>();

            List<AtlasClassification> toAdd = new ArrayList<>();
            List<AtlasClassification> toUpdate = new ArrayList<>();
            List<AtlasClassification> toRemove = new ArrayList<>();
            List<AtlasClassification> toPreserve = new ArrayList<>();

            Map<String, AtlasClassification> currentTagWithKeys = new HashMap<>();
            Optional.ofNullable(currentTags).orElse(Collections.emptyList()).forEach(x -> currentTagWithKeys.put(generateClassificationComparisonKey(x), x));

            Map<String, AtlasClassification> newTagWithKeys = new HashMap<>();
            Optional.ofNullable(newTags).orElse(Collections.emptyList()).forEach(x -> newTagWithKeys.put(generateClassificationComparisonKey(x), x));

            for (AtlasClassification tagToRemove: Optional.ofNullable(tagsToRemove).orElse(Collections.emptyList())) {
                if (StringUtils.isEmpty(tagToRemove.getEntityGuid())) {
                    tagToRemove.setEntityGuid(entityGuid);
                }

                String tagToRemoveKey = generateClassificationComparisonKey(tagToRemove);
                if (currentTagWithKeys.containsKey(tagToRemoveKey)) {
                    toRemove.add(tagToRemove);
                    newTagWithKeys.remove(tagToRemoveKey); // performs dedup across addOrUpdate & remove tags list
                } else {
                    //ignoring the tag as it was not already present on the asset
                }
            }

            for (String newTagKey: newTagWithKeys.keySet()) {
                AtlasClassification newTag = newTagWithKeys.get(newTagKey);

                if (StringUtils.isEmpty(newTag.getEntityGuid())) {
                    newTag.setEntityGuid(entityGuid);
                }

                if (currentTagWithKeys.containsKey(newTagKey)) {
                    boolean hasDiff = !newTag.checkForUpdate(currentTagWithKeys.get(newTagKey));
                    if (hasDiff) {
                        toUpdate.add(newTag);
                    } else {
                        toPreserve.add(newTag);
                    }
                } else {
                    toAdd.add(newTag);
                }
            }

            bucket(PROCESS_DELETE, operationListMap, toRemove);
            bucket(PROCESS_UPDATE, operationListMap, toUpdate);
            bucket(PROCESS_ADD, operationListMap, toAdd);
            bucket(PROCESS_NOOP, operationListMap, toPreserve);

            return operationListMap;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    /*public static Map<String, List<AtlasClassification>> validateAndGetTagsDiff(String entityGuid,
                                                                                List<AtlasClassification> newTags,
                                                                                List<AtlasClassification> currentTags,
                                                                                List<AtlasClassification> tagsToRemove) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("validateAndGetTagsDiff");

        try {
            Map<String, List<AtlasClassification>> operationListMap = new HashMap<>();
            Set<String> preExistingClassificationKeys = new HashSet<>();
            List<AtlasClassification> filteredRemoveClassifications = new ArrayList<>();

            ClassificationAssociator.ListOps<AtlasClassification> listOps = new ClassificationAssociator.ListOps<>();

            for (AtlasClassification classification : Optional.ofNullable(currentTags).orElse(Collections.emptyList())) {
                if (entityGuid.equals(classification.getEntityGuid())) {
                    String key = generateClassificationComparisonKey(classification);
                    preExistingClassificationKeys.add(key);  // Track pre-existing keys
                }
            }

            for (AtlasClassification classification : Optional.ofNullable(tagsToRemove).orElse(Collections.emptyList())) {
                if (entityGuid.equals(classification.getEntityGuid())) {
                    String key = generateClassificationComparisonKey(classification);
                    // If the classification doesn't exist in pre-existing keys, log it
                    if (!preExistingClassificationKeys.contains(key)) {
                        String typeName = key.split("\\|")[1];
                        LOG.info("Classification {} is not associated with entity {}", typeName, entityGuid);
                    } else {
                        filteredRemoveClassifications.add(classification);
                    }
                }
            }

            List<AtlasClassification> filteredClassifications = Optional.ofNullable(newTags)
                    .orElse(Collections.emptyList())
                    .stream()
                    .filter(classification -> classification.getEntityGuid().equals(entityGuid))
                    .collect(Collectors.toList());

            List<AtlasClassification> incomingClassifications = listOps.filter(entityGuid, filteredClassifications);
            List<AtlasClassification> entityClassifications = listOps.filter(entityGuid, currentTags);

            bucket(PROCESS_DELETE, operationListMap, filteredRemoveClassifications);
            bucket(PROCESS_UPDATE, operationListMap, listOps.intersect(incomingClassifications, entityClassifications));
            bucket(PROCESS_ADD, operationListMap, listOps.subtract(incomingClassifications, entityClassifications));

            return operationListMap;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }*/

    private static String generateClassificationComparisonKey(AtlasClassification classification) {
        return generateClassificationComparisonKey(classification.getEntityGuid(), classification);
    }

    private static String generateClassificationComparisonKey(String entityGuid, AtlasClassification classification) {
        return entityGuid + "|" + classification.getTypeName();
    }

    private static void bucket(String op, Map<String, List<AtlasClassification>> operationListMap, List<AtlasClassification> results) {
        if (CollectionUtils.isEmpty(results)) {
            return;
        }

        operationListMap.put(op, results);
    }
}
