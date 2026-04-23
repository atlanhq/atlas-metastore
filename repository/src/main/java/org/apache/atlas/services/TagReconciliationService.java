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
package org.apache.atlas.services;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.Tag;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.repository.store.graph.v2.ESConnector;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAOCassandraImpl;
import org.apache.atlas.service.config.ConfigKey;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.atlas.service.redis.RedisService;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.*;

/**
 * Change-log-driven tag reconciliation service.
 *
 * Drains the tags.tag_change_log table, and for each vertex:
 * 1. Reads all tags from Cassandra (source of truth)
 * 2. Detects and cleans orphaned propagated tags
 * 3. Computes the 5 denorm fields and writes a full snapshot to ES
 *
 * The change log is populated by:
 * - Initial seed: K8s Job using cqlsh COPY TO/FROM (one-time backfill)
 * - Forward path: tag write operations INSERT into the change log (added incrementally)
 *
 * Disabled by default. Enable per-tenant:
 *   DynamicConfigStore.setConfig("TAG_RECONCILER_ENABLED", "true", "admin")
 *
 * Runs within a configurable nightly window. Resumes from where it left off.
 * Single-pod execution via Redis distributed lock.
 */
@Component
@DependsOn("dynamicConfigStore")
public class TagReconciliationService {
    private static final Logger LOG = LoggerFactory.getLogger(TagReconciliationService.class);

    private static final String LOCK_KEY       = "atlas:tag:reconciler:lock";
    private static final String CONFIG_ENABLED = ConfigKey.TAG_RECONCILER_ENABLED.getKey();
    private static final String CONFIG_STATE   = ConfigKey.TAG_RECONCILER_STATE.getKey();
    private static final String UPDATER        = "tag-reconciler";

    private static final int WINDOW_START_HOUR = Integer.getInteger("atlas.tag.reconciler.window.start.hour", 0);
    private static final int WINDOW_DURATION_H = Integer.getInteger("atlas.tag.reconciler.window.duration.hours", 4);
    private static final int PAGE_SIZE         = Integer.getInteger("atlas.tag.reconciler.page.size", 500);
    private static final int ES_BATCH_SIZE     = Integer.getInteger("atlas.tag.reconciler.es.batch.size", 50);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RedisService redisService;

    @Inject
    public TagReconciliationService(RedisService redisService) {
        this.redisService = redisService;
    }

    // ── Scheduled entry point ───────────────────────────────────────────

    @Scheduled(fixedDelayString = "${atlas.tag.reconciler.poll.interval:900000}")
    public void tick() {
        if (!isEnabled() || !isWithinWindow()) {
            return;
        }

        boolean lockAcquired = false;
        try {
            lockAcquired = redisService.acquireDistributedLock(LOCK_KEY);
            if (!lockAcquired) {
                return;
            }
            LOG.info("TagReconciler: acquired lock, starting cycle");
            runCycle();
        } catch (Exception e) {
            LOG.error("TagReconciler: error during cycle", e);
        } finally {
            if (lockAcquired) {
                try { redisService.releaseDistributedLock(LOCK_KEY); } catch (Exception ignored) {}
            }
        }
    }

    // ── Core loop — drains the change log ───────────────────────────────

    private void runCycle() throws Exception {
        ReconcileState state = loadState();
        TagDAOCassandraImpl tagDAO = TagDAOCassandraImpl.getInstance();
        long windowEnd = computeWindowEndMs();
        long cycleStart = System.currentTimeMillis();

        int entriesProcessed = 0;
        int verticesReconciled = 0;
        int orphansCleaned = 0;

        // Source cache: sourceVertexId|tagTypeName -> has active direct tag
        Map<String, Boolean> sourceCache = new HashMap<>();

        String currentDayBucket = state.dayBucket != null ? state.dayBucket : oldestDayBucket();

        LOG.info("TagReconciler: starting from dayBucket={}, lastTimestamp={}, lastVertexId={}",
                currentDayBucket, state.lastTimestamp, state.lastVertexId);

        String today = LocalDate.now(ZoneOffset.UTC).toString();

        while (System.currentTimeMillis() < windowEnd) {
            if (currentDayBucket == null) {
                currentDayBucket = oldestDayBucket();
                if (currentDayBucket == null) {
                    LOG.info("TagReconciler: change log is empty, nothing to do");
                    break;
                }
            }

            // Don't process future days
            if (currentDayBucket.compareTo(today) > 0) {
                LOG.info("TagReconciler: caught up to today, done for this cycle");
                break;
            }

            List<TagDAO.TagChangeLogEntry> entries = tagDAO.readChangeLog(
                    currentDayBucket, state.lastTimestamp, state.lastVertexId, PAGE_SIZE);

            if (entries.isEmpty()) {
                // This day bucket is exhausted, move to next day
                currentDayBucket = nextDay(currentDayBucket);
                state.dayBucket = currentDayBucket;
                state.lastTimestamp = 0;
                state.lastVertexId = null;
                saveState(state);

                if (currentDayBucket.compareTo(today) > 0) {
                    LOG.info("TagReconciler: finished all day buckets up to today");
                    break;
                }
                continue;
            }

            // Deduplicate vertex IDs in this page
            Set<String> vertexIds = entries.stream()
                    .map(e -> e.vertexId)
                    .collect(Collectors.toCollection(LinkedHashSet::new));

            Map<String, Map<String, Object>> esBatch = new HashMap<>();

            for (String vertexId : vertexIds) {
                try {
                    ReconcileResult result = reconcileVertex(tagDAO, vertexId, sourceCache);
                    verticesReconciled++;
                    orphansCleaned += result.orphansCleaned;

                    if (result.denormFields != null) {
                        esBatch.put(vertexId, result.denormFields);
                    }

                    if (esBatch.size() >= ES_BATCH_SIZE) {
                        flushToES(esBatch);
                        esBatch.clear();
                    }
                } catch (Exception e) {
                    LOG.warn("TagReconciler: error processing vertex {}", vertexId, e);
                }
            }

            if (!esBatch.isEmpty()) {
                flushToES(esBatch);
            }

            // Update cursor to the last entry in this page
            TagDAO.TagChangeLogEntry lastEntry = entries.get(entries.size() - 1);
            state.dayBucket = currentDayBucket;
            state.lastTimestamp = lastEntry.createdAt;
            state.lastVertexId = lastEntry.vertexId;
            entriesProcessed += entries.size();

            saveState(state);
        }

        long elapsed = System.currentTimeMillis() - cycleStart;
        LOG.info("TagReconciler: cycle done — entries={}, vertices={}, orphans={}, elapsed={}ms",
                entriesProcessed, verticesReconciled, orphansCleaned, elapsed);
    }

    // ── Per-vertex reconciliation ───────────────────────────────────────

    private ReconcileResult reconcileVertex(TagDAOCassandraImpl tagDAO, String vertexId,
                                            Map<String, Boolean> sourceCache) throws AtlasBaseException {
        ReconcileResult result = new ReconcileResult();
        List<Tag> allTags = tagDAO.getAllTagsByVertexId(vertexId);

        List<String> directTagNames = new ArrayList<>();
        List<String> propagatedTagNames = new ArrayList<>();
        List<Tag> orphanTags = new ArrayList<>();

        for (Tag tag : allTags) {
            boolean isPropagated = !vertexId.equals(tag.getSourceVertexId());

            if (isPropagated) {
                String cacheKey = tag.getSourceVertexId() + "|" + tag.getTagTypeName();

                if (!sourceCache.containsKey(cacheKey)) {
                    try {
                        AtlasClassification directTag = tagDAO.findDirectTagByVertexIdAndTagTypeName(
                                tag.getSourceVertexId(), tag.getTagTypeName(), false);
                        sourceCache.put(cacheKey, directTag != null);
                    } catch (Exception e) {
                        sourceCache.put(cacheKey, true); // assume valid on error
                    }
                }

                if (sourceCache.get(cacheKey)) {
                    propagatedTagNames.add(tag.getTagTypeName());
                } else {
                    orphanTags.add(tag);
                }
            } else {
                directTagNames.add(tag.getTagTypeName());
            }
        }

        // Batch-delete all orphans in one call
        if (!orphanTags.isEmpty()) {
            try {
                tagDAO.deleteTags(orphanTags);
                result.orphansCleaned = orphanTags.size();
                for (Tag orphan : orphanTags) {
                    LOG.info("TagReconciler: cleaned orphan tag {} on vertex {} from source {}",
                            orphan.getTagTypeName(), vertexId, orphan.getSourceVertexId());
                }
            } catch (Exception e) {
                LOG.warn("TagReconciler: failed to clean {} orphans on vertex {}", orphanTags.size(), vertexId, e);
            }
        }

        result.denormFields = computeDenormFields(directTagNames, propagatedTagNames);
        return result;
    }

    // ── Denorm computation ──────────────────────────────────────────────

    private Map<String, Object> computeDenormFields(List<String> directNames, List<String> propagatedNames) {
        Map<String, Object> fields = new HashMap<>();

        fields.put(TRAIT_NAMES_PROPERTY_KEY, new ArrayList<>(directNames));
        fields.put(CLASSIFICATION_NAMES_KEY, directNames.isEmpty() ? null
                : CLASSIFICATION_NAME_DELIMITER + String.join(CLASSIFICATION_NAME_DELIMITER, directNames) + CLASSIFICATION_NAME_DELIMITER);

        fields.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, new ArrayList<>(propagatedNames));
        fields.put(PROPAGATED_CLASSIFICATION_NAMES_KEY, propagatedNames.isEmpty() ? null
                : CLASSIFICATION_NAME_DELIMITER + String.join(CLASSIFICATION_NAME_DELIMITER, propagatedNames) + CLASSIFICATION_NAME_DELIMITER);

        Set<String> allNames = new LinkedHashSet<>();
        allNames.addAll(directNames);
        allNames.addAll(propagatedNames);
        fields.put(CLASSIFICATION_TEXT_KEY, allNames.isEmpty() ? "" : String.join(" ", allNames));

        return fields;
    }

    // ── ES flush ────────────────────────────────────────────────────────

    private void flushToES(Map<String, Map<String, Object>> batch) {
        try {
            ESConnector.writeTagProperties(batch);
        } catch (Exception e) {
            LOG.error("TagReconciler: failed to flush {} vertices to ES", batch.size(), e);
        }
    }

    // ── Day bucket helpers ──────────────────────────────────────────────

    private String oldestDayBucket() {
        // Start from 7 days ago (TTL window)
        return LocalDate.now(ZoneOffset.UTC).minusDays(7).toString();
    }

    private String nextDay(String dayBucket) {
        return LocalDate.parse(dayBucket).plusDays(1).toString();
    }

    // ── Window check ────────────────────────────────────────────────────

    private boolean isWithinWindow() {
        int hour = LocalTime.now(ZoneOffset.UTC).getHour();
        int windowEnd = (WINDOW_START_HOUR + WINDOW_DURATION_H) % 24;

        if (WINDOW_START_HOUR < windowEnd) {
            return hour >= WINDOW_START_HOUR && hour < windowEnd;
        } else {
            return hour >= WINDOW_START_HOUR || hour < windowEnd;
        }
    }

    private long computeWindowEndMs() {
        // Compute actual window end time today
        int windowEndHour = (WINDOW_START_HOUR + WINDOW_DURATION_H) % 24;
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        long windowEndMs;

        if (WINDOW_START_HOUR < windowEndHour) {
            windowEndMs = today.atTime(windowEndHour, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        } else {
            // Wraps midnight — end is next day
            windowEndMs = today.plusDays(1).atTime(windowEndHour, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        }

        return windowEndMs;
    }

    // ── Config ──────────────────────────────────────────────────────────

    private boolean isEnabled() {
        try {
            return "true".equalsIgnoreCase(DynamicConfigStore.getConfig(CONFIG_ENABLED));
        } catch (Exception e) {
            return false;
        }
    }

    // ── State persistence ───────────────────────────────────────────────

    private ReconcileState loadState() {
        try {
            String json = DynamicConfigStore.getConfig(CONFIG_STATE);
            if (StringUtils.isNotEmpty(json)) {
                return MAPPER.readValue(json, ReconcileState.class);
            }
        } catch (Exception e) {
            LOG.warn("TagReconciler: failed to load state, starting fresh", e);
        }
        return new ReconcileState();
    }

    private void saveState(ReconcileState state) {
        try {
            DynamicConfigStore.setConfig(CONFIG_STATE, MAPPER.writeValueAsString(state), UPDATER);
        } catch (Exception e) {
            LOG.error("TagReconciler: failed to save state", e);
        }
    }

    // ── Models ──────────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    static class ReconcileState {
        public String dayBucket;
        public long   lastTimestamp;
        public String lastVertexId;
    }

    private static class ReconcileResult {
        int orphansCleaned;
        Map<String, Object> denormFields;
    }
}
