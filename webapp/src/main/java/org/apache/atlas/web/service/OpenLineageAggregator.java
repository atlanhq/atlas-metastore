package org.apache.atlas.web.service;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class OpenLineageAggregator implements Processor<String, JsonNode, String, JsonNode> {

    /**
     * Keep terminal state around to accept late arrivals; delete only after this idle period.
     */
    private static final Duration TERMINAL_TTL = Duration.ofMinutes(10);

    /**
     * Evict non-terminal runs after this much inactivity (processing-time).
     */
    private static final Duration INACTIVITY_WINDOW = Duration.ofHours(4);

    /**
     * How often we scan the RocksDB store for terminal TTL + inactivity eviction.
     */
    private static final Duration SWEEP_EVERY = Duration.ofSeconds(30);

    private static final String FIELD_SKIP_ENTITY_STORE = "skipEntityStore";
    private static final String FIELD_VERSIONED_LOOKUP = "versionedLookup";
    private static final String FIELD_METADATA = "metadata";
    private static final String FIELD_PAYLOAD = "payload";
    private static final String FIELD_UUID = "uuid";
    private static final String FIELD_EVENT_TIME = "eventTime";

    private final String outputSinkName;
    private final String errorSinkName;

    private ProcessorContext<String, JsonNode> ctx;
    private KeyValueStore<String, JsonNode> store;

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public OpenLineageAggregator(String outputSinkName, String errorSinkName) {
        this.outputSinkName = Objects.requireNonNull(outputSinkName, "outputSinkName");
        this.errorSinkName = Objects.requireNonNull(errorSinkName, "errorSinkName");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext<String, JsonNode> context) {
        this.ctx = context;
        this.store = context.getStateStore(LineageTopology.STORE_NAME);
        context.schedule(SWEEP_EVERY, PunctuationType.STREAM_TIME, this::sweep);
    }

    @Override
    public void process(Record<String, JsonNode> record) {
        JsonNode evNode = record.value();
        if (evNode == null || !evNode.isObject()) return;

        ObjectNode ev = (ObjectNode) evNode;

        try {
            // Validate OpenLineage required (non-optional) fields we rely on.
            validateOpenLineageNonOptional(ev);

            // Extract run id from event every time (fallback to record key)
            String runId = runId(record.key(), ev);
            if (runId == null) {
                // runId is required by our assumptions; treat as validation failure
                throw new OpenLineageValidationException("Missing required runId (record key blank and run.runId missing/blank)");
            }

            ObjectNode agg = asObject(store.get(runId));

            // 1) If there is no aggregate in the store, we use the first event.
            if (agg == null) {
                agg = ev.deepCopy();
            } else {
                // Subsequent events (including OTHER) merge datasets + facets + eventTime
                mergeOpenLineageAggregate(agg, ev);
            }

            store.put(runId, agg);

            // Emit every lineage event with metadata carrying skipEntityStore/versionedLookup
            long eventTimeMs = eventTimeMillis(ev.get("eventTime"));
            emit(runId, ev, eventTimeMs, true, true);

            // Emit immediately if terminal
            String eventType = text(agg, "eventType");
            long lastEventTimeMs = eventTimeMillis(agg.get("eventTime"));
            if (isTerminal(eventType)) {
                emit(runId, agg, lastEventTimeMs, false, false);
            }

        } catch (OpenLineageValidationException e) {
            // 1) place event on error queue
            forwardToErrorQueue(record, ev, e);

            // 2) throw exception (will typically fail the task and surface the bad record)
            throw e;
        } catch (RuntimeException e) {
            // If you also want unexpected runtime errors to go to error queue, keep this:
            forwardToErrorQueue(record, ev, e);
            throw e;
        }
    }

    private void sweep(long streamTimeMs) {
        try (KeyValueIterator<String, JsonNode> it = store.all()) {
            while (it.hasNext()) {
                var kv = it.next();
                String runId = kv.key;
                ObjectNode agg = asObject(kv.value);
                if (runId == null || runId.isBlank() || agg == null) continue;

                // Use aggregate's latest eventTime as "last seen"
                long lastEventTimeMs = eventTimeMillis(agg.get("eventTime"));
                if (lastEventTimeMs <= 0L) {
                    continue;
                }

                long idleMs = streamTimeMs - lastEventTimeMs;
                if (idleMs < 0) idleMs = 0;

                String eventType = text(agg, "eventType");

                // Terminal: delete only after TTL since last eventTime
                if (isTerminal(eventType)) {
                    if (idleMs >= TERMINAL_TTL.toMillis()) {
                        store.delete(runId);
                    }
                    continue;
                }

                // Non-terminal: evict after inactivity window since last eventTime
                if (idleMs >= INACTIVITY_WINDOW.toMillis()) {
                    store.delete(runId);
                }
            }
        }
    }

    private void emit(String runId, ObjectNode payload, long timestamp, boolean skipEntityStore, boolean versionedLookup) {
        ObjectNode metadata = MAPPER.createObjectNode();
        metadata.put(FIELD_SKIP_ENTITY_STORE, skipEntityStore);
        metadata.put(FIELD_VERSIONED_LOOKUP, versionedLookup);

        ObjectNode envelope = MAPPER.createObjectNode();
        envelope.put(FIELD_UUID, UUIDs.timeBased().toString());
        envelope.put(FIELD_EVENT_TIME, timestamp > 0L ? timestamp : System.currentTimeMillis());
        envelope.set(FIELD_METADATA, metadata);
        envelope.set(FIELD_PAYLOAD, payload);

        ctx.forward(new Record<>(runId, envelope, timestamp), outputSinkName);
    }

    // ---------------- Error queue ----------------

    private void forwardToErrorQueue(Record<String, JsonNode> record, ObjectNode originalEvent, Exception e) {
        // Try to keep a stable key for the error topic:
        String key = runId(record.key(), originalEvent);
        if (key == null || key.isBlank()) key = record.key();
        if (key == null || key.isBlank()) key = UUID.randomUUID().toString();

        ObjectNode err = MAPPER.createObjectNode();
        err.put("errorType", e.getClass().getName());
        err.put("errorMessage", String.valueOf(e.getMessage()));
        err.put("sourceTopicKey", record.key() == null ? null : record.key());
        err.put("processor", this.getClass().getSimpleName());
        err.set("originalEvent", originalEvent);

        // Preserve input record timestamp for easier correlation
        long ts = record.timestamp();

        ctx.forward(new Record<>(key, err, ts), errorSinkName);
    }

    // ---------------- Validation ----------------

    /**
     * Enforces the OpenLineage "non-optional" fields we rely on.
     *
     * This is intentionally strict: if violated, we route to error queue and throw.
     */
    private static void validateOpenLineageNonOptional(ObjectNode ev) {
        // eventType (required by OL spec, and by our terminal logic)
        String eventType = text(ev, "eventType");
        if (eventType == null) {
            throw new OpenLineageValidationException("Missing required field: eventType");
        }

        // eventTime (required by OL spec, and by our aggregation / TTL logic)
        JsonNode eventTimeNode = ev.get("eventTime");
        if (eventTimeNode == null || eventTimeNode.isNull()) {
            throw new OpenLineageValidationException("Missing required field: eventTime");
        }
        long ms = eventTimeMillis(eventTimeNode);
        if (ms <= 0L) {
            throw new OpenLineageValidationException("Invalid required field: eventTime (must be RFC3339 string or millis number)");
        }

        // run.runId (required by OL spec; we also use it for aggregation key when record.key is blank)
        ObjectNode run = asObject(ev.get("run"));
        if (run == null) {
            throw new OpenLineageValidationException("Missing required object: run");
        }
        String runId = text(run, "runId");
        if (runId == null) {
            throw new OpenLineageValidationException("Missing required field: run.runId");
        }

        // job.{namespace,name} (required by OL spec)
        ObjectNode job = asObject(ev.get("job"));
        if (job == null) {
            throw new OpenLineageValidationException("Missing required object: job");
        }
        String jobNs = text(job, "namespace");
        String jobName = text(job, "name");
        if (jobNs == null) {
            throw new OpenLineageValidationException("Missing required field: job.namespace");
        }
        if (jobName == null) {
            throw new OpenLineageValidationException("Missing required field: job.name");
        }

        // datasets (if present) must have namespace + name (required by OL dataset identifier)
        validateDatasetArray(ev, "inputs");
        validateDatasetArray(ev, "outputs");
    }

    private static void validateDatasetArray(ObjectNode ev, String field) {
        JsonNode n = ev.get(field);
        if (n == null || n.isNull()) return; // optional arrays

        if (!n.isArray()) {
            throw new OpenLineageValidationException("Field " + field + " must be an array when present");
        }
        ArrayNode arr = (ArrayNode) n;
        for (int i = 0; i < arr.size(); i++) {
            JsonNode item = arr.get(i);
            if (item == null || !item.isObject()) {
                throw new OpenLineageValidationException("Field " + field + "[" + i + "] must be an object");
            }
            ObjectNode ds = (ObjectNode) item;

            // OL requires dataset namespace + name for dataset identifier
            String ns = text(ds, "namespace");
            String name = text(ds, "name");
            if (ns == null) {
                throw new OpenLineageValidationException("Missing required field: " + field + "[" + i + "].namespace");
            }
            if (name == null) {
                throw new OpenLineageValidationException("Missing required field: " + field + "[" + i + "].name");
            }
        }
    }

    public static class OpenLineageValidationException extends RuntimeException {
        public OpenLineageValidationException(String message) {
            super(message);
        }

        public OpenLineageValidationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // ---------------- Merge logic ----------------

    private static void mergeOpenLineageAggregate(ObjectNode agg, ObjectNode ev) {
        // eventType: OTHER never overwrites an existing aggregate type
        mergeEventType(agg, ev);

        // eventTime: keep the latest (max)
        mergeEventTimeMax(agg, ev);

        // merge run.facets and job.facets shallowly by facet key
        mergeFacetsAtPath(agg, ev, "run");
        mergeFacetsAtPath(agg, ev, "job");

        // merge datasets by identity; merge facets across all datasets; overwrite other fields
        mergeDatasetsArray(agg, ev, "inputs");
        mergeDatasetsArray(agg, ev, "outputs");
    }

    private static void mergeEventType(ObjectNode agg, ObjectNode ev) {
        String incomingType = text(ev, "eventType");

        // If incoming is OTHER, do not override existing type
        if ("OTHER".equalsIgnoreCase(incomingType)) {
            return;
        }

        // Non-OTHER always sets
        agg.put("eventType", incomingType);
    }

    private static void mergeEventTimeMax(ObjectNode agg, ObjectNode ev) {
        JsonNode incoming = ev.get("eventTime");
        JsonNode existing = agg.get("eventTime");

        long inMs = eventTimeMillis(incoming);
        long exMs = eventTimeMillis(existing);

        if (inMs > 0 && exMs > 0) {
            if (inMs >= exMs) agg.set("eventTime", incoming);
        } else {
            agg.set("eventTime", incoming);
        }
    }

    private static long eventTimeMillis(JsonNode eventTimeNode) {
        if (eventTimeNode == null || eventTimeNode.isNull()) return -1L;

        try {
            if (eventTimeNode.isTextual()) {
                Instant t = Instant.parse(eventTimeNode.asText());
                return t.toEpochMilli();
            }
            if (eventTimeNode.isNumber()) return eventTimeNode.asLong();
        } catch (Exception ignored) {
        }
        return -1L;
    }

    private static void mergeFacetsAtPath(ObjectNode agg, ObjectNode ev, String parentField) {
        JsonNode evParent = ev.get(parentField);
        if (evParent == null || !evParent.isObject()) return;

        ObjectNode aggParent = ensureObject(agg, parentField);
        ObjectNode evFacets = asObject(evParent.get("facets"));
        if (evFacets == null) return;

        ObjectNode aggFacets = ensureObject(aggParent, "facets");
        mergeFacetsShallow(aggFacets, evFacets);
    }

    private static void mergeFacetsShallow(ObjectNode targetFacets, ObjectNode incomingFacets) {
        incomingFacets.fields().forEachRemaining(e -> {
            if (e.getValue() != null && !e.getValue().isNull()) {
                targetFacets.set(e.getKey(), e.getValue());
            }
        });
    }

    private static void mergeDatasetsArray(ObjectNode agg, ObjectNode ev, String field) {
        ArrayNode incoming = asArray(ev.get(field));
        if (incoming == null || incoming.isEmpty()) return;

        ArrayNode existing = asArray(agg.get(field));
        if (existing == null) {
            agg.set(field, incoming);
            return;
        }

        Map<String, ObjectNode> byKey = new LinkedHashMap<>();
        for (JsonNode n : existing) {
            ObjectNode o = asObject(n);
            if (o == null) continue;
            String k = datasetKey(o);
            if (k != null) byKey.put(k, o);
        }

        for (JsonNode n : incoming) {
            ObjectNode inc = asObject(n);
            if (inc == null) continue;

            String k = datasetKey(inc);
            if (k == null) {
                existing.add(inc);
                continue;
            }

            ObjectNode cur = byKey.get(k);
            if (cur == null) {
                ObjectNode copy = inc.deepCopy();
                existing.add(copy);
                byKey.put(k, copy);
            } else {
                overwriteAllExceptFacets(cur, inc);

                ObjectNode curFacets = ensureObject(cur, "facets");
                ObjectNode incFacets = asObject(inc.get("facets"));
                if (incFacets != null) mergeFacetsShallow(curFacets, incFacets);
            }
        }
    }

    private static void overwriteAllExceptFacets(ObjectNode target, ObjectNode incoming) {
        incoming.fields().forEachRemaining(e -> {
            String k = e.getKey();
            if ("facets".equals(k)) return;
            JsonNode v = e.getValue();
            if (v != null && !v.isNull()) target.set(k, v);
        });
    }

    private static String datasetKey(ObjectNode ds) {
        String ns = text(ds, "namespace");
        String name = text(ds, "name");
        if ((ns == null || ns.isBlank()) && (name == null || name.isBlank())) return null;
        return (ns == null ? "" : ns) + "\u0001" + (name == null ? "" : name);
    }

    private static ArrayNode asArray(JsonNode n) {
        return (n != null && n.isArray()) ? (ArrayNode) n : null;
    }

    private static ObjectNode asObject(JsonNode n) {
        return (n != null && n.isObject()) ? (ObjectNode) n : null;
    }

    private static String runId(String key, ObjectNode ev) {
        if (key != null && !key.isBlank()) return key;

        JsonNode run = ev.get("run");
        if (run != null && run.isObject()) {
            String rid = text((ObjectNode) run, "runId");
            if (rid != null && !rid.isBlank()) return rid;
        }
        return null;
    }

    private static boolean isTerminal(String t) {
        if (t == null) return false;
        return t.equalsIgnoreCase("COMPLETE") || t.equalsIgnoreCase("FAIL") || t.equalsIgnoreCase("ABORT");
    }

    private static ObjectNode ensureObject(ObjectNode parent, String field) {
        JsonNode n = parent.get(field);
        if (n != null && n.isObject()) return (ObjectNode) n;
        ObjectNode created = MAPPER.createObjectNode();
        parent.set(field, created);
        return created;
    }

    private static String text(ObjectNode obj, String field) {
        JsonNode n = obj.get(field);
        if (n == null || n.isNull()) return null;
        String s = n.isTextual() ? n.asText() : n.toString();
        return (s == null || s.isBlank()) ? null : s;
    }

    // ------------ Topology + JSON Serde ------------

    public enum LineageTopologyType {
        IN_MEM, ROCK_DB
    }

    public static class LineageTopology {
        public static final String STORE_NAME = "run-aggregate-store";

        private static final String SOURCE = "openlineageevents";
        private static final String PROC = "run-aggregator";
        private static final String FINAL_SINK = "events-topic";
        private static final String ERROR_SINK = "open-lineage-errors";

        public static Topology build(String inputTopic, String outputTopic, String errorTopic, LineageTopologyType topologyType) {
            Objects.requireNonNull(inputTopic, "inputTopic");
            Objects.requireNonNull(outputTopic, "outputTopic");
            Objects.requireNonNull(errorTopic, "errorTopic");

            Serde<JsonNode> jsonSerde = new JacksonJsonSerde();

            Topology t = new Topology();

            t.addSource(
                    SOURCE,
                    Serdes.String().deserializer(),
                    jsonSerde.deserializer(),
                    inputTopic
            );

            t.addProcessor(
                    PROC,
                    () -> new OpenLineageAggregator(FINAL_SINK, ERROR_SINK),
                    SOURCE
            );

            t.addStateStore(
                    Stores.keyValueStoreBuilder(
                            topologyType == LineageTopologyType.IN_MEM ?
                                    Stores.inMemoryKeyValueStore(STORE_NAME) :
                                    Stores.persistentKeyValueStore(STORE_NAME),
                            Serdes.String(),
                            jsonSerde
                    ),
                    PROC
            );

            t.addSink(
                    FINAL_SINK,
                    outputTopic,
                    Serdes.String().serializer(),
                    jsonSerde.serializer(),
                    PROC
            );

            t.addSink(
                    ERROR_SINK,
                    errorTopic,
                    Serdes.String().serializer(),
                    jsonSerde.serializer(),
                    PROC
            );

            return t;
        }

        private LineageTopology() {
        }
    }

    /**
     * Serde that keeps payloads as JSON trees (JsonNode).
     * No POJO (de)serialization.
     */
    public static class JacksonJsonSerde implements Serde<JsonNode> {

        @Override
        public Serializer<JsonNode> serializer() {
            return (topic, data) -> {
                try {
                    return data == null ? null : MAPPER.writeValueAsBytes(data);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("serialize JsonNode", e);
                }
            };
        }

        @Override
        public Deserializer<JsonNode> deserializer() {
            return (topic, bytes) -> {
                try {
                    return bytes == null ? null : MAPPER.readTree(bytes);
                } catch (Exception e) {
                    throw new RuntimeException("deserialize JsonNode", e);
                }
            };
        }
    }
}
