package org.apache.atlas.repository.tagoutbox;

import org.apache.atlas.model.ESDeferredOperation;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.ESWriteFailureRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Surface 2 bridge: consumes {@code ESWriteFailureRegistry} records (emitted
 * by {@code EntityMutationService.executeESPostProcessing} when direct
 * tag-attachment paths fail the ES deferred-op flush) and enqueues the failed
 * entities into the tag-outbox for background repair.
 *
 * <p>Installed at {@link TagOutboxService} startup via
 * {@code ESWriteFailureRegistry.setSink(this)}. Until this sink is installed,
 * {@code ESWriteFailureRegistry} runs with its default no-op sink and failures
 * are lost — so this bridge is the enablement point for durable Surface 2
 * recovery.</p>
 *
 * <p>The incoming {@link ESWriteFailureRegistry.ESWriteFailure} carries a list
 * of {@link ESDeferredOperation}s whose payloads are keyed by Cassandra vertex
 * id. Outbox rows are keyed by entity GUID, so we resolve vertex id → GUID via
 * {@code AtlasGraph.getVertex(id).__guid}. Vertices that can't be resolved
 * (deleted between the failure and this sink's run) are dropped — they can't
 * be repaired anyway.</p>
 *
 * <p>{@code ESWriteFailureRegistry.record} wraps every sink call in a
 * try/catch, so throwing here is safe for the caller's write path. Still, we
 * guard against unexpected input shapes defensively and log rather than throw.</p>
 */
public final class TagOutboxFailureSink implements ESWriteFailureRegistry.FailureSink {
    private static final Logger LOG = LoggerFactory.getLogger(TagOutboxFailureSink.class);

    private final AtlasGraph     graph;
    private final TagOutboxSink  sink;

    public TagOutboxFailureSink(AtlasGraph graph, TagOutboxSink sink) {
        this.graph = Objects.requireNonNull(graph, "graph");
        this.sink  = Objects.requireNonNull(sink, "sink");
    }

    @Override
    public void accept(ESWriteFailureRegistry.ESWriteFailure failure) {
        if (failure == null) return;

        // 1. Gather all vertex ids mentioned across the failed ops + failedVertexIds list.
        Set<String> vertexIds = collectVertexIds(failure);
        if (vertexIds.isEmpty()) {
            LOG.debug("TagOutboxFailureSink: no vertex ids found in failure (stage='{}')", failure.stage);
            return;
        }

        // 2. Resolve vertex ids → GUIDs. The outbox row key is entity_guid, not vertex id.
        Set<String> guids = resolveGuids(vertexIds);
        if (guids.isEmpty()) {
            LOG.warn("TagOutboxFailureSink: could not resolve any GUIDs for {} vertex ids " +
                    "(stage='{}') — dropping, nothing to enqueue", vertexIds.size(), failure.stage);
            return;
        }

        // 3. Enqueue via the instance sink (same code path as Surface 1).
        LOG.info("TagOutboxFailureSink: enqueueing {} GUID(s) from Surface 2 failure (stage='{}', vertex_ids={})",
                guids.size(), failure.stage, vertexIds.size());
        sink.enqueueInternal(guids);
    }

    /**
     * Collect every vertex id referenced by the failure — both the outer
     * {@code failedVertexIds} list and every key in each operation's payload map.
     * Also the per-operation {@code entityId} if present.
     */
    private static Set<String> collectVertexIds(ESWriteFailureRegistry.ESWriteFailure failure) {
        Set<String> ids = new LinkedHashSet<>();
        if (failure.failedVertexIds != null) {
            for (String id : failure.failedVertexIds) {
                if (id != null && !id.isEmpty()) ids.add(id);
            }
        }
        if (failure.operations != null) {
            for (ESDeferredOperation op : failure.operations) {
                if (op == null) continue;
                if (op.getEntityId() != null && !op.getEntityId().isEmpty()) {
                    ids.add(op.getEntityId());
                }
                if (op.getPayload() != null) {
                    for (String k : op.getPayload().keySet()) {
                        if (k != null && !k.isEmpty()) ids.add(k);
                    }
                }
            }
        }
        return ids;
    }

    /**
     * Resolve vertex ids to entity GUIDs via {@link AtlasGraph#getVertex(String)}
     * and {@link GraphHelper#getGuid(AtlasVertex)}. A vertex that's been deleted
     * or never existed is silently skipped (nothing to repair in ES either).
     */
    private Set<String> resolveGuids(Set<String> vertexIds) {
        Set<String> guids = new LinkedHashSet<>();
        for (String vid : vertexIds) {
            try {
                AtlasVertex v = graph.getVertex(vid);
                if (v == null) continue;
                String guid = GraphHelper.getGuid(v);
                if (guid != null && !guid.isEmpty()) guids.add(guid);
            } catch (Exception e) {
                LOG.warn("TagOutboxFailureSink: vertex lookup failed for id='{}': {}", vid, e.getMessage());
            }
        }
        return guids;
    }
}
