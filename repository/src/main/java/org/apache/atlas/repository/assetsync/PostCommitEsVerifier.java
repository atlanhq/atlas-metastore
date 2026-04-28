package org.apache.atlas.repository.assetsync;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.elasticsearch.AtlasElasticsearchDatabase;
import org.apache.atlas.type.AtlasType;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Post-commit ES presence verifier (MS-1010, Option B).
 *
 * <p>After every Atlas entity create/update commit, the calling code hands the
 * committed entity GUIDs here. We schedule an async ES presence check after a
 * short delay (past the ES refresh window) and, for any GUID that ES doesn't
 * have, push it through the {@link AssetSyncSink} into the outbox for the relay
 * to repair via {@code RepairIndex.restoreByIds}.</p>
 *
 * <p>Why this design (Option B):</p>
 * <ul>
 *     <li>Catches BOTH thrown JG ES failures AND silent ones (the standard ES
 *         IndexProvider sometimes logs and continues — IndexProvider override
 *         alone misses those).</li>
 *     <li>Doesn't fight JG's stored backend identifier check, doesn't depend
 *         on JG version.</li>
 *     <li>Atlas-level only — no JG plugin loading concerns.</li>
 * </ul>
 *
 * <p>Cost: one ES {@code _search} (terms query on {@code __guid}) per commit
 * batch, fired async ~2s after commit. Outbox enqueues happen only on actual
 * misses, so under normal operation this is a read-only sanity check.</p>
 */
public final class PostCommitEsVerifier {
    private static final Logger LOG = LoggerFactory.getLogger(PostCommitEsVerifier.class);

    /** Atlas stores the entity GUID in this ES field on the vertex_index. */
    private static final String GUID_FIELD = "__guid";

    /**
     * Process-wide singleton, installed by {@code AssetSyncOutboxService} on startup.
     * {@code EntityMutationService.executeESPostProcessing} calls
     * {@link #postCommit(Set)} which is null-safe when the service is disabled
     * or hasn't bootstrapped yet — no Spring DI cycle through the asset-sync
     * service is needed.
     */
    private static volatile PostCommitEsVerifier INSTANCE;

    public static void install(PostCommitEsVerifier verifier) { INSTANCE = verifier; }

    /** Convenience: enqueue the committed GUIDs for async ES verify. No-op if not installed. */
    public static void postCommit(Set<String> committedGuids) {
        PostCommitEsVerifier v = INSTANCE;
        if (v != null) v.verifyAsync(committedGuids);
    }

    private final AssetSyncSink sink;
    private final ScheduledExecutorService scheduler;
    private final boolean enabled;
    private final int     delaySeconds;
    private final int     batchSize;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public PostCommitEsVerifier(AssetSyncSink sink) {
        this.sink         = sink;
        this.enabled      = AtlasConfiguration.ASSET_SYNC_VERIFY_ENABLED.getBoolean();
        this.delaySeconds = AtlasConfiguration.ASSET_SYNC_VERIFY_DELAY_SECONDS.getInt();
        this.batchSize    = AtlasConfiguration.ASSET_SYNC_VERIFY_BATCH_SIZE.getInt();
        int poolSize      = AtlasConfiguration.ASSET_SYNC_VERIFY_THREAD_POOL_SIZE.getInt();

        this.scheduler = Executors.newScheduledThreadPool(poolSize, r -> {
            Thread t = new Thread(r, "asset-sync-verify");
            t.setDaemon(true);
            return t;
        });
        LOG.info("PostCommitEsVerifier initialized (enabled={}, delay={}s, batch={}, threads={})",
                enabled, delaySeconds, batchSize, poolSize);
    }

    /**
     * Schedule an async ES presence check for the given committed GUIDs.
     * Returns immediately. The actual check runs after the configured delay.
     */
    public void verifyAsync(Set<String> committedGuids) {
        if (!enabled || committedGuids == null || committedGuids.isEmpty() || !running.get()) return;
        // Defensive copy — caller may clear the original set after returning.
        Set<String> snapshot = new HashSet<>(committedGuids);
        try {
            scheduler.schedule(() -> verifyAndEnqueue(snapshot), delaySeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Scheduler may reject if shutting down — log + drop.
            LOG.warn("PostCommitEsVerifier: failed to schedule verify for {} guids: {}",
                    snapshot.size(), e.getMessage());
        }
    }

    public void shutdown() {
        running.set(false);
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOG.info("PostCommitEsVerifier: shutdown complete");
    }

    private void verifyAndEnqueue(Set<String> guids) {
        // Process in chunks so a giant tx doesn't trigger a huge terms query
        List<String> all = new ArrayList<>(guids);
        for (int i = 0; i < all.size(); i += batchSize) {
            int end = Math.min(i + batchSize, all.size());
            List<String> chunk = all.subList(i, end);
            try {
                Set<String> missing = findMissing(chunk);
                if (!missing.isEmpty()) {
                    LOG.warn("PostCommitEsVerifier: {} of {} committed GUIDs missing from ES — enqueueing for replay",
                            missing.size(), chunk.size());
                    sink.enqueueMissing(missing);
                }
            } catch (Exception e) {
                // ES read failed — conservatively enqueue all in this chunk.
                // Better to over-replay (idempotent) than miss real drift.
                LOG.warn("PostCommitEsVerifier: ES verify failed for chunk of {}, enqueueing all conservatively: {}",
                        chunk.size(), e.getMessage());
                sink.enqueueMissing(new HashSet<>(chunk));
            }
        }
    }

    /**
     * Issues a terms query against the vertex_index for the given GUIDs and
     * returns the subset that ES did not return (= missing from ES).
     */
    @SuppressWarnings("unchecked")
    private Set<String> findMissing(List<String> guids) throws Exception {
        RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
        if (client == null) {
            // ES client not available — treat as missing (relay will repair when ES is back)
            return new HashSet<>(guids);
        }

        Map<String, Object> termsClause = Collections.singletonMap(GUID_FIELD, guids);
        Map<String, Object> query = Collections.singletonMap("terms", termsClause);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("size", guids.size());
        body.put("_source", Collections.singletonList(GUID_FIELD));
        body.put("query", query);

        Request req = new Request("POST", "/" + Constants.VERTEX_INDEX_NAME + "/_search");
        req.setEntity(new StringEntity(AtlasType.toJson(body), ContentType.APPLICATION_JSON));

        Response resp = client.performRequest(req);
        int status = resp.getStatusLine().getStatusCode();
        if (status < 200 || status >= 300) {
            throw new RuntimeException("ES search returned status " + status);
        }

        String respBody = EntityUtils.toString(resp.getEntity());
        Map<String, Object> parsed = AtlasType.fromJson(respBody, Map.class);
        Map<String, Object> hits = (Map<String, Object>) parsed.get("hits");
        List<Map<String, Object>> hitList = hits == null ? Collections.emptyList()
                : (List<Map<String, Object>>) hits.get("hits");

        Set<String> found = new HashSet<>();
        for (Map<String, Object> hit : hitList) {
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            if (source == null) continue;
            Object guidField = source.get(GUID_FIELD);
            if (guidField instanceof String) found.add((String) guidField);
        }

        Set<String> missing = new HashSet<>(guids);
        missing.removeAll(found);
        return missing;
    }
}
