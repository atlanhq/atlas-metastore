package org.apache.atlas.repository.tagoutbox;

import io.micrometer.core.instrument.Timer;
import org.apache.atlas.repository.assetsync.EntityGuidRef;
import org.apache.atlas.repository.assetsync.Outbox;
import org.apache.atlas.repository.assetsync.OutboxEntry;
import org.apache.atlas.repository.assetsync.OutboxEntryId;
import org.apache.atlas.repository.outbox.shared.OutboxMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/**
 * Write side of the tag-outbox. Two producers call into this sink:
 *
 * <ul>
 *     <li><b>Surface 1 — propagation failures</b> ({@code EntityGraphMapper.flushTagDenormToES}):
 *         three call sites inside that method dispatch failed GUIDs here via the
 *         static {@link #enqueue(Set)} accessor. Accessed statically because
 *         {@code flushTagDenormToES} is a method on an already-wired Spring bean
 *         — injecting another dependency would risk bean cycles.</li>
 *     <li><b>Surface 2 — direct-attachment failures</b>: {@code TagOutboxFailureSink}
 *         (installed on {@code ESWriteFailureRegistry}) extracts GUIDs from the
 *         failed {@code ESDeferredOperation}s and calls {@link #enqueue(Set)} with them.</li>
 * </ul>
 *
 * <p>Both paths end up at the same row in {@code tag_outbox} — the relay and
 * reconciler have no idea which producer emitted a given row, and don't need to.</p>
 *
 * <p><b>Best-effort:</b> enqueue failures are counted as write_errors and logged,
 * never rethrown. A failure here must never propagate back into the caller
 * (which is in the middle of either a tag commit or a post-commit hook).</p>
 */
public final class TagOutboxSink {
    private static final Logger LOG = LoggerFactory.getLogger(TagOutboxSink.class);

    private final Outbox<EntityGuidRef> outbox;
    private final OutboxMetrics         metrics;

    /**
     * Process-wide singleton, installed by {@link TagOutboxService} on startup.
     * Mirrors the {@code AssetSyncSink}/{@code PostCommitEsVerifier.install} pattern
     * so call sites in already-Spring-wired beans (like {@code EntityGraphMapper})
     * can reach the sink via a static accessor without introducing a DI cycle or
     * an additional {@code @Autowired} field. Null-safe: {@link #enqueue(Set)} is a
     * no-op when the outbox subsystem is disabled or hasn't bootstrapped yet.
     */
    private static volatile TagOutboxSink INSTANCE;

    public static void install(TagOutboxSink sink) { INSTANCE = sink; }

    /**
     * Null-safe static entry point. No-op when the subsystem is disabled
     * ({@code atlas.tag.outbox.enabled=false}) or when {@link TagOutboxService}
     * init has not yet installed the singleton.
     */
    public static void enqueue(Set<String> guids) {
        TagOutboxSink s = INSTANCE;
        if (s != null) s.enqueueInternal(guids);
    }

    public TagOutboxSink(Outbox<EntityGuidRef> outbox, OutboxMetrics metrics) {
        this.outbox  = Objects.requireNonNull(outbox, "outbox");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    /** Instance entry point; exposed for tests and the Surface 2 bridge. */
    public void enqueueInternal(Set<String> guids) {
        if (guids == null || guids.isEmpty()) return;
        Instant now = Instant.now();
        for (String guid : guids) {
            if (guid == null || guid.isEmpty()) continue;
            try {
                OutboxEntry<EntityGuidRef> entry = new OutboxEntry<>(
                        new OutboxEntryId(guid, ""),
                        new EntityGuidRef(guid),
                        0,                  // fresh enqueue — attempt count starts at 0
                        now,
                        null                // lastAttemptedAt: null for new entries
                );
                Timer.Sample t = metrics.startWriteTimer();
                try {
                    outbox.enqueue(entry);
                    metrics.recordWrite();
                } finally {
                    metrics.stopWriteTimer(t);
                }
            } catch (Throwable t) {
                metrics.recordWriteError(reasonOf(t));
                LOG.error("TagOutboxSink: enqueue failed for guid='{}': {}", guid, t.getMessage(), t);
            }
        }
    }

    private static String reasonOf(Throwable t) {
        if (t == null) return "unknown";
        String name = t.getClass().getSimpleName();
        return name.isEmpty() ? "unknown" : name;
    }
}
