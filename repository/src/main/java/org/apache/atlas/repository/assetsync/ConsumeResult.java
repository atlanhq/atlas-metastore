package org.apache.atlas.repository.assetsync;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Per-entry outcome from {@link OutboxConsumer#consume}.
 *
 * <p>Three classifications:
 * <ul>
 *     <li><b>succeeded</b> — entries that delivered cleanly. Relay marks them done and removes from outbox.</li>
 *     <li><b>retryable</b> — transient failures (5xx, 429, IO). Relay increments attempt count and releases for next poll cycle.</li>
 *     <li><b>permanentlyFailed</b> — non-retryable failures (4xx, mapping errors). Relay moves them to FAILED state for reconciliation.</li>
 * </ul>
 *
 * <p>Each entry id appears in exactly one bucket. Returning an id in zero or
 * multiple buckets is a consumer bug — the relay logs but won't double-process.</p>
 */
public final class ConsumeResult {

    private final List<OutboxEntryId>            succeeded;
    private final List<OutboxEntryId>            retryable;
    private final Map<OutboxEntryId, Throwable>  permanentlyFailed;

    public ConsumeResult(List<OutboxEntryId> succeeded,
                         List<OutboxEntryId> retryable,
                         Map<OutboxEntryId, Throwable> permanentlyFailed) {
        this.succeeded         = succeeded         != null ? succeeded         : Collections.emptyList();
        this.retryable         = retryable         != null ? retryable         : Collections.emptyList();
        this.permanentlyFailed = permanentlyFailed != null ? permanentlyFailed : Collections.emptyMap();
    }

    public List<OutboxEntryId>           getSucceeded()         { return succeeded; }
    public List<OutboxEntryId>           getRetryable()         { return retryable; }
    public Map<OutboxEntryId, Throwable> getPermanentlyFailed() { return permanentlyFailed; }

    public int total() {
        return succeeded.size() + retryable.size() + permanentlyFailed.size();
    }

    public static ConsumeResult allSucceeded(List<OutboxEntryId> ids) {
        return new ConsumeResult(new ArrayList<>(ids), Collections.emptyList(), Collections.emptyMap());
    }

    public static ConsumeResult empty() {
        return new ConsumeResult(Collections.emptyList(), Collections.emptyList(), Collections.emptyMap());
    }
}
