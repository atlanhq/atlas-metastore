package org.apache.atlas.repository.outbox.shared;

/**
 * Centralised pod identity for outbox subsystems.
 *
 * <p>Resolved once on first class access from the {@code HOSTNAME} environment
 * variable (Kubernetes sets this to the pod name) with a PID fallback for
 * local development.</p>
 *
 * <p>Used by lease managers and outbox claim machinery so that all components
 * within the same pod present the same identity — required for correct
 * leader-election semantics (an LWT acquire followed by a heartbeat must
 * present the same owner string).</p>
 *
 * <p>Subsystem-neutral by design: both tag-outbox and (future) asset-sync
 * migrations can share this helper without pulling in subsystem-specific
 * naming.</p>
 */
public final class OutboxPodId {
    private static final String VALUE = resolve();

    private OutboxPodId() {}

    public static String get() { return VALUE; }

    private static String resolve() {
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isEmpty()) return hostname;
        return "local-" + ProcessHandle.current().pid();
    }
}
