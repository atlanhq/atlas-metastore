package org.apache.atlas.repository.assetsync;

/**
 * Centralised pod identity for the asset-sync subsystem (MS-1010).
 *
 * <p>Resolved once on first access from the {@code HOSTNAME} environment
 * variable (Kubernetes sets this to the pod name) with a PID fallback for
 * local dev. Used by the lease manager and the outbox claim machinery so
 * both refer to the same identity.</p>
 */
public final class AssetSyncPodId {
    private static final String VALUE = resolve();

    private AssetSyncPodId() {}

    public static String get() { return VALUE; }

    private static String resolve() {
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isEmpty()) return hostname;
        return "local-" + ProcessHandle.current().pid();
    }
}
