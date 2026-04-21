package org.apache.atlas.repository.assetsync;

/**
 * Distributed lease abstraction (MS-1010).
 *
 * <p>Two implementations live alongside this interface today:
 * {@link AssetSyncLeaseManager} (Cassandra LWT, production), and the test
 * fakes used by the relay's unit tests. Splitting the contract lets tests
 * exercise the relay's leader-election branches without standing up Cassandra.</p>
 */
public interface LeaseManager {
    boolean tryAcquire(String jobName, int ttlSeconds);
    boolean heartbeat(String jobName, int ttlSeconds);
    void    release(String jobName);
    boolean isHeldByMe(String jobName);
    String  getPodId();
}
