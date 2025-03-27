package org.apache.atlas.repository.store.graph.v3;

public interface MigrationService extends Runnable {
    void startMigration() throws Exception;
}
