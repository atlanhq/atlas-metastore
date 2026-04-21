package org.apache.atlas.repository.assetsync;

import java.util.Objects;

/**
 * Slim outbox payload for the post-commit verify path (MS-1010, Option B).
 *
 * <p>Identifies one Atlas entity (by GUID) that was committed via JG but is
 * absent from Elasticsearch by the time the post-commit verifier checked.
 * The relay re-derives the ES doc from Cassandra/JG via
 * {@code RepairIndex.restoreByIds} and re-fires the index write.</p>
 *
 * <p>No payload data is stored — Cassandra is the source of truth and the
 * relay always reads the latest entity state.</p>
 */
public final class EntityGuidRef {
    private final String entityGuid;

    public EntityGuidRef(String entityGuid) {
        this.entityGuid = Objects.requireNonNull(entityGuid, "entityGuid");
    }

    public String getEntityGuid() { return entityGuid; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntityGuidRef)) return false;
        return entityGuid.equals(((EntityGuidRef) o).entityGuid);
    }

    @Override
    public int hashCode() { return entityGuid.hashCode(); }

    @Override
    public String toString() { return entityGuid; }
}
