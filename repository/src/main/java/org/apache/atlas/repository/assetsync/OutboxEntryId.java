package org.apache.atlas.repository.assetsync;

import java.util.Objects;

/**
 * Composite identifier for an outbox entry (MS-1010).
 *
 * <p>Generic two-part natural key. For the JG-failure asset-sync use case the
 * pair is {@code (indexName, docId)} — JG mixed-index store name and ES {@code _id}.
 * Other consumers can use whatever pair makes sense for their PK.</p>
 *
 * <p>Multiple failure events with the same {@code (partA, partB)} natural-dedupe
 * to one row at the schema level — correct because every replay re-derives state
 * from the source of truth, so collapsing duplicate failure events is safe.</p>
 */
public final class OutboxEntryId {

    private final String partA;
    private final String partB;

    public OutboxEntryId(String partA, String partB) {
        this.partA = Objects.requireNonNull(partA, "partA");
        this.partB = Objects.requireNonNull(partB, "partB");
    }

    /** First component of the composite key (e.g. JG index store name). */
    public String getPartA() { return partA; }

    /** Second component of the composite key (e.g. ES {@code _id}). */
    public String getPartB() { return partB; }

    // Asset-sync friendly aliases
    public String getIndexName() { return partA; }
    public String getDocId()     { return partB; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OutboxEntryId)) return false;
        OutboxEntryId that = (OutboxEntryId) o;
        return partA.equals(that.partA) && partB.equals(that.partB);
    }

    @Override
    public int hashCode() { return Objects.hash(partA, partB); }

    @Override
    public String toString() { return partA + ":" + partB; }
}
