package org.apache.atlas.repository.graphdb.migrator;

import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.graphdb.types.system.SystemTypeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A thread-safe, pre-loaded TypeInspector backed by a HashMap.
 *
 * Replaces per-thread StandardJanusGraphTx for type resolution during migration.
 * All type definitions (PropertyKeys + EdgeLabels) are loaded once at startup
 * into a HashMap, eliminating per-relation Cassandra lookups during scanning.
 *
 * Thread safety: uses ConcurrentHashMap. The bulk of entries are populated during
 * construction; rare cache misses (JG internal types not returned by mgmt.getRelationTypes())
 * are resolved via the backing tx and cached on first access.
 * Type metadata (multiplicity, sortKey, dataType, etc.) is force-loaded during
 * construction so all subsequent reads are pure in-memory.
 */
public class CachedTypeInspector implements TypeInspector, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CachedTypeInspector.class);

    private final Map<Long, RelationType> typeCache;
    private final StandardJanusGraphTx backingTx;
    private final AtomicLong cacheMissLogged = new AtomicLong(0);

    public CachedTypeInspector(StandardJanusGraph janusGraph) {
        this.typeCache = new ConcurrentHashMap<>();

        // Open a read-only tx with a large vertex cache to hold all types.
        // This tx stays open for the lifetime of the migration to keep
        // the type objects' internal property caches valid.
        this.backingTx = (StandardJanusGraphTx) janusGraph.buildTransaction()
            .readOnly()
            .vertexCacheSize(20000)
            .start();

        preloadAllTypes(janusGraph);

        LOG.info("CachedTypeInspector initialized: {} user-defined types pre-loaded into HashMap", typeCache.size());
    }

    private void preloadAllTypes(StandardJanusGraph janusGraph) {
        JanusGraphManagement mgmt = janusGraph.openManagement();
        try {
            int pkCount = 0;
            for (PropertyKey pk : mgmt.getRelationTypes(PropertyKey.class)) {
                long typeId = pk.longId();
                RelationType rt = backingTx.getExistingRelationType(typeId);
                if (rt != null) {
                    // Force-load all metadata to ensure thread-safe reads later.
                    // After this, all property accesses are pure in-memory cache hits.
                    forceLoadMetadata(rt);
                    typeCache.put(typeId, rt);
                    pkCount++;
                }
            }

            int elCount = 0;
            for (EdgeLabel el : mgmt.getRelationTypes(EdgeLabel.class)) {
                long typeId = el.longId();
                RelationType rt = backingTx.getExistingRelationType(typeId);
                if (rt != null) {
                    forceLoadMetadata(rt);
                    typeCache.put(typeId, rt);
                    elCount++;
                }
            }

            LOG.info("Pre-loaded {} PropertyKeys and {} EdgeLabels", pkCount, elCount);
        } finally {
            mgmt.rollback();
        }
    }

    /**
     * Force-load all metadata that EdgeSerializer.parseRelation() accesses.
     * This ensures the type object's internal caches are fully populated
     * before any concurrent scanner thread touches them.
     */
    private void forceLoadMetadata(RelationType rt) {
        try {
            rt.name();
            rt.isPropertyKey();
            rt.isEdgeLabel();

            if (rt instanceof InternalRelationType) {
                InternalRelationType irt = (InternalRelationType) rt;
                irt.multiplicity();
                irt.getSortKey();
                irt.getSortOrder();
                irt.getSignature();
            }

            if (rt instanceof PropertyKey) {
                ((PropertyKey) rt).dataType();
                ((PropertyKey) rt).cardinality();
            }
        } catch (Exception e) {
            LOG.warn("Failed to force-load metadata for type {} (id={}): {}",
                     rt.name(), rt.longId(), e.getMessage());
        }
    }

    @Override
    public RelationType getExistingRelationType(long id) {
        // System types are statically cached in JanusGraph — always available
        if (IDManager.isSystemRelationTypeId(id)) {
            return (RelationType) SystemTypeManager.getSystemType(id);
        }
        RelationType cached = typeCache.get(id);
        if (cached != null) {
            return cached;
        }
        // Fallback: some JG internal types aren't returned by mgmt.getRelationTypes()
        // but are referenced in schema vertex entries. Query the backing tx.
        try {
            RelationType fromTx = backingTx.getExistingRelationType(id);
            if (fromTx != null) {
                typeCache.put(id, fromTx);
                if (cacheMissLogged.incrementAndGet() <= 10) {
                    LOG.debug("CachedTypeInspector cache miss resolved via backing tx: id={}, name={}",
                             id, fromTx.name());
                }
            }
            return fromTx;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public VertexLabel getExistingVertexLabel(long id) {
        throw new UnsupportedOperationException("getExistingVertexLabel not needed for migration");
    }

    @Override
    public boolean containsRelationType(String name) {
        throw new UnsupportedOperationException("containsRelationType not needed for migration");
    }

    @Override
    public RelationType getRelationType(String name) {
        throw new UnsupportedOperationException("getRelationType not needed for migration");
    }

    public int size() {
        return typeCache.size();
    }

    @Override
    public void close() {
        try {
            backingTx.rollback();
        } catch (Exception e) {
            LOG.debug("Error closing backing tx", e);
        }
    }
}
