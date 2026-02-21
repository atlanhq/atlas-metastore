package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Buffers graph mutations in memory until commit or rollback.
 * This provides application-level transaction semantics over Cassandra.
 */
public class TransactionBuffer {

    private final Map<String, CassandraVertex> newVertices     = new LinkedHashMap<>();
    private final Map<String, CassandraVertex> dirtyVertices   = new LinkedHashMap<>();
    private final Map<String, CassandraVertex> removedVertices = new LinkedHashMap<>();
    private final Map<String, CassandraEdge>   newEdges        = new LinkedHashMap<>();
    private final Map<String, CassandraEdge>   dirtyEdges      = new LinkedHashMap<>();
    private final Map<String, CassandraEdge>   removedEdges    = new LinkedHashMap<>();

    public void addVertex(CassandraVertex vertex) {
        newVertices.put(vertex.getIdString(), vertex);
    }

    public void markVertexDirty(CassandraVertex vertex) {
        if (!newVertices.containsKey(vertex.getIdString())) {
            dirtyVertices.put(vertex.getIdString(), vertex);
        }
    }

    public void removeVertex(CassandraVertex vertex) {
        String id = vertex.getIdString();
        newVertices.remove(id);
        dirtyVertices.remove(id);
        removedVertices.put(id, vertex);
    }

    public void addEdge(CassandraEdge edge) {
        newEdges.put(edge.getIdString(), edge);
    }

    public void markEdgeDirty(CassandraEdge edge) {
        if (!newEdges.containsKey(edge.getIdString())) {
            dirtyEdges.put(edge.getIdString(), edge);
        }
    }

    public void removeEdge(CassandraEdge edge) {
        String id = edge.getIdString();
        newEdges.remove(id);
        dirtyEdges.remove(id);
        removedEdges.put(id, edge);
    }

    public boolean isEdgeRemoved(String edgeId) {
        return removedEdges.containsKey(edgeId);
    }

    public List<CassandraVertex> getNewVertices() {
        return new ArrayList<>(newVertices.values());
    }

    public List<CassandraVertex> getDirtyVertices() {
        return new ArrayList<>(dirtyVertices.values());
    }

    public List<CassandraVertex> getRemovedVertices() {
        return new ArrayList<>(removedVertices.values());
    }

    public List<CassandraEdge> getNewEdges() {
        return new ArrayList<>(newEdges.values());
    }

    public List<CassandraEdge> getDirtyEdges() {
        return new ArrayList<>(dirtyEdges.values());
    }

    public List<CassandraEdge> getRemovedEdges() {
        return new ArrayList<>(removedEdges.values());
    }

    public List<CassandraEdge> getEdgesForVertex(String vertexId, AtlasEdgeDirection direction, String edgeLabel) {
        List<CassandraEdge> result = new ArrayList<>();

        for (CassandraEdge edge : newEdges.values()) {
            if (removedEdges.containsKey(edge.getIdString())) {
                continue;
            }

            if (edgeLabel != null && !edgeLabel.equals(edge.getLabel())) {
                continue;
            }

            boolean matches = false;
            switch (direction) {
                case OUT:
                    matches = edge.getOutVertexId().equals(vertexId);
                    break;
                case IN:
                    matches = edge.getInVertexId().equals(vertexId);
                    break;
                case BOTH:
                    matches = edge.getOutVertexId().equals(vertexId) || edge.getInVertexId().equals(vertexId);
                    break;
            }

            if (matches) {
                result.add(edge);
            }
        }

        return result;
    }

    public void clear() {
        newVertices.clear();
        dirtyVertices.clear();
        removedVertices.clear();
        newEdges.clear();
        dirtyEdges.clear();
        removedEdges.clear();
    }

    public boolean isEmpty() {
        return newVertices.isEmpty() && dirtyVertices.isEmpty() && removedVertices.isEmpty()
                && newEdges.isEmpty() && dirtyEdges.isEmpty() && removedEdges.isEmpty();
    }
}
