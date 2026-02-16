package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GraphDatabase;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra+Elasticsearch implementation of GraphDatabase.
 * This replaces JanusGraph's GraphDatabase implementation.
 *
 * Configure via atlas-application.properties:
 *   atlas.graphdb.backend=cassandra
 *   atlas.GraphDatabase.impl=org.apache.atlas.repository.graphdb.cassandra.CassandraGraphDatabase
 *   atlas.cassandra.graph.hostname=localhost
 *   atlas.cassandra.graph.port=9042
 *   atlas.cassandra.graph.keyspace=atlas_graph
 *   atlas.cassandra.graph.datacenter=datacenter1
 */
public class CassandraGraphDatabase implements GraphDatabase<CassandraVertex, CassandraEdge> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraGraphDatabase.class);

    private static volatile CassandraGraph graphInstance;

    @Override
    public boolean isGraphLoaded() {
        return graphInstance != null;
    }

    @Override
    public AtlasGraph<CassandraVertex, CassandraEdge> getGraph() {
        if (graphInstance == null) {
            synchronized (CassandraGraphDatabase.class) {
                if (graphInstance == null) {
                    graphInstance = createGraph();
                }
            }
        }
        return graphInstance;
    }

    @Override
    public AtlasGraph<CassandraVertex, CassandraEdge> getGraphBulkLoading() {
        // For bulk loading, return the same graph instance
        // (in JanusGraph, this returns a graph with different tx settings)
        return getGraph();
    }

    @Override
    public void initializeTestGraph() {
        graphInstance = createGraph();
    }

    @Override
    public void cleanup() {
        if (graphInstance != null) {
            graphInstance.clear();
            graphInstance.shutdown();
            graphInstance = null;
        }
    }

    private CassandraGraph createGraph() {
        try {
            LOG.info("Creating CassandraGraph...");
            Configuration configuration = ApplicationProperties.get();
            CqlSession session = CassandraSessionProvider.getSession(configuration);
            CassandraGraph graph = new CassandraGraph(session);
            LOG.info("CassandraGraph created successfully.");
            return graph;
        } catch (AtlasException e) {
            throw new RuntimeException("Failed to create CassandraGraph", e);
        }
    }
}
