package org.apache.atlas.auth.client.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class CassandraClient {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraClient.class);
    Configuration configuration;

    {
        try {
            configuration = ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }

    private static volatile CqlSession session;

    public void updateTableGCGraceSeconds(int gcGraceSeconds) {

        String CQLStatement = String.format("ALTER TABLE %s.edgestore WITH gc_grace_seconds = %d", configuration.getString("atlas.graph.storage.cql.keyspace", "atlas"), gcGraceSeconds);
        try {
            session.execute(CQLStatement);
            LOG.info("Updated gc_grace_seconds to 0 for table edgestore");
        } catch (Exception e) {
            LOG.error("Error updating gc_grace_seconds: ", e);
        }
    }

    public void close() {
        if (session != null) {
            session.close();
        }
    }

    // Get synchronous session
    public CassandraClient getInstance() {
        // Return thread-safe singleton instance
        if (session == null) {
            synchronized (CassandraClient.class) {
                if (session == null) {
                    session = CqlSession.builder()
                            .addContactPoint(new InetSocketAddress(configuration.getString("atlas.graph.storage.hostname", "atlas-cassandra.atlas.svc.cluster.local"), Integer.parseInt(configuration.getString("atlas.graph.storage.port", "9042"))))
                            .withLocalDatacenter("datacenter1")
                            .build();
                }
            }
        }
        return this;
    }
}
