package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class CassandraSessionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraSessionProvider.class);

    private static final String CONFIG_PREFIX    = "atlas.cassandra.graph.";
    private static final String DEFAULT_KEYSPACE = "atlas_graph";
    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final int    DEFAULT_PORT     = 9042;
    private static final String DEFAULT_DC       = "datacenter1";

    private static volatile CqlSession session;

    public static CqlSession getSession(Configuration configuration) {
        if (session == null || session.isClosed()) {
            synchronized (CassandraSessionProvider.class) {
                if (session == null || session.isClosed()) {
                    session = createSession(configuration);
                }
            }
        }
        return session;
    }

    private static CqlSession createSession(Configuration configuration) {
        String hostname = configuration.getString(CONFIG_PREFIX + "hostname", DEFAULT_HOSTNAME);
        int    port     = configuration.getInt(CONFIG_PREFIX + "port", DEFAULT_PORT);
        String keyspace = configuration.getString(CONFIG_PREFIX + "keyspace", DEFAULT_KEYSPACE);
        String dc       = configuration.getString(CONFIG_PREFIX + "datacenter", DEFAULT_DC);

        LOG.info("Initializing Cassandra session: host={}, port={}, keyspace={}, dc={}", hostname, port, keyspace, dc);

        CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(hostname, port))
                .withLocalDatacenter(dc);

        CqlSession initSession = builder.build();

        // Create keyspace if it doesn't exist
        initSession.execute(
            "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
            " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        );
        initSession.close();

        // Reconnect with keyspace
        CqlSession keyspaceSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(hostname, port))
                .withLocalDatacenter(dc)
                .withKeyspace(keyspace)
                .build();

        createTables(keyspaceSession);

        LOG.info("Cassandra session initialized successfully for keyspace: {}", keyspace);

        return keyspaceSession;
    }

    private static void createTables(CqlSession session) {
        // Vertex table: stores all vertex properties as a JSON blob
        session.execute(
            "CREATE TABLE IF NOT EXISTS vertices (" +
            "  vertex_id text PRIMARY KEY," +
            "  properties text," +
            "  vertex_label text," +
            "  type_name text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp" +
            ")"
        );

        // Edges by out-vertex (for outgoing traversals)
        session.execute(
            "CREATE TABLE IF NOT EXISTS edges_out (" +
            "  out_vertex_id text," +
            "  edge_label text," +
            "  edge_id text," +
            "  in_vertex_id text," +
            "  properties text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp," +
            "  PRIMARY KEY ((out_vertex_id), edge_label, edge_id)" +
            ") WITH CLUSTERING ORDER BY (edge_label ASC, edge_id ASC)"
        );

        // Edges by in-vertex (for incoming traversals)
        session.execute(
            "CREATE TABLE IF NOT EXISTS edges_in (" +
            "  in_vertex_id text," +
            "  edge_label text," +
            "  edge_id text," +
            "  out_vertex_id text," +
            "  properties text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp," +
            "  PRIMARY KEY ((in_vertex_id), edge_label, edge_id)" +
            ") WITH CLUSTERING ORDER BY (edge_label ASC, edge_id ASC)"
        );

        // Edge lookup by ID
        session.execute(
            "CREATE TABLE IF NOT EXISTS edges_by_id (" +
            "  edge_id text PRIMARY KEY," +
            "  out_vertex_id text," +
            "  in_vertex_id text," +
            "  edge_label text," +
            "  properties text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp" +
            ")"
        );

        // Composite index simulation table (1:1 unique lookups)
        session.execute(
            "CREATE TABLE IF NOT EXISTS vertex_index (" +
            "  index_name text," +
            "  index_value text," +
            "  vertex_id text," +
            "  PRIMARY KEY ((index_name, index_value))" +
            ")"
        );

        // Edge property index table (1:1 lookups, e.g., relationship GUID → edge_id)
        session.execute(
            "CREATE TABLE IF NOT EXISTS edge_index (" +
            "  index_name text," +
            "  index_value text," +
            "  edge_id text," +
            "  PRIMARY KEY ((index_name, index_value))" +
            ")"
        );

        // Property index table (1:N lookups - multiple vertices per index key)
        session.execute(
            "CREATE TABLE IF NOT EXISTS vertex_property_index (" +
            "  index_name text," +
            "  index_value text," +
            "  vertex_id text," +
            "  PRIMARY KEY ((index_name, index_value), vertex_id)" +
            ")"
        );

        // Schema registry for property keys
        session.execute(
            "CREATE TABLE IF NOT EXISTS schema_registry (" +
            "  property_name text PRIMARY KEY," +
            "  property_class text," +
            "  cardinality text," +
            "  created_at timestamp" +
            ")"
        );

        // Dedicated TypeDef storage: fast primary-key lookup by type_name
        session.execute(
            "CREATE TABLE IF NOT EXISTS type_definitions (" +
            "  type_name text PRIMARY KEY," +
            "  type_category text," +
            "  vertex_id text," +
            "  created_at timestamp," +
            "  modified_at timestamp" +
            ")"
        );

        // TypeDef lookup by category (1:N — e.g. all ENTITY typedefs)
        session.execute(
            "CREATE TABLE IF NOT EXISTS type_definitions_by_category (" +
            "  type_category text," +
            "  type_name text," +
            "  vertex_id text," +
            "  PRIMARY KEY ((type_category), type_name)" +
            ") WITH CLUSTERING ORDER BY (type_name ASC)"
        );

        LOG.info("Cassandra graph tables created/verified.");
    }

    public static void shutdown() {
        if (session != null && !session.isClosed()) {
            session.close();
            session = null;
        }
    }
}
