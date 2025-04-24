package org.apache.atlas.auth.client.jmx;

import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.HashMap;
import java.util.Map;

public class JMXCassandraConnect {
    private static final Logger LOG = LoggerFactory.getLogger(JMXCassandraConnect.class);
    Configuration configuration;

    MBeanServerConnection mbeanServerConnection;

    public JMXCassandraConnect() {
        try {
            configuration = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.error("Error initializing configuration: ", e);
        }
        String cassandraHost = configuration.getString("atlas.graph.storage.hostname", "atlas-cassandra.atlas.svc.cluster.local");
        String jmxUrl = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", cassandraHost, 7199);
        this.mbeanServerConnection = initConn(jmxUrl);
    }

    public MBeanServerConnection initConn(String jmxUrl) {
        try {
            JMXServiceURL serviceURL = new JMXServiceURL(jmxUrl);
            Map<String, Object> env = new HashMap<>();
            JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceURL, env);
            return jmxConnector.getMBeanServerConnection();
        } catch (Exception e) {
            LOG.error("Error connecting to JMX: ", e);
            return null;
        }
    }

    public void invokeCassandraGarbageCollection() throws MalformedObjectNameException {
        String keyspace = configuration.getString("atlas.graph.storage.cql.keyspace", "atlas");
        String table = configuration.getString("atlas.graph.storage.cql.table", "edgestore");
        ObjectName name = new ObjectName("org.apache.cassandra.db:type=StorageService");
        Object[] params = new Object[]{
                "NONE",
                2,
                keyspace,
                new String[]{table}
        };
        String[] signature = new String[]{
                String.class.getName(),      // Signature for tombstoneOption (java.lang.String)
                int.class.getName(),         // Signature for jobs (primitive int)
                String.class.getName(),      // Signature for keyspace (java.lang.String)
                String[].class.getName()     // Signature for tableNames ([Ljava.lang.String;) - String array
        };
        try {
            mbeanServerConnection.invoke(name, "garbageCollect", params, signature);
            LOG.info("Garbage collection initiated successfully for {}.{}", keyspace, table);
        } catch (Exception e) {
            LOG.error("Error invoking garbage collection: ", e);
        }
    }







}
