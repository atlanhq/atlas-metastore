package org.apache.atlas.repository.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.Map;

public class Validation  {

    public static void main(String[] args)  {
        CqlSession cqlSession = null;
        try {
            CassandraConfig config = new CassandraConfig();
            cqlSession = config.cqlSession();
            VertexRetrievalService batchVertexRetrievalService = new VertexRetrievalService(cqlSession, new ObjectMapper());
            Map<String, DynamicVertex> vertexPropertiesMap = batchVertexRetrievalService.retrieveVertices(Arrays.asList("40964232", "122884120"));
            System.out.println("vertexPropertiesMap = " + vertexPropertiesMap);

        } catch (Exception e) {
            System.out.println("Error while closing cqlSession: " + e.getMessage());
        } finally {
            if (cqlSession != null) {
                cqlSession.close();
            }
        }
    }
}
