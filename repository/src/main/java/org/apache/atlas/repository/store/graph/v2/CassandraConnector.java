package org.apache.atlas.repository.store.graph.v2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.AtlasConstants.DEFAULT_CLUSTER_NAME;
import static org.apache.atlas.repository.audit.CassandraBasedAuditRepository.CASSANDRA_CLUSTERNAME_PROPERTY;
import static org.apache.atlas.repository.audit.CassandraBasedAuditRepository.CASSANDRA_HOSTNAME_PROPERTY;
import static org.apache.atlas.repository.audit.CassandraBasedAuditRepository.CASSANDRA_NEW_KEYSPACE_PROPERTY;

public class CassandraConnector {
    private static final Logger LOG      = LoggerFactory.getLogger(EntityGraphMapper.class);

    private static CqlSession cassSession;
    private static String keyspace;

    private static ObjectMapper objectMapper = new ObjectMapper();

    static {
        try {
            keyspace = ApplicationProperties.get().getString(CASSANDRA_NEW_KEYSPACE_PROPERTY, "atlandata2_0_temp");
            String hostname = ApplicationProperties.get().getString(CASSANDRA_HOSTNAME_PROPERTY, "localhost");
            String clusterName = ApplicationProperties.get().getString(CASSANDRA_CLUSTERNAME_PROPERTY, DEFAULT_CLUSTER_NAME);
            int port = 9042;

            cassSession = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(hostname, 9042))
                    .withLocalDatacenter("datacenter1")
                    .withKeyspace(keyspace)
                    .build();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> getVertexProperties(String vertexId) {
        String query = "SELECT * FROM vertices where id = '" + vertexId + "'";
        ResultSet resultSet = cassSession.execute(query);

        Map<String, Object> ret = new HashMap<>();
        for (Row row : resultSet) {
            System.out.println("Vertex: " + AtlasType.toJson(row));
            return convertRowToMap(row);
        }
        LOG.info("Returning null for vertex {}", vertexId);
        return null;
    }

    public static Map<String, Object> getVertexPropertiesByGuid(String guid) {
        String query = "SELECT * FROM vertices where guid = '" + guid + "'";
        ResultSet resultSet = cassSession.execute(query);

        for (Row row : resultSet) {
            System.out.println("Vertex: " + AtlasType.toJson(row));
            return convertRowToMap(row);
        }
        LOG.info("Returning null vertex for GUID {}", guid);
        return null;
    }

    public static Map<String, Object> getEdgeProperties(String edgeId) {
        String query = "SELECT * FROM edges where id = '" + edgeId + "'";
        ResultSet resultSet = cassSession.execute(query);

        for (Row row : resultSet) {
            System.out.println("Edge: " + AtlasType.toJson(row));
            return convertRowToMap(row);
        }
        LOG.info("Returning null for edge {}", edgeId);
        return null;
    }

    public static Map<String, Object> convertRowToMap(Row row) {
        Map<String, Object> map = new HashMap<>();
        row.getColumnDefinitions().forEach(column -> {
            String columnName = column.getName().toString();
            Object columnValue = row.getObject(columnName);

            if (columnName.equals("json_data")) {
                Map<String, Object> interimValue = null;
                try {
                    interimValue = objectMapper.readValue(columnValue.toString(), new TypeReference<Map<String, Object>>() {});
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
                //Map<String, Object> interimValue = AtlasType.fromJson(columnValue.toString(), Map.class);

                //columnValue = AtlasType.fromJson(columnValue, Map.class);

                for (String attribute : interimValue.keySet()) {
                    map.put(attribute, interimValue.get(attribute));
                }
            } else {
                map.put(columnName, columnValue);
            }

        });
        return map;
    }

    public static Map<String, Object> tempPutTagMap(String tagVertexId, String typeName, String entityGuid) throws JsonProcessingException {

        String json_data = "{\"id\":"+ tagVertexId +",\"__typeName\":\""+ typeName +"\",\"__modifiedBy\":\"service-account-atlan-argo\",\"__state\":\"ACTIVE\",\"__propagate\":true,\"__restrictPropagationThroughLineage\":false,\"__removePropagations\":true,\"__restrictPropagationThroughHierarchy\":false,\"__entityGuid\":\" " +entityGuid+ "\",\"__createdBy\":\"service-account-atlan-argo\",\"__modificationTimestamp\":1743060425553,\"__entityStatus\":\"ACTIVE\",\"__timestamp\":1743060425553}";

        String insert = "INSERT INTO vertices (id, name, created_at, json_data) VALUES (?, ?, ?, ?)";
        PreparedStatement preparedStmt = cassSession.prepare(insert);
        BoundStatement boundStmt  = preparedStmt.bind( tagVertexId, "placeholder", System.currentTimeMillis(), json_data);
        cassSession.execute(boundStmt);

        return  objectMapper.readValue(json_data, new TypeReference<Map<String, Object>>() {});
    }

    /*public static void updateEntity(Map<String, Object> entityMap) {

        String query = "UPDATE vertices SET json_data = " + AtlasType.toJson(entityMap) + " WHERE id = " +  entityMap.get("id");
        cassSession.execute(query);

    }*/

    public static void putEntities(Collection<Map<String, Object>> entitiesMap) {
        StringBuilder batchQuery = new StringBuilder();
        batchQuery.append("BEGIN BATCH ");

        for (Map entry : entitiesMap) {
            String update = "UPDATE vertices SET json_data = '" + AtlasType.toJson(entry) + "' WHERE id = '" +  entry.get("id") + "'";
            batchQuery.append(update).append(";");
        }

        batchQuery.append("APPLY BATCH;");
        cassSession.execute(batchQuery.toString());
    }
}
