package org.apache.atlas.repository.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasEntityHeaders;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.type.AtlasTypeRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestEntityStreamUtil {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static class EntityCreationRequest {
        private final String jsonRequest;
        private List<AtlasEntity> entities;
        private Map<String, AtlasEntityHeader> guidHeaderMap;

        public EntityCreationRequest(String jsonRequest) {
            this.jsonRequest = jsonRequest;
            this.entities = new ArrayList<>();
            this.guidHeaderMap = new HashMap<>();
        }

        public EntityStream toEntityStream(AtlasTypeRegistry typeRegistry) throws IOException {
            JsonNode root = OBJECT_MAPPER.readTree(jsonRequest);
            ArrayNode entitiesNode = (ArrayNode) root.get("entities");

            for (JsonNode entityNode : entitiesNode) {
                AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
                AtlasEntity entity = new AtlasEntity();

                entity.setTypeName(entityNode.get("typeName").asText());

                // Handle attributes
                JsonNode attributesNode = entityNode.get("attributes");
                Map<String, Object> attributes = new HashMap<>();
                attributesNode.fields().forEachRemaining(field ->
                        attributes.put(field.getKey(), field.getValue().asText())
                );
                entity.setAttributes(attributes);

                entityWithExtInfo.setEntity(entity);
                entities.add(entity);
            }

            return new AtlasEntityStream(entities);
        }

        public void updateWithMutationResponse(EntityMutationResponse response) {
            // Update entities with assigned GUIDs
            for (AtlasEntityHeader header : response.getCreatedEntities()) {
                guidHeaderMap.put(header.getGuid(), header);
            }
        }

        public AtlasEntityHeaders toEntityHeaders() {
            AtlasEntityHeaders headers = new AtlasEntityHeaders();
            headers.setGuidHeaderMap(guidHeaderMap);
            return headers;
        }
    }

    public static EntityCreationRequest fromJson(String json) {
        return new EntityCreationRequest(json);
    }

} 