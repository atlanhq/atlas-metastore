package org.apache.atlas.auth.client.heracles;

import org.apache.atlas.auth.client.config.AuthConfig;
import org.apache.atlas.auth.client.heracles.models.HeraclesGroupViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.HeraclesRoleViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.HeraclesUserViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.IdentityGroupRepresentation;
import org.apache.atlas.auth.client.heracles.models.IdentityRoleRepresentation;
import org.apache.atlas.auth.client.heracles.models.IdentityUserRepresentation;
import org.apache.atlas.exception.AtlasBaseException;
import org.keycloak.representations.idm.UserRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AtlasHeraclesClient {
    public final static Logger LOG = LoggerFactory.getLogger(AtlasHeraclesClient.class);

    private static HeraclesRestClient HERACLES;
    private static AtlasHeraclesClient HERACLES_CLIENT;

    public AtlasHeraclesClient() {}

    public static AtlasHeraclesClient getHeraclesClient() {
        if(Objects.isNull(HERACLES_CLIENT)) {
            LOG.info("Initializing Heracles client..");
            try{
                init(AuthConfig.getConfig());
            } catch (Exception e) {
                LOG.error("Error initializing Heracles client", e);
            }
        }
        return HERACLES_CLIENT;
    }

    private static void init(AuthConfig authConfig) {
        synchronized (AtlasHeraclesClient.class) {
            if (HERACLES == null) {
                HERACLES = new HeraclesRestClient(authConfig);
                HERACLES_CLIENT = new AtlasHeraclesClient();
            }
        }
    }

    public List<UserRepresentation> getUsersMappings(int start, int size, String[] columns) throws AtlasBaseException {;
        List<HeraclesUserViewRepresentation> views =  HERACLES.getUsersMappings(start, size, HeraclesUserViewRepresentation.sortBy, columns).body();
        return views.stream().map(x -> {
            UserRepresentation userRepresentation = new UserRepresentation();
            userRepresentation.setId(x.getId());
            userRepresentation.setUsername(x.getUsername());
            userRepresentation.setRealmRoles(x.getRoles());
            userRepresentation.setGroups(x.getGroups());
            return userRepresentation;
        }).collect(Collectors.toList());
    }

    public List<HeraclesRoleViewRepresentation> getRolesMappings(int start, int size,  String[] columns) throws AtlasBaseException {
        return HERACLES.getRolesMappings(start, size, HeraclesRoleViewRepresentation.sortBy, columns).body();
    }

    /**
     * Fetch groups from Heracles API (v2) with relation lookups
     * 
     * @param start Offset for pagination
     * @param size The numbers of items to return
     * @param columns Column names to project
     * @return List of groups
     */
    public List<HeraclesGroupViewRepresentation> getGroupsMappingsV2(int start, int size, String[] columns) throws AtlasBaseException {
        var response = HERACLES.getGroupsV2(start, size, new String[]{HeraclesGroupViewRepresentation.sortBy}, columns, null, null, null);
        var body = response.body();
        return body != null ? body.getRecords() : null;
    }

    // --- Identity API (Redis-backed) ---

    public IdentityRoleRepresentation getIdentityRoleById(String roleId, String... columns) throws AtlasBaseException {
        String filter = String.format("{\"id\":{\"$eq\":\"%s\"}}", roleId);
        List<IdentityRoleRepresentation> results = HERACLES.getIdentityRoles(filter, columns);
        return (results != null && !results.isEmpty()) ? results.get(0) : null;
    }

    public IdentityUserRepresentation getIdentityUserById(String userId, String... columns) throws AtlasBaseException {
        String filter = String.format("{\"id\":{\"$eq\":\"%s\"}}", userId);
        List<IdentityUserRepresentation> results = HERACLES.getIdentityUsers(filter, columns);
        return (results != null && !results.isEmpty()) ? results.get(0) : null;
    }

    public IdentityGroupRepresentation getIdentityGroupById(String groupId, String... columns) throws AtlasBaseException {
        String filter = String.format("{\"id\":{\"$eq\":\"%s\"}}", groupId);
        List<IdentityGroupRepresentation> results = HERACLES.getIdentityGroups(filter, columns);
        return (results != null && !results.isEmpty()) ? results.get(0) : null;
    }

    public List<IdentityUserRepresentation> getIdentityUsersByIds(List<String> userIds, String... columns) throws AtlasBaseException {
        String ids = userIds.stream().map(id -> "\"" + id + "\"").collect(Collectors.joining(","));
        String filter = String.format("{\"id\":{\"$in\":[%s]}}", ids);
        return HERACLES.getIdentityUsers(filter, columns);
    }

    public List<IdentityRoleRepresentation> getIdentityRolesByIds(List<String> roleIds, String... columns) throws AtlasBaseException {
        String ids = roleIds.stream().map(id -> "\"" + id + "\"").collect(Collectors.joining(","));
        String filter = String.format("{\"id\":{\"$in\":[%s]}}", ids);
        return HERACLES.getIdentityRoles(filter, columns);
    }

    public List<IdentityGroupRepresentation> getIdentityGroupsForUser(String userId) throws AtlasBaseException {
        String filter = String.format("{\"id\":{\"$eq\":\"%s\"}}", userId);
        List<IdentityUserRepresentation> results = HERACLES.getIdentityUsers(filter, "groups");
        if (results != null && !results.isEmpty() && results.get(0).getGroups() != null) {
            return results.get(0).getGroups();
        }
        return List.of();
    }
}
