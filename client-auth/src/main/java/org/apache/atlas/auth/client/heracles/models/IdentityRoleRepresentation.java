package org.apache.atlas.auth.client.heracles.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.keycloak.representations.idm.RoleRepresentation;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IdentityRoleRepresentation {
    protected String id;
    protected String name;
    protected String description;
    protected String realmId;
    protected boolean clientRole;

    public IdentityRoleRepresentation() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getRealmId() { return realmId; }
    public void setRealmId(String realmId) { this.realmId = realmId; }

    public boolean isClientRole() { return clientRole; }
    public void setClientRole(boolean clientRole) { this.clientRole = clientRole; }

    public RoleRepresentation toKeycloakRole() {
        RoleRepresentation role = new RoleRepresentation();
        role.setId(this.id);
        role.setName(this.name);
        role.setDescription(this.description);
        role.setClientRole(this.clientRole);
        return role;
    }
}
