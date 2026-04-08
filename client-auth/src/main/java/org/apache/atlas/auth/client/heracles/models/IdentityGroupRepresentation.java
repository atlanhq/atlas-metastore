package org.apache.atlas.auth.client.heracles.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IdentityGroupRepresentation {
    protected String id;
    protected String name;
    protected String parentGroup;
    protected String realmId;
    protected List<IdentityRoleRepresentation> roles;
    protected Map<String, String> attributes;

    public IdentityGroupRepresentation() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getParentGroup() { return parentGroup; }
    public void setParentGroup(String parentGroup) { this.parentGroup = parentGroup; }

    public String getRealmId() { return realmId; }
    public void setRealmId(String realmId) { this.realmId = realmId; }

    public List<IdentityRoleRepresentation> getRoles() { return roles; }
    public void setRoles(List<IdentityRoleRepresentation> roles) { this.roles = roles; }

    public Map<String, String> getAttributes() { return attributes; }
    public void setAttributes(Map<String, String> attributes) { this.attributes = attributes; }
}
