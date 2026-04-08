package org.apache.atlas.auth.client.heracles.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IdentityUserRepresentation {
    protected String id;
    protected String username;
    protected String email;
    protected String firstName;
    protected String lastName;
    protected String status;
    protected String realmId;
    protected boolean enabled;
    protected long createdTimestamp;
    protected List<IdentityRoleRepresentation> roles;
    protected List<IdentityGroupRepresentation> groups;
    protected Map<String, String> attributes;

    public IdentityUserRepresentation() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }

    public String getFirstName() { return firstName; }
    public void setFirstName(String firstName) { this.firstName = firstName; }

    public String getLastName() { return lastName; }
    public void setLastName(String lastName) { this.lastName = lastName; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getRealmId() { return realmId; }
    public void setRealmId(String realmId) { this.realmId = realmId; }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public long getCreatedTimestamp() { return createdTimestamp; }
    public void setCreatedTimestamp(long createdTimestamp) { this.createdTimestamp = createdTimestamp; }

    public List<IdentityRoleRepresentation> getRoles() { return roles; }
    public void setRoles(List<IdentityRoleRepresentation> roles) { this.roles = roles; }

    public List<IdentityGroupRepresentation> getGroups() { return groups; }
    public void setGroups(List<IdentityGroupRepresentation> groups) { this.groups = groups; }

    public Map<String, String> getAttributes() { return attributes; }
    public void setAttributes(Map<String, String> attributes) { this.attributes = attributes; }
}
