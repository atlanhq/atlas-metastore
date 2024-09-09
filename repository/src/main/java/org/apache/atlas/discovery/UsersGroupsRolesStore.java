package org.apache.atlas.discovery;

import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;

import java.util.List;

public class UsersGroupsRolesStore {

    private RangerUserStore userStore;
    private RangerRoles allRoles;
    private List<RangerPolicy> resourcePolicies;
    private List<RangerPolicy> tagPolicies;
    private List<RangerPolicy> abacPolicies;
    private static UsersGroupsRolesStore usersGroupsRolesStore;

    public static UsersGroupsRolesStore getInstance() {
        synchronized (UsersGroupsRolesStore.class) {
            if (usersGroupsRolesStore == null) {
                usersGroupsRolesStore = new UsersGroupsRolesStore();
            }
            return usersGroupsRolesStore;
        }
    }

    public UsersGroupsRolesStore () {}

    public void setUserStore(RangerUserStore userStore) {
        this.userStore = userStore;
    }

    public RangerUserStore getUserStore() {
        return userStore;
    }

    public void setAllRoles(RangerRoles allRoles) {
        this.allRoles = allRoles;
    }

    public RangerRoles getAllRoles() {
        return allRoles;
    }

    public void setResourcePolicies(List<RangerPolicy> resourcePolicies) {
        this.resourcePolicies = resourcePolicies;
    }

    public List<RangerPolicy> getResourcePolicies() {
        return resourcePolicies;
    }

    public void setTagPolicies(List<RangerPolicy> tagPolicies) {
        this.tagPolicies = tagPolicies;
    }

    public List<RangerPolicy> getTagPolicies() {
        return tagPolicies;
    }

    public void setAbacPolicies(List<RangerPolicy> abacPolicies) {
        this.abacPolicies = abacPolicies;
    }

    public List<RangerPolicy> getAbacPolicies() {
        return abacPolicies;
    }

}
