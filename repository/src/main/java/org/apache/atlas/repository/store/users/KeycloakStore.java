/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.store.users;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.RESOURCE_NOT_FOUND;
import static org.apache.atlas.auth.client.keycloak.AtlasKeycloakClient.getKeycloakClient;

public class KeycloakStore {
    private static final Logger LOG = LoggerFactory.getLogger(KeycloakStore.class);

    private boolean saveUsersToAttributes = false;
    private boolean saveGroupsToAttributes = false;

    public KeycloakStore() {}

    public KeycloakStore(boolean saveUsersToAttributes, boolean saveGroupsToAttributes) {
        this.saveUsersToAttributes  = saveUsersToAttributes;
        this.saveGroupsToAttributes  = saveGroupsToAttributes;
    }

    public RoleRepresentation createRole(RoleRepresentation role) throws AtlasBaseException {
        getKeycloakClient().createRole(role);
        return getKeycloakClient().getRoleByName(role.getName());
    }

    public RoleRepresentation getRole(String roleName) throws AtlasBaseException {
        RoleRepresentation roleRepresentation = null;
        try{
            roleRepresentation = getKeycloakClient().getRoleByName(roleName);
        } catch (AtlasBaseException e) {
            return null;
        }
        return roleRepresentation;
    }
}
