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

package org.apache.atlas.ha;

import org.apache.atlas.AtlasConstants;
import org.apache.atlas.security.SecurityProperties;
import org.apache.commons.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HAConfigurationTest {

    private static final String[] TEST_ATLAS_SERVER_IDS_HA = new String[] { "id1", "id2" };

    @BeforeEach
    public void setup() {
        System.setProperty(AtlasConstants.SYSTEM_PROPERTY_APP_PORT, AtlasConstants.DEFAULT_APP_PORT_STR);
    }

    private Configuration createConfiguration() {
        return new PropertiesConfiguration();
    }

    @Test
    public void testIsHAEnabledByLegacyConfiguration() {
        Configuration configuration = createConfiguration();
        configuration.setProperty(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, true);

        boolean isHAEnabled = HAConfiguration.isHAEnabled(configuration);
        assertTrue(isHAEnabled);

        // restore
        configuration.clearProperty(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY);
        isHAEnabled = HAConfiguration.isHAEnabled(configuration);
        assertFalse(isHAEnabled);
    }

    @Test
    public void testIsHAEnabledByIds() {
        Configuration configuration = createConfiguration();
        configuration.setProperty(HAConfiguration.ATLAS_SERVER_IDS, TEST_ATLAS_SERVER_IDS_HA);
        boolean isHAEnabled = HAConfiguration.isHAEnabled(configuration);
        assertTrue(isHAEnabled);

        // restore
        configuration.setProperty(HAConfiguration.ATLAS_SERVER_IDS, new String[] {"id1"});
        isHAEnabled = HAConfiguration.isHAEnabled(configuration);
        assertFalse(isHAEnabled);
    }

    @Test
    public void testShouldReturnHTTPSBoundAddress() {
        Configuration configuration = createConfiguration();
        configuration.setProperty(HAConfiguration.ATLAS_SERVER_ADDRESS_PREFIX + "id1", "127.0.0.1:21443");
        configuration.setProperty(SecurityProperties.TLS_ENABLED, true);

        String address = HAConfiguration.getBoundAddressForId(configuration, "id1");

        assertEquals(address, "https://127.0.0.1:21443");
    }

    @Test
    public void testShouldReturnListOfAddressesInConfig() {
        Configuration configuration = createConfiguration();
        configuration.setProperty(HAConfiguration.ATLAS_SERVER_IDS, TEST_ATLAS_SERVER_IDS_HA);
        configuration.setProperty(HAConfiguration.ATLAS_SERVER_ADDRESS_PREFIX + "id1", "127.0.0.1:21000");
        configuration.setProperty(HAConfiguration.ATLAS_SERVER_ADDRESS_PREFIX + "id2", "127.0.0.1:31000");
        configuration.setProperty(SecurityProperties.TLS_ENABLED, false);

        List<String> serverInstances = HAConfiguration.getServerInstances(configuration);
        assertEquals(serverInstances.size(), 2);
        assertTrue(serverInstances.contains("http://127.0.0.1:21000"));
        assertTrue(serverInstances.contains("http://127.0.0.1:31000"));
    }

    @Test
    public void testShouldGetZookeeperAcl() {
        Configuration configuration = createConfiguration();
        configuration.setProperty(HAConfiguration.HA_ZOOKEEPER_ACL, "sasl:myclient@EXAMPLE.COM");

        HAConfiguration.ZookeeperProperties zookeeperProperties =
                HAConfiguration.getZookeeperProperties(configuration);
        assertTrue(zookeeperProperties.hasAcl());
    }

    @Test
    public void testShouldGetZookeeperAuth() {
        Configuration configuration = createConfiguration();
        configuration.setProperty(HAConfiguration.HA_ZOOKEEPER_AUTH, "sasl:myclient@EXAMPLE.COM");

        HAConfiguration.ZookeeperProperties zookeeperProperties =
                HAConfiguration.getZookeeperProperties(configuration);
        assertTrue(zookeeperProperties.hasAuth());
    }
}
