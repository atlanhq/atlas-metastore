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
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.graph.GraphSandboxUtil;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.runner.LocalSolrRunner;
import org.apache.commons.configuration.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.apache.atlas.graph.GraphSandboxUtil.useLocalSolr;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JanusGraphProviderTest {

    private Configuration configuration;
    private AtlasGraph<?, ?> graph;
    private String originalAtlasConf;

    @BeforeAll
    public void setUp() throws Exception {
        originalAtlasConf = System.getProperty("atlas.conf");
        System.setProperty("atlas.conf", "src/test/resources");
        ApplicationProperties.forceReload();

        GraphSandboxUtil.create();

        if (useLocalSolr()) {
            LocalSolrRunner.start();
        }

        //First get Instance
        graph         = new AtlasJanusGraph();
        configuration = ApplicationProperties.getSubsetConfiguration(ApplicationProperties.get(), AtlasJanusGraphDatabase.GRAPH_PREFIX);
    }

    @AfterAll
    public void tearDown() throws Exception {
        try {
            graph.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            graph.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (useLocalSolr()) {
            LocalSolrRunner.stop();
        }

        if (originalAtlasConf != null) {
            System.setProperty("atlas.conf", originalAtlasConf);
        } else {
            System.clearProperty("atlas.conf");
        }
        ApplicationProperties.forceReload();
    }

    @Test
    public void testValidate() throws AtlasException {
        try {
            AtlasJanusGraphDatabase.validateIndexBackend(configuration);
        } catch (Exception e) {
            Assertions.fail("Unexpected exception ", e);
        }

        // Change backend to a different value from whatever is currently configured.
        String currentBackend = configuration.getString(AtlasJanusGraphDatabase.INDEX_BACKEND_CONF);
        String changedBackend = AtlasJanusGraphDatabase.INDEX_BACKEND_LUCENE.equals(currentBackend)
                ? "solr"
                : AtlasJanusGraphDatabase.INDEX_BACKEND_LUCENE;

        configuration.setProperty(AtlasJanusGraphDatabase.INDEX_BACKEND_CONF, changedBackend);
        try {
            AtlasJanusGraphDatabase.validateIndexBackend(configuration);
            Assertions.fail("Expected exception");
        } catch (Exception e) {
            Assertions.assertEquals(e.getMessage(),
                    "Configured Index Backend " + changedBackend + " differs from earlier configured "
                    + "Index Backend " + currentBackend + ". Aborting!");
        }
    }
}
