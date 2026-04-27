/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AtlasTypeDefESUtils.typeHasInstanceVertex() — ES-based implementation.
 */
class AtlasGraphUtilsV2TypeHasInstanceVertexTest {

    static {
        try {
            PropertiesConfiguration config = new PropertiesConfiguration();
            config.setProperty("atlas.graph.storage.hostname", "localhost");
            config.setProperty("atlas.graph.index.search.hostname", "localhost:9200");
            ApplicationProperties.set(config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize test configuration", e);
        }
    }

    @Mock
    private AtlasGraph mockGraph;

    @Mock
    private AtlasIndexQuery mockIndexQuery;

    private MockedStatic<AtlasGraphProvider> mockedGraphProvider;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        mockedGraphProvider = mockStatic(AtlasGraphProvider.class);
        mockedGraphProvider.when(AtlasGraphProvider::getGraphInstance).thenReturn(mockGraph);

        when(mockGraph.elasticsearchQuery(anyString())).thenReturn(mockIndexQuery);
    }

    @AfterEach
    void tearDown() {
        if (mockedGraphProvider != null) {
            mockedGraphProvider.close();
        }
    }

    @Test
    void testReturnsTrueWhenInstancesExist() throws AtlasBaseException {
        when(mockIndexQuery.countIndexQuery(anyString())).thenReturn(5L);

        boolean result = AtlasTypeDefESUtils.typeHasInstanceVertex("Table");

        assertTrue(result);
        verify(mockIndexQuery).countIndexQuery(contains("__typeName.keyword"));
        verify(mockIndexQuery).countIndexQuery(contains("Table"));
    }

    @Test
    void testReturnsFalseWhenNoInstances() throws AtlasBaseException {
        when(mockIndexQuery.countIndexQuery(anyString())).thenReturn(0L);

        boolean result = AtlasTypeDefESUtils.typeHasInstanceVertex("Table");

        assertFalse(result);
    }

    @Test
    void testReturnsFalseWhenCountIsNull() throws AtlasBaseException {
        when(mockIndexQuery.countIndexQuery(anyString())).thenReturn(null);

        boolean result = AtlasTypeDefESUtils.typeHasInstanceVertex("Table");

        assertFalse(result);
    }

    @Test
    void testReturnsTrueOnESException() throws AtlasBaseException {
        // Conservative: if ES fails, assume instances exist to prevent accidental deletion
        when(mockIndexQuery.countIndexQuery(anyString()))
                .thenThrow(new RuntimeException("ES connection refused"));

        boolean result = AtlasTypeDefESUtils.typeHasInstanceVertex("Table");

        assertTrue(result, "Should return true (conservative) when ES query fails");
    }

    @Test
    void testReturnsTrueOnGraphException() throws AtlasBaseException {
        // Graph itself throws when creating the index query
        when(mockGraph.elasticsearchQuery(anyString()))
                .thenThrow(new AtlasBaseException("Graph unavailable"));

        boolean result = AtlasTypeDefESUtils.typeHasInstanceVertex("Table");

        assertTrue(result, "Should return true (conservative) when graph is unavailable");
    }

    @Test
    void testQueryUsesCorrectIndexAndTypeName() throws AtlasBaseException {
        when(mockIndexQuery.countIndexQuery(anyString())).thenReturn(1L);

        AtlasTypeDefESUtils.typeHasInstanceVertex("DataSet");

        verify(mockGraph).elasticsearchQuery(Constants.getVertexIndexName());
        verify(mockIndexQuery).countIndexQuery(contains("DataSet"));
        verify(mockIndexQuery).countIndexQuery(contains(Constants.TYPE_NAME_PROPERTY_KEY + ".keyword"));
    }

    @Test
    void testReturnsTrueForCountOfOne() throws AtlasBaseException {
        when(mockIndexQuery.countIndexQuery(anyString())).thenReturn(1L);

        boolean result = AtlasTypeDefESUtils.typeHasInstanceVertex("Column");

        assertTrue(result);
    }

    @Test
    void testReturnsFalseForNegativeCount() throws AtlasBaseException {
        // Defensive: negative count should be treated as no instances
        when(mockIndexQuery.countIndexQuery(anyString())).thenReturn(-1L);

        boolean result = AtlasTypeDefESUtils.typeHasInstanceVertex("Table");

        assertFalse(result);
    }
}
