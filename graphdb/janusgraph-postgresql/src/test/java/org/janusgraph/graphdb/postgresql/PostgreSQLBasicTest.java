// Copyright 2026 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.postgresql;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.postgresql.PostgreSQLConfigOptions;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class PostgreSQLBasicTest {

    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
        .withDatabaseName("janusgraph")
        .withUsername("janusgraph")
        .withPassword("janusgraph");

    @Test
    public void testBasicGraphOperations() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration()
            .set(GraphDatabaseConfiguration.STORAGE_BACKEND, "postgresql")
            .set(PostgreSQLConfigOptions.HOST, postgres.getHost())
            .set(PostgreSQLConfigOptions.PORT, postgres.getMappedPort(5432))
            .set(PostgreSQLConfigOptions.DATABASE, postgres.getDatabaseName())
            .set(PostgreSQLConfigOptions.USERNAME, postgres.getUsername())
            .set(PostgreSQLConfigOptions.PASSWORD, postgres.getPassword())
            .set(GraphDatabaseConfiguration.DROP_ON_CLEAR, true);

        JanusGraph graph = JanusGraphFactory.open(config);
        try {
            Vertex v1 = graph.addVertex();
            Vertex v2 = graph.addVertex();
            v1.addEdge("knows", v2);
            graph.tx().commit();

            long count = graph.traversal().V().count().next();
            assertEquals(2L, count);
        } finally {
            graph.close();
        }
    }
}
