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
package org.apache.atlas.repository.store.graph.v2.lineage;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.lineage.OpenLineageEvent;

import java.util.List;

/**
 * Data Access Object interface for OpenLineage events in Cassandra.
 *
 * Provides methods to store and retrieve OpenLineage events following
 * the OpenLineage specification for data lineage tracking.
 */
public interface OpenLineageEventDAO {

    /**
     * Stores an OpenLineage event in Cassandra.
     *
     * @param event The OpenLineage event to store
     * @throws AtlasBaseException if the storage operation fails
     */
    void storeEvent(OpenLineageEvent event) throws AtlasBaseException;

    /**
     * Retrieves all events for a specific run ID.
     *
     * @param runId The run ID to query events for
     * @return List of OpenLineage events for the given run ID
     * @throws AtlasBaseException if the query fails
     */
    List<OpenLineageEvent> getEventsByRunId(String runId) throws AtlasBaseException;

    /**
     * Retrieves a specific event by run ID and event ID.
     *
     * @param runId   The run ID
     * @param eventId The event ID
     * @return The OpenLineage event, or null if not found
     * @throws AtlasBaseException if the query fails
     */
    OpenLineageEvent getEventByRunIdAndEventId(String runId, String eventId) throws AtlasBaseException;

    /**
     * Checks if the Cassandra connection is healthy.
     *
     * @return true if healthy, false otherwise
     */
    boolean isHealthy();
}
