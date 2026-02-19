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
package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.ConnectionBulkPurgeService;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

/**
 * REST endpoint for lightweight bulk purge of all assets under a connection.
 *
 * Bypasses the standard entity delete path (no preprocessors, no Kafka notifications,
 * no audit logging, no lineage recalculation) for maximum throughput. Directly removes
 * vertices and edges from JanusGraph and cleans up Elasticsearch.
 *
 * Requires ADMIN_PURGE privilege. Runs asynchronously with Redis-based status tracking.
 */
@Path("connection")
@Singleton
@Service
@Consumes({MediaType.APPLICATION_JSON})
@Produces({MediaType.APPLICATION_JSON})
public class ConnectionBulkPurgeREST {
    private static final Logger LOG      = LoggerFactory.getLogger(ConnectionBulkPurgeREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.ConnectionBulkPurgeREST");

    private final AtlasGraph   graph;
    private final RedisService redisService;

    @Inject
    public ConnectionBulkPurgeREST(AtlasGraph graph, RedisService redisService) {
        this.graph        = graph;
        this.redisService = redisService;
    }

    /**
     * Submit a bulk purge of all assets under a connection.
     *
     * This removes all vertices with matching connectionQualifiedName directly from
     * JanusGraph and Elasticsearch. The Connection entity itself is NOT deleted.
     *
     * The operation runs asynchronously. Use the status endpoint to poll for completion.
     *
     * @param connectionQualifiedName the qualifiedName of the connection whose assets to purge
     * @return acknowledgment that purge has been submitted
     */
    @POST
    @Path("/bulk-purge")
    @Timed
    public Response submitBulkPurge(@QueryParam("connectionQualifiedName") String connectionQualifiedName) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG,
                        "ConnectionBulkPurgeREST.submitBulkPurge(" + connectionQualifiedName + ")");
            }

            if (StringUtils.isBlank(connectionQualifiedName)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "connectionQualifiedName is required");
            }

            AtlasAuthorizationUtils.verifyAccess(
                    new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_PURGE),
                    "Bulk purge connection assets: " + connectionQualifiedName
            );

            // Check if a purge is already in progress for this connection
            String redisKey = ConnectionBulkPurgeService.REDIS_KEY_PREFIX + connectionQualifiedName;
            String currentStatus = redisService.getValue(redisKey);

            if ("IN_PROGRESS".equals(currentStatus)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                        "Bulk purge is already in progress for connection: " + connectionQualifiedName);
            }

            ConnectionBulkPurgeService purgeService = new ConnectionBulkPurgeService(graph, redisService, connectionQualifiedName);
            Thread purgeThread = new Thread(purgeService, "bulk-purge-" + connectionQualifiedName);
            purgeThread.setDaemon(true);
            purgeThread.start();

            LOG.info("ConnectionBulkPurge: Submitted purge for connectionQualifiedName={}", connectionQualifiedName);

            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("message", "Bulk purge submitted");
            responseBody.put("connectionQualifiedName", connectionQualifiedName);
            responseBody.put("statusEndpoint", "/api/atlas/connection/bulk-purge/status?connectionQualifiedName=" + connectionQualifiedName);

            return Response.ok(responseBody).build();

        } catch (AtlasBaseException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("ConnectionBulkPurge: Error submitting purge for {}", connectionQualifiedName, e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR,
                    "Failed to submit bulk purge: " + e.getMessage());
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get the status of a bulk purge operation.
     *
     * @param connectionQualifiedName the qualifiedName of the connection
     * @return status string: IN_PROGRESS, SUCCESSFUL:<count>, FAILED:<message>, or NOT_FOUND
     */
    @GET
    @Path("/bulk-purge/status")
    @Timed
    public Response getBulkPurgeStatus(@QueryParam("connectionQualifiedName") String connectionQualifiedName) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG,
                        "ConnectionBulkPurgeREST.getBulkPurgeStatus(" + connectionQualifiedName + ")");
            }

            if (StringUtils.isBlank(connectionQualifiedName)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "connectionQualifiedName is required");
            }

            String redisKey = ConnectionBulkPurgeService.REDIS_KEY_PREFIX + connectionQualifiedName;
            String status = redisService.getValue(redisKey);

            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("connectionQualifiedName", connectionQualifiedName);
            responseBody.put("status", status != null ? status : "NOT_FOUND");

            return Response.ok(responseBody).build();

        } catch (AtlasBaseException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("ConnectionBulkPurge: Error fetching status for {}", connectionQualifiedName, e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR,
                    "Failed to get purge status: " + e.getMessage());
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }
}
