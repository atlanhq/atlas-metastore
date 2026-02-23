package org.apache.atlas.web.rest;

import org.apache.atlas.web.service.AsyncIngestionConsumerService;
import org.apache.atlas.web.util.Servlets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * REST endpoints for managing the async ingestion consumer (leangraph migration).
 */
@Path("async-ingestion/consumer")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class AsyncIngestionConsumerController {

    @Autowired
    private AsyncIngestionConsumerService consumerService;

    @GET
    @Path("/status")
    public Map<String, Object> getStatus() {
        return consumerService.getStatus();
    }

    @POST
    @Path("/start")
    public Map<String, Object> start() {
        consumerService.start();
        return consumerService.getStatus();
    }

    @POST
    @Path("/stop")
    public Map<String, Object> stop() {
        consumerService.shutdown();
        return consumerService.getStatus();
    }

    @GET
    @Path("/lag")
    public Map<String, Object> getLag() {
        return consumerService.getConsumerLag();
    }

    @POST
    @Path("/switchover/readiness")
    public Map<String, Object> checkSwitchoverReadiness() {
        Map<String, Object> result = new LinkedHashMap<>();
        Map<String, Object> lagInfo = consumerService.getConsumerLag();

        boolean isRunning = consumerService.isRunning();
        boolean isHealthy = consumerService.isHealthy();
        long totalLag = ((Number) lagInfo.getOrDefault("totalLag", -1L)).longValue();
        boolean ready = isRunning && isHealthy && totalLag == 0;

        result.put("ready", ready);
        result.put("isRunning", isRunning);
        result.put("isHealthy", isHealthy);
        result.put("consumerLag", lagInfo);

        if (!ready) {
            if (!isRunning) {
                result.put("reason", "Consumer is not running");
            } else if (!isHealthy) {
                result.put("reason", "Consumer is unhealthy");
            } else if (totalLag > 0) {
                result.put("reason", "Consumer lag is " + totalLag + " (must be 0)");
            } else {
                result.put("reason", "Unable to determine lag");
            }
        }

        return result;
    }
}
