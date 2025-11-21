package org.apache.atlas.web.rest;

import org.apache.atlas.annotation.Timed;
import org.apache.atlas.web.util.Servlets;
import org.springframework.stereotype.Service;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

@Path("test")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class IntegrationTestREST {

    @GET
    @Path("slowOperation")
    @Timed
    public Response slowOperation(@Context HttpServletResponse response) {
        try {
            Thread.sleep(15000);
            return Response.ok(Map.of("status", "success")).build();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
