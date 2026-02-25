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

package org.apache.atlas.web.service;

import io.micrometer.core.instrument.binder.jetty.JettyConnectionMetrics;
import io.micrometer.core.instrument.binder.jetty.JettyServerThreadPoolMetrics;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.util.BeanUtil;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.AtlasAuditEntry;
import org.apache.commons.configuration.Configuration;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.springframework.web.context.ContextLoaderListener;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.sun.jersey.spi.spring.container.servlet.SpringServlet;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;

/**  
 * This class embeds a Jetty server and a connector.
 */
public class EmbeddedServer {
    public static final Logger LOG = LoggerFactory.getLogger(EmbeddedServer.class);

    public static final String ATLAS_DEFAULT_BIND_ADDRESS = "0.0.0.0";

    protected final Server server;

    protected String atlasPath;

    public EmbeddedServer(String host, int port, String path) throws IOException {
        int                           queueSize     = AtlasConfiguration.WEBSERVER_QUEUE_SIZE.getInt();
        LinkedBlockingQueue<Runnable> queue         = new LinkedBlockingQueue<>(queueSize);
        int                           minThreads    = AtlasConfiguration.WEBSERVER_MIN_THREADS.getInt();
        int                           maxThreads    = AtlasConfiguration.WEBSERVER_MAX_THREADS.getInt();
        int                           reservedThreads    = AtlasConfiguration.WEBSERVER_RESERVED_THREADS.getInt();
        long                          keepAliveTime = AtlasConfiguration.WEBSERVER_KEEPALIVE_SECONDS.getLong();
        ThreadPoolExecutor            executor      = new ThreadPoolExecutor(maxThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS, queue);
        ExecutorThreadPool            pool          = new ExecutorThreadPool(executor, reservedThreads);

        server = new Server(pool);
        atlasPath = path;

        Connector connector = getConnector(host, port);
        connector.addBean(new JettyConnectionMetrics(getMeterRegistry()));
        new JettyServerThreadPoolMetrics(pool, Collections.emptyList()).bindTo(getMeterRegistry());
        server.addConnector(connector);

        WebAppContext application = getWebAppContext(path);
        server.setHandler(application);
    }

    protected WebAppContext getWebAppContext(String path) {
        LOG.info("Registering Atlas V2 API Fast-Lane shallow stack Servlet");
        WebAppContext application = new WebAppContext(path, "/");

        // // Stage 1: Register the Lean Servlet early
        // application.addLifeCycleListener(new org.eclipse.jetty.util.component.AbstractLifeCycle.AbstractLifeCycleListener() {
        //     @Override
        //     public void lifeCycleStarting(org.eclipse.jetty.util.component.LifeCycle event) {
        //         try {
        //             LOG.info("In lifeCycleStarting for shallow stack registration.");
        //             org.eclipse.jetty.servlet.ServletHolder holder = new org.eclipse.jetty.servlet.ServletHolder();
        //             holder.setName("atlas-v2-shallowstack");
        //             holder.setClassName("com.sun.jersey.spi.spring.container.servlet.SpringServlet");

        //             // FIX FOR 500: Use ClassNamesResourceConfig instead of PackagesResourceConfig
        //             holder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", 
        //                                     "com.sun.jersey.api.core.ClassNamesResourceConfig");

        //             // Explicitly list the resources to avoid the bug related to classpath scanner crashing
        //             holder.setInitParameter("com.sun.jersey.config.property.classnames", 
        //                 "org.apache.atlas.web.resources.AdminResource;" + 
        //                 "org.apache.atlas.web.resources.EntityResourceV2;" +
        //                 "org.apache.atlas.web.providers.AtlasResourceContextBinder");

        //             holder.setInitOrder(1);

        //             // Map the lean servlet to our fast-lane paths
        //             application.getServletHandler().addServletWithMapping(holder, "/api/atlas/v2/*");
        //             application.getServletHandler().addServletWithMapping(holder, "/api/atlas/admin/health");
        //             application.getServletHandler().addServletWithMapping(holder, "/api/atlas/admin/status");

        //             LOG.info("Shallow stack Servlet registered for /v2 and health endpoints.");
        //         } catch (Exception e) {
        //             LOG.error("Failed to register shallow servlet", e);
        //         }
        //     }

        //     @Override
        //     public void lifeCycleStarted(org.eclipse.jetty.util.component.LifeCycle event) {
        //         try {
        //             LOG.info("Jetty Started. Finalizing filter order for bypass.");
                    
        //             org.eclipse.jetty.servlet.FilterMapping securityMapping = new org.eclipse.jetty.servlet.FilterMapping();
        //             securityMapping.setFilterName("springSecurityFilterChain");
        //             securityMapping.setPathSpecs(new String[]{"/api/atlas/v2/*", "/api/atlas/admin/health", "/api/atlas/admin/status"});
        //             securityMapping.setDispatcherTypes(java.util.EnumSet.of(javax.servlet.DispatcherType.REQUEST));

        //             org.eclipse.jetty.servlet.ServletHandler handler = application.getServletHandler();
        //             org.eclipse.jetty.servlet.FilterMapping[] currentMappings = handler.getFilterMappings();

        //             if (currentMappings != null) {
        //                 // Prepend the security filter so it is index 0, jumping over AuditFilter (503 source)
        //                 org.eclipse.jetty.servlet.FilterMapping[] newMappings = new org.eclipse.jetty.servlet.FilterMapping[currentMappings.length + 1];
        //                 newMappings[0] = securityMapping;
        //                 System.arraycopy(currentMappings, 0, newMappings, 1, currentMappings.length);
                        
        //                 handler.setFilterMappings(newMappings);
        //                 LOG.info("Security filter successfully prepended to the front of the final chain.");
        //             }
        //         } catch (Exception e) {
        //             LOG.error("Failed to re-order filters", e);
        //         }
        //     }
        // });

        application.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
        return application;
    }

    public static EmbeddedServer newServer(String host, int port, String path, boolean secure)
            throws IOException {
        if (secure) {
            return new SecureEmbeddedServer(host, port, path);
        } else {
            return new EmbeddedServer(host, port, path);
        }
    }

    protected Connector getConnector(String host, int port) throws IOException {
        HttpConfiguration http_config = new HttpConfiguration();
        // this is to enable large header sizes when Kerberos is enabled with AD
        final int bufferSize = AtlasConfiguration.WEBSERVER_REQUEST_BUFFER_SIZE.getInt();;
        http_config.setResponseHeaderSize(bufferSize);
        http_config.setRequestHeaderSize(bufferSize);
        http_config.setSendServerVersion(false);

        ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory(http_config));
        connector.setPort(port);
        connector.setHost(host);
        return connector;
    }

    // public void start() throws AtlasBaseException {
    //     // try {
    //     //     server.start();

    //     //    // server.join();
    //     // } catch(Exception e) {
    //     //     throw new AtlasBaseException(AtlasErrorCode.EMBEDDED_SERVER_START, e);
    //     // }
    //     try {
    //              // Get the deep stack Main App
    //             org.eclipse.jetty.webapp.WebAppContext mainAppContext = getWebAppContext(atlasPath);
    //             mainAppContext.setServer(server);
    //             //  Forcing  Jetty to scan the WAR and create the ClassLoader 
    //             // Without this, the ClassLoader is null until the server actually starts.
    //             mainAppContext.preConfigure();

    //             ClassLoader atlasClassLoader = mainAppContext.getClassLoader();

    //             // CREATE THE slim stack context (The Fast-Lane)
    //             org.eclipse.jetty.servlet.ServletContextHandler fastLaneContext = 
    //                 new org.eclipse.jetty.servlet.ServletContextHandler(org.eclipse.jetty.servlet.ServletContextHandler.SESSIONS);
    //             fastLaneContext.setContextPath("/api/atlas");
    //             fastLaneContext.setClassLoader(atlasClassLoader);

    //         //    // Use a Listener to "Hydrate" the Fast-Lane once the Main App is ready
    //         //     mainAppContext.addLifeCycleListener(new org.eclipse.jetty.util.component.AbstractLifeCycle.AbstractLifeCycleListener() {
    //         //         @Override
    //         //         public void lifeCycleStarted(org.eclipse.jetty.util.component.LifeCycle event) {
    //         //             // Now that the main app is started, the ClassLoader is ready!
    //         //             fastLaneContext.setClassLoader(mainAppContext.getClassLoader());
                        
    //         //             // Lean V2 API Servlet reroute-(Jersey)
    //         //             fastLaneContext.getServletContext().setAttribute(
    //         //                 org.springframework.web.context.WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE,
    //         //                 mainAppContext.getServletContext().getAttribute(org.springframework.web.context.WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE)
    //         //             );

    //         //             // Add Jersey to the Fast-Lane
    //         //             org.eclipse.jetty.servlet.ServletHolder v2Holder = new org.eclipse.jetty.servlet.ServletHolder(
    //         //                 new com.sun.jersey.spi.container.servlet.ServletContainer()
    //         //             );
    //         //             v2Holder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.ClassNamesResourceConfig");
    //         //             v2Holder.setInitParameter("com.sun.jersey.config.property.classnames", "org.apache.atlas.web.resources.EntityResourceV2");
                        
    //         //             fastLaneContext.addServlet(v2Holder, "/v2/*");
    //         //             try {
    //         //                 fastLaneContext.start();
    //         //                 LOG.info("Fast-Lane V2 Optimization Active (Bypassing 150+ filters)");
    //         //             } catch (Exception e) {
    //         //                 LOG.error("Failed to start Fast-Lane V2", e);
    //         //             }
    //         //         }
    //         //     });
    //             // Isolated Health check( with Zero dependency on current jetty servlet at this stage)
    //             // This ensures Kubernetes 'Ready' checks pass immediately.
    //             org.eclipse.jetty.servlet.ServletHolder healthHolder = new org.eclipse.jetty.servlet.ServletHolder(new javax.servlet.http.HttpServlet() {
    //                 @Override
    //                 protected void doGet(javax.servlet.http.HttpServletRequest req, javax.servlet.http.HttpServletResponse resp) 
    //                     throws javax.servlet.ServletException, java.io.IOException {
    //                     resp.setContentType("application/json");
    //                     resp.setStatus(200);
    //                     resp.getWriter().println("{\"status\":\"PASSIVE_READY\"}");
    //                 }
    //             });
    //             fastLaneContext.addServlet(healthHolder, "/admin/health");

    //             org.eclipse.jetty.servlet.ServletHolder v2Holder = new org.eclipse.jetty.servlet.ServletHolder(
    //                 new com.sun.jersey.spi.container.servlet.ServletContainer()
    //             );
    //             v2Holder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.ClassNamesResourceConfig");
    //             v2Holder.setInitParameter("com.sun.jersey.config.property.classnames", "org.apache.atlas.web.resources.EntityResourceV2");
                
    //             // Use -1 to delay initialization until the first request hits
    //             v2Holder.setInitOrder(-1); 
    //             fastLaneContext.addServlet(v2Holder, "/v2/*");
                
    //             //Routing  
    //             org.eclipse.jetty.server.handler.ContextHandlerCollection contexts = 
    //                 new org.eclipse.jetty.server.handler.ContextHandlerCollection();
                
    //            // Fast-lane (Slim) comes first to intercept /admin/health and /v2/*
    //             contexts.setHandlers(new org.eclipse.jetty.server.Handler[] { fastLaneContext, mainAppContext });

    //             server.setHandler(contexts);
    //             server.start();
    //             server.join();

    //         } catch(Exception e) {
    //             throw new AtlasBaseException(AtlasErrorCode.EMBEDDED_SERVER_START, e);
    //         }

    // }


    // public void start() throws AtlasBaseException {
    //     try {
    //         final org.eclipse.jetty.webapp.WebAppContext mainAppContext = getWebAppContext(atlasPath);

    //         // Setup the Fast-Lane (The Lean Optimization)
    //         final org.eclipse.jetty.servlet.ServletContextHandler fastLaneContext = 
    //             new org.eclipse.jetty.servlet.ServletContextHandler(org.eclipse.jetty.servlet.ServletContextHandler.SESSIONS);
    //         fastLaneContext.setContextPath("/api/atlas/v2");

    //         // Immediate Health Check (Bypass everything for K8s)
    //         fastLaneContext.addServlet(new org.eclipse.jetty.servlet.ServletHolder(new javax.servlet.http.HttpServlet() {
    //             @Override
    //             protected void doGet(javax.servlet.http.HttpServletRequest req, javax.servlet.http.HttpServletResponse resp) 
    //                 throws java.io.IOException {
    //                 resp.setContentType("application/json");
    //                 resp.getWriter().println("{\"status\":\"PASSIVE_READY\"}");
    //             }
    //         }), "/admin/health");

    //         //  Listen for the Main App context to finish starting
    //         mainAppContext.addLifeCycleListener(new org.eclipse.jetty.util.component.AbstractLifeCycle.AbstractLifeCycleListener() {
    //             @Override
    //             public void lifeCycleStarted(org.eclipse.jetty.util.component.LifeCycle event) {
    //                 try {
    //                     LOG.info("Main App started. Initializing Optimized V2 Fast-Lane...");
                        
    //                     // Now the ClassLoader is guaranteed to be fully hydrated with Atlas JARs
    //                     fastLaneContext.setClassLoader(mainAppContext.getClassLoader());

    //                     // Add the Jersey V2 Servlet (The Performance Win)
    //                     org.eclipse.jetty.servlet.ServletHolder v2Holder = new org.eclipse.jetty.servlet.ServletHolder(
    //                         new com.sun.jersey.spi.container.servlet.ServletContainer()
    //                     );
    //                     v2Holder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.ClassNamesResourceConfig");
    //                     v2Holder.setInitParameter("com.sun.jersey.config.property.classnames", "org.apache.atlas.web.resources.EntityResourceV2");
    //                     //v2Holder.setInitParameter("com.sun.jersey.config.property.packages", "org.apache.atlas.web.resources");

    //                     // Borrow Spring beans so V2 API can actually talk to the database
    //                     fastLaneContext.getServletContext().setAttribute(
    //                         org.springframework.web.context.WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE,
    //                         mainAppContext.getServletContext().getAttribute(org.springframework.web.context.WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE)
    //                     );

    //                     fastLaneContext.addServlet(v2Holder, "/*");
                        
    //                     // Start the optimized context manually
    //                     if (!fastLaneContext.isRunning()) {
    //                         fastLaneContext.start();
    //                     }
    //                     LOG.info("V2 Optimization Active: Bypassing 150+ filter calls.");
    //                 } catch (Exception e) {
    //                     LOG.error("Critical Servlet error: Failed to start V2 optimization path", e);
    //                 } 
    //             }
    //         });

    //         // ROUTING
    //         org.eclipse.jetty.server.handler.ContextHandlerCollection contexts = new org.eclipse.jetty.server.handler.ContextHandlerCollection();
    //         // fastLaneContext is first so it intercepts /v2/ calls
    //         contexts.setHandlers(new org.eclipse.jetty.server.Handler[] { fastLaneContext, mainAppContext });

    //         server.setHandler(contexts);
    //         server.start();
    //         server.join();

    //     } catch(Exception e) {
    //         throw new AtlasBaseException(AtlasErrorCode.EMBEDDED_SERVER_START, e);
    //     }
    // }


    private void syncFastLane(WebAppContext mainAppContext, ServletContextHandler fastLaneContext, ServletHolder v2Holder) {
        try {

            LOG.info("Main App started. Linking dependencies to V2 Fast-Lane...");
            if (fastLaneContext.getServletContext().getAttribute(org.springframework.web.context.WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE) != null) {
                LOG.info("Fast lane context started already, returning now...");
                return; 
            }
            LOG.info("No root web app context. Setting up sync");
            // Sync ClassLoaders
            fastLaneContext.setClassLoader(mainAppContext.getClassLoader());

            // Sync Spring Context so @Inject works in EntityResourceV2
            Object springContext = mainAppContext.getServletContext().getAttribute(
                org.springframework.web.context.WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE);
            
            fastLaneContext.getServletContext().setAttribute(
                org.springframework.web.context.WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE, 
                springContext);

            if (!fastLaneContext.isStarted()) {
                fastLaneContext.start();
                //v2Holder.start();
                v2Holder.getServletHandler().initialize();
                LOG.info("V2 Fast-Lane is now ONLINE.");
            }
            else
            {
                LOG.info("Fast lane context hasn't started");
            }
            
            LOG.info("V2 Optimization Path Linked and Synchronized.");
        } catch (Exception e) {
            LOG.error("Critical error linking V2 optimization path", e);
        }
    }

    public void start() throws AtlasBaseException {
    try {
        final org.eclipse.jetty.webapp.WebAppContext mainAppContext = getWebAppContext(atlasPath);

        // Setup the Fast-Lane Context immediately
        final org.eclipse.jetty.servlet.ServletContextHandler fastLaneContext = 
            new org.eclipse.jetty.servlet.ServletContextHandler(org.eclipse.jetty.servlet.ServletContextHandler.SESSIONS);
        fastLaneContext.setClassLoader(mainAppContext.getClassLoader());

        //correct cast
        if (fastLaneContext instanceof org.eclipse.jetty.webapp.WebAppContext) {
            ((org.eclipse.jetty.webapp.WebAppContext) fastLaneContext).setParentLoaderPriority(true);
        }

        fastLaneContext.setContextPath("/api");
        fastLaneContext.setResourceBase("/opt/apache-atlas/server/webapp/atlas"); //TODO : remove this hard code
        

        //  Add Health Check (Always available)
        fastLaneContext.addServlet(new org.eclipse.jetty.servlet.ServletHolder(new javax.servlet.http.HttpServlet() {
            @Override
            protected void doGet(javax.servlet.http.HttpServletRequest req, javax.servlet.http.HttpServletResponse resp) 
                throws java.io.IOException {
                resp.setContentType("application/json");
                resp.getWriter().println("{\"status\":\"FAST_LANE_READY\"}");
            }
        }), "/atlas/admin/health");

        //  Prepare the V2 Holder but ***without*** starting; the context is then started manually in a listener
        // Use the Spring-specific servlet implementation
        org.eclipse.jetty.servlet.ServletHolder v2Holder = new org.eclipse.jetty.servlet.ServletHolder( );
        v2Holder.setClassName("com.sun.jersey.spi.spring.container.servlet.SpringServlet");
        v2Holder.setInitOrder(-1);
        v2Holder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", 
                          "com.sun.jersey.api.core.ClassNamesResourceConfig");
        v2Holder.setInitParameter("com.sun.jersey.spi.container.ContainerRequestFilters", 
                          "com.sun.jersey.api.container.filter.LoggingFilter");
        v2Holder.setInitParameter("com.sun.jersey.spi.container.ContainerResponseFilters", 
                                "com.sun.jersey.api.container.filter.LoggingFilter");

        // Only register what exists in Atlas built JARs
        v2Holder.setInitParameter("com.sun.jersey.config.property.classnames", 
            "org.apache.atlas.web.resources.AdminResource;" +
            "org.apache.atlas.web.rest.EntityREST;" +
            "org.apache.atlas.web.rest.DirectSearchREST;" + 
            "org.apache.atlas.web.rest.DiscoveryREST;" +
            "org.apache.atlas.web.rest.TypesREST;" +
            "org.apache.atlas.web.rest.GlossaryREST;" +
            "org.apache.atlas.web.rest.LineageREST;" +
            "org.apache.atlas.web.rest.RelationshipREST;" +

            "org.apache.atlas.web.util.Servlets;" +
            // Jackson 2 provider for SearchLog classes
            "com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;" +
            
            // The JSON/Jackson providers (Required for AdminResource to work)
            "org.codehaus.jackson.jaxrs.JacksonJsonProvider;" +
            "org.codehaus.jackson.jaxrs.JacksonJaxbJsonProvider;" +
        
            // Exception mappers (Required to handle errors gracefully)
            "org.apache.atlas.web.errors.AtlasBaseExceptionMapper;" +
            "org.apache.atlas.web.errors.AllExceptionMapper;" +
            "org.apache.atlas.web.errors.NotFoundExceptionMapper;" +
                    
            "org.apache.atlas.web.filters.AtlasAuthenticationFilter" 
        );
        //v2Holder.setInitParameter("com.sun.jersey.config.property.packages", "org.apache.atlas.web.rest;org.apache.atlas.web.resources");

        // This tells Jersey to use Spring as the IoC factory for the classes added above
        v2Holder.setInitParameter("com.sun.jersey.spi.spring.container.servlet.SpringServlet", "com.sun.jersey.spi.spring.container.servlet.SpringServlet");

        // Add /v2 calls to the context now, Jetty will handle the start sequence
        fastLaneContext.addServlet(v2Holder, "/atlas/v2/*");
        fastLaneContext.addServlet(v2Holder, "/meta/*");

        // This Listener is **only** to inject the dependencies once Main App is ready
        mainAppContext.addLifeCycleListener(new org.eclipse.jetty.util.component.AbstractLifeCycle.AbstractLifeCycleListener() {
            @Override
            public void lifeCycleStarted(org.eclipse.jetty.util.component.LifeCycle event) {
               syncFastLane(mainAppContext, fastLaneContext, v2Holder);
            }
        });

        // Routing with correct ordered  setup - first fastLaneContext
        org.eclipse.jetty.server.handler.ContextHandlerCollection contexts = new org.eclipse.jetty.server.handler.ContextHandlerCollection();
        contexts.setHandlers(new org.eclipse.jetty.server.Handler[] { mainAppContext, fastLaneContext });

        server.setHandler(contexts);
        // This will start both contexts in the correct order
        try {
            server.start(); 
            LOG.info("Jetty Server start signal sent. Proceeding to manual sync...");
        } catch (Exception e) {
            LOG.error("Fatal: Jetty failed to start. Manual sync aborted.", e);
            throw e;
        }
        LOG.info("Server started. Triggering V2 Fast-Lane slim stack synchronization manually not waiting for lifeCycleStarted...");
        try {
            LOG.info("Manual fast lane Sync Trigger: Starting bridge between Main and V2...");
            syncFastLane(mainAppContext, fastLaneContext, v2Holder);
        } catch (Exception e) {
            LOG.error("Manual fast lane Sync failed: The bridge could not be established.", e);
        }
        server.join();

    } catch(Exception e) {
        throw new AtlasBaseException(AtlasErrorCode.EMBEDDED_SERVER_START, e);
    }
}

    public Server getServer() {
        return this.server;
    }

    public void stop() {
        try {
            server.stop();
        } catch (Exception e) {
            LOG.warn("Error during shutdown", e);
        }
    }
}
