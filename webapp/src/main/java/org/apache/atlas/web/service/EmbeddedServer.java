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
import org.eclipse.jetty.servlet.ServletHolder;
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

        Connector connector = getConnector(host, port);
        connector.addBean(new JettyConnectionMetrics(getMeterRegistry()));
        new JettyServerThreadPoolMetrics(pool, Collections.emptyList()).bindTo(getMeterRegistry());
        server.addConnector(connector);

        WebAppContext application = getWebAppContext(path);
        server.setHandler(application);
    }

    protected WebAppContext getWebAppContext(String path) {
        LOG.info("Registering Atlas V2 API Fast-Lane shallow stack Servlet with ClassLoader Alignment");
        WebAppContext application = new WebAppContext(path, "/");

        try {
            Configuration configuration = ApplicationProperties.get();

            if (configuration.getProperty("atlas.graph.kafka.bootstrap.servers") == null &&
                configuration.getProperty("atlas.kafka.bootstrap.servers") != null) {
                LOG.info("Explicitly setting atlas.graph.kafka.bootstrap.servers");
                configuration.setProperty(
                    "atlas.graph.kafka.bootstrap.servers",
                    configuration.getProperty("atlas.kafka.bootstrap.servers")
                );
            }
        } catch (AtlasException e) {
            LOG.error("Failed to load application properties", e);
            throw new RuntimeException("Configuration initialization failed", e);
        }
        // final ClassLoader atlasLoader = Thread.currentThread().getContextClassLoader();
        // application.setClassLoader(atlasLoader);
        
        // --- LOAD CHANGE: API FAST-LANE REGISTRATION --- 
        // We manually register the Jersey Spring Servlet to ensure there is a straight path 
        // for the V2 API that bypasses the global filter chain in web.xml.
        // ----- IMP* This change is to reduce Jetty's deep stack all filter call for every
        // authenticated API call that lands in Atlas ------
        // try {
            // Using the SpringServlet which integrates Jersey with  Spring context
        //     com.sun.jersey.spi.spring.container.servlet.SpringServlet jerseyServlet = 
        //         new com.sun.jersey.spi.spring.container.servlet.SpringServlet();
            
        //     org.eclipse.jetty.servlet.ServletHolder holder = new org.eclipse.jetty.servlet.ServletHolder(jerseyServlet);
        //     holder.setName("atlas-v2-shallowstack"); // TODO : "Shallowstack"( instead of "fastlane") is another potential name 
            
        //     //SET THE CLASSLOADER EXPLICITLY for servlet
        //    // holder.setClassLoader(atlasClassLoader);

        //     // This parameter tells Jersey where Atlas API Resource classes are located
        //     holder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", 
        //                             "com.sun.jersey.api.core.ClassNamesResourceConfig");
         
        //     //explicit comma-separated list of main V2 classes to avoid resource scanning
        //     holder.setInitParameter("com.sun.jersey.config.property.classnames", 
        //                             "org.apache.atlas.web.resources.EntityResourceV2,org.apache.atlas.web.resources.TypesResourceV2, org.apache.atlas.web.resources.DiscoveryResourceV2");
       
        //     holder.setInitOrder(1);

        //     // We map this specifically to the V2 API path.
        //     //  Need to check that all keycloak or other filter calls happen before /v2 mapping
        //     application.addServlet(holder, "/api/atlas/v2/*");

            
        //     LOG.info("Successfully registered Atlas V2 API Fast-Lane shallow stack Servlet with ClassLoader Alignment");
        // } catch (Exception e) {
        //     //No action other than error logging needed. Revert back to original flow in web.xml
        //     LOG.error("Failed to register Fast-Lane shallow stack Servlet, falling back to default web.xml flow", e);
        // }
      
        //LifeCycle Listener for Late Binding of resource files after all the JARs are loaded
        //and Atlas finds resource classes
        application.addLifeCycleListener(new org.eclipse.jetty.util.component.AbstractLifeCycle.AbstractLifeCycleListener() {
            @Override
            public void lifeCycleStarting(org.eclipse.jetty.util.component.LifeCycle event) {
                try {
                    // At this point, Jetty has set up the WebAppClassLoader
                    ClassLoader webAppLoader = application.getClassLoader();
                    
                    // com.sun.jersey.spi.spring.container.servlet.SpringServlet jerseyServlet = 
                    //     new com.sun.jersey.spi.spring.container.servlet.SpringServlet();
                    
                    // org.eclipse.jetty.servlet.ServletHolder holder = new org.eclipse.jetty.servlet.ServletHolder(jerseyServlet);
                    // holder.setName("atlas-v2-shallowstack");

                    // org.eclipse.jetty.servlet.ServletHolder holder = new org.eclipse.jetty.servlet.ServletHolder(
                    //     "atlas-v2-shallowstack", 
                    //     com.sun.jersey.spi.spring.container.servlet.SpringServlet.class
                    // );

                    org.eclipse.jetty.servlet.ServletHolder holder = new org.eclipse.jetty.servlet.ServletHolder();
                    holder.setName("atlas-v2-shallowstack");
                    holder.setClassName("com.sun.jersey.spi.spring.container.servlet.SpringServlet");

                    // Use the late-bound loader
                    holder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", 
                                            "com.sun.jersey.api.core.ClassNamesResourceConfig");
                    
                    
                    holder.setInitParameter("com.sun.jersey.config.property.packages", "org.apache.atlas.web.resources");              
                  // holder.setInitParameter("com.sun.jersey.config.property.classnames",   
                    //                         "org.apache.atlas.web.resources.EntityResourceV2," +
                    //                         "org.apache.atlas.web.resources.TypesResourceV2," +
                    //                         "org.apache.atlas.web.resources.DiscoveryResourceV2");
                    
                    holder.setInitOrder(1);

                    application.getServletHandler().addServletWithMapping(holder, "/api/atlas/v2/*");
                    application.getServletHandler().addServletWithMapping(holder, "/api/atlas/admin/health");
                    application.getServletHandler().addServletWithMapping(holder, "/api/atlas/admin/status");
                    LOG.info("Late-bound Atlas V2 Fast-Lane registered successfully via LifeCycle Listener.");
                } catch (Exception e) {
                    LOG.error("Failed to register Fast-Lane in LifeCycle event", e);
                }
            }
        });

        // Disable directory listing 
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

    public void start() throws AtlasBaseException {
        try {
            server.start();

           // server.join();
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
