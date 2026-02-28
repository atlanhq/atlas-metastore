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

    private static class LazySpringContext extends org.springframework.web.context.support.GenericWebApplicationContext {
        private Object delegate; // Store as Object to avoid ClassCastException
        private java.lang.reflect.Method getBeanByNameMethod;
        private java.lang.reflect.Method getBeanByTypeMethod;
        private boolean synchronizedDone = false;

        public LazySpringContext(org.eclipse.jetty.servlet.ServletContextHandler fastLane) {
            super(fastLane.getServletContext());
            setDisplayName("FastLane-Slimstack-Bridge-Context");
            //this.refresh();
            LOG.info("LazySpringContext: construction complete, diagnostic calls active.");
        }

        public void setDelegate(Object delegate) {
            if (delegate == null) {
                LOG.warn("LazySpringContext: Received null delegate. Main context not ready yet.");
                return;
            }

            // Check if the delegate is actually an Exception (common when Spring fails to start)
            if (delegate instanceof Throwable) {
                LOG.error("LazySpringContext: Cannot set delegate! Main Spring Context failed with: " + 
                        ((Throwable)delegate).getMessage(), (Throwable)delegate);
                return;
            }
           
            try {
                this.delegate = delegate;
                // Find the getBean(String) method on whatever class the delegate is
                //this.getBeanMethod = delegate.getClass().getMethod("getBean", String.class);
                this.getBeanByNameMethod = delegate.getClass().getMethod("getBean", String.class);
                this.getBeanByTypeMethod = delegate.getClass().getMethod("getBean", Class.class);
                LOG.info("Reflection Bridge - Successfully mapped getBean methods.");
            } catch (Exception e) {
                LOG.error("Failed to map getBean method via reflection", e);
            }
        }

        public synchronized boolean isSynchronized() {
            return synchronizedDone;
        }

        public synchronized void markSynchronized() {
            this.synchronizedDone = true;
        }

        @Override
        public long getStartupDate() {
            LOG.info("Diagnostic: getStartupDate was probed by Jersey.");
            return System.currentTimeMillis();
        }

        @Override
        public String getId() {
             LOG.info("Diagnostic: getId was probed by Jersey.");
            return "fast-lane-bridge";
        }

        // @Override
        // public org.springframework.beans.factory.config.ConfigurableListableBeanFactory getBeanFactory() {
        //     // Jersey often calls this to see if the context is 'real'
        //     LOG.info("Diagnostic: getBeanFactory() was probed by Jersey.");
        //     return super.getBeanFactory();
        // }

        @Override
        public java.util.Map<String, Object> getBeansWithAnnotation(Class<? extends java.lang.annotation.Annotation> annotationType) {
            LOG.info("getBeansWithAnnotation called.");
            if (delegate != null) {
                try {
                    java.lang.reflect.Method m = delegate.getClass().getMethod("getBeansWithAnnotation", Class.class);
                    java.util.Map<String, Object> beans = (java.util.Map<String, Object>) m.invoke(delegate, annotationType);
                    LOG.info("Reflection Bridge - Found {} beans for annotation: {}", 
                        (beans != null ? beans.size() : 0), annotationType.getSimpleName());
                    return beans;
                } catch (Exception e) {
                    LOG.error("Reflection bridge (Annotation) failed for: " + annotationType.getName());
                }
            }
            return super.getBeansWithAnnotation(annotationType);
        }

        @Override
        public <T> T getBean(String name, Class<T> requiredType) throws org.springframework.beans.BeansException {
            LOG.info("Diagnostic: getBean(String, Class) called for: {} ({})", name, requiredType.getSimpleName());
            if (delegate != null) {
                try {
                    java.lang.reflect.Method m = delegate.getClass().getMethod("getBean", String.class, Class.class);
                    return (T) m.invoke(delegate, name, requiredType);
                } catch (Exception e) {
                    LOG.error("Reflection bridge failed for: " + name);
                }
            }
            return super.getBean(name, requiredType);
        }

        //Type-only discovery (Returns all beans of a type)
        @Override
        public <T> java.util.Map<String, T> getBeansOfType(Class<T> type) throws org.springframework.beans.BeansException {
            LOG.info("Diagnostic: getBeansOfType called for: {}", type.getSimpleName());
            if (delegate != null) {
                try {
                    java.lang.reflect.Method m = delegate.getClass().getMethod("getBeansOfType", Class.class);
                    return (java.util.Map<String, T>) m.invoke(delegate, type);
                } catch (Exception e) {
                    LOG.error("Reflection bridge (TypeMap) failed");
                }
            }
            return super.getBeansOfType(type);
        }

        @Override
        public <T> T getBean(Class<T> requiredType) throws org.springframework.beans.BeansException {
            LOG.info("getBean( requiredType) called");
            if (delegate != null && getBeanByTypeMethod != null) {
                try {
                    // Use reflection to call getBean(Class) on the delegate
                   // java.lang.reflect.Method m = delegate.getClass().getMethod("getBean", Class.class);
                    Object bean = getBeanByTypeMethod.invoke(delegate, requiredType);
                    if (bean != null) {
                        LOG.info("Reflection Bridge (Type) - Found: '{}', Type: {}, Loader: {}", 
                            requiredType.getSimpleName(), bean.getClass().getName(), bean.getClass().getClassLoader());
                    }
                    return (T) bean;
                } catch (Exception e) {
                    LOG.error("Reflection bridge (type) failed for: " + requiredType.getName());
                }
            }
            else
            {
                LOG.info("getBean called with requiredType {} isn't processed as either delegate {} or getBeanByTypeMethod {} is null",
                 requiredType, 
                 delegate,
                 getBeanByTypeMethod );
            }
            return super.getBean(requiredType);
        }

        @Override
        public Object getBean(String name) throws org.springframework.beans.BeansException {
            LOG.info("getBean( name) called");
            if (delegate != null && getBeanByNameMethod != null) {
                try {
                    Object bean =  getBeanByNameMethod.invoke(delegate, name);
                    //diagnostic block
                    Object parentBean = super.getBean(name);
                    if (bean != null) {
                        String parentBeanName ;
                        if( parentBean != null)
                        {
                            parentBeanName = parentBean.getClass().getName();
                        }
                        else
                        {
                            parentBeanName = "<none>";
                        }
                        // Diagnostic Logging: See what the "real" bean looks like
                        LOG.info("Reflection Bridge - Found bean: '{}', Type: {}, Loader: {}, Parent Type: {}", 
                            name, 
                            bean.getClass().getName(), 
                            bean.getClass().getClassLoader(), 
                            parentBean
                            );
                    } else {
                        LOG.warn("Reflection Bridge - Bean '{}' returned null from delegate", name);
                    }
                    return bean;
                } catch (Exception e) {
                    LOG.error("Reflection bridge failed for bean: " + name, e);
                }
            }
            else
            {
                LOG.info("getBean called with name {} isn't processed as either delegate {} or getBeanByNameMethod {} is null",
                 name, 
                 delegate,
                 getBeanByNameMethod );
            }
            return super.getBean(name);
        }

        //lifecylce events
        @Override
        public void start() {
            LOG.info("Diagnostic: start() called on LazySpringContext");
            super.start();
        }

        @Override
        public void stop() {
            LOG.info("Diagnostic: stop() called on LazySpringContext");
            super.stop();
        }

        @Override
        public boolean isActive() {
            boolean active = super.isActive();
            LOG.info("Diagnostic: isActive() check: {}", active);
            return true;
        }

        @Override
        public boolean isRunning() {
            boolean running = super.isRunning();
            LOG.info("Diagnostic: isRunning() check: {}", running);
            return running;
        }

        // @Override
        // public boolean isActive() { 
        //     LOG.info(" LazySpringContext isActive called. Returning true ");
        //     return true;
        //  }
    }

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
        final org.eclipse.jetty.webapp.WebAppContext mainAppContext = getWebAppContext(atlasPath);
        // Setup the Fast-Lane Context immediately
        final org.eclipse.jetty.servlet.ServletContextHandler fastLaneContext = 
            new org.eclipse.jetty.servlet.ServletContextHandler(org.eclipse.jetty.servlet.ServletContextHandler.SESSIONS);
        fastLaneContext.setClassLoader(mainAppContext.getClassLoader());
        fastLaneContext.setContextPath("/api");
        fastLaneContext.setResourceBase("/opt/apache-atlas/server/webapp/atlas"); //TODO : remove this hard code
        
        Object springContext = mainAppContext.getServletContext().getAttribute(
            org.springframework.web.context.WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE);
        Object curContext = null;
        if (springContext != null) {
            curContext = springContext;
            LOG.info("Spring context ready. Linking to fast lane, slim stack context.");
        } else {
            // This is the "Critical Error" path - it means Main App isn't ready.
            // We will let sync handle it later once the listener fires.
            LOG.warn("Spring Context not ready during Fast-Lane creation. Let's link to a LazySpringContext.");
            curContext = new LazySpringContext(fastLaneContext);     
        }
        
        //correct cast
        if (fastLaneContext instanceof org.eclipse.jetty.webapp.WebAppContext) {
            ((org.eclipse.jetty.webapp.WebAppContext) fastLaneContext).setParentLoaderPriority(true);
        }

        //  Add Health Check (Always available)
        fastLaneContext.addServlet(new org.eclipse.jetty.servlet.ServletHolder(new javax.servlet.http.HttpServlet() {
            @Override
            protected void doGet(javax.servlet.http.HttpServletRequest req, javax.servlet.http.HttpServletResponse resp) 
                throws java.io.IOException {
                resp.setContentType("application/json");
                resp.getWriter().println("{\"status\":\"FAST_LANE_READY\"}");
            }
        }), "/atlas/admin/health");

        final Object curContextForFastLane = curContext;
        //  Prepare the V2 Holder but ***without*** starting; the context is then started manually in a listener
        // Use the Spring-specific servlet implementation
        org.eclipse.jetty.servlet.ServletHolder v2Holder = new org.eclipse.jetty.servlet.ServletHolder( ){
            @Override
            public void doStart() throws Exception {
                LOG.info("V2Holder: Intercepting doStart to ensure Spring Context Bridge is present.");
                
                super.doStart();
            }
        };

        // --- DIAGNOSTIC BLOCK 1: EARLY CHECK ---
        try {
            LOG.info("[DIAG-EARLY] === Checking Initial Fast-Lane State ===");
    
            // Check Jersey Interface
            try {
                Class<?> providerIntf = com.sun.jersey.spi.container.WebApplicationProvider.class;
                LOG.info("[DIAG-EARLY] Jersey Interface Loader: " + providerIntf.getClassLoader());
                LOG.info("[DIAG-EARLY] Jersey Interface Source: " + providerIntf.getProtectionDomain().getCodeSource().getLocation());
            } catch (Throwable t) {
                LOG.info("[DIAG-EARLY] Jersey Interface not yet loaded (this is preferred).");
            }

            //  Check Objects and their ClassLoaders
            LOG.info("[DIAG-EARLY] fastLaneContext Class: " + fastLaneContext.getClass().getName());
            LOG.info("[DIAG-EARLY] fastLaneContext Loader: " + fastLaneContext.getClassLoader());
            
            if (v2Holder != null) {
                LOG.info("[DIAG-EARLY] v2Holder Loader: " + v2Holder.getClass().getClassLoader());
                // Check if v2Holder already has a servlet instance (should be null at this point)
               // LOG.info("[DIAG-EARLY] v2Holder Servlet Instance: " + v2Holder.getServlet());
                LOG.info("[DIAG-EARLY] Holder State: {}, Servlet Instance: {}, Available: {}", 
                                        v2Holder.getState(), 
                                        v2Holder.getServlet(), // If this is NOT null before sync, it's a problem
                                        v2Holder.isAvailable());
                // Context State
                if (fastLaneContext != null) {
                    ClassLoader cl = fastLaneContext.getClassLoader();
                    LOG.info("[DIAG-EARLY] Context: {}, State: {}, Loader: {} (hash: {})", 
                        fastLaneContext.getDisplayName(), 
                        fastLaneContext.getState(), 
                        cl, 
                        (cl != null ? System.identityHashCode(cl) : "null"));
                }


                // Spring Attribute Check
                Object springCtx = mainAppContext.getServletContext().getAttribute(
                    org.springframework.web.context.WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE);
                LOG.info("Spring Context Attribute Present: {}", (springCtx != null));

            }

            if (mainAppContext != null) {
                LOG.info("[DIAG-EARLY] mainAppContext (Atlas) Loader: " + mainAppContext.getClassLoader());
                ClassLoader cl = mainAppContext.getClassLoader();
                    LOG.info("[DIAG-EARLY] Context: {}, State: {}, Loader: {} (hash: {})", 
                        mainAppContext.getDisplayName(), 
                        mainAppContext.getState(), 
                        cl, 
                        (cl != null ? System.identityHashCode(cl) : "null"));
            }

            LOG.info("[DIAG-EARLY] Current Thread Context Loader: " + Thread.currentThread().getContextClassLoader());
            LOG.info("[DIAG-EARLY] EmbeddedServer Loader: " + this.getClass().getClassLoader());
            LOG.info("[DIAG-EARLY] ==============================================");
        } catch (Throwable t) {
            LOG.info("[DIAG-EARLY] Jersey Interface not yet loaded (this is good).");
        }
        // ----------------------------------------

        v2Holder.setClassName("com.sun.jersey.spi.spring.container.servlet.SpringServlet");
        v2Holder.setInitOrder(-1);
        v2Holder.setInitParameter("com.sun.jersey.spi.container.ContainerRequestFilters", 
                          "com.sun.jersey.api.container.filter.LoggingFilter");
        v2Holder.setInitParameter("com.sun.jersey.spi.container.ContainerResponseFilters", 
                                "com.sun.jersey.api.container.filter.LoggingFilter");

        //v2Holder.setInitParameter("contextConfigLocation", "file:/opt/apache-atlas/server/webapp/atlas/WEB-INF/applicationContext.xml");
        v2Holder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", 
                          "com.sun.jersey.api.core.ClassNamesResourceConfig");
        
        //Register classloader for resource loalding from main
        // v2Holder.setInitParameter("com.sun.jersey.config.property.classloader", "true");
        // Explicitly tell Jersey NOT to scan any packages
        v2Holder.setInitParameter("com.sun.jersey.config.property.packages", "");
        // Disable WADL generation (which often triggers an internal scan)
        v2Holder.setInitParameter("com.sun.jersey.config.feature.DisableWadl", "true");
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
   
        // Add /v2 calls to the context now, Jetty will handle the start sequence
        fastLaneContext.addServlet(v2Holder, "/atlas/v2/*");
        fastLaneContext.addServlet(v2Holder, "/meta/*");

        // This Listener is **only** to inject the dependencies once Main App is ready
        // TODO UPDATE : This is only for logging rifht now. Remove after flow check
        mainAppContext.addLifeCycleListener(new org.eclipse.jetty.util.component.AbstractLifeCycle.AbstractLifeCycleListener() {
            @Override
            public void lifeCycleStarted(org.eclipse.jetty.util.component.LifeCycle event) {
               // 
               LOG.info("Diagnostics: Jetty Server called lifeCycleStarted. Is it too early? Before the springContext is ready?...");
            }
        });

        // Routing with correct ordered  setup - first fastLaneContext
        org.eclipse.jetty.server.handler.ContextHandlerCollection contexts = new org.eclipse.jetty.server.handler.ContextHandlerCollection();
        contexts.setHandlers(new org.eclipse.jetty.server.Handler[] {mainAppContext, fastLaneContext});
        server.setHandler(contexts);
        Object mainSpringContext = null;
        // This will start both contexts in the correct order
        try {

            // Iterate through the main app's servlets and disable scanning for its Jersey instances
            for (org.eclipse.jetty.servlet.ServletHolder holder : mainAppContext.getServletHandler().getServlets()) {
                if (holder.getClassName() != null && holder.getClassName().contains("SpringServlet")) {
                    LOG.info("Applying Java 17 compatibility fix to Main App Servlet: " + holder.getName());
                    holder.setInitParameter("com.sun.jersey.config.feature.DisableWadl", "true");
                    // This is to prevent any "IllegalArgumentException" during main app startup
                }
            }

            server.start(); 
            LOG.info("Server started. Main initialization and Context handlers sync block ");
            int attempts = 0;
            //Explicitly sleep for full initialization main
            //Thread.sleep(2000); 
            while (mainSpringContext == null && attempts < 10) {
                mainSpringContext = mainAppContext.getServletContext().getAttribute(
                    org.springframework.web.context.WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE);
                if (mainSpringContext == null) {
                    LOG.info("Waiting for Main App Spring Context to initialize , sleeping for 2s(Attempt {}/10)...", attempts++);
                    Thread.sleep(2000); 
                }
                else
                {
                     LOG.info(" Main App Spring Context ready after {} attempts...", attempts++);
                }
            }
            LOG.info("Jetty Server start signal sent. Proceeding to manual sync...");
        } catch (Exception e) {
            LOG.error("Fatal: Jetty failed to start. Manual sync aborted.", e);
            throw e;
        }
        LOG.info("Server started. Triggering V2 Fast-Lane slim stack synchronization manually not waiting for lifeCycleStarted...");
        try {
            LOG.info("Manual fast lane Sync Trigger: Starting bridge between Main and V2...");
            // check context type
            if (curContext instanceof LazySpringContext) {
                LOG.info("Filling Fast-Lane bridge with real Spring Context.");
                LazySpringContext bridge = (LazySpringContext) curContext;
                // Protection against double-invocation
                if (bridge.isSynchronized()) {
                    LOG.info("Fast-Lane already synchronized. Skipping redundant init.");
                    
                }
                else {
                    LOG.info("Plugging real Spring Context  from main into Reflection Bridge...");
                    bridge.setDelegate(mainSpringContext);
                    LOG.info("Delegate set to main spring context...");
                    LOG.info("V2Holder: Forcing TCCL synchronization before start.");
               
                    // FIX: Initialize the servlet even if the context is already started
                    try {
      
                        //force stop before start
                        //at this time we have fastLaneContext as LazySpringContext . 
                        //Switch and sync
                        //Temporarily set the TCCL to the Main WebApp's loader
                        
                        // Ensure FastLane uses the same loader as the main app
                        v2Holder.stop();
                        
                        Object realMainSpringContext = mainAppContext.getServletContext().getAttribute(
                            org.springframework.web.context.WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE);
                        if(realMainSpringContext != null){
                            //Spring Diagnostics
                            // Log class and loader to ensure it matches what we expect
                            LOG.info("[DIAG-SPRING-BEFORE] Context Loader: " + realMainSpringContext.getClass().getClassLoader());
                            
                            if (realMainSpringContext instanceof org.springframework.context.ConfigurableApplicationContext) {
                                org.springframework.context.ConfigurableApplicationContext cfgCtx = 
                                    (org.springframework.context.ConfigurableApplicationContext) realMainSpringContext;
                                
                                LOG.info("[DIAG-SPRING-BEFORE] ID: {}, Active: {}, Startup: {}", 
                                        cfgCtx.getId(), cfgCtx.isActive(), new java.util.Date(cfgCtx.getStartupDate()));
                                LOG.info("[DIAG-SPRING-BEFORE] Bean Count: {}", cfgCtx.getBeanDefinitionCount());
                            }
                            else
                            {
                                LOG.info("[DIAG-SPRING-BEFORE] realMainSpringContext is not of ConfigurableApplicationContext type  ");
                            }
                            //End Spring Diagnostics
                            //Stop fastLaneContext, if started to change the class loader
                            if (fastLaneContext.isStarted()) {
                                LOG.info("Fast-lane context is already started. Stopping it to apply ClassLoader...");
                                fastLaneContext.stop();
                            }
                            //align classloaders
                            ClassLoader webAppClassLoader = realMainSpringContext.getClass().getClassLoader();
                            fastLaneContext.setClassLoader(webAppClassLoader);
                            ClassLoader originalTCCL = Thread.currentThread().getContextClassLoader();
                             
                            // --- START DIAGNOSTIC BLOCK 2---
                            try {
                                Class<?> providerIntf = com.sun.jersey.spi.container.WebApplicationProvider.class;
                                LOG.info("[DIAG-2] WebApplicationProvider Interface Loader: " + providerIntf.getClassLoader());
                                LOG.info("[DIAG-2] WebApplicationProvider Interface Source: " + providerIntf.getProtectionDomain().getCodeSource().getLocation());

                                try {
                                    Class<?> providerImpl = Class.forName("com.sun.jersey.server.impl.container.WebApplicationProviderImpl", true, webAppClassLoader);
                                    LOG.info("[DIAG-2] WebApplicationProvider Impl Loader: " + providerImpl.getClassLoader());
                                    LOG.info("[DIAG-2] WebApplicationProvider Impl Source: " + providerImpl.getProtectionDomain().getCodeSource().getLocation());
                                    LOG.info("[DIAG-2] Assignment Compatible? " + providerIntf.isAssignableFrom(providerImpl));
                                    LOG.info("[DIAG-2] V2 Holder State -2 : {}, Servlet Instance: {}, Available: {}", 
                                        v2Holder.getState(), 
                                        v2Holder.getServlet(), // If this is NOT null before sync, it's a problem
                                        v2Holder.isAvailable());
                                     
                                    // Context State
                                    if (fastLaneContext != null) {
                                        ClassLoader cl = fastLaneContext.getClassLoader();
                                        LOG.info("[DIAG-2] Fast Context: {}, State: {}, Loader: {} (hash: {})", 
                                            fastLaneContext.getDisplayName(), 
                                            fastLaneContext.getState(), 
                                            cl, 
                                            (cl != null ? System.identityHashCode(cl) : "null"));
                                    }
                                    if (mainAppContext != null) {
                                        LOG.info("[DIAG-2] mainAppContext (Atlas) Loader: " + mainAppContext.getClassLoader());
                                        ClassLoader cl = mainAppContext.getClassLoader();
                                            LOG.info("[DIAG-2] main Context: {}, State: {}, Loader: {} (hash: {})", 
                                                mainAppContext.getDisplayName(), 
                                                mainAppContext.getState(), 
                                                cl, 
                                                (cl != null ? System.identityHashCode(cl) : "null"));
                                    }

                                } catch (ClassNotFoundException e) {
                                    LOG.error("[DIAG-2] WebApplicationProviderImpl NOT FOUND in webAppClassLoader!");
                                }
                            } catch (Throwable t) {
                                LOG.error("[DIAG-2] Diagnostic block failed: " + t.getMessage());
                            }
                            // --- END DIAGNOSTIC BLOCK ---
                            Thread.currentThread().setContextClassLoader(webAppClassLoader);
                            LOG.info("TCCL switched. Calling v2Holder.start()...");
                            LOG.info("Fast-Lane ClassLoader synchronized with Main App.");
                            fastLaneContext.getServletContext().setAttribute(
                                org.springframework.web.context.WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE,
                                realMainSpringContext);
                            if (fastLaneContext instanceof org.eclipse.jetty.webapp.WebAppContext) {
                                ((org.eclipse.jetty.webapp.WebAppContext) fastLaneContext).setParentLoaderPriority(true);
                                LOG.info("ParentLoaderPriority is set to true for Fast-Lane WebAppContext.");
                            } 
                            else
                            {
                                LOG.warn("setParentLoaderPriority not set as fastLaneContext hasn't switched yetfastLaneContext type is: {}", 
                                    fastLaneContext.getClass().getName());
                            }

                            LOG.info("Fast lane context parent priority set. fastLaneContext type is: {} . Ready to call start on fastlaneContext ", 
                                    fastLaneContext.getClass().getName());
                         
                            fastLaneContext.start();
                            LOG.info("Fast lane context started. Ready to start v2Holder");
                                            
                            v2Holder.start();
                            LOG.info("[DIAG-3] Holder State -3 : {}, Servlet Instance: {}, Available: {}", 
                                        v2Holder.getState(), 
                                        v2Holder.getServlet(), // If this is NOT null before sync, it's a problem
                                        v2Holder.isAvailable());
                            // Context State
                            if (fastLaneContext != null) {
                                ClassLoader cl = fastLaneContext.getClassLoader();
                                LOG.info("[DIAG-3] fast Context: {}, State: {}, Loader: {} (hash: {})", 
                                    fastLaneContext.getDisplayName(), 
                                    fastLaneContext.getState(), 
                                    cl, 
                                    (cl != null ? System.identityHashCode(cl) : "null"));
                            }
                            if (mainAppContext != null) {
                                LOG.info("[DIAG-3] mainAppContext (Atlas) Loader: " + mainAppContext.getClassLoader());
                                ClassLoader cl = mainAppContext.getClassLoader();
                                    LOG.info("[DIAG-3] main Context: {}, State: {}, Loader: {} (hash: {})", 
                                        mainAppContext.getDisplayName(), 
                                        mainAppContext.getState(), 
                                        cl, 
                                        (cl != null ? System.identityHashCode(cl) : "null"));
                            }
                            //Spring Diagnostics
                            LOG.info("[DIAG-SPRING-AFTER] Verifying context state after Servlet Start...");
                            if (realMainSpringContext instanceof org.springframework.context.ConfigurableApplicationContext) {
                                boolean active = ((org.springframework.context.ConfigurableApplicationContext) realMainSpringContext).isActive();
                                LOG.info("[DIAG-SPRING-AFTER] Context still active: {}", active);
                            }
                            else
                            {
                                LOG.info("[DIAG-SPRING-AFTER] realMainSpringContext is not of ConfigurableApplicationContext type  ");
                            }
                            //End Spring Diagnostics
                            bridge.markSynchronized();
                            LOG.info("V2 Fast-Lane Jersey Servlet initialized successfully.");
                            Thread.currentThread().setContextClassLoader(originalTCCL);
                        }
                        else
                        {
                            LOG.error("Critical error : main spring context is null, can't create fast lane");
                        }

                    } catch (Exception e) {
                        LOG.error("Failed to initialize V2 Jersey Servlet", e);
                    }
                }   
                LOG.info("V2 Optimization Path Linked and Synchronized.");

            }
            else
            {
                LOG.info("Fast lane context can't start as delegated spring context does not exist");
            }



        } catch (Exception e) {
            LOG.error("Critical error linking V2 optimization path. Manual fast lane Sync failed: The bridge could not be established.", e);
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
