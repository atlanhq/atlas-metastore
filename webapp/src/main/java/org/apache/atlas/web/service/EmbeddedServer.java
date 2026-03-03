package org.apache.atlas.web.service;

import io.micrometer.core.instrument.binder.jetty.JettyConnectionMetrics;
import io.micrometer.core.instrument.binder.jetty.JettyServerThreadPoolMetrics;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.filter.DelegatingFilterProxy;

import javax.servlet.DispatcherType;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;

public class EmbeddedServer {
    public static final Logger LOG = LoggerFactory.getLogger(EmbeddedServer.class);
    protected final Server server;
    protected String atlasPath;

    /**
     * LazySpringContext acts as a bridge between the Fast-Lane Jersey Servlet 
     * and the main Spring WebApplicationContext which is loaded later.
     */
    private static class LazySpringContext extends org.springframework.web.context.support.GenericWebApplicationContext {
        private Object delegate;
        private boolean synchronizedDone = false;

        public LazySpringContext(ServletContextHandler fastLane) {
            super(fastLane.getServletContext());
            setDisplayName("FastLane-Bridge-Context");
        }

        public void setDelegate(Object delegate) {
            this.delegate = delegate;
            LOG.info("LazySpringContext: Main Spring Context delegate linked.");
        }

        public synchronized boolean isSynchronized() { return synchronizedDone; }
        public synchronized void markSynchronized() { this.synchronizedDone = true; }

        @Override
        public Object getBean(String name) {
            if (delegate != null) {
                try {
                    return delegate.getClass().getMethod("getBean", String.class).invoke(delegate, name);
                } catch (Exception e) { LOG.error("Bridge failed for bean: " + name); }
            }
            return super.getBean(name);
        }

        @Override
        public <T> T getBean(Class<T> requiredType) {
            if (delegate != null) {
                try {
                    return (T) delegate.getClass().getMethod("getBean", Class.class).invoke(delegate, requiredType);
                } catch (Exception e) { LOG.error("Bridge failed for type: " + requiredType.getName()); }
            }
            return super.getBean(requiredType);
        }
    }

    public EmbeddedServer(String host, int port, String atlasPath) throws IOException {
        int queueSize = AtlasConfiguration.WEBSERVER_QUEUE_SIZE.getInt();
        int minThreads = AtlasConfiguration.WEBSERVER_MIN_THREADS.getInt();
        int maxThreads = AtlasConfiguration.WEBSERVER_MAX_THREADS.getInt();
        long keepAlive = AtlasConfiguration.WEBSERVER_KEEPALIVE_SECONDS.getLong();
        
        ThreadPoolExecutor executor = new ThreadPoolExecutor(minThreads, maxThreads, keepAlive, TimeUnit.SECONDS, new LinkedBlockingQueue<>(queueSize));
        executor.allowCoreThreadTimeOut(true);

        this.server = new Server(new ExecutorThreadPool(executor));
        this.atlasPath = atlasPath;

        HttpConfiguration http_config = new HttpConfiguration();
        http_config.setSendServerVersion(false);
        ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory(http_config));
        connector.setPort(port);
        connector.setHost(host);
        server.addConnector(connector);

        getMeterRegistry().bindTo(server);
        new JettyServerThreadPoolMetrics(executor, Collections.emptyList()).bindTo(getMeterRegistry());
        new JettyConnectionMetrics(server).bindTo(getMeterRegistry());
    }

    protected WebAppContext getWebAppContext(String path) {
        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setContextPath("/");
        webAppContext.setWar(path);
        return webAppContext;
    }

public void start() throws AtlasBaseException {
        try {
            final WebAppContext mainAppContext = getWebAppContext(atlasPath);
            LOG.info("Start method called for setting up fast lane slim stack servlet and context...");
            // --- JAVA 17 ASM SCANNING WORKAROUND ---
            final String jerseyClassNames = 
                "org.apache.atlas.web.resources.AdminResource;" +
                "org.apache.atlas.web.rest.AttributeREST;" +
                "org.apache.atlas.web.rest.AuthREST;" +
                "org.apache.atlas.web.rest.BusinessLineageREST;" +
                "org.apache.atlas.web.rest.BusinessPolicyREST;" +
                "org.apache.atlas.web.rest.ConfigCacheRefreshREST;" +
                "org.apache.atlas.web.rest.ConfigREST;" +
                "org.apache.atlas.web.rest.DirectSearchREST;" +
                "org.apache.atlas.web.rest.DiscoveryREST;" +
                "org.apache.atlas.web.rest.EntityREST;" +
                "org.apache.atlas.web.rest.FeatureFlagREST;" +
                "org.apache.atlas.web.rest.GlossaryREST;" +
                "org.apache.atlas.web.rest.LineageREST;" +
                "org.apache.atlas.web.rest.MeshEntityAssetLinkREST;" +
                "org.apache.atlas.web.rest.MigrationREST;" +
                "org.apache.atlas.web.rest.ModelREST;" +
                "org.apache.atlas.web.rest.RelationshipREST;" +
                "org.apache.atlas.web.rest.RepairREST;" +
                "org.apache.atlas.web.rest.TaskREST;" +
                "org.apache.atlas.web.rest.TypeCacheRefreshREST;" +
                "org.apache.atlas.web.rest.TypesREST;" +
                "org.apache.atlas.web.rest.PurposeDiscoveryREST;" + // Restored
                "org.apache.atlas.web.util.Servlets;" +
                "com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;" +
                "org.apache.atlas.web.util.AtlasJsonProvider;" +
                "org.apache.atlas.web.errors.AtlasBaseExceptionMapper;" +
                "org.apache.atlas.web.errors.AllExceptionMapper;" +
                "org.apache.atlas.web.errors.NotFoundExceptionMapper;" +
                "org.apache.atlas.web.filters.AtlasAuthenticationFilter";

            mainAppContext.addEventListener(new javax.servlet.ServletContextListener() {
                @Override
                public void contextInitialized(javax.servlet.ServletContextEvent sce) {
                    LOG.info("Context initialized for Main App Context...");
                    for (ServletHolder holder : mainAppContext.getServletHandler().getServlets()) {
                        LOG.info("Initializing Servlet: {}", holder.getClassName());
                        if (holder.getClassName() != null && holder.getClassName().contains("SpringServlet")) {
                            holder.setInitParameter("com.sun.jersey.config.property.classnames", jerseyClassNames);
                        }
                    }
                }
                @Override public void contextDestroyed(javax.servlet.ServletContextEvent sce) {}
            });

            // Start Server with ONLY the Main Context
            ContextHandlerCollection contexts = new ContextHandlerCollection();
            contexts.setHandlers(new Handler[] {mainAppContext});
            LoG.info("Configuring Jetty with Main App Context...");
            server.setHandler(contexts);

            LOG.info("Starting Jetty with Main App Context...");
            server.start();

            //Background Thread to Wait and Inject Fast-Lane
            new Thread(() -> {
                try {
                    Object mainSpringContext = null;
                    int attempts = 0;
                    while (mainSpringContext == null && attempts < 20) {
                        mainSpringContext = mainAppContext.getServletContext().getAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE);
                        if (mainSpringContext == null) {
                            LOG.info("Staged Start: Waiting for Main Spring (Attempt {})...", ++attempts);
                            Thread.sleep(5000);
                        }
                    }

                    if (mainSpringContext != null) {
                        LOG.info("Staged Start: Main Spring ready. Injecting Fast-Lane...");
                        
                        WebApplicationContext spring = (WebApplicationContext) mainSpringContext;
                        ClassLoader webAppClassLoader = spring.getClassLoader();
                        Log.info("Fast-Lane will use main context's WebApp ClassLoader: {}", webAppClassLoader);
                        // Create and Configure Fast-Lane AFTER Spring is ready
                        ServletContextHandler fastLaneContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
                        fastLaneContext.setContextPath("/api");
                        fastLaneContext.setClassLoader(webAppClassLoader);
                        
                        // Link the active SessionHandler
                        fastLaneContext.setSessionHandler(mainAppContext.getSessionHandler());
                        
                        // Pass the real Spring context directly
                        fastLaneContext.getServletContext().setAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE, spring);

                        ServletHolder v2Holder = new ServletHolder();
                        v2Holder.setClassName("com.sun.jersey.spi.spring.container.servlet.SpringServlet");
                        v2Holder.setInitOrder(1);
                        v2Holder.setInitParameter("com.sun.jersey.config.property.classnames", jerseyClassNames);
                        v2Holder.setInitParameter("com.sun.jersey.config.feature.DisableWadl", "true");

                        // Security
                        FilterHolder securityFilter = new FilterHolder(new DelegatingFilterProxy("springSecurityFilterChain"));
                        fastLaneContext.addFilter(securityFilter, "/*", EnumSet.allOf(DispatcherType.class));

                        // Mappings
                        fastLaneContext.addServlet(v2Holder, "/atlas/v2/*");
                        fastLaneContext.addServlet(v2Holder, "/meta/*");
                        fastLaneContext.addServlet(v2Holder, "/atlas/v1/search/*");
                        fastLaneContext.addServlet(v2Holder, "/atlas/lineage/*");
                        fastLaneContext.addServlet(v2Holder, "/atlas/admin/*");
                        Log.info("Fast-Lane ServletHolder configured with Jersey and Spring integration.");

                        // Health Check
                        protected void doGet(javax.servlet.http.HttpServletRequest req, javax.servlet.http.HttpServletResponse resp) 
                                throws java.io.IOException {
                                resp.setContentType("application/json");
                                resp.setStatus(200);
                                resp.getWriter().println("{\"status\":\"FAST_LANE_PASSIVE_READY\"}");
                            }
                        }), "/atlas/admin/health");

                        // STEP 4: Add to collection and start the new context
                        contexts.addHandler(fastLaneContext);
                        Log.info("Fast-Lane context added to Jetty. Starting Fast-Lane context...");
                        fastLaneContext.start();
                        
                        LOG.info("V2 Fast-Lane Optimization Path Successfully Injected and Started.");
                    }
                } catch (Exception e) {
                    LOG.error("Failed to inject Fast-Lane context", e);
                }
            }).start();

            server.join();
        } catch (Exception e) {
            throw new AtlasBaseException(AtlasErrorCode.EMBEDDED_SERVER_START, e);
        }
    }

    public void stop() {
        try { server.stop(); } catch (Exception e) { LOG.warn("Error during shutdown", e); }
    }
}