package org.apache.atlas.web.service;

import io.micrometer.core.instrument.binder.jetty.JettyConnectionMetrics;
import io.micrometer.core.instrument.binder.jetty.JettyServerThreadPoolMetrics;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.eclipse.jetty.server.Connector;
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
//import org.springframework.web.context.WebApplicationContext;
//import org.springframework.web.filter.DelegatingFilterProxy;

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
    public static final String ATLAS_DEFAULT_BIND_ADDRESS = "0.0.0.0";
    private static final String SPRING_CONTEXT_KEY = "org.springframework.web.context.WebApplicationContext.ROOT";
    protected final Server server;
    protected String atlasPath;

    /**
     * LazySpringContext acts as a bridge between the Fast-Lane Jersey Servlet 
     * and the main Spring WebApplicationContext which is loaded later.
     */
    // private static class LazySpringContext extends org.springframework.web.context.support.GenericWebApplicationContext {
    //     private Object delegate;
    //     private boolean synchronizedDone = false;

    //     public LazySpringContext(ServletContextHandler fastLane) {
    //         super(fastLane.getServletContext());
    //         setDisplayName("FastLane-Bridge-Context");
    //     }

    //     public void setDelegate(Object delegate) {
    //         this.delegate = delegate;
    //         LOG.info("LazySpringContext: Main Spring Context delegate linked.");
    //     }

    //     public synchronized boolean isSynchronized() { return synchronizedDone; }
    //     public synchronized void markSynchronized() { this.synchronizedDone = true; }

    //     @Override
    //     public Object getBean(String name) {
    //         if (delegate != null) {
    //             try {
    //                 return delegate.getClass().getMethod("getBean", String.class).invoke(delegate, name);
    //             } catch (Exception e) { LOG.error("Bridge failed for bean: " + name); }
    //         }
    //         return super.getBean(name);
    //     }

    //     @Override
    //     public <T> T getBean(Class<T> requiredType) {
    //         if (delegate != null) {
    //             try {
    //                 return (T) delegate.getClass().getMethod("getBean", Class.class).invoke(delegate, requiredType);
    //             } catch (Exception e) { LOG.error("Bridge failed for type: " + requiredType.getName()); }
    //         }
    //         return super.getBean(requiredType);
    //     }
    // }

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
        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setContextPath("/");
        webAppContext.setWar(path);
        return webAppContext;
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


    public static EmbeddedServer newServer(String host, int port, String path, boolean secure)
            throws IOException {
        if (secure) {
            return new SecureEmbeddedServer(host, port, path);
        } else {
            return new EmbeddedServer(host, port, path);
        }
    }

    public void start() throws AtlasBaseException {
        try {
            final WebAppContext mainAppContext = getWebAppContext(atlasPath);

            // Complete list of Atlas REST resources for the Fast-Lane
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
                "org.apache.atlas.web.rest.PurposeDiscoveryREST;" +
                "org.apache.atlas.web.util.Servlets;" +
                "com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;" +
                "org.apache.atlas.web.util.AtlasJsonProvider;" +
                "org.apache.atlas.web.errors.AtlasBaseExceptionMapper;" +
                "org.apache.atlas.web.errors.AllExceptionMapper;" +
                "org.apache.atlas.web.errors.NotFoundExceptionMapper;" +
                "org.apache.atlas.web.filters.AtlasAuthenticationFilter";

            ContextHandlerCollection contexts = new ContextHandlerCollection();
            contexts.setHandlers(new Handler[] {mainAppContext});
            server.setHandler(contexts);

            LOG.info("Starting Main Atlas Engine...");
            server.start();

            // Use a thread to avoid blocking the main server join
            new Thread(() -> {
                try {
                    Object springContext = null;
                    int attempts = 0;
                    String contextKey = "org.springframework.web.context.WebApplicationContext.ROOT";

                    // Wait for the WAR's Spring Context to be initialized inside the Jetty ClassLoader
                    while (springContext == null && attempts < 60) {
                        springContext = mainAppContext.getServletContext().getAttribute(contextKey);
                        if (springContext == null) {
                            if (attempts % 5 == 0) LOG.info("Fast-Lane: Waiting for Spring Context (Attempt {}/60)...", attempts);
                            Thread.sleep(5000);
                            attempts++;
                        }
                    }

                    if (springContext != null) {
                        LOG.info("Fast-Lane: Spring Context found. Injecting optimization path...");
                        
                        // Create context using the same ClassLoader as the WAR
                        ServletContextHandler fastLaneContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
                        fastLaneContext.setContextPath("/api");
                        fastLaneContext.setClassLoader(mainAppContext.getClassLoader());
                        
                        // Hand over the Spring Context as a generic Object to avoid ClassCastException
                        fastLaneContext.getServletContext().setAttribute(contextKey, springContext);

                        // Configure Filter by class name string
                        FilterHolder securityFilter = new FilterHolder();
                        securityFilter.setClassName("org.springframework.web.filter.DelegatingFilterProxy");
                        securityFilter.setInitParameter("targetBeanName", "springSecurityFilterChain");
                        fastLaneContext.addFilter(securityFilter, "/*", EnumSet.allOf(DispatcherType.class));

                        // Configure Jersey Servlet by class name string
                        ServletHolder v2Holder = new ServletHolder();
                        v2Holder.setClassName("com.sun.jersey.spi.spring.container.servlet.SpringServlet");
                        v2Holder.setInitParameter("com.sun.jersey.config.property.classnames", jerseyClassNames);
                        v2Holder.setInitParameter("com.sun.jersey.config.feature.DisableWadl", "true");
                        v2Holder.setInitOrder(1);

                        // Map optimized endpoints
                        fastLaneContext.addServlet(v2Holder, "/atlas/v2/*");
                        fastLaneContext.addServlet(v2Holder, "/meta/*");
                        fastLaneContext.addServlet(v2Holder, "/atlas/lineage/*");
                        fastLaneContext.addServlet(v2Holder, "/atlas/admin/*");

                        // Dynamically add to the running server
                        contexts.addHandler(fastLaneContext);
                        fastLaneContext.start();
                        
                        LOG.info("V2 Fast-Lane Optimization Path ACTIVE.");
                    } else {
                        LOG.error("Fast-Lane failed: Spring context timeout.");
                    }
                } catch (Exception e) {
                    LOG.error("Error during Fast-Lane injection", e);
                }
            }, "FastLane-Injector").start();

            server.join();
        } catch (Exception e) {
            throw new AtlasBaseException(AtlasErrorCode.EMBEDDED_SERVER_START, e);
        }
    }

    public Server getServer() {
        return this.server;
    }

    public void stop() {
        try { server.stop(); } catch (Exception e) { LOG.warn("Error during shutdown", e); }
    }
}