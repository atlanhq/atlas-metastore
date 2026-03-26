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

import io.micrometer.core.instrument.Gauge;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.FilterInfo;
import io.undertow.servlet.api.ListenerInfo;
import io.undertow.servlet.api.ServletInfo;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Options;
import org.xnio.XnioWorker;

import javax.servlet.DispatcherType;
import javax.servlet.ServletException;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;

/**
 * Embedded HTTP server backed by Undertow.
 *
 * Migrated from Jetty 9.4 to Undertow for improved throughput:
 * - XNIO non-blocking I/O: idle connections consume zero worker threads
 * - Better memory efficiency: only worker threads for active requests
 * - Same javax.servlet API: all filters, Spring Security, Keycloak adapter work unchanged
 *
 * @see <a href="https://linear.app/atlan-epd/issue/MS-885">MS-885</a>
 */
public class EmbeddedServer {
    public static final Logger LOG = LoggerFactory.getLogger(EmbeddedServer.class);
    public static final String ATLAS_DEFAULT_BIND_ADDRESS = "0.0.0.0";

    protected Undertow server;
    protected DeploymentManager deploymentManager;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    // Exposed for metrics — the worker thread pool backing Undertow's task threads
    protected ThreadPoolExecutor workerExecutor;
    protected LinkedBlockingQueue<Runnable> workerQueue;

    public EmbeddedServer(String host, int port, String path) throws IOException {
        int  queueSize     = AtlasConfiguration.WEBSERVER_QUEUE_SIZE.getInt();
        int  minThreads    = AtlasConfiguration.WEBSERVER_MIN_THREADS.getInt();
        int  maxThreads    = AtlasConfiguration.WEBSERVER_MAX_THREADS.getInt();
        long keepAliveTime = AtlasConfiguration.WEBSERVER_KEEPALIVE_SECONDS.getLong();
        int  bufferSize    = AtlasConfiguration.WEBSERVER_REQUEST_BUFFER_SIZE.getInt();
        long idleTimeoutMs = AtlasConfiguration.WEBSERVER_IDLE_TIMEOUT_MS.getLong();

        // I/O threads handle NIO select loop (non-blocking). Rule of thumb: 1-2 per core.
        int ioThreads = Math.max(2, Runtime.getRuntime().availableProcessors());

        try {
            // Deploy the webapp using Undertow's servlet container
            HttpHandler servletHandler = deployWebapp(path);

            // Build the Undertow server
            Undertow.Builder builder = Undertow.builder();
            configureListener(builder, host, port);

            builder.setHandler(servletHandler)
                    .setIoThreads(ioThreads)
                    .setWorkerThreads(maxThreads)
                    // Worker thread pool options
                    .setWorkerOption(Options.WORKER_TASK_KEEPALIVE, (int) (keepAliveTime * 1000))
                    .setWorkerOption(Options.WORKER_TASK_MAX_THREADS, maxThreads)
                    .setWorkerOption(Options.WORKER_TASK_CORE_THREADS, minThreads)
                    // Connection options
                    .setSocketOption(Options.KEEP_ALIVE, true)
                    // Buffer configuration
                    .setBufferSize(bufferSize)
                    // Server options
                    .setServerOption(io.undertow.UndertowOptions.IDLE_TIMEOUT, (int) idleTimeoutMs)
                    .setServerOption(io.undertow.UndertowOptions.NO_REQUEST_TIMEOUT, (int) idleTimeoutMs)
                    .setServerOption(io.undertow.UndertowOptions.MAX_HEADER_SIZE, bufferSize)
                    .setServerOption(io.undertow.UndertowOptions.ENABLE_HTTP2, false);

            server = builder.build();

            // Register metrics after build (worker is created during build)
            LOG.info("Undertow configured: ioThreads={}, workerThreads={}, queueSize={}, " +
                            "keepAlive={}s, idleTimeout={}ms, bufferSize={}",
                    ioThreads, maxThreads, queueSize, keepAliveTime, idleTimeoutMs, bufferSize);

        } catch (ServletException e) {
            throw new IOException("Failed to deploy webapp at " + path, e);
        }
    }

    /**
     * Deploy the webapp from the expanded WAR directory using Undertow's DeploymentManager.
     *
     * Unlike Jetty's WebAppContext which auto-parses web.xml, Undertow's DeploymentInfo
     * requires explicit registration of servlets, filters, and listeners. This method
     * programmatically mirrors the declarations in WEB-INF/web.xml.
     */
    protected HttpHandler deployWebapp(String path) throws ServletException {
        File webappDir = new File(path);
        if (!webappDir.exists()) {
            throw new ServletException("Webapp directory does not exist: " + path);
        }

        DeploymentInfo deploymentInfo = Servlets.deployment()
                .setClassLoader(Thread.currentThread().getContextClassLoader())
                .setContextPath("/")
                .setDeploymentName("atlas")
                .setResourceManager(new io.undertow.server.handlers.resource.FileResourceManager(webappDir, 0))
                .setDefaultEncoding("UTF-8")
                .setDefaultSessionTimeout(3600); // 60 minutes, matching web.xml

        // === Listeners (from web.xml) ===
        // KerberosAwareListener extends ContextLoaderListener — initializes Spring context
        deploymentInfo.addListener(new ListenerInfo(org.apache.atlas.web.setup.KerberosAwareListener.class));
        deploymentInfo.addListener(new ListenerInfo(org.springframework.web.context.request.RequestContextListener.class));
        deploymentInfo.addListener(new ListenerInfo(org.springframework.security.web.session.HttpSessionEventPublisher.class));

        // Spring context config location (normally from web.xml context-param)
        deploymentInfo.addInitParameter("contextConfigLocation", "classpath*:META-INF/spring/*.xml,/WEB-INF/applicationContext.xml");

        // === Servlets (from web.xml) ===
        ServletInfo jerseyServlet = new ServletInfo("jersey-servlet",
                com.sun.jersey.spi.spring.container.servlet.SpringServlet.class)
                .addInitParam("com.sun.jersey.api.json.POJOMappingFeature", "true")
                .addMapping("/api/atlas/*")
                .addMapping("/api/atlas/v2/*")
                .addMapping("/api/meta/*")
                .setLoadOnStartup(1);
        deploymentInfo.addServlet(jerseyServlet);

        ServletInfo loginServlet = new ServletInfo("LoginServlet",
                org.apache.atlas.web.servlets.AtlasLoginServlet.class)
                .addMapping("/login.jsp");
        deploymentInfo.addServlet(loginServlet);

        ServletInfo errorServlet = new ServletInfo("ErrorServlet",
                org.apache.atlas.web.servlets.AtlasErrorServlet.class)
                .addMapping("/error.jsp");
        deploymentInfo.addServlet(errorServlet);

        // === Filters (from web.xml, order matters) ===
        deploymentInfo.addFilter(new FilterInfo("springSecurityFilterChain",
                org.springframework.web.filter.DelegatingFilterProxy.class));
        deploymentInfo.addFilterUrlMapping("springSecurityFilterChain", "/*", DispatcherType.REQUEST);

        deploymentInfo.addFilter(new FilterInfo("MetricsFilter",
                org.apache.atlas.web.filters.MetricsFilter.class));
        deploymentInfo.addFilterUrlMapping("MetricsFilter", "/*", DispatcherType.REQUEST);

        deploymentInfo.addFilter(new FilterInfo("AuditFilter",
                org.apache.atlas.web.filters.AuditFilter.class));
        deploymentInfo.addFilterUrlMapping("AuditFilter", "/*", DispatcherType.REQUEST);

        deploymentInfo.addFilter(new FilterInfo("HeaderFilter",
                org.apache.atlas.web.filters.AtlasHeaderFilter.class));
        deploymentInfo.addFilterUrlMapping("HeaderFilter", "/api/atlas/admin/metrics", DispatcherType.REQUEST);
        deploymentInfo.addFilterUrlMapping("HeaderFilter", "/api/atlas/admin/metrics/prometheus", DispatcherType.REQUEST);
        deploymentInfo.addFilterUrlMapping("HeaderFilter", "/api/atlas/admin/status", DispatcherType.REQUEST);

        deploymentInfo.addFilter(new FilterInfo("brotliFilter",
                org.apache.atlas.web.filters.BrotliCompressionFilter.class));
        deploymentInfo.addFilterUrlMapping("brotliFilter", "/*", DispatcherType.REQUEST);

        // === Session config (from web.xml) ===
        deploymentInfo.setDefaultSessionTimeout(3600); // 60 minutes
        deploymentInfo.setServletSessionConfig(new io.undertow.servlet.api.ServletSessionConfig()
                .setName("ATLASSESSIONID")
                .setHttpOnly(true)
                .setSecure(true));

        // === Error pages (from web.xml) ===
        deploymentInfo.addErrorPage(new io.undertow.servlet.api.ErrorPage("/error.jsp", 404));
        deploymentInfo.addErrorPage(new io.undertow.servlet.api.ErrorPage("/error.jsp", 500));
        deploymentInfo.addErrorPage(new io.undertow.servlet.api.ErrorPage("/error.jsp", Throwable.class));

        LOG.info("Deploying webapp from {} with {} servlets, {} filters, {} listeners",
                path, 3, 5, 3);

        deploymentManager = Servlets.defaultContainer().addDeployment(deploymentInfo);
        deploymentManager.deploy();

        return deploymentManager.start();
    }

    /**
     * Configure the HTTP listener. Overridden by SecureEmbeddedServer for HTTPS.
     */
    protected void configureListener(Undertow.Builder builder, String host, int port) {
        builder.addHttpListener(port, host);
    }

    public static EmbeddedServer newServer(String host, int port, String path, boolean secure)
            throws IOException {
        if (secure) {
            return new SecureEmbeddedServer(host, port, path);
        } else {
            return new EmbeddedServer(host, port, path);
        }
    }

    /**
     * Register metrics for the Undertow worker thread pool.
     * Called after server.start() when the XNIO worker is available.
     */
    protected void registerMetrics() {
        try {
            XnioWorker worker = server.getWorker();
            if (worker != null) {
                // Thread pool metrics via XNIO worker stats
                Gauge.builder("undertow.worker.io.threads", worker, w -> (double) w.getIoThreadCount())
                        .description("Number of XNIO I/O threads (NIO select loop)")
                        .register(getMeterRegistry());

                // Undertow doesn't expose the internal task pool directly via XnioWorker.
                // We use the MXBean approach for worker thread stats.
                Gauge.builder("undertow.worker.task.threads.active", worker,
                                w -> (double) w.getMXBean().getWorkerPoolSize())
                        .description("Current worker thread pool size")
                        .register(getMeterRegistry());

                Gauge.builder("undertow.worker.task.threads.busy", worker,
                                w -> (double) (w.getMXBean().getWorkerPoolSize() - w.getMXBean().getWorkerQueueSize()))
                        .description("Busy worker threads (pool size minus queue)")
                        .register(getMeterRegistry());

                Gauge.builder("undertow.worker.task.queue.size", worker,
                                w -> (double) w.getMXBean().getWorkerQueueSize())
                        .description("Tasks queued waiting for a worker thread")
                        .register(getMeterRegistry());

                // Preserve backward-compatible metric names for existing dashboards
                Gauge.builder("jetty.threads.current", worker,
                                w -> (double) w.getMXBean().getWorkerPoolSize())
                        .description("Current thread count (backward-compatible name)")
                        .register(getMeterRegistry());

                Gauge.builder("jetty.threads.idle", worker,
                                w -> (double) w.getMXBean().getWorkerQueueSize())
                        .description("Idle threads (backward-compatible name)")
                        .register(getMeterRegistry());
            }
        } catch (Exception e) {
            LOG.warn("Failed to register Undertow worker metrics", e);
        }

        // Connection metrics via Undertow's built-in statistics handler
        // These are registered via the ConnectorStatistics listener if available
        LOG.info("Undertow metrics registered");
    }

    public void start() throws AtlasBaseException {
        try {
            server.start();
            registerMetrics();
            LOG.info("Undertow server started");

            // Block the main thread (equivalent to Jetty's server.join())
            shutdownLatch.await();
        } catch (Exception e) {
            throw new AtlasBaseException(AtlasErrorCode.EMBEDDED_SERVER_START, e);
        }
    }

    public void stop() {
        try {
            if (deploymentManager != null) {
                deploymentManager.stop();
                deploymentManager.undeploy();
            }
            if (server != null) {
                server.stop();
            }
            shutdownLatch.countDown();
            LOG.info("Undertow server stopped");
        } catch (Exception e) {
            LOG.warn("Error during shutdown", e);
        }
    }
}

