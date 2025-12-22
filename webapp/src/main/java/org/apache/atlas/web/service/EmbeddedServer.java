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
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.eclipse.jetty.ee8.webapp.WebAppContext;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;

/**
 * This class embeds a Jetty server and a connector.
 * Updated for Jetty 12 with EE8 (javax.servlet) compatibility.
 */
public class EmbeddedServer {
    public static final Logger LOG = LoggerFactory.getLogger(EmbeddedServer.class);

    public static final String ATLAS_DEFAULT_BIND_ADDRESS = "0.0.0.0";

    protected final Server server;

    public EmbeddedServer(String host, int port, String path) throws IOException {
        int minThreads    = AtlasConfiguration.WEBSERVER_MIN_THREADS.getInt();
        int maxThreads    = AtlasConfiguration.WEBSERVER_MAX_THREADS.getInt();
        int idleTimeout   = (int) AtlasConfiguration.WEBSERVER_KEEPALIVE_SECONDS.getLong() * 1000;

        // Jetty 12 uses QueuedThreadPool instead of ExecutorThreadPool
        QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeout);

        server = new Server(threadPool);

        Connector connector = getConnector(host, port);
        connector.addBean(new JettyConnectionMetrics(getMeterRegistry()));
        new JettyServerThreadPoolMetrics(threadPool, Collections.emptyList()).bindTo(getMeterRegistry());
        server.addConnector(connector);

        WebAppContext application = getWebAppContext(path);
        server.setHandler(application);
    }

    protected WebAppContext getWebAppContext(String path) {
        // Jetty 12 EE8 WebAppContext (javax.servlet compatible)
        WebAppContext application = new WebAppContext();
        application.setContextPath("/");
        application.setWar(path);
        application.setClassLoader(Thread.currentThread().getContextClassLoader());
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

            server.join();
        } catch(Exception e) {
            throw new AtlasBaseException(AtlasErrorCode.EMBEDDED_SERVER_START, e);
        }
    }

    public void stop() {
        try {
            server.stop();
        } catch (Exception e) {
            LOG.warn("Error during shutdown", e);
        }
    }
}
