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

import io.undertow.Undertow;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.List;

import static org.apache.atlas.security.SecurityProperties.*;
import static org.apache.atlas.security.SecurityUtil.getPassword;

/**
 * Undertow-based HTTPS embedded server with client certificate support.
 * Replaces the Jetty-based SecureEmbeddedServer.
 */
public class SecureEmbeddedServer extends EmbeddedServer {

    private static final Logger LOG = LoggerFactory.getLogger(SecureEmbeddedServer.class);

    public static final String ATLAS_KEYSTORE_FILE_TYPE_DEFAULT = "jks";
    public static final String ATLAS_TRUSTSTORE_FILE_TYPE_DEFAULT = "jks";
    public static final String ATLAS_TLS_CONTEXT_ALGO_TYPE = "TLS";
    public static final String ATLAS_TLS_KEYMANAGER_DEFAULT_ALGO_TYPE = KeyManagerFactory.getDefaultAlgorithm();
    public static final String ATLAS_TLS_TRUSTMANAGER_DEFAULT_ALGO_TYPE = TrustManagerFactory.getDefaultAlgorithm();

    public SecureEmbeddedServer(String host, int port, String path) throws IOException {
        super(host, port, path);
    }

    @Override
    protected void configureListener(Undertow.Builder builder, String host, int port) {
        SSLContext sslContext = getSSLContext();
        if (sslContext != null) {
            builder.addHttpsListener(port, host, sslContext);
            LOG.info("Undertow HTTPS listener configured on {}:{}", host, port);
        } else {
            LOG.warn("SSLContext is null, falling back to HTTP listener");
            builder.addHttpListener(port, host);
        }
    }

    protected org.apache.commons.configuration.Configuration getConfiguration() {
        try {
            return ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new RuntimeException("Unable to load configuration: " + ApplicationProperties.APPLICATION_PROPERTIES);
        }
    }

    private SSLContext getSSLContext() {
        KeyManager[] kmList = getKeyManagers();
        TrustManager[] tmList = getTrustManagers();
        SSLContext sslContext = null;
        if (tmList != null) {
            try {
                sslContext = SSLContext.getInstance(ATLAS_TLS_CONTEXT_ALGO_TYPE);
                sslContext.init(kmList, tmList, new SecureRandom());
            } catch (NoSuchAlgorithmException e) {
                LOG.error("SSL algorithm is not available in the environment. Reason: " + e.toString());
            } catch (KeyManagementException e) {
                LOG.error("Unable to initialize the SSLContext. Reason: " + e.toString());
            }
        }
        return sslContext;
    }

    private KeyManager[] getKeyManagers() {
        KeyManager[] kmList = null;
        try {
            String keyStoreFile = getConfiguration().getString(KEYSTORE_FILE_KEY,
                    System.getProperty(KEYSTORE_FILE_KEY, DEFAULT_KEYSTORE_FILE_LOCATION));
            String keyStoreFilepwd = getPassword(getConfiguration(), KEYSTORE_PASSWORD_KEY);

            if (StringUtils.isNotEmpty(keyStoreFile) && StringUtils.isNotEmpty(keyStoreFilepwd)) {
                InputStream in = null;
                try {
                    in = getFileInputStream(keyStoreFile);
                    if (in != null) {
                        KeyStore keyStore = KeyStore.getInstance(
                                getConfiguration().getString(KEYSTORE_TYPE, ATLAS_KEYSTORE_FILE_TYPE_DEFAULT));
                        keyStore.load(in, keyStoreFilepwd.toCharArray());
                        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(ATLAS_TLS_KEYMANAGER_DEFAULT_ALGO_TYPE);
                        keyManagerFactory.init(keyStore, keyStoreFilepwd.toCharArray());
                        kmList = keyManagerFactory.getKeyManagers();
                    } else {
                        LOG.error("Unable to obtain keystore from file [" + keyStoreFile + "]");
                    }
                } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException |
                         FileNotFoundException | UnrecoverableKeyException e) {
                    LOG.error("Error loading keystore", e);
                } finally {
                    close(in, keyStoreFile);
                }
            }
        } catch (IOException exception) {
            LOG.error(exception.getMessage());
        }
        return kmList;
    }

    private TrustManager[] getTrustManagers() {
        TrustManager[] tmList = null;
        try {
            String truststoreFile = getConfiguration().getString(TRUSTSTORE_FILE_KEY,
                    System.getProperty(TRUSTSTORE_FILE_KEY, DEFATULT_TRUSTORE_FILE_LOCATION));
            String trustStoreFilepwd = getPassword(getConfiguration(), TRUSTSTORE_PASSWORD_KEY);

            if (StringUtils.isNotEmpty(truststoreFile) && StringUtils.isNotEmpty(trustStoreFilepwd)) {
                InputStream in = null;
                try {
                    in = getFileInputStream(truststoreFile);
                    if (in != null) {
                        KeyStore trustStore = KeyStore.getInstance(
                                getConfiguration().getString(TRUSTSTORE_TYPE, ATLAS_TRUSTSTORE_FILE_TYPE_DEFAULT));
                        trustStore.load(in, trustStoreFilepwd.toCharArray());
                        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(ATLAS_TLS_TRUSTMANAGER_DEFAULT_ALGO_TYPE);
                        trustManagerFactory.init(trustStore);
                        tmList = trustManagerFactory.getTrustManagers();
                    } else {
                        LOG.error("Unable to obtain truststore from file [" + truststoreFile + "]");
                    }
                } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException |
                         FileNotFoundException e) {
                    LOG.error("Error loading truststore", e);
                } finally {
                    close(in, truststoreFile);
                }
            }
        } catch (IOException exception) {
            LOG.error(exception.getMessage());
        }
        return tmList;
    }

    private InputStream getFileInputStream(String fileName) throws IOException {
        if (StringUtils.isNotEmpty(fileName)) {
            File f = new File(fileName);
            if (f.exists()) {
                return new FileInputStream(f);
            } else {
                return ClassLoader.getSystemResourceAsStream(fileName);
            }
        }
        return null;
    }

    private void close(InputStream str, String filename) {
        if (str != null) {
            try {
                str.close();
            } catch (IOException excp) {
                LOG.error("Error while closing file: [" + filename + "]", excp);
            }
        }
    }
}
