package org.apache.atlas.repository.graph;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Component
public class TypeCacheRefresher {
    private static final Logger LOG = LoggerFactory.getLogger(TypeCacheRefresher.class);
    private final IAtlasGraphProvider provider;
    private KubernetesClient k8sClient;
    private CloseableHttpClient httpClient;

    @Value("${atlas.refresh.timeout:30}")
    private int refreshTimeoutSeconds;

    @Value("${atlas.refresh.retries:2}")
    private long refreshRetries;


    @Inject
    public TypeCacheRefresher(final IAtlasGraphProvider provider) {
        this.provider = provider;
    }

    @PostConstruct
    public void init() throws AtlasException {
        // Only initialize K8s client in non-local environments
        if (isKubernetesEnvironment()) {
            try {
                this.k8sClient = new KubernetesClientBuilder().build();
                LOG.info("Kubernetes client initialized successfully");
            } catch (Exception e) {
                LOG.warn("Failed to initialize Kubernetes client: {}. Pod discovery disabled.", e.getMessage());
                this.k8sClient = null;
            }
        } else {
            LOG.info("Running in local environment. Pod discovery disabled.");
            this.k8sClient = null;
        }
        // Initialize Apache HttpClient with connection pooling and timeouts
        initHttpClient();
    }

    @PreDestroy
    public void cleanup() {
        if (k8sClient != null) {
            k8sClient.close();
        }
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                LOG.warn("Error closing HttpClient", e);
            }
        }
    }


    /**
     * Notify all other Atlas pods of typedef update
     */
    public void refreshAllHostCache() {
        final String traceId = RequestContext.get().getTraceId();

        // Get current pod's IP to exclude self
        String currentPodIp = getCurrentPodIp();

        // Discover other Atlas pod IPs
        List<String> otherPodIps = getOtherAtlasPodIps(currentPodIp);

        if (otherPodIps.isEmpty()) {
            LOG.info("No other Atlas pods found to notify. Current env");
            return;
        }

        LOG.info("Notifying {} other Atlas pods of typedef update", otherPodIps.size());

        // Parallel refresh of all other pods
        List<CompletableFuture<RefreshResult>> futures = otherPodIps.stream()
                .map(podIp -> CompletableFuture.supplyAsync(() ->
                                refreshPodWithRetry(podIp, traceId),
                        getAsyncExecutor()
                ))
                .collect(Collectors.toList());

        try {
            // Wait for all refreshes to complete (with timeout)
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(refreshTimeoutSeconds + 5, TimeUnit.SECONDS);

            // Check results
            List<RefreshResult> results = futures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList());

            long successful = results.stream().filter(RefreshResult::isSuccess).count();
            LOG.info("TypeDef refresh completed: {}/{} pods succeeded", successful, results.size());

            if (successful < results.size()) {
                List<String> failedPods = results.stream()
                        .filter(r -> !r.isSuccess())
                        .map(RefreshResult::getPodIp)
                        .collect(Collectors.toList());
                LOG.warn("Failed to refresh pods: {}", failedPods);
            }

        } catch (TimeoutException e) {
            LOG.error("Timeout waiting for pod refreshes to complete", e);
        } catch (Exception e) {
            LOG.error("Error during pod refresh notification", e);
        }
    }

    /**
     * Check if running in Kubernetes environment
     */
    private boolean isKubernetesEnvironment() {
        String kubernetesServiceHost = System.getenv("KUBERNETES_SERVICE_HOST");
        if (kubernetesServiceHost == null || kubernetesServiceHost.isEmpty()) {
            return false;
        }
        return true;
    }

    private void initHttpClient() {
        try {
            // Configure timeouts
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(5000) // 5 seconds connect timeout
                    .setSocketTimeout(refreshTimeoutSeconds * 1000) // response timeout
                    .setConnectionRequestTimeout(5000) // timeout waiting for connection from pool
                    .build();

            // Configure connection pooling
            PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
            connectionManager.setMaxTotal(50); // max total connections
            connectionManager.setDefaultMaxPerRoute(10); // max connections per route

            // Build HttpClient
            this.httpClient = HttpClientBuilder.create()
                    .setConnectionManager(connectionManager)
                    .setDefaultRequestConfig(requestConfig)
                    .disableAutomaticRetries() // we handle retries manually
                    .build();

            LOG.info("HttpClient initialized with timeout: {}s", refreshTimeoutSeconds);
        } catch (Exception e) {
            LOG.error("Failed to initialize HttpClient", e);
            throw new RuntimeException("Failed to initialize HttpClient", e);
        }
    }

    /**
     * Get list of other Atlas pod IPs (excluding current pod)
     */
    private List<String> getOtherAtlasPodIps(String currentPodIp) {
        // If not in Kubernetes, return empty list
        if (k8sClient == null) {
            LOG.debug("Kubernetes client not available, returning empty pod list");
            return Collections.emptyList();
        }

        try {
            List<String> podIps = k8sClient.pods()
                    .inNamespace("atlas")
                    .withLabel("app", "atlas")
                    .list()
                    .getItems()
                    .stream()
                    .filter(pod -> {
                        // Only include Running pods
                        String phase = pod.getStatus().getPhase();
                        return "Running".equals(phase);
                    })
                    .map(pod -> pod.getStatus().getPodIP())
                    .filter(ip -> ip != null && !ip.equals(currentPodIp))
                    .collect(Collectors.toList());

            LOG.debug("Discovered {} other Atlas pods: {}", podIps.size(), podIps);
            return podIps;

        } catch (KubernetesClientException e) {
            LOG.error("Error querying Kubernetes API for Atlas pods: {}", e.getMessage());
            return Collections.emptyList();
        } catch (Exception e) {
            LOG.error("Unexpected error discovering Atlas pods", e);
            return Collections.emptyList();
        }
    }

    /**
     * Get current pod's IP address
     */
    private String getCurrentPodIp() {
        // Method 1: Try POD_IP environment variable (set by Kubernetes)
        String podIp = System.getenv("POD_IP");
        if (podIp != null && !podIp.isEmpty()) {
            return podIp;
        }

        // Method 2: Try to get from local network interface
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            return localHost.getHostAddress();
        } catch (UnknownHostException e) {
            LOG.warn("Could not determine current pod IP", e);
            return "unknown";
        }
    }

    /**
     * Refresh typedef cache on a specific pod with retry logic
     */
    private RefreshResult refreshPodWithRetry(String podIp, String traceId) {
        String lastError = null;

        for (int attempt = 1; attempt <= refreshRetries; attempt++) {
            RefreshResult result = refreshPod(podIp, traceId, attempt);

            if (result.isSuccess()) {
                return result;
            }

            lastError = result.getError();

            // Retry with exponential backoff
            if (attempt < refreshRetries) {
                try {
                    int backoffMs = 1000 * attempt; // 1s, 2s, 3s, etc.
                    LOG.warn("Retry {}/{} for pod {} after {}ms", attempt, refreshRetries, podIp, backoffMs);
                    Thread.sleep(backoffMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.error("Interrupted during retry backoff", e);
                    break;
                }
            }
        }

        LOG.error("Failed to refresh pod {} after {} attempts. Last error: {}",
                podIp, refreshRetries, lastError);
        return new RefreshResult(podIp, false, 0, refreshRetries, lastError);
    }

    /**
     * Refresh typedef cache on a specific pod using Apache HttpClient
     */
    private RefreshResult refreshPod(String podIp, String traceId, int attempt) {
        String url = String.format("http://%s:21000/api/atlas/admin/types/refresh?traceId=%s",
                podIp, traceId);

        HttpPost httpPost = new HttpPost(url);
        long startTime = System.currentTimeMillis();

        try {
            LOG.debug("Sending refresh request to pod {} (attempt {}): {}", podIp, attempt, url);

            CloseableHttpResponse response = httpClient.execute(httpPost);
            long duration = System.currentTimeMillis() - startTime;

            try {
                int statusCode = response.getStatusLine().getStatusCode();
                boolean success = statusCode == 200;

                if (success) {
                    LOG.info("Successfully refreshed pod {} in {}ms (attempt {})", podIp, duration, attempt);
                } else {
                    LOG.warn("Pod {} returned non-200 status: {} (attempt {})", podIp, statusCode, attempt);
                }

                return new RefreshResult(podIp, success, duration, attempt,
                        success ? null : "HTTP " + statusCode);

            } finally {
                response.close();
            }

        } catch (SocketTimeoutException e) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.error("Timeout refreshing pod {} after {}ms (attempt {}): {}",
                    podIp, duration, attempt, e.getMessage());
            return new RefreshResult(podIp, false, duration, attempt,
                    "Timeout after " + refreshTimeoutSeconds + "s");

        } catch (ConnectTimeoutException e) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.error("Connection timeout to pod {} after {}ms (attempt {}): {}",
                    podIp, duration, attempt, e.getMessage());
            return new RefreshResult(podIp, false, duration, attempt,
                    "Connection timeout: " + e.getMessage());

        } catch (IOException e) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.error("IO error refreshing pod {} after {}ms (attempt {}): {}",
                    podIp, duration, attempt, e.getMessage());
            return new RefreshResult(podIp, false, duration, attempt,
                    "IO error: " + e.getMessage());

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.error("Unexpected error refreshing pod {} after {}ms (attempt {}): {}",
                    podIp, duration, attempt, e.getMessage(), e);
            return new RefreshResult(podIp, false, duration, attempt,
                    "Unexpected error: " + e.getMessage());

        } finally {
            httpPost.reset();
        }
    }

    private Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(25);
        executor.setThreadNamePrefix("typedef-refresh-");
        executor.initialize();
        return executor;
    }

    /**
     * Result of a pod refresh operation
     */
    @Data
    @AllArgsConstructor
    public static class RefreshResult {
        private String podIp;
        private boolean success;
        private long durationMs;
        private long refreshRetries;
        private String error;
    }

}