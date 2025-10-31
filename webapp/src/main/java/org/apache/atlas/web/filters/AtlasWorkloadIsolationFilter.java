package org.apache.atlas.web.filters;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Gauge;

import javax.annotation.PostConstruct;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class AtlasWorkloadIsolationFilter extends OncePerRequestFilter {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasWorkloadIsolationFilter.class);
    
    // Headers
    private static final String HEADER_CLIENT_ORIGIN = "X-Atlan-Client-Origin";
    private static final String HEADER_AGENT_ID = "x-atlan-agent-id";
    
    // Client origin values
    private static final String ORIGIN_PRODUCT_WEBAPP = "product_webapp";
    private static final String ORIGIN_PRODUCT_SDK = "product_sdk";
    private static final String ORIGIN_WORKFLOW = "workflow";
    private static final String ORIGIN_NUMAFLOW = "numaflow";
    private static final String ORIGIN_UNKNOWN = "unknown";
    
    // Throttled endpoints
    private static final Set<String> THROTTLED_PATHS = new HashSet<>(Arrays.asList(
        "/api/atlas/v2/search/indexsearch",
        "/api/atlas/v2/entity/bulk"
    ));

    private final Map<String, WorkloadLimiter> clientLimiters = new ConcurrentHashMap<>();
    
    // To prevent unlimited growth of clientLimiters map
    private static final int MAX_CLIENT_LIMITERS = 1000;
    
    // System resource capacity
    private SystemCapacity systemCapacity;

    @Autowired
    private Environment environment;

    @Autowired(required = false)
    private MeterRegistry meterRegistry;
    
    /**
     * System capacity holder - calculated at startup
     */
    private static class SystemCapacity {
        final int availableCpuCores;
        final long availableMemoryMB;
        final int baselineConcurrency;      // Based on CPU cores
        final int baselineRatePerSec;       // Based on upstream nginx limits
        
        SystemCapacity(int cpuCores, long memoryMB, int baselineConcurrency, int baselineRatePerSec) {
            this.availableCpuCores = cpuCores;
            this.availableMemoryMB = memoryMB;
            this.baselineConcurrency = baselineConcurrency;
            this.baselineRatePerSec = baselineRatePerSec;
        }
        
        @Override
        public String toString() {
            return String.format("CPU=%d cores, Memory=%d MB, Baseline: %d concurrent, %d req/sec",
                    availableCpuCores, availableMemoryMB, baselineConcurrency, baselineRatePerSec);
        }
    }

    static class WorkloadLimiter {
        final Bulkhead concurrencyLimit;    // Max concurrent requests
        final RateLimiter sustainedRate;     // Sustained throughput
        final String clientType;

        WorkloadLimiter(String clientType, int maxConcurrent, int ratePerSec) {
            this.clientType = clientType;
            this.concurrencyLimit = Bulkhead.of(clientType + "-concurrent",
                    BulkheadConfig.custom()
                            .maxConcurrentCalls(maxConcurrent)
                            .maxWaitDuration(Duration.ofSeconds(30))
                            .build()
            );
            this.sustainedRate = RateLimiter.of(clientType + "-rate",
                    RateLimiterConfig.custom()
                            .limitRefreshPeriod(Duration.ofSeconds(1))
                            .limitForPeriod(ratePerSec)
                            .timeoutDuration(Duration.ofSeconds(30))
                            .build());
        }

        void acquire() throws Throwable {
            // First: Check sustained rate
            boolean acquired = sustainedRate.acquirePermission();
            if (!acquired) {
                throw RequestNotPermitted.createRequestNotPermitted(sustainedRate);
            }

            // Second: Check concurrency (blocks if needed)
            Bulkhead.decorateCheckedRunnable(concurrencyLimit, () -> {}).run();
        }

        void release() {
            // Bulkhead auto-releases when decorated runnable completes
        }
    }

    @PostConstruct
    public void initLimiters() {
        // Step 1: Detect system capacity
        systemCapacity = detectSystemCapacity();
        LOG.info("Detected system capacity: {}", systemCapacity);
        
        // Step 2: Check if using hierarchical ratio system
        // Two-level ratio: webapp vs. rest, then divide rest by priority
        double restCapacityRatioConcurrent = getDoubleProperty("atlas.throttle.rest.concurrentRatio", -1.0);
        double restCapacityRatioRate = getDoubleProperty("atlas.throttle.rest.rateRatio", -1.0);
        
        boolean useHierarchicalRatios = (restCapacityRatioConcurrent > 0 || restCapacityRatioRate > 0);
        
        if (useHierarchicalRatios) {
            LOG.info("Using HIERARCHICAL ratio system: webapp vs. rest, then priority distribution");
            initLimitersHierarchical(restCapacityRatioConcurrent, restCapacityRatioRate);
        } else {
            LOG.info("Using DIRECT ratio system: each client gets % of total capacity");
            initLimitersDirect();
        }
        
        // Register metrics
        registerCapacityMetrics();
    }
    
    /**
     * Initialize limiters using DIRECT ratios (original method)
     * Each client gets a percentage of total system capacity
     */
    private void initLimitersDirect() {
        // Priority Order (highest to lowest):
        // 1. product_webapp: NO THROTTLING (handled separately in filter logic)
        // 2. workflow: TOP PRIORITY (highest ratios)
        // 3. product_sdk: SECOND PRIORITY
        // 4. numaflow: THIRD PRIORITY
        // 5. unknown: LOWEST PRIORITY (most restrictive)
        
        // workflow throttling - TOP PRIORITY (highest ratios)
        int workflowConcurrent = calculateLimit("workflow", "concurrent", 0.30, 20);
        int workflowRate = calculateLimit("workflow", "rate", 0.35, 80);
        clientLimiters.put(ORIGIN_WORKFLOW, new WorkloadLimiter(ORIGIN_WORKFLOW, workflowConcurrent, workflowRate));
        
        // product_sdk throttling - SECOND PRIORITY
        int sdkConcurrent = calculateLimit("product_sdk", "concurrent", 0.20, 15);
        int sdkRate = calculateLimit("product_sdk", "rate", 0.25, 60);
        clientLimiters.put(ORIGIN_PRODUCT_SDK, new WorkloadLimiter(ORIGIN_PRODUCT_SDK, sdkConcurrent, sdkRate));
        
        // Default numaflow - THIRD PRIORITY (if pipeline-specific config not found)
        int numaflowDefaultConcurrent = calculateLimit("numaflow.default", "concurrent", 0.15, 12);
        int numaflowDefaultRate = calculateLimit("numaflow.default", "rate", 0.20, 25);
        clientLimiters.put(ORIGIN_NUMAFLOW + ":default", new WorkloadLimiter(ORIGIN_NUMAFLOW + ":default", numaflowDefaultConcurrent, numaflowDefaultRate));
        
        // unknown/missing origin - LOWEST PRIORITY (most restrictive)
        int unknownConcurrent = calculateLimit("unknown", "concurrent", 0.05, 3);
        int unknownRate = calculateLimit("unknown", "rate", 0.05, 5);
        clientLimiters.put(ORIGIN_UNKNOWN, new WorkloadLimiter(ORIGIN_UNKNOWN, unknownConcurrent, unknownRate));
        
        LOG.info("Atlas workload isolation filter initialized (priority order):");
        LOG.info("  1. product_webapp: NO THROTTLING (unlimited)");
        LOG.info("  2. workflow: {} concurrent, {} req/sec (TOP PRIORITY)", workflowConcurrent, workflowRate);
        LOG.info("  3. product_sdk: {} concurrent, {} req/sec (2nd priority)", sdkConcurrent, sdkRate);
        LOG.info("  4. numaflow default: {} concurrent, {} req/sec (3rd priority)", numaflowDefaultConcurrent, numaflowDefaultRate);
        LOG.info("  5. unknown clients: {} concurrent, {} req/sec (LOWEST - most restrictive)", unknownConcurrent, unknownRate);
        LOG.info("  - Throttled paths: {}", THROTTLED_PATHS);
    }
    
    /**
     * Initialize limiters using HIERARCHICAL ratios
     * Level 1: webapp vs. rest (total capacity for non-webapp clients)
     * Level 2: Distribute rest capacity by priority weights
     */
    private void initLimitersHierarchical(double restCapacityRatioConcurrent, double restCapacityRatioRate) {
        // Level 1: Calculate total capacity for "rest" clients (non-webapp)
        // Default: 70% for rest, leaving 30% buffer for webapp
        if (restCapacityRatioConcurrent <= 0) {
            restCapacityRatioConcurrent = 0.70;  // 70% default
        }
        if (restCapacityRatioRate <= 0) {
            restCapacityRatioRate = 0.70;  // 70% default
        }
        
        int restBaselineConcurrent = (int) Math.ceil(systemCapacity.baselineConcurrency * restCapacityRatioConcurrent);
        int restBaselineRate = (int) Math.ceil(systemCapacity.baselineRatePerSec * restCapacityRatioRate);
        
        LOG.info("Hierarchical Level 1 - REST capacity: {} concurrent ({:.1f}%), {} req/sec ({:.1f}%)",
                restBaselineConcurrent, restCapacityRatioConcurrent * 100,
                restBaselineRate, restCapacityRatioRate * 100);
        
        // Level 2: Get priority weights (default to equal distribution if not configured)
        // Priority weights determine how the REST capacity is divided
        double workflowWeight = getDoubleProperty("atlas.throttle.priority.workflow.weight", 0.43);    // ~30% of 70%
        double sdkWeight = getDoubleProperty("atlas.throttle.priority.product_sdk.weight", 0.29);      // ~20% of 70%
        double numaflowWeight = getDoubleProperty("atlas.throttle.priority.numaflow.weight", 0.21);    // ~15% of 70%
        double unknownWeight = getDoubleProperty("atlas.throttle.priority.unknown.weight", 0.07);      // ~5% of 70%
        
        // Normalize weights to sum to 1.0
        double totalWeight = workflowWeight + sdkWeight + numaflowWeight + unknownWeight;
        workflowWeight /= totalWeight;
        sdkWeight /= totalWeight;
        numaflowWeight /= totalWeight;
        unknownWeight /= totalWeight;
        
        LOG.info("Hierarchical Level 2 - Priority weights (normalized): workflow={:.1f}%, sdk={:.1f}%, numaflow={:.1f}%, unknown={:.1f}%",
                workflowWeight * 100, sdkWeight * 100, numaflowWeight * 100, unknownWeight * 100);
        
        // Calculate limits for each client based on their weight of REST capacity
        int workflowConcurrent = (int) Math.ceil(restBaselineConcurrent * workflowWeight);
        int workflowRate = (int) Math.ceil(restBaselineRate * workflowWeight);
        clientLimiters.put(ORIGIN_WORKFLOW, new WorkloadLimiter(ORIGIN_WORKFLOW, workflowConcurrent, workflowRate));
        
        int sdkConcurrent = (int) Math.ceil(restBaselineConcurrent * sdkWeight);
        int sdkRate = (int) Math.ceil(restBaselineRate * sdkWeight);
        clientLimiters.put(ORIGIN_PRODUCT_SDK, new WorkloadLimiter(ORIGIN_PRODUCT_SDK, sdkConcurrent, sdkRate));
        
        int numaflowDefaultConcurrent = (int) Math.ceil(restBaselineConcurrent * numaflowWeight);
        int numaflowDefaultRate = (int) Math.ceil(restBaselineRate * numaflowWeight);
        clientLimiters.put(ORIGIN_NUMAFLOW + ":default", new WorkloadLimiter(ORIGIN_NUMAFLOW + ":default", numaflowDefaultConcurrent, numaflowDefaultRate));
        
        int unknownConcurrent = (int) Math.ceil(restBaselineConcurrent * unknownWeight);
        int unknownRate = (int) Math.ceil(restBaselineRate * unknownWeight);
        clientLimiters.put(ORIGIN_UNKNOWN, new WorkloadLimiter(ORIGIN_UNKNOWN, unknownConcurrent, unknownRate));
        
        LOG.info("Atlas workload isolation filter initialized (HIERARCHICAL priority order):");
        LOG.info("  Total system capacity: {} concurrent, {} req/sec", systemCapacity.baselineConcurrency, systemCapacity.baselineRatePerSec);
        LOG.info("  REST clients capacity: {} concurrent ({:.1f}%), {} req/sec ({:.1f}%)", 
                restBaselineConcurrent, restCapacityRatioConcurrent * 100,
                restBaselineRate, restCapacityRatioRate * 100);
        LOG.info("  Reserved for webapp: {} concurrent ({:.1f}%), {} req/sec ({:.1f}%)",
                systemCapacity.baselineConcurrency - restBaselineConcurrent, (1 - restCapacityRatioConcurrent) * 100,
                systemCapacity.baselineRatePerSec - restBaselineRate, (1 - restCapacityRatioRate) * 100);
        LOG.info("");
        LOG.info("  1. product_webapp: NO THROTTLING (unlimited)");
        LOG.info("  2. workflow: {} concurrent ({:.1f}% of rest), {} req/sec (TOP PRIORITY)", 
                workflowConcurrent, workflowWeight * 100, workflowRate);
        LOG.info("  3. product_sdk: {} concurrent ({:.1f}% of rest), {} req/sec (2nd priority)", 
                sdkConcurrent, sdkWeight * 100, sdkRate);
        LOG.info("  4. numaflow default: {} concurrent ({:.1f}% of rest), {} req/sec (3rd priority)", 
                numaflowDefaultConcurrent, numaflowWeight * 100, numaflowDefaultRate);
        LOG.info("  5. unknown clients: {} concurrent ({:.1f}% of rest), {} req/sec (LOWEST - most restrictive)", 
                unknownConcurrent, unknownWeight * 100, unknownRate);
        LOG.info("  - Throttled paths: {}", THROTTLED_PATHS);
    }
    
    /**
     * Detect system capacity (CPU, memory) and calculate baseline limits
     */
    private SystemCapacity detectSystemCapacity() {
        // Get available CPU cores
        int cpuCores = Runtime.getRuntime().availableProcessors();
        
        // Get available memory (heap + non-heap)
        long maxMemoryBytes = Runtime.getRuntime().maxMemory();
        long maxMemoryMB = maxMemoryBytes / (1024 * 1024);
        
        // Get upstream nginx rate limits (configured)
        // Nginx: 1000/min for indexsearch, 400/min for bulk
        // Average: ~700/min = ~12 req/sec baseline
        int nginxRatePerMin = getIntProperty("atlas.throttle.nginx.upstream.ratePerMin", 700);
        int baselineRatePerSec = Math.max(nginxRatePerMin / 60, 10);
        
        // Calculate baseline concurrency from CPU cores
        // Rule of thumb: 10-20 concurrent requests per core for I/O-bound workloads
        int concurrencyPerCore = getIntProperty("atlas.throttle.concurrency.perCore", 15);
        int baselineConcurrency = Math.max(cpuCores * concurrencyPerCore, 20);
        
        LOG.info("System resources detected: {} CPU cores, {} MB heap", cpuCores, maxMemoryMB);
        LOG.info("Baseline capacity: {} concurrent requests, {} req/sec (from nginx upstream: {} req/min)", 
                baselineConcurrency, baselineRatePerSec, nginxRatePerMin);
        
        return new SystemCapacity(cpuCores, maxMemoryMB, baselineConcurrency, baselineRatePerSec);
    }
    
    /**
     * Calculate limit for a client type
     * Supports both absolute config (backward compatible) and ratio-based config
     * 
     * @param clientType e.g., "product_sdk", "workflow"
     * @param limitType "concurrent" or "rate"
     * @param defaultRatio default ratio if no config found (e.g., 0.20 = 20%)
     * @param absoluteFallback absolute number fallback
     * @return calculated limit
     */
    private int calculateLimit(String clientType, String limitType, double defaultRatio, int absoluteFallback) {
        String configKey;
        int baseline;
        
        if ("concurrent".equals(limitType)) {
            // Check for absolute value first (backward compatible)
            configKey = "atlas.throttle." + clientType + ".concurrent";
            int absoluteValue = getIntProperty(configKey, -1);
            if (absoluteValue > 0) {
                LOG.debug("Using absolute concurrent limit for {}: {}", clientType, absoluteValue);
                return absoluteValue;
            }
            
            // Check for ratio-based config
            configKey = "atlas.throttle." + clientType + ".concurrentRatio";
            double ratio = getDoubleProperty(configKey, defaultRatio);
            baseline = systemCapacity.baselineConcurrency;
            
            int calculated = (int) Math.ceil(baseline * ratio);
            LOG.debug("Calculated concurrent limit for {} using ratio {}: {} (baseline={})", 
                    clientType, ratio, calculated, baseline);
            return Math.max(calculated, 1); // At least 1
            
        } else if ("rate".equals(limitType)) {
            // Check for absolute value first (backward compatible)
            configKey = "atlas.throttle." + clientType + ".ratePerSec";
            int absoluteValue = getIntProperty(configKey, -1);
            if (absoluteValue > 0) {
                LOG.debug("Using absolute rate limit for {}: {}", clientType, absoluteValue);
                return absoluteValue;
            }
            
            // Check for ratio-based config
            configKey = "atlas.throttle." + clientType + ".rateRatio";
            double ratio = getDoubleProperty(configKey, defaultRatio);
            baseline = systemCapacity.baselineRatePerSec;
            
            int calculated = (int) Math.ceil(baseline * ratio);
            LOG.debug("Calculated rate limit for {} using ratio {}: {} (baseline={})", 
                    clientType, ratio, calculated, baseline);
            return Math.max(calculated, 1); // At least 1
            
        } else {
            LOG.warn("Unknown limit type: {}, using fallback", limitType);
            return absoluteFallback;
        }
    }
    
    private int getIntProperty(String key, int defaultValue) {
        return environment.getProperty(key, Integer.class, defaultValue);
    }
    
    private double getDoubleProperty(String key, double defaultValue) {
        return environment.getProperty(key, Double.class, defaultValue);
    }
    
    /**
     * Register metrics for system capacity and utilization
     */
    private void registerCapacityMetrics() {
        if (meterRegistry == null) {
            return;
        }
        
        // Register capacity metrics
        Gauge.builder("atlas.throttle.capacity.cpu_cores", systemCapacity, sc -> sc.availableCpuCores)
                .description("Available CPU cores")
                .register(meterRegistry);
        
        Gauge.builder("atlas.throttle.capacity.memory_mb", systemCapacity, sc -> sc.availableMemoryMB)
                .description("Available memory in MB")
                .register(meterRegistry);
        
        Gauge.builder("atlas.throttle.capacity.baseline_concurrency", systemCapacity, sc -> sc.baselineConcurrency)
                .description("Baseline concurrent request capacity")
                .register(meterRegistry);
        
        Gauge.builder("atlas.throttle.capacity.baseline_rate", systemCapacity, sc -> sc.baselineRatePerSec)
                .description("Baseline rate limit (req/sec)")
                .register(meterRegistry);
        
        Gauge.builder("atlas.throttle.limiters.count", clientLimiters, Map::size)
                .description("Number of active client limiters")
                .register(meterRegistry);
    }
    
    /**
     * Get or create limiter for a specific numaflow pipeline
     * Supports both absolute and ratio-based configuration
     */
    private WorkloadLimiter getOrCreateNumaflowLimiter(String pipelineId) {
        String clientKey = ORIGIN_NUMAFLOW + ":" + pipelineId;
        
        return clientLimiters.computeIfAbsent(clientKey, key -> {
            // Check for pipeline-specific config
            String sanitizedId = sanitizeConfigKey(pipelineId);
            String configKeyPrefix = "atlas.throttle.numaflow.pipeline." + sanitizedId;
            
            // Try absolute values first (backward compatible)
            int concurrent = getIntProperty(configKeyPrefix + ".concurrent", -1);
            int rate = getIntProperty(configKeyPrefix + ".ratePerSec", -1);
            
            // If no absolute config, try ratio-based
            if (concurrent == -1) {
                double concurrentRatio = getDoubleProperty(configKeyPrefix + ".concurrentRatio", -1.0);
                if (concurrentRatio > 0) {
                    concurrent = (int) Math.ceil(systemCapacity.baselineConcurrency * concurrentRatio);
                    LOG.debug("Calculated concurrent for pipeline {} using ratio {}: {}", pipelineId, concurrentRatio, concurrent);
                }
            }
            
            if (rate == -1) {
                double rateRatio = getDoubleProperty(configKeyPrefix + ".rateRatio", -1.0);
                if (rateRatio > 0) {
                    rate = (int) Math.ceil(systemCapacity.baselineRatePerSec * rateRatio);
                    LOG.debug("Calculated rate for pipeline {} using ratio {}: {}", pipelineId, rateRatio, rate);
                }
            }
            
            // If no specific config found, use numaflow default
            if (concurrent == -1 || rate == -1) {
                WorkloadLimiter defaultLimiter = clientLimiters.get(ORIGIN_NUMAFLOW + ":default");
                concurrent = concurrent == -1 ? defaultLimiter.concurrencyLimit.getBulkheadConfig().getMaxConcurrentCalls() : concurrent;
                rate = rate == -1 ? defaultLimiter.sustainedRate.getRateLimiterConfig().getLimitForPeriod() : rate;
            }
            
            // Check map size to prevent memory issues
            if (clientLimiters.size() >= MAX_CLIENT_LIMITERS) {
                LOG.warn("Max client limiters ({}) reached. Using numaflow default for: {}", MAX_CLIENT_LIMITERS, pipelineId);
                return clientLimiters.get(ORIGIN_NUMAFLOW + ":default");
            }
            
            LOG.info("Created limiter for numaflow pipeline '{}': {} concurrent, {} req/sec", pipelineId, concurrent, rate);
            return new WorkloadLimiter(clientKey, concurrent, rate);
        });
    }
    
    /**
     * Sanitize pipeline ID for use as config key
     */
    private String sanitizeConfigKey(String pipelineId) {
        // Replace non-alphanumeric with underscore, convert to lowercase
        return pipelineId.replaceAll("[^a-zA-Z0-9-]", "_").toLowerCase();
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain)
            throws ServletException, IOException {

        // Check if this path should be throttled
        if (!shouldThrottle(request)) {
            filterChain.doFilter(request, response);
            return;
        }

        // Get client identification from headers
        String clientOrigin = request.getHeader(HEADER_CLIENT_ORIGIN);
        String agentId = request.getHeader(HEADER_AGENT_ID);
        
        // Log headers for debugging (remove in production or use DEBUG level)
        LOG.debug("Request from origin: {}, agent: {}, path: {}", clientOrigin, agentId, request.getRequestURI());

        // product_webapp is NOT throttled
        if (ORIGIN_PRODUCT_WEBAPP.equalsIgnoreCase(clientOrigin)) {
            LOG.debug("Skipping throttle for product_webapp");
            filterChain.doFilter(request, response);
            return;
        }

        // Identify client and get appropriate limiter
        ClientIdentity clientIdentity = identifyClient(clientOrigin, agentId);
        WorkloadLimiter limiter = getLimiterForClient(clientIdentity);

        long startTime = System.currentTimeMillis();

        try {
            // Acquire permits (blocks with backpressure)
            limiter.acquire();

            try {
                filterChain.doFilter(request, response);
            } finally {
                limiter.release();
            }

        } catch (BulkheadFullException e) {
            LOG.warn("Bulkhead full for client: {}, URI: {}", clientIdentity, request.getRequestURI());
            handleBackpressure(response, clientIdentity.toString(), "concurrency", limiter);

        } catch (RequestNotPermitted e) {
            LOG.warn("Rate limit exceeded for client: {}, URI: {}", clientIdentity, request.getRequestURI());
            handleBackpressure(response, clientIdentity.toString(), "rate", limiter);

        } catch (Exception e) {
            LOG.error("Throttling error for client: {}", clientIdentity, e);
            throw new ServletException("Throttling error", e);

        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            recordMetrics(clientIdentity.toString(), request, System.currentTimeMillis() - startTime);
        }
    }
    
    /**
     * Check if the request path should be throttled
     */
    private boolean shouldThrottle(HttpServletRequest request) {
        String requestURI = request.getRequestURI();
        
        // Check if path matches any throttled paths
        for (String throttledPath : THROTTLED_PATHS) {
            if (requestURI.contains(throttledPath)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Client identity - contains origin and optional pipeline ID
     */
    private static class ClientIdentity {
        final String origin;
        final String pipelineId;
        
        ClientIdentity(String origin, String pipelineId) {
            this.origin = origin;
            this.pipelineId = pipelineId;
        }
        
        @Override
        public String toString() {
            if (pipelineId != null) {
                return origin + ":" + pipelineId;
            }
            return origin;
        }
    }
    
    /**
     * Identify client based on headers
     */
    private ClientIdentity identifyClient(String clientOrigin, String agentId) {
        // No origin header = unknown/untrusted client = maximum throttling
        if (clientOrigin == null || clientOrigin.trim().isEmpty()) {
            LOG.debug("No X-Atlan-Client-Origin header found, treating as unknown");
            return new ClientIdentity(ORIGIN_UNKNOWN, null);
        }
        
        String origin = clientOrigin.trim().toLowerCase();
        
        // numaflow: use agent ID to identify specific pipeline
        if (ORIGIN_NUMAFLOW.equalsIgnoreCase(origin)) {
            if (agentId != null && !agentId.trim().isEmpty()) {
                return new ClientIdentity(ORIGIN_NUMAFLOW, agentId.trim());
            } else {
                LOG.warn("numaflow client without x-atlan-agent-id header, using default limits");
                return new ClientIdentity(ORIGIN_NUMAFLOW, "default");
            }
        }
        
        // workflow
        if (ORIGIN_WORKFLOW.equalsIgnoreCase(origin)) {
            return new ClientIdentity(ORIGIN_WORKFLOW, null);
        }
        
        // product_sdk
        if (ORIGIN_PRODUCT_SDK.equalsIgnoreCase(origin)) {
            return new ClientIdentity(ORIGIN_PRODUCT_SDK, null);
        }
        
        // Unknown origin value = treat as unknown
        LOG.warn("Unknown client origin: {}, treating as unknown", clientOrigin);
        return new ClientIdentity(ORIGIN_UNKNOWN, null);
    }
    
    /**
     * Get limiter for identified client
     */
    private WorkloadLimiter getLimiterForClient(ClientIdentity clientIdentity) {
        if (ORIGIN_NUMAFLOW.equals(clientIdentity.origin) && clientIdentity.pipelineId != null) {
            return getOrCreateNumaflowLimiter(clientIdentity.pipelineId);
        }
        
        WorkloadLimiter limiter = clientLimiters.get(clientIdentity.origin);
        if (limiter == null) {
            LOG.warn("No limiter found for origin: {}, using unknown limits", clientIdentity.origin);
            limiter = clientLimiters.get(ORIGIN_UNKNOWN);
        }
        
        return limiter;
    }

    private void handleBackpressure(HttpServletResponse response,
                                    String clientType,
                                    String limitType,
                                    WorkloadLimiter limiter) throws IOException {
        response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        response.setHeader("Retry-After", "30");
        response.setHeader("X-Throttle-Reason", limitType);
        response.setHeader("X-Client-Type", clientType);
        response.setContentType("application/json");

        // Get metrics from bulkhead
        Bulkhead.Metrics metrics = limiter.concurrencyLimit.getMetrics();
        int availableConcurrent = metrics.getAvailableConcurrentCalls();
        int maxConcurrent = limiter.concurrencyLimit.getBulkheadConfig().getMaxConcurrentCalls();

        String errorMessage = String.format(
                "{\"error\": \"Atlas capacity limit reached\", " +
                        "\"clientType\": \"%s\", " +
                        "\"limitType\": \"%s\", " +
                        "\"availableConcurrentCalls\": %d, " +
                        "\"maxConcurrentCalls\": %d, " +
                        "\"message\": \"Please implement exponential backoff and retry after 30 seconds.\"}",
                clientType,
                limitType,
                availableConcurrent,
                maxConcurrent
        );

        response.getWriter().write(errorMessage);

        // Record rejection metric
        if (meterRegistry != null) {
            Counter.builder("atlas.throttle.rejected")
                    .tag("client_type", clientType)
                    .tag("limit_type", limitType)
                    .register(meterRegistry)
                    .increment();
        }
    }


    private void recordMetrics(String clientType, HttpServletRequest request, long durationMs) {
        if (meterRegistry == null) {
            return;
        }

        // Record request count by client type
        Counter.builder("atlas.requests.by_client")
                .tag("client_type", clientType)
                .tag("endpoint", request.getRequestURI())
                .tag("method", request.getMethod())
                .register(meterRegistry)
                .increment();

        // Record request duration
        Timer.builder("atlas.request.duration")
                .tag("client_type", clientType)
                .register(meterRegistry)
                .record(durationMs, TimeUnit.MILLISECONDS);
    }
}