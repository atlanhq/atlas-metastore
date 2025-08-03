package org.apache.atlas.web.service;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.lang.management.ManagementFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * CIRCUIT BREAKER: Monitors system resources and rejects requests when CPU or heap usage is too high
 * 
 * This service prevents system overload by:
 * 1. Monitoring CPU usage through OperatingSystemMXBean (direct JVM access)
 * 2. Monitoring heap usage through MemoryMXBean (direct JVM access)
 * 3. Maintaining sliding window averages for stability
 * 4. Rejecting requests with 429 status when thresholds exceeded
 * 5. Providing retry-after headers based on current load
 * 6. Auto-closing circuit when system recovers (background monitoring)
 * 
 * No external metrics dependencies required - uses native JVM management beans.
 * 
 * Configuration:
 * - atlas.circuitbreaker.enable: Enable/disable circuit breaker (default: true)
 * - atlas.circuitbreaker.cpu.threshold: CPU threshold % (default: 85)
 * - atlas.circuitbreaker.heap.threshold: Heap threshold % (default: 80) 
 * - atlas.circuitbreaker.check.interval: Check interval ms (default: 1000)
 * - atlas.circuitbreaker.window.size: Sliding window size (default: 5)
 */
@Service
public class AtlasCircuitBreakerService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasCircuitBreakerService.class);
    
    // Configuration keys
    private static final String ENABLE_PROPERTY = "atlas.circuitbreaker.enable";
    private static final String CPU_THRESHOLD_PROPERTY = "atlas.circuitbreaker.cpu.threshold";
    private static final String HEAP_THRESHOLD_PROPERTY = "atlas.circuitbreaker.heap.threshold";
    private static final String CHECK_INTERVAL_PROPERTY = "atlas.circuitbreaker.check.interval";
    private static final String WINDOW_SIZE_PROPERTY = "atlas.circuitbreaker.window.size";
    
    // Default values
    private static final boolean DEFAULT_ENABLED = true;
    private static final double DEFAULT_CPU_THRESHOLD = 85.0;
    private static final double DEFAULT_HEAP_THRESHOLD = 80.0;
    private static final long DEFAULT_CHECK_INTERVAL = 1000; // 1 second
    private static final int DEFAULT_WINDOW_SIZE = 5;
    
    // Configuration
    private boolean enabled;
    private double cpuThreshold;
    private double heapThreshold;
    private long checkInterval;
    private int windowSize;
    
    // JVM monitoring beans
    private final OperatingSystemMXBean osBean;
    private final MemoryMXBean memoryBean;
    private final com.sun.management.OperatingSystemMXBean sunOsBean;
    
    // Circuit breaker state
    private volatile boolean circuitOpen = false;
    private volatile double currentCpuUsage = 0.0;
    private volatile double currentHeapUsage = 0.0;
    private volatile long lastCheckTime = 0;
    private final AtomicLong rejectedRequestCount = new AtomicLong(0);
    private final AtomicLong totalRequestCount = new AtomicLong(0);
    private final AtomicReference<CircuitBreakerState> state = new AtomicReference<>(CircuitBreakerState.CLOSED);
    
    // Sliding window for stability
    private final double[] cpuWindow;
    private final double[] heapWindow;
    private int windowIndex = 0;
    private boolean windowFull = false;
    
    // Background scheduler for metric updates (ensures circuit can close even with no requests)
    private ScheduledExecutorService backgroundScheduler;
    
    public enum CircuitBreakerState {
        CLOSED(0),     // Normal operation
        OPEN(1),       // Rejecting requests
        HALF_OPEN(2);  // Testing if system recovered
        
        private final int value;
        CircuitBreakerState(int value) { this.value = value; }
        public int getValue() { return value; }
    }
    
    public AtlasCircuitBreakerService() {
        this.osBean = ManagementFactory.getOperatingSystemMXBean();
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        
        // Try to get Sun-specific bean for CPU monitoring
        com.sun.management.OperatingSystemMXBean sunBean = null;
        try {
            if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
                sunBean = (com.sun.management.OperatingSystemMXBean) osBean;
            }
        } catch (Exception e) {
            LOG.warn("Sun OperatingSystemMXBean not available - CPU monitoring will be limited", e);
        }
        this.sunOsBean = sunBean;
        
        // Initialize sliding windows
        this.windowSize = DEFAULT_WINDOW_SIZE;
        this.cpuWindow = new double[windowSize];
        this.heapWindow = new double[windowSize];
    }
    
    @PostConstruct
    public void initialize() {
        try {
            // Load configuration
            loadConfiguration();
            
            if (!enabled) {
                LOG.info("Circuit breaker is disabled");
                return;
            }
            
            // Start background scheduler for metric updates (ensures auto-close even with no requests)
            startBackgroundScheduler(); // Re-enabled with memory leak fixes
            
            LOG.info("Circuit breaker initialized - CPU threshold: {}%, Heap threshold: {}%, Check interval: {}ms",
                    cpuThreshold, heapThreshold, checkInterval);
                    
        } catch (Exception e) {
            LOG.error("Failed to initialize circuit breaker - disabling", e);
            enabled = false;
        }
    }
    
    private void loadConfiguration() throws AtlasException {
        enabled = ApplicationProperties.get().getBoolean(ENABLE_PROPERTY, DEFAULT_ENABLED);
        cpuThreshold = ApplicationProperties.get().getDouble(CPU_THRESHOLD_PROPERTY, DEFAULT_CPU_THRESHOLD);
        heapThreshold = ApplicationProperties.get().getDouble(HEAP_THRESHOLD_PROPERTY, DEFAULT_HEAP_THRESHOLD);
        checkInterval = ApplicationProperties.get().getLong(CHECK_INTERVAL_PROPERTY, DEFAULT_CHECK_INTERVAL);
        windowSize = ApplicationProperties.get().getInt(WINDOW_SIZE_PROPERTY, DEFAULT_WINDOW_SIZE);
        
        // Validate configuration
        if (cpuThreshold <= 0 || cpuThreshold > 100) {
            throw new AtlasException("Invalid CPU threshold: " + cpuThreshold + " (must be 0-100)");
        }
        if (heapThreshold <= 0 || heapThreshold > 100) {
            throw new AtlasException("Invalid heap threshold: " + heapThreshold + " (must be 0-100)");
        }
        if (checkInterval <= 0) {
            throw new AtlasException("Invalid check interval: " + checkInterval + " (must be > 0)");
        }
    }
    
    private void startBackgroundScheduler() {
        // MEMORY LEAK FIX: Re-enable with memory management and reduced frequency
        backgroundScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "atlas-circuit-breaker-monitor");
            t.setDaemon(true);
            return t;
        });
        
        // Use longer interval to reduce memory churn (30 seconds minimum)
        long memoryOptimizedInterval = Math.max(30000, checkInterval * 6);
        
        backgroundScheduler.scheduleAtFixedRate(() -> {
            try {
                updateMetrics();
                
                // MEMORY LEAK FIX: Periodic cleanup to prevent accumulation
                if (System.currentTimeMillis() % 300000 < memoryOptimizedInterval) { // Every 5 minutes
                    // Clear any accumulated references
                    System.gc();
                }
            } catch (Exception e) {
                LOG.warn("Error in background circuit breaker metric update", e);
            }
        }, memoryOptimizedInterval, memoryOptimizedInterval, TimeUnit.MILLISECONDS);
        
        LOG.info("Circuit breaker background scheduler started with {}ms interval (memory optimized)", memoryOptimizedInterval);
    }
    
    @PreDestroy
    public void cleanup() {
        // MEMORY LEAK FIX: No scheduler to clean up since it's disabled
        if (backgroundScheduler != null && !backgroundScheduler.isShutdown()) {
            LOG.info("Shutting down circuit breaker background scheduler");
            backgroundScheduler.shutdown();
            try {
                if (!backgroundScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    backgroundScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                backgroundScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        } else {
            LOG.debug("Circuit breaker background scheduler was disabled - no cleanup needed");
        }
    }
    
    /**
     * Check if the circuit breaker should allow a request
     * This is the main entry point called by filters
     */
    public CircuitBreakerResult checkRequest() {
        if (!enabled) {
            return CircuitBreakerResult.allowed();
        }
        
        totalRequestCount.incrementAndGet();
        
        // Update metrics if enough time has passed
        updateMetricsIfNeeded();
        
        // Check if circuit should be open
        if (shouldOpenCircuit()) {
            rejectedRequestCount.incrementAndGet();
            return CircuitBreakerResult.rejected(calculateRetryAfterSeconds());
        }
        
        return CircuitBreakerResult.allowed();
    }
    
    private void updateMetricsIfNeeded() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastCheckTime >= checkInterval) {
            updateMetrics();
            lastCheckTime = currentTime;
        }
    }
    
    private void updateMetrics() {
        try {
            // Update CPU usage
            double cpuUsage = getCpuUsage();
            if (cpuUsage >= 0) {
                addToWindow(cpuWindow, cpuUsage);
                currentCpuUsage = getWindowAverage(cpuWindow);
            }
            
            // Update heap usage
            double heapUsage = getHeapUsage();
            addToWindow(heapWindow, heapUsage);
            currentHeapUsage = getWindowAverage(heapWindow);
            
            // Update circuit state
            updateCircuitState();
            
        } catch (Exception e) {
            LOG.warn("Error updating circuit breaker metrics", e);
        }
    }
    
    private double getCpuUsage() {
        try {
            if (sunOsBean != null) {
                // Use Sun-specific bean for accurate CPU usage
                double usage = sunOsBean.getProcessCpuLoad() * 100.0;
                return usage >= 0 ? usage : -1;
            } else {
                // Fallback: estimate from system load
                double load = osBean.getSystemLoadAverage();
                int processors = osBean.getAvailableProcessors();
                if (load > 0 && processors > 0) {
                    return Math.min((load / processors) * 100.0, 100.0);
                }
            }
        } catch (Exception e) {
            LOG.debug("Error getting CPU usage", e);
        }
        return -1;
    }
    
    private double getHeapUsage() {
        try {
            MemoryUsage heapMemoryUsage = memoryBean.getHeapMemoryUsage();
            long used = heapMemoryUsage.getUsed();
            long max = heapMemoryUsage.getMax();
            
            if (max > 0) {
                return (double) used / max * 100.0;
            }
        } catch (Exception e) {
            LOG.debug("Error getting heap usage", e);
        }
        return 0.0;
    }
    
    private void addToWindow(double[] window, double value) {
        window[windowIndex] = value;
        windowIndex = (windowIndex + 1) % windowSize;
        if (windowIndex == 0) {
            windowFull = true;
        }
    }
    
    private double getWindowAverage(double[] window) {
        int count = windowFull ? windowSize : windowIndex;
        if (count == 0) return 0.0;
        
        double sum = 0.0;
        for (int i = 0; i < count; i++) {
            sum += window[i];
        }
        return sum / count;
    }
    
    private boolean shouldOpenCircuit() {
        // Check heap threshold (always available)
        if (currentHeapUsage > heapThreshold) {
            LOG.debug("Circuit breaker triggered by heap usage: {}% > {}%", currentHeapUsage, heapThreshold);
            return true;
        }
        
        // Check CPU threshold (if available)
        if (currentCpuUsage > 0 && currentCpuUsage > cpuThreshold) {
            LOG.debug("Circuit breaker triggered by CPU usage: {}% > {}%", currentCpuUsage, cpuThreshold);
            return true;
        }
        
        return false;
    }
    
    private void updateCircuitState() {
        boolean shouldOpen = shouldOpenCircuit();
        CircuitBreakerState currentState = state.get();
        
        if (shouldOpen && currentState == CircuitBreakerState.CLOSED) {
            state.set(CircuitBreakerState.OPEN);
            circuitOpen = true;
            LOG.warn("Circuit breaker OPENED - CPU: {}%, Heap: {}%", currentCpuUsage, currentHeapUsage);
        } else if (!shouldOpen && currentState == CircuitBreakerState.OPEN) {
            state.set(CircuitBreakerState.CLOSED);
            circuitOpen = false;
            LOG.info("Circuit breaker CLOSED - CPU: {}%, Heap: {}%", currentCpuUsage, currentHeapUsage);
        }
    }
    
    private int calculateRetryAfterSeconds() {
        // Calculate retry-after based on current load
        double maxUsage = Math.max(currentCpuUsage / cpuThreshold, currentHeapUsage / heapThreshold);
        
        if (maxUsage > 1.5) {
            return 60; // High load - wait 1 minute
        } else if (maxUsage > 1.2) {
            return 30; // Medium load - wait 30 seconds
        } else {
            return 10; // Just over threshold - wait 10 seconds
        }
    }
    
    // Getters for metrics and monitoring
    public double getCurrentCpuUsage() { return currentCpuUsage; }
    public double getCurrentHeapUsage() { return currentHeapUsage; }
    public CircuitBreakerState getState() { return state.get(); }
    public boolean isCircuitOpen() { return circuitOpen; }
    public long getRejectedRequestCount() { return rejectedRequestCount.get(); }
    public long getTotalRequestCount() { return totalRequestCount.get(); }
    public boolean isEnabled() { return enabled; }
    
    /**
     * Result of circuit breaker check
     */
    public static class CircuitBreakerResult {
        private final boolean allowed;
        private final int retryAfterSeconds;
        private final String reason;
        
        private CircuitBreakerResult(boolean allowed, int retryAfterSeconds, String reason) {
            this.allowed = allowed;
            this.retryAfterSeconds = retryAfterSeconds;
            this.reason = reason;
        }
        
        public static CircuitBreakerResult allowed() {
            return new CircuitBreakerResult(true, 0, null);
        }
        
        public static CircuitBreakerResult rejected(int retryAfterSeconds) {
            return new CircuitBreakerResult(false, retryAfterSeconds, "System overloaded");
        }
        
        public boolean isAllowed() { return allowed; }
        public int getRetryAfterSeconds() { return retryAfterSeconds; }
        public String getReason() { return reason; }
    }
} 