/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.discovery.searchpipeline;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.atlas.AtlasErrorCode.INTERNAL_ERROR;

/**
 * Wraps each pipeline stage with timeout tracking and adaptive retry for
 * retryable Cassandra exceptions.
 *
 * <p>Current IndexSearch code silently swallows all CQL exceptions (LOG.warn + continue
 * with missing data). This executor retries retryable failures and fails fast on
 * non-retryable ones.</p>
 *
 * <p>Retry strategy: exponential backoff with jitter.
 * Backoff: 50ms → 100ms → 200ms + random 0-50ms jitter per attempt.
 * Max retries configurable via {@code atlas.indexsearch.stage.max.retries} (default 2).</p>
 *
 * <p>Retryable exceptions are identified by class name (not compile-time type) to avoid
 * hard dependency on specific Cassandra driver versions across modules.</p>
 */
public class StageExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(StageExecutor.class);

    private static final long BASE_BACKOFF_MS = 50;
    private static final long MAX_JITTER_MS   = 50;

    /**
     * Cassandra driver exception class names that are safe to retry.
     * Checked by simple name to avoid compile-time dependency on driver version.
     */
    private static final Set<String> RETRYABLE_EXCEPTION_NAMES = Set.of(
            "ReadTimeoutException",
            "WriteTimeoutException",
            "UnavailableException",
            "OverloadedException",
            "AllNodesFailedException",
            "DriverTimeoutException",
            "NoNodeAvailableException",
            "BusyConnectionException",
            "ConnectionInitException"
    );

    /**
     * Execute a pipeline stage with adaptive retry on retryable Cassandra exceptions.
     *
     * @param stage   the enrichment stage to execute
     * @param context the shared pipeline context
     * @throws AtlasBaseException if the stage fails after all retries or with a non-retryable exception
     */
    public void execute(EnrichmentStage stage, SearchEnrichmentContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("indexSearch.stage." + stage.name());
        int maxRetries = AtlasConfiguration.ATLAS_INDEXSEARCH_STAGE_MAX_RETRIES.getInt();

        try {
            for (int attempt = 0; attempt <= maxRetries; attempt++) {
                try {
                    stage.enrich(context);
                    return; // success
                } catch (AtlasBaseException e) {
                    // AtlasBaseException is our application-level exception — don't retry
                    throw e;
                } catch (Exception e) {
                    if (!isRetryable(e) || attempt == maxRetries) {
                        LOG.error("Stage {} failed (attempt {}/{}, non-retryable or retries exhausted): {}",
                                stage.name(), attempt + 1, maxRetries + 1, e.getMessage());
                        throw new AtlasBaseException(INTERNAL_ERROR, e,
                                "IndexSearch stage " + stage.name() + " failed: " + e.getMessage());
                    }

                    LOG.warn("Stage {} failed with retryable error (attempt {}/{}): {}",
                            stage.name(), attempt + 1, maxRetries + 1, e.getMessage());

                    // Adaptive backoff: exponential with jitter
                    long delay = (BASE_BACKOFF_MS << attempt) + ThreadLocalRandom.current().nextLong(MAX_JITTER_MS);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new AtlasBaseException(INTERNAL_ERROR, ie,
                                "IndexSearch stage " + stage.name() + " interrupted during retry backoff");
                    }
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    /**
     * Determine if an exception is retryable by walking the cause chain and checking
     * class names against known Cassandra driver retryable exceptions.
     *
     * <p>Uses class name matching (not instanceof) to avoid compile-time dependency
     * on specific Cassandra driver versions across modules.</p>
     */
    static boolean isRetryable(Throwable t) {
        Throwable cause = t;
        while (cause != null) {
            if (RETRYABLE_EXCEPTION_NAMES.contains(cause.getClass().getSimpleName())) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }
}
