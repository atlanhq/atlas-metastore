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

import org.apache.atlas.exception.AtlasBaseException;

/**
 * A single stage in the IndexSearch enrichment pipeline.
 *
 * Each stage reads inputs from and writes outputs to the shared
 * {@link SearchEnrichmentContext}. Stages are executed sequentially
 * by the pipeline orchestrator, with per-stage metrics recorded
 * via {@code AtlasPerfMetrics.MetricRecorder}.
 */
public interface EnrichmentStage {

    /**
     * Execute this enrichment stage, reading from and writing to the shared context.
     *
     * @param context shared pipeline context
     * @throws AtlasBaseException if the stage encounters an unrecoverable error
     */
    void enrich(SearchEnrichmentContext context) throws AtlasBaseException;

    /**
     * Stage name used for metrics recording: {@code "indexSearch.stage.<name>"}.
     */
    String name();
}
