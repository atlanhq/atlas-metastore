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
package org.apache.atlas.authorizer.trace;

/**
 * ThreadLocal context manager for decision trace collection.
 * Provides a way to collect policy match information during authorization
 * without modifying existing method signatures.
 */
public class AccessDecisionContext {
    private static final ThreadLocal<DecisionTraceCollector> context = new ThreadLocal<>();

    /**
     * Starts trace collection for the current thread.
     * Must be paired with endTrace() in a finally block to prevent memory leaks.
     */
    public static void startTrace() {
        context.set(new DecisionTraceCollector());
    }

    /**
     * Gets the current trace collector for this thread.
     * @return the DecisionTraceCollector or null if trace not started
     */
    public static DecisionTraceCollector getCurrentTrace() {
        return context.get();
    }

    /**
     * Ends trace collection and cleans up ThreadLocal.
     * MUST be called in a finally block to prevent memory leaks.
     */
    public static void endTrace() {
        context.remove();
    }

    /**
     * Checks if trace collection is enabled for the current thread.
     * @return true if trace is active, false otherwise
     */
    public static boolean isTraceEnabled() {
        return context.get() != null;
    }
}
