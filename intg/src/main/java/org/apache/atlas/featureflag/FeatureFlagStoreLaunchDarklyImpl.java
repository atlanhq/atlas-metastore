/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.featureflag;

import com.launchdarkly.sdk.LDContext;
import com.launchdarkly.sdk.server.LDClient;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

@Lazy(value = false)
@Service
public class FeatureFlagStoreLaunchDarklyImpl  {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureFlagStoreLaunchDarklyImpl.class);

    private static volatile FeatureFlagStoreLaunchDarklyImpl INSTANCE;

    private final LDClient client;
    private final long ttlMs = 30_000L;
    private final static String LAUNCH_DARKLY_SDK_KEY = Objects.toString(System.getenv("USER_LAUNCH_DARKLY_SDK_KEY"), "");
    public final static String UNQ_CONTEXT_KEY = "context-atlas";
    public final static String CONTEXT_NAME = "Atlas";


    private static final String GLOBAL_CONTEXT_KEY = "__global__";

    private final Cache<String, Boolean> ttlCache = Caffeine.newBuilder()
            .expireAfterWrite(ttlMs, TimeUnit.MILLISECONDS)
            .build();

    private final Cache<String, Boolean> lkgCache = Caffeine.newBuilder().build();

    // Manually declared flag keys to preload on startup
    public static final String ENABLE_TAG_V2 = "atlas-enable-tag-v2";

    private static final Set<String> PRELOAD_FLAGS;
    static {
        PRELOAD_FLAGS = Set.of(
                ENABLE_TAG_V2
        );
    }

    @Inject
    public FeatureFlagStoreLaunchDarklyImpl() {
        this.client = new LDClient(LAUNCH_DARKLY_SDK_KEY);
    }

    @PostConstruct
    public void preloadOrFail() {

        waitForClientReadyOrFail();

        LDContext ctx = LDContext.builder(UNQ_CONTEXT_KEY)
                .name(CONTEXT_NAME)
                .build();

        for (String flagKey : PRELOAD_FLAGS) {
            try {
                boolean value = client.boolVariation(flagKey, ctx, false);
                ttlCache.put(globalKey(flagKey), value);
                lkgCache.put(globalKey(flagKey), value);
            } catch (Exception e) {
                LOG.error("Failed to preload feature flag {}", flagKey, e);
                throw new IllegalStateException("Failed to preload feature flag: " + flagKey, e);
            }
        }

        INSTANCE = this;
    }

    public static boolean evaluate(String flagKey, String key, boolean value) {
        return getInstance().evaluateInternal(flagKey, key, value);
    }

    public static boolean evaluate(String flagKey, String key, String value) {
        return getInstance().evaluateInternal(flagKey, key, value);
    }

    private boolean evaluateInternal(String flagKey, String attributeKey, Object attributeValue) {
        LDContext ctx = (attributeValue instanceof Boolean)
                ? getContext(attributeKey, ((Boolean) attributeValue).booleanValue())
                : getContext(attributeKey, String.valueOf(attributeValue));
        String key = cacheKey(flagKey, attributeKey, attributeValue);

        Boolean cached = ttlCache.getIfPresent(key);
        if (cached != null) {
            return cached.booleanValue();
        }

        try {
            boolean fresh = client.boolVariation(flagKey, ctx, false);
            ttlCache.put(key, fresh);
            ttlCache.put(globalKey(flagKey), fresh);
            lkgCache.put(key, fresh);
            lkgCache.put(globalKey(flagKey), fresh);
            return fresh;
        } catch (Exception e) {
            LOG.warn("Feature flag evaluation failed for {} key={} value={} — serving last-known-good if present", flagKey, attributeKey, attributeValue);
            Boolean lkg = lkgCache.getIfPresent(key);
            if (lkg == null) {
                return Boolean.TRUE.equals(lkgCache.getIfPresent(globalKey(flagKey)));
            }
            return lkg;
        }
    }

    private static FeatureFlagStoreLaunchDarklyImpl getInstance() {
        FeatureFlagStoreLaunchDarklyImpl ref = INSTANCE;
        if (ref == null) {
            throw new IllegalStateException("FeatureFlagStoreLaunchDarklyImpl is not initialized yet");
        }
        return ref;
    }

    private void waitForClientReadyOrFail() {
        try {
            int attempts = 150; // ~15s at 100ms interval
            while (attempts-- > 0) {
                if (client.isInitialized()) {
                    return;
                }
                Thread.sleep(100);
            }
            throw new IllegalStateException("LaunchDarkly client failed to initialize within timeout");
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for LaunchDarkly initialization", ie);
        }
    }

    

    private LDContext getContext(String key, String value) {

        return LDContext.builder(UNQ_CONTEXT_KEY)
                .name(CONTEXT_NAME)
                .set(key, value)
                .build();
    }

    private LDContext getContext(String key, boolean value) {

        return LDContext.builder(UNQ_CONTEXT_KEY)
                .name(CONTEXT_NAME)
                .set(key, value)
                .build();
    }

    private String cacheKey(String flagKey, String attributeKey, Object attributeValue) {
        String normalizedValue = normalizeValue(attributeValue);
        return flagKey + "|" + (attributeKey == null ? "" : attributeKey) + "=" + normalizedValue;
    }

    private String globalKey(String flagKey) {
        return flagKey + "|" + GLOBAL_CONTEXT_KEY;
    }

    private String normalizeValue(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof String) {
            return ((String) value).trim().toLowerCase();
        }
        return String.valueOf(value);
    }
}