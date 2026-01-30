/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.web.service;

import org.apache.commons.lang.StringUtils;
import org.aspectj.lang.Signature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.web.service.DebugMetricsWrapper.Constants.*;

/**
 * Debug metrics source - simplified implementation without Hadoop metrics2.
 * Metrics are collected but not exposed via Hadoop metrics system.
 */
public class AtlasDebugMetricsSource {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasDebugMetricsSource.class);

    public static final Map<String, String> fieldLowerCaseUpperCaseMap = new HashMap<>();
    public static final Set<String>         debugMetricsAttributes     = new HashSet<>();

    public AtlasDebugMetricsSource() {
        initAttrList();
    }

    public void update(Signature name, Long timeConsumed) {
        // No-op: Hadoop metrics2 removed
        // Metrics collection is disabled
        if (LOG.isTraceEnabled()) {
            LOG.trace("Debug metric update: {} took {}ms", name.toShortString(), timeConsumed);
        }
    }

    private void initAttrList() {
        debugMetricsAttributes.add(NUM_OPS);
        debugMetricsAttributes.add(MIN_TIME);
        debugMetricsAttributes.add(STD_DEV_TIME);
        debugMetricsAttributes.add(MAX_TIME);
        debugMetricsAttributes.add(AVG_TIME);
    }
}
