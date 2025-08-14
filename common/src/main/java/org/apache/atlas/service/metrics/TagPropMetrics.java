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
package org.apache.atlas.service.metrics;

import org.apache.atlas.RequestContext;
import org.apache.atlas.utils.AtlasMetricType;
import org.apache.atlas.utils.AtlasPerfMetrics;

public final class TagPropMetrics {
    private TagPropMetrics() { }

    public static void emitTimer(String metricName, String version, String type, String tagType, String status, long millis) {
        AtlasPerfMetrics.Metric metric = new AtlasPerfMetrics.Metric(metricName);
        metric.setMetricType(AtlasMetricType.TIMER);
        metric.setTotalTimeMSecs(millis);
        metric.addTag("name", metricName);
        metric.addTag("version", version);
        metric.addTag("type", type);
        metric.addTag("tag_type", tagType);
        metric.addTag("status", status);
        RequestContext.get().addApplicationMetrics(metric);
    }

    public static void emitCounter(String metricName, String version, String type, String tagType, String status, long count) {
        AtlasPerfMetrics.Metric metric = new AtlasPerfMetrics.Metric(metricName);
        metric.setMetricType(AtlasMetricType.COUNTER);
        metric.setInvocations(count);
        metric.addTag("name", metricName);
        metric.addTag("version", version);
        metric.addTag("type", type);
        metric.addTag("tag_type", tagType);
        metric.addTag("status", status);
        RequestContext.get().addApplicationMetrics(metric);
    }
}


