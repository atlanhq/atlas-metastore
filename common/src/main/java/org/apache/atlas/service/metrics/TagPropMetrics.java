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


