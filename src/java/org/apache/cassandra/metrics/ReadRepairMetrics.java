package org.apache.cassandra.metrics;

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.util.RatioGauge;

/**
 * Metrics related to Read Repair.
 */
public class ReadRepairMetrics {
    public static final String GROUP_NAME = "org.apache.cassandra.metrics";
    public static final String TYPE_NAME = "ReadRepair";
    
    public static final Meter repairedBlocking =
            Metrics.newMeter(new MetricName(GROUP_NAME, TYPE_NAME, "RepairedBlocking"), "RepairedBlocking", TimeUnit.SECONDS);
    public static final Meter repairedBackground =
            Metrics.newMeter(new MetricName(GROUP_NAME, TYPE_NAME, "RepairedBackground"), "RepairedBackground", TimeUnit.SECONDS);
    public static final Meter attempted = 
            Metrics.newMeter(new MetricName(GROUP_NAME, TYPE_NAME, "Attempted"), "Attempted", TimeUnit.SECONDS);
}
