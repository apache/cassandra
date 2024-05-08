/*
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
package org.apache.cassandra.metrics;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.db.virtual.model.CounterMetricRow;
import org.apache.cassandra.db.virtual.model.GaugeMetricRow;
import org.apache.cassandra.db.virtual.model.HistogramMetricRow;
import org.apache.cassandra.db.virtual.model.MeterMetricRow;
import org.apache.cassandra.db.virtual.model.MetricGroupRow;
import org.apache.cassandra.db.virtual.model.MetricRow;
import org.apache.cassandra.db.virtual.model.TimerMetricRow;
import org.apache.cassandra.db.virtual.walker.CounterMetricRowWalker;
import org.apache.cassandra.db.virtual.walker.GaugeMetricRowWalker;
import org.apache.cassandra.db.virtual.walker.HistogramMetricRowWalker;
import org.apache.cassandra.db.virtual.walker.MeterMetricRowWalker;
import org.apache.cassandra.db.virtual.walker.MetricGroupRowWalker;
import org.apache.cassandra.db.virtual.walker.MetricRowWalker;
import org.apache.cassandra.db.virtual.walker.TimerMetricRowWalker;
import org.apache.cassandra.index.sai.metrics.AbstractMetrics;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.memory.MemtablePool;

import static java.util.Optional.ofNullable;
import static org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter.createSinglePartitionedKeyFiltered;
import static org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter.createSinglePartitionedValueFiltered;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_METRICS;

/**
 * Dropwizard metrics registry extension for Cassandra, as of for now uses the latest version of Dropwizard metrics
 * library {@code 4.2.x} that has a pretty good integration with JMX. The registry is used by Cassandra to
 * store all metrics and expose them to JMX and {@link org.apache.cassandra.db.virtual.VirtualTable}.
 * In addition to that, the registry provides a way to store aliases for metrics and group metrics by
 * the Cassandra-specific metric groups, which are used to expose metrics in that way.
 *
 * @see org.apache.cassandra.db.virtual.VirtualTable
 * @see org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter
 */
public class CassandraMetricsRegistry extends MetricRegistry
{
    public static final UnaryOperator<String> METRICS_GROUP_POSTFIX = name -> name + "_group";

    /** A set of all known metric groups, used to validate metric groups that are statically defined in Cassandra. */
    static final Set<String> metricGroups;

    /**
     * Root metrics registry that is used by Cassandra to store all metrics.
     * All modifications to the registry are delegated to the corresponding listeners as well.
     */
    public static final CassandraMetricsRegistry Metrics = new CassandraMetricsRegistry();

    private final Map<String, ThreadPoolMetrics> threadPoolMetrics = new ConcurrentHashMap<>();
    public static final String METRIC_SCOPE_UNDEFINED = "undefined";
    public static final TimeUnit DEFAULT_TIMER_UNIT = TimeUnit.MICROSECONDS;

    static
    {
        // We have to initialize metric group names like this, because we can't register them dynamically
        // as it is done for the dropwizard metrics. So we have to be sure, that all these metric groups are
        // initialized at the time #start() method is called. The virtual kespaces are immutable, drivers also
        // rely on the fact that virtual keyspaces are immutable, so they won't receive any updates if we change them.
        //
        // For example, if a new metric group is added, the appropriate tests will fail, because of the missing
        // group in this collection, which in turn guarantees that all metric groups are registered and exposed
        // for virtual tables.
        metricGroups = ImmutableSet.<String>builder()
                                   .add(AbstractMetrics.TYPE)
                                   .add(BatchMetrics.TYPE_NAME)
                                   .add(BufferPoolMetrics.TYPE_NAME)
                                   .add(CIDRAuthorizerMetrics.TYPE_NAME)
                                   .add(CQLMetrics.TYPE_NAME)
                                   .add(CacheMetrics.TYPE_NAME)
                                   .add(ChunkCacheMetrics.TYPE_NAME)
                                   .add(ClientMessageSizeMetrics.TYPE)
                                   .add(ClientMetrics.TYPE_NAME)
                                   .add(ClientRequestMetrics.TYPE_NAME)
                                   .add(ClientRequestSizeMetrics.TYPE)
                                   .add(CommitLogMetrics.TYPE_NAME)
                                   .add(CompactionMetrics.TYPE_NAME)
                                   .add(DenylistMetrics.TYPE_NAME)
                                   .add(DroppedMessageMetrics.TYPE)
                                   .add(HintedHandoffMetrics.TYPE_NAME)
                                   .add(HintsServiceMetrics.TYPE_NAME)
                                   .add(InternodeInboundMetrics.TYPE_NAME)
                                   .add(InternodeOutboundMetrics.TYPE_NAME)
                                   .add(KeyspaceMetrics.TYPE_NAME)
                                   .add(MemtablePool.TYPE_NAME)
                                   .add(MessagingMetrics.TYPE_NAME)
                                   .add(MutualTlsMetrics.TYPE_NAME)
                                   .add(PaxosMetrics.TYPE_NAME)
                                   .add(ReadRepairMetrics.TYPE_NAME)
                                   .add(RepairMetrics.TYPE_NAME)
                                   .add(RowIndexEntry.TYPE_NAME)
                                   .add(StorageMetrics.TYPE_NAME)
                                   .add(StreamingMetrics.TYPE_NAME)
                                   .add(TCMMetrics.TYPE_NAME)
                                   .add(TableMetrics.ALIAS_TYPE_NAME)
                                   .add(TableMetrics.TYPE_NAME)
                                   .add(TableMetrics.INDEX_TYPE_NAME)
                                   .add(TableMetrics.INDEX_ALIAS_TYPE_NAME)
                                   .add(ThreadPoolMetrics.TYPE_NAME)
                                   .add(TrieMemtableMetricsView.TYPE_NAME)
                                   .add(UnweightedCacheMetrics.TYPE_NAME)
                                   .build();
    }

    private CassandraMetricsRegistry()
    {
    }

    @SuppressWarnings("rawtypes")
    public static String getValueAsString(Metric metric)
    {
        if (metric instanceof Counter)
            return Long.toString(((Counter) metric).getCount());
        else if (metric instanceof Gauge)
            return getGaugeValue((Gauge) metric);
        else if (metric instanceof Histogram)
            return Double.toString(((Histogram) metric).getSnapshot().getMedian());
        else if (metric instanceof Meter)
            return Long.toString(((Meter) metric).getCount());
        else if (metric instanceof Timer)
            return Long.toString(((Timer) metric).getCount());
        else
            throw new IllegalStateException("Unknown metric type: " + metric.getClass().getName());
    }

    public static String getGaugeValue(Gauge<?> gauge)
    {
        Object value = gauge.getValue();
        if (value == null)
            return "null";
        else if (value instanceof long[])
            return Arrays.toString((long[]) value);
        else if (value instanceof double[])
            return Arrays.toString((double[]) value);
        else if (value instanceof short[])
            return Arrays.toString((short[]) value);
        else if (value instanceof int[])
            return Arrays.toString((int[]) value);
        else if (value instanceof float[])
            return Arrays.toString((float[]) value);
        else if (value instanceof byte[])
            return Arrays.toString((byte[]) value);
        else if (value instanceof Object[])
            return Arrays.toString((Object[]) value);
        else
            return value.toString();
    }

    public static List<VirtualTable> createMetricsKeyspaceTables()
    {
        ImmutableList.Builder<VirtualTable> builder = ImmutableList.builder();
        metricGroups.forEach(groupName -> {
            // This is a very efficient way to filter metrics by group name, so make sure that metrics group name
            // and metric type following the same order as it constructed in MetricName class.
            final String groupPrefix = DefaultNameFactory.GROUP_NAME + '.' + groupName + '.';
            builder.add(createSinglePartitionedKeyFiltered(VIRTUAL_METRICS,
                                                           METRICS_GROUP_POSTFIX.apply(groupName),
                                                           "All metrics for \"" + groupName + "\" metric group",
                                                           new MetricRowWalker(),
                                                           Metrics.getMetrics(),
                                                           key -> key.startsWith(groupPrefix),
                                                           MetricRow::new));
        });
        // Register virtual table of all known metric groups.
        builder.add(CollectionVirtualTableAdapter.create(VIRTUAL_METRICS,
                                                         "all_groups",
                                                         "All metric group names",
                                                         new MetricGroupRowWalker(),
                                                         metricGroups,
                                                         MetricGroupRow::new))
               // Register virtual tables of all metrics types similar to the JMX MBean structure,
               // e.g.: HistogramJmxMBean, MeterJmxMBean, etc.
               .add(createSinglePartitionedValueFiltered(VIRTUAL_METRICS,
                                                         "type_counter",
                                                         "All metrics with type \"Counter\"",
                                                         new CounterMetricRowWalker(),
                                                         Metrics.getMetrics(),
                                                         Counter.class::isInstance,
                                                         CounterMetricRow::new))
               .add(createSinglePartitionedValueFiltered(VIRTUAL_METRICS,
                                                         "type_gauge",
                                                         "All metrics with type \"Gauge\"",
                                                         new GaugeMetricRowWalker(),
                                                         Metrics.getMetrics(),
                                                         Gauge.class::isInstance,
                                                         GaugeMetricRow::new))
               .add(createSinglePartitionedValueFiltered(VIRTUAL_METRICS,
                                                         "type_histogram",
                                                         "All metrics with type \"Histogram\"",
                                                         new HistogramMetricRowWalker(),
                                                         Metrics.getMetrics(),
                                                         Histogram.class::isInstance,
                                                         HistogramMetricRow::new))
               .add(createSinglePartitionedValueFiltered(VIRTUAL_METRICS,
                                                         "type_meter",
                                                         "All metrics with type \"Meter\"",
                                                         new MeterMetricRowWalker(),
                                                         Metrics.getMetrics(),
                                                         Meter.class::isInstance,
                                                         MeterMetricRow::new))
               .add(createSinglePartitionedValueFiltered(VIRTUAL_METRICS,
                                                         "type_timer",
                                                         "All metrics with type \"Timer\"",
                                                         new TimerMetricRowWalker(),
                                                         Metrics.getMetrics(),
                                                         Timer.class::isInstance,
                                                         TimerMetricRow::new));
        return builder.build();
    }

    private static void verifyUnknownMetric(MetricName newMetricName)
    {
        String type = newMetricName.getType();
        if (type.indexOf('.') >= 0)
            throw new IllegalStateException(
                "Metric type must not contain '.' character as it results in the efficiency of the metric collection traversal: " + type);
        if (!metricGroups.contains(newMetricName.getType()))
            throw new IllegalStateException("Unknown metric group: " + newMetricName.getType());
        if (!metricGroups.contains(newMetricName.getSystemViewName()))
            throw new IllegalStateException("Metric view name must match statically registered groups: " +
                                            newMetricName.getSystemViewName());
    }

    public String getMetricScope(String metricName)
    {
        int groupLen = DefaultNameFactory.GROUP_NAME.length();
        int lastIndex = findNthIndexOf(metricName, groupLen, 2);
        return metricName.length() <= groupLen || lastIndex == -1 ? METRIC_SCOPE_UNDEFINED :
               metricName.substring(lastIndex + 1);
    }

    // Helper method to find the index of the nth occurrence of a specified character
    private static int findNthIndexOf(String str, int start, int n)
    {
        int index = start;
        while (n-- > 0)
        {
            index = str.indexOf('.', index + 1);
            if (index == -1) break;
        }
        return index;
    }

    public Counter counter(MetricName... name)
    {
        Counter counter = super.counter(name[0].getMetricName());
        Stream.of(name).forEach(n -> register(n, counter));
        return counter;
    }

    public Meter meter(MetricName... name)
    {
        Meter meter = super.meter(name[0].getMetricName());
        Stream.of(name).forEach(n -> register(n, meter));
        return meter;
    }

    public Histogram histogram(MetricName name, boolean considerZeroes)
    {
        return register(name, new ClearableHistogram(new DecayingEstimatedHistogramReservoir(considerZeroes)));
    }

    public Histogram histogram(MetricName name, MetricName alias, boolean considerZeroes)
    {
        Histogram histogram = histogram(name, considerZeroes);
        register(alias, histogram);
        return histogram;
    }

    public <T extends Gauge<?>> T gauge(MetricName name, MetricName alias, T gauge)
    {
        T gaugeLoc = register(name, gauge);
        register(alias, gaugeLoc);
        return gaugeLoc;
    }

    public Timer timer(MetricName name)
    {
        return timer(name, DEFAULT_TIMER_UNIT);
    }

    public SnapshottingTimer timer(MetricName name, MetricName alias)
    {
        return timer(name, alias, DEFAULT_TIMER_UNIT);
    }

    private SnapshottingTimer timer(MetricName name, TimeUnit durationUnit)
    {
        return register(name, new SnapshottingTimer(CassandraMetricsRegistry.createReservoir(durationUnit)));
    }

    public SnapshottingTimer timer(MetricName name, MetricName alias, TimeUnit durationUnit)
    {
        SnapshottingTimer timer = timer(name, durationUnit);
        register(alias, timer);
        return timer;
    }

    public static SnapshottingReservoir createReservoir(TimeUnit durationUnit)
    {
        SnapshottingReservoir reservoir;
        if (durationUnit != TimeUnit.NANOSECONDS)
        {
            SnapshottingReservoir underlying = new DecayingEstimatedHistogramReservoir(DecayingEstimatedHistogramReservoir.DEFAULT_ZERO_CONSIDERATION,
                                                                           DecayingEstimatedHistogramReservoir.LOW_BUCKET_COUNT,
                                                                           DecayingEstimatedHistogramReservoir.DEFAULT_STRIPE_COUNT);
            // fewer buckets should suffice if timer is not based on nanos
            reservoir = new ScalingReservoir(underlying,
                                             // timer update values in nanos.
                                             v -> durationUnit.convert(v, TimeUnit.NANOSECONDS));
        }
        else
        {
            // Use more buckets if timer is created with nanos resolution.
            reservoir = new DecayingEstimatedHistogramReservoir();
        }
        return reservoir;
    }

    public <T extends Metric> T register(MetricName name, T metric)
    {
        if (metric instanceof MetricSet)
            throw new IllegalArgumentException("MetricSet registration using MetricName is not supported");

        try
        {
            verifyUnknownMetric(name);
            registerMBean(metric, name.getMBeanName(), MBeanWrapper.instance);
            return super.register(name.getMetricName(), metric);
        }
        catch (IllegalArgumentException e)
        {
            Metric existing = Metrics.getMetrics().get(name.getMetricName());
            return (T)existing;
        }
    }

    public Collection<ThreadPoolMetrics> allThreadPoolMetrics()
    {
        return Collections.unmodifiableCollection(threadPoolMetrics.values());
    }

    public Optional<ThreadPoolMetrics> getThreadPoolMetrics(String poolName)
    {
        return ofNullable(threadPoolMetrics.get(poolName));
    }

    ThreadPoolMetrics register(ThreadPoolMetrics metrics)
    {
        threadPoolMetrics.put(metrics.poolName, metrics);
        return metrics;
    }

    void remove(ThreadPoolMetrics metrics)
    {
        threadPoolMetrics.remove(metrics.poolName, metrics);
    }

    public <T extends Metric> T register(MetricName name, T metric, MetricName... aliases)
    {
        T metricLoc = register(name, metric);
        Stream.of(aliases).forEach(n -> register(n, metricLoc));
        return metricLoc;
    }

    /**
     * Removes all metrics that match the given predicate.
     *
     * @param resolver a function that resolves a short metric name from a full metric name,
     * or @{code null} if the metric should not be removed
     * @param factory a function that creates a metric name from a short metric name.
     * @param onRemoved a callback that is called for each removed metric.
     */
    public void removeIfMatch(MetricNameResolver resolver,
                              Function<String, MetricName> factory,
                              Consumer<MetricName> onRemoved)
    {
        removeMatching((full, metric) -> {
            String shortName = resolver.resolve(full);
            if (shortName == null)
                return false;

            MetricName metricName = factory.apply(shortName);
            boolean remove = metricName.getMetricName().equals(full);

            if (remove)
            {
                unregisterMBean(metricName.getMBeanName(), MBeanWrapper.instance);
                onRemoved.accept(metricName);
            }
            return remove;
        });
    }

    /**
     * Default implementation of the {@link MetricNameResolver} that resolves a short metric name from a full metric name,
     * assuming that the full metric name doesn't contain dots. Returns {@code null} if the full metric name doesn't match
     * the provided group and type.
     * <p>
     * The {@code scope} is the last part of the full metric name. Can be {@code null} and it's used for search efficiency.
     *
     * @param fullName full metric name
     * @param group metric group
     * @param type metric type
     * @param scope metric scope, which is the last part of the full metric name and used for search efficiency.
     * @return short metric name or {@code null} if the full metric name doesn't match the group, type, and scope.
     */
    public static @Nullable String resolveShortMetricName(String fullName, String group, String type, @Nullable String scope)
    {
        String prefix = name(group, type);
        if (fullName.startsWith(prefix))
        {
            int lastDot = fullName.indexOf('.', prefix.length() + 1);
            // If a metric scope is null (dots not found), the metric name is the last part of the full metric name.
            if (lastDot == -1)
                return fullName.substring(prefix.length() + 1);

            if (scope == null)
                return fullName.substring(prefix.length() + 1, lastDot);

            return fullName.substring(lastDot + 1).equals(scope) ?
                   fullName.substring(prefix.length() + 1, lastDot) :
                   null;
        }

        return null;
    }

    public void remove(MetricName name)
    {
        boolean success = remove(name.getMetricName());
        if (success)
            unregisterMBean(name.getMBeanName(), MBeanWrapper.instance);
    }

    @FunctionalInterface
    public interface MetricNameResolver
    {
        @Nullable String resolve(String fullName);
    }

    private void registerMBean(Metric metric, ObjectName name, MBeanWrapper mBeanServer)
    {
        AbstractBean mbean;

        if (metric instanceof Gauge)
            mbean = new JmxGauge((Gauge<?>) metric, name);
        else if (metric instanceof Counter)
            mbean = new JmxCounter((Counter) metric, name);
        else if (metric instanceof Histogram)
            mbean = new JmxHistogram((Histogram) metric, name);
        else if (metric instanceof Timer)
            mbean = new JmxTimer((Timer) metric, name, TimeUnit.SECONDS, DEFAULT_TIMER_UNIT);
        else if (metric instanceof Metered)
            mbean = new JmxMeter((Metered) metric, name, TimeUnit.SECONDS);
        else
            throw new IllegalArgumentException("Unknown metric type: " + metric.getClass());

        if (mBeanServer != null && !mBeanServer.isRegistered(name))
            mBeanServer.registerMBean(mbean, name, MBeanWrapper.OnException.LOG);
    }

    private void unregisterMBean(ObjectName name, MBeanWrapper mBeanWrapper)
    {
        mBeanWrapper.unregisterMBean(name, MBeanWrapper.OnException.IGNORE);
    }
    
    /**
     * Strips a single final '$' from input
     * 
     * @param s String to strip
     * @return a string with one less '$' at end
     */
    private static String withoutFinalDollar(String s)
    {
        int l = s.length();
        return (l!=0 && '$' == s.charAt(l-1))?s.substring(0,l-1):s;
    }

    public interface MetricMBean
    {
        ObjectName objectName();
    }

    private abstract static class AbstractBean implements MetricMBean
    {
        private final ObjectName objectName;

        AbstractBean(ObjectName objectName)
        {
            this.objectName = objectName;
        }

        @Override
        public ObjectName objectName()
        {
            return objectName;
        }
    }

    /**
     * Exports a gauge as a JMX MBean, check corresponding {@link org.apache.cassandra.db.virtual.model.GaugeMetricRow}
     * for the same functionality for virtual tables.
     */
    public interface JmxGaugeMBean extends MetricMBean
    {
        Object getValue();
    }

    private static class JmxGauge extends AbstractBean implements JmxGaugeMBean
    {
        private final Gauge<?> metric;

        private JmxGauge(Gauge<?> metric, ObjectName objectName)
        {
            super(objectName);
            this.metric = metric;
        }

        @Override
        public Object getValue()
        {
            return metric.getValue();
        }
    }

    /**
     * Exports a histogram as a JMX MBean, check corresponding {@link org.apache.cassandra.db.virtual.model.HistogramMetricRow}
     * for the same functionality for virtual tables.
     */
    public interface JmxHistogramMBean extends MetricMBean
    {
        long getCount();

        long getMin();

        long getMax();

        double getMean();

        double getStdDev();

        double get50thPercentile();

        double get75thPercentile();

        double get95thPercentile();

        double get98thPercentile();

        double get99thPercentile();

        double get999thPercentile();

        long[] values();

        long[] getRecentValues();
    }

    private static class JmxHistogram extends AbstractBean implements JmxHistogramMBean
    {
        private final Histogram metric;
        private long[] last = null;

        private JmxHistogram(Histogram metric, ObjectName objectName)
        {
            super(objectName);
            this.metric = metric;
        }

        @Override
        public double get50thPercentile()
        {
            return metric.getSnapshot().getMedian();
        }

        @Override
        public long getCount()
        {
            return metric.getCount();
        }

        @Override
        public long getMin()
        {
            return metric.getSnapshot().getMin();
        }

        @Override
        public long getMax()
        {
            return metric.getSnapshot().getMax();
        }

        @Override
        public double getMean()
        {
            return metric.getSnapshot().getMean();
        }

        @Override
        public double getStdDev()
        {
            return metric.getSnapshot().getStdDev();
        }

        @Override
        public double get75thPercentile()
        {
            return metric.getSnapshot().get75thPercentile();
        }

        @Override
        public double get95thPercentile()
        {
            return metric.getSnapshot().get95thPercentile();
        }

        @Override
        public double get98thPercentile()
        {
            return metric.getSnapshot().get98thPercentile();
        }

        @Override
        public double get99thPercentile()
        {
            return metric.getSnapshot().get99thPercentile();
        }

        @Override
        public double get999thPercentile()
        {
            return metric.getSnapshot().get999thPercentile();
        }

        @Override
        public long[] values()
        {
            return metric.getSnapshot().getValues();
        }

        /**
         * Returns a histogram describing the values recorded since the last time this method was called.
         *
         * ex. If the counts are [0, 1, 2, 1] at the time the first caller arrives, but change to [1, 2, 3, 2] by the 
         * time a second caller arrives, the second caller will receive [1, 1, 1, 1].
         *
         * @return a histogram whose bucket offsets are assumed to be in nanoseconds
         */
        @Override
        public synchronized long[] getRecentValues()
        {
            long[] now = metric.getSnapshot().getValues();
            long[] delta = delta(now, last);
            last = now;
            return delta;
        }
    }

    /**
     * Exports a counter as a JMX MBean, check corresponding {@link org.apache.cassandra.db.virtual.model.CounterMetricRow}
     * for the same functionality for virtual tables.
     */
    public interface JmxCounterMBean extends MetricMBean
    {
        long getCount();
    }

    private static class JmxCounter extends AbstractBean implements JmxCounterMBean
    {
        private final Counter metric;

        private JmxCounter(Counter metric, ObjectName objectName)
        {
            super(objectName);
            this.metric = metric;
        }

        @Override
        public long getCount()
        {
            return metric.getCount();
        }
    }

    /**
     * Exports a meter as a JMX MBean, check corresponding {@link org.apache.cassandra.db.virtual.model.MeterMetricRow}
     * for the same functionality for virtual tables.
     */
    public interface JmxMeterMBean extends MetricMBean
    {
        long getCount();

        double getMeanRate();

        double getOneMinuteRate();

        double getFiveMinuteRate();

        double getFifteenMinuteRate();

        String getRateUnit();
    }

    /**
     * Exports a timer as a JMX MBean, check corresponding {@link org.apache.cassandra.db.virtual.model.TimerMetricRow}
     * for the same functionality for virtual tables.
     */
    private static class JmxMeter extends AbstractBean implements JmxMeterMBean
    {
        private final Metered metric;
        private final double rateFactor;
        private final String rateUnit;

        private JmxMeter(Metered metric, ObjectName objectName, TimeUnit rateUnit)
        {
            super(objectName);
            this.metric = metric;
            this.rateFactor = rateUnit.toSeconds(1);
            this.rateUnit = "events/" + calculateRateUnit(rateUnit);
        }

        @Override
        public long getCount()
        {
            return metric.getCount();
        }

        @Override
        public double getMeanRate()
        {
            return metric.getMeanRate() * rateFactor;
        }

        @Override
        public double getOneMinuteRate()
        {
            return metric.getOneMinuteRate() * rateFactor;
        }

        @Override
        public double getFiveMinuteRate()
        {
            return metric.getFiveMinuteRate() * rateFactor;
        }

        @Override
        public double getFifteenMinuteRate()
        {
            return metric.getFifteenMinuteRate() * rateFactor;
        }

        @Override
        public String getRateUnit()
        {
            return rateUnit;
        }

        private String calculateRateUnit(TimeUnit unit)
        {
            final String s = unit.toString().toLowerCase(Locale.US);
            return s.substring(0, s.length() - 1);
        }
    }

    /**
     * Exports a timer as a JMX MBean, check corresponding {@link org.apache.cassandra.db.virtual.model.TimerMetricRow}
     * for the same functionality for virtual tables.
     */
    public interface JmxTimerMBean extends JmxMeterMBean
    {
        double getMin();

        double getMax();

        double getMean();

        double getStdDev();

        double get50thPercentile();

        double get75thPercentile();

        double get95thPercentile();

        double get98thPercentile();

        double get99thPercentile();

        double get999thPercentile();

        long[] values();

        long[] getRecentValues();

        String getDurationUnit();
    }

    static class JmxTimer extends JmxMeter implements JmxTimerMBean
    {
        private final Timer metric;
        private final String durationUnit;
        private long[] last = null;

        private JmxTimer(Timer metric,
                         ObjectName objectName,
                         TimeUnit rateUnit,
                         TimeUnit durationUnit)
        {
            super(metric, objectName, rateUnit);
            this.metric = metric;
            this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
        }

        @Override
        public double get50thPercentile()
        {
            return metric.getSnapshot().getMedian();
        }

        @Override
        public double getMin()
        {
            return metric.getSnapshot().getMin();
        }

        @Override
        public double getMax()
        {
            return metric.getSnapshot().getMax();
        }

        @Override
        public double getMean()
        {
            return metric.getSnapshot().getMean();
        }

        @Override
        public double getStdDev()
        {
            return metric.getSnapshot().getStdDev();
        }

        @Override
        public double get75thPercentile()
        {
            return metric.getSnapshot().get75thPercentile();
        }

        @Override
        public double get95thPercentile()
        {
            return metric.getSnapshot().get95thPercentile();
        }

        @Override
        public double get98thPercentile()
        {
            return metric.getSnapshot().get98thPercentile();
        }

        @Override
        public double get99thPercentile()
        {
            return metric.getSnapshot().get99thPercentile();
        }

        @Override
        public double get999thPercentile()
        {
            return metric.getSnapshot().get999thPercentile();
        }

        @Override
        public long[] values()
        {
            return metric.getSnapshot().getValues();
        }

        /**
         * Returns a histogram describing the values recorded since the last time this method was called.
         * 
         * ex. If the counts are [0, 1, 2, 1] at the time the first caller arrives, but change to [1, 2, 3, 2] by the 
         * time a second caller arrives, the second caller will receive [1, 1, 1, 1].
         * 
         * @return a histogram whose bucket offsets are assumed to be in nanoseconds
         */
        @Override
        public synchronized long[] getRecentValues()
        {
            long[] now = metric.getSnapshot().getValues();
            long[] delta = delta(now, last);
            last = now;
            return delta;
        }

        @Override
        public String getDurationUnit()
        {
            return durationUnit;
        }
    }

    /**
     * Used to determine the changes in a histogram since the last time checked.
     *
     * @param now The current histogram
     * @param last The previous value of the histogram
     * @return the difference between <i>now</i> and <i>last</i>
     */
    @VisibleForTesting
    static long[] delta(long[] now, long[] last)
    {
        long[] delta = new long[now.length];
        if (last == null)
        {
            last = new long[now.length];
        }
        for(int i = 0; i< now.length; i++)
        {
            delta[i] = now[i] - (i < last.length? last[i] : 0);
        }
        return delta;
    }

    /**
     * A value class encapsulating a metric's owning class and name.
     */
    public static class MetricName implements Comparable<MetricName>
    {
        public static final MetricName EMPTY = new MetricName(MetricName.class, "EMPTY");
        private final String group;
        private final String type;
        private final String name;
        private final String scope;
        private final String mBeanName;
        private final String systemViewName;

        /**
         * Creates a new {@link MetricName} without a scope.
         *
         * @param klass the {@link Class} to which the {@link Metric} belongs
         * @param name  the name of the {@link Metric}
         */
        public MetricName(Class<?> klass, String name)
        {
            this(klass, name, null);
        }

        /**
         * Creates a new {@link MetricName} without a scope.
         *
         * @param group the group to which the {@link Metric} belongs
         * @param type  the type to which the {@link Metric} belongs
         * @param name  the name of the {@link Metric}
         */
        public MetricName(String group, String type, String name)
        {
            this(group, type, name, null);
        }

        /**
         * Creates a new {@link MetricName} without a scope.
         *
         * @param klass the {@link Class} to which the {@link Metric} belongs
         * @param name  the name of the {@link Metric}
         * @param scope the scope of the {@link Metric}
         */
        public MetricName(Class<?> klass, String name, String scope)
        {
            this(klass.getPackage() == null ? "" : klass.getPackage().getName(),
                    withoutFinalDollar(klass.getSimpleName()),
                    name,
                    scope);
        }

        /**
         * Creates a new {@link MetricName} without a scope.
         *
         * @param group the group to which the {@link Metric} belongs
         * @param type  the type to which the {@link Metric} belongs
         * @param name  the name of the {@link Metric}
         * @param scope the scope of the {@link Metric}
         */
        public MetricName(String group, String type, String name, String scope)
        {
            this(group, type, name, scope, createMBeanName(group, type, name, scope));
        }

        public MetricName(String group, String type, String name, String scope, String mBeanName)
        {
            this(group, type, name, scope, mBeanName, type);
        }

        /**
         * Creates a new {@link MetricName} without a scope.
         *
         * @param group the group to which the {@link Metric} belongs
         * @param type the type to which the {@link Metric} belongs
         * @param name the name of the {@link Metric}
         * @param scope the scope of the {@link Metric}
         * @param mBeanName the 'ObjectName', represented as a string, to use when registering the MBean.
         * @param systemViewName the name of the virtual table to which the {@link Metric} belongs.
         */
        public MetricName(String group, String type, String name, String scope, String mBeanName, String systemViewName)
        {
            if (group == null || type == null)
            {
                throw new IllegalArgumentException("Both group and type need to be specified");
            }
            if (name == null)
            {
                throw new IllegalArgumentException("Name needs to be specified");
            }
            if (scope != null && scope.contains(name))
            {
                throw new IllegalArgumentException("Scope cannot contain name, this is not neccessary and will cause performance issues. " +
                                                   "Scope: " + scope + " Name: " + name);
            }
            this.group = group;
            this.type = type;
            this.name = name;
            this.scope = scope;
            this.mBeanName = mBeanName;
            this.systemViewName = systemViewName;
        }

        /**
         * Returns the group to which the {@link Metric} belongs. For class-based metrics, this will be
         * the package name of the {@link Class} to which the {@link Metric} belongs.
         *
         * @return the group to which the {@link Metric} belongs
         */
        public String getGroup()
        {
            return group;
        }

        /**
         * Returns the type to which the {@link Metric} belongs. For class-based metrics, this will be
         * the simple class name of the {@link Class} to which the {@link Metric} belongs.
         *
         * @return the type to which the {@link Metric} belongs
         */
        public String getType()
        {
            return type;
        }

        /**
         * Returns the name of the {@link Metric}.
         *
         * @return the name of the {@link Metric}
         */
        public String getName()
        {
            return name;
        }

        public String getMetricName()
        {
            return MetricRegistry.name(group, type, name, scope);
        }

        /**
         * Returns the scope of the {@link Metric}.
         *
         * @return the scope of the {@link Metric}
         */
        public String getScope()
        {
            return scope;
        }

        /**
         * Returns {@code true} if the {@link Metric} has a scope, {@code false} otherwise.
         *
         * @return {@code true} if the {@link Metric} has a scope
         */
        public boolean hasScope()
        {
            return scope != null;
        }

        /**
         * Returns the MBean name for the {@link Metric} identified by this metric name.
         *
         * @return the MBean name
         */
        public ObjectName getMBeanName()
        {

            String mname = mBeanName;

            if (mname == null)
                mname = getMetricName();

            try
            {

                return new ObjectName(mname);
            } catch (MalformedObjectNameException e)
            {
                try
                {
                    return new ObjectName(ObjectName.quote(mname));
                } catch (MalformedObjectNameException e1)
                {
                    throw new RuntimeException(e1);
                }
            }
        }

        public String getSystemViewName()
        {
            return systemViewName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            final MetricName that = (MetricName) o;
            return mBeanName.equals(that.mBeanName);
        }

        @Override
        public int hashCode()
        {
            return mBeanName.hashCode();
        }

        @Override
        public String toString()
        {
            return mBeanName;
        }

        @Override
        public int compareTo(MetricName o)
        {
            return mBeanName.compareTo(o.mBeanName);
        }

        private static String createMBeanName(String group, String type, String name, String scope)
        {
            final StringBuilder nameBuilder = new StringBuilder();
            nameBuilder.append(ObjectName.quote(group));
            nameBuilder.append(":type=");
            nameBuilder.append(ObjectName.quote(type));
            if (scope != null)
            {
                nameBuilder.append(",scope=");
                nameBuilder.append(ObjectName.quote(scope));
            }
            if (name.length() > 0)
            {
                nameBuilder.append(",name=");
                nameBuilder.append(ObjectName.quote(name));
            }
            return nameBuilder.toString();
        }

        /**
         * If the group is empty, use the package name of the given class. Otherwise use group
         *
         * @param group The group to use by default
         * @param klass The class being tracked
         * @return a group for the metric
         */
        public static String chooseGroup(String group, Class<?> klass)
        {
            if (group == null || group.isEmpty())
            {
                group = klass.getPackage() == null ? "" : klass.getPackage().getName();
            }
            return group;
        }

        /**
         * If the type is empty, use the simple name of the given class. Otherwise use type
         *
         * @param type  The type to use by default
         * @param klass The class being tracked
         * @return a type for the metric
         */
        public static String chooseType(String type, Class<?> klass)
        {
            if (type == null || type.isEmpty())
            {
                type = withoutFinalDollar(klass.getSimpleName());
            }
            return type;
        }

        /**
         * If name is empty, use the name of the given method. Otherwise use name
         *
         * @param name   The name to use by default
         * @param method The method being tracked
         * @return a name for the metric
         */
        public static String chooseName(String name, Method method)
        {
            if (name == null || name.isEmpty())
            {
                name = method.getName();
            }
            return name;
        }
    }
}
