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

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.*;
import javax.management.*;

/**
 * Makes integrating 3.0 metrics API with 2.0.
 * <p/>
 * The 3.0 API comes with poor JMX integration
 */
public class CassandraMetricsRegistry extends MetricRegistry
{
    protected static final Logger logger = LoggerFactory.getLogger(CassandraMetricsRegistry.class);

    public static final CassandraMetricsRegistry Metrics = new CassandraMetricsRegistry();
    private MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    private CassandraMetricsRegistry()
    {
        super();
    }

    public Counter counter(MetricName name)
    {
        Counter counter = counter(name.getMetricName());
        registerMBean(counter, name.getMBeanName());

        return counter;
    }

    public Meter meter(MetricName name)
    {
        Meter meter = meter(name.getMetricName());
        registerMBean(meter, name.getMBeanName());

        return meter;
    }

    public Histogram histogram(MetricName name)
    {
        Histogram histogram = register(name, new ClearableHistogram(new EstimatedHistogramReservoir()));
        registerMBean(histogram, name.getMBeanName());

        return histogram;
    }

    public Timer timer(MetricName name)
    {
        Timer timer = register(name, new Timer(new EstimatedHistogramReservoir()));
        registerMBean(timer, name.getMBeanName());

        return timer;
    }

    public <T extends Metric> T register(MetricName name, T metric)
    {
        try
        {
            register(name.getMetricName(), metric);
            registerMBean(metric, name.getMBeanName());
            return metric;
        }
        catch (IllegalArgumentException e)
        {
            Metric existing = Metrics.getMetrics().get(name.getMetricName());
            return (T)existing;
        }
    }

    public boolean remove(MetricName name)
    {
        boolean removed = remove(name.getMetricName());

        try
        {
            mBeanServer.unregisterMBean(name.getMBeanName());
        } catch (InstanceNotFoundException | MBeanRegistrationException e)
        {
            logger.debug("Unable to remove mbean");
        }

        return removed;
    }

    private void registerMBean(Metric metric, ObjectName name)
    {
        AbstractBean mbean;

        if (metric instanceof Gauge)
        {
            mbean = new JmxGauge((Gauge<?>) metric, name);
        } else if (metric instanceof Counter)
        {
            mbean = new JmxCounter((Counter) metric, name);
        } else if (metric instanceof Histogram)
        {
            mbean = new JmxHistogram((Histogram) metric, name);
        } else if (metric instanceof Meter)
        {
            mbean = new JmxMeter((Meter) metric, name, TimeUnit.SECONDS);
        } else if (metric instanceof Timer)
        {
            mbean = new JmxTimer((Timer) metric, name, TimeUnit.SECONDS, TimeUnit.MICROSECONDS);
        } else
        {
            throw new IllegalArgumentException("Unknown metric type: " + metric.getClass());
        }

        try
        {
            mBeanServer.registerMBean(mbean, name);
        } catch (InstanceAlreadyExistsException e)
        {
            logger.debug("Metric bean already exists", e);
        } catch (MBeanRegistrationException e)
        {
            logger.debug("Unable to register metric bean", e);
        } catch (NotCompliantMBeanException e)
        {
            logger.warn("Unable to register metric bean", e);
        }
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
    }

    private static class JmxHistogram extends AbstractBean implements JmxHistogramMBean
    {
        private final Histogram metric;

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
    }

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

    public interface JmxMeterMBean extends MetricMBean
    {
        long getCount();

        double getMeanRate();

        double getOneMinuteRate();

        double getFiveMinuteRate();

        double getFifteenMinuteRate();

        String getRateUnit();
    }

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

        String getDurationUnit();
    }

    static class JmxTimer extends JmxMeter implements JmxTimerMBean
    {
        private final Timer metric;
        private final double durationFactor;
        private final String durationUnit;

        private JmxTimer(Timer metric,
                         ObjectName objectName,
                         TimeUnit rateUnit,
                         TimeUnit durationUnit)
        {
            super(metric, objectName, rateUnit);
            this.metric = metric;
            this.durationFactor = 1.0 / durationUnit.toNanos(1);
            this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
        }

        @Override
        public double get50thPercentile()
        {
            return metric.getSnapshot().getMedian() * durationFactor;
        }

        @Override
        public double getMin()
        {
            return metric.getSnapshot().getMin() * durationFactor;
        }

        @Override
        public double getMax()
        {
            return metric.getSnapshot().getMax() * durationFactor;
        }

        @Override
        public double getMean()
        {
            return metric.getSnapshot().getMean() * durationFactor;
        }

        @Override
        public double getStdDev()
        {
            return metric.getSnapshot().getStdDev() * durationFactor;
        }

        @Override
        public double get75thPercentile()
        {
            return metric.getSnapshot().get75thPercentile() * durationFactor;
        }

        @Override
        public double get95thPercentile()
        {
            return metric.getSnapshot().get95thPercentile() * durationFactor;
        }

        @Override
        public double get98thPercentile()
        {
            return metric.getSnapshot().get98thPercentile() * durationFactor;
        }

        @Override
        public double get99thPercentile()
        {
            return metric.getSnapshot().get99thPercentile() * durationFactor;
        }

        @Override
        public double get999thPercentile()
        {
            return metric.getSnapshot().get999thPercentile() * durationFactor;
        }

        @Override
        public long[] values()
        {
            return metric.getSnapshot().getValues();
        }

        @Override
        public String getDurationUnit()
        {
            return durationUnit;
        }
    }

    /**
     * A value class encapsulating a metric's owning class and name.
     */
    public static class MetricName implements Comparable<MetricName>
    {
        private final String group;
        private final String type;
        private final String name;
        private final String scope;
        private final String mBeanName;

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
                    klass.getSimpleName().replaceAll("\\$$", ""),
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

        /**
         * Creates a new {@link MetricName} without a scope.
         *
         * @param group     the group to which the {@link Metric} belongs
         * @param type      the type to which the {@link Metric} belongs
         * @param name      the name of the {@link Metric}
         * @param scope     the scope of the {@link Metric}
         * @param mBeanName the 'ObjectName', represented as a string, to use when registering the
         *                  MBean.
         */
        public MetricName(String group, String type, String name, String scope, String mBeanName)
        {
            if (group == null || type == null)
            {
                throw new IllegalArgumentException("Both group and type need to be specified");
            }
            if (name == null)
            {
                throw new IllegalArgumentException("Name needs to be specified");
            }
            this.group = group;
            this.type = type;
            this.name = name;
            this.scope = scope;
            this.mBeanName = mBeanName;
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
                type = klass.getSimpleName().replaceAll("\\$$", "");
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


