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

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.management.JMX;
import javax.management.ObjectName;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter;

import static java.util.Objects.requireNonNull;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.METRICS_GROUP_POSTFIX;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_METRICS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test compares JMX metrics to virtual table metrics values, basically all metric values must be equal and
 * have the same representation in both places.
 */
public class JmxVirtualTableMetricsTest extends CQLTester
{
    private final Map<MetricType, Metric> metricToNameMap = new EnumMap<>(MetricType.class);
    private final AtomicInteger gaugeValue = new AtomicInteger(123);

    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        createMBeanServerConnection();
        addMetricsKeyspace();
    }

    @Before
    public void beforeTest()
    {
        metricToNameMap.clear();
        MetricRegistry registry = new MetricRegistry();

        metricToNameMap.put(MetricType.METER, registry.meter("meter"));
        metricToNameMap.put(MetricType.COUNTER, registry.counter("counter"));
        metricToNameMap.put(MetricType.HISTOGRAM, registry.histogram("histogram"));
        metricToNameMap.put(MetricType.TIMER, registry.timer("timer"));
        metricToNameMap.put(MetricType.GAUGE, registry.gauge("gauge", () -> gaugeValue::get));

        CassandraMetricsRegistry.metricGroups.forEach(group -> {
            MetricNameFactory factory = new DefaultNameFactory(group, "jmx.virtual");
            CassandraMetricsRegistry.Metrics.register(factory.createMetricName(MetricType.METER.metricName),
                                                      metricToNameMap.get(MetricType.METER));
            CassandraMetricsRegistry.Metrics.register(factory.createMetricName(MetricType.COUNTER.metricName),
                                                      metricToNameMap.get(MetricType.COUNTER));
            CassandraMetricsRegistry.Metrics.register(factory.createMetricName(MetricType.HISTOGRAM.metricName),
                                                      metricToNameMap.get(MetricType.HISTOGRAM));
            CassandraMetricsRegistry.Metrics.register(factory.createMetricName(MetricType.TIMER.metricName),
                                                      metricToNameMap.get(MetricType.TIMER));
            CassandraMetricsRegistry.Metrics.register(factory.createMetricName(MetricType.GAUGE.metricName),
                                                      metricToNameMap.get(MetricType.GAUGE));
        });
    }

    @Test
    public void testJmxEqualVirtualTableByMetricGroup() throws Exception
    {
        Map<String, List<ObjectName>> mbeanByMetricGroup = jmxConnection.queryNames(null, null)
                                                                        .stream()
                                                                        .filter(this::isLocalMetric)
                                                                        .collect(Collectors.groupingBy(
                                                                            on -> requireNonNull(
                                                                                on.getKeyPropertyList().get("type"))));

        for (Map.Entry<String, List<ObjectName>> e : mbeanByMetricGroup.entrySet())
        {
            assertRowsContains(executeNet(String.format("SELECT * FROM %s.%s", VIRTUAL_METRICS,
                                                        METRICS_GROUP_POSTFIX.apply(
                                                            CollectionVirtualTableAdapter.virtualTableNameStyle(
                                                                e.getKey())))),
                               e.getValue().stream().map(this::makeMetricRow).collect(Collectors.toList()));
        }
    }

    @Test
    public void testJmxEqualVirtualTableByMetricType() throws Exception
    {
        Map<MetricType, List<ObjectName>> mbeanByMetricGroup = jmxConnection.queryNames(null, null)
                                                                            .stream()
                                                                            .filter(this::isLocalMetric)
                                                                            .collect(Collectors.groupingBy(
                                                                                on -> MetricType.find(
                                                                                                    on.getKeyPropertyList().get("name"))
                                                                                                .orElseThrow()));

        for (Map.Entry<MetricType, List<ObjectName>> e : mbeanByMetricGroup.entrySet())
        {
            switch (e.getKey())
            {
                case METER:
                    assertRowsContains(executeNet(String.format("SELECT * FROM %s.type_meter", VIRTUAL_METRICS)),
                                       e.getValue().stream().map(this::makeMeterRow).collect(Collectors.toList()));
                    break;
                case COUNTER:
                    assertRowsContains(executeNet(String.format("SELECT * FROM %s.type_counter", VIRTUAL_METRICS)),
                                       e.getValue().stream().map(this::makeCounterRow).collect(Collectors.toList()));
                    break;
                case HISTOGRAM:
                    assertRowsContains(executeNet(String.format("SELECT * FROM %s.type_histogram", VIRTUAL_METRICS)),
                                       e.getValue().stream().map(this::makeHistogramRow).collect(Collectors.toList()));
                    break;
                case TIMER:
                    assertRowsContains(executeNet(String.format("SELECT * FROM %s.type_timer", VIRTUAL_METRICS)),
                                       e.getValue().stream().map(this::makeTimerRow).collect(Collectors.toList()));
                    break;
                case GAUGE:
                    assertRowsContains(executeNet(String.format("SELECT * FROM %s.type_gauge", VIRTUAL_METRICS)),
                                       e.getValue().stream().map(this::makeGaugeRow).collect(Collectors.toList()));
                    break;
            }
        }
    }

    @Test
    public void testAliasesWithJmxVirtualTables() throws Exception
    {
        CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
        MetricNameFactory factory = new DefaultNameFactory("Table", "AliasTestScopeFirst");
        MetricNameFactory aliasFactory = new DefaultNameFactory("Index", "AliasTestScopeSecond");

        CassandraMetricsRegistry.MetricName meter = factory.createMetricName("TestMeter");
        CassandraMetricsRegistry.MetricName meterAlias = aliasFactory.createMetricName("TestMeterAlias");
        registry.meter(meter, meterAlias);

        assertRowsContains(executeNet(String.format("SELECT * FROM %s.type_meter", VIRTUAL_METRICS)),
                           row(meter.getMetricName(), 0L, 0.0, 0.0, 0.0, 0.0, "AliasTestScopeFirst"),
                           row(meterAlias.getMetricName(), 0L, 0.0, 0.0, 0.0, 0.0, "AliasTestScopeSecond"));

        CassandraMetricsRegistry.JmxMeterMBean bean = JMX.newMBeanProxy(jmxConnection,
                                                                        meter.getMBeanName(),
                                                                        CassandraMetricsRegistry.JmxMeterMBean.class);

        CassandraMetricsRegistry.JmxMeterMBean beanAlias = JMX.newMBeanProxy(jmxConnection,
                                                                             meterAlias.getMBeanName(),
                                                                             CassandraMetricsRegistry.JmxMeterMBean.class);

        assertNotNull(bean);
        assertNotNull(beanAlias);
        assertEquals(bean.getCount(), beanAlias.getCount());
        assertEquals(bean.getFifteenMinuteRate(), beanAlias.getFifteenMinuteRate(), 0.0);
        assertEquals(bean.getFiveMinuteRate(), beanAlias.getFiveMinuteRate(), 0.0);
        assertEquals(bean.getMeanRate(), beanAlias.getMeanRate(), 0.0);
        assertEquals(bean.getOneMinuteRate(), beanAlias.getOneMinuteRate(), 0.0);
    }

    private Object[] makeMetricRow(ObjectName objectName)
    {
        MetricType type = MetricType.find(objectName.getKeyPropertyList().get("name")).orElseThrow();
        String jmxValue;
        switch (type)
        {
            case METER:
                jmxValue = String.valueOf(JMX.newMBeanProxy(jmxConnection,
                                                            objectName,
                                                            CassandraMetricsRegistry.JmxMeterMBean.class).getCount());
                break;
            case COUNTER:
                jmxValue = String.valueOf(JMX.newMBeanProxy(jmxConnection,
                                                            objectName,
                                                            CassandraMetricsRegistry.JmxCounterMBean.class).getCount());
                break;
            case HISTOGRAM:
                jmxValue = String.valueOf(JMX.newMBeanProxy(jmxConnection,
                                                            objectName,
                                                            CassandraMetricsRegistry.JmxHistogramMBean.class).get50thPercentile());
                break;
            case TIMER:
                jmxValue = String.valueOf(JMX.newMBeanProxy(jmxConnection,
                                                            objectName,
                                                            CassandraMetricsRegistry.JmxTimerMBean.class).getCount());
                break;
            case GAUGE:
                jmxValue = String.valueOf(JMX.newMBeanProxy(jmxConnection,
                                                            objectName,
                                                            CassandraMetricsRegistry.JmxGaugeMBean.class).getValue());
                break;
            default:
                throw new RuntimeException("Unknown metric type: " + objectName.getKeyPropertyList().get("name"));
        }
        return CQLTester.row(getFullMetricName(objectName),
                             requireNonNull(objectName.getKeyPropertyList().get("scope")),
                             MetricType.find(objectName.getKeyPropertyList().get("name"))
                                       .map(MetricType::name)
                                       .map(String::toLowerCase)
                                       .orElseThrow(),
                             jmxValue);
    }

    private Object[] makeMeterRow(ObjectName objectName)
    {
        assertEquals(MetricType.METER.metricName, objectName.getKeyPropertyList().get("name"));
        CassandraMetricsRegistry.JmxMeterMBean bean = JMX.newMBeanProxy(jmxConnection,
                                                                        objectName,
                                                                        CassandraMetricsRegistry.JmxMeterMBean.class);

        return CQLTester.row(getFullMetricName(objectName),
                             bean.getCount(),
                             bean.getFifteenMinuteRate(),
                             bean.getFiveMinuteRate(),
                             bean.getMeanRate(),
                             bean.getOneMinuteRate(),
                             requireNonNull(objectName.getKeyPropertyList().get("scope")));
    }

    private Object[] makeCounterRow(ObjectName objectName)
    {
        assertEquals(MetricType.COUNTER.metricName, objectName.getKeyPropertyList().get("name"));
        CassandraMetricsRegistry.JmxCounterMBean bean = JMX.newMBeanProxy(jmxConnection,
                                                                          objectName,
                                                                          CassandraMetricsRegistry.JmxCounterMBean.class);

        return CQLTester.row(getFullMetricName(objectName),
                             requireNonNull(objectName.getKeyPropertyList().get("scope")),
                             bean.getCount());
    }

    private Object[] makeHistogramRow(ObjectName objectName)
    {
        assertEquals(MetricType.HISTOGRAM.metricName, objectName.getKeyPropertyList().get("name"));
        CassandraMetricsRegistry.JmxHistogramMBean bean = JMX.newMBeanProxy(jmxConnection,
                                                                            objectName,
                                                                            CassandraMetricsRegistry.JmxHistogramMBean.class);

        return CQLTester.row(getFullMetricName(objectName),
                             bean.getMax(),
                             bean.getMean(),
                             bean.getMin(),
                             bean.get75thPercentile(),
                             bean.get95thPercentile(),
                             bean.get98thPercentile(),
                             bean.get999thPercentile(),
                             bean.get99thPercentile(),
                             requireNonNull(objectName.getKeyPropertyList().get("scope")));
    }

    private Object[] makeTimerRow(ObjectName objectName)
    {
        assertEquals(MetricType.TIMER.metricName, objectName.getKeyPropertyList().get("name"));
        CassandraMetricsRegistry.JmxTimerMBean bean = JMX.newMBeanProxy(jmxConnection,
                                                                        objectName,
                                                                        CassandraMetricsRegistry.JmxTimerMBean.class);

        return CQLTester.row(getFullMetricName(objectName),
                             bean.getCount(),
                             bean.getFifteenMinuteRate(),
                             bean.getFiveMinuteRate(),
                             bean.getMeanRate(),
                             bean.getOneMinuteRate(),
                             requireNonNull(objectName.getKeyPropertyList().get("scope")));
    }

    private Object[] makeGaugeRow(ObjectName objectName)
    {
        assertEquals(MetricType.GAUGE.metricName, objectName.getKeyPropertyList().get("name"));
        String jmxValue = String.valueOf(JMX.newMBeanProxy(jmxConnection,
                                                           objectName,
                                                           CassandraMetricsRegistry.JmxGaugeMBean.class).getValue());

        return CQLTester.row(getFullMetricName(objectName),
                             requireNonNull(objectName.getKeyPropertyList().get("scope")),
                             jmxValue);
    }

    private boolean isLocalMetric(ObjectName mBean)
    {
        MetricType type = MetricType.find(mBean.getKeyPropertyList().get("name")).orElse(null);
        return mBean.toString().startsWith(DefaultNameFactory.GROUP_NAME) && metricToNameMap.containsKey(type);
    }

    private static String getFullMetricName(ObjectName objectName)
    {
        Map<String, String> props = objectName.getKeyPropertyList();
        return new CassandraMetricsRegistry.MetricName(DefaultNameFactory.GROUP_NAME, requireNonNull(props.get("type")),
                                                       requireNonNull(props.get("name")),
                                                       requireNonNull(props.get("scope"))).getMetricName();
    }

    private enum MetricType
    {
        METER("MeterTestMetric"),
        COUNTER("CounterTestMetric"),
        HISTOGRAM("HistogramTestMetric"),
        TIMER("TimerTestMetric"),
        GAUGE("GaugeTestMetric");

        private final String metricName;

        MetricType(String metricName)
        {
            this.metricName = metricName;
        }

        public static Optional<MetricType> find(String metricName)
        {
            for (MetricType type : values())
                if (type.metricName.equals(metricName))
                    return Optional.of(type);

            return Optional.empty();
        }
    }
}
