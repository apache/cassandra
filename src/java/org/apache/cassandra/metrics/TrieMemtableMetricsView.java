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

import com.codahale.metrics.Counter;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class TrieMemtableMetricsView
{
    private static final String UNCONTENDED_PUTS = "Uncontended memtable puts";
    private static final String CONTENDED_PUTS = "Contended memtable puts";
    private static final String CONTENTION_TIME = "Contention time";
    private static final String LAST_FLUSH_SHARD_SIZES = "Shard sizes during last flush";

    // the number of memtable puts that did not need to wait on write lock
    public final Counter uncontendedPuts;

    // the number of memtable puts that needed to wait on write lock
    public final Counter contendedPuts;

    // shard put contention measurements
    public final LatencyMetrics contentionTime;

    // shard sizes distribution
    public final MinMaxAvgMetric lastFlushShardDataSizes;

    private final TrieMemtableMetricNameFactory factory;

    public TrieMemtableMetricsView(String keyspace, String table)
    {
        factory = new TrieMemtableMetricNameFactory(keyspace, table);
        
        uncontendedPuts = Metrics.counter(factory.createMetricName(UNCONTENDED_PUTS));
        contendedPuts = Metrics.counter(factory.createMetricName(CONTENDED_PUTS));
        contentionTime = new LatencyMetrics(factory, CONTENTION_TIME);
        lastFlushShardDataSizes = new MinMaxAvgMetric(factory, LAST_FLUSH_SHARD_SIZES);
    }

    public void release()
    {
        Metrics.remove(factory.createMetricName(UNCONTENDED_PUTS));
        Metrics.remove(factory.createMetricName(CONTENDED_PUTS));
        contentionTime.release();
        lastFlushShardDataSizes.release();
    }

    static class TrieMemtableMetricNameFactory implements MetricNameFactory
    {
        private final String keyspace;
        private final String table;

        TrieMemtableMetricNameFactory(String keyspace, String table)
        {
            this.keyspace = keyspace;
            this.table = table;
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            String groupName = TableMetrics.class.getPackage().getName();
            String type = "TrieMemtable";

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=").append(type);
            mbeanName.append(",keyspace=").append(keyspace);
            mbeanName.append(",scope=").append(table);
            mbeanName.append(",name=").append(metricName);

            return new CassandraMetricsRegistry.MetricName(groupName, type, metricName, keyspace + "." + table, mbeanName.toString());
        }
    }
}
