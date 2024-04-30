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

import static com.codahale.metrics.MetricRegistry.name;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.resolveShortMetricName;
import static org.apache.cassandra.metrics.DefaultNameFactory.GROUP_NAME;

/**
 * Metrics for TrieMemtable, the metrics are shared across all memtables in a single column family and
 * are updated by all memtables in the column family.
 */
public class TrieMemtableMetricsView
{
    public static final String TYPE_NAME = "TrieMemtable";
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

    public TrieMemtableMetricsView(String keyspace, String table)
    {
        MetricNameFactory factory = new TrieMemtableMetricNameFactory(keyspace, table);
        
        uncontendedPuts = Metrics.counter(factory.createMetricName(UNCONTENDED_PUTS));
        contendedPuts = Metrics.counter(factory.createMetricName(CONTENDED_PUTS));
        contentionTime = new LatencyMetrics(factory, CONTENTION_TIME);
        lastFlushShardDataSizes = new MinMaxAvgMetric(factory, LAST_FLUSH_SHARD_SIZES);
    }

    public static void release(String keyspace, String table)
    {
        TrieMemtableMetricNameFactory factory = new TrieMemtableMetricNameFactory(keyspace, table);
        Metrics.removeIfMatch(fullName -> resolveShortMetricName(fullName, GROUP_NAME, TYPE_NAME, factory.scope()),
                              factory::createMetricName,
                              m -> {});
    }

    private static class TrieMemtableMetricNameFactory implements MetricNameFactory
    {
        private final String keyspace;
        private final String table;

        TrieMemtableMetricNameFactory(String keyspace, String table)
        {
            this.keyspace = keyspace;
            this.table = table;
        }

        public String scope()
        {
            return name(keyspace, table);
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            assert metricName.indexOf('.') == -1 : "metricName should not contain '.'; got " + metricName;
            return new CassandraMetricsRegistry.MetricName(GROUP_NAME, TYPE_NAME, metricName, scope(),
                                                           GROUP_NAME + ':' +
                                                           "type=" + TYPE_NAME +
                                                           ",keyspace=" + keyspace +
                                                           ",scope=" + table +
                                                           ",name=" + metricName);
        }
    }
}
