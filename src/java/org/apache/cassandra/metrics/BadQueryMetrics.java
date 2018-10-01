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
import com.codahale.metrics.Gauge;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.monitoring.BadQuery;
import org.apache.cassandra.repair.AutoRepair;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics related to BadQuery.
 */
public class BadQueryMetrics
{
    private static MetricNameFactory factory;

    public static Gauge<Integer> slowLocalReadCount;
    public static Gauge<Integer> slowCoordReadCount;
    public static Gauge<Integer> slowLocalWriteCount;
    public static Gauge<Integer> slowCoordWriteCount;
    public static Gauge<Integer> largePartitionReadCount;
    public static Gauge<Integer> largePartitionWriteCount;
    public static Gauge<Integer> incorrectCompactionCount;
    public static Gauge<Integer> incorrectCLCount;
    public static Gauge<Integer> tooManyTombstoneCount;

    public static void setup()
    {
        factory = new BadQueryTypeFactory();
        slowLocalReadCount = Metrics.register(factory.createMetricName("slowLocalReadCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return DatabaseDescriptor.getBadQueryReporter().getStats(BadQuery.BadQueryCategory.SLOW_READ_LOCAL);
            }
        });
        slowCoordReadCount = Metrics.register(factory.createMetricName("slowCoordReadCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return DatabaseDescriptor.getBadQueryReporter().getStats(BadQuery.BadQueryCategory.SLOW_READ_COORINATOR);
            }
        });
        slowLocalWriteCount = Metrics.register(factory.createMetricName("slowLocalWriteCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return DatabaseDescriptor.getBadQueryReporter().getStats(BadQuery.BadQueryCategory.SLOW_WRITE_LOCAL);
            }
        });
        slowCoordWriteCount = Metrics.register(factory.createMetricName("slowCoordWriteCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return DatabaseDescriptor.getBadQueryReporter().getStats(BadQuery.BadQueryCategory.SLOW_WRITE_COORINATOR);
            }
        });
        largePartitionReadCount = Metrics.register(factory.createMetricName("largePartitionReadCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return DatabaseDescriptor.getBadQueryReporter().getStats(BadQuery.BadQueryCategory.LARGE_PARTITION_READ);
            }
        });
        largePartitionWriteCount = Metrics.register(factory.createMetricName("largePartitionWriteCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return DatabaseDescriptor.getBadQueryReporter().getStats(BadQuery.BadQueryCategory.LARGE_PARTITION_WRITE);
            }
        });
        incorrectCompactionCount = Metrics.register(factory.createMetricName("incorrectCompactionCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return DatabaseDescriptor.getBadQueryReporter().getStats(BadQuery.BadQueryCategory.INCORRECT_COMPACTION_STRATEBY);
            }
        });
        incorrectCLCount = Metrics.register(factory.createMetricName("incorrectCLCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return DatabaseDescriptor.getBadQueryReporter().getStats(BadQuery.BadQueryCategory.INCORRECT_CONSISTENCY_LEVEL);
            }
        });
        tooManyTombstoneCount = Metrics.register(factory.createMetricName("tooManyTombstoneCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return DatabaseDescriptor.getBadQueryReporter().getStats(BadQuery.BadQueryCategory.TOO_MANY_TOMBSTONES);
            }
        });
    }

    static class BadQueryTypeFactory implements MetricNameFactory
    {
        public CassandraMetricsRegistry.MetricName createMetricName(String type)
        {
            String groupName = TableMetrics.class.getPackage().getName();

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=BadQuery");
            mbeanName.append(",badquerytype=").append(type);
            mbeanName.append(",name=Count");

            return new CassandraMetricsRegistry.MetricName(groupName, "BadQuery", "Count", type, mbeanName.toString());
        }
    }
}
