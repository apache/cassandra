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
package org.apache.cassandra.index.sai.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.RatioGauge;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class TableStateMetrics extends AbstractMetrics
{
    public static final String TABLE_STATE_METRIC_TYPE = "TableStateMetrics";

    public TableStateMetrics(TableMetadata table, StorageAttachedIndexGroup group)
    {
        super(table.keyspace, table.name, TABLE_STATE_METRIC_TYPE);
        Metrics.register(createMetricName("DiskUsedBytes"), (Gauge<Long>) group::totalDiskUsage);
        Metrics.register(createMetricName("DiskPercentageOfBaseTable"), (Gauge<Double>) new RatioGauge() {
            @Override
            protected Ratio getRatio()
            {
                return Ratio.of(group.totalDiskUsage(), group.table().metric.liveDiskSpaceUsed.getCount());
            }
        });
        Metrics.register(createMetricName("TotalIndexCount"), (Gauge<Integer>) group::totalIndexCount);
        Metrics.register(createMetricName("TotalQueryableIndexCount"), (Gauge<Integer>) group::totalQueryableIndexCount);
        Metrics.register(createMetricName("TotalIndexBuildsInProgress"), (Gauge<Integer>) group::totalIndexBuildsInProgress);
    }
}
