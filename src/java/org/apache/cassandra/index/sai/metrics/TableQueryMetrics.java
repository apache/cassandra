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

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class TableQueryMetrics extends AbstractMetrics
{
    public static final String TABLE_QUERY_METRIC_TYPE = "TableQueryMetrics";

    private final PerQueryMetrics perQueryMetrics;
    private final Counter totalQueryTimeouts;
    private final Counter totalPartitionReads;
    private final Counter totalRowsFiltered;
    private final Counter totalQueriesCompleted;

    public TableQueryMetrics(TableMetadata table)
    {
        super(table.keyspace, table.name, TABLE_QUERY_METRIC_TYPE);

        perQueryMetrics = new PerQueryMetrics(table);

        totalPartitionReads = Metrics.counter(createMetricName("TotalPartitionReads"));
        totalRowsFiltered = Metrics.counter(createMetricName("TotalRowsFiltered"));
        totalQueriesCompleted = Metrics.counter(createMetricName("TotalQueriesCompleted"));
        totalQueryTimeouts = Metrics.counter(createMetricName("TotalQueryTimeouts"));
    }

    public void record(QueryContext queryContext)
    {
        if (queryContext.queryTimeouts > 0)
        {
            assert queryContext.queryTimeouts == 1;

            totalQueryTimeouts.inc();
        }

        perQueryMetrics.record(queryContext);
    }

    public void release()
    {
        super.release();
        perQueryMetrics.release();
    }

    public class PerQueryMetrics extends AbstractMetrics
    {
        private final Timer queryLatency;

        /**
         * Global metrics for all indexes hit during the query.
         */
        private final Histogram partitionReads;
        private final Histogram rowsFiltered;

        public PerQueryMetrics(TableMetadata table)
        {
            super(table.keyspace, table.name, "PerQuery");

            queryLatency = Metrics.timer(createMetricName("QueryLatency"));

            partitionReads = Metrics.histogram(createMetricName("PartitionReads"), false);
            rowsFiltered = Metrics.histogram(createMetricName("RowsFiltered"), false);
        }

        public void record(QueryContext queryContext)
        {
            final long totalQueryTimeNs = queryContext.totalQueryTimeNs();
            queryLatency.update(totalQueryTimeNs, TimeUnit.NANOSECONDS);
            final long queryLatencyMicros = TimeUnit.NANOSECONDS.toMicros(totalQueryTimeNs);

            final long partitionsRead = queryContext.partitionsRead;
            final long rowsFiltered = queryContext.rowsFiltered;

            partitionReads.update(partitionsRead);
            totalPartitionReads.inc(partitionsRead);

            this.rowsFiltered.update(rowsFiltered);
            totalRowsFiltered.inc(rowsFiltered);

            if (Tracing.isTracing())
            {
                Tracing.trace("Index query accessed memtable indexes and took {} microseconds.",
                              queryLatencyMicros);
            }

            totalQueriesCompleted.inc();
        }
    }
}
