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
import com.codahale.metrics.Histogram;
import org.apache.cassandra.cql3.statements.BatchStatement;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class BatchMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("Batch");

    public final Counter numLoggedBatches;
    public final Counter numUnloggedBatches;
    public final Counter numCounterBatches;

    public final Histogram partitionsPerLoggedBatch;
    public final Histogram partitionsPerUnloggedBatch;
    public final Histogram partitionsPerCounterBatch;

    public final Histogram columnsPerLoggedBatch;
    public final Histogram columnsPerUnloggedBatch;
    public final Histogram columnsPerCounterBatch;

    public BatchMetrics()
    {
        numLoggedBatches = Metrics.counter(factory.createMetricName("NumLoggedBatches"));
        numUnloggedBatches = Metrics.counter(factory.createMetricName("NumUnloggedBatches"));
        numCounterBatches = Metrics.counter(factory.createMetricName("NumCounterBatches"));

        partitionsPerLoggedBatch = Metrics.histogram(factory.createMetricName("PartitionsPerLoggedBatch"), false);
        partitionsPerUnloggedBatch = Metrics.histogram(factory.createMetricName("PartitionsPerUnloggedBatch"), false);
        partitionsPerCounterBatch = Metrics.histogram(factory.createMetricName("PartitionsPerCounterBatch"), false);

        columnsPerLoggedBatch = Metrics.histogram(factory.createMetricName("ColumnsPerLoggedBatch"), false);
        columnsPerUnloggedBatch = Metrics.histogram(factory.createMetricName("ColumnsPerUnloggedBatch"), false);
        columnsPerCounterBatch = Metrics.histogram(factory.createMetricName("ColumnsPerCounterBatch"), false);
    }

    public void update(BatchStatement.Type batchType, int updatedPartitions, int updatedColumns)
    {
        switch (batchType)
        {
            case LOGGED:
                numLoggedBatches.inc();
                partitionsPerLoggedBatch.update(updatedPartitions);
                columnsPerLoggedBatch.update(updatedColumns);
                break;
            case COUNTER:
                numCounterBatches.inc();
                partitionsPerCounterBatch.update(updatedPartitions);
                columnsPerCounterBatch.update(updatedColumns);
                break;
            case UNLOGGED:
                numUnloggedBatches.inc();
                partitionsPerUnloggedBatch.update(updatedPartitions);
                columnsPerUnloggedBatch.update(updatedColumns);
                break;
            default:
                throw new IllegalStateException("Unexpected batch type: " + batchType);
        }
    }
}
