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

package org.apache.cassandra.db.virtual;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.metrics.BatchMetrics;
import org.apache.cassandra.schema.TableMetadata;

public class BatchMetricsTable extends AbstractVirtualTable
{

    private static final String PARTITIONS_PER_LOGGED_BATCH = "partitions_per_logged_batch";
    private static final String PARTITIONS_PER_UNLOGGED_BATCH = "partitions_per_unlogged_batch";
    private static final String PARTITIONS_PER_COUNTER_BATCH = "partitions_per_counter_batch";
    private final static String P50 = "p50th";
    private final static String P99 = "p99th";
    private final static String P999 = "p999th";
    private final static String MAX = "max";

    BatchMetricsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "batch_metrics")
                           .comment("Metrics specific to batch statements")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn("name", UTF8Type.instance)
                           .addRegularColumn(P50, DoubleType.instance)
                           .addRegularColumn(P99, DoubleType.instance)
                           .addRegularColumn(P999, DoubleType.instance)
                           .addRegularColumn(MAX, LongType.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        BatchMetrics metrics = BatchStatement.metrics;
        addRow(result, PARTITIONS_PER_LOGGED_BATCH, metrics.partitionsPerLoggedBatch.getSnapshot());
        addRow(result, PARTITIONS_PER_UNLOGGED_BATCH, metrics.partitionsPerUnloggedBatch.getSnapshot());
        addRow(result, PARTITIONS_PER_COUNTER_BATCH, metrics.partitionsPerCounterBatch.getSnapshot());

        return result;
    }

    private void addRow(SimpleDataSet dataSet, String name, Snapshot snapshot)
    {
        dataSet.row(name)
               .column(P50, snapshot.getMedian())
               .column(P99, snapshot.get99thPercentile())
               .column(P999, snapshot.get999thPercentile())
               .column(MAX, snapshot.getMax());
    }
}
