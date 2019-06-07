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

import java.util.Collection;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Sampling;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Contains multiple the Table Metric virtual tables. This is not a direct wrapper over the Metrics like with JMX but a
 * view to the metrics so that the underlying mechanism can change but still give same appearance (like nodetool).
 */
public class TableMetricTables
{
    private final static String KEYSPACE_NAME = "keyspace_name";
    private final static String TABLE_NAME = "table_name";
    private final static String MEDIAN = "median";
    private final static String P99 = "99th";
    private final static String MAX = "max";
    private final static String RATE = "per_second";

    private final static AbstractType<?> TYPE = CompositeType.getInstance(ReversedType.getInstance(LongType.instance),
                                                                          UTF8Type.instance,
                                                                          UTF8Type.instance);
    private final static IPartitioner PARTITIONER = new LocalPartitioner(TYPE);

    /**
     * Generates all table metric tables in a collection
     */
    public static Collection<VirtualTable> getAll(String name)
    {
        return ImmutableList.of(
        getMetricTable(name, "local_reads", t -> t.readLatency.latency),
        getMetricTable(name, "local_scans", t -> t.rangeLatency.latency),
        getMetricTable(name, "coordinator_reads", t -> t.coordinatorReadLatency),
        getMetricTable(name, "coordinator_scans", t -> t.coordinatorScanLatency),
        getMetricTable(name, "local_writes", t -> t.writeLatency.latency),
        getMetricTable(name, "coordinator_writes", t -> t.coordinatorWriteLatency),
        getMetricTable(name, "tombstones_scanned", t -> t.tombstoneScannedHistogram.cf),
        getMetricTable(name, "live_scanned", t -> t.liveScannedHistogram.cf),
        getMetricTable(name, "disk_usage", t -> t.totalDiskSpaceUsed, "disk_space"),
        getMetricTable(name, "max_partition_size", t -> t.maxPartitionSize, "max_partition_size"));
    }

    public static VirtualTable getMetricTable(String keyspace, String table, Function<TableMetrics, Metric> func)
    {
        return getMetricTable(keyspace, table, func, "count");
    }

    /**
     * Abstraction over the Metrics Gauge, Counter, and Timer that will turn it into a ([pk], keyspace_name, table_name)
     * table. The primary key (default 'count') is in descending orde in order to visually sort the rows when selecting
     * the entire table in CQLSH.
     */
    public static VirtualTable getMetricTable(String keyspace, String table, Function<TableMetrics, Metric> func, String pk)
    {
        TableMetadata.Builder metadata = TableMetadata.builder(keyspace, table)
                                                      .kind(TableMetadata.Kind.VIRTUAL)
                                                      .addPartitionKeyColumn(pk, ReversedType.getInstance(LongType.instance))
                                                      .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                                                      .addPartitionKeyColumn(TABLE_NAME, UTF8Type.instance)
                                                      .partitioner(PARTITIONER);

        Keyspace system = Keyspace.system().iterator().next();

        // Identify the type of Metric it is (gauge, counter etc) and verify the types work
        Metric test = func.apply(system.getColumnFamilyStores().iterator().next().metric);
        if(test instanceof Counting)
        {
            if (test instanceof Sampling)
            {
                metadata.addRegularColumn(MEDIAN, LongType.instance)
                        .addRegularColumn(P99, LongType.instance)
                        .addRegularColumn(MAX, LongType.instance);
            }
            if (test instanceof Metered)
            {
                metadata.addRegularColumn(RATE, DoubleType.instance);
            }
        }
        else if (test instanceof Gauge)
        {
            Preconditions.checkArgument(((Gauge) test).getValue().getClass().isAssignableFrom(Long.class));
        }

        // Create the VirtualTable that will walk through all tables and get the Metric for each to build the tables
        // SimpleDataSet
        return new AbstractVirtualTable(metadata.build())
        {
            public DataSet data()
            {
                SimpleDataSet result = new SimpleDataSet(metadata());
                for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
                {
                    Metric metric = func.apply(cfs.metric);

                    if(metric instanceof Counting)
                    {
                        Counting counting = (Counting) metric;
                        result.row(counting.getCount(), cfs.keyspace.getName(), cfs.name);
                        if (metric instanceof Sampling)
                        {
                            Sampling histo = (Sampling) metric;
                            Snapshot snapshot = histo.getSnapshot();
                            result.column(MEDIAN, (long) snapshot.getMedian())
                                  .column(P99, (long) snapshot.get99thPercentile())
                                  .column(MAX, (long) snapshot.getMax());
                        }
                        if (metric instanceof Metered)
                        {
                            Metered timer = (Metered) metric;
                            result.column(RATE, timer.getFiveMinuteRate());
                        }
                    }
                    else if (metric instanceof Gauge)
                    {
                        result.row(((Gauge) metric).getValue(), cfs.keyspace.getName(), cfs.name);
                    }
                    else if (metric instanceof Counter)
                    {
                        result.row(((Counter) metric).getCount(), cfs.keyspace.getName(), cfs.name);
                    }
                }
                return result;
            }
        };
    }
}
