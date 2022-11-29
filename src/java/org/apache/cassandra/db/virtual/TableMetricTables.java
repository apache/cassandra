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

import java.math.BigDecimal;
import java.util.Collection;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.util.Precision;

import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Sampling;
import com.codahale.metrics.Snapshot;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.LongType;
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
    private final static String P50 = "p50th";
    private final static String P99 = "p99th";
    private final static String MAX = "max";
    private final static String RATE = "per_second";
    private final static double BYTES_TO_MIB = 1.0 / (1024 * 1024);
    private final static double NS_TO_MS = 0.000001;

    private final static AbstractType<?> TYPE = CompositeType.getInstance(UTF8Type.instance,
                                                                          UTF8Type.instance);
    private final static IPartitioner PARTITIONER = new LocalPartitioner(TYPE);

    /**
     * Generates all table metric tables in a collection
     */
    public static Collection<VirtualTable> getAll(String name)
    {
        return ImmutableList.of(
            new LatencyTableMetric(name, "local_read_latency", t -> t.readLatency.latency),
            new LatencyTableMetric(name, "local_scan_latency", t -> t.rangeLatency.latency),
            new LatencyTableMetric(name, "coordinator_read_latency", t -> t.coordinatorReadLatency),
            new LatencyTableMetric(name, "coordinator_scan_latency", t -> t.coordinatorScanLatency),
            new LatencyTableMetric(name, "local_write_latency", t -> t.writeLatency.latency),
            new LatencyTableMetric(name, "coordinator_write_latency", t -> t.coordinatorWriteLatency),
            new HistogramTableMetric(name, "tombstones_per_read", t -> t.tombstoneScannedHistogram.cf),
            new HistogramTableMetric(name, "rows_per_read", t -> t.liveScannedHistogram.cf),
            new StorageTableMetric(name, "disk_usage", (TableMetrics t) -> t.totalDiskSpaceUsed),
            new StorageTableMetric(name, "max_partition_size", (TableMetrics t) -> t.maxPartitionSize),
            new StorageTableMetric(name, "max_sstable_size", (TableMetrics t) -> t.maxSSTableSize),
            new TableMetricTable(name, "max_sstable_duration", t -> t.maxSSTableDuration, "max_sstable_duration", LongType.instance, ""));
    }

    /**
     * A table that describes a some amount of disk on space in a Counter or Gauge
     */
    private static class StorageTableMetric extends TableMetricTable
    {
        interface GaugeFunction extends Function<TableMetrics, Gauge<Long>> {}
        interface CountingFunction<M extends Metric & Counting> extends Function<TableMetrics, M> {}

        <M extends Metric & Counting> StorageTableMetric(String keyspace, String table, CountingFunction<M> func)
        {
            super(keyspace, table, func, "mebibytes", LongType.instance, "");
        }

        StorageTableMetric(String keyspace, String table, GaugeFunction func)
        {
            super(keyspace, table, func, "mebibytes", LongType.instance, "");
        }

        /**
         * Convert bytes to mebibytes, always round up to nearest MiB
         */
        public void add(SimpleDataSet result, String column, long value)
        {
            result.column(column, (long) Math.ceil(value * BYTES_TO_MIB));
        }
    }

    /**
     * A table that describes a Latency metric, specifically a Timer
     */
    private static class HistogramTableMetric extends TableMetricTable
    {
        <M extends Metric & Sampling> HistogramTableMetric(String keyspace, String table, Function<TableMetrics, M> func)
        {
            this(keyspace, table, func, "");
        }

        <M extends Metric & Sampling> HistogramTableMetric(String keyspace, String table, Function<TableMetrics, M> func, String suffix)
        {
            super(keyspace, table, func, "count", LongType.instance, suffix);
        }

        /**
         * When displaying in cqlsh if we allow doubles to be too precise we get scientific notation which is hard to
         * read so round off at 0.000.
         */
        public void add(SimpleDataSet result, String column, double value)
        {
            result.column(column, Precision.round(value, 3, BigDecimal.ROUND_HALF_UP));
        }
    }

    /**
     * A table that describes a Latency metric, specifically a Timer
     */
    private static class LatencyTableMetric extends HistogramTableMetric
    {
        <M extends Metric & Sampling> LatencyTableMetric(String keyspace, String table, Function<TableMetrics, M> func)
        {
            super(keyspace, table, func, "_ms");
        }

        /**
         * For the metrics that are time based, convert to to milliseconds
         */
        public void add(SimpleDataSet result, String column, double value)
        {
            if (column.endsWith(suffix))
                value *= NS_TO_MS;

            super.add(result, column, value);
        }
    }

    /**
     * Abstraction over the Metrics Gauge, Counter, and Timer that will turn it into a (keyspace_name, table_name)
     * table.
     */
    private static class TableMetricTable extends AbstractVirtualTable
    {
        final Function<TableMetrics, ? extends Metric> func;
        final String columnName;
        final String suffix;

        TableMetricTable(String keyspace, String table, Function<TableMetrics, ? extends Metric> func,
                                String colName, AbstractType colType, String suffix)
        {
            super(buildMetadata(keyspace, table, func, colName, colType, suffix));
            this.func = func;
            this.columnName = colName;
            this.suffix = suffix;
        }

        public void add(SimpleDataSet result, String column, double value)
        {
            result.column(column, value);
        }

        public void add(SimpleDataSet result, String column, long value)
        {
            result.column(column,  value);
        }

        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());

            // Iterate over all tables and get metric by function
            for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            {
                Metric metric = func.apply(cfs.metric);

                // set new partition for this table
                result.row(cfs.getKeyspaceName(), cfs.name);

                // extract information by metric type and put it in row based on implementation of `add`
                if (metric instanceof Counting)
                {
                    add(result, columnName, ((Counting) metric).getCount());
                    if (metric instanceof Sampling)
                    {
                        Sampling histo = (Sampling) metric;
                        Snapshot snapshot = histo.getSnapshot();
                        // EstimatedHistogram keeping them in ns is hard to parse as a human so convert to ms
                        add(result, P50 + suffix, snapshot.getMedian());
                        add(result, P99 + suffix, snapshot.get99thPercentile());
                        add(result, MAX + suffix, (double) snapshot.getMax());
                    }
                    if (metric instanceof Metered)
                    {
                        Metered timer = (Metered) metric;
                        add(result, RATE, timer.getFiveMinuteRate());
                    }
                }
                else if (metric instanceof Gauge)
                {
                    add(result, columnName, (long) ((Gauge) metric).getValue());
                }
            }
            return result;
        }
    }

    /**
     *  Identify the type of Metric it is (gauge, counter etc) abd create the TableMetadata. The column name
     *  and type for a counter/gauge is formatted differently based on the units (bytes/time) so allowed to
     *  be set.
     */
    private static TableMetadata buildMetadata(String keyspace, String table, Function<TableMetrics, ? extends Metric> func,
                                              String colName, AbstractType colType, String suffix)
    {
        TableMetadata.Builder metadata = TableMetadata.builder(keyspace, table)
                                                      .kind(TableMetadata.Kind.VIRTUAL)
                                                      .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                                                      .addPartitionKeyColumn(TABLE_NAME, UTF8Type.instance)
                                                      .partitioner(PARTITIONER);

        // get a table from system keyspace and get metric from it for determining type of metric
        Keyspace system = Keyspace.system().iterator().next();
        Metric test = func.apply(system.getColumnFamilyStores().iterator().next().metric);

        if (test instanceof Counting)
        {
            metadata.addRegularColumn(colName, colType);
            // if it has a Histogram include some information about distribution
            if (test instanceof Sampling)
            {
                metadata.addRegularColumn(P50 + suffix, DoubleType.instance)
                        .addRegularColumn(P99 + suffix, DoubleType.instance)
                        .addRegularColumn(MAX + suffix, DoubleType.instance);
            }
            if (test instanceof Metered)
            {
                metadata.addRegularColumn(RATE, DoubleType.instance);
            }
        }
        else if (test instanceof Gauge)
        {
            metadata.addRegularColumn(colName, colType);
        }
        return metadata.build();
    }
}
