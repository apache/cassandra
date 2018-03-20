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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.InMemoryVirtualTable;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.metrics.LatencyMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Sampling;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableSet;

public class TableStats extends InMemoryVirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(TableStats.class);

    private static final String COUNT = "count";
    private static final String MEDIAN = "median";
    private static final String STD_DEV = "std_dev";
    private static final String MEAN = "mean";
    private static final String MAX = "max";
    private static final String MIN = "min";
    private static final String P999TH = "p999th";
    private static final String P99TH = "p99th";
    private static final String P95TH = "p95th";
    private static final String P75TH = "p75th";
    private static final String ONE_MIN_RATE = "one_min_rate";
    private static final String MEAN_RATE = "mean_rate";
    private static final String FIVE_MIN_RATE = "five_min_rate";
    private static final String FIFTEEN_MIN_RATE = "fifteen_min_rate";
    private static final String VALUE = "value";
    private static final String METRIC = "metric";
    private static final String TABLE_NAME = "table_name";
    private static final String KEYSPACE_NAME = "keyspace_name";

    private static final Field[] FIELDS = org.apache.cassandra.metrics.TableMetrics.class.getFields();
    static
    {
        Arrays.sort(FIELDS, Comparator.comparing(Field::getName));

        Map<String, CQL3Type> definitions = new HashMap<>();
        definitions.put(KEYSPACE_NAME, CQL3Type.Native.TEXT);
        definitions.put(TABLE_NAME, CQL3Type.Native.TEXT);
        definitions.put(METRIC, CQL3Type.Native.TEXT);
        definitions.put(VALUE, CQL3Type.Native.TEXT);
        definitions.put(FIFTEEN_MIN_RATE, CQL3Type.Native.DOUBLE);
        definitions.put(FIVE_MIN_RATE, CQL3Type.Native.DOUBLE);
        definitions.put(MEAN_RATE, CQL3Type.Native.DOUBLE);
        definitions.put(ONE_MIN_RATE, CQL3Type.Native.DOUBLE);
        definitions.put(P75TH, CQL3Type.Native.DOUBLE);
        definitions.put(P95TH, CQL3Type.Native.DOUBLE);
        definitions.put(P99TH, CQL3Type.Native.DOUBLE);
        definitions.put(P999TH, CQL3Type.Native.DOUBLE);
        definitions.put(MIN, CQL3Type.Native.BIGINT);
        definitions.put(MAX, CQL3Type.Native.BIGINT);
        definitions.put(MEAN, CQL3Type.Native.DOUBLE);
        definitions.put(STD_DEV, CQL3Type.Native.DOUBLE);
        definitions.put(MEDIAN, CQL3Type.Native.DOUBLE);
        definitions.put(COUNT, CQL3Type.Native.BIGINT);

        schemaBuilder(definitions)
                .addKey(KEYSPACE_NAME)
                .addKey(TABLE_NAME)
                .addClustering(METRIC)
                .register();
    }

    private static final Collection<String> EH_GAUGES = ImmutableSet.of(
            "estimatedPartitionSizeHistogram",
            "estimatedColumnCountHistogram");

    private CompositeType keyType;

    public TableStats(TableMetadata metadata)
    {
        super(metadata);
        keyType = (CompositeType) metadata.partitionKeyType;
    }

    private void add(ColumnFamilyStore cfs, ResultBuilder result)
    {
        for (Field f : FIELDS)
        {
            try
            {
                Object metric = f.get(cfs.metric);
                if (!Modifier.isStatic(f.getModifiers()) && (metric instanceof Metric || metric instanceof LatencyMetrics))
                {
                    PartitionBuilder partition = result.row(cfs.keyspace.getName(), cfs.name, f.getName());
                    if (EH_GAUGES.contains(f.getName()))
                    {
                        long[] data = ((Gauge<long[]>) metric).getValue();
                        if (data.length > 0)
                        {
                            EstimatedHistogram eh = new EstimatedHistogram(data);
                            partition.column(MIN, eh.min());
                            partition.column(MAX, eh.max());
                            partition.column(MEAN, (double) eh.mean());
                            partition.column(MEDIAN, (double) eh.percentile(0.5));
                            partition.column(P75TH, (double) eh.percentile(0.75));
                            partition.column(P95TH, (double) eh.percentile(0.95));
                            partition.column(P99TH, (double) eh.percentile(0.99));
                            partition.column(P999TH, (double) eh.percentile(0.999));
                        }
                    }
                    else if (metric instanceof LatencyMetrics)
                    {
                        metric = ((LatencyMetrics) metric).latency;
                    }
                    else if (metric instanceof Gauge)
                        partition.column(VALUE, ((Gauge) metric).getValue().toString());
                    if (metric instanceof Counting)
                        partition.column(COUNT, ((Counting) metric).getCount());
                    if (metric instanceof Timer)
                    {
                        Timer t = (Timer) metric;
                        partition.column(FIFTEEN_MIN_RATE, t.getFifteenMinuteRate());
                        partition.column(FIVE_MIN_RATE, t.getFiveMinuteRate());
                        partition.column(ONE_MIN_RATE, t.getOneMinuteRate());
                        partition.column(MEAN_RATE, t.getMeanRate());
                    }
                    if (metric instanceof Sampling)
                    {
                        Snapshot s = ((Sampling) metric).getSnapshot();
                        partition.column(MIN, s.getMin());
                        partition.column(MAX, s.getMax());
                        partition.column(MEAN, s.getMean());
                        partition.column(MEDIAN, s.getMedian());
                        partition.column(STD_DEV, s.getStdDev());
                        partition.column(P75TH, s.get75thPercentile());
                        partition.column(P95TH, s.get95thPercentile());
                        partition.column(P99TH, s.get99thPercentile());
                        partition.column(P999TH, s.get999thPercentile());
                    }
                    partition.endRow();
                }
            }
            catch (IllegalArgumentException | IllegalAccessException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public void read(StatementRestrictions restrictions, QueryOptions options, ResultBuilder result)
    {
        if (!restrictions.getPartitionKeyRestrictions().hasUnrestrictedPartitionKeyComponents(metadata))
        {
            List<ByteBuffer> keys = restrictions.getPartitionKeys(options);
            for (ByteBuffer key : keys)
            {
                ByteBuffer[] bb = keyType.split(key);
                String keyspace = keyType.types.get(0).getString(bb[0]);
                String table = keyType.types.get(1).getString(bb[1]);
                add(ColumnFamilyStore.getIfExists(keyspace, table), result);
            }
        }
        else
        {
            for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            {
                add(cfs, result);
            }
        }
    }

}
