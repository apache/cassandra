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
package org.apache.cassandra.tools.nodetool;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.utils.EstimatedHistogram;

import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.ArrayUtils;

@Command(name = "tablehistograms", description = "Print statistic histograms for a given table")
public class TableHistograms extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace> <table> | <keyspace.table>]", description = "The keyspace and table name")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        Multimap<String, String> tablesList = HashMultimap.create();

        // a <keyspace, set<table>> mapping for verification or as reference if none provided
        Multimap<String, String> allTables = HashMultimap.create();
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> tableMBeans = probe.getColumnFamilyStoreMBeanProxies();
        while (tableMBeans.hasNext())
        {
            Map.Entry<String, ColumnFamilyStoreMBean> entry = tableMBeans.next();
            allTables.put(entry.getKey(), entry.getValue().getTableName());
        }

        if (args.size() == 2 && args.stream().noneMatch(arg -> arg.contains(".")))
        {
            tablesList.put(args.get(0), args.get(1));
        }
        else if (args.size() == 1)
        {
            Pair<String, String> ksTbPair = parseTheKsTbPair(args.get(0));
            tablesList.put(ksTbPair.left, ksTbPair.right);
        }
        else if (args.size() == 0)
        {
            // use all tables
            tablesList = allTables;
        }
        else
        {
            throw new IllegalArgumentException("tablehistograms requires <keyspace> <table> or <keyspace.table> format argument.");
        }

        // verify that all tables to list exist
        for (String keyspace : tablesList.keys())
        {
            for (String table : tablesList.get(keyspace))
            {
                if (!allTables.containsEntry(keyspace, table))
                    throw new IllegalArgumentException("Unknown table " + keyspace + '.' + table);
            }
        }

        for (String keyspace : tablesList.keys().elementSet())
        {
            for (String table : tablesList.get(keyspace))
            {
                // calculate percentile of row size and column count
                long[] estimatedPartitionSize = (long[]) probe.getColumnFamilyMetric(keyspace, table, "EstimatedPartitionSizeHistogram");
                long[] estimatedColumnCount = (long[]) probe.getColumnFamilyMetric(keyspace, table, "EstimatedColumnCountHistogram");

                // build arrays to store percentile values
                double[] estimatedRowSizePercentiles = new double[7];
                double[] estimatedColumnCountPercentiles = new double[7];
                double[] offsetPercentiles = new double[]{0.5, 0.75, 0.95, 0.98, 0.99};

                if (ArrayUtils.isEmpty(estimatedPartitionSize) || ArrayUtils.isEmpty(estimatedColumnCount))
                {
                    out.println("No SSTables exists, unable to calculate 'Partition Size' and 'Cell Count' percentiles");

                    for (int i = 0; i < 7; i++)
                    {
                        estimatedRowSizePercentiles[i] = Double.NaN;
                        estimatedColumnCountPercentiles[i] = Double.NaN;
                    }
                }
                else
                {
                    EstimatedHistogram partitionSizeHist = new EstimatedHistogram(estimatedPartitionSize);
                    EstimatedHistogram columnCountHist = new EstimatedHistogram(estimatedColumnCount);

                    if (partitionSizeHist.isOverflowed())
                    {
                        out.println(String.format("Row sizes are larger than %s, unable to calculate percentiles", partitionSizeHist.getLargestBucketOffset()));
                        for (int i = 0; i < offsetPercentiles.length; i++)
                            estimatedRowSizePercentiles[i] = Double.NaN;
                    }
                    else
                    {
                        for (int i = 0; i < offsetPercentiles.length; i++)
                            estimatedRowSizePercentiles[i] = partitionSizeHist.percentile(offsetPercentiles[i]);
                    }

                    if (columnCountHist.isOverflowed())
                    {
                        out.println(String.format("Column counts are larger than %s, unable to calculate percentiles", columnCountHist.getLargestBucketOffset()));
                        for (int i = 0; i < estimatedColumnCountPercentiles.length; i++)
                            estimatedColumnCountPercentiles[i] = Double.NaN;
                    }
                    else
                    {
                        for (int i = 0; i < offsetPercentiles.length; i++)
                            estimatedColumnCountPercentiles[i] = columnCountHist.percentile(offsetPercentiles[i]);
                    }

                    // min value
                    estimatedRowSizePercentiles[5] = partitionSizeHist.min();
                    estimatedColumnCountPercentiles[5] = columnCountHist.min();
                    // max value
                    estimatedRowSizePercentiles[6] = partitionSizeHist.max();
                    estimatedColumnCountPercentiles[6] = columnCountHist.max();
                }

                String[] percentiles = new String[]{"50%", "75%", "95%", "98%", "99%", "Min", "Max"};
                Double[] readLatency = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspace, table, "ReadLatency"));
                Double[] writeLatency = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspace, table, "WriteLatency"));
                Double[] sstablesPerRead = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxHistogramMBean) probe.getColumnFamilyMetric(keyspace, table, "SSTablesPerReadHistogram"));

                out.println(format("%s/%s histograms", keyspace, table));
                out.println(format("%-10s%18s%18s%18s%18s%18s",
                        "Percentile", "Read Latency", "Write Latency", "SSTables", "Partition Size", "Cell Count"));
                out.println(format("%-10s%18s%18s%18s%18s%18s",
                        "", "(micros)", "(micros)", "", "(bytes)", ""));

                for (int i = 0; i < percentiles.length; i++)
                {
                    out.println(format("%-10s%18.2f%18.2f%18.2f%18.0f%18.0f",
                            percentiles[i],
                            readLatency[i],
                            writeLatency[i],
                            sstablesPerRead[i],
                            estimatedRowSizePercentiles[i],
                            estimatedColumnCountPercentiles[i]));
                }
                out.println();
            }
        }
    }

    private Pair<String, String> parseTheKsTbPair(String ksAndTb)
    {
        String[] input = args.get(0).split("\\.");
        checkArgument(input.length == 2, "tablehistograms requires keyspace and table name arguments");
        return Pair.create(input[0], input[1]);
    }
}
