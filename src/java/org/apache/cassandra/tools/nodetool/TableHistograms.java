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
import io.airlift.command.Arguments;
import io.airlift.command.Command;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.commons.lang3.ArrayUtils;

@Command(name = "tablehistograms", description = "Print statistic histograms for a given table")
public class TableHistograms extends NodeToolCmd
{
    @Arguments(usage = "<keyspace> <table> | <keyspace.table>", description = "The keyspace and table name")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        String keyspace = null, table = null;
        if (args.size() == 2)
        {
            keyspace = args.get(0);
            table = args.get(1);
        }
        else if (args.size() == 1)
        {
            String[] input = args.get(0).split("\\.");
            checkArgument(input.length == 2, "tablehistograms requires keyspace and table name arguments");
            keyspace = input[0];
            table = input[1];
        }
        else
        {
            checkArgument(false, "tablehistograms requires keyspace and table name arguments");
        }

        // calculate percentile of row size and column count
        long[] estimatedPartitionSize = (long[]) probe.getColumnFamilyMetric(keyspace, table, "EstimatedPartitionSizeHistogram");
        long[] estimatedColumnCount = (long[]) probe.getColumnFamilyMetric(keyspace, table, "EstimatedColumnCountHistogram");

        // build arrays to store percentile values
        double[] estimatedRowSizePercentiles = new double[7];
        double[] estimatedColumnCountPercentiles = new double[7];
        double[] offsetPercentiles = new double[]{0.5, 0.75, 0.95, 0.98, 0.99};

        if (ArrayUtils.isEmpty(estimatedPartitionSize) || ArrayUtils.isEmpty(estimatedColumnCount))
        {
            System.err.println("No SSTables exists, unable to calculate 'Partition Size' and 'Cell Count' percentiles");

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
                System.err.println(String.format("Row sizes are larger than %s, unable to calculate percentiles", partitionSizeHist.getLargestBucketOffset()));
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
                System.err.println(String.format("Column counts are larger than %s, unable to calculate percentiles", columnCountHist.getLargestBucketOffset()));
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
        double[] readLatency = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspace, table, "ReadLatency"));
        double[] writeLatency = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspace, table, "WriteLatency"));
        double[] sstablesPerRead = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxHistogramMBean) probe.getColumnFamilyMetric(keyspace, table, "SSTablesPerReadHistogram"));

        System.out.println(format("%s/%s histograms", keyspace, table));
        System.out.println(format("%-10s%10s%18s%18s%18s%18s",
                "Percentile", "SSTables", "Write Latency", "Read Latency", "Partition Size", "Cell Count"));
        System.out.println(format("%-10s%10s%18s%18s%18s%18s",
                "", "", "(micros)", "(micros)", "(bytes)", ""));

        for (int i = 0; i < percentiles.length; i++)
        {
            System.out.println(format("%-10s%10.2f%18.2f%18.2f%18.0f%18.0f",
                    percentiles[i],
                    sstablesPerRead[i],
                    writeLatency[i],
                    readLatency[i],
                    estimatedRowSizePercentiles[i],
                    estimatedColumnCountPercentiles[i]));
        }
        System.out.println();
    }
}
