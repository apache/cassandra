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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.airlift.airline.Command;
import org.apache.cassandra.db.virtual.CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable;
import org.apache.cassandra.db.virtual.CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

/**
 * Nodetool command to view stats related to CIDR filtering
 */
@Command(name = "cidrfilteringstats", description = "Print statistics on CIDR filtering")
public class CIDRFilteringStats extends NodeToolCmd
{
    private void printCountsMetrics(NodeProbe probe, PrintStream out)
    {
        out.println(CIDRFilteringMetricsCountsTable.TABLE_NAME + ": ");
        TableBuilder outputTable = new TableBuilder();
        outputTable.add(CIDRFilteringMetricsCountsTable.NAME_COL,
                        CIDRFilteringMetricsCountsTable.VALUE_COL);

        Map<String, Long> metricRows = probe.getCountsMetricsFromVtable();

        for (Map.Entry<String, Long> row : metricRows.entrySet())
        {
            outputTable.add(row.getKey(), Long.toString(row.getValue()));
        }

        outputTable.printTo(out);
        out.println();
    }

    private void printLatenciesMetrics(NodeProbe probe, PrintStream out)
    {
        out.println(CIDRFilteringMetricsLatenciesTable.TABLE_NAME + ": ");
        TableBuilder outputTable = new TableBuilder();
        outputTable.add(CIDRFilteringMetricsLatenciesTable.NAME_COL,
                        CIDRFilteringMetricsLatenciesTable.P50_COL,
                        CIDRFilteringMetricsLatenciesTable.P95_COL,
                        CIDRFilteringMetricsLatenciesTable.P99_COL,
                        CIDRFilteringMetricsLatenciesTable.P999_COL,
                        CIDRFilteringMetricsLatenciesTable.MAX_COL);

        Map<String, List<Double>> metricRows = probe.getLatenciesMetricsFromVtable();

        for (Map.Entry<String, List<Double>> row : metricRows.entrySet())
        {
            List<String> values = new ArrayList<>();
            values.add(row.getKey());
            for (Double col : row.getValue())
            {
                values.add(Double.toString(col));
            }
            outputTable.add(values);
        }

        outputTable.printTo(out);
        out.println();
    }

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;

        // Read rows from corresponding virtual tables and print
        printCountsMetrics(probe, out);
        printLatenciesMetrics(probe, out);
    }
}
