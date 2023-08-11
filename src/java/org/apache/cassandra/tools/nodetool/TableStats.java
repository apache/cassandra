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

import java.util.*;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.stats.*;

@Command(name = "tablestats", description = "Print statistics on tables")
public class TableStats extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace.table>...]", description = "List of tables (or keyspace) names")
    private List<String> tableNames = new ArrayList<>();

    @Option(name = "-i", description = "Ignore the list of tables and display the remaining tables")
    private boolean ignore = false;

    @Option(title = "human_readable",
            name = {"-H", "--human-readable"},
            description = "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB")
    private boolean humanReadable = false;

    @Option(title = "format",
            name = {"-F", "--format"},
            description = "Output format (json, yaml)")
    private String outputFormat = "";

    @Option(title = "sort_key",
            name = {"-s", "--sort"},
            description = "Sort tables by specified sort key (average_live_cells_per_slice_last_five_minutes, "
                        + "average_tombstones_per_slice_last_five_minutes, bloom_filter_false_positives, "
                        + "bloom_filter_false_ratio, bloom_filter_off_heap_memory_used, bloom_filter_space_used, "
                        + "compacted_partition_maximum_bytes, compacted_partition_mean_bytes, "
                        + "compacted_partition_minimum_bytes, compression_metadata_off_heap_memory_used, "
                        + "full_name, index_summary_off_heap_memory_used, local_read_count, "
                        + "local_read_latency_ms, local_write_latency_ms, "
                        + "maximum_live_cells_per_slice_last_five_minutes, "
                        + "maximum_tombstones_per_slice_last_five_minutes, memtable_cell_count, memtable_data_size, "
                        + "memtable_off_heap_memory_used, memtable_switch_count, number_of_partitions_estimate, "
                        + "off_heap_memory_used_total, pending_flushes, percent_repaired, read_latency, reads, "
                        + "space_used_by_snapshots_total, space_used_live, space_used_total, "
                        + "sstable_compression_ratio, sstable_count, table_name, write_latency, writes, " +
                          "max_sstable_size, local_read_write_ratio, twcs_max_duration)")
    private String sortKey = "";

    @Option(title = "top",
            name = {"-t", "--top"},
            description = "Show only the top K tables for the sort key (specify the number K of tables to be shown")
    private int top = 0;

    @Option(title = "sstable_location_check",
            name = {"-l", "--sstable-location-check"},
            description = "Check whether or not the SSTables are in the correct location.")
    private boolean locationCheck = false;

    @Override
    public void execute(NodeProbe probe)
    {
        if (!outputFormat.isEmpty() && !"json".equals(outputFormat) && !"yaml".equals(outputFormat))
        {
            throw new IllegalArgumentException("arguments for -F are json,yaml only.");
        }

        if (!sortKey.isEmpty() && !Arrays.asList(StatsTableComparator.supportedSortKeys).contains(sortKey))
        {
            throw new IllegalArgumentException(String.format("argument for sort must be one of: %s",
                                               String.join(", ", StatsTableComparator.supportedSortKeys)));
        }

        if (top > 0 && sortKey.isEmpty())
        {
            throw new IllegalArgumentException("cannot filter top K tables without specifying a sort key.");
        }

        if (top < 0)
        {
            throw new IllegalArgumentException("argument for top must be a positive integer.");
        }

        StatsHolder holder = new TableStatsHolder(probe, humanReadable, ignore, tableNames, sortKey, top, locationCheck);
        // print out the keyspace and table statistics
        StatsPrinter printer = TableStatsPrinter.from(outputFormat, !sortKey.isEmpty());
        printer.print(holder, probe.output().out);
    }

}
