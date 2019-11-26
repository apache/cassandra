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

package org.apache.cassandra.tools.nodetool.stats;

import java.io.PrintStream;
import java.util.List;

public class TableStatsPrinter
{
    public static StatsPrinter from(String format, boolean sorted)
    {
        switch (format)
        {
            case "json":
                return new StatsPrinter.JsonPrinter();
            case "yaml":
                return new StatsPrinter.YamlPrinter();
            default:
                if (sorted)
                    return new SortedDefaultPrinter();
                else
                    return new DefaultPrinter();
        }
    }

    /**
     * A StatsPrinter to print stats in a keyspace-centric way, nesting stats for each table under their parent keyspaces.
     */
    private static class DefaultPrinter implements StatsPrinter<TableStatsHolder>
    {
        @Override
        public void print(TableStatsHolder data, PrintStream out)
        {
            out.println("Total number of tables: " + data.numberOfTables);
            out.println("----------------");

            List<StatsKeyspace> keyspaces = data.keyspaces;
            for (StatsKeyspace keyspace : keyspaces)
            {
                // print each keyspace's information
                out.println("Keyspace : " + keyspace.name);
                out.println("\tRead Count: " + keyspace.readCount);
                out.println("\tRead Latency: " + keyspace.readLatency() + " ms");
                out.println("\tWrite Count: " + keyspace.writeCount);
                out.println("\tWrite Latency: " + keyspace.writeLatency() + " ms");
                out.println("\tPending Flushes: " + keyspace.pendingFlushes);

                // print each table's information
                List<StatsTable> tables = keyspace.tables;
                for (StatsTable table : tables)
                {
                    printStatsTable(table, table.tableName, "\t\t", out);
                }
                out.println("----------------");
            }
        }

        protected void printStatsTable(StatsTable table, String tableDisplayName, String indent, PrintStream out)
        {
            out.println(indent + "Table" + (table.isIndex ? " (index): " : ": ") + tableDisplayName);
            out.println(indent + "SSTable count: " + table.sstableCount);
            out.println(indent + "Old SSTable count: " + table.oldSSTableCount);
            if (table.isLeveledSstable)
                out.println(indent + "SSTables in each level: [" + String.join(", ",
                                                                          table.sstablesInEachLevel) + "]");

            out.println(indent + "Space used (live): " + table.spaceUsedLive);
            out.println(indent + "Space used (total): " + table.spaceUsedTotal);
            out.println(indent + "Space used by snapshots (total): " + table.spaceUsedBySnapshotsTotal);

            if (table.offHeapUsed)
                out.println(indent + "Off heap memory used (total): " + table.offHeapMemoryUsedTotal);
            out.println(indent + "SSTable Compression Ratio: " + table.sstableCompressionRatio);
            out.println(indent + "Number of partitions (estimate): " + table.numberOfPartitionsEstimate);
            out.println(indent + "Memtable cell count: " + table.memtableCellCount);
            out.println(indent + "Memtable data size: " + table.memtableDataSize);

            if (table.memtableOffHeapUsed)
                out.println(indent + "Memtable off heap memory used: " + table.memtableOffHeapMemoryUsed);
            out.println(indent + "Memtable switch count: " + table.memtableSwitchCount);
            out.println(indent + "Local read count: " + table.localReadCount);
            out.printf(indent + "Local read latency: %01.3f ms%n", table.localReadLatencyMs);
            out.println(indent + "Local write count: " + table.localWriteCount);
            out.printf(indent + "Local write latency: %01.3f ms%n", table.localWriteLatencyMs);
            out.println(indent + "Pending flushes: " + table.pendingFlushes);
            out.println(indent + "Percent repaired: " + table.percentRepaired);

            out.println(indent + "Bloom filter false positives: " + table.bloomFilterFalsePositives);
            out.printf(indent + "Bloom filter false ratio: %01.5f%n", table.bloomFilterFalseRatio);
            out.println(indent + "Bloom filter space used: " + table.bloomFilterSpaceUsed);

            if (table.bloomFilterOffHeapUsed)
                out.println(indent + "Bloom filter off heap memory used: " + table.bloomFilterOffHeapMemoryUsed);
            if (table.indexSummaryOffHeapUsed)
                out.println(indent + "Index summary off heap memory used: " + table.indexSummaryOffHeapMemoryUsed);
            if (table.compressionMetadataOffHeapUsed)
                out.println(indent + "Compression metadata off heap memory used: " + table.compressionMetadataOffHeapMemoryUsed);

            out.println(indent + "Compacted partition minimum bytes: " + table.compactedPartitionMinimumBytes);
            out.println(indent + "Compacted partition maximum bytes: " + table.compactedPartitionMaximumBytes);
            out.println(indent + "Compacted partition mean bytes: " + table.compactedPartitionMeanBytes);
            out.println(indent + "Average live cells per slice (last five minutes): " + table.averageLiveCellsPerSliceLastFiveMinutes);
            out.println(indent + "Maximum live cells per slice (last five minutes): " + table.maximumLiveCellsPerSliceLastFiveMinutes);
            out.println(indent + "Average tombstones per slice (last five minutes): " + table.averageTombstonesPerSliceLastFiveMinutes);
            out.println(indent + "Maximum tombstones per slice (last five minutes): " + table.maximumTombstonesPerSliceLastFiveMinutes);
            out.println(indent + "Dropped Mutations: " + table.droppedMutations);
            out.println("");
        }
    }

    /**
     * A StatsPrinter to print stats in a sorted, table-centric way.
     */
    private static class SortedDefaultPrinter extends DefaultPrinter
    {
        @Override
        public void print(TableStatsHolder data, PrintStream out)
        {
            List<StatsTable> tables = data.getSortedFilteredTables();
            String totalTablesSummary = String.format("Total number of tables: %d", data.numberOfTables);
            if (data.top > 0)
            {
                int k = (data.top <= data.numberOfTables) ? data.top : data.numberOfTables;
                totalTablesSummary += String.format(" (showing top %d by %s)", k, data.sortKey);
            }
            out.println(totalTablesSummary);
            out.println("----------------");
            for (StatsTable table : tables)
            {
                printStatsTable(table, table.keyspaceName + "." + table.tableName, "\t", out);
            }
            out.println("----------------");
        }
    }
}
