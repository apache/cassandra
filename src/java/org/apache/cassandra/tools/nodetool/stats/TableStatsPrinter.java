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
    public static StatsPrinter from(String format)
    {
        switch (format)
        {
            case "json":
                return new StatsPrinter.JsonPrinter();
            case "yaml":
                return new StatsPrinter.YamlPrinter();
            default:
                return new DefaultPrinter();
        }
    }

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
                    out.println("\t\tTable" + (table.isIndex ? " (index): " + table.name : ": ") + table.name);
                    out.println("\t\tSSTable count: " + table.sstableCount);
                    if (table.isLeveledSstable)
                        out.println("\t\tSSTables in each level: [" + String.join(", ",
                                                                                  table.sstablesInEachLevel) + "]");

                    out.println("\t\tSpace used (live): " + table.spaceUsedLive);
                    out.println("\t\tSpace used (total): " + table.spaceUsedTotal);
                    out.println("\t\tSpace used by snapshots (total): " + table.spaceUsedBySnapshotsTotal);

                    if (table.offHeapUsed)
                        out.println("\t\tOff heap memory used (total): " + table.offHeapMemoryUsedTotal);
                    out.println("\t\tSSTable Compression Ratio: " + table.sstableCompressionRatio);
                    out.println("\t\tNumber of partitions (estimate): " + table.numberOfPartitionsEstimate);
                    out.println("\t\tMemtable cell count: " + table.memtableCellCount);
                    out.println("\t\tMemtable data size: " + table.memtableDataSize);

                    if (table.memtableOffHeapUsed)
                        out.println("\t\tMemtable off heap memory used: " + table.memtableOffHeapMemoryUsed);
                    out.println("\t\tMemtable switch count: " + table.memtableSwitchCount);
                    out.println("\t\tLocal read count: " + table.localReadCount);
                    out.printf("\t\tLocal read latency: %01.3f ms%n", table.localReadLatencyMs);
                    out.println("\t\tLocal write count: " + table.localWriteCount);
                    out.printf("\t\tLocal write latency: %01.3f ms%n", table.localWriteLatencyMs);
                    out.println("\t\tPending flushes: " + table.pendingFlushes);
                    out.println("\t\tPercent repaired: " + table.percentRepaired);

                    out.println("\t\tBloom filter false positives: " + table.bloomFilterFalsePositives);
                    out.printf("\t\tBloom filter false ratio: %01.5f%n", table.bloomFilterFalseRatio);
                    out.println("\t\tBloom filter space used: " + table.bloomFilterSpaceUsed);

                    if (table.bloomFilterOffHeapUsed)
                        out.println("\t\tBloom filter off heap memory used: " + table.bloomFilterOffHeapMemoryUsed);
                    if (table.indexSummaryOffHeapUsed)
                        out.println("\t\tIndex summary off heap memory used: " + table.indexSummaryOffHeapMemoryUsed);
                    if (table.compressionMetadataOffHeapUsed)
                        out.println("\t\tCompression metadata off heap memory used: " + table.compressionMetadataOffHeapMemoryUsed);

                    out.println("\t\tCompacted partition minimum bytes: " + table.compactedPartitionMinimumBytes);
                    out.println("\t\tCompacted partition maximum bytes: " + table.compactedPartitionMaximumBytes);
                    out.println("\t\tCompacted partition mean bytes: " + table.compactedPartitionMeanBytes);
                    out.println("\t\tAverage live cells per slice (last five minutes): " + table.averageLiveCellsPerSliceLastFiveMinutes);
                    out.println("\t\tMaximum live cells per slice (last five minutes): " + table.maximumLiveCellsPerSliceLastFiveMinutes);
                    out.println("\t\tAverage tombstones per slice (last five minutes): " + table.averageTombstonesPerSliceLastFiveMinutes);
                    out.println("\t\tMaximum tombstones per slice (last five minutes): " + table.maximumTombstonesPerSliceLastFiveMinutes);
                    out.println("\t\tDropped Mutations: " + table.droppedMutations);
                    out.println("");
                }
                out.println("----------------");
            }
        }
    }
}
