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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatsHolder
{
    public List<StatsKeyspace> keyspaces;

    public StatsHolder()
    {
        keyspaces = new ArrayList<>();
    }

    public Map<String, HashMap<String, Object>> convert2Map()
    {
        HashMap<String, HashMap<String, Object>> mpRet = new HashMap<>();
        for (StatsKeyspace keyspace : keyspaces)
        {
            // store each keyspace's metrics to map
            HashMap<String, Object> mpKeyspace = new HashMap<>();
            mpKeyspace.put("read_latency", keyspace.readLatency());
            mpKeyspace.put("read_count", keyspace.readCount);
            mpKeyspace.put("read_latency_ms", keyspace.readLatency());
            mpKeyspace.put("write_count", keyspace.writeCount);
            mpKeyspace.put("write_latency_ms", keyspace.writeLatency());
            mpKeyspace.put("pending_flushes", keyspace.pendingFlushes);

            // store each table's metrics to map
            List<StatsTable> tables = keyspace.tables;
            Map<String, Map<String, Object>> mpTables = new HashMap<>();
            for (StatsTable table : tables)
            {
                Map<String, Object> mpTable = new HashMap<>();

                mpTable.put("sstables_in_each_level", table.sstablesInEachLevel);
                mpTable.put("space_used_live", table.spaceUsedLive);
                mpTable.put("space_used_total", table.spaceUsedTotal);
                mpTable.put("space_used_by_snapshots_total", table.spaceUsedBySnapshotsTotal);
                if (table.offHeapUsed)
                    mpTable.put("off_heap_memory_used_total", table.offHeapMemoryUsedTotal);
                mpTable.put("sstable_compression_ratio", table.sstableCompressionRatio);
                mpTable.put("number_of_keys_estimate", table.numberOfKeysEstimate);
                mpTable.put("memtable_cell_count", table.memtableCellCount);
                mpTable.put("memtable_data_size", table.memtableDataSize);
                if (table.memtableOffHeapUsed)
                    mpTable.put("memtable_off_heap_memory_used", table.memtableOffHeapMemoryUsed);
                mpTable.put("memtable_switch_count", table.memtableSwitchCount);
                mpTable.put("local_read_count", table.localReadCount);
                mpTable.put("local_read_latency_ms", String.format("%01.3f", table.localReadLatencyMs));
                mpTable.put("local_write_count", table.localWriteCount);
                mpTable.put("local_write_latency_ms", String.format("%01.3f", table.localWriteLatencyMs));
                mpTable.put("pending_flushes", table.pendingFlushes);
                mpTable.put("percent_repaired", table.percentRepaired);
                mpTable.put("bloom_filter_false_positives", table.bloomFilterFalsePositives);
                mpTable.put("bloom_filter_false_ratio", String.format("%01.5f", table.bloomFilterFalseRatio));
                mpTable.put("bloom_filter_space_used", table.bloomFilterSpaceUsed);
                if (table.bloomFilterOffHeapUsed)
                    mpTable.put("bloom_filter_off_heap_memory_used", table.bloomFilterOffHeapMemoryUsed);
                if (table.indexSummaryOffHeapUsed)
                    mpTable.put("index_summary_off_heap_memory_used", table.indexSummaryOffHeapMemoryUsed);
                if (table.compressionMetadataOffHeapUsed)
                    mpTable.put("compression_metadata_off_heap_memory_used",
                                table.compressionMetadataOffHeapMemoryUsed);
                mpTable.put("compacted_partition_minimum_bytes", table.compactedPartitionMinimumBytes);
                mpTable.put("compacted_partition_maximum_bytes", table.compactedPartitionMaximumBytes);
                mpTable.put("compacted_partition_mean_bytes", table.compactedPartitionMeanBytes);
                mpTable.put("average_live_cells_per_slice_last_five_minutes",
                            table.averageLiveCellsPerSliceLastFiveMinutes);
                mpTable.put("maximum_live_cells_per_slice_last_five_minutes",
                            table.maximumLiveCellsPerSliceLastFiveMinutes);
                mpTable.put("average_tombstones_per_slice_last_five_minutes",
                            table.averageTombstonesPerSliceLastFiveMinutes);
                mpTable.put("maximum_tombstones_per_slice_last_five_minutes",
                            table.maximumTombstonesPerSliceLastFiveMinutes);
                mpTable.put("dropped_mutations", table.droppedMutations);

                mpTables.put(table.name, mpTable);
            }
            mpKeyspace.put("tables", mpTables);
            mpRet.put(keyspace.name, mpKeyspace);
        }
        return mpRet;
    }
}
