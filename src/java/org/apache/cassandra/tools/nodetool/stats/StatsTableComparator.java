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

import java.util.Comparator;

import org.apache.cassandra.io.util.FileUtils;

/**
 * Comparator to sort StatsTables by a named statistic.
 */
public class StatsTableComparator implements Comparator<StatsTable>
{

    /**
     * Name of the stat that will be used as the sort key.
     */
    private final String sortKey;

    /**
     * Whether data size stats are printed human readable.
     */
    private final boolean humanReadable;

    /**
     * Whether sorting should be done ascending.
     */
    private final boolean ascending;

    /**
     * Names of supported sort keys as they should be specified on the command line.
     */
    public static final String[] supportedSortKeys = { "average_live_cells_per_slice_last_five_minutes",
                                                       "average_tombstones_per_slice_last_five_minutes",
                                                       "bloom_filter_false_positives", "bloom_filter_false_ratio",
                                                       "bloom_filter_off_heap_memory_used", "bloom_filter_space_used",
                                                       "compacted_partition_maximum_bytes",
                                                       "compacted_partition_mean_bytes",
                                                       "compacted_partition_minimum_bytes",
                                                       "compression_metadata_off_heap_memory_used", "dropped_mutations",
                                                       "full_name", "index_summary_off_heap_memory_used",
                                                       "local_read_count", "local_read_latency_ms",
                                                       "local_write_latency_ms",
                                                       "maximum_live_cells_per_slice_last_five_minutes",
                                                       "maximum_tombstones_per_slice_last_five_minutes",
                                                       "memtable_cell_count", "memtable_data_size",
                                                       "memtable_off_heap_memory_used", "memtable_switch_count",
                                                       "number_of_partitions_estimate", "off_heap_memory_used_total",
                                                       "pending_flushes", "percent_repaired", "read_latency", "reads",
                                                       "space_used_by_snapshots_total", "space_used_live",
                                                       "space_used_total", "sstable_compression_ratio", "sstable_count",
                                                       "table_name", "write_latency", "writes", "max_sstable_size",
                                                       "local_read_write_ratio", "twcs_max_duration"};

    public StatsTableComparator(String sortKey, boolean humanReadable)
    {
        this(sortKey, humanReadable, false);
    }
    
    public StatsTableComparator(String sortKey, boolean humanReadable, boolean ascending)
    {
        this.sortKey = sortKey;
        this.humanReadable = humanReadable;
        this.ascending = ascending;
    }

    /**
     * Compare stats represented as doubles
     */
    private int compareDoubles(double x, double y)
    {
        int sign = ascending ? 1 : -1;
        if (Double.isNaN(x) && !Double.isNaN(y))
            return sign * Double.valueOf(0D).compareTo(Double.valueOf(y));
        else if (!Double.isNaN(x) && Double.isNaN(y))
            return sign * Double.valueOf(x).compareTo(Double.valueOf(0D));
        else if (Double.isNaN(x) && Double.isNaN(y))
            return 0;
        else
            return sign * Double.valueOf(x).compareTo(Double.valueOf(y));
    }

    /**
     * Compare file size stats represented as strings
     */
    private int compareFileSizes(String x, String y)
    {
        int sign = ascending ? 1 : -1;
        if (null == x && null != y)
            return sign * -1;
        else if (null != x && null == y)
            return sign;
        else if (null == x && null == y)
            return 0;
        long sizeX = humanReadable ? FileUtils.parseFileSize(x) : Long.valueOf(x);
        long sizeY = humanReadable ? FileUtils.parseFileSize(y) : Long.valueOf(y);
        return sign * Long.compare(sizeX, sizeY);
    }

    /**
     * Compare StatsTable instances based on this instance's sortKey.
     */
    public int compare(StatsTable stx, StatsTable sty)
    {
        if (stx == null || sty == null)
            throw new NullPointerException("StatsTableComparator cannot compare null objects");
        int sign = ascending ? 1 : -1;
        int result = 0;
        if (sortKey.equals("average_live_cells_per_slice_last_five_minutes"))
        {
            result = compareDoubles(stx.averageLiveCellsPerSliceLastFiveMinutes,
                                    sty.averageLiveCellsPerSliceLastFiveMinutes);
        }
        else if (sortKey.equals("average_tombstones_per_slice_last_five_minutes"))
        {
            result = compareDoubles(stx.averageTombstonesPerSliceLastFiveMinutes,
                                    sty.averageTombstonesPerSliceLastFiveMinutes);
        }
        else if (sortKey.equals("bloom_filter_false_positives"))
        {
            result = sign * ((Long) stx.bloomFilterFalsePositives)
                                    .compareTo((Long) sty.bloomFilterFalsePositives);
        }
        else if (sortKey.equals("bloom_filter_false_ratio"))
        {
            result = compareDoubles((Double) stx.bloomFilterFalseRatio,
                                    (Double) sty.bloomFilterFalseRatio);
        }
        else if (sortKey.equals("bloom_filter_off_heap_memory_used"))
        {
            if (stx.bloomFilterOffHeapUsed && !sty.bloomFilterOffHeapUsed)
                return sign;
            else if (!stx.bloomFilterOffHeapUsed && sty.bloomFilterOffHeapUsed)
                return sign * -1;
            else if (!stx.bloomFilterOffHeapUsed && !sty.bloomFilterOffHeapUsed)
                result = 0;
            else
            {
                result = compareFileSizes(stx.bloomFilterOffHeapMemoryUsed,
                                          sty.bloomFilterOffHeapMemoryUsed);
            }
        }
        else if (sortKey.equals("bloom_filter_space_used"))
        {
            result = compareFileSizes(stx.bloomFilterSpaceUsed,
                                      sty.bloomFilterSpaceUsed);
        }
        else if (sortKey.equals("compacted_partition_maximum_bytes"))
        {
            result = sign * Long.valueOf(stx.compactedPartitionMaximumBytes)
                            .compareTo(Long.valueOf(sty.compactedPartitionMaximumBytes));
        }
        else if (sortKey.equals("compacted_partition_mean_bytes"))
        {
            result = sign * Long.valueOf(stx.compactedPartitionMeanBytes)
                            .compareTo(Long.valueOf(sty.compactedPartitionMeanBytes));
        }
        else if (sortKey.equals("compacted_partition_minimum_bytes"))
        {
            result = sign * Long.valueOf(stx.compactedPartitionMinimumBytes)
                            .compareTo(Long.valueOf(sty.compactedPartitionMinimumBytes));
        }
        else if (sortKey.equals("compression_metadata_off_heap_memory_used"))
        {
            if (stx.compressionMetadataOffHeapUsed && !sty.compressionMetadataOffHeapUsed)
                return sign;
            else if (!stx.compressionMetadataOffHeapUsed && sty.compressionMetadataOffHeapUsed)
                return sign * -1;
            else if (!stx.compressionMetadataOffHeapUsed && !sty.compressionMetadataOffHeapUsed)
                result = 0;
            else
            {
                result = compareFileSizes(stx.compressionMetadataOffHeapMemoryUsed,
                                          sty.compressionMetadataOffHeapMemoryUsed);
            }
        }
        else if (sortKey.equals("full_name"))
        {
            return sign * stx.fullName.compareTo(sty.fullName);
        }
        else if (sortKey.equals("index_summary_off_heap_memory_used"))
        {
            if (stx.indexSummaryOffHeapUsed && !sty.indexSummaryOffHeapUsed)
                return sign;
            else if (!stx.indexSummaryOffHeapUsed && sty.indexSummaryOffHeapUsed)
                return sign * -1;
            else if (!stx.indexSummaryOffHeapUsed && !sty.indexSummaryOffHeapUsed)
                result = 0;
            else
            {
                result = compareFileSizes(stx.indexSummaryOffHeapMemoryUsed,
                                          sty.indexSummaryOffHeapMemoryUsed);
            }
        }
        else if (sortKey.equals("local_read_count") || sortKey.equals("reads"))
        {
            result = sign * Long.valueOf(stx.localReadCount)
                            .compareTo(Long.valueOf(sty.localReadCount));
        }
        else if (sortKey.equals("local_read_latency_ms") || sortKey.equals("read_latency"))
        {
            result = compareDoubles(stx.localReadLatencyMs, sty.localReadLatencyMs);
        }
        else if (sortKey.equals("local_write_count") || sortKey.equals("writes"))
        {
            result = sign * Long.valueOf(stx.localWriteCount)
                            .compareTo(Long.valueOf(sty.localWriteCount));
        }
        else if (sortKey.equals("local_write_latency_ms") || sortKey.equals("write_latency"))
        {
            result = compareDoubles(stx.localWriteLatencyMs, sty.localWriteLatencyMs);
        }
        else if (sortKey.equals("local_read_write_ratio"))
        {
            result = compareDoubles(stx.localReadWriteRatio, sty.localReadWriteRatio);
        }
        else if (sortKey.equals("maximum_live_cells_per_slice_last_five_minutes"))
        {
            result = sign * Long.valueOf(stx.maximumLiveCellsPerSliceLastFiveMinutes)
                            .compareTo(Long.valueOf(sty.maximumLiveCellsPerSliceLastFiveMinutes));
        }
        else if (sortKey.equals("maximum_tombstones_per_slice_last_five_minutes"))
        {
            result = sign * Long.valueOf(stx.maximumTombstonesPerSliceLastFiveMinutes)
                            .compareTo(Long.valueOf(sty.maximumTombstonesPerSliceLastFiveMinutes));
        }
        else if (sortKey.equals("memtable_cell_count"))
        {
            result = sign * ((Long) stx.memtableCellCount)
                                    .compareTo((Long) sty.memtableCellCount); 
        }
        else if (sortKey.equals("memtable_data_size"))
        {
            result = compareFileSizes(stx.memtableDataSize, sty.memtableDataSize);
        }
        else if (sortKey.equals("memtable_off_heap_memory_used"))
        {
            if (stx.memtableOffHeapUsed && !sty.memtableOffHeapUsed)
                return sign;
            else if (!stx.memtableOffHeapUsed && sty.memtableOffHeapUsed)
                return sign * -1;
            else if (!stx.memtableOffHeapUsed && !sty.memtableOffHeapUsed)
                result = 0;
            else
            {
                result = compareFileSizes(stx.memtableOffHeapMemoryUsed,
                                          sty.memtableOffHeapMemoryUsed);
            }
        }
        else if (sortKey.equals("memtable_switch_count"))
        {
            result = sign * ((Long) stx.memtableSwitchCount)
                                    .compareTo((Long) sty.memtableSwitchCount); 
        }
        else if (sortKey.equals("number_of_partitions_estimate"))
        {
            result = sign * ((Long) stx.numberOfPartitionsEstimate)
                                    .compareTo((Long) sty.numberOfPartitionsEstimate);
        }
        else if (sortKey.equals("off_heap_memory_used_total"))
        {
            if (stx.offHeapUsed && !sty.offHeapUsed)
                return sign;
            else if (!stx.offHeapUsed && sty.offHeapUsed)
                return sign * -1;
            else if (!stx.offHeapUsed && !sty.offHeapUsed)
                result = 0;
            else
            {
                result = compareFileSizes(stx.offHeapMemoryUsedTotal,
                                          sty.offHeapMemoryUsedTotal);
            }
        }
        else if (sortKey.equals("pending_flushes"))
        {
            result = sign * ((Long) stx.pendingFlushes)
                                    .compareTo((Long) sty.pendingFlushes);
        }
        else if (sortKey.equals("percent_repaired"))
        {
            result = compareDoubles(stx.percentRepaired, sty.percentRepaired);
        }
        else if (sortKey.equals("max_sstable_size"))
        {
            result = sign * stx.maxSSTableSize.compareTo(sty.maxSSTableSize);
        }
        else if (sortKey.equals("twcs_max_duration"))
        {
            if (stx.twcsDurationInMillis != null && sty.twcsDurationInMillis == null)
                return sign;
            else if (stx.twcsDurationInMillis == null && sty.twcsDurationInMillis != null)
                return sign * -1;
            else if (stx.twcsDurationInMillis == null)
                return 0;
            else
                result = sign * stx.twcsDurationInMillis.compareTo(sty.twcsDurationInMillis);
        }
        else if (sortKey.equals("space_used_by_snapshots_total"))
        {
            result = compareFileSizes(stx.spaceUsedBySnapshotsTotal,
                                      sty.spaceUsedBySnapshotsTotal);
        }
        else if (sortKey.equals("space_used_live"))
        {
            result = compareFileSizes(stx.spaceUsedLive, sty.spaceUsedLive);
        }
        else if (sortKey.equals("space_used_total"))
        {
            result = compareFileSizes(stx.spaceUsedTotal, sty.spaceUsedTotal);
        }
        else if (sortKey.equals("sstable_compression_ratio"))
        {
            result = compareDoubles((Double) stx.sstableCompressionRatio,
                                    (Double) sty.sstableCompressionRatio);
        }
        else if (sortKey.equals("sstable_count"))
        {
            result = sign * ((Integer) stx.sstableCount)
                                       .compareTo((Integer) sty.sstableCount);
        }
        else if (sortKey.equals("table_name"))
        {
            return sign * stx.tableName.compareTo(sty.tableName);
        }
        else
        {
            throw new IllegalStateException(String.format("Unsupported sort key: %s", sortKey));
        }
        return (result == 0) ? stx.fullName.compareTo(sty.fullName) : result;
    }
}
