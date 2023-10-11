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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import com.google.common.collect.ArrayListMultimap;

import javax.management.InstanceNotFoundException;

import org.apache.commons.lang3.time.DurationFormatUtils;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyOptions;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.metrics.*;
import org.apache.cassandra.tools.*;

public class TableStatsHolder implements StatsHolder
{
    public final List<StatsKeyspace> keyspaces;
    public int numberOfTables = 0;
    public final boolean humanReadable;
    public final String sortKey;
    public final int top;
    public final boolean locationCheck;

    public TableStatsHolder(NodeProbe probe, boolean humanReadable, boolean ignore, List<String> tableNames, String sortKey, int top, boolean locationCheck)
    {
        this.keyspaces = new ArrayList<>();
        this.humanReadable = humanReadable;
        this.sortKey = sortKey;
        this.top = top;
        this.locationCheck = locationCheck;

        if (!this.isTestTableStatsHolder())
            this.initializeKeyspaces(probe, ignore, tableNames);
    }

    @Override
    public Map<String, Object> convert2Map()
    {
        if (sortKey.isEmpty())
            return convertAllToMap();
        else
            return convertSortedFilteredSubsetToMap();
    }

    /**
     * @return {@code Map<String, Object>} a nested HashMap of keyspaces, their tables, and the tables' statistics.
     */
    private Map<String, Object> convertAllToMap()
    {
        HashMap<String, Object> mpRet = new HashMap<>();
        mpRet.put("total_number_of_tables", numberOfTables);
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
                Map<String, Object> mpTable = convertStatsTableToMap(table);
                mpTables.put(table.tableName, mpTable);
            }
            mpKeyspace.put("tables", mpTables);
            mpRet.put(keyspace.name, mpKeyspace);
        }
        return mpRet;
    }

    /**
     * @return {@code Map<String, Object>} a nested HashMap of the sorted and filtered table names and the HashMaps of their statistics.
     */
    private Map<String, Object> convertSortedFilteredSubsetToMap()
    {
        HashMap<String, Object> mpRet = new HashMap<>();
        mpRet.put("total_number_of_tables", numberOfTables);
        List<StatsTable> sortedFilteredTables = getSortedFilteredTables();
        for (StatsTable table : sortedFilteredTables)
        {
            String tableDisplayName = table.keyspaceName + "." + table.tableName;
            Map<String, Object> mpTable = convertStatsTableToMap(table);
            mpRet.put(tableDisplayName, mpTable);
        }
        return mpRet;
    }

    private Map<String, Object> convertStatsTableToMap(StatsTable table)
    {
        Map<String, Object> mpTable = new HashMap<>();
        mpTable.put("sstable_count", table.sstableCount);
        mpTable.put("old_sstable_count", table.oldSSTableCount);
        mpTable.put("sstables_in_each_level", table.sstablesInEachLevel);
        mpTable.put("sstable_bytes_in_each_level", table.sstableBytesInEachLevel);
        mpTable.put("max_sstable_size", table.maxSSTableSize);
        mpTable.put("twcs", table.twcs);
        mpTable.put("space_used_live", table.spaceUsedLive);
        mpTable.put("space_used_total", table.spaceUsedTotal);
        mpTable.put("space_used_by_snapshots_total", table.spaceUsedBySnapshotsTotal);
        if (table.offHeapUsed)
            mpTable.put("off_heap_memory_used_total", table.offHeapMemoryUsedTotal);
        mpTable.put("sstable_compression_ratio", table.sstableCompressionRatio);
        mpTable.put("number_of_partitions_estimate", table.numberOfPartitionsEstimate);
        mpTable.put("memtable_cell_count", table.memtableCellCount);
        mpTable.put("memtable_data_size", table.memtableDataSize);
        if (table.memtableOffHeapUsed)
            mpTable.put("memtable_off_heap_memory_used", table.memtableOffHeapMemoryUsed);
        mpTable.put("memtable_switch_count", table.memtableSwitchCount);
        mpTable.put("speculative_retries", table.speculativeRetries);
        mpTable.put("local_read_count", table.localReadCount);
        mpTable.put("local_read_latency_ms", String.format("%01.3f", table.localReadLatencyMs));
        mpTable.put("local_write_count", table.localWriteCount);
        mpTable.put("local_write_latency_ms", String.format("%01.3f", table.localWriteLatencyMs));
        mpTable.put("local_read_write_ratio", String.format("%01.5f", table.localReadWriteRatio));
        mpTable.put("pending_flushes", table.pendingFlushes);
        mpTable.put("percent_repaired", table.percentRepaired);
        mpTable.put("bytes_repaired", table.bytesRepaired);
        mpTable.put("bytes_unrepaired", table.bytesUnrepaired);
        mpTable.put("bytes_pending_repair", table.bytesPendingRepair);
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
        mpTable.put("droppable_tombstone_ratio",
                    String.format("%01.5f", table.droppableTombstoneRatio));
        mpTable.put("top_size_partitions", table.topSizePartitions);
        mpTable.put("top_tombstone_partitions", table.topTombstonePartitions);
        if (locationCheck)
            mpTable.put("sstables_in_correct_location", table.isInCorrectLocation);
        return mpTable;
    }

    private void initializeKeyspaces(NodeProbe probe, boolean ignore, List<String> tableNames)
    {
        OptionFilter filter = new OptionFilter(ignore, tableNames);
        ArrayListMultimap<String, ColumnFamilyStoreMBean> selectedTableMbeans = ArrayListMultimap.create();
        Map<String, StatsKeyspace> keyspaceStats = new HashMap<>();

        // get a list of table stores
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> tableMBeans = probe.getColumnFamilyStoreMBeanProxies();

        while (tableMBeans.hasNext())
        {
            Map.Entry<String, ColumnFamilyStoreMBean> entry = tableMBeans.next();
            String keyspaceName = entry.getKey();
            ColumnFamilyStoreMBean tableProxy = entry.getValue();

            if (filter.isKeyspaceIncluded(keyspaceName))
            {
                StatsKeyspace stats = keyspaceStats.get(keyspaceName);
                if (stats == null)
                {
                    stats = new StatsKeyspace(probe, keyspaceName);
                    keyspaceStats.put(keyspaceName, stats);
                }
                stats.add(tableProxy);

                if (filter.isTableIncluded(keyspaceName, tableProxy.getTableName()))
                    selectedTableMbeans.put(keyspaceName, tableProxy);
            }
        }

        numberOfTables = selectedTableMbeans.size();

        // make sure all specified keyspace and tables exist
        filter.verifyKeyspaces(probe.getKeyspaces());
        filter.verifyTables();

        // get metrics of keyspace
        for (Map.Entry<String, Collection<ColumnFamilyStoreMBean>> entry : selectedTableMbeans.asMap().entrySet())
        {
            String keyspaceName = entry.getKey();
            Collection<ColumnFamilyStoreMBean> tables = entry.getValue();
            StatsKeyspace statsKeyspace = keyspaceStats.get(keyspaceName);

            // get metrics of table statistics for this keyspace
            for (ColumnFamilyStoreMBean table : tables)
            {
                String tableName = table.getTableName();
                StatsTable statsTable = new StatsTable();
                statsTable.fullName = keyspaceName + "." + tableName;
                statsTable.keyspaceName = keyspaceName;
                statsTable.tableName = tableName;
                statsTable.isIndex = tableName.contains(".");
                statsTable.sstableCount = probe.getColumnFamilyMetric(keyspaceName, tableName, "LiveSSTableCount");
                statsTable.oldSSTableCount = probe.getColumnFamilyMetric(keyspaceName, tableName, "OldVersionSSTableCount");
                Long sstableSize = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MaxSSTableSize");
                statsTable.maxSSTableSize = sstableSize == null ? 0 : sstableSize;

                int[] leveledSStables = table.getSSTableCountPerLevel();
                if (leveledSStables != null)
                {
                    statsTable.isLeveledSstable = true;

                    for (int level = 0; level < leveledSStables.length; level++)
                    {
                        int count = leveledSStables[level];
                        long maxCount = 4L; // for L0
                        if (level > 0)
                            maxCount = (long) Math.pow(table.getLevelFanoutSize(), level);
                        // show max threshold for level when exceeded
                        statsTable.sstablesInEachLevel.add(count + ((count > maxCount) ? "/" + maxCount : ""));
                    }
                }
                statsTable.sstableCountPerTWCSBucket = table.getSSTableCountPerTWCSBucket();

                long[] leveledSSTablesBytes = table.getPerLevelSizeBytes();
                if (leveledSSTablesBytes != null)
                {
                    statsTable.isLeveledSstable = true;
                    for (int level = 0; level < leveledSSTablesBytes.length; level++)
                    {
                        long size = leveledSSTablesBytes[level];
                        statsTable.sstableBytesInEachLevel.add(format(size, humanReadable));
                    }
                }

                if (locationCheck)
                    statsTable.isInCorrectLocation = !table.hasMisplacedSSTables();

                Long memtableOffHeapSize = null;
                Long bloomFilterOffHeapSize = null;
                Long indexSummaryOffHeapSize = null;
                Long compressionMetadataOffHeapSize = null;
                Long offHeapSize = null;
                Double percentRepaired = null;
                Long bytesRepaired = null;
                Long bytesUnrepaired = null;
                Long bytesPendingRepair = null;

                try
                {
                    memtableOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MemtableOffHeapSize");
                    bloomFilterOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "BloomFilterOffHeapMemoryUsed");
                    indexSummaryOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "IndexSummaryOffHeapMemoryUsed");
                    compressionMetadataOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "CompressionMetadataOffHeapMemoryUsed");
                    offHeapSize = memtableOffHeapSize + bloomFilterOffHeapSize + indexSummaryOffHeapSize + compressionMetadataOffHeapSize;
                    percentRepaired = (Double) probe.getColumnFamilyMetric(keyspaceName, tableName, "PercentRepaired");
                    bytesRepaired = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "BytesRepaired");
                    bytesUnrepaired = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "BytesUnrepaired");
                    bytesPendingRepair = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "BytesPendingRepair");
                }
                catch (RuntimeException e)
                {
                    // offheap-metrics introduced in 2.1.3 - older versions do not have the appropriate mbeans
                    if (!(e.getCause() instanceof InstanceNotFoundException))
                        throw e;
                }

                statsTable.spaceUsedLive = format((Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "LiveDiskSpaceUsed"), humanReadable);
                statsTable.spaceUsedTotal = format((Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "TotalDiskSpaceUsed"), humanReadable);
                statsTable.spaceUsedBySnapshotsTotal = format((Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "SnapshotsSize"), humanReadable);

                maybeAddTWCSWindowWithMaxDuration(statsTable, probe, keyspaceName, tableName);

                if (offHeapSize != null)
                {
                    statsTable.offHeapUsed = true;
                    statsTable.offHeapMemoryUsedTotal = format(offHeapSize, humanReadable);

                }
                if (percentRepaired != null)
                {
                    statsTable.percentRepaired = Math.round(100 * percentRepaired) / 100.0;
                }

                statsTable.bytesRepaired = bytesRepaired != null ? bytesRepaired : 0;
                statsTable.bytesUnrepaired = bytesUnrepaired != null ? bytesUnrepaired : 0;
                statsTable.bytesPendingRepair = bytesPendingRepair != null ? bytesPendingRepair : 0;

                statsTable.sstableCompressionRatio = probe.getColumnFamilyMetric(keyspaceName, tableName, "CompressionRatio");
                Object estimatedPartitionCount = probe.getColumnFamilyMetric(keyspaceName, tableName, "EstimatedPartitionCount");
                if (Long.valueOf(-1L).equals(estimatedPartitionCount))
                {
                    estimatedPartitionCount = 0L;
                }
                statsTable.numberOfPartitionsEstimate = estimatedPartitionCount;

                statsTable.memtableCellCount = probe.getColumnFamilyMetric(keyspaceName, tableName, "MemtableColumnsCount");
                statsTable.memtableDataSize = format((Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MemtableLiveDataSize"), humanReadable);
                if (memtableOffHeapSize != null)
                {
                    statsTable.memtableOffHeapUsed = true;
                    statsTable.memtableOffHeapMemoryUsed = format(memtableOffHeapSize, humanReadable);
                }
                statsTable.memtableSwitchCount = probe.getColumnFamilyMetric(keyspaceName, tableName, "MemtableSwitchCount");
                statsTable.speculativeRetries = probe.getColumnFamilyMetric(keyspaceName, tableName, "SpeculativeRetries");
                statsTable.localReadCount = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, tableName, "ReadLatency")).getCount();

                double localReadLatency = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, tableName, "ReadLatency")).getMean() / 1000;
                double localRLatency = localReadLatency > 0 ? localReadLatency : Double.NaN;
                statsTable.localReadLatencyMs = localRLatency;
                statsTable.localWriteCount = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, tableName, "WriteLatency")).getCount();

                double localWriteLatency = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, tableName, "WriteLatency")).getMean() / 1000;
                double localWLatency = localWriteLatency > 0 ? localWriteLatency : Double.NaN;

                statsTable.localReadWriteRatio = statsTable.localWriteCount > 0 ? statsTable.localReadCount / (double) statsTable.localWriteCount : 0;

                statsTable.localWriteLatencyMs = localWLatency;
                statsTable.pendingFlushes = probe.getColumnFamilyMetric(keyspaceName, tableName, "PendingFlushes");

                statsTable.bloomFilterFalsePositives = probe.getColumnFamilyMetric(keyspaceName, tableName, "BloomFilterFalsePositives");
                statsTable.bloomFilterFalseRatio = probe.getColumnFamilyMetric(keyspaceName, tableName, "RecentBloomFilterFalseRatio");
                statsTable.bloomFilterSpaceUsed = format((Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "BloomFilterDiskSpaceUsed"), humanReadable);

                if (bloomFilterOffHeapSize != null)
                {
                    statsTable.bloomFilterOffHeapUsed = true;
                    statsTable.bloomFilterOffHeapMemoryUsed = format(bloomFilterOffHeapSize, humanReadable);
                }

                if (indexSummaryOffHeapSize != null)
                {
                    statsTable.indexSummaryOffHeapUsed = true;
                    statsTable.indexSummaryOffHeapMemoryUsed = format(indexSummaryOffHeapSize, humanReadable);
                }
                if (compressionMetadataOffHeapSize != null)
                {
                    statsTable.compressionMetadataOffHeapUsed = true;
                    statsTable.compressionMetadataOffHeapMemoryUsed = format(compressionMetadataOffHeapSize, humanReadable);
                }
                statsTable.compactedPartitionMinimumBytes = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MinPartitionSize");
                statsTable.compactedPartitionMaximumBytes = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MaxPartitionSize");
                statsTable.compactedPartitionMeanBytes = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MeanPartitionSize");

                CassandraMetricsRegistry.JmxHistogramMBean histogram = (CassandraMetricsRegistry.JmxHistogramMBean) probe.getColumnFamilyMetric(keyspaceName, tableName, "LiveScannedHistogram");
                statsTable.averageLiveCellsPerSliceLastFiveMinutes = histogram.getMean();
                statsTable.maximumLiveCellsPerSliceLastFiveMinutes = histogram.getMax();

                histogram = (CassandraMetricsRegistry.JmxHistogramMBean) probe.getColumnFamilyMetric(keyspaceName, tableName, "TombstoneScannedHistogram");
                statsTable.averageTombstonesPerSliceLastFiveMinutes = histogram.getMean();
                statsTable.maximumTombstonesPerSliceLastFiveMinutes = histogram.getMax();
                statsTable.droppableTombstoneRatio = probe.getDroppableTombstoneRatio(keyspaceName, tableName);
                statsTable.topSizePartitions = format(table.getTopSizePartitions(), humanReadable);
                if (table.getTopSizePartitionsLastUpdate() != null)
                    statsTable.topSizePartitionsLastUpdate = millisToDateString(table.getTopSizePartitionsLastUpdate());
                statsTable.topTombstonePartitions = table.getTopTombstonePartitions();
                if (table.getTopTombstonePartitionsLastUpdate() != null)
                    statsTable.topTombstonePartitionsLastUpdate = millisToDateString(table.getTopTombstonePartitionsLastUpdate());

                statsKeyspace.tables.add(statsTable);
            }
            keyspaces.add(statsKeyspace);
        }
    }

    private void maybeAddTWCSWindowWithMaxDuration(StatsTable statsTable, NodeProbe probe, String keyspaceName, String tableName)
    {
        Map<String, String> compactionParameters = probe.getCfsProxy(statsTable.keyspaceName, statsTable.tableName)
                                                        .getCompactionParameters();

        if (compactionParameters == null)
            return;

        String compactor = compactionParameters.get("class");

        if (compactor == null || !compactor.endsWith(TimeWindowCompactionStrategy.class.getSimpleName()))
            return;

        String unit = compactionParameters.get(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY);
        String size = compactionParameters.get(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY);

        statsTable.twcsDurationInMillis = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MaxSSTableDuration");
        String maxDuration = millisToDuration(statsTable.twcsDurationInMillis);
        statsTable.twcs = String.format("%s %s, max duration: %s", size, unit, maxDuration);
    }

    private String format(long bytes, boolean humanReadable)
    {
        return humanReadable ? FileUtils.stringifyFileSize(bytes) : Long.toString(bytes);
    }

    private Map<String, String> format(Map<String, Long> map, boolean humanReadable)
    {
        LinkedHashMap<String, String> retMap = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : map.entrySet())
            retMap.put(entry.getKey(), format(entry.getValue(), humanReadable));
        return retMap;
    }

    private String millisToDateString(long millis)
    {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        df.setTimeZone(tz);
        return df.format(new Date(millis));
    }

    private String millisToDuration(long millis)
    {
        return DurationFormatUtils.formatDurationWords(millis, true, true);
    }

    /**
     * Sort and filter this TableStatHolder's tables as specified by its sortKey and top attributes.
     */
    public List<StatsTable> getSortedFilteredTables() {
        List<StatsTable> tables = new ArrayList<>();
        for (StatsKeyspace keyspace : keyspaces)
            tables.addAll(keyspace.tables);
        Collections.sort(tables, new StatsTableComparator(sortKey, humanReadable));
        int k = (tables.size() >= top) ? top : tables.size();
        if (k > 0)
            tables = tables.subList(0, k);
        return tables;
    }

    protected boolean isTestTableStatsHolder() {
        return false;
    }

    /**
     * Used for filtering keyspaces and tables to be displayed using the tablestats command.
     */
    private static class OptionFilter
    {
        private final Map<String, List<String>> filter = new HashMap<>();
        private final Map<String, List<String>> verifier = new HashMap<>(); // Same as filter initially, but we remove tables every time we've checked them for inclusion
        // in isTableIncluded() so that we detect if those table requested don't exist (verifyTables())
        private final List<String> filterList = new ArrayList<>();
        private final boolean ignoreMode;

        OptionFilter(boolean ignoreMode, List<String> filterList)
        {
            this.filterList.addAll(filterList);
            this.ignoreMode = ignoreMode;

            for (String s : filterList)
            {
                String[] keyValues = s.split("\\.", 2);

                // build the map that stores the keyspaces and tables to use
                if (!filter.containsKey(keyValues[0]))
                {
                    filter.put(keyValues[0], new ArrayList<>());
                    verifier.put(keyValues[0], new ArrayList<>());
                }

                if (keyValues.length == 2)
                {
                    filter.get(keyValues[0]).add(keyValues[1]);
                    verifier.get(keyValues[0]).add(keyValues[1]);
                }
            }
        }

        public boolean isTableIncluded(String keyspace, String table)
        {
            // supplying empty params list is treated as wanting to display all keyspaces and tables
            if (filterList.isEmpty())
                return !ignoreMode;

            List<String> tables = filter.get(keyspace);

            // no such keyspace is in the map
            if (tables == null)
                return ignoreMode;
                // only a keyspace with no tables was supplied
                // so ignore or include (based on the flag) every column family in specified keyspace
            else if (tables.isEmpty())
                return !ignoreMode;

            // keyspace exists, and it contains specific table
            verifier.get(keyspace).remove(table);
            return ignoreMode ^ tables.contains(table);
        }

        public boolean isKeyspaceIncluded(String keyspace)
        {
            // supplying empty params list is treated as wanting to display all keyspaces and tables
            if (filterList.isEmpty())
                return !ignoreMode;

            // Note that if there is any table for the keyspace, we want to include the keyspace irregarding
            // of the ignoreMode, since the ignoreMode then apply to the table inside the keyspace but the
            // keyspace itself is not ignored
            return filter.get(keyspace) != null || ignoreMode;
        }

        public void verifyKeyspaces(Collection<String> keyspaces)
        {
            for (String ks : verifier.keySet())
                if (!keyspaces.contains(ks))
                    throw new IllegalArgumentException("Unknown keyspace: " + ks);
        }

        public void verifyTables()
        {
            for (String ks : filter.keySet())
                if (!verifier.get(ks).isEmpty())
                    throw new IllegalArgumentException("Unknown tables: " + verifier.get(ks) + " in keyspace: " + ks);
        }
    }
}
