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
import java.util.List;

import org.junit.BeforeClass;

/**
 * Create a test vector for unit testing of TableStats features.
 */
public class TableStatsTestBase
{

    /**
     * A test vector of StatsKeyspace and StatsTable objects loaded with human readable stats.
     */
    protected static List<StatsKeyspace> humanReadableKeyspaces;

    /**
     * A test vector of StatsTable objects loaded with human readable statistics.
     */
    protected static List<StatsTable> humanReadableTables;

    /**
     * A test vector of StatsKeyspace and StatsTable objects.
     */
    protected static List<StatsKeyspace> testKeyspaces;

    /**
     * A test vector of StatsTable objects.
     */
    protected static List<StatsTable> testTables;

    /**
     * @return StatsKeyspace an instance of StatsKeyspace preset with values for use in a test vector
     */
    private static StatsKeyspace createStatsKeyspaceTemplate(String keyspaceName)
    {
        return new StatsKeyspace(null, keyspaceName);
    }

    /**
     * @return StatsTable an instance of StatsTable preset with values for use in a test vector
     */
    private static StatsTable createStatsTableTemplate(String keyspaceName, String tableName)
    {
        StatsTable template = new StatsTable();
        template.fullName = keyspaceName + "." + tableName;
        template.keyspaceName = keyspaceName;
        template.tableName = tableName;
        template.isIndex = false;
        template.sstableCount = 0L;
        template.oldSSTableCount = 0L;
        template.spaceUsedLive = "0";
        template.spaceUsedTotal = "0";
        template.maxSSTableSize = 0L;
        template.spaceUsedBySnapshotsTotal = "0";
        template.percentRepaired = 1.0D;
        template.bytesRepaired = 0L;
        template.bytesUnrepaired = 0L;
        template.bytesPendingRepair = 0L;
        template.sstableCompressionRatio = -1.0D;
        template.numberOfPartitionsEstimate = 0L;
        template.memtableCellCount = 0L;
        template.memtableDataSize = "0";
        template.memtableSwitchCount = 0L;
        template.speculativeRetries = 0L;
        template.localReadCount =0L;
        template.localReadLatencyMs = Double.NaN;
        template.localWriteCount = 0L;
        template.localWriteLatencyMs = 0D;
        template.pendingFlushes = 0L;
        template.bloomFilterFalsePositives = 0L;
        template.bloomFilterFalseRatio = 0D;
        template.bloomFilterSpaceUsed = "0";
        template.indexSummaryOffHeapMemoryUsed = "0";
        template.compressionMetadataOffHeapMemoryUsed = "0";
        template.compactedPartitionMinimumBytes = 0L;
        template.compactedPartitionMaximumBytes = 0L;
        template.compactedPartitionMeanBytes = 0L;
        template.averageLiveCellsPerSliceLastFiveMinutes = Double.NaN;
        template.maximumLiveCellsPerSliceLastFiveMinutes = 0L;
        template.averageTombstonesPerSliceLastFiveMinutes = Double.NaN;
        template.maximumTombstonesPerSliceLastFiveMinutes = 0L;
        template.twcs = null;
        template.twcsDurationInMillis = 0L;
        return template;
    }

    @BeforeClass
    public static void createTestVector()
    {
        // create test tables from templates
        StatsTable table1 = createStatsTableTemplate("keyspace1", "table1");
        StatsTable table2 = createStatsTableTemplate("keyspace1", "table2");
        StatsTable table3 = createStatsTableTemplate("keyspace1", "table3");
        StatsTable table4 = createStatsTableTemplate("keyspace2", "table4");
        StatsTable table5 = createStatsTableTemplate("keyspace2", "table5");
        StatsTable table6 = createStatsTableTemplate("keyspace3", "table6");
        // average live cells: 1 > 6 > 2 > 5 > 3 > 4
        table1.averageLiveCellsPerSliceLastFiveMinutes = 6D;
        table2.averageLiveCellsPerSliceLastFiveMinutes = 4.01D;
        table3.averageLiveCellsPerSliceLastFiveMinutes = 0D;
        table4.averageLiveCellsPerSliceLastFiveMinutes = Double.NaN;
        table5.averageLiveCellsPerSliceLastFiveMinutes = 4D;
        table6.averageLiveCellsPerSliceLastFiveMinutes = 5D;
        // average tombstones: 6 > 1 > 5 > 2 > 3 > 4
        table1.averageTombstonesPerSliceLastFiveMinutes = 5D;
        table2.averageTombstonesPerSliceLastFiveMinutes = 4.001D;
        table3.averageTombstonesPerSliceLastFiveMinutes = Double.NaN; 
        table4.averageTombstonesPerSliceLastFiveMinutes = 0D;
        table5.averageTombstonesPerSliceLastFiveMinutes = 4.01D;
        table6.averageTombstonesPerSliceLastFiveMinutes = 6D;
        // bloom filter false positives: 2 > 4 > 6 > 1 > 3 > 5
        table1.bloomFilterFalsePositives = 30L;
        table2.bloomFilterFalsePositives = 600L;
        table3.bloomFilterFalsePositives = 20L;
        table4.bloomFilterFalsePositives = 500L;
        table5.bloomFilterFalsePositives = 10L;
        table6.bloomFilterFalsePositives = 400L;
        // bloom filter false positive ratio: 5 > 3 > 1 > 6 > 4 > 2
        table1.bloomFilterFalseRatio = 0.40D;
        table2.bloomFilterFalseRatio = 0.01D;
        table3.bloomFilterFalseRatio = 0.50D;
        table4.bloomFilterFalseRatio = 0.02D;
        table5.bloomFilterFalseRatio = 0.60D;
        table6.bloomFilterFalseRatio = 0.03D;
        // bloom filter space used: 2 > 4 > 6 > 1 > 3 > 5
        table1.bloomFilterSpaceUsed = "789";
        table2.bloomFilterSpaceUsed = "161718";
        table3.bloomFilterSpaceUsed = "456";
        table4.bloomFilterSpaceUsed = "131415";
        table5.bloomFilterSpaceUsed = "123";
        table6.bloomFilterSpaceUsed = "101112";
        // compacted partition maximum bytes: 1 > 3 > 5 > 2 > 4 = 6 
        table1.compactedPartitionMaximumBytes = 60L;
        table2.compactedPartitionMaximumBytes = 30L;
        table3.compactedPartitionMaximumBytes = 50L;
        table4.compactedPartitionMaximumBytes = 20L;
        table5.compactedPartitionMaximumBytes = 40L;
        table6.compactedPartitionMaximumBytes = 20L;
        // compacted partition mean bytes: 1 > 3 > 2 = 4 = 5 > 6
        table1.compactedPartitionMeanBytes = 6L;
        table2.compactedPartitionMeanBytes = 4L;
        table3.compactedPartitionMeanBytes = 5L;
        table4.compactedPartitionMeanBytes = 4L;
        table5.compactedPartitionMeanBytes = 4L;
        table6.compactedPartitionMeanBytes = 3L;
        // compacted partition minimum bytes: 6 > 4 > 2 > 5 > 1 = 3
        table1.compactedPartitionMinimumBytes = 2L;
        table2.compactedPartitionMinimumBytes = 4L;
        table3.compactedPartitionMinimumBytes = 2L;
        table4.compactedPartitionMinimumBytes = 5L;
        table5.compactedPartitionMinimumBytes = 3L;
        table6.compactedPartitionMinimumBytes = 6L;
        // local reads: 6 > 5 > 4 > 3 > 2 > 1
        table1.localReadCount = 0L;
        table2.localReadCount = 1L;
        table3.localReadCount = 2L;
        table4.localReadCount = 3L;
        table5.localReadCount = 4L;
        table6.localReadCount = 5L;
        // local read latency: 3 > 2 > 1 > 6 > 4 > 5
        table1.localReadLatencyMs = 2D;
        table2.localReadLatencyMs = 3D;
        table3.localReadLatencyMs = 4D;
        table4.localReadLatencyMs = Double.NaN;
        table5.localReadLatencyMs = 0D;
        table6.localReadLatencyMs = 1D;
        // local writes: 1 > 2 > 3 > 4 > 5 > 6
        table1.localWriteCount = 5L;
        table2.localWriteCount = 4L;
        table3.localWriteCount = 3L;
        table4.localWriteCount = 2L;
        table5.localWriteCount = 1L;
        table6.localWriteCount = 0L;
        // local write latency: 4 > 5 > 6 > 1 > 2 > 3
        table1.localWriteLatencyMs = 0.05D;
        table2.localWriteLatencyMs = 0D;
        table3.localWriteLatencyMs = Double.NaN;
        table4.localWriteLatencyMs = 2D;
        table5.localWriteLatencyMs = 1D;
        table6.localWriteLatencyMs = 0.5D;
        // maximum live cells last five minutes: 1 > 2 = 3 > 4 = 5 > 6
        table1.maximumLiveCellsPerSliceLastFiveMinutes = 6L;
        table2.maximumLiveCellsPerSliceLastFiveMinutes = 5L;
        table3.maximumLiveCellsPerSliceLastFiveMinutes = 5L;
        table4.maximumLiveCellsPerSliceLastFiveMinutes = 3L;
        table5.maximumLiveCellsPerSliceLastFiveMinutes = 3L;
        table6.maximumLiveCellsPerSliceLastFiveMinutes = 2L;
        // maximum tombstones last five minutes: 6 > 5 > 3 = 4 > 2 > 1
        table1.maximumTombstonesPerSliceLastFiveMinutes = 1L;
        table2.maximumTombstonesPerSliceLastFiveMinutes = 2L;
        table3.maximumTombstonesPerSliceLastFiveMinutes = 3L;
        table4.maximumTombstonesPerSliceLastFiveMinutes = 3L;
        table5.maximumTombstonesPerSliceLastFiveMinutes = 5L;
        table6.maximumTombstonesPerSliceLastFiveMinutes = 6L;
        // memtable cell count: 3 > 5 > 6 > 1 > 2 > 4
        table1.memtableCellCount = 111L;
        table2.memtableCellCount = 22L;
        table3.memtableCellCount = 333333L;
        table4.memtableCellCount = 4L;
        table5.memtableCellCount = 55555L;
        table6.memtableCellCount = 6666L;
        // memtable data size: 6 > 5 > 4 > 3 > 2 > 1
        table1.memtableDataSize = "0";
        table2.memtableDataSize = "900";
        table3.memtableDataSize = "1999";
        table4.memtableDataSize = "3000";
        table5.memtableDataSize = "20000";
        table6.memtableDataSize = "1000000";
        // memtable switch count: 4 > 2 > 3 > 6 > 5 > 1
        table1.memtableSwitchCount = 1L;
        table2.memtableSwitchCount = 22222L;
        table3.memtableSwitchCount = 3333L;
        table4.memtableSwitchCount = 444444L;
        table5.memtableSwitchCount = 5L;
        table6.memtableSwitchCount = 6L;
        // number of partitions estimate: 1 > 2 > 3 > 4 > 5 > 6
        table1.numberOfPartitionsEstimate = 111111L;
        table2.numberOfPartitionsEstimate = 22222L;
        table3.numberOfPartitionsEstimate = 3333L;
        table4.numberOfPartitionsEstimate = 444L;
        table5.numberOfPartitionsEstimate = 55L;
        table6.numberOfPartitionsEstimate = 6L;
        // pending flushes: 2 > 1 > 4 > 3 > 6 > 5
        table1.pendingFlushes = 11111L;
        table2.pendingFlushes = 222222L;
        table3.pendingFlushes = 333L;
        table4.pendingFlushes = 4444L;
        table5.pendingFlushes = 5L;
        table6.pendingFlushes = 66L;
        // percent repaired: 1 > 2 > 3 > 5 > 4 > 6
        table1.percentRepaired = 100.0D;
        table2.percentRepaired = 99.9D;
        table3.percentRepaired = 99.8D;
        table4.percentRepaired = 50.0D;
        table5.percentRepaired = 93.0D;
        table6.percentRepaired = 0.0D;
        // space used by snapshots: 5 > 1 > 2 > 4 > 3 = 6
        table1.spaceUsedBySnapshotsTotal = "1111";
        table2.spaceUsedBySnapshotsTotal = "222";
        table3.spaceUsedBySnapshotsTotal = "0";
        table4.spaceUsedBySnapshotsTotal = "44";
        table5.spaceUsedBySnapshotsTotal = "55555";
        table6.spaceUsedBySnapshotsTotal = "0";
        // space used live: 6 > 5 > 4 > 2 > 1 = 3
        table1.spaceUsedLive = "0";
        table2.spaceUsedLive = "22";
        table3.spaceUsedLive = "0";
        table4.spaceUsedLive = "4444";
        table5.spaceUsedLive = "55555";
        table6.spaceUsedLive = "666666";
        // space used total: 1 > 2 > 3 > 4 > 5 > 6
        table1.spaceUsedTotal = "9001";
        table2.spaceUsedTotal = "1024";
        table3.spaceUsedTotal = "512";
        table4.spaceUsedTotal = "256";
        table5.spaceUsedTotal = "64";
        table6.spaceUsedTotal = "0";
        // sstable compression ratio: 5 > 4 > 1 = 2 = 6 > 3
        table1.sstableCompressionRatio = 0.68D;
        table2.sstableCompressionRatio = 0.68D;
        table3.sstableCompressionRatio = 0.32D;
        table4.sstableCompressionRatio = 0.95D;
        table5.sstableCompressionRatio = 0.99D;
        table6.sstableCompressionRatio = 0.68D;
        // sstable count: 1 > 3 > 5 > 2 > 4 > 6
        table1.sstableCount = 60000;
        table2.sstableCount = 3000;
        table3.sstableCount = 50000;
        table4.sstableCount = 2000;
        table5.sstableCount = 40000;
        table6.sstableCount = 1000;
        // Droppable Tombstone ratio
        table1.droppableTombstoneRatio = 0;
        table2.droppableTombstoneRatio = 0.222222;
        table3.droppableTombstoneRatio = 0.333333;
        table4.droppableTombstoneRatio = 0.444444;
        table5.droppableTombstoneRatio = 0.555555;
        table6.droppableTombstoneRatio = 0.666666;
        // set even numbered tables to have some offheap usage
        table2.offHeapUsed = true;
        table4.offHeapUsed = true;
        table6.offHeapUsed = true;
        table2.memtableOffHeapUsed = true;
        table4.memtableOffHeapUsed = true;
        table6.memtableOffHeapUsed = true;
        table2.bloomFilterOffHeapUsed = true;
        table4.bloomFilterOffHeapUsed = true;
        table6.bloomFilterOffHeapUsed = true;
        table2.compressionMetadataOffHeapUsed = true;
        table4.compressionMetadataOffHeapUsed = true;
        table6.compressionMetadataOffHeapUsed = true;
        table2.indexSummaryOffHeapUsed = true;
        table4.indexSummaryOffHeapUsed = true;
        table6.indexSummaryOffHeapUsed = true;
        // offheap memory total: 4 > 2 > 6 > 1 = 3 = 5
        table2.offHeapMemoryUsedTotal = "314159367";
        table4.offHeapMemoryUsedTotal = "441213818";
        table6.offHeapMemoryUsedTotal = "162470810";
        // bloom filter offheap: 4 > 6 > 2 > 1 = 3 = 5
        table2.bloomFilterOffHeapMemoryUsed = "98";
        table4.bloomFilterOffHeapMemoryUsed = "299792458";
        table6.bloomFilterOffHeapMemoryUsed = "667408";
        // compression metadata offheap: 2 > 4 > 6 > 1 = 3 = 5
        table2.compressionMetadataOffHeapMemoryUsed = "3";
        table4.compressionMetadataOffHeapMemoryUsed = "2";
        table6.compressionMetadataOffHeapMemoryUsed = "1";
        // index summary offheap: 6 > 4 > 2 > 1 = 3 = 5
        table2.indexSummaryOffHeapMemoryUsed = "1";
        table4.indexSummaryOffHeapMemoryUsed = "2";
        table6.indexSummaryOffHeapMemoryUsed = "3";
        // memtable offheap: 2 > 6 > 4 > 1 = 3 = 5
        table2.memtableOffHeapMemoryUsed = "314159265";
        table4.memtableOffHeapMemoryUsed = "141421356";
        table6.memtableOffHeapMemoryUsed = "161803398";
        // twcs max duration: 2 > 4 > 1 = 3 = 6 = 5
        table2.twcsDurationInMillis = 2000L;
        table4.twcsDurationInMillis = 1000L;
        table5.twcsDurationInMillis = null;
        // create test keyspaces from templates
        testKeyspaces = new ArrayList<>();
        StatsKeyspace keyspace1 = createStatsKeyspaceTemplate("keyspace1");
        StatsKeyspace keyspace2 = createStatsKeyspaceTemplate("keyspace2");
        StatsKeyspace keyspace3 = createStatsKeyspaceTemplate("keyspace3");
        // populate StatsKeyspace tables lists
        keyspace1.tables.add(table1);
        keyspace1.tables.add(table2);
        keyspace1.tables.add(table3);
        keyspace2.tables.add(table4);
        keyspace2.tables.add(table5);
        keyspace3.tables.add(table6);
        // populate testKeyspaces test vector
        testKeyspaces.add(keyspace1);
        testKeyspaces.add(keyspace2);
        testKeyspaces.add(keyspace3);
        // compute keyspace statistics from relevant table metrics
        for (int i = 0; i < testKeyspaces.size(); i++)
        {
            StatsKeyspace ks = testKeyspaces.get(i);
            for (StatsTable st : ks.tables)
            {
                ks.readCount += st.localReadCount;
                ks.writeCount += st.localWriteCount;
                ks.pendingFlushes += (long) st.pendingFlushes;
            }
            testKeyspaces.set(i, ks);
        }
        // populate testTables test vector
        testTables = new ArrayList<>();
        testTables.add(table1);
        testTables.add(table2);
        testTables.add(table3);
        testTables.add(table4);
        testTables.add(table5);
        testTables.add(table6);
        //
        // create test vector for human readable case
        StatsTable humanReadableTable1 = createStatsTableTemplate("keyspace1", "table1");
        StatsTable humanReadableTable2 = createStatsTableTemplate("keyspace1", "table2");
        StatsTable humanReadableTable3 = createStatsTableTemplate("keyspace1", "table3");
        StatsTable humanReadableTable4 = createStatsTableTemplate("keyspace2", "table4");
        StatsTable humanReadableTable5 = createStatsTableTemplate("keyspace2", "table5");
        StatsTable humanReadableTable6 = createStatsTableTemplate("keyspace3", "table6");
        // human readable space used total: 6 > 5 > 4 > 3 > 2 > 1
        humanReadableTable1.spaceUsedTotal = "999 bytes";
        humanReadableTable2.spaceUsedTotal = "5 KiB";
        humanReadableTable3.spaceUsedTotal = "40 KiB";
        humanReadableTable4.spaceUsedTotal = "3 MiB";
        humanReadableTable5.spaceUsedTotal = "2 GiB";
        humanReadableTable6.spaceUsedTotal = "1 TiB";
        // human readable memtable data size: 1 > 3 > 5 > 2 > 4 > 6
        humanReadableTable1.memtableDataSize = "1.21 TiB";
        humanReadableTable2.memtableDataSize = "42 KiB";
        humanReadableTable3.memtableDataSize = "2.71 GiB";
        humanReadableTable4.memtableDataSize = "999 bytes";
        humanReadableTable5.memtableDataSize = "3.14 MiB";
        humanReadableTable6.memtableDataSize = "0 bytes";
        // create human readable keyspaces from template
        humanReadableKeyspaces = new ArrayList<>();
        StatsKeyspace humanReadableKeyspace1 = createStatsKeyspaceTemplate("keyspace1");
        StatsKeyspace humanReadableKeyspace2 = createStatsKeyspaceTemplate("keyspace2");
        StatsKeyspace humanReadableKeyspace3 = createStatsKeyspaceTemplate("keyspace3");
        // populate human readable StatsKeyspace tables lists
        humanReadableKeyspace1.tables.add(humanReadableTable1);
        humanReadableKeyspace1.tables.add(humanReadableTable2);
        humanReadableKeyspace1.tables.add(humanReadableTable3);
        humanReadableKeyspace2.tables.add(humanReadableTable4);
        humanReadableKeyspace2.tables.add(humanReadableTable5);
        humanReadableKeyspace3.tables.add(humanReadableTable6);
        // populate human readable keyspaces test vector
        humanReadableKeyspaces.add(humanReadableKeyspace1);
        humanReadableKeyspaces.add(humanReadableKeyspace2);
        humanReadableKeyspaces.add(humanReadableKeyspace3);
        // compute human readable keyspace statistics from relevant table metrics
        for (int i = 0; i < humanReadableKeyspaces.size(); i++)
        {
            StatsKeyspace ks = humanReadableKeyspaces.get(i);
            for (StatsTable st : ks.tables)
            {
                ks.readCount += st.localReadCount;
                ks.writeCount += st.localWriteCount;
                ks.pendingFlushes += (long) st.pendingFlushes;
            }
            humanReadableKeyspaces.set(i, ks);
        }
        // populate human readable tables test vector
        humanReadableTables = new ArrayList<>();
        humanReadableTables.add(humanReadableTable1);
        humanReadableTables.add(humanReadableTable2);
        humanReadableTables.add(humanReadableTable3);
        humanReadableTables.add(humanReadableTable4);
        humanReadableTables.add(humanReadableTable5);
        humanReadableTables.add(humanReadableTable6);
    }
}
