/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.unified.AdaptiveController;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.StaticController;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * CQL tests on a table configured with Unified Compaction.
 *
 * The unified compaction strategy is described in this design document:
 *
 * TODO: link to design doc or SEP
 *
 * It has properties of  both tiered and leveled compactions and it adapts to the workload
 * by switching between strategies or increasing / decreasing the fanout factor.
 *
 * The essential formulae are the calculations of buckets:
 *
 * S = ⌊log_oF(size / m)⌋ = ⌊(ln size - ln m) / (ln F + ln o)⌋
 *
 * where log_oF is the log with oF as the base
 * o is the survival factor, currently fixed to 1
 * F is the fanout factor calculated below
 * m is the minimal size, fixed in the strategy options
 * size is the sorted run size (sum of all the sizes of the sstables in the sorted run)
 *
 * Also, T is the number of sorted runs that trigger compaction.
 *
 * Give a parameter W, which is fixed in these tests, then T and F are calculated as follows:
 *
 * - W < 0 then T = 2 and F = 2 - W (leveled merge policy)
 * - W > 0 then T = F and F = 2 + W (tiered merge policy)
 * - W = 0 then T = F = 2 (middle ground)
 */
public class CQLUnifiedCompactionTest extends CQLTester
{
    @BeforeClass
    public static void beforeClass()
    {
        CQLTester.setUpClass();
        StorageService.instance.initServer();
    }

    @After
    public void tearDown()
    {
        // This prevents unwanted flushing in future tests
        // Dirty CL segments cause memtables to be flushed after a schema change and we don't want this
        // to happen asynchronously in CQLTester.afterTest() because it would interfere with the tests
        // that rely on an exact number of sstables

        for (String table: currentTables())
        {
            logger.debug("Dropping {} synchronously to prevent unwanted flushing due to CL dirty", table);
            schemaChange(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, table));
        }

        CommitLog.instance.forceRecycleAllSegments();
    }

    @Test
    public void testCreateTable()
    {
        createTable("create table %s (id int primary key, val text) with compaction = {'class':'UnifiedCompactionStrategy'}");
        assertTrue(getCurrentCompactionStrategy() instanceof UnifiedCompactionStrategy);
    }

    @Test
    public void testStaticOptions()
    {
        testStaticOptions(512, 2, 50, -2);
        testStaticOptions(1024, 4, 150, 0);
        testStaticOptions(2048, 10, 250, 2);
    }

    private void testStaticOptions(int dataSetSizeGB, int numShards, int minSstableSizeMB, int ... Ws)
    {
        String scalingParametersStr = String.join(",", Arrays.stream(Ws)
                                                          .mapToObj(i -> Integer.toString(i))
                                                          .collect(Collectors.toList()));

        createTable("create table %s (id int primary key, val text) with compaction = " +
                    "{'class':'UnifiedCompactionStrategy', 'adaptive' : 'false', " +
                    String.format("'dataset_size_in_gb' : '%d', ", dataSetSizeGB) +
                    String.format("'num_shards' : '%d', ", numShards) +
                    String.format("'min_sstable_size_in_mb' : '%d', ", minSstableSizeMB) +
                    String.format("'static_scaling_parameters' : '%s'}", scalingParametersStr));

        CompactionStrategy strategy = getCurrentCompactionStrategy();
        assertTrue(strategy instanceof UnifiedCompactionStrategy);

        UnifiedCompactionStrategy unifiedCompactionStrategy = (UnifiedCompactionStrategy) strategy;
        Controller controller = unifiedCompactionStrategy.getController();
        assertEquals((long) dataSetSizeGB << 30, controller.getDataSetSizeBytes());
        assertEquals(numShards, controller.getNumShards());
        assertEquals((long) minSstableSizeMB << 20, controller.getMinSstableSizeBytes());

        assertTrue(unifiedCompactionStrategy.getController() instanceof StaticController);
        for (int i = 0; i < Ws.length; i++)
            assertEquals(Ws[i], unifiedCompactionStrategy.getW(i));
    }

    @Test
    public void testAdaptiveOptions()
    {
        testAdaptiveOptions(512, 2, 50, -2);
        testAdaptiveOptions(1024, 4, 150, 0);
        testAdaptiveOptions(2048, 10, 250, 2);
    }

    private void testAdaptiveOptions(int dataSetSizeGB, int numShards, int sstableSizeMB, int w)
    {
        createTable("create table %s (id int primary key, val text) with compaction = " +
                    "{'class':'UnifiedCompactionStrategy', 'adaptive' : 'true', " +
                    String.format("'dataset_size_in_gb' : '%d', ", dataSetSizeGB) +
                    String.format("'num_shards' : '%d', ", numShards) +
                    String.format("'min_sstable_size_in_mb' : '%d', ", sstableSizeMB) +
                    String.format("'adaptive_starting_scaling_parameter' : '%s', ", w) +
                    String.format("'adaptive_min_scaling_parameter' : '%s', ", -6) +
                    String.format("'adaptive_max_scaling_parameter' : '%s', ", 16) +
                    String.format("'adaptive_interval_sec': '%d', ", 300) +
                    String.format("'adaptive_threshold': '%f', ", 0.25) +
                    String.format("'adaptive_min_cost': '%d'}", 1));

        CompactionStrategy strategy = getCurrentCompactionStrategy();
        assertTrue(strategy instanceof UnifiedCompactionStrategy);

        UnifiedCompactionStrategy unifiedCompactionStrategy = (UnifiedCompactionStrategy) strategy;
        assertEquals(sstableSizeMB << 20, unifiedCompactionStrategy.getController().getMinSstableSizeBytes());

        assertTrue(unifiedCompactionStrategy.getController() instanceof AdaptiveController);
        for (int i = 0; i < 10; i++)
            assertEquals(w, unifiedCompactionStrategy.getW(i));

        AdaptiveController controller = (AdaptiveController) unifiedCompactionStrategy.getController();
        assertEquals((long) dataSetSizeGB << 30, controller.getDataSetSizeBytes());
        assertEquals(numShards, controller.getNumShards());
        assertEquals((long) sstableSizeMB << 20, controller.getMinSstableSizeBytes());
        assertEquals(-6, controller.getMinW());
        assertEquals(16, controller.getMaxW());
        assertEquals(300, controller.getInterval());
        assertEquals(0.25, controller.getThreshold(), 0.000001);
        assertEquals(1, controller.getMinCost());
    }

    @Test
    public void testAlterTable()
    {
        createTable("create table %s (id int primary key, val text) with compaction = {'class' : 'SizeTieredCompactionStrategy'}");
        assertFalse(getCurrentCompactionStrategy() instanceof UnifiedCompactionStrategy);

        alterTable("alter table %s with compaction = {'class' : 'UnifiedCompactionStrategy', 'adaptive' : 'true'}");
        assertTrue(getCurrentCompactionStrategy() instanceof UnifiedCompactionStrategy);
        assertTrue(((UnifiedCompactionStrategy) getCurrentCompactionStrategy()).getController() instanceof AdaptiveController);

        alterTable("alter table %s with compaction = {'class' : 'UnifiedCompactionStrategy', 'adaptive' : 'false'}");
        assertTrue(getCurrentCompactionStrategy() instanceof UnifiedCompactionStrategy);
        assertTrue(((UnifiedCompactionStrategy) getCurrentCompactionStrategy()).getController() instanceof StaticController);
    }

    @Test
    public void testSingleCompaction() throws Throwable
    {
        testSingleCompaction(4, 6); // W = 4 => T = 6 sstables required to trigger a compaction, see doc for formula
        testSingleCompaction(2, 4); // W = 2 => T = 4
        testSingleCompaction(0, 2); // W = 0 => T = 2
        testSingleCompaction(-2, 2); // W = -2 => T = 2
        testSingleCompaction(-4, 2); // W = -4 => T = 2
    }

    private void testSingleCompaction(int W, int T) throws Throwable
    {
        // Start with sstables whose size is minimal_size_in_mb, 1mb, ensure that there are no overlaps between sstables
        int numInserts = 1024;
        int valSize = 1024;

        createTable("create table %s (id int primary key, val blob) with compaction = {'class':'UnifiedCompactionStrategy', 'adaptive' : 'false', " +
                    String.format("'static_scaling_parameters' : '%d', 'min_sstable_size_in_mb' : '1', 'num_shards': '1', 'log_all' : 'true'}", W));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        assertEquals(0, cfs.getLiveSSTables().size());

        int key = 0;
        ByteBuffer val = ByteBuffer.wrap(new byte[valSize]);
        for (int i = 0; i < T; i++)
            key = insertAndFlush(numInserts, key, val);

        int expectedInserts = numInserts * T;

        assertEquals(T, cfs.getLiveSSTables().size());
        assertEquals(expectedInserts, getRows(execute("SELECT * FROM %s")).length);

        cfs.enableAutoCompaction(true);

        assertEquals(1, cfs.getLiveSSTables().size());
        assertEquals(expectedInserts, getRows(execute("SELECT * FROM %s")).length);
    }

    @Test
    public void testMultipleCompactionsSingleW_Static() throws Throwable
    {
        // tiered tests with W = 2 and T = F = 4
        testMultipleCompactions(4, 1, 1, new int[] {2});  //  4 sstables should be compacted into 1
        testMultipleCompactions(8, 1, 1, new int[] {2});  //  8 sstables should be compacted into 1
        testMultipleCompactions(16, 1, 1, new int[] {2}); // 16 sstables should be compacted into 1

        // middle-point tests between tiered and leveled with W = 0, T = F = 2
        testMultipleCompactions(2, 1, 1, new int[] {0});   // 2 sstables should be compacted into 1
        testMultipleCompactions(4, 1, 1, new int[] {0});   // 4 sstables should be compacted into 1
        testMultipleCompactions(8, 1, 1, new int[] {0});   // 2 sstables should be compacted into 1
        testMultipleCompactions(16, 1, 1, new int[] {0});  // 16 sstables should be compacted into 1

        // leveled tests with W = -2 and T = 2, F = 4
        testMultipleCompactions(2, 1, 1, new int[] {-2});  //  2 sstables should be compacted into 1
        testMultipleCompactions(4, 1, 1, new int[] {-2});  //  4 sstables should be compacted into 1
        testMultipleCompactions(8, 1, 1, new int[] {-2});  //  8 sstables should be compacted into 1
        testMultipleCompactions(9, 1, 1, new int[] {-2});  //  9 sstables should be compacted into 2
        testMultipleCompactions(16, 1, 1, new int[] {-2}); // 12 sstables should be compacted into 1
    }

    @Test
    public void testMultipleCompactionsDifferentWs_Static() throws Throwable
    {
        // tiered tests with W = [4, -6] and T = [6, 2], F = [6, 8]
        testMultipleCompactions(12, 1, 1, new int[] {4, -6});  //  sstables: 12 -> (6,6) => 2 => 1

        // tiered tests with W = [30, 2, -6] and T = [32, 4, 2], F = [32, 4, 8]
        testMultipleCompactions(128, 1, 1, new int[] {30, 2, -6});  //  sstables: 128 -> (32,32, 32, 32) => 4 => (4) => 1
    }

    @Test
    public void testMultipleCompactionsSingleW_TwoShards() throws Throwable
    {
        testMultipleCompactions(4, 1, 2, new int[]{2});  //  4 sstables should be compacted into 1
        testMultipleCompactions(8, 1, 2, new int[]{2});  //  8 sstables should be compacted into 1
    }

    private void testMultipleCompactions(int numInitialSSTables, int numFinalSSTables, int numShards, int[] Ws) throws Throwable
    {
        int numInserts = 1024 * numShards;
        int valSize = 2048;

        String scalingParamsStr = Arrays.stream(Ws)
                                        .mapToObj(Integer::toString)
                                        .collect(Collectors.joining(","));

        createTable("create table %s (id int primary key, val blob) with compression = { 'enabled' : false } AND " +
                    "compaction = {'class':'UnifiedCompactionStrategy', 'adaptive' : 'false', " +
                    String.format("'static_scaling_parameters' : '%s', 'min_sstable_size_in_mb' : '1', 'num_shards': '%d', 'log_all' : 'true'}",
                                  scalingParamsStr, numShards));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        int key = 0;
        byte[] bytes = new byte[valSize];
        (new Random(87652)).nextBytes(bytes);
        ByteBuffer val = ByteBuffer.wrap(bytes);

        for (int i = 0; i < numInitialSSTables; i++)
            key = insertAndFlush(numInserts, key, val);

        int expectedInserts = numInserts * numInitialSSTables;

        assertEquals(expectedInserts, getRows(execute("SELECT * FROM %s")).length);
        assertEquals(numInitialSSTables * numShards, cfs.getLiveSSTables().size());

        // trigger a compaction, wait for the future because otherwise the check below
        // may be called before the strategy has executed getNextBackgroundTask()
        cfs.enableAutoCompaction(true);

        int numChecks = 0;
        int numTimesWithNoCompactions = 0;
        while(numTimesWithNoCompactions < 10  && numChecks < 1500) // 15 seconds
        {
            // check multiple times because we don't look ahead to future buckets at the moment so there is a brief
            // window without pending compactions and without compactions in progress, this may make the test flaky on slow J2
            if (cfs.getCompactionStrategy().getTotalCompactions() == 0)
                numTimesWithNoCompactions++;

            FBUtilities.sleepQuietly(10);
            numChecks++;
        }

        assertEquals(expectedInserts, getRows(execute("SELECT * FROM %s")).length);
        assertEquals(numFinalSSTables * numShards, cfs.getLiveSSTables().size());
    }

    private int insertAndFlush(int numInserts, int key, ByteBuffer val) throws Throwable
    {
        for (int i = 0; i < numInserts; i++)
            execute("INSERT INTO %s (id, val) VALUES(?,?)", key++, val);

        flush();
        return key;
    }

    private CompactionStrategy getCurrentCompactionStrategy()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        return cfs.getCompactionStrategyContainer()
                  .getStrategies()
                  .get(0);
    }
}
