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

package org.apache.cassandra.repair.unifiedrepair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Sets;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.RepairCoordinator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.unifiedrepair.IUnifiedRepairTokenRangeSplitter.RepairAssignment;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.service.StorageService;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.UnifiedRepairMetricsManager;
import org.apache.cassandra.metrics.UnifiedRepairMetrics;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.UnifiedRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.apache.cassandra.Util.setUnifiedRepairEnabled;
import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_DEFAULT_RF;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.repair.unifiedrepair.UnifiedRepairUtils.RepairTurn.NOT_MY_TURN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class UnifiedRepairParameterizedTest extends CQLTester
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";
    private static final String TABLE_DISABLED_UNIFIED_REPAIR = "tbl_disabled_unified_repair";
    private static final String MV = "mv";
    private static TableMetadata cfm;
    private static TableMetadata cfmDisabledUnifiedRepair;
    private static Keyspace keyspace;
    private static int timeFuncCalls;
    @Mock
    ScheduledExecutorPlus mockExecutor;
    @Mock
    UnifiedRepairState unifiedRepairState;
    @Mock
    RepairCoordinator repairRunnable;
    private static UnifiedRepairConfig defaultConfig;


    @Parameterized.Parameter()
    public UnifiedRepairConfig.RepairType repairType;

    @Parameterized.Parameters(name = "repairType={0}")
    public static Collection<UnifiedRepairConfig.RepairType> repairTypes()
    {
        return Arrays.asList(UnifiedRepairConfig.RepairType.values());
    }

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SYSTEM_DISTRIBUTED_DEFAULT_RF.setInt(1);
        UnifiedRepair.SLEEP_IF_REPAIR_FINISHES_QUICKLY = new DurationSpec.IntSecondsBound("0s");
        setUnifiedRepairEnabled(true);
        requireNetwork();
        UnifiedRepairUtils.setup();
        StorageService.instance.doUnifiedRepairSetup();
        DatabaseDescriptor.setCDCEnabled(false);
    }

    @Before
    public void setup()
    {
        SYSTEM_DISTRIBUTED_DEFAULT_RF.setInt(1);
        QueryProcessor.executeInternal(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
        QueryProcessor.executeInternal(String.format("CREATE TABLE %s.%s (k text, s text static, i int, v text, primary key(k,i))", KEYSPACE, TABLE));
        QueryProcessor.executeInternal(String.format("CREATE TABLE %s.%s (k text, s text static, i int, v text, primary key(k,i)) WITH repair_full = {'enabled': 'false'} AND repair_incremental = {'enabled': 'false'}", KEYSPACE, TABLE_DISABLED_UNIFIED_REPAIR));

        QueryProcessor.executeInternal(String.format("CREATE MATERIALIZED VIEW %s.%s AS SELECT i, k from %s.%s " +
                "WHERE k IS NOT null AND i IS NOT null PRIMARY KEY (i, k)", KEYSPACE, MV, KEYSPACE, TABLE));

        DatabaseDescriptor.setCDCOnRepairEnabled(false);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(false);
        MockitoAnnotations.initMocks(this);

        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).truncateBlocking();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).disableAutoCompaction();

        Keyspace.open(KEYSPACE).getColumnFamilyStore(MV).truncateBlocking();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(MV).disableAutoCompaction();

        Keyspace.open(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME).getColumnFamilyStore(SystemDistributedKeyspace.UNIFIED_REPAIR_PRIORITY).truncateBlocking();
        Keyspace.open(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME).getColumnFamilyStore(SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY).truncateBlocking();


        UnifiedRepair.instance = new UnifiedRepair();
        executeCQL();

        timeFuncCalls = 0;
        UnifiedRepair.timeFunc = System::currentTimeMillis;
        resetCounters();
        resetConfig();

        UnifiedRepair.shuffleFunc = java.util.Collections::shuffle;

        keyspace = Keyspace.open(KEYSPACE);
        cfm = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata();
        cfmDisabledUnifiedRepair = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE_DISABLED_UNIFIED_REPAIR).metadata();
        DatabaseDescriptor.setCDCOnRepairEnabled(false);
    }

    @After
    public void tearDown()
    {
        System.clearProperty("cassandra.streaming.requires_view_build_during_repair");
    }

    private void resetCounters()
    {
        UnifiedRepairMetrics metrics = UnifiedRepairMetricsManager.getMetrics(repairType);
        Metrics.removeMatching((name, metric) -> name.startsWith("repairTurn"));
        metrics.repairTurnMyTurn = Metrics.counter(String.format("repairTurnMyTurn-%s", repairType));
        metrics.repairTurnMyTurnForceRepair = Metrics.counter(String.format("repairTurnMyTurnForceRepair-%s", repairType));
        metrics.repairTurnMyTurnDueToPriority = Metrics.counter(String.format("repairTurnMyTurnDueToPriority-%s", repairType));
    }

    private void resetConfig()
    {
        // prepare a fresh default config
        defaultConfig = new UnifiedRepairConfig(true);
        for (UnifiedRepairConfig.RepairType repairType : UnifiedRepairConfig.RepairType.values())
        {
            defaultConfig.setUnifiedRepairEnabled(repairType, true);
            defaultConfig.setMVRepairEnabled(repairType, false);
        }

        // reset the UnifiedRepairService config to default
        UnifiedRepairConfig config = UnifiedRepairService.instance.getUnifiedRepairConfig();
        config.repair_type_overrides = defaultConfig.repair_type_overrides;
        config.global_settings = defaultConfig.global_settings;
        config.history_clear_delete_hosts_buffer_interval = defaultConfig.history_clear_delete_hosts_buffer_interval;
        config.setRepairSubRangeNum(repairType, 1);
    }

    private void executeCQL()
    {
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s) VALUES ('k', 's')");
        QueryProcessor.executeInternal("SELECT s FROM ks.tbl WHERE k='k'");
        Keyspace.open(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME)
                .getColumnFamilyStore(SystemDistributedKeyspace.UNIFIED_REPAIR_PRIORITY)
                .forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    @Test(expected = ConfigurationException.class)
    public void testRepairAsyncWithRepairTypeDisabled()
    {
        UnifiedRepairService.instance.getUnifiedRepairConfig().setUnifiedRepairEnabled(repairType, false);

        UnifiedRepair.instance.repairAsync(repairType);
    }

    @Test
    public void testRepairAsync()
    {
        UnifiedRepair.instance.repairExecutors.put(repairType, mockExecutor);

        UnifiedRepair.instance.repairAsync(repairType);

        verify(mockExecutor, Mockito.times(1)).submit(any(Runnable.class));
    }

    @Test
    public void testRepairTurn()
    {
        UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertTrue("Expected my turn for the repair", UnifiedRepairUtils.myTurnToRunRepair(repairType, myId) != NOT_MY_TURN);
    }

    @Test
    public void testRepair()
    {
        UnifiedRepairService.instance.getUnifiedRepairConfig().setRepairMinInterval(repairType, "0s");
        UnifiedRepair.instance.repair(repairType);
        assertEquals(0, UnifiedRepair.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(0, UnifiedRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        long lastRepairTime = UnifiedRepair.instance.repairStates.get(repairType).getLastRepairTime();
        //if repair was done then lastRepairTime should be non-zero
        Assert.assertTrue(String.format("Expected lastRepairTime > 0, actual value lastRepairTime %d",
                                        lastRepairTime), lastRepairTime > 0);
    }

    @Test
    public void testTooFrequentRepairs()
    {
        UnifiedRepairConfig config = UnifiedRepairService.instance.getUnifiedRepairConfig();
        //in the first round let repair run
        config.setRepairMinInterval(repairType, "0s");
        UnifiedRepair.instance.repair(repairType);
        long lastRepairTime1 = UnifiedRepair.instance.repairStates.get(repairType).getLastRepairTime();
        int consideredTables = UnifiedRepair.instance.repairStates.get(repairType).getTotalTablesConsideredForRepair();
        Assert.assertNotSame(String.format("Expected total repaired tables > 0, actual value %s ", consideredTables),
                             consideredTables, 0);

        //if repair was done in last 24 hours then it should not trigger another repair
        config.setRepairMinInterval(repairType, "24h");
        UnifiedRepair.instance.repair(repairType);
        long lastRepairTime2 = UnifiedRepair.instance.repairStates.get(repairType).getLastRepairTime();
        Assert.assertEquals(String.format("Expected repair time to be same, actual value lastRepairTime1 %d, " +
                                          "lastRepairTime2 %d", lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime2);
        assertEquals(0, UnifiedRepair.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(0, UnifiedRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
    }

    @Test
    public void testNonFrequentRepairs()
    {
        Integer prevMetricsCount = UnifiedRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue();
        UnifiedRepairState state = UnifiedRepair.instance.repairStates.get(repairType);
        long prevCount = state.getTotalMVTablesConsideredForRepair();
        UnifiedRepairService.instance.getUnifiedRepairConfig().setRepairMinInterval(repairType, "0s");
        UnifiedRepair.instance.repair(repairType);
        long lastRepairTime1 = UnifiedRepair.instance.repairStates.get(repairType).getLastRepairTime();
        Assert.assertTrue(String.format("Expected lastRepairTime1 > 0, actual value lastRepairTime1 %d",
                                        lastRepairTime1), lastRepairTime1 > 0);
        UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertTrue("Expected my turn for the repair",
                          UnifiedRepairUtils.myTurnToRunRepair(repairType, myId) != NOT_MY_TURN);
        UnifiedRepair.instance.repair(repairType);
        long lastRepairTime2 = UnifiedRepair.instance.repairStates.get(repairType).getLastRepairTime();
        Assert.assertNotSame(String.format("Expected repair time to be same, actual value lastRepairTime1 %d, " +
                                           "lastRepairTime2 ", lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime2);
        assertEquals(prevCount, state.getTotalMVTablesConsideredForRepair());
        assertEquals(prevMetricsCount, UnifiedRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue());
    }

    @Test
    public void testGetPriorityHosts()
    {
        Integer prevMetricsCount = UnifiedRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue();
        UnifiedRepairState state = UnifiedRepair.instance.repairStates.get(repairType);
        long prevCount = state.getTotalMVTablesConsideredForRepair();
        UnifiedRepairService.instance.getUnifiedRepairConfig().setRepairMinInterval(repairType, "0s");
        Assert.assertSame(String.format("Priority host count is not same, actual value %d, expected value %d",
                                        UnifiedRepairUtils.getPriorityHosts(repairType).size(), 0), UnifiedRepairUtils.getPriorityHosts(repairType).size(), 0);
        UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertTrue("Expected my turn for the repair", UnifiedRepairUtils.myTurnToRunRepair(repairType, myId) !=
                                                             NOT_MY_TURN);
        UnifiedRepair.instance.repair(repairType);
        UnifiedRepairUtils.addPriorityHosts(repairType, Sets.newHashSet(FBUtilities.getBroadcastAddressAndPort()));
        UnifiedRepair.instance.repair(repairType);
        Assert.assertSame(String.format("Priority host count is not same actual value %d, expected value %d",
                                        UnifiedRepairUtils.getPriorityHosts(repairType).size(), 0), UnifiedRepairUtils.getPriorityHosts(repairType).size(), 0);
        assertEquals(prevCount, state.getTotalMVTablesConsideredForRepair());
        assertEquals(prevMetricsCount, UnifiedRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue());
    }

    @Test
    public void testCheckUnifiedRepairStartStop() throws Throwable
    {
        Integer prevMetricsCount = UnifiedRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue();
        UnifiedRepairState state = UnifiedRepair.instance.repairStates.get(repairType);
        UnifiedRepairConfig config = UnifiedRepairService.instance.getUnifiedRepairConfig();
        long prevCount = state.getTotalMVTablesConsideredForRepair();
        config.setRepairMinInterval(repairType, "0s");
        config.setUnifiedRepairEnabled(repairType, false);
        long lastRepairTime1 = UnifiedRepair.instance.repairStates.get(repairType).getLastRepairTime();
        UnifiedRepair.instance.repair(repairType);
        long lastRepairTime2 = UnifiedRepair.instance.repairStates.get(repairType).getLastRepairTime();
        //Since repair has not happened, both the last repair times should be same
        Assert.assertEquals(String.format("Expected lastRepairTime1 %d, and lastRepairTime2 %d to be same",
                                          lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime2);

        config.setUnifiedRepairEnabled(repairType, true);
        UnifiedRepair.instance.repair(repairType);
        //since repair is done now, so lastRepairTime1/lastRepairTime2 and lastRepairTime3 should not be same
        long lastRepairTime3 = UnifiedRepair.instance.repairStates.get(repairType).getLastRepairTime();
        Assert.assertNotSame(String.format("Expected lastRepairTime1 %d, and lastRepairTime3 %d to be not same",
                                           lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime3);
        assertEquals(prevCount, state.getTotalMVTablesConsideredForRepair());
        assertEquals(prevMetricsCount, UnifiedRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue());
    }

    @Test
    public void testRepairPrimaryRangesByDefault()
    {
        Assert.assertTrue("Expected primary range repair only",
                          UnifiedRepairService.instance.getUnifiedRepairConfig().getRepairPrimaryTokenRangeOnly(repairType));
    }

    @Test
    public void testGetAllMVs()
    {
        UnifiedRepairConfig config = UnifiedRepairService.instance.getUnifiedRepairConfig();
        config.setMVRepairEnabled(repairType, false);
        assertFalse(config.getMVRepairEnabled(repairType));
        assertEquals(0, UnifiedRepairUtils.getAllMVs(repairType, keyspace, cfm).size());

        config.setMVRepairEnabled(repairType, true);

        assertTrue(config.getMVRepairEnabled(repairType));
        assertEquals(Arrays.asList(MV), UnifiedRepairUtils.getAllMVs(repairType, keyspace, cfm));
        config.setMVRepairEnabled(repairType, false);
    }


    @Test
    public void testMVRepair()
    {
        UnifiedRepairConfig config = UnifiedRepairService.instance.getUnifiedRepairConfig();
        config.setMVRepairEnabled(repairType, true);
        config.setRepairMinInterval(repairType, "0s");
        UnifiedRepair.instance.repairStates.get(repairType).setLastRepairTime(System.currentTimeMillis());
        UnifiedRepair.instance.repair(repairType);
        assertEquals(1, UnifiedRepair.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(1, UnifiedRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());

        config.setMVRepairEnabled(repairType, false);
        UnifiedRepair.instance.repairStates.get(repairType).setLastRepairTime(System.currentTimeMillis());
        UnifiedRepair.instance.repair(repairType);
        assertEquals(0, UnifiedRepair.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(0, UnifiedRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());

        config.setMVRepairEnabled(repairType, true);
        UnifiedRepair.instance.repairStates.get(repairType).setLastRepairTime(System.currentTimeMillis());
        UnifiedRepair.instance.repair(repairType);
        assertEquals(1, UnifiedRepair.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(1, UnifiedRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
    }

    @Test
    public void testSkipRepairSSTableCountHigherThreshold()
    {
        UnifiedRepairConfig config = UnifiedRepairService.instance.getUnifiedRepairConfig();
        UnifiedRepairState state = UnifiedRepair.instance.repairStates.get(repairType);
        ColumnFamilyStore cfsBaseTable = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        ColumnFamilyStore cfsMVTable = Keyspace.open(KEYSPACE).getColumnFamilyStore(MV);
        Set<SSTableReader> preBaseTable = cfsBaseTable.getLiveSSTables();
        Set<SSTableReader> preMVTable = cfsBaseTable.getLiveSSTables();
        config.setRepairMinInterval(repairType, "0s");

        for (int i = 0; i < 10; i++)
        {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, i, v) VALUES('k1', %d, 'v1')", KEYSPACE, TABLE, i));
            cfsBaseTable.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            cfsMVTable.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }

        Set<SSTableReader> postBaseTable = cfsBaseTable.getLiveSSTables();
        Set<SSTableReader> diffBaseTable = new HashSet<>(postBaseTable);
        diffBaseTable.removeAll(preBaseTable);
        assert diffBaseTable.size() == 10;

        Set<SSTableReader> postMVTable = cfsBaseTable.getLiveSSTables();
        Set<SSTableReader> diffMVTable = new HashSet<>(postMVTable);
        diffMVTable.removeAll(preMVTable);
        assert diffMVTable.size() == 10;

        int beforeCount = config.getRepairSSTableCountHigherThreshold(repairType);
        config.setMVRepairEnabled(repairType, true);
        config.setRepairSSTableCountHigherThreshold(repairType, 9);
        assertEquals(0, state.getSkippedTokenRangesCount());
        assertEquals(0, UnifiedRepairMetricsManager.getMetrics(repairType).skippedTokenRangesCount.getValue().intValue());
        state.setLastRepairTime(0);
        UnifiedRepair.instance.repair(repairType);
        assertEquals(0, state.getTotalMVTablesConsideredForRepair());
        assertEquals(0, UnifiedRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        // skipping both the tables - one table is due to its repair has been disabled, and another one due to high sstable count
        assertEquals(0, state.getSkippedTokenRangesCount());
        assertEquals(0, UnifiedRepairMetricsManager.getMetrics(repairType).skippedTokenRangesCount.getValue().intValue());
        assertEquals(2, state.getSkippedTablesCount());
        assertEquals(2, UnifiedRepairMetricsManager.getMetrics(repairType).skippedTablesCount.getValue().intValue());

        // set it to higher value, and this time, the tables should not be skipped
        config.setRepairSSTableCountHigherThreshold(repairType, beforeCount);
        state.setLastRepairTime(0);
        state.setSkippedTablesCount(0);
        state.setTotalMVTablesConsideredForRepair(0);
        UnifiedRepair.instance.repair(repairType);
        assertEquals(1, state.getTotalMVTablesConsideredForRepair());
        assertEquals(1, UnifiedRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        assertEquals(0, state.getSkippedTokenRangesCount());
        assertEquals(0, UnifiedRepairMetricsManager.getMetrics(repairType).skippedTokenRangesCount.getValue().intValue());
        assertEquals(1, state.getSkippedTablesCount());
        assertEquals(1, UnifiedRepairMetricsManager.getMetrics(repairType).skippedTablesCount.getValue().intValue());
    }

    @Test
    public void testGetRepairState()
    {
        assertEquals(0, UnifiedRepair.instance.repairStates.get(repairType).getRepairKeyspaceCount());

        UnifiedRepairState state = UnifiedRepair.instance.getRepairState(repairType);
        state.setRepairKeyspaceCount(100);

        assertEquals(100L, UnifiedRepair.instance.getRepairState(repairType).getRepairKeyspaceCount());
    }

    @Test
    public void testMetrics()
    {
        UnifiedRepairConfig config = UnifiedRepairService.instance.getUnifiedRepairConfig();
        config.setMVRepairEnabled(repairType, true);
        config.setRepairMinInterval(repairType, "0s");
        config.setUnifiedRepairTableMaxRepairTime(repairType, "0s");
        UnifiedRepair.timeFunc = () -> {
            timeFuncCalls++;
            return timeFuncCalls * 1000L;
        };
        UnifiedRepair.instance.repairStates.get(repairType).setLastRepairTime(1000L);

        UnifiedRepair.instance.repair(repairType);

        assertEquals(1, UnifiedRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        assertTrue(UnifiedRepairMetricsManager.getMetrics(repairType).nodeRepairTimeInSec.getValue() > 0);
        assertTrue(UnifiedRepairMetricsManager.getMetrics(repairType).clusterRepairTimeInSec.getValue() > 0);
        assertEquals(1, UnifiedRepairMetricsManager.getMetrics(repairType).repairTurnMyTurn.getCount());
        assertTrue(UnifiedRepairMetricsManager.getMetrics(repairType).skippedTokenRangesCount.getValue() > 0);
        assertEquals(0, UnifiedRepairMetricsManager.getMetrics(repairType).longestUnrepairedSec.getValue().intValue());

        config.setUnifiedRepairTableMaxRepairTime(repairType, String.valueOf(Integer.MAX_VALUE - 1) + 's');
        UnifiedRepair.instance.repairStates.put(repairType, unifiedRepairState);
        when(unifiedRepairState.getRepairRunnable(any(), any(), any(), anyBoolean()))
        .thenReturn(repairRunnable);
        when(unifiedRepairState.getFailedTokenRangesCount()).thenReturn(10);
        when(unifiedRepairState.getSucceededTokenRangesCount()).thenReturn(11);
        when(unifiedRepairState.getLongestUnrepairedSec()).thenReturn(10);

        UnifiedRepair.instance.repair(repairType);
        assertEquals(0, UnifiedRepairMetricsManager.getMetrics(repairType).skippedTokenRangesCount.getValue().intValue());
        assertTrue(UnifiedRepairMetricsManager.getMetrics(repairType).failedTokenRangesCount.getValue() > 0);
        assertTrue(UnifiedRepairMetricsManager.getMetrics(repairType).succeededTokenRangesCount.getValue() > 0);
        assertTrue(UnifiedRepairMetricsManager.getMetrics(repairType).longestUnrepairedSec.getValue() > 0);
    }

    @Test
    public void testRepairWaitsForRepairToFinishBeforeSchedullingNewSession() throws Exception
    {
        UnifiedRepairConfig config = UnifiedRepairService.instance.getUnifiedRepairConfig();
        config.setMVRepairEnabled(repairType, false);
        config.setRepairRetryBackoff("0s");
        when(unifiedRepairState.getRepairRunnable(any(), any(), any(), anyBoolean()))
        .thenReturn(repairRunnable);
        UnifiedRepair.instance.repairStates.put(repairType, unifiedRepairState);
        when(unifiedRepairState.getLastRepairTime()).thenReturn((long) 0);
        AtomicInteger resetWaitConditionCalls = new AtomicInteger();
        AtomicInteger waitForRepairCompletedCalls = new AtomicInteger();
        doAnswer(invocation -> {
            resetWaitConditionCalls.getAndIncrement();
            assertEquals("waitForRepairToComplete was called before resetWaitCondition",
                         resetWaitConditionCalls.get(), waitForRepairCompletedCalls.get() + 1);
            return null;
        }).when(unifiedRepairState).resetWaitCondition();
        doAnswer(invocation -> {
            waitForRepairCompletedCalls.getAndIncrement();
            assertEquals("resetWaitCondition was not called before waitForRepairToComplete",
                         resetWaitConditionCalls.get(), waitForRepairCompletedCalls.get());
            return null;
        }).when(unifiedRepairState).waitForRepairToComplete(config.getRepairSessionTimeout(repairType));

        UnifiedRepair.instance.repair(repairType);
        UnifiedRepair.instance.repair(repairType);
        UnifiedRepair.instance.repair(repairType);
    }

    @Test
    public void testDisabledUnifiedRepairForATableThroughTableLevelConfiguration()
    {
        Assert.assertTrue(cfm.params.unifiedRepair.get(UnifiedRepairConfig.RepairType.full).repairEnabled());
        Assert.assertTrue(cfm.params.unifiedRepair.get(UnifiedRepairConfig.RepairType.incremental).repairEnabled());
        Assert.assertFalse(cfmDisabledUnifiedRepair.params.unifiedRepair.get(UnifiedRepairConfig.RepairType.full).repairEnabled());
        Assert.assertFalse(cfmDisabledUnifiedRepair.params.unifiedRepair.get(UnifiedRepairConfig.RepairType.incremental).repairEnabled());

        UnifiedRepairConfig config = UnifiedRepairService.instance.getUnifiedRepairConfig();
        config.setRepairMinInterval(repairType, "0s");
        int disabledTablesRepairCountBefore = UnifiedRepair.instance.repairStates.get(repairType).getTotalDisabledTablesRepairCount();
        UnifiedRepair.instance.repair(repairType);
        int consideredTables = UnifiedRepair.instance.repairStates.get(repairType).getTotalTablesConsideredForRepair();
        Assert.assertNotSame(String.format("Expected total repaired tables > 0, actual value %s ", consideredTables),
                             consideredTables, 0);
        int disabledTablesRepairCountAfter = UnifiedRepair.instance.repairStates.get(repairType).getTotalDisabledTablesRepairCount();
        Assert.assertTrue(String.format("A table %s should be skipped from unified repair, expected value: %d, actual value %d ", TABLE_DISABLED_UNIFIED_REPAIR, disabledTablesRepairCountBefore + 1, disabledTablesRepairCountAfter),
                            disabledTablesRepairCountBefore < disabledTablesRepairCountAfter);
    }

    @Test
    public void testTokenRangesNoSplit()
    {
        Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRanges(KEYSPACE);
        assertEquals(1, tokens.size());
        List<Range<Token>> expectedToken = new ArrayList<>(tokens);

        List<RepairAssignment> assignments = new DefaultUnifiedRepairTokenSplitter().getRepairAssignments(repairType, true, KEYSPACE, Collections.singletonList(TABLE));
        assertEquals(1, assignments.size());
        assertEquals(expectedToken.get(0).left, assignments.get(0).getTokenRange().left);
        assertEquals(expectedToken.get(0).right, assignments.get(0).getTokenRange().right);
    }

    @Test
    public void testTableAttribute()
    {
        assertTrue(TableAttributes.validKeywords().contains("repair_full"));
        assertTrue(TableAttributes.validKeywords().contains("repair_incremental"));
    }

    @Test
    public void testDefaultUnifiedRepair()
    {
        Assert.assertTrue(cfm.params.unifiedRepair.get(UnifiedRepairConfig.RepairType.full).repairEnabled());
        Assert.assertTrue(cfm.params.unifiedRepair.get(UnifiedRepairConfig.RepairType.incremental).repairEnabled());
        Assert.assertFalse(cfmDisabledUnifiedRepair.params.unifiedRepair.get(UnifiedRepairConfig.RepairType.full).repairEnabled());
        Assert.assertFalse(cfmDisabledUnifiedRepair.params.unifiedRepair.get(UnifiedRepairConfig.RepairType.incremental).repairEnabled());
    }

    @Test
    public void testRepairShufflesKeyspacesAndTables()
    {
        AtomicInteger shuffleKeyspacesCall = new AtomicInteger();
        AtomicInteger shuffleTablesCall = new AtomicInteger();
        UnifiedRepair.shuffleFunc = (List<?> list) -> {
            if (!list.isEmpty())
            {
                assertTrue(list.get(0) instanceof Keyspace || list.get(0) instanceof String);
                if (list.get(0) instanceof Keyspace)
                {
                    shuffleKeyspacesCall.getAndIncrement();
                    assertFalse(list.isEmpty());
                }
                else if (list.get(0) instanceof String)
                {
                    shuffleTablesCall.getAndIncrement();
                }
            }
        };

        UnifiedRepairConfig config = UnifiedRepairService.instance.getUnifiedRepairConfig();
        config.setRepairMinInterval(repairType, "0s");
        UnifiedRepair.instance.repair(repairType);

        assertEquals(1, shuffleKeyspacesCall.get());
        assertEquals(5, shuffleTablesCall.get());
    }

    @Test
    public void testRepairTakesLastRepairTimeFromDB()
    {
        UnifiedRepairConfig config = UnifiedRepairService.instance.getUnifiedRepairConfig();
        config.setMVRepairEnabled(repairType, true);
        long lastRepairTime = System.currentTimeMillis() - 1000;
        UnifiedRepairUtils.insertNewRepairHistory(repairType, 0, lastRepairTime);
        UnifiedRepair.instance.repairStates.get(repairType).setLastRepairTime(0);
        config.setRepairMinInterval(repairType, "1h");

        UnifiedRepair.instance.repair(repairType);

        // repair scheduler should not attempt to run repair as last repair time in DB is current time - 1s
        assertEquals(0, UnifiedRepair.instance.repairStates.get(repairType).getTotalTablesConsideredForRepair());
        // repair scheduler should load the repair time from the DB
        assertEquals(lastRepairTime, UnifiedRepair.instance.repairStates.get(repairType).getLastRepairTime());
    }

    @Test
    public void testRepairMaxRetries()
    {
        when(unifiedRepairState.getRepairRunnable(any(), any(), any(), anyBoolean())).thenReturn(repairRunnable);
        when(unifiedRepairState.isSuccess()).thenReturn(false);
        UnifiedRepairConfig config = UnifiedRepairService.instance.getUnifiedRepairConfig();
        AtomicInteger sleepCalls = new AtomicInteger();
        UnifiedRepair.sleepFunc = (Long duration, TimeUnit unit) -> {
            sleepCalls.getAndIncrement();
            assertEquals(TimeUnit.SECONDS, unit);
            assertEquals(config.getRepairRetryBackoff().toSeconds(), (long) duration);
        };
        config.setRepairMinInterval(repairType, "0s");
        UnifiedRepair.instance.repairStates.put(repairType, unifiedRepairState);

        UnifiedRepair.instance.repair(repairType);

        //system_auth.role_permissions,system_auth.network_permissions,system_auth.role_members,system_auth.roles,
        // system_auth.resource_role_permissons_index,system_traces.sessions,system_traces.events,ks.tbl,
        // system_distributed.unified_repair_priority,system_distributed.repair_history,system_distributed.unified_repair_history,
        // system_distributed.view_build_status,system_distributed.parent_repair_history,system_distributed.partition_denylist
        int exptedTablesGoingThroughRepair = 18;
        assertEquals(config.getRepairMaxRetries()*exptedTablesGoingThroughRepair, sleepCalls.get());
        verify(unifiedRepairState, Mockito.times(1)).setSucceededTokenRangesCount(0);
        verify(unifiedRepairState, Mockito.times(1)).setSkippedTokenRangesCount(0);
        verify(unifiedRepairState, Mockito.times(1)).setFailedTokenRangesCount(exptedTablesGoingThroughRepair);
    }

    @Test
    public void testRepairSuccessAfterRetry()
    {
        when(unifiedRepairState.getRepairRunnable(any(), any(), any(), anyBoolean())).thenReturn(repairRunnable);

        UnifiedRepairConfig config = UnifiedRepairService.instance.getUnifiedRepairConfig();
        AtomicInteger sleepCalls = new AtomicInteger();
        UnifiedRepair.sleepFunc = (Long duration, TimeUnit unit) -> {
            sleepCalls.getAndIncrement();
            assertEquals(TimeUnit.SECONDS, unit);
            assertEquals(config.getRepairRetryBackoff().toSeconds(), (long) duration);
        };
        when(unifiedRepairState.isSuccess()).then((invocationOnMock) -> {
            if (sleepCalls.get() == 0) {
                return false;
            }
            return true;
        });
        config.setRepairMinInterval(repairType, "0s");
        UnifiedRepair.instance.repairStates.put(repairType, unifiedRepairState);
        UnifiedRepair.instance.repair(repairType);

        assertEquals(1, sleepCalls.get());
        verify(unifiedRepairState, Mockito.times(1)).setSucceededTokenRangesCount(18);
        verify(unifiedRepairState, Mockito.times(1)).setSkippedTokenRangesCount(0);
        verify(unifiedRepairState, Mockito.times(1)).setFailedTokenRangesCount(0);
    }

    @Test
    public void testRepairThrowsForIRWithMVReplay()
    {
        UnifiedRepair.instance.setup();
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(true);

        if (repairType == UnifiedRepairConfig.RepairType.incremental)
        {
            try
            {
                UnifiedRepair.instance.repair(repairType);
                fail("Expected ConfigurationException");
            }
            catch (ConfigurationException ignored)
            {
            }
        }
        else
        {
            UnifiedRepair.instance.repair(repairType);
        }
    }


    @Test
    public void testRepairThrowsForIRWithCDCReplay()
    {
        UnifiedRepair.instance.setup();
        DatabaseDescriptor.setCDCOnRepairEnabled(true);

        if (repairType == UnifiedRepairConfig.RepairType.incremental)
        {
            try
            {
                UnifiedRepair.instance.repair(repairType);
                fail("Expected ConfigurationException");
            }
            catch (ConfigurationException ignored)
            {
            }
        }
        else
        {
            UnifiedRepair.instance.repair(repairType);
        }
    }
}
