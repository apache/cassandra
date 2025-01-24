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

package org.apache.cassandra.repair;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;
import org.apache.cassandra.metrics.AutoRepairMetricsV2;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.repair.state.AutoRepairState;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.ProgressListener;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;

import static org.apache.cassandra.Util.setAutoRepairEnabled;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.repair.AutoRepairUtilsV2.RepairTurn.NOT_MY_TURN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class AutoRepairV2ParameterizedTest extends CQLTester
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";
    private static final String MV = "mv";
    private static TableMetadata cfm;
    private static Keyspace keyspace;
    private static int timeFuncCalls;
    @Mock
    ScheduledExecutorPlus mockExecutor;
    @Mock
    AutoRepairState autoRepairState;
    @Mock
    RepairRunnable repairRunnable;
    private static AutoRepairConfig defaultConfig;


    @Parameterized.Parameter()
    public AutoRepairConfig.RepairType repairType;

    @Parameterized.Parameters(name = "repairType={0}")
    public static Collection<AutoRepairConfig.RepairType> repairTypes()
    {
        return Arrays.asList(AutoRepairConfig.RepairType.values());
    }

    @BeforeClass
    public static void setupClass() throws Exception
    {
        setAutoRepairEnabled(true);
        requireNetwork();
        AutoRepairUtilsV2.setup();

        cfm = TableMetadata.builder(KEYSPACE, TABLE)
                           .addPartitionKeyColumn("k", UTF8Type.instance)
                           .addStaticColumn("s", UTF8Type.instance)
                           .addClusteringColumn("i", IntegerType.instance)
                           .addRegularColumn("v", UTF8Type.instance)
                           .build();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), cfm);
        cfm = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        keyspace = Keyspace.open(KEYSPACE);
        DatabaseDescriptor.setMaterializedViewsEnabled(true);
        QueryProcessor.executeInternal(String.format("CREATE MATERIALIZED VIEW %s.%s AS SELECT i, k from %s.%s " +
                                                     "WHERE k IS NOT null AND i IS NOT null PRIMARY KEY (i, k)", KEYSPACE, MV, KEYSPACE, TABLE));

        DatabaseDescriptor.setMaterializedViewsEnabled(false);
        DatabaseDescriptor.setCDCEnabled(false);
    }

    @Before
    public void setup()
    {
        MockitoAnnotations.initMocks(this);

        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).truncateBlocking();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).disableAutoCompaction();

        Keyspace.open(KEYSPACE).getColumnFamilyStore(MV).truncateBlocking();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(MV).disableAutoCompaction();

        Keyspace.open(SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME).getColumnFamilyStore(AutoRepairKeyspace.AUTO_REPAIR_PRIORITY_V2).truncateBlocking();
        Keyspace.open(SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME).getColumnFamilyStore(AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2).truncateBlocking();


        AutoRepairV2.instance = new AutoRepairV2();
        executeCQL();

        timeFuncCalls = 0;
        AutoRepairV2.timeFunc = System::currentTimeMillis;
        AutoRepairV2.sleepFunc = (Long startTime, TimeUnit unit) -> {};
        resetCounters();
        resetConfig();
    }

    @After
    public void tearDown()
    {
        System.clearProperty("cassandra.streaming.requires_view_build_during_repair");
        System.clearProperty("cassandra.streaming.requires_cdc_replay");
    }

    private void resetCounters()
    {
        AutoRepairMetricsV2 metrics = AutoRepairMetricsManager.getMetrics(repairType);
        Metrics.removeMatching((name, metric) -> name.startsWith("repairTurn"));
        metrics.repairTurnMyTurn = Metrics.counter(String.format("repairTurnMyTurn-%s", repairType));
        metrics.repairTurnMyTurnForceRepair = Metrics.counter(String.format("repairTurnMyTurnForceRepair-%s", repairType));
        metrics.repairTurnMyTurnDueToPriority = Metrics.counter(String.format("repairTurnMyTurnDueToPriority-%s", repairType));
    }

    private void resetConfig()
    {
        // prepare a fresh default config
        defaultConfig = new AutoRepairConfig(true);
        for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values())
        {
            defaultConfig.setAutoRepairEnabled(repairType, true);
            defaultConfig.setMVRepairEnabled(repairType, false);
        }

        // reset the AutoRepairService config to default
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.repair_type_overrides = defaultConfig.repair_type_overrides;
        config.global_settings = defaultConfig.global_settings;
        config.history_clear_delete_hosts_buffer_in_sec = defaultConfig.history_clear_delete_hosts_buffer_in_sec;
        config.repair_task_min_duration = new DurationSpec.LongSecondsBound("0s");
    }

    private void executeCQL()
    {
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s) VALUES ('k', 's')");
        QueryProcessor.executeInternal("SELECT s FROM ks.tbl WHERE k='k'");
        Keyspace.open(SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME)
                .getColumnFamilyStore(AutoRepairKeyspace.AUTO_REPAIR_PRIORITY_V2)
                .forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }


    @Test(expected = ConfigurationException.class)
    public void testRepairAsyncWithRepairTypeDisabled()
    {
        AutoRepairService.instance.getAutoRepairConfig().setAutoRepairEnabled(repairType, false);

        AutoRepairV2.instance.repairAsync(repairType, 60);
    }

    @Test
    public void testRepairAsync()
    {
        AutoRepairV2.instance.repairExecutors.put(repairType, mockExecutor);

        AutoRepairV2.instance.repairAsync(repairType, 60);

        verify(mockExecutor, times(1)).submit(any(Runnable.class));
    }

    @Test
    public void testRepairTurn()
    {
        UUID myId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertTrue("Expected my turn for the repair", AutoRepairUtilsV2.myTurnToRunRepair(repairType, myId) != NOT_MY_TURN);
    }

    @Test
    public void testRepair() throws Throwable
    {
        AutoRepairService.instance.getAutoRepairConfig().setRepairMinIntervalInHours(repairType, -1);
        AutoRepairV2.instance.repair(repairType, 0);
        assertEquals(0, AutoRepairV2.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        long lastRepairTime = AutoRepairV2.instance.repairStates.get(repairType).getLastRepairTime();
        //if repair was done then lastRepairTime should be non-zero
        Assert.assertTrue(String.format("Expected lastRepairTime > 0, actual value lastRepairTime %d",
                                        lastRepairTime), lastRepairTime > 0);
    }

    @Test
    public void testTooFrequentRepairs()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        //in the first round let repair run
        config.setRepairMinIntervalInHours(repairType, -1);
        AutoRepairV2.instance.repair(repairType, 0);
        long lastRepairTime1 = AutoRepairV2.instance.repairStates.get(repairType).getLastRepairTime();
        int consideredTables = AutoRepairV2.instance.repairStates.get(repairType).getTotalTablesConsideredForRepair();
        Assert.assertNotSame(String.format("Expected total repaired tables > 0, actual value %s ", consideredTables),
                             consideredTables, 0);

        //if repair was done in last 24 hours then it should not trigger another repair
        config.setRepairMinIntervalInHours(repairType, 24);
        AutoRepairV2.instance.repair(repairType, 0);
        long lastRepairTime2 = AutoRepairV2.instance.repairStates.get(repairType).getLastRepairTime();
        Assert.assertEquals(String.format("Expected repair time to be same, actual value lastRepairTime1 %d, " +
                                          "lastRepairTime2 %d", lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime2);
        assertEquals(0, AutoRepairV2.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
    }

    @Test
    public void testNonFrequentRepairs() throws Throwable
    {
        Integer prevMetricsCount = AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue();
        AutoRepairState state = AutoRepairV2.instance.repairStates.get(repairType);
        long prevCount = state.getTotalMVTablesConsideredForRepair();
        AutoRepairService.instance.getAutoRepairConfig().setRepairMinIntervalInHours(repairType, -1);
        AutoRepairV2.instance.repair(repairType, 0);
        long lastRepairTime1 = AutoRepairV2.instance.repairStates.get(repairType).getLastRepairTime();
        Assert.assertTrue(String.format("Expected lastRepairTime1 > 0, actual value lastRepairTime1 %d",
                                        lastRepairTime1), lastRepairTime1 > 0);
        UUID myId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertTrue("Expected my turn for the repair",
                          AutoRepairUtilsV2.myTurnToRunRepair(repairType, myId) != NOT_MY_TURN);
        AutoRepairV2.instance.repair(repairType, 0);
        long lastRepairTime2 = AutoRepairV2.instance.repairStates.get(repairType).getLastRepairTime();
        Assert.assertNotSame(String.format("Expected repair time to be same, actual value lastRepairTime1 %d, " +
                                           "lastRepairTime2 ", lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime2);
        assertEquals(prevCount, state.getTotalMVTablesConsideredForRepair());
        assertEquals(prevMetricsCount, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue());
    }

    @Test
    public void testGetPriorityHosts() throws Throwable
    {
        Integer prevMetricsCount = AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue();
        AutoRepairState state = AutoRepairV2.instance.repairStates.get(repairType);
        long prevCount = state.getTotalMVTablesConsideredForRepair();
        AutoRepairService.instance.getAutoRepairConfig().setRepairMinIntervalInHours(repairType, -1);
        Assert.assertSame(String.format("Priority host count is not same, actual value %d, expected value %d",
                                        AutoRepairUtilsV2.getPriorityHosts(repairType).size(), 0), AutoRepairUtilsV2.getPriorityHosts(repairType).size(), 0);
        UUID myId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertTrue("Expected my turn for the repair", AutoRepairUtilsV2.myTurnToRunRepair(repairType, myId) !=
                                                             NOT_MY_TURN);
        AutoRepairV2.instance.repair(repairType, 0);
        AutoRepairUtilsV2.addPriorityHosts(repairType, Sets.newHashSet(FBUtilities.getBroadcastAddressAndPort()));
        AutoRepairV2.instance.repair(repairType, 0);
        Assert.assertSame(String.format("Priority host count is not same actual value %d, expected value %d",
                                        AutoRepairUtilsV2.getPriorityHosts(repairType).size(), 0), AutoRepairUtilsV2.getPriorityHosts(repairType).size(), 0);
        assertEquals(prevCount, state.getTotalMVTablesConsideredForRepair());
        assertEquals(prevMetricsCount, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue());
    }

    @Test
    public void testCheckAutoRepairStartStop() throws Throwable
    {
        Integer prevMetricsCount = AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue();
        AutoRepairState state = AutoRepairV2.instance.repairStates.get(repairType);
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        long prevCount = state.getTotalMVTablesConsideredForRepair();
        config.setRepairMinIntervalInHours(repairType, -1);
        config.setAutoRepairEnabled(repairType, false);
        long lastRepairTime1 = AutoRepairV2.instance.repairStates.get(repairType).getLastRepairTime();
        AutoRepairV2.instance.repair(repairType, 0);
        long lastRepairTime2 = AutoRepairV2.instance.repairStates.get(repairType).getLastRepairTime();
        //Since repair has not happened, both the last repair times should be same
        Assert.assertEquals(String.format("Expected lastRepairTime1 %d, and lastRepairTime2 %d to be same",
                                          lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime2);

        config.setAutoRepairEnabled(repairType, true);
        AutoRepairV2.instance.repair(repairType, 0);
        //since repair is done now, so lastRepairTime1/lastRepairTime2 and lastRepairTime3 should not be same
        long lastRepairTime3 = AutoRepairV2.instance.repairStates.get(repairType).getLastRepairTime();
        Assert.assertNotSame(String.format("Expected lastRepairTime1 %d, and lastRepairTime3 %d to be not same",
                                           lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime3);
        assertEquals(prevCount, state.getTotalMVTablesConsideredForRepair());
        assertEquals(prevMetricsCount, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue());
    }

    @Test
    public void testRepairPrimaryRangesByDefault()
    {
        Assert.assertTrue("Expected primary range repair only",
                          AutoRepairService.instance.getAutoRepairConfig().getRepairPrimaryTokenRangeOnly(repairType));
    }

    @Test
    public void testGetAllMVs()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setMVRepairEnabled(repairType, false);
        assertFalse(config.getMVRepairEnabled(repairType));
        assertEquals(0, AutoRepairUtilsV2.getAllMVs(repairType, keyspace, cfm).size());

        config.setMVRepairEnabled(repairType, true);

        assertTrue(config.getMVRepairEnabled(repairType));
        assertEquals(Arrays.asList(MV), AutoRepairUtilsV2.getAllMVs(repairType, keyspace, cfm));
        config.setMVRepairEnabled(repairType, false);
    }


    @Test
    public void testMVRepair()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setMVRepairEnabled(repairType, true);
        config.setRepairMinIntervalInHours(repairType, -1);
        AutoRepairV2.instance.repairStates.get(repairType).setLastRepairTime(System.currentTimeMillis());
        AutoRepairV2.instance.repair(repairType, 0);
        assertEquals(1, AutoRepairV2.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());

        config.setMVRepairEnabled(repairType, false);
        AutoRepairV2.instance.repairStates.get(repairType).setLastRepairTime(System.currentTimeMillis());
        AutoRepairV2.instance.repair(repairType, 0);
        assertEquals(0, AutoRepairV2.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());

        config.setMVRepairEnabled(repairType, true);
        AutoRepairV2.instance.repairStates.get(repairType).setLastRepairTime(System.currentTimeMillis());
        AutoRepairV2.instance.repair(repairType, 0);
        assertEquals(1, AutoRepairV2.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
    }

    @Test
    public void testSkipRepairSSTableCountHigherThreshold()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        AutoRepairState state = AutoRepairV2.instance.repairStates.get(repairType);
        ColumnFamilyStore cfsBaseTable = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        ColumnFamilyStore cfsMVTable = Keyspace.open(KEYSPACE).getColumnFamilyStore(MV);
        Set<SSTableReader> preBaseTable = cfsBaseTable.getLiveSSTables();
        Set<SSTableReader> preMVTable = cfsBaseTable.getLiveSSTables();
        config.setRepairMinIntervalInHours(repairType, -1);

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
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).skippedTokenRangesCount.getValue().intValue());
        state.setLastRepairTime(0);
        AutoRepairV2.instance.repair(repairType, 0);
        assertEquals(1, state.getTotalMVTablesConsideredForRepair());
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        // skipping one time for the base table and another time for MV table
        assertEquals(2, state.getSkippedTokenRangesCount());
        assertEquals(2, AutoRepairMetricsManager.getMetrics(repairType).skippedTokenRangesCount.getValue().intValue());

        // set it to higher value, and this time, the tables should not be skipped
        config.setRepairSSTableCountHigherThreshold(repairType, 11);
        config.setRepairSSTableCountHigherThreshold(repairType, beforeCount);
        state.setLastRepairTime(0);
        AutoRepairV2.instance.repair(repairType, 0);
        assertEquals(1, state.getTotalMVTablesConsideredForRepair());
        assertEquals(0, state.getSkippedTokenRangesCount());
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).skippedTokenRangesCount.getValue().intValue());
    }

    @Test
    public void testGetRepairState()
    {
        assertEquals(0, AutoRepairV2.instance.repairStates.get(repairType).getRepairKeyspaceCount());

        AutoRepairState state = AutoRepairV2.instance.getRepairState(repairType);
        state.setRepairKeyspaceCount(100);

        assertEquals(100L, AutoRepairV2.instance.getRepairState(repairType).getRepairKeyspaceCount());
    }

    @Test
    public void testMetrics()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setMVRepairEnabled(repairType, true);
        config.setRepairMinIntervalInHours(repairType, -1);
        config.setAutoRepairTableMaxRepairTimeInSec(repairType, 0);
        AutoRepairV2.timeFunc = () -> {
            timeFuncCalls++;
            return timeFuncCalls * 1000L;
        };
        AutoRepairV2.instance.repairStates.get(repairType).setLastRepairTime(1000L);

        AutoRepairV2.instance.repair(repairType, 0);

        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        assertTrue(AutoRepairMetricsManager.getMetrics(repairType).nodeRepairTimeInSec.getValue() > 0);
        assertTrue(AutoRepairMetricsManager.getMetrics(repairType).clusterRepairTimeInSec.getValue() > 0);
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).repairTurnMyTurn.getCount());
        assertTrue(AutoRepairMetricsManager.getMetrics(repairType).skippedTokenRangesCount.getValue() > 0);
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).longestUnrepairedSec.getValue().intValue());

        config.setAutoRepairTableMaxRepairTimeInSec(repairType, Long.MAX_VALUE);
        AutoRepairV2.instance.repairStates.put(repairType, autoRepairState);
        when(autoRepairState.getRepairRunnable(any(), any(), any(), anyBoolean(), any()))
        .thenReturn(repairRunnable);
        doAnswer(invocation -> {
            invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0));
            return null;
        }).when(repairRunnable).addProgressListener(any());
        when(autoRepairState.getFailedTokenRangesCount()).thenReturn(10);
        when(autoRepairState.getSucceededTokenRangesCount()).thenReturn(11);
        when(autoRepairState.getLongestUnrepairedSec()).thenReturn(10);

        AutoRepairV2.instance.repair(repairType, 0);
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).skippedTokenRangesCount.getValue().intValue());
        assertTrue(AutoRepairMetricsManager.getMetrics(repairType).failedTokenRangesCount.getValue() > 0);
        assertTrue(AutoRepairMetricsManager.getMetrics(repairType).succeededTokenRangesCount.getValue() > 0);
        assertTrue(AutoRepairMetricsManager.getMetrics(repairType).longestUnrepairedSec.getValue() > 0);
    }

    @Test
    public void testRepairWaitsForRepairToFinishBeforeSchedullingNewSession() throws Exception
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setMVRepairEnabled(repairType, false);
        AutoRepairV2.instance.repairStates.put(repairType, autoRepairState);
        when(autoRepairState.getLastRepairTime()).thenReturn((long) 0);
        AtomicInteger getRepairRunnableCalls = new AtomicInteger();
        AtomicReference<AutoRepairV2.RepairProgressListener> prevListener = new AtomicReference<>();
        doAnswer(invocation -> {
            if (getRepairRunnableCalls.getAndIncrement() > 0)
            {
                // progress listener from previous repair should be signalled before starting new repair
                assertTrue(prevListener.get().condition.isSignalled());
            }
            getRepairRunnableCalls.incrementAndGet();
            return repairRunnable;
        }).when(autoRepairState).getRepairRunnable(any(), any(), any(), anyBoolean(), any());
        doAnswer(invocation -> {
            // sending out a COMPLETE event with a 10ms delay
            Executors.newScheduledThreadPool(1).schedule(() -> {
                invocation.getArgument(0, AutoRepairV2.RepairProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0));
            }, 10, TimeUnit.MILLISECONDS);
            return null;
        }).when(repairRunnable).addProgressListener(any());

        AutoRepairV2.instance.repair(repairType, 0);
        AutoRepairV2.instance.repair(repairType, 0);
        AutoRepairV2.instance.repair(repairType, 0);

    }

    @Test
    public void testRepairDCGroups()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setRepairMinIntervalInHours(repairType, -1);
        config.setDCGroups(repairType, Sets.newHashSet(DatabaseDescriptor.getLocalDataCenter(), "dc2"));
        AutoRepairService.instance.getAutoRepairConfig().setRepairMinIntervalInHours(repairType, -1);
        AutoRepairV2.instance.repair(repairType, 0);
        assertEquals(0, AutoRepairV2.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        long lastRepairTime = AutoRepairV2.instance.repairStates.get(repairType).getLastRepairTime();
        //if repair was done then lastRepairTime should be non-zero
        Assert.assertTrue(String.format("Expected lastRepairTime > 0, actual value lastRepairTime %d",
                                        lastRepairTime), lastRepairTime > 0);
    }

    @Test
    public void testRepairShufflesKeyspacesAndTables()
    {
        AtomicInteger shuffleKeyspacesCall = new AtomicInteger();
        AtomicInteger shuffleTablesCall = new AtomicInteger();
        AutoRepairV2.shuffleFunc = (List<?> list) -> {
            assertFalse(list.isEmpty());
            assertTrue(list.get(0) instanceof Keyspace || list.get(0) instanceof String);
            if (list.get(0) instanceof Keyspace)
            {
                shuffleKeyspacesCall.getAndIncrement();
            }
            else if (list.get(0) instanceof String)
            {
                shuffleTablesCall.getAndIncrement();
            }
        };
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setRepairMinIntervalInHours(repairType, -1);

        AutoRepairV2.instance.repair(repairType, 0);

        assertEquals(1, shuffleKeyspacesCall.get());
        assertEquals(4, shuffleTablesCall.get());
    }

    @Test
    public void testRepairTakesLastRepairTimeFromDB()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setMVRepairEnabled(repairType, true);
        long lastRepairTime = System.currentTimeMillis() - 1000;
        AutoRepairUtilsV2.insertNewRepairHistory(repairType, 0, lastRepairTime);
        AutoRepairV2.instance.repairStates.get(repairType).setLastRepairTime(0);
        config.setRepairMinIntervalInHours(repairType, 1);

        AutoRepairV2.instance.repair(repairType, 0);

        // repair scheduler should not attempt to run repair as last repair time in DB is current time - 1s
        assertEquals(0, AutoRepairV2.instance.repairStates.get(repairType).getTotalTablesConsideredForRepair());
        // repair scheduler should load the repair time from the DB
        assertEquals(lastRepairTime, AutoRepairV2.instance.repairStates.get(repairType).getLastRepairTime());
    }

    @Test
    public void testRepairMaxRetries()
    {
        when(autoRepairState.getRepairRunnable(any(), any(), any(), anyBoolean(), any())).thenReturn(repairRunnable);
        doAnswer(invocation -> {
            invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.ERROR, 0, 0));
            return null;
        }).when(repairRunnable).addProgressListener(any());
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        AtomicInteger sleepCalls = new AtomicInteger();
        AutoRepairV2.sleepFunc = (Long duration, TimeUnit unit) -> {
            sleepCalls.getAndIncrement();
            assertEquals(TimeUnit.SECONDS, unit);
            assertEquals(config.getRepairRetryBackoffInSec(), (long) duration);
        };
        config.setRepairMinIntervalInHours(repairType, -1);
        config.setRepairOnlyKeyspaces(repairType, KEYSPACE);
        AutoRepairV2.instance.repairStates.put(repairType, autoRepairState);

        AutoRepairV2.instance.repair(repairType, 0);

        assertEquals(config.getRepairMaxRetries(), sleepCalls.get());
        verify(autoRepairState, times(1)).setSucceededTokenRangesCount(0);
        verify(autoRepairState, times(1)).setSkippedTokenRangesCount(0);
        verify(autoRepairState, times(1)).setFailedTokenRangesCount(1);
    }

    @Test
    public void testRepairSuccessAfterRetry()
    {
        when(autoRepairState.getRepairRunnable(any(), any(), any(), anyBoolean(), any())).thenReturn(repairRunnable);

        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        AtomicInteger sleepCalls = new AtomicInteger();
        AutoRepairV2.sleepFunc = (Long duration, TimeUnit unit) -> {
            sleepCalls.getAndIncrement();
            assertEquals(TimeUnit.SECONDS, unit);
            assertEquals(config.getRepairRetryBackoffInSec(), (long) duration);
        };
        doAnswer(invocation -> {
            if (sleepCalls.get() == 0) {
                invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.ERROR, 0, 0));
            }
            else {
                invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0));
            }

            return null;
        }).when(repairRunnable).addProgressListener(any());
        config.setRepairMinIntervalInHours(repairType, -1);
        config.setRepairOnlyKeyspaces(repairType, KEYSPACE);
        config.setRepairMaxRetries(1);
        AutoRepairV2.instance.repairStates.put(repairType, autoRepairState);

        AutoRepairV2.instance.repair(repairType, 0);

        assertEquals(1, sleepCalls.get());
        verify(autoRepairState, times(1)).setSucceededTokenRangesCount(1);
        verify(autoRepairState, times(1)).setSkippedTokenRangesCount(0);
        verify(autoRepairState, times(1)).setFailedTokenRangesCount(0);
    }

    @Test
    public void testRepairThrowsForIRWithMVReplay()
    {
        AutoRepairV2.instance.setup();
        System.setProperty("cassandra.streaming.requires_view_build_during_repair", "true");

        if (repairType == AutoRepairConfig.RepairType.incremental)
        {
            try
            {
                AutoRepairV2.instance.repair(repairType, 0);
                fail("Expected ConfigurationException");
            }
            catch (ConfigurationException ignored)
            {
            }
        }
        else
        {
            AutoRepairV2.instance.repair(repairType, 0);
        }
    }

    @Test
    public void testRepairThrowsForIRWithCDCReplay()
    {
        AutoRepairV2.instance.setup();
        System.setProperty("cassandra.streaming.requires_cdc_replay", "true");

        if (repairType == AutoRepairConfig.RepairType.incremental)
        {
            try
            {
                AutoRepairV2.instance.repair(repairType, 0);
                fail("Expected ConfigurationException");
            }
            catch (ConfigurationException ignored)
            {
            }
        }
        else
        {
            AutoRepairV2.instance.repair(repairType, 0);
        }
    }

    @Test
    public void testSoakAfterImmediateRepair()
    {
        when(autoRepairState.getRepairRunnable(any(), any(), any(), anyBoolean(), any())).thenReturn(repairRunnable);
        doAnswer(invocation -> {
            invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0));
            return null;
        }).when(repairRunnable).addProgressListener(any());
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.repair_task_min_duration = new DurationSpec.LongSecondsBound("1s");
        AtomicInteger sleepCalls = new AtomicInteger();
        AutoRepairV2.sleepFunc = (Long duration, TimeUnit unit) -> {
            sleepCalls.getAndIncrement();
            assertEquals(TimeUnit.MILLISECONDS, unit);
            assertTrue(config.getRepairTaskMinDuration().toMilliseconds() >= duration);
        };
        config.setRepairMinIntervalInHours(repairType, -1);
        config.setRepairOnlyKeyspaces(repairType, KEYSPACE);
        AutoRepairV2.instance.repairStates.put(repairType, autoRepairState);

        AutoRepairV2.instance.repair(repairType, 0);

        assertEquals(1, sleepCalls.get());
        verify(autoRepairState, times(1)).setSucceededTokenRangesCount(1);
        verify(autoRepairState, times(1)).setSkippedTokenRangesCount(0);
        verify(autoRepairState, times(1)).setFailedTokenRangesCount(0);
    }

    @Test
    public void testNoSoakAfterRepair()
    {
        when(autoRepairState.getRepairRunnable(any(), any(), any(), anyBoolean(), any())).thenReturn(repairRunnable);
        doAnswer(invocation -> {
            invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0));
            return null;
        }).when(repairRunnable).addProgressListener(any());
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.repair_task_min_duration = new DurationSpec.LongSecondsBound("0s");
        AutoRepairV2.sleepFunc = (Long duration, TimeUnit unit) -> {
            fail("Should not sleep after repair");
        };
        config.setRepairMinIntervalInHours(repairType, -1);
        config.setRepairOnlyKeyspaces(repairType, KEYSPACE);
        AutoRepairV2.instance.repairStates.put(repairType, autoRepairState);

        AutoRepairV2.instance.repair(repairType, 0);

        verify(autoRepairState, times(1)).setSucceededTokenRangesCount(1);
        verify(autoRepairState, times(1)).setSkippedTokenRangesCount(0);
        verify(autoRepairState, times(1)).setFailedTokenRangesCount(0);
    }

    @Test
    public void testSchedulerIgnoresErrorsFromUnrelatedRepairRunables()
    {
        RepairOption options = new RepairOption(RepairParallelism.PARALLEL, true, repairType == AutoRepairConfig.RepairType.incremental, false,
                                               AutoRepairService.instance.getAutoRepairConfig().getRepairThreads(repairType), Set.of(),
                                               false, false, false, PreviewKind.NONE, false, true, false, false);
        AutoRepairState repairState = AutoRepairV2.instance.repairStates.get(repairType);
        AutoRepairState spyState = spy(repairState);
        AtomicReference<RepairRunnable> failingRepair = new AtomicReference<>(new RepairRunnable(StorageService.instance, StorageService.nextRepairCommand.incrementAndGet(), options, keyspace.getName()));
        AtomicReference<AutoRepairV2.RepairProgressListener> failingListener = new AtomicReference<>();
        AtomicReference<RepairRunnable> succeedingRepair = new AtomicReference<>(new RepairRunnable(StorageService.instance, StorageService.nextRepairCommand.incrementAndGet(), options, keyspace.getName()));
        AtomicInteger repairRunableCalls = new AtomicInteger();
        doAnswer((InvocationOnMock inv ) -> {
            RepairRunnable runnable = spy(repairState.getRepairRunnable(inv.getArgument(0), inv.getArgument(1), inv.getArgument(2),
                                                                    inv.getArgument(3), inv.getArgument(4)));
            if (repairRunableCalls.getAndIncrement() == 0)
            {
                // this will be used for first repair job
                doAnswer(invocation -> {
                    // repair runnable for the first repair job will immediately fail
                    failingListener.set(invocation.getArgument(0, AutoRepairV2.RepairProgressListener.class));
                    invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.ERROR, 0, 0));
                    return null;
                }).when(runnable).addProgressListener(any());
                failingRepair.set(runnable);
            }
            else
            {
                // this will be used for subsequent repair jobs
                doAnswer(invocation -> {
                    if (repairRunableCalls.get() > 0)
                    {
                        // repair runnable for the subsequent repair jobs will immediately complete
                        invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0));

                    }
                    // repair runnable for the first repair job will continue firing ERROR events
                    failingListener.get().progress("test", new ProgressEvent(ProgressEventType.ERROR, 0, 0));
                    return null;
                }).when(runnable).addProgressListener(any());
                succeedingRepair.set(runnable);
            }
            return runnable;
        }).when(spyState).getRepairRunnable(any(), any(), any(), anyBoolean(), any());
        when(spyState.getLastRepairTime()).thenReturn((long) 0);
        AutoRepairService.instance.getAutoRepairConfig().setRepairMaxRetries(0);
        AutoRepairV2.instance.repairStates.put(repairType, spyState);

        AutoRepairV2.instance.repair(repairType, 0);

        assertEquals(1, (int) AutoRepairMetricsManager.getMetrics(repairType).failedTokenRangesCount.getValue());
        // only the first repair job should have failed despite it continuously firing ERROR events
        verify(spyState, times(1)).setFailedTokenRangesCount(1);
    }

    @Test
    public void testProgressError()
    {
        AutoRepairV2.RepairProgressListener listener = new AutoRepairV2.RepairProgressListener(repairType);

        listener.progress("test", new ProgressEvent(ProgressEventType.ERROR, 0, 0, "test"));

        assertFalse(listener.success);
        assertTrue(listener.condition.isSignalled());
    }

    @Test
    public void testProgressProgress()
    {
        AutoRepairV2.RepairProgressListener listener = new AutoRepairV2.RepairProgressListener(repairType);

        listener.progress("test", new ProgressEvent(ProgressEventType.PROGRESS, 0, 0, "test"));

        assertFalse(listener.success);
        assertFalse(listener.condition.isSignalled());
    }

    @Test
    public void testProgresComplete()
    {
        AutoRepairV2.RepairProgressListener listener = new AutoRepairV2.RepairProgressListener(repairType);

        listener.progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0, "test"));

        assertTrue(listener.success);
        assertTrue(listener.condition.isSignalled());
    }

    @Test
    public void testAwait() throws Exception
    {
        AutoRepairV2.RepairProgressListener listener = new AutoRepairV2.RepairProgressListener(repairType);
        listener.progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0, "test"));

        listener.await();
    }
}
