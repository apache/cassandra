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

package org.apache.cassandra.repair.autorepair;

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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.RepairRunnable;
import org.apache.cassandra.repair.autorepair.IAutoRepairTokenRangeSplitter.RepairAssignment;
import org.apache.cassandra.schema.AutoRepairParams;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.StorageService;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;
import org.apache.cassandra.metrics.AutoRepairMetrics;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.apache.cassandra.Util.setAutoRepairEnabled;
import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_DEFAULT_RF;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.RepairTurn.NOT_MY_TURN;
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
public class AutoRepairParameterizedTest extends CQLTester
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";
    private static final String TABLE_DISABLED_AUTO_REPAIR = "tbl_disabled_auto_repair";
    private static final String MV = "mv";
    private static TableMetadata cfm;
    private static TableMetadata cfmDisabledAutoRepair;
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
        AutoRepair.SLEEP_IF_REPAIR_FINISHES_QUICKLY = new DurationSpec.IntSecondsBound("0s");
        SYSTEM_DISTRIBUTED_DEFAULT_RF.setInt(1);
        setAutoRepairEnabled(true);
        requireNetwork();
        AutoRepairUtils.setup();


        cfm = TableMetadata.builder(KEYSPACE, TABLE)
                           .addPartitionKeyColumn("k", UTF8Type.instance)
                           .addStaticColumn("s", UTF8Type.instance)
                           .addClusteringColumn("i", IntegerType.instance)
                           .addRegularColumn("v", UTF8Type.instance)
                           .params(TableParams.builder().automatedRepairFull(AutoRepairParams.create(AutoRepairConfig.RepairType.full, ImmutableMap.of(AutoRepairParams.Option.ENABLED.toString(), Boolean.toString(true)))).
                                              automatedRepairIncremental(AutoRepairParams.create(AutoRepairConfig.RepairType.incremental, ImmutableMap.of(AutoRepairParams.Option.ENABLED.toString(), Boolean.toString(true)))).build())
                           .build();

        cfmDisabledAutoRepair = TableMetadata.builder(KEYSPACE, TABLE_DISABLED_AUTO_REPAIR)
                                             .addPartitionKeyColumn("k", UTF8Type.instance)
                                             .addStaticColumn("s", UTF8Type.instance)
                                             .addClusteringColumn("i", IntegerType.instance)
                                             .addRegularColumn("v", UTF8Type.instance)
                                             .params(TableParams.builder().automatedRepairFull(AutoRepairParams.create(AutoRepairConfig.RepairType.full, ImmutableMap.of(AutoRepairParams.Option.ENABLED.toString(), Boolean.toString(false)))).
                                                                automatedRepairIncremental(AutoRepairParams.create(AutoRepairConfig.RepairType.incremental, ImmutableMap.of(AutoRepairParams.Option.ENABLED.toString(), Boolean.toString(false)))).build())
                                             .build();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), cfm, cfmDisabledAutoRepair);
        cfm = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        cfmDisabledAutoRepair = Schema.instance.getTableMetadata(KEYSPACE, TABLE_DISABLED_AUTO_REPAIR);
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
        System.setProperty("cassandra.streaming.requires_cdc_replay", "false");
        System.setProperty("cassandra.streaming.requires_view_build_during_repair", "false");
        MockitoAnnotations.initMocks(this);

        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).truncateBlocking();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).disableAutoCompaction();

        Keyspace.open(KEYSPACE).getColumnFamilyStore(MV).truncateBlocking();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(MV).disableAutoCompaction();

        Keyspace.open(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME).getColumnFamilyStore(SystemDistributedKeyspace.AUTO_REPAIR_PRIORITY).truncateBlocking();
        Keyspace.open(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME).getColumnFamilyStore(SystemDistributedKeyspace.AUTO_REPAIR_HISTORY).truncateBlocking();


        AutoRepair.instance = new AutoRepair();
        executeCQL();

        timeFuncCalls = 0;
        AutoRepair.timeFunc = System::currentTimeMillis;
        resetCounters();
        resetConfig();

        AutoRepair.shuffleFunc = java.util.Collections::shuffle;
    }

    @After
    public void tearDown()
    {
        System.clearProperty("cassandra.streaming.requires_view_build_during_repair");
        System.clearProperty("cassandra.streaming.requires_cdc_replay");
    }

    private void resetCounters()
    {
        AutoRepairMetrics metrics = AutoRepairMetricsManager.getMetrics(repairType);
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
        config.history_clear_delete_hosts_buffer_interval = defaultConfig.history_clear_delete_hosts_buffer_interval;
        config.setRepairSubRangeNum(repairType, 1);
    }

    private void executeCQL()
    {
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s) VALUES ('k', 's')");
        QueryProcessor.executeInternal("SELECT s FROM ks.tbl WHERE k='k'");
        Keyspace.open(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME)
                .getColumnFamilyStore(SystemDistributedKeyspace.AUTO_REPAIR_PRIORITY)
                .forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    @Test(expected = ConfigurationException.class)
    public void testRepairAsyncWithRepairTypeDisabled()
    {
        AutoRepairService.instance.getAutoRepairConfig().setAutoRepairEnabled(repairType, false);

        AutoRepair.instance.repairAsync(repairType);
    }

    @Test
    public void testRepairAsync()
    {
        AutoRepair.instance.repairExecutors.put(repairType, mockExecutor);

        AutoRepair.instance.repairAsync(repairType);

        verify(mockExecutor, Mockito.times(1)).submit(any(Runnable.class));
    }

    @Test
    public void testRepairTurn()
    {
        UUID myId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertTrue("Expected my turn for the repair", AutoRepairUtils.myTurnToRunRepair(repairType, myId) != NOT_MY_TURN);
    }

    @Test
    public void testRepair()
    {
        AutoRepairService.instance.getAutoRepairConfig().setRepairMinInterval(repairType, "0s");
        AutoRepair.instance.repair(repairType);
        assertEquals(0, AutoRepair.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        long lastRepairTime = AutoRepair.instance.repairStates.get(repairType).getLastRepairTime();
        //if repair was done then lastRepairTime should be non-zero
        Assert.assertTrue(String.format("Expected lastRepairTime > 0, actual value lastRepairTime %d",
                                        lastRepairTime), lastRepairTime > 0);
    }

    @Test
    public void testTooFrequentRepairs()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        //in the first round let repair run
        config.setRepairMinInterval(repairType, "0s");
        AutoRepair.instance.repair(repairType);
        long lastRepairTime1 = AutoRepair.instance.repairStates.get(repairType).getLastRepairTime();
        int consideredTables = AutoRepair.instance.repairStates.get(repairType).getTotalTablesConsideredForRepair();
        Assert.assertNotSame(String.format("Expected total repaired tables > 0, actual value %s ", consideredTables),
                             consideredTables, 0);

        //if repair was done in last 24 hours then it should not trigger another repair
        config.setRepairMinInterval(repairType, "24h");
        AutoRepair.instance.repair(repairType);
        long lastRepairTime2 = AutoRepair.instance.repairStates.get(repairType).getLastRepairTime();
        Assert.assertEquals(String.format("Expected repair time to be same, actual value lastRepairTime1 %d, " +
                                          "lastRepairTime2 %d", lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime2);
        assertEquals(0, AutoRepair.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
    }

    @Test
    public void testNonFrequentRepairs()
    {
        Integer prevMetricsCount = AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue();
        AutoRepairState state = AutoRepair.instance.repairStates.get(repairType);
        long prevCount = state.getTotalMVTablesConsideredForRepair();
        AutoRepairService.instance.getAutoRepairConfig().setRepairMinInterval(repairType, "0s");
        AutoRepair.instance.repair(repairType);
        long lastRepairTime1 = AutoRepair.instance.repairStates.get(repairType).getLastRepairTime();
        Assert.assertTrue(String.format("Expected lastRepairTime1 > 0, actual value lastRepairTime1 %d",
                                        lastRepairTime1), lastRepairTime1 > 0);
        UUID myId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertTrue("Expected my turn for the repair",
                          AutoRepairUtils.myTurnToRunRepair(repairType, myId) != NOT_MY_TURN);
        AutoRepair.instance.repair(repairType);
        long lastRepairTime2 = AutoRepair.instance.repairStates.get(repairType).getLastRepairTime();
        Assert.assertNotSame(String.format("Expected repair time to be same, actual value lastRepairTime1 %d, " +
                                           "lastRepairTime2 ", lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime2);
        assertEquals(prevCount, state.getTotalMVTablesConsideredForRepair());
        assertEquals(prevMetricsCount, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue());
    }

    @Test
    public void testGetPriorityHosts()
    {
        Integer prevMetricsCount = AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue();
        AutoRepairState state = AutoRepair.instance.repairStates.get(repairType);
        long prevCount = state.getTotalMVTablesConsideredForRepair();
        AutoRepairService.instance.getAutoRepairConfig().setRepairMinInterval(repairType, "0s");
        Assert.assertSame(String.format("Priority host count is not same, actual value %d, expected value %d",
                                        AutoRepairUtils.getPriorityHosts(repairType).size(), 0), AutoRepairUtils.getPriorityHosts(repairType).size(), 0);
        UUID myId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertTrue("Expected my turn for the repair", AutoRepairUtils.myTurnToRunRepair(repairType, myId) !=
                                                             NOT_MY_TURN);
        AutoRepair.instance.repair(repairType);
        AutoRepairUtils.addPriorityHosts(repairType, Sets.newHashSet(FBUtilities.getBroadcastAddressAndPort()));
        AutoRepair.instance.repair(repairType);
        Assert.assertSame(String.format("Priority host count is not same actual value %d, expected value %d",
                                        AutoRepairUtils.getPriorityHosts(repairType).size(), 0), AutoRepairUtils.getPriorityHosts(repairType).size(), 0);
        assertEquals(prevCount, state.getTotalMVTablesConsideredForRepair());
        assertEquals(prevMetricsCount, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue());
    }

    @Test
    public void testCheckAutoRepairStartStop() throws Throwable
    {
        Integer prevMetricsCount = AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue();
        AutoRepairState state = AutoRepair.instance.repairStates.get(repairType);
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        long prevCount = state.getTotalMVTablesConsideredForRepair();
        config.setRepairMinInterval(repairType, "0s");
        config.setAutoRepairEnabled(repairType, false);
        long lastRepairTime1 = AutoRepair.instance.repairStates.get(repairType).getLastRepairTime();
        AutoRepair.instance.repair(repairType);
        long lastRepairTime2 = AutoRepair.instance.repairStates.get(repairType).getLastRepairTime();
        //Since repair has not happened, both the last repair times should be same
        Assert.assertEquals(String.format("Expected lastRepairTime1 %d, and lastRepairTime2 %d to be same",
                                          lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime2);

        config.setAutoRepairEnabled(repairType, true);
        AutoRepair.instance.repair(repairType);
        //since repair is done now, so lastRepairTime1/lastRepairTime2 and lastRepairTime3 should not be same
        long lastRepairTime3 = AutoRepair.instance.repairStates.get(repairType).getLastRepairTime();
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
        assertEquals(0, AutoRepairUtils.getAllMVs(repairType, keyspace, cfm).size());

        config.setMVRepairEnabled(repairType, true);

        assertTrue(config.getMVRepairEnabled(repairType));
        assertEquals(Arrays.asList(MV), AutoRepairUtils.getAllMVs(repairType, keyspace, cfm));
        config.setMVRepairEnabled(repairType, false);
    }


    @Test
    public void testMVRepair()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setMVRepairEnabled(repairType, true);
        config.setRepairMinInterval(repairType, "0s");
        AutoRepair.instance.repairStates.get(repairType).setLastRepairTime(System.currentTimeMillis());
        AutoRepair.instance.repair(repairType);
        assertEquals(1, AutoRepair.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());

        config.setMVRepairEnabled(repairType, false);
        AutoRepair.instance.repairStates.get(repairType).setLastRepairTime(System.currentTimeMillis());
        AutoRepair.instance.repair(repairType);
        assertEquals(0, AutoRepair.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());

        config.setMVRepairEnabled(repairType, true);
        AutoRepair.instance.repairStates.get(repairType).setLastRepairTime(System.currentTimeMillis());
        AutoRepair.instance.repair(repairType);
        assertEquals(1, AutoRepair.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
    }

    @Test
    public void testSkipRepairSSTableCountHigherThreshold()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        AutoRepairState state = AutoRepair.instance.repairStates.get(repairType);
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
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).skippedTokenRangesCount.getValue().intValue());
        state.setLastRepairTime(0);
        AutoRepair.instance.repair(repairType);
        assertEquals(0, state.getTotalMVTablesConsideredForRepair());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        // skipping both the tables - one table is due to its repair has been disabled, and another one due to high sstable count
        assertEquals(0, state.getSkippedTokenRangesCount());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).skippedTokenRangesCount.getValue().intValue());
        assertEquals(2, state.getSkippedTablesCount());
        assertEquals(2, AutoRepairMetricsManager.getMetrics(repairType).skippedTablesCount.getValue().intValue());

        // set it to higher value, and this time, the tables should not be skipped
        config.setRepairSSTableCountHigherThreshold(repairType, beforeCount);
        state.setLastRepairTime(0);
        state.setSkippedTablesCount(0);
        state.setTotalMVTablesConsideredForRepair(0);
        AutoRepair.instance.repair(repairType);
        assertEquals(1, state.getTotalMVTablesConsideredForRepair());
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        assertEquals(0, state.getSkippedTokenRangesCount());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).skippedTokenRangesCount.getValue().intValue());
        assertEquals(1, state.getSkippedTablesCount());
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).skippedTablesCount.getValue().intValue());
    }

    @Test
    public void testGetRepairState()
    {
        assertEquals(0, AutoRepair.instance.repairStates.get(repairType).getRepairKeyspaceCount());

        AutoRepairState state = AutoRepair.instance.getRepairState(repairType);
        state.setRepairKeyspaceCount(100);

        assertEquals(100L, AutoRepair.instance.getRepairState(repairType).getRepairKeyspaceCount());
    }

    @Test
    public void testMetrics()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setMVRepairEnabled(repairType, true);
        config.setRepairMinInterval(repairType, "0s");
        config.setAutoRepairTableMaxRepairTime(repairType, "0s");
        AutoRepair.timeFunc = () -> {
            timeFuncCalls++;
            return timeFuncCalls * 1000L;
        };
        AutoRepair.instance.repairStates.get(repairType).setLastRepairTime(1000L);

        AutoRepair.instance.repair(repairType);

        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        assertTrue(AutoRepairMetricsManager.getMetrics(repairType).nodeRepairTimeInSec.getValue() > 0);
        assertTrue(AutoRepairMetricsManager.getMetrics(repairType).clusterRepairTimeInSec.getValue() > 0);
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).repairTurnMyTurn.getCount());
        assertTrue(AutoRepairMetricsManager.getMetrics(repairType).skippedTokenRangesCount.getValue() > 0);
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).longestUnrepairedSec.getValue().intValue());

        config.setAutoRepairTableMaxRepairTime(repairType, String.valueOf(Integer.MAX_VALUE-1) + 's');
        AutoRepair.instance.repairStates.put(repairType, autoRepairState);
        when(autoRepairState.getRepairRunnable(any(), any(), any(), anyBoolean()))
        .thenReturn(repairRunnable);
        when(autoRepairState.getFailedTokenRangesCount()).thenReturn(10);
        when(autoRepairState.getSucceededTokenRangesCount()).thenReturn(11);
        when(autoRepairState.getLongestUnrepairedSec()).thenReturn(10);

        AutoRepair.instance.repair(repairType);
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
        config.setRepairRetryBackoff("0s");
        when(autoRepairState.getRepairRunnable(any(), any(), any(), anyBoolean()))
        .thenReturn(repairRunnable);
        AutoRepair.instance.repairStates.put(repairType, autoRepairState);
        when(autoRepairState.getLastRepairTime()).thenReturn((long) 0);
        AtomicInteger resetWaitConditionCalls = new AtomicInteger();
        AtomicInteger waitForRepairCompletedCalls = new AtomicInteger();
        doAnswer(invocation -> {
            resetWaitConditionCalls.getAndIncrement();
            assertEquals("waitForRepairToComplete was called before resetWaitCondition",
                         resetWaitConditionCalls.get(), waitForRepairCompletedCalls.get() + 1);
            return null;
        }).when(autoRepairState).resetWaitCondition();
        doAnswer(invocation -> {
            waitForRepairCompletedCalls.getAndIncrement();
            assertEquals("resetWaitCondition was not called before waitForRepairToComplete",
                         resetWaitConditionCalls.get(), waitForRepairCompletedCalls.get());
            return null;
        }).when(autoRepairState).waitForRepairToComplete(config.getRepairSessionTimeout(repairType));

        AutoRepair.instance.repair(repairType);
        AutoRepair.instance.repair(repairType);
        AutoRepair.instance.repair(repairType);
    }

    @Test
    public void testDisabledAutoRepairForATableThroughTableLevelConfiguration()
    {
        Assert.assertTrue(cfm.params.automatedRepair.get(AutoRepairConfig.RepairType.full).repairEnabled());
        Assert.assertTrue(cfm.params.automatedRepair.get(AutoRepairConfig.RepairType.incremental).repairEnabled());
        Assert.assertFalse(cfmDisabledAutoRepair.params.automatedRepair.get(AutoRepairConfig.RepairType.full).repairEnabled());
        Assert.assertFalse(cfmDisabledAutoRepair.params.automatedRepair.get(AutoRepairConfig.RepairType.incremental).repairEnabled());

        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setRepairMinInterval(repairType, "0s");
        int disabledTablesRepairCountBefore = AutoRepair.instance.repairStates.get(repairType).getTotalDisabledTablesRepairCount();
        AutoRepair.instance.repair(repairType);
        int consideredTables = AutoRepair.instance.repairStates.get(repairType).getTotalTablesConsideredForRepair();
        Assert.assertNotSame(String.format("Expected total repaired tables > 0, actual value %s ", consideredTables),
                             consideredTables, 0);
        int disabledTablesRepairCountAfter = AutoRepair.instance.repairStates.get(repairType).getTotalDisabledTablesRepairCount();
        Assert.assertTrue(String.format("A table %s should be skipped from auto repair, expected value: %d, actual value %d ", TABLE_DISABLED_AUTO_REPAIR, disabledTablesRepairCountBefore + 1, disabledTablesRepairCountAfter),
                            disabledTablesRepairCountBefore < disabledTablesRepairCountAfter);
    }

    @Test
    public void testTokenRangesNoSplit()
    {
        Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRanges(KEYSPACE);
        assertEquals(1, tokens.size());
        List<Range<Token>> expectedToken = new ArrayList<>(tokens);

        List<RepairAssignment> assignments = new DefaultAutoRepairTokenSplitter().getRepairAssignments(repairType, true, KEYSPACE, Collections.singletonList(TABLE));
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
    public void testDefaultAutomatedRepair()
    {
        Assert.assertTrue(cfm.params.automatedRepair.get(AutoRepairConfig.RepairType.full).repairEnabled());
        Assert.assertTrue(cfm.params.automatedRepair.get(AutoRepairConfig.RepairType.incremental).repairEnabled());
        Assert.assertFalse(cfmDisabledAutoRepair.params.automatedRepair.get(AutoRepairConfig.RepairType.full).repairEnabled());
        Assert.assertFalse(cfmDisabledAutoRepair.params.automatedRepair.get(AutoRepairConfig.RepairType.incremental).repairEnabled());
    }

    @Test
    public void testRepairShufflesKeyspacesAndTables()
    {
        AtomicInteger shuffleKeyspacesCall = new AtomicInteger();
        AtomicInteger shuffleTablesCall = new AtomicInteger();
        AutoRepair.shuffleFunc = (List<?> list) -> {
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
        };

        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setRepairMinInterval(repairType, "0s");
        AutoRepair.instance.repair(repairType);

        assertEquals(1, shuffleKeyspacesCall.get());
        assertEquals(4, shuffleTablesCall.get());
    }

    @Test
    public void testRepairTakesLastRepairTimeFromDB()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setMVRepairEnabled(repairType, true);
        long lastRepairTime = System.currentTimeMillis() - 1000;
        AutoRepairUtils.insertNewRepairHistory(repairType, 0, lastRepairTime);
        AutoRepair.instance.repairStates.get(repairType).setLastRepairTime(0);
        config.setRepairMinInterval(repairType, "1h");

        AutoRepair.instance.repair(repairType);

        // repair scheduler should not attempt to run repair as last repair time in DB is current time - 1s
        assertEquals(0, AutoRepair.instance.repairStates.get(repairType).getTotalTablesConsideredForRepair());
        // repair scheduler should load the repair time from the DB
        assertEquals(lastRepairTime, AutoRepair.instance.repairStates.get(repairType).getLastRepairTime());
    }

    @Test
    public void testRepairMaxRetries()
    {
        when(autoRepairState.getRepairRunnable(any(), any(), any(), anyBoolean())).thenReturn(repairRunnable);
        when(autoRepairState.isSuccess()).thenReturn(false);
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        AtomicInteger sleepCalls = new AtomicInteger();
        AutoRepair.sleepFunc = (Long duration, TimeUnit unit) -> {
            sleepCalls.getAndIncrement();
            assertEquals(TimeUnit.SECONDS, unit);
            assertEquals(config.getRepairRetryBackoff().toSeconds(), (long) duration);
        };
        config.setRepairMinInterval(repairType, "0s");
        AutoRepair.instance.repairStates.put(repairType, autoRepairState);

        AutoRepair.instance.repair(repairType);

        //system_auth.role_permissions,system_auth.network_permissions,system_auth.role_members,system_auth.roles,
        // system_auth.resource_role_permissons_index,system_traces.sessions,system_traces.events,ks.tbl,
        // system_distributed.auto_repair_priority,system_distributed.repair_history,system_distributed.auto_repair_history,
        // system_distributed.view_build_status,system_distributed.parent_repair_history,system_distributed.partition_denylist
        int exptedTablesGoingThroughRepair = 14;
        assertEquals(config.getRepairMaxRetries()*exptedTablesGoingThroughRepair, sleepCalls.get());
        verify(autoRepairState, Mockito.times(1)).setSucceededTokenRangesCount(0);
        verify(autoRepairState, Mockito.times(1)).setSkippedTokenRangesCount(0);
        verify(autoRepairState, Mockito.times(1)).setFailedTokenRangesCount(exptedTablesGoingThroughRepair);
    }

    @Test
    public void testRepairSuccessAfterRetry()
    {
        when(autoRepairState.getRepairRunnable(any(), any(), any(), anyBoolean())).thenReturn(repairRunnable);

        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        AtomicInteger sleepCalls = new AtomicInteger();
        AutoRepair.sleepFunc = (Long duration, TimeUnit unit) -> {
            sleepCalls.getAndIncrement();
            assertEquals(TimeUnit.SECONDS, unit);
            assertEquals(config.getRepairRetryBackoff().toSeconds(), (long) duration);
        };
        when(autoRepairState.isSuccess()).then((invocationOnMock) -> {
            if (sleepCalls.get() == 0) {
                return false;
            }
            return true;
        });
        config.setRepairMinInterval(repairType, "0s");
        AutoRepair.instance.repairStates.put(repairType, autoRepairState);
        AutoRepair.instance.repair(repairType);

        assertEquals(1, sleepCalls.get());
        verify(autoRepairState, Mockito.times(1)).setSucceededTokenRangesCount(14);
        verify(autoRepairState, Mockito.times(1)).setSkippedTokenRangesCount(0);
        verify(autoRepairState, Mockito.times(1)).setFailedTokenRangesCount(0);
    }

    @Test
    public void testRepairThrowsForIRWithMVReplay()
    {
        AutoRepair.instance.setup();
        System.setProperty("cassandra.streaming.requires_view_build_during_repair", "true");

        if (repairType == AutoRepairConfig.RepairType.incremental)
        {
            try
            {
                AutoRepair.instance.repair(repairType);
                fail("Expected ConfigurationException");
            }
            catch (ConfigurationException ignored)
            {
            }
        }
        else
        {
            AutoRepair.instance.repair(repairType);
        }
    }


    @Test
    public void testRepairThrowsForIRWithCDCReplay()
    {
        AutoRepair.instance.setup();
        System.setProperty("cassandra.streaming.requires_cdc_replay", "true");

        if (repairType == AutoRepairConfig.RepairType.incremental)
        {
            try
            {
                AutoRepair.instance.repair(repairType);
                fail("Expected ConfigurationException");
            }
            catch (ConfigurationException ignored)
            {
            }
        }
        else
        {
            AutoRepair.instance.repair(repairType);
        }
    }
}
