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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Sets;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.repair.RepairCoordinator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
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
import org.apache.cassandra.metrics.AutoRepairMetricsManager;
import org.apache.cassandra.metrics.AutoRepairMetrics;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.ProgressListener;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;

import static org.apache.cassandra.Util.setAutoRepairEnabled;
import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_DEFAULT_RF;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.RepairTurn.NOT_MY_TURN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link org.apache.cassandra.repair.autorepair.AutoRepair}
 */
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
    RepairCoordinator repairRunnable;

    // Expected number of repairs to be executed.
    private static int expectedRepairAssignments;

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
        SYSTEM_DISTRIBUTED_DEFAULT_RF.setInt(1);
        AutoRepair.SLEEP_IF_REPAIR_FINISHES_QUICKLY = new DurationSpec.IntSecondsBound("0s");
        setAutoRepairEnabled(true);
        requireNetwork();
        AutoRepairUtils.setup();
        StorageService.instance.doAutoRepairSetup();
        DatabaseDescriptor.setCDCEnabled(false);

        // Calculate the expected number of keyspaces to be repaired, this should be all system keyspaces that are
        // distributed, plus 1 for the table we created (ks.tbl).
        int expectedKeyspacesGoingThroughRepair = 0;
        for (Keyspace keyspace : Keyspace.all())
        {
            // skip LocalStrategy keyspaces as these aren't repaired.
            if (keyspace.getReplicationStrategy() instanceof LocalStrategy)
            {
                continue;
            }
            // skip system_traces keyspaces
            if (keyspace.getName().equalsIgnoreCase(SchemaConstants.TRACE_KEYSPACE_NAME))
            {
                continue;
            }

            expectedKeyspacesGoingThroughRepair += 1;
        }
        // Since the splitter will unwrap a full token range, we expect twice as many repairs.
        expectedRepairAssignments = expectedKeyspacesGoingThroughRepair * 2;
    }

    @Before
    public void setup()
    {
        SYSTEM_DISTRIBUTED_DEFAULT_RF.setInt(1);
        QueryProcessor.executeInternal(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
        QueryProcessor.executeInternal(String.format("CREATE TABLE %s.%s (k text, s text static, i int, v text, primary key(k,i))", KEYSPACE, TABLE));
        QueryProcessor.executeInternal(String.format("CREATE TABLE %s.%s (k text, s text static, i int, v text, primary key(k,i)) WITH auto_repair = {'full_enabled': 'false', 'incremental_enabled': 'false', 'preview_repaired_enabled': 'false', 'priority': '0'}", KEYSPACE, TABLE_DISABLED_AUTO_REPAIR));

        QueryProcessor.executeInternal(String.format("CREATE MATERIALIZED VIEW %s.%s AS SELECT i, k from %s.%s " +
                "WHERE k IS NOT null AND i IS NOT null PRIMARY KEY (i, k)", KEYSPACE, MV, KEYSPACE, TABLE));

        DatabaseDescriptor.setCDCOnRepairEnabled(false);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(false);
        //noinspection resource
        MockitoAnnotations.openMocks(this);

        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).truncateBlocking();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).disableAutoCompaction();

        Keyspace.open(KEYSPACE).getColumnFamilyStore(MV).truncateBlocking();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(MV).disableAutoCompaction();

        Keyspace.open(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME).getColumnFamilyStore(SystemDistributedKeyspace.AUTO_REPAIR_PRIORITY).truncateBlocking();
        Keyspace.open(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME).getColumnFamilyStore(SystemDistributedKeyspace.AUTO_REPAIR_HISTORY).truncateBlocking();

        AutoRepair.instance.isSetupDone = false;
        AutoRepair.instance.setup();
        executeCQL();

        timeFuncCalls = 0;
        AutoRepair.timeFunc = System::currentTimeMillis;
        AutoRepair.sleepFunc = (Long startTime, TimeUnit unit) -> {};
        resetCounters();
        resetConfig();

        AutoRepair.shuffleFunc = java.util.Collections::shuffle;

        keyspace = Keyspace.open(KEYSPACE);
        cfm = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata();
        cfmDisabledAutoRepair = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE_DISABLED_AUTO_REPAIR).metadata();
        DatabaseDescriptor.setCDCOnRepairEnabled(false);
    }

    @After
    public void tearDown()
    {
        System.clearProperty("cassandra.streaming.requires_view_build_during_repair");
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
        AutoRepairConfig defaultConfig = new AutoRepairConfig(true);
        for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values())
        {
            defaultConfig.setAutoRepairEnabled(repairType, true);
            defaultConfig.setMaterializedViewRepairEnabled(repairType, false);
        }

        // reset the AutoRepairService config to default
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.repair_type_overrides = defaultConfig.repair_type_overrides;
        config.global_settings = defaultConfig.global_settings;
        config.history_clear_delete_hosts_buffer_interval = defaultConfig.history_clear_delete_hosts_buffer_interval;
        config.repair_task_min_duration = new DurationSpec.LongSecondsBound("0s");
    }

    private void executeCQL()
    {
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s) VALUES ('k', 's')");
        QueryProcessor.executeInternal("SELECT s FROM ks.tbl WHERE k='k'");
        Keyspace.open(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME)
                .getColumnFamilyStore(SystemDistributedKeyspace.AUTO_REPAIR_PRIORITY)
                .forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    @Test
    public void testRepairTurn()
    {
        UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertNotEquals("Expected my turn for the repair", NOT_MY_TURN, AutoRepairUtils.myTurnToRunRepair(repairType, myId));
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
        // repair start lag sec should be reset  on a successful repair
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).repairStartLagSec.getValue().intValue());
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
        Assert.assertNotEquals(String.format("Expected total repaired tables > 0, actual value %s ", consideredTables),
                               0, consideredTables);

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
        UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertNotEquals("Expected my turn for the repair",
                               NOT_MY_TURN, AutoRepairUtils.myTurnToRunRepair(repairType, myId));
        AutoRepair.instance.repair(repairType);
        long lastRepairTime2 = AutoRepair.instance.repairStates.get(repairType).getLastRepairTime();
        Assert.assertNotSame(String.format("Expected repair time to be same, actual value lastRepairTime1 %d, " +
                                           "lastRepairTime2 %d", lastRepairTime1, lastRepairTime2), lastRepairTime1, lastRepairTime2);
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
        Assert.assertEquals(String.format("Priority host count is not same, actual value %d, expected value %d",
                                          AutoRepairUtils.getPriorityHosts(repairType).size(), 0), 0, AutoRepairUtils.getPriorityHosts(repairType).size());
        UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());
        Assert.assertNotEquals("Expected my turn for the repair", NOT_MY_TURN, AutoRepairUtils.myTurnToRunRepair(repairType, myId));
        AutoRepair.instance.repair(repairType);
        AutoRepairUtils.addPriorityHosts(repairType, Sets.newHashSet(FBUtilities.getBroadcastAddressAndPort()));
        AutoRepair.instance.repair(repairType);
        Assert.assertEquals(String.format("Priority host count is not same actual value %d, expected value %d",
                                          AutoRepairUtils.getPriorityHosts(repairType).size(), 0), 0, AutoRepairUtils.getPriorityHosts(repairType).size());
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
        config.setMaterializedViewRepairEnabled(repairType, false);
        assertFalse(config.getMaterializedViewRepairEnabled(repairType));
        assertEquals(0, AutoRepairUtils.getAllMVs(repairType, keyspace, cfm).size());

        config.setMaterializedViewRepairEnabled(repairType, true);

        assertTrue(config.getMaterializedViewRepairEnabled(repairType));
        assertEquals(Collections.singletonList(MV), AutoRepairUtils.getAllMVs(repairType, keyspace, cfm));
        config.setMaterializedViewRepairEnabled(repairType, false);
    }


    @Test
    public void testMVRepair()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setMaterializedViewRepairEnabled(repairType, true);
        config.setRepairMinInterval(repairType, "0s");
        AutoRepair.instance.repairStates.get(repairType).setLastRepairTime(System.currentTimeMillis());
        AutoRepair.instance.repair(repairType);
        assertEquals(1, AutoRepair.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());

        config.setMaterializedViewRepairEnabled(repairType, false);
        AutoRepair.instance.repairStates.get(repairType).setLastRepairTime(System.currentTimeMillis());
        AutoRepair.instance.repair(repairType);
        assertEquals(0, AutoRepair.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());

        config.setMaterializedViewRepairEnabled(repairType, true);
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
        config.setMaterializedViewRepairEnabled(repairType, true);
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
        config.setMaterializedViewRepairEnabled(repairType, true);
        config.setRepairMinInterval(repairType, "0s");
        config.setRepairRetryBackoff(repairType, "0s");
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
        when(autoRepairState.getRepairRunnable(Mockito.any(), Mockito.any(), Mockito.any(), anyBoolean()))
        .thenReturn(repairRunnable);
        doAnswer(invocation -> {
            invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0));
            return null;
        }).when(repairRunnable).addProgressListener(Mockito.any());
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
    public void testRepairWaitsForRepairToFinishBeforeSchedullingNewSession()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        AutoRepair.instance.repairStates.put(repairType, autoRepairState);
        when(autoRepairState.getLastRepairTime()).thenReturn((long) 0);
        AtomicInteger getRepairRunnableCalls = new AtomicInteger();
        AtomicReference<AutoRepair.RepairProgressListener> prevListener = new AtomicReference<>();
        doAnswer(invocation -> {
            if (getRepairRunnableCalls.getAndIncrement() > 0)
            {
                // progress listener from previous repair should be signalled before starting new repair
                assertTrue(prevListener.get().condition.isSignalled());
            }
            getRepairRunnableCalls.incrementAndGet();
            return repairRunnable;
        }).when(autoRepairState).getRepairRunnable(Mockito.any(), Mockito.any(), Mockito.any(), anyBoolean());
        doAnswer(invocation -> {
            // sending out a COMPLETE event with a 10ms delay
            Executors.newScheduledThreadPool(1).schedule(() -> {
                invocation.getArgument(0, AutoRepair.RepairProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0));
            }, 10, TimeUnit.MILLISECONDS);
            return null;
        }).when(repairRunnable).addProgressListener(Mockito.any());

        AutoRepair.instance.repair(repairType);
        AutoRepair.instance.repair(repairType);
        AutoRepair.instance.repair(repairType);
    }

    @Test
    public void testDisabledAutoRepairForATableThroughTableLevelConfiguration()
    {
        Assert.assertTrue(cfm.params.autoRepair.repairEnabled(AutoRepairConfig.RepairType.FULL));
        Assert.assertTrue(cfm.params.autoRepair.repairEnabled(AutoRepairConfig.RepairType.INCREMENTAL));
        Assert.assertFalse(cfmDisabledAutoRepair.params.autoRepair.repairEnabled(AutoRepairConfig.RepairType.FULL));
        Assert.assertFalse(cfmDisabledAutoRepair.params.autoRepair.repairEnabled(AutoRepairConfig.RepairType.INCREMENTAL));

        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setRepairMinInterval(repairType, "0s");
        int disabledTablesRepairCountBefore = AutoRepair.instance.repairStates.get(repairType).getTotalDisabledTablesRepairCount();
        AutoRepair.instance.repair(repairType);
        int consideredTables = AutoRepair.instance.repairStates.get(repairType).getTotalTablesConsideredForRepair();
        Assert.assertNotSame(String.format("Expected total repaired tables > 0, actual value %s ", consideredTables),
                             0, consideredTables);
        int disabledTablesRepairCountAfter = AutoRepair.instance.repairStates.get(repairType).getTotalDisabledTablesRepairCount();
        Assert.assertTrue(String.format("A table %s should be skipped from auto repair, expected value: %d, actual value %d ", TABLE_DISABLED_AUTO_REPAIR, disabledTablesRepairCountBefore + 1, disabledTablesRepairCountAfter),
                            disabledTablesRepairCountBefore < disabledTablesRepairCountAfter);
    }

    @Test
    public void testTableAttribute()
    {
        assertTrue(TableAttributes.validKeywords().contains("auto_repair"));
    }

    @Test
    public void testDefaultAutomatedRepair()
    {
        for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values())
        {
            Assert.assertTrue(String.format("expected repair type %s to be enabled on table %s", repairType, cfm.name),
                              cfm.params.autoRepair.repairEnabled(repairType));
            Assert.assertFalse(String.format("expected repair type %s to be disabled on table %s", repairType, cfmDisabledAutoRepair.name),
                               cfmDisabledAutoRepair.params.autoRepair.repairEnabled(repairType));
        }
    }

    @Test
    public void testRepairShufflesKeyspacesAndTables()
    {
        AtomicInteger shuffleKeyspacesCall = new AtomicInteger();
        AtomicInteger shuffleTablesCall = new AtomicInteger();
        AtomicInteger keyspaceCount = new AtomicInteger();
        AutoRepair.shuffleFunc = (List<String> list) -> {
            // check whether was invoked for keyspaces or tables
            if (list.contains(KEYSPACE))
            {
                shuffleKeyspacesCall.getAndIncrement();
                keyspaceCount.set(list.size());
            }
            else
                // presume list not containing a keyspace is for tables.
                shuffleTablesCall.getAndIncrement();
        };

        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setRepairMinInterval(repairType, "0s");
        AutoRepair.instance.repair(repairType);

        // Expect a single invocation for keyspaces
        assertEquals(1, shuffleKeyspacesCall.get());
        // Expect an invocation for tables for each keyspace
        assertNotEquals(0, keyspaceCount.get());
        assertEquals(keyspaceCount.get(), shuffleTablesCall.get());
    }

    @Test
    public void testRepairTakesLastRepairTimeFromDB()
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setMaterializedViewRepairEnabled(repairType, true);
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
        when(autoRepairState.getRepairRunnable(Mockito.any(), Mockito.any(), Mockito.any(), anyBoolean())).thenReturn(repairRunnable);
        doAnswer(invocation -> {
            invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.ERROR, 0, 0));
            return null;
        }).when(repairRunnable).addProgressListener(Mockito.any());
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        AtomicInteger sleepCalls = new AtomicInteger();
        AutoRepair.sleepFunc = (Long duration, TimeUnit unit) -> {
            sleepCalls.getAndIncrement();
            assertEquals(TimeUnit.SECONDS, unit);
            assertEquals(config.getRepairRetryBackoff(repairType).toSeconds(), (long) duration);
        };
        config.setRepairMinInterval(repairType, "0s");
        AutoRepair.instance.repairStates.put(repairType, autoRepairState);

        AutoRepair.instance.repair(repairType);

        // Expect configured retries for each keyspace expected to be repaired
        assertEquals(config.getRepairMaxRetries(repairType)*expectedRepairAssignments, sleepCalls.get());
        verify(autoRepairState, times(1)).setSucceededTokenRangesCount(0);
        verify(autoRepairState, times(1)).setSkippedTokenRangesCount(0);
        verify(autoRepairState, times(1)).setFailedTokenRangesCount(expectedRepairAssignments);
    }

    @Test
    public void testRepairSuccessAfterRetry()
    {
        when(autoRepairState.getRepairRunnable(Mockito.any(), Mockito.any(), Mockito.any(), anyBoolean())).thenReturn(repairRunnable);

        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        AtomicInteger sleepCalls = new AtomicInteger();
        AutoRepair.sleepFunc = (Long duration, TimeUnit unit) -> {
            sleepCalls.getAndIncrement();
            assertEquals(TimeUnit.SECONDS, unit);
            assertEquals(config.getRepairRetryBackoff(repairType).toSeconds(), (long) duration);
        };
        doAnswer(invocation -> {
            if (sleepCalls.get() == 0)
            {
                invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.ERROR, 0, 0));
            }
            else
            {
                invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0));
            }

            return null;
        }).when(repairRunnable).addProgressListener(Mockito.any());
        config.setRepairMinInterval(repairType, "0s");
        config.setRepairMaxRetries(repairType, 1);
        AutoRepair.instance.repairStates.put(repairType, autoRepairState);
        AutoRepair.instance.repair(repairType);

        assertEquals(1, sleepCalls.get());
        verify(autoRepairState, times(1)).setSucceededTokenRangesCount(expectedRepairAssignments);
        verify(autoRepairState, times(1)).setSkippedTokenRangesCount(0);
        verify(autoRepairState, times(1)).setFailedTokenRangesCount(0);
    }

    @Test
    public void testRepairDoesNotThrowsForIRWithMVReplayButMVRepairDisabled()
    {
        AutoRepair.instance.setup();
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(true);
        AutoRepairService.instance.getAutoRepairConfig().setMaterializedViewRepairEnabled(repairType, false);

        if (repairType == AutoRepairConfig.RepairType.INCREMENTAL)
        {
            try
            {
                AutoRepair.instance.repair(repairType);
            }
            catch (ConfigurationException ignored)
            {
                fail("ConfigurationException not expected");
            }
        }
        else
        {
            AutoRepair.instance.repair(repairType);
        }
    }

    @Test
    public void testRepairThrowsForIRWithMVReplay()
    {
        AutoRepair.instance.setup();
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(true);
        AutoRepairService.instance.getAutoRepairConfig().setMaterializedViewRepairEnabled(repairType, true);

        if (repairType == AutoRepairConfig.RepairType.INCREMENTAL)
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
        DatabaseDescriptor.setCDCEnabled(true);
        DatabaseDescriptor.setCDCOnRepairEnabled(true);

        if (repairType == AutoRepairConfig.RepairType.INCREMENTAL)
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
    public void testSoakAfterImmediateRepair()
    {
        when(autoRepairState.getRepairRunnable(Mockito.any(), Mockito.any(), Mockito.any(), anyBoolean())).thenReturn(repairRunnable);
        doAnswer(invocation -> {
            invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0));
            return null;
        }).when(repairRunnable).addProgressListener(Mockito.any());
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.repair_task_min_duration = new DurationSpec.LongSecondsBound("10s");
        AtomicInteger sleepCalls = new AtomicInteger();
        AutoRepair.sleepFunc = (Long duration, TimeUnit unit) -> {
            sleepCalls.getAndIncrement();
            assertEquals(TimeUnit.MILLISECONDS, unit);
            assertTrue(config.getRepairTaskMinDuration().toMilliseconds() >= duration);
            config.repair_task_min_duration = new DurationSpec.LongSecondsBound("0s");
        };
        config.setRepairMinInterval(repairType, "0s");
        AutoRepair.instance.repairStates.put(repairType, autoRepairState);

        AutoRepair.instance.repair(repairType);

        assertEquals(1, sleepCalls.get());
        verify(autoRepairState, times(1)).setSucceededTokenRangesCount(expectedRepairAssignments);
        verify(autoRepairState, times(1)).setSkippedTokenRangesCount(0);
        verify(autoRepairState, times(1)).setFailedTokenRangesCount(0);
    }

    @Test
    public void testNoSoakAfterRepair()
    {
        when(autoRepairState.getRepairRunnable(Mockito.any(), Mockito.any(), Mockito.any(), anyBoolean())).thenReturn(repairRunnable);
        doAnswer(invocation -> {
            invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0));
            return null;
        }).when(repairRunnable).addProgressListener(Mockito.any());
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.repair_task_min_duration = new DurationSpec.LongSecondsBound("0s");
        AutoRepair.sleepFunc = (Long duration, TimeUnit unit) -> {
            fail("Should not sleep after repair");
        };
        config.setRepairMinInterval(repairType, "0s");
        AutoRepair.instance.repairStates.put(repairType, autoRepairState);

        AutoRepair.instance.repair(repairType);

        verify(autoRepairState, times(1)).setSucceededTokenRangesCount(expectedRepairAssignments);
        verify(autoRepairState, times(1)).setSkippedTokenRangesCount(0);
        verify(autoRepairState, times(1)).setFailedTokenRangesCount(0);
    }

    @Test
    public void testSchedulerIgnoresErrorsFromUnrelatedRepairRunables()
    {
        RepairOption options = new RepairOption(RepairParallelism.PARALLEL, true, repairType == AutoRepairConfig.RepairType.INCREMENTAL, false,
                                                AutoRepairService.instance.getAutoRepairConfig().getRepairThreads(repairType), Collections.emptySet(),
                                                 false, false, PreviewKind.NONE, false, true, true, false, false, false);
        AutoRepairState repairState = AutoRepair.instance.repairStates.get(repairType);
        AutoRepairState spyState = spy(repairState);
        AtomicReference<AutoRepair.RepairProgressListener> failingListener = new AtomicReference<>();
        AtomicInteger repairRunableCalls = new AtomicInteger();
        doAnswer((InvocationOnMock inv ) -> {
            RepairCoordinator runnable = spy(repairState.getRepairRunnable(inv.getArgument(0), inv.getArgument(1), inv.getArgument(2),
                                                                        inv.getArgument(3)));
            if (repairRunableCalls.getAndIncrement() == 0)
            {
                // this will be used for first repair job
                doAnswer(invocation -> {
                    // repair runnable for the first repair job will immediately fail
                    failingListener.set(invocation.getArgument(0, AutoRepair.RepairProgressListener.class));
                    invocation.getArgument(0, ProgressListener.class).progress("test", new ProgressEvent(ProgressEventType.ERROR, 0, 0));
                    return null;
                }).when(runnable).addProgressListener(Mockito.any());
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
                }).when(runnable).addProgressListener(Mockito.any());
            }
            return runnable;
        }).when(spyState).getRepairRunnable(Mockito.any(), Mockito.any(), Mockito.any(), anyBoolean());
        when(spyState.getLastRepairTime()).thenReturn((long) 0);
        AutoRepairService.instance.getAutoRepairConfig().setRepairMaxRetries(repairType, 0);
        AutoRepair.instance.repairStates.put(repairType, spyState);

        AutoRepair.instance.repair(repairType);

        assertEquals(1, (int) AutoRepairMetricsManager.getMetrics(repairType).failedTokenRangesCount.getValue());
        // only the first repair job should have failed despite it continuously firing ERROR events
        verify(spyState, times(1)).setFailedTokenRangesCount(1);
    }

    @Test
    public void testProgressError()
    {
        AutoRepair.RepairProgressListener listener = new AutoRepair.RepairProgressListener(repairType);

        listener.progress("test", new ProgressEvent(ProgressEventType.ERROR, 0, 0, "test"));

        assertFalse(listener.success);
        assertTrue(listener.condition.isSignalled());
    }

    @Test
    public void testProgressProgress()
    {
        AutoRepair.RepairProgressListener listener = new AutoRepair.RepairProgressListener(repairType);

        listener.progress("test", new ProgressEvent(ProgressEventType.PROGRESS, 0, 0, "test"));

        assertFalse(listener.success);
        assertFalse(listener.condition.isSignalled());
    }

    @Test
    public void testProgresComplete()
    {
        AutoRepair.RepairProgressListener listener = new AutoRepair.RepairProgressListener(repairType);

        listener.progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0, "test"));

        assertTrue(listener.success);
        assertTrue(listener.condition.isSignalled());
    }

    @Test
    public void testAwait() throws Exception
    {
        AutoRepair.RepairProgressListener listener = new AutoRepair.RepairProgressListener(repairType);
        listener.progress("test", new ProgressEvent(ProgressEventType.COMPLETE, 0, 0, "test"));

        listener.await(new DurationSpec.IntSecondsBound("12h"));
    }
}
