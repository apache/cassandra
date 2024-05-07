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
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import junit.framework.Assert;
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
import org.apache.cassandra.metrics.AutoRepairMetricsV2;
import org.apache.cassandra.repair.state.AutoRepairState;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.apache.cassandra.Util.setAutoRepairEnabled;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.repair.AutoRepairUtilsV2.RepairTurn.NOT_MY_TURN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
    ProgressEvent progressEvent;
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

        defaultConfig = new AutoRepairConfig(true);
        DatabaseDescriptor.setMaterializedViewsEnabled(false);
        DatabaseDescriptor.setCDCEnabled(false);
        for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values()) {
            defaultConfig.setAutoRepairEnabled(repairType, true);
            defaultConfig.setMVRepairEnabled(repairType, false);
        }
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
        resetCounters();
        resetConfig();
    }

    private void resetCounters() {
        AutoRepairMetricsV2 metrics = AutoRepairMetricsManager.getMetrics(repairType);
        Metrics.removeMatching((name, metric) -> name.startsWith("repairTurn"));
        metrics.repairTurnMyTurn = Metrics.counter(String.format("repairTurnMyTurn-%s", repairType));
        metrics.repairTurnMyTurnForceRepair = Metrics.counter(String.format("repairTurnMyTurnForceRepair-%s", repairType));
        metrics.repairTurnMyTurnDueToPriority = Metrics.counter(String.format("repairTurnMyTurnDueToPriority-%s", repairType));
    }

    private void resetConfig() {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.repair_type_overrides = defaultConfig.repair_type_overrides;
        config.global_settings = defaultConfig.global_settings;
        config.history_clear_delete_hosts_buffer_in_sec = defaultConfig.history_clear_delete_hosts_buffer_in_sec;
    }

    private void executeCQL()
    {
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s) VALUES ('k', 's')");
        QueryProcessor.executeInternal("SELECT s FROM ks.tbl WHERE k='k'");
        Keyspace.open(SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME)
                .getColumnFamilyStore(AutoRepairKeyspace.AUTO_REPAIR_PRIORITY_V2)
                .forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }


    @Test
    public void testRepairAsync()
    {
        AutoRepairV2.instance.repairExecutors.put(repairType, mockExecutor);

        AutoRepairV2.instance.repairAsync(repairType, 60);

        verify(mockExecutor, Mockito.times(1)).submit(Mockito.any(Runnable.class));
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
        consideredTables = AutoRepairV2.instance.repairStates.get(repairType).getTotalTablesConsideredForRepair();
        Assert.assertEquals("Expected total repaired tables = 0, actual value: " + consideredTables,
                            consideredTables, 0);
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
        AutoRepairV2.instance.repairStates.get(repairType).setLastRepairTime(0);
        AutoRepairV2.instance.repair(repairType, 0);
        assertEquals(1, AutoRepairV2.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());

        config.setMVRepairEnabled(repairType, false);
        AutoRepairV2.instance.repairStates.get(repairType).setLastRepairTime(0);
        AutoRepairV2.instance.repair(repairType, 0);
        assertEquals(0, AutoRepairV2.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());

        config.setMVRepairEnabled(repairType, true);
        AutoRepairV2.instance.repairStates.get(repairType).setLastRepairTime(0);
        AutoRepairV2.instance.repair(repairType, 0);
        assertEquals(1, AutoRepairV2.instance.repairStates.get(repairType).getTotalMVTablesConsideredForRepair());
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());

        config.setMVRepairEnabled(repairType, false);
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
        assertEquals(0, state.getRepairSkippedTablesCount());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).skippedTablesCount.getValue().intValue());
        state.setLastRepairTime(0);
        AutoRepairV2.instance.repair(repairType, 0);
        assertEquals(1, state.getTotalMVTablesConsideredForRepair());
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        // skipping one time for the base table and another time for MV table
        assertEquals(2, state.getRepairSkippedTablesCount());
        assertEquals(2, AutoRepairMetricsManager.getMetrics(repairType).skippedTablesCount.getValue().intValue());

        // set it to higher value, and this time, the tables should not be skipped
        config.setRepairSSTableCountHigherThreshold(repairType, 11);
        config.setRepairSSTableCountHigherThreshold(repairType, beforeCount);
        state.setLastRepairTime(0);
        AutoRepairV2.instance.repair(repairType, 0);
        assertEquals(1, state.getTotalMVTablesConsideredForRepair());
        assertEquals(0, state.getRepairSkippedTablesCount());
        assertEquals(1, AutoRepairMetricsManager.getMetrics(repairType).totalMVTablesConsideredForRepair.getValue().intValue());
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).skippedTablesCount.getValue().intValue());

        config.setMVRepairEnabled(repairType, false);
        config.setRepairSSTableCountHigherThreshold(repairType, beforeCount);
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
        assertTrue(AutoRepairMetricsManager.getMetrics(repairType).skippedTablesCount.getValue() > 0);
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).longestUnrepairedSec.getValue().intValue());

        config.setAutoRepairTableMaxRepairTimeInSec(repairType, Long.MAX_VALUE);
        when(progressEvent.getType()).thenReturn(ProgressEventType.ERROR);
        AutoRepairV2.instance.repairStates.get(repairType).progress("", progressEvent);

        AutoRepairV2.instance.repair(repairType, 0);
        assertEquals(0, AutoRepairMetricsManager.getMetrics(repairType).skippedTablesCount.getValue().intValue());
        assertTrue(AutoRepairMetricsManager.getMetrics(repairType).failedTablesCount.getValue() > 0);
        assertTrue(AutoRepairMetricsManager.getMetrics(repairType).longestUnrepairedSec.getValue() > 0);
    }
}
