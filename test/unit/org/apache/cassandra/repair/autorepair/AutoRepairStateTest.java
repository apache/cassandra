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
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils.AutoRepairHistory;
import org.apache.cassandra.service.AutoRepairService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * Unit tests for {@link org.apache.cassandra.repair.autorepair.AutoRepairState}
 */
@RunWith(Parameterized.class)
public class AutoRepairStateTest extends CQLTester
{
    private static final String testTable = "test";

    @Parameterized.Parameter
    public RepairType repairType;

    @Parameterized.Parameters
    public static Collection<RepairType> repairTypes()
    {
        return Arrays.asList(RepairType.values());
    }

    @Before
    public void setUp()
    {
        AutoRepair.SLEEP_IF_REPAIR_FINISHES_QUICKLY = new DurationSpec.IntSecondsBound("0s");
        initMocks(this);
        createTable(String.format("CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", KEYSPACE, testTable));
    }

    @Test
    public void testGetRepairRunnable()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        AutoRepairService.setup();

        Runnable runnable = state.getRepairRunnable(KEYSPACE, ImmutableList.of(testTable), ImmutableSet.of(), false);

        assertNotNull(runnable);
    }

    @Test
    public void testTotalBytesIncludesMemtableOnlyRepairAssignment()
    {
        assertMemtableEstimateIsStable(1, () -> {});
    }

    @Test
    public void testMemtableEstimateSurvivesFlush()
    {
        assertMemtableEstimateIsStable(1, () -> ColumnFamilyStore.getIfExists(KEYSPACE, testTable)
                                                                                .forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
    }

    @Test
    public void testMemtableEstimateSurvivesWrites()
    {
        assertMemtableEstimateIsStable(1, () -> execute(String.format("INSERT INTO %s.%s (pk, v) VALUES (2, 2)", KEYSPACE, testTable)));
    }

    @Test
    public void testMemtableEstimateIsSharedAcrossRanges()
    {
        assertMemtableEstimateIsStable(3, () -> {});
    }

    @Test
    public void testMemtableEstimateWithMoreRangesThanBytes()
    {
        assertMemtableEstimateIsStable(256, () -> {});
    }

    private void assertMemtableEstimateIsStable(int numberOfRanges, Runnable afterStatistics)
    {
        execute(String.format("INSERT INTO %s.%s (pk, v) VALUES (1, 1)", KEYSPACE, testTable));
        long memtableBytes = ColumnFamilyStore.getIfExists(KEYSPACE, testTable).getTracker().getView().getCurrentMemtable().getLiveDataSize();
        assertTrue(memtableBytes > 0);

        Range<Token> range = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                         DatabaseDescriptor.getPartitioner().getMaximumTokenForSplitting());
        List<Range<Token>> ranges = new ArrayList<>(AutoRepairUtils.split(range, numberOfRanges));
        KeyspaceRepairPlan plan = new KeyspaceRepairPlan(KEYSPACE, ImmutableList.of(testTable), ranges,
                                                        AutoRepairUtils.calcTotalBytesToBeRepaired(repairType, KEYSPACE,
                                                                                                   ImmutableList.of(testTable), ranges));
        for (Range<Token> tokenRange : ranges)
            assertEquals(0, plan.getSizeEstimate(AutoRepairUtils.getKeyspaceTableName(KEYSPACE, testTable), tokenRange).sizeForRepair);

        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.updateRepairScheduleStatistics(ImmutableList.of(new PrioritizedRepairPlan(0, ImmutableList.of(plan))));
        afterStatistics.run();

        RepairTokenRangeSplitter splitter = new RepairTokenRangeSplitter(repairType, Collections.emptyMap());
        long assignmentBytes = 0;
        for (Range<Token> tokenRange : ranges)
        {
            Collection<RepairTokenRangeSplitter.SizedRepairAssignment> assignments = splitter.getRepairAssignmentsForTable(plan, testTable, tokenRange);
            assertEquals(1, assignments.size());
            assignmentBytes += assignments.iterator().next().getEstimatedBytes();
        }
        assertEquals(state.getTotalBytesToRepair(), assignmentBytes);
        assertEquals(memtableBytes, state.getTotalBytesToRepair());
        assertEquals(memtableBytes, plan.getTableEstimatedBytes(AutoRepairUtils.getKeyspaceTableName(KEYSPACE, testTable)));
    }

    @Test
    public void testFixedSplitMemtableAssignmentsMatchTotalBytes()
    {
        assertFixedSplitAssignmentsMatchTotalBytes(true, false);
    }

    @Test
    public void testFixedSplitMemtableEstimateSurvivesFlush()
    {
        assertFixedSplitAssignmentsMatchTotalBytes(true, true);
    }

    @Test
    public void testFixedSplitEmptyTableAssignmentsHaveZeroBytes()
    {
        assertFixedSplitAssignmentsMatchTotalBytes(false, false);
    }

    private void assertFixedSplitAssignmentsMatchTotalBytes(boolean withData, boolean flush)
    {
        AutoRepairService.setup();
        String secondTable = createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        List<String> tables = ImmutableList.of(testTable, secondTable);
        if (withData)
        {
            execute(String.format("INSERT INTO %s.%s (pk, v) VALUES (1, 1)", KEYSPACE, testTable));
            execute(String.format("INSERT INTO %s.%s (pk, v) VALUES (1, 1)", KEYSPACE, secondTable));
            execute(String.format("INSERT INTO %s.%s (pk, v) VALUES (2, 2)", KEYSPACE, secondTable));
        }
        long memtableBytes = tables.stream().mapToLong(table -> ColumnFamilyStore.getIfExists(KEYSPACE, table)
                                                                               .getTracker().getView().getCurrentMemtable().getLiveDataSize()).sum();
        List<Range<Token>> ranges = AutoRepairUtils.getTokenRanges(true, KEYSPACE);
        assertTrue(ranges.size() > 1);
        KeyspaceRepairPlan plan = new KeyspaceRepairPlan(KEYSPACE, tables, ranges,
                                                        AutoRepairUtils.calcTotalBytesToBeRepaired(repairType, KEYSPACE, tables, ranges));
        List<PrioritizedRepairPlan> plans = ImmutableList.of(new PrioritizedRepairPlan(0, ImmutableList.of(plan)));
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        AutoRepairState state = RepairType.getAutoRepairState(repairType, config);
        state.updateRepairScheduleStatistics(plans);

        if (flush)
        {
            for (String table : tables)
                ColumnFamilyStore.getIfExists(KEYSPACE, table).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }

        boolean previousByKeyspace = config.getRepairByKeyspace(repairType);
        try
        {
            for (boolean byKeyspace : new boolean[]{ false, true })
            {
                config.setRepairByKeyspace(repairType, byKeyspace);
                for (int subranges : new int[]{ 1, 4, 256 })
                {
                    FixedSplitTokenRangeSplitter splitter = new FixedSplitTokenRangeSplitter(repairType,
                                                                                             Collections.singletonMap(FixedSplitTokenRangeSplitter.NUMBER_OF_SUBRANGES,
                                                                                                                      Integer.toString(subranges)));
                    List<RepairAssignment> assignments = splitter.getRepairAssignments(true, plans).next().getRepairAssignments();
                    assertEquals(ranges.size() * Math.max(1, subranges / ranges.size()) * (byKeyspace ? 1 : tables.size()), assignments.size());
                    assertEquals(state.getTotalBytesToRepair(), assignments.stream().mapToLong(RepairAssignment::getEstimatedBytes).sum());
                    assertTrue(assignments.stream().allMatch(assignment -> assignment.getEstimatedBytes() >= 0));
                    if (!byKeyspace)
                    {
                        for (String table : tables)
                            assertEquals(plan.getTableEstimatedBytes(AutoRepairUtils.getKeyspaceTableName(KEYSPACE, table)),
                                         assignments.stream().filter(assignment -> assignment.getTableNames().contains(table))
                                                    .mapToLong(RepairAssignment::getEstimatedBytes).sum());
                    }
                }
            }
        }
        finally
        {
            config.setRepairByKeyspace(repairType, previousByKeyspace);
        }
        assertEquals(memtableBytes, state.getTotalBytesToRepair());
        assertEquals(memtableBytes, plan.getEstimatedBytes());
    }

    @Test
    public void testTotalBytesDoesNotAddMemtableToSSTableEstimate()
    {
        execute(String.format("INSERT INTO %s.%s (pk, v) VALUES (1, 1)", KEYSPACE, testTable));
        ColumnFamilyStore.getIfExists(KEYSPACE, testTable).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        execute(String.format("INSERT INTO %s.%s (pk, v) VALUES (2, 2)", KEYSPACE, testTable));

        Range<Token> range = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                         DatabaseDescriptor.getPartitioner().getMaximumTokenForSplitting());
        String keyspaceTableName = AutoRepairUtils.getKeyspaceTableName(KEYSPACE, testTable);
        AutoRepairUtils.SizeEstimate estimate = AutoRepairUtils.getRangeSizeEstimate(repairType, KEYSPACE, testTable, range);
        assertTrue(estimate.sizeForRepair > 0);
        assertTrue(estimate.memtableSize > 0);
        KeyspaceRepairPlan plan = new KeyspaceRepairPlan(KEYSPACE, ImmutableList.of(testTable), Collections.singletonList(range),
                                                        Collections.singletonMap(keyspaceTableName, Collections.singletonMap(range, estimate)));
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.updateRepairScheduleStatistics(ImmutableList.of(new PrioritizedRepairPlan(0, ImmutableList.of(plan))));

        assertEquals(estimate.sizeForRepair, plan.getTableEstimatedBytes(keyspaceTableName));
        assertEquals(estimate.sizeForRepair, state.getTotalBytesToRepair());
        RepairTokenRangeSplitter splitter = new RepairTokenRangeSplitter(repairType, Collections.emptyMap());
        assertEquals(estimate.sizeForRepair, splitter.getRepairAssignmentsForTable(plan, testTable, range)
                                                    .stream().mapToLong(RepairAssignment::getEstimatedBytes).sum());
    }

    @Test
    public void testGetLastRepairTime()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.lastRepairFinishTimeInMs = 1;

        assertEquals(1, state.getLastRepairFinishTime());
    }

    @Test
    public void testSetTotalTablesConsideredForRepair()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());

        state.setTotalTablesConsideredForRepair(1);

        assertEquals(1, state.totalTablesConsideredForRepair);
    }

    @Test
    public void testGetTotalTablesConsideredForRepair()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.totalTablesConsideredForRepair = 1;

        assertEquals(1, state.getTotalTablesConsideredForRepair());
    }

    @Test
    public void testSetLastRepairTimeInMs()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());

        state.setLastRepairFinishTime(1);

        assertEquals(1, state.lastRepairFinishTimeInMs);
    }

    @Test
    public void testGetClusterRepairTimeInSec()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.clusterRepairTimeInSec = 1;

        assertEquals(1, state.getClusterRepairTimeInSec());
    }

    @Test
    public void testGetNodeRepairTimeInSec()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.nodeRepairTimeInSec = 1;

        assertEquals(1, state.getNodeRepairTimeInSec());
    }

    @Test
    public void testSetRepairInProgress()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());

        state.setRepairInProgress(true);

        assertTrue(state.repairInProgress);
    }

    @Test
    public void testIsRepairInProgress()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.repairInProgress = true;

        assertTrue(state.isRepairInProgress());
    }

    @Test
    public void testSetSkippedTokenRangesCount()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());

        state.setSkippedTokenRangesCount(1);

        assertEquals(1, state.skippedTokenRangesCount);
    }

    @Test
    public void testGetSkippedTokenRangesCount()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.skippedTokenRangesCount = 1;

        assertEquals(1, state.getSkippedTokenRangesCount());
    }

    @Test
    public void testGetLongestUnrepairedSecNull()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.longestUnrepairedNode = null;

        try
        {
            assertEquals(0, state.getLongestUnrepairedSec());
        }
        catch (Exception e)
        {
            assertNull(e);
        }
    }

    @Test
    public void testGetLongestUnrepairedSec()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.longestUnrepairedNode = new AutoRepairHistory(UUID.randomUUID(), "", 0, 1000,
                                                            null, 0, false);
        AutoRepairState.timeFunc = () -> 2000L;

        try
        {
            assertEquals(1, state.getLongestUnrepairedSec());
        }
        catch (Exception e)
        {
            assertNull(e);
        }
    }

    @Test
    public void testSetTotalMVTablesConsideredForRepair()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());

        state.setTotalMVTablesConsideredForRepair(1);

        assertEquals(1, state.totalMVTablesConsideredForRepair);
    }

    @Test
    public void testGetTotalMVTablesConsideredForRepair()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.totalMVTablesConsideredForRepair = 1;

        assertEquals(1, state.getTotalMVTablesConsideredForRepair());
    }

    @Test
    public void testSetNodeRepairTimeInSec()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());

        state.setNodeRepairTimeInSec(1);

        assertEquals(1, state.nodeRepairTimeInSec);
    }

    @Test
    public void testSetClusterRepairTimeInSec()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());

        state.setClusterRepairTimeInSec(1);

        assertEquals(1, state.clusterRepairTimeInSec);
    }

    @Test
    public void testSetRepairKeyspaceCount()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());

        state.setRepairKeyspaceCount(1);

        assertEquals(1, state.repairKeyspaceCount);
    }

    @Test
    public void testGetRepairKeyspaceCount()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.repairKeyspaceCount = 1;

        assertEquals(1, state.getRepairKeyspaceCount());
    }

    @Test
    public void testSetLongestUnrepairedNode()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        AutoRepairHistory history = new AutoRepairHistory(UUID.randomUUID(), "", 0, 0, null, 0, false);

        state.setLongestUnrepairedNode(history);

        assertEquals(history, state.longestUnrepairedNode);
    }

    @Test
    public void testSetSucceededTokenRangesCount()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());

        state.setSucceededTokenRangesCount(1);

        assertEquals(1, state.succeededTokenRangesCount);
    }

    @Test
    public void testGetSucceededTokenRangesCount()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.succeededTokenRangesCount = 1;

        assertEquals(1, state.getSucceededTokenRangesCount());
    }

    @Test
    public void testSetFailedTokenRangesCount()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());

        state.setFailedTokenRangesCount(1);

        assertEquals(1, state.failedTokenRangesCount);
    }

    @Test
    public void testGetFailedTokenRangesCount()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.failedTokenRangesCount = 1;

        assertEquals(1, state.getFailedTokenRangesCount());
    }
}
