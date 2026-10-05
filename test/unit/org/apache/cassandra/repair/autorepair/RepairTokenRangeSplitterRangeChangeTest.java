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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DataStorageSpec.LongMebibytesBound;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.RepairTokenRangeSplitter.FilteredRepairAssignments;
import org.apache.cassandra.repair.autorepair.RepairTokenRangeSplitter.SizedRepairAssignment;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.Move;
import org.apache.cassandra.tcm.transformations.PrepareMove;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.Unregister;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;

import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.getKeyspaceTableName;
import static org.apache.cassandra.repair.autorepair.RepairTokenRangeSplitter.BYTES_PER_ASSIGNMENT;
import static org.apache.cassandra.repair.autorepair.RepairTokenRangeSplitter.MAX_BYTES_PER_SCHEDULE;
import static org.apache.cassandra.repair.autorepair.RepairTokenRangeSplitter.PARTITIONS_PER_ASSIGNMENT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Regression coverage for range changes between collecting statistics and generating assignments.
 */
@RunWith(Parameterized.class)
public class RepairTokenRangeSplitterRangeChangeTest extends CQLTester
{
    private String tableName;
    private Range<Token> fullRange;

    @Parameterized.Parameter
    public RepairType repairType;

    @Parameterized.Parameters(name = "repairType={0}")
    public static Collection<RepairType> repairTypes()
    {
        return Arrays.asList(RepairType.values());
    }

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        AutoRepairService.setup();
    }

    @Before
    public void setUp()
    {
        fullRange = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                DatabaseDescriptor.getPartitioner().getMaximumTokenForSplitting());
        tableName = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");
        assertTrue(getCurrentColumnFamilyStore().getLiveSSTables().isEmpty());
        assertTrue(memtableBytes(tableName) > 0);
    }

    @Test
    public void testUnchangedRangesPreserveMemtableEstimate()
    {
        List<Range<Token>> ranges = new ArrayList<>(AutoRepairUtils.split(fullRange, 3));
        KeyspaceRepairPlan plan = buildPlan(tableName, ranges);
        RepairTokenRangeSplitter splitter = new RepairTokenRangeSplitter(repairType, Collections.emptyMap());

        long assignmentBytes = getAssignments(splitter, plan, tableName, ranges).stream()
                                                                             .mapToLong(RepairAssignment::getEstimatedBytes).sum();
        assertEquals(plan.getEstimatedBytes(), assignmentBytes);
    }

    @Test
    public void testByteSplitterPreservesRemainder()
    {
        assertByteSplitterPreservesEstimate(fullRange, 53, 3);
    }

    @Test
    public void testByteSplitterPreservesZeroByteAssignments()
    {
        assertByteSplitterPreservesEstimate(fullRange, 2, 4);
    }

    @Test
    public void testByteSplitterUsesActualSplitCount()
    {
        assertByteSplitterPreservesEstimate(new Range<>(new LongToken(0), new LongToken(1)), 53, 3);
    }

    private void assertByteSplitterPreservesEstimate(Range<Token> range, long bytes, int partitions)
    {
        RepairTokenRangeSplitter splitter = new RepairTokenRangeSplitter(repairType, Map.of(PARTITIONS_PER_ASSIGNMENT, "1"));
        AutoRepairUtils.SizeEstimate estimate = new AutoRepairUtils.SizeEstimate(repairType, KEYSPACE, tableName, range, partitions, bytes, bytes);
        List<SizedRepairAssignment> assignments = splitter.getRepairAssignments(estimate);
        assertEquals(AutoRepairUtils.split(range, partitions).size(), assignments.size());
        assertEquals(bytes, assignments.stream().mapToLong(RepairAssignment::getEstimatedBytes).sum());
    }

    @Test
    public void testChangedRangesPreserveMemtableEstimate()
    {
        List<Range<Token>> plannedRanges = new ArrayList<>(AutoRepairUtils.split(fullRange, 3));
        KeyspaceRepairPlan plan = buildPlan(tableName, plannedRanges);
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.updateRepairScheduleStatistics(Collections.singletonList(new PrioritizedRepairPlan(0, Collections.singletonList(plan))));

        assertFalse(new HashSet<>(plannedRanges).equals(new HashSet<>(AutoRepairUtils.getTokenRanges(true, KEYSPACE))));

        RepairTokenRangeSplitter splitter = new RepairTokenRangeSplitter(repairType, Collections.emptyMap());
        List<RepairAssignment> assignments = splitter.getRepairAssignments(true, Collections.singletonList(new PrioritizedRepairPlan(0, Collections.singletonList(plan))))
                                                    .next().getRepairAssignments();
        assertEquals(plannedRanges.size(), assignments.size());
        assertTrue(assignments.stream().allMatch(assignment -> plannedRanges.contains(assignment.getTokenRange())));
        long assignmentBytes = assignments.stream().mapToLong(RepairAssignment::getEstimatedBytes).sum();
        assertEquals("Range changes must not count the table's memtable once per new range", state.getTotalBytesToRepair(), assignmentBytes);
    }

    @Test
    public void testUnplannedRangeIsRejected()
    {
        KeyspaceRepairPlan plan = buildPlan(tableName, Collections.singletonList(fullRange));
        RepairTokenRangeSplitter splitter = new RepairTokenRangeSplitter(repairType, Collections.emptyMap());
        Range<Token> unplannedRange = AutoRepairUtils.split(fullRange, 3).iterator().next();
        assertThatThrownBy(() -> splitter.getRepairAssignmentsForTable(plan, tableName, unplannedRange))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Missing planned size estimate");
    }

    @Test
    public void testChangedRangesLeaveBudgetForFollowingTable()
    {
        KeyspaceRepairPlan plan = buildPlan(tableName, new ArrayList<>(AutoRepairUtils.split(fullRange, 3)));
        String followingTable = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");
        KeyspaceRepairPlan followingPlan = buildPlan(followingTable, Collections.singletonList(fullRange));

        long budget = new LongMebibytesBound("1MiB").toBytes();
        RepairTokenRangeSplitter splitter = new RepairTokenRangeSplitter(repairType, Map.of(BYTES_PER_ASSIGNMENT, "1MiB",
                                                                                          MAX_BYTES_PER_SCHEDULE, "1MiB"));
        Iterator<KeyspaceRepairAssignments> iterator = splitter.getRepairAssignments(true, List.of(new PrioritizedRepairPlan(0, List.of(plan, followingPlan))));
        List<SizedRepairAssignment> assignments = new ArrayList<>();
        for (RepairAssignment assignment : iterator.next().getRepairAssignments())
            assignments.add((SizedRepairAssignment) assignment);
        List<RepairAssignment> followingAssignments = iterator.next().getRepairAssignments();
        assertEquals(1, followingAssignments.size());
        for (RepairAssignment assignment : followingAssignments)
            assignments.add((SizedRepairAssignment) assignment);

        long remainingBudget = plan.getEstimatedBytes() + followingPlan.getEstimatedBytes();
        assertTrue(remainingBudget < budget);
        FilteredRepairAssignments filtered = splitter.filterRepairAssignments(0, KEYSPACE, assignments, budget - remainingBudget);

        assertEquals("Both tables fit the planned budget; inflated fallback bytes must not exclude assignments",
                     assignments.size(), filtered.repairAssignments.size());
        assertTrue(filtered.repairAssignments.contains(followingAssignments.get(0)));
    }

    @Test
    public void testEmptyRangeSnapshotProducesNoAssignments()
    {
        KeyspaceRepairPlan plan = new KeyspaceRepairPlan(KEYSPACE, List.of(tableName), Collections.emptyList(),
                                                        Map.of(getKeyspaceTableName(KEYSPACE, tableName), Collections.emptyMap()));
        List<PrioritizedRepairPlan> plans = List.of(new PrioritizedRepairPlan(0, List.of(plan)));
        assertTrue(new RepairTokenRangeSplitter(repairType, Collections.emptyMap()).getRepairAssignments(true, plans).next().getRepairAssignments().isEmpty());
        assertTrue(new FixedSplitTokenRangeSplitter(repairType, Collections.emptyMap()).getRepairAssignments(true, plans).next().getRepairAssignments().isEmpty());
        assertEquals(0, plan.getEstimatedBytes());
    }

    @Test
    public void testPrioritiesShareRangeSnapshot() throws Exception
    {
        String highPriorityTable = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH auto_repair = {'priority': '2'}");
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");
        Token localToken = ClusterMetadata.current().tokenMap.tokens(ClusterMetadata.current().myNodeId()).iterator().next();
        NodeId joiningNode = Register.register(new NodeAddresses(InetAddressAndPort.getByName("127.0.0.2")));
        try
        {
            List<PrioritizedRepairPlan> plans = PrioritizedRepairPlan.build(Map.of(KEYSPACE, List.of(highPriorityTable, tableName)), repairType,
                                                                          names -> {
                                                                              if (names.equals(List.of(tableName)))
                                                                                  UnsafeJoin.unsafeJoin(joiningNode, Collections.singleton(localToken.nextValidToken()));
                                                                          }, true);
            assertEquals(2, plans.size());
            KeyspaceRepairPlan first = plans.get(0).getKeyspaceRepairPlans().get(0);
            KeyspaceRepairPlan second = plans.get(1).getKeyspaceRepairPlans().get(0);
            assertEquals(first.getTokenRanges(), second.getTokenRanges());
            assertFalse(new HashSet<>(first.getTokenRanges()).equals(new HashSet<>(AutoRepairUtils.getTokenRanges(true, KEYSPACE))));
        }
        finally
        {
            Unregister.unregister(joiningNode);
        }
    }

    @Test
    public void testNodeJoinPreservesPrimaryRangeEstimate() throws Exception
    {
        assertNodeJoinPreservesEstimate(true);
    }

    @Test
    public void testNodeJoinPreservesLocalRangeEstimate() throws Exception
    {
        assertNodeJoinPreservesEstimate(false);
    }

    @Test
    public void testNodeLeavePreservesRangeEstimates() throws Exception
    {
        assertNodeChangePreservesEstimate(false);
    }

    @Test
    public void testNodeMovePreservesRangeEstimates() throws Exception
    {
        assertNodeChangePreservesEstimate(true);
    }

    private void assertNodeChangePreservesEstimate(boolean moving) throws Exception
    {
        Token localToken = ClusterMetadata.current().tokenMap.tokens(ClusterMetadata.current().myNodeId()).iterator().next();
        NodeId remoteNode = Register.register(new NodeAddresses(InetAddressAndPort.getByName("127.0.0.2")));
        boolean registered = true;
        try
        {
            UnsafeJoin.unsafeJoin(remoteNode, Collections.singleton(localToken.nextValidToken()));
            List<PrioritizedRepairPlan> primaryPlans = PrioritizedRepairPlan.build(Map.of(KEYSPACE, List.of(tableName)), repairType, names -> {}, true);
            List<PrioritizedRepairPlan> localPlans = PrioritizedRepairPlan.build(Map.of(KEYSPACE, List.of(tableName)), repairType, names -> {}, false);
            if (moving)
            {
                ClusterMetadataService cms = ClusterMetadataService.instance();
                cms.commit(new PrepareMove(remoteNode, Collections.singleton(localToken.nextValidToken().nextValidToken()), cms.placementProvider(), false));
                Move move = (Move) ClusterMetadata.current().inProgressSequences.get(remoteNode);
                cms.commit(move.startMove);
                cms.commit(move.midMove);
                cms.commit(move.finishMove);
            }
            else
            {
                Unregister.unregister(remoteNode);
                registered = false;
            }
            assertSplittersUsePlanAfterRangeChange(true, primaryPlans);
            assertSplittersUsePlanAfterRangeChange(false, localPlans);
        }
        finally
        {
            if (registered)
                Unregister.unregister(remoteNode);
        }
    }

    private void assertNodeJoinPreservesEstimate(boolean primaryRangeOnly) throws Exception
    {
        List<PrioritizedRepairPlan> plans = PrioritizedRepairPlan.build(Map.of(KEYSPACE, Collections.singletonList(tableName)),
                                                                      repairType, names -> {}, primaryRangeOnly);
        KeyspaceRepairPlan plan = plans.get(0).getKeyspaceRepairPlans().get(0);
        assertEquals(memtableBytes(tableName), plan.getEstimatedBytes());
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.updateRepairScheduleStatistics(plans);
        List<Range<Token>> originalRanges = AutoRepairUtils.getTokenRanges(primaryRangeOnly, KEYSPACE);

        Collection<Token> localTokens = ClusterMetadata.current().tokenMap.tokens(ClusterMetadata.current().myNodeId());
        assertEquals(1, localTokens.size());
        Token localToken = localTokens.iterator().next();
        Token joiningToken = localToken.nextValidToken();
        assertTrue(joiningToken.compareTo(localToken) > 0);
        NodeId joiningNode = Register.register(new NodeAddresses(InetAddressAndPort.getByName("127.0.0.2")));
        try
        {
            UnsafeJoin.unsafeJoin(joiningNode, Collections.singleton(joiningToken));
            List<Range<Token>> changedRanges = AutoRepairUtils.getTokenRanges(primaryRangeOnly, KEYSPACE);
            assertFalse(originalRanges.equals(changedRanges));
            assertTrue(changedRanges.stream().anyMatch(range -> plan.getSizeEstimate(getKeyspaceTableName(KEYSPACE, tableName), range) == null));

            assertSplittersUsePlanAfterRangeChange(primaryRangeOnly, plans);
            assertEquals(memtableBytes(tableName), state.getTotalBytesToRepair());

            Range<Token> invalidRange = originalRanges.stream()
                                                     .filter(range -> StorageService.instance.getLocalRanges(KEYSPACE).stream().noneMatch(localRange -> localRange.contains(range)))
                                                     .findFirst().orElseThrow(AssertionError::new);
            assertThatThrownBy(() -> ActiveRepairService.instance().getNeighbors(KEYSPACE, StorageService.instance.getLocalRanges(KEYSPACE), invalidRange))
            .isInstanceOf(IllegalArgumentException.class);
        }
        finally
        {
            Unregister.unregister(joiningNode);
        }
    }

    private void assertSplittersUsePlanAfterRangeChange(boolean primaryRangeOnly, List<PrioritizedRepairPlan> plans)
    {
        KeyspaceRepairPlan plan = plans.get(0).getKeyspaceRepairPlans().get(0);
        assertFalse(new HashSet<>(plan.getTokenRanges()).equals(new HashSet<>(AutoRepairUtils.getTokenRanges(primaryRangeOnly, KEYSPACE))));
        assertEquals(memtableBytes(tableName), plan.getEstimatedBytes());
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        boolean previousByKeyspace = config.getRepairByKeyspace(repairType);
        try
        {
            for (boolean byKeyspace : new boolean[]{ false, true })
            {
                config.setRepairByKeyspace(repairType, byKeyspace);
                assertAssignmentsUsePlan(new RepairTokenRangeSplitter(repairType, Collections.emptyMap()), primaryRangeOnly, plans, plan);
                for (int splits : new int[]{ 1, 4, 256 })
                    assertAssignmentsUsePlan(new FixedSplitTokenRangeSplitter(repairType,
                                                                             Map.of(FixedSplitTokenRangeSplitter.NUMBER_OF_SUBRANGES, Integer.toString(splits))),
                                             primaryRangeOnly, plans, plan);
            }
        }
        finally
        {
            config.setRepairByKeyspace(repairType, previousByKeyspace);
        }
    }

    private void assertAssignmentsUsePlan(IAutoRepairTokenRangeSplitter splitter, boolean primaryRangeOnly,
                                          List<PrioritizedRepairPlan> plans, KeyspaceRepairPlan plan)
    {
        List<RepairAssignment> assignments = splitter.getRepairAssignments(primaryRangeOnly, plans).next().getRepairAssignments();
        assertFalse(assignments.isEmpty());
        assertEquals(plan.getEstimatedBytes(), assignments.stream().mapToLong(RepairAssignment::getEstimatedBytes).sum());
        assertTrue(assignments.stream().allMatch(assignment -> plan.getTokenRanges().stream().anyMatch(range -> range.contains(assignment.getTokenRange()))));
    }

    private KeyspaceRepairPlan buildPlan(String table, List<Range<Token>> ranges)
    {
        KeyspaceRepairPlan plan = new KeyspaceRepairPlan(KEYSPACE, Collections.singletonList(table), ranges,
                                                        AutoRepairUtils.calcTotalBytesToBeRepaired(repairType, KEYSPACE,
                                                                                                   Collections.singletonList(table), ranges));
        assertEquals(memtableBytes(table), plan.getEstimatedBytes());
        return plan;
    }

    private List<SizedRepairAssignment> getAssignments(RepairTokenRangeSplitter splitter, KeyspaceRepairPlan plan,
                                                      String table, List<Range<Token>> ranges)
    {
        List<SizedRepairAssignment> assignments = new ArrayList<>();
        for (Range<Token> range : ranges)
            assignments.addAll(splitter.getRepairAssignmentsForTable(plan, table, range));
        return assignments;
    }

    private long memtableBytes(String table)
    {
        return ColumnFamilyStore.getIfExists(KEYSPACE, table).getTracker().getView().getCurrentMemtable().getLiveDataSize();
    }
}
