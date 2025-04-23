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

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.DataStorageSpec.LongMebibytesBound;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.RepairTokenRangeSplitter.FilteredRepairAssignments;
import org.apache.cassandra.repair.autorepair.RepairTokenRangeSplitter.SizeEstimate;
import org.apache.cassandra.repair.autorepair.RepairTokenRangeSplitter.SizedRepairAssignment;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.repair.autorepair.RepairTokenRangeSplitter.MAX_BYTES_PER_SCHEDULE;
import static org.apache.cassandra.repair.autorepair.RepairTokenRangeSplitter.BYTES_PER_ASSIGNMENT;
import static org.apache.cassandra.repair.autorepair.RepairTokenRangeSplitter.MAX_TABLES_PER_ASSIGNMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link org.apache.cassandra.repair.autorepair.RepairTokenRangeSplitter}
 */
@RunWith(Parameterized.class)
public class RepairTokenRangeSplitterTest extends CQLTester
{
    private RepairTokenRangeSplitter repairRangeSplitter;
    private String tableName;
    private static Range<Token> FULL_RANGE;

    @Parameterized.Parameter()
    public String sstableFormat;

    @Parameterized.Parameters(name = "sstableFormat={0}")
    public static Collection<String> sstableFormats()
    {
        return List.of(BtiFormat.NAME, BigFormat.NAME);
    }

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        AutoRepairService.setup();
        FULL_RANGE = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(), DatabaseDescriptor.getPartitioner().getMaximumTokenForSplitting());
    }

    @Before
    public void setUp()
    {
        AutoRepairService.instance.getAutoRepairConfig().setRepairByKeyspace(RepairType.FULL, true);
        DatabaseDescriptor.setSelectedSSTableFormat(DatabaseDescriptor.getSSTableFormats().get(sstableFormat));
        repairRangeSplitter = new RepairTokenRangeSplitter(RepairType.FULL, Collections.emptyMap());
        tableName = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT)");
        // ensure correct format is selected.
        if (sstableFormat.equalsIgnoreCase(BigFormat.NAME))
        {
            assertTrue(BigFormat.isSelected());
        }
        else
        {
            assertTrue(BtiFormat.isSelected());
        }
    }

    @Test
    public void testSizePartitionCount()
    {
        insertAndFlushTable(tableName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        try (Refs<SSTableReader> sstables = RepairTokenRangeSplitter.getSSTableReaderRefs(RepairType.FULL, KEYSPACE, tableName, FULL_RANGE))
        {
            assertEquals(10, sstables.iterator().next().getEstimatedPartitionSize().count());
            SizeEstimate sizes = RepairTokenRangeSplitter.getSizesForRangeOfSSTables(RepairType.FULL, KEYSPACE, tableName, FULL_RANGE, sstables);
            assertEquals(10, sizes.partitions);
        }
    }

    @Test
    public void testSizePartitionCountSplit()
    {
        int partitionCount = 100_000;
        int[] values = new int[partitionCount];
        for (int i = 0; i < values.length; i++)
            values[i] = i + 1;
        insertAndFlushTable(tableName, values);
        Iterator<Range<Token>> range = AutoRepairUtils.split(FULL_RANGE, 2).iterator();
        Range<Token> tokenRange1 = range.next();
        Range<Token> tokenRange2 = range.next();
        Assert.assertFalse(range.hasNext());

        try (Refs<SSTableReader> sstables1 = RepairTokenRangeSplitter.getSSTableReaderRefs(RepairType.FULL, KEYSPACE, tableName, tokenRange1);
             Refs<SSTableReader> sstables2 = RepairTokenRangeSplitter.getSSTableReaderRefs(RepairType.FULL, KEYSPACE, tableName, tokenRange2))
        {
            SizeEstimate sizes1 = RepairTokenRangeSplitter.getSizesForRangeOfSSTables(RepairType.FULL, KEYSPACE, tableName, tokenRange1, sstables1);
            SizeEstimate sizes2 = RepairTokenRangeSplitter.getSizesForRangeOfSSTables(RepairType.FULL, KEYSPACE, tableName, tokenRange2, sstables2);

            // +-5% because including entire compression blocks covering token range, HLL merge and the applying of range size approx ratio causes estimation errors
            long allowableDelta = (long) (partitionCount * .05);
            long estimatedPartitionDelta = Math.abs(partitionCount - (sizes1.partitions + sizes2.partitions));
            assertTrue("Partition count delta was +/-" + estimatedPartitionDelta + " but expected +/- " + allowableDelta, estimatedPartitionDelta <= allowableDelta);
        }
    }

    @Test
    public void testGetRepairAssignmentsForTable_NoSSTables()
    {
        // Should return 1 assignment if there are no SSTables
        List<SizedRepairAssignment> assignments = repairRangeSplitter.getRepairAssignmentsForTable(CQLTester.KEYSPACE, tableName, FULL_RANGE);
        assertEquals(1, assignments.size());
    }

    @Test
    public void testGetRepairAssignmentsForTable_Single()
    {
        insertAndFlushSingleTable();
        List<SizedRepairAssignment> assignments = repairRangeSplitter.getRepairAssignmentsForTable(CQLTester.KEYSPACE, tableName, FULL_RANGE);
        assertEquals(1, assignments.size());
    }

    @Test
    public void testGetRepairAssignmentsForTable_BatchingTables()
    {
        repairRangeSplitter = new RepairTokenRangeSplitter(RepairType.FULL, Collections.singletonMap(MAX_TABLES_PER_ASSIGNMENT, "2"));

        List<String> tableNames = createAndInsertTables(3);
        List<SizedRepairAssignment> assignments = repairRangeSplitter.getRepairAssignmentsForKeyspace(RepairType.FULL, KEYSPACE, tableNames, FULL_RANGE);

        // We expect two assignments, one with table1 and table2 batched, and one with table3
        assertEquals(2, assignments.size());
        assertEquals(2, assignments.get(0).getTableNames().size());
        assertEquals(1, assignments.get(1).getTableNames().size());
    }

    @Test
    public void testGetRepairAssignmentsForTable_BatchSize()
    {
        repairRangeSplitter = new RepairTokenRangeSplitter(RepairType.FULL, Collections.singletonMap(MAX_TABLES_PER_ASSIGNMENT, "2"));

        List<String> tableNames = createAndInsertTables(2);
        List<SizedRepairAssignment> assignments = repairRangeSplitter.getRepairAssignmentsForKeyspace(RepairType.FULL, KEYSPACE, tableNames, FULL_RANGE);

        // We expect one assignment, with two tables batched
        assertEquals(1, assignments.size());
        assertEquals(2, assignments.get(0).getTableNames().size());
    }

    @Test
    public void testGetRepairAssignmentsForTable_NoBatching()
    {
        repairRangeSplitter = new RepairTokenRangeSplitter(RepairType.FULL, Collections.singletonMap(MAX_TABLES_PER_ASSIGNMENT, "1"));

        List<String> tableNames = createAndInsertTables(3);
        List<SizedRepairAssignment> assignments = repairRangeSplitter.getRepairAssignmentsForKeyspace(RepairType.FULL, KEYSPACE, tableNames, FULL_RANGE);

        assertEquals(3, assignments.size());
    }

    @Test
    public void testGetRepairAssignmentsForTable_AllBatched()
    {
        repairRangeSplitter = new RepairTokenRangeSplitter(RepairType.FULL, Collections.singletonMap(MAX_TABLES_PER_ASSIGNMENT, "100"));

        List<String> tableNames = createAndInsertTables(5);
        List<SizedRepairAssignment> assignments = repairRangeSplitter.getRepairAssignmentsForKeyspace(RepairType.FULL, KEYSPACE, tableNames, FULL_RANGE);

        assertEquals(1, assignments.size());
    }

    @Test(expected = IllegalStateException.class)
    public void testMergeEmptyAssignments()
    {
        // Test when the list of assignments is empty
        List<SizedRepairAssignment> emptyAssignments = Collections.emptyList();
        RepairTokenRangeSplitter.merge(emptyAssignments);
    }

    @Test
    public void testMergeSingleAssignment()
    {
        // Test when there is only one assignment in the list
        String keyspaceName = "testKeyspace";
        List<String> tableNames = Arrays.asList("table1", "table2");

        SizedRepairAssignment assignment = new SizedRepairAssignment(FULL_RANGE, keyspaceName, tableNames);
        List<SizedRepairAssignment> assignments = Collections.singletonList(assignment);

        SizedRepairAssignment result = RepairTokenRangeSplitter.merge(assignments);

        assertEquals(FULL_RANGE, result.getTokenRange());
        assertEquals(keyspaceName, result.getKeyspaceName());
        assertEquals(new HashSet<>(tableNames), new HashSet<>(result.getTableNames()));
    }

    @Test
    public void testMergeMultipleAssignmentsWithSameTokenRangeAndKeyspace()
    {
        // Test merging multiple assignments with the same token range and keyspace
        String keyspaceName = "testKeyspace";
        List<String> tableNames1 = Arrays.asList("table1", "table2");
        List<String> tableNames2 = Arrays.asList("table2", "table3");

        SizedRepairAssignment assignment1 = new SizedRepairAssignment(FULL_RANGE, keyspaceName, tableNames1);
        SizedRepairAssignment assignment2 = new SizedRepairAssignment(FULL_RANGE, keyspaceName, tableNames2);
        List<SizedRepairAssignment> assignments = Arrays.asList(assignment1, assignment2);

        SizedRepairAssignment result = RepairTokenRangeSplitter.merge(assignments);

        assertEquals(FULL_RANGE, result.getTokenRange());
        assertEquals(keyspaceName, result.getKeyspaceName());
        assertEquals(new HashSet<>(Arrays.asList("table1", "table2", "table3")), new HashSet<>(result.getTableNames()));
    }

    @Test(expected = IllegalStateException.class)
    public void testMergeDifferentTokenRange()
    {
        // Test merging assignments with different token ranges
        Iterator<Range<Token>> range = AutoRepairUtils.split(FULL_RANGE, 2).iterator(); // Split the full range into two ranges ie (0-100, 100-200
        Range<Token> tokenRange1 = range.next();
        Range<Token> tokenRange2 = range.next();
        Assert.assertFalse(range.hasNext());

        String keyspaceName = "testKeyspace";
        List<String> tableNames = Arrays.asList("table1", "table2");

        SizedRepairAssignment assignment1 = new SizedRepairAssignment(tokenRange1, keyspaceName, tableNames);
        SizedRepairAssignment assignment2 = new SizedRepairAssignment(tokenRange2, keyspaceName, tableNames);
        List<SizedRepairAssignment> assignments = Arrays.asList(assignment1, assignment2);

        RepairTokenRangeSplitter.merge(assignments); // Should throw IllegalStateException
    }

    @Test(expected = IllegalStateException.class)
    public void testMergeDifferentKeyspaceName()
    {
        // Test merging assignments with different keyspace names
        List<String> tableNames = Arrays.asList("table1", "table2");

        SizedRepairAssignment assignment1 = new SizedRepairAssignment(FULL_RANGE, "keyspace1", tableNames);
        SizedRepairAssignment assignment2 = new SizedRepairAssignment(FULL_RANGE, "keyspace2", tableNames);
        List<SizedRepairAssignment> assignments = Arrays.asList(assignment1, assignment2);

        RepairTokenRangeSplitter.merge(assignments); // Should throw IllegalStateException
    }

    @Test
    public void testMergeWithDuplicateTables()
    {
        // Test merging assignments with duplicate table names
        String keyspaceName = "testKeyspace";
        List<String> tableNames1 = Arrays.asList("table1", "table2");
        List<String> tableNames2 = Arrays.asList("table2", "table3");

        SizedRepairAssignment assignment1 = new SizedRepairAssignment(FULL_RANGE, keyspaceName, tableNames1);
        SizedRepairAssignment assignment2 = new SizedRepairAssignment(FULL_RANGE, keyspaceName, tableNames2);
        List<SizedRepairAssignment> assignments = Arrays.asList(assignment1, assignment2);

        RepairAssignment result = RepairTokenRangeSplitter.merge(assignments);

        // The merged result should contain all unique table names
        assertEquals(new HashSet<>(Arrays.asList("table1", "table2", "table3")), new HashSet<>(result.getTableNames()));
    }

    @Test
    public void testGetRepairAssignmentsSplitsBySubrangeSizeAndFilterLimitsByMaxBytesPerSchedule()
    {
        // Ensures that getRepairAssignments splits by BYTES_PER_ASSIGNMENT and filterRepairAssignments limits by MAX_BYTES_PER_SCHEDULE.
        repairRangeSplitter = new RepairTokenRangeSplitter(RepairType.INCREMENTAL, Collections.emptyMap());
        repairRangeSplitter.setParameter(BYTES_PER_ASSIGNMENT, "50GiB");
        repairRangeSplitter.setParameter(MAX_BYTES_PER_SCHEDULE, "100GiB");

        // Given a size estimate of 1024GiB, we should expect 21 splits (50GiB*21 = 1050GiB < 1024GiB)
        SizeEstimate sizeEstimate = sizeEstimateByBytes(new LongMebibytesBound("1024GiB"));

        List<SizedRepairAssignment> assignments = repairRangeSplitter.getRepairAssignments(Collections.singletonList(sizeEstimate));

        // Should be 21 assignments, each being ~48.76 GiB
        assertEquals(21, assignments.size());
        long expectedBytes = 52357696560L;
        for (int i = 0; i < assignments.size(); i++)
        {
            SizedRepairAssignment assignment = assignments.get(i);
            assertEquals("Did not get expected value for assignment " + i, 52357696560L, assignment.getEstimatedBytes());
        }

        // When filtering we should only get 2 assignments back (48.76 * 2 < 100GiB)
        FilteredRepairAssignments filteredRepairAssignments = repairRangeSplitter.filterRepairAssignments(0, KEYSPACE, assignments, 0);
        List<RepairAssignment> finalRepairAssignments = filteredRepairAssignments.repairAssignments;
        assertEquals(2, finalRepairAssignments.size());
        assertEquals(expectedBytes * 2, filteredRepairAssignments.newBytesSoFar);
    }

    @Test
    public void testTokenRangesRepairByKeyspace()
    {
        AutoRepairService.instance.getAutoRepairConfig().setRepairByKeyspace(RepairType.FULL, true);

        final KeyspaceRepairPlan repairPlan = new KeyspaceRepairPlan("system_auth", new ArrayList<>(AuthKeyspace.TABLE_NAMES));
        final PrioritizedRepairPlan prioritizedRepairPlan = new PrioritizedRepairPlan(0, List.of(repairPlan));

        Iterator<KeyspaceRepairAssignments> keyspaceAssignments = repairRangeSplitter.getRepairAssignments(true, List.of(prioritizedRepairPlan));

        // should be only 1 entry for the keyspace.
        assertTrue(keyspaceAssignments.hasNext());
        KeyspaceRepairAssignments keyspace = keyspaceAssignments.next();
        assertFalse(keyspaceAssignments.hasNext());

        List<RepairAssignment> assignments = keyspace.getRepairAssignments();
        assertNotNull(assignments);

        // Should only be two assignments (since single node encompasses the whole range, should get 2 primary ranges)
        // to account for the range wrapping the ring.
        assertEquals(2, assignments.size());

        for (RepairAssignment assignment : assignments)
        {
            assertEquals(AuthKeyspace.TABLE_NAMES.size(), assignment.getTableNames().size());
        }
    }

    @Test
    public void testTokenRangesRepairByKeyspaceFalse()
    {
        AutoRepairService.instance.getAutoRepairConfig().setRepairByKeyspace(RepairType.FULL, false);

        final KeyspaceRepairPlan repairPlan = new KeyspaceRepairPlan("system_auth", new ArrayList<>(AuthKeyspace.TABLE_NAMES));
        final PrioritizedRepairPlan prioritizedRepairPlan = new PrioritizedRepairPlan(0, List.of(repairPlan));

        Iterator<KeyspaceRepairAssignments> keyspaceAssignments = repairRangeSplitter.getRepairAssignments(true, List.of(prioritizedRepairPlan));

        // should be only 1 entry for the keyspace.
        assertTrue(keyspaceAssignments.hasNext());
        KeyspaceRepairAssignments keyspace = keyspaceAssignments.next();
        assertFalse(keyspaceAssignments.hasNext());

        List<RepairAssignment> assignments = keyspace.getRepairAssignments();
        assertNotNull(assignments);

        // Should be two ranges * X system_auth table names assignments
        assertEquals(2 * AuthKeyspace.TABLE_NAMES.size(), assignments.size());

        // each assignment should only include one table.
        for (RepairAssignment assignment : assignments)
        {
            assertEquals(1, assignment.getTableNames().size());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetParameterShouldNotAllowUnknownParameter()
    {
        repairRangeSplitter.setParameter("unknown", "x");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetParameterShouldNotAllowSettingBytesPerAssignmentGreaterThanMaxBytesPerSchedule()
    {
        repairRangeSplitter.setParameter(MAX_BYTES_PER_SCHEDULE, "500GiB");
        repairRangeSplitter.setParameter(BYTES_PER_ASSIGNMENT, "600GiB");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetParameterShouldNotAllowSettingMaxBytesPerScheduleLessThanBytesPerAssignment()
    {
        repairRangeSplitter.setParameter(BYTES_PER_ASSIGNMENT, "100MiB");
        repairRangeSplitter.setParameter(MAX_BYTES_PER_SCHEDULE, "50MiB");
    }

    @Test
    public void testGetParameters()
    {
        repairRangeSplitter.setParameter(BYTES_PER_ASSIGNMENT, "100MiB");
        repairRangeSplitter.setParameter(MAX_TABLES_PER_ASSIGNMENT, "5");

        Map<String, String> parameters = repairRangeSplitter.getParameters();
        // Each parameter should be present.
        assertEquals(RepairTokenRangeSplitter.PARAMETERS.size(), parameters.size());
        // The parameters we explicitly set should be set exactly as we set them.
        assertEquals("100MiB", parameters.get(BYTES_PER_ASSIGNMENT));
        assertEquals("5", parameters.get(MAX_TABLES_PER_ASSIGNMENT));
    }

    private SizeEstimate sizeEstimateByBytes(LongMebibytesBound totalSize)
    {
        return sizeEstimateByBytes(totalSize, totalSize);
    }

    private SizeEstimate sizeEstimateByBytes(LongMebibytesBound sizeInRange, LongMebibytesBound totalSize)
    {
        return new SizeEstimate(RepairType.INCREMENTAL, KEYSPACE, "table1", FULL_RANGE, 1, sizeInRange.toBytes(), totalSize.toBytes());
    }

    private void insertAndFlushSingleTable()
    {
        execute("INSERT INTO %s (k, v) values (?, ?)", 1, 1);
        flush();
    }

    private List<String> createAndInsertTables(int count)
    {
        List<String> tableNames = new ArrayList<>();
        for (int i = 0; i < count; i++)
        {
            String tableName = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT)");
            tableNames.add(tableName);
            insertAndFlushTable(tableName);
        }
        return tableNames;
    }

    private void insertAndFlushTable(String tableName)
    {
        insertAndFlushTable(tableName, 1);
    }

    private void insertAndFlushTable(String tableName, int... vals)
    {
        for (int i : vals)
        {
            executeFormattedQuery("INSERT INTO " + KEYSPACE + '.' + tableName + " (k, v) values (?, ?)", i, i);
        }
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, tableName);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }
}
