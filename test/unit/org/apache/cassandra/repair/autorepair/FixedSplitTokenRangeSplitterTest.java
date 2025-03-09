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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_DEFAULT_RF;
import static org.apache.cassandra.cql3.CQLTester.Fuzzed.setupSeed;
import static org.apache.cassandra.cql3.CQLTester.Fuzzed.updateConfigs;
import static org.apache.cassandra.repair.autorepair.FixedSplitTokenRangeSplitter.DEFAULT_SUBRANGES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link org.apache.cassandra.repair.autorepair.FixedSplitTokenRangeSplitter}
 */
@RunWith(Parameterized.class)
public class FixedSplitTokenRangeSplitterTest
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE1 = "tbl1";
    private static final String TABLE2 = "tbl2";
    private static final String TABLE3 = "tbl3";

    @Parameterized.Parameter(0)
    public AutoRepairConfig.RepairType repairType;

    @Parameterized.Parameter(1)
    public int numberOfSubRanges;

    @Parameterized.Parameter(2)
    public int numTokens;

    public static final int curTokenRange = 1;

    @Parameterized.Parameters(name = "repairType={0}, numberOfSubRanges={1}, numTokens={2}")
    public static Collection<Object[]> parameters()
    {
        List<Object[]> params = new ArrayList<>();
        for (AutoRepairConfig.RepairType type : AutoRepairConfig.RepairType.values())
        {
            for (int subRange : Arrays.asList(1, 2, 4, 8, 16, 32, 64, 128, 256))
            {
                for (int numToken : Arrays.asList(curTokenRange))
                {
                    params.add(new Object[]{ type, subRange, numToken });
                }
            }
        }
        return params;
    }

    @BeforeClass
    public static void setupClass() throws Exception
    {
        setupSeed();
        updateConfigs();
        DatabaseDescriptor.setPartitioner("org.apache.cassandra.dht.Murmur3Partitioner");
        ServerTestUtils.prepareServerNoRegister();

        Set<Token> tokens1 = BootStrapper.getRandomTokens(ClusterMetadata.current(), curTokenRange);
        ServerTestUtils.registerLocal(tokens1);
        // Ensure that the on-disk format statics are loaded before the test run
        Version.LATEST.onDiskFormat();
        StorageService.instance.doAutoRepairSetup();

        SYSTEM_DISTRIBUTED_DEFAULT_RF.setInt(1);
        QueryProcessor.executeInternal(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
    }

    @Test
    public void testTokenRangesSplitByTable()
    {
        int numberOfSplits = calcSplits(numberOfSubRanges);
        AutoRepairService.instance.getAutoRepairConfig().setRepairByKeyspace(repairType, false);
        Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRanges(KEYSPACE);
        assertEquals(numTokens, tokens.size());
        List<String> tables = Arrays.asList(TABLE1, TABLE2, TABLE3);
        List<Range<Token>> expectedToken = new ArrayList<>();
        for (int i = 0; i < tables.size(); i++)
        {
            for (Range<Token> range : tokens)
            {
                expectedToken.addAll(AutoRepairUtils.split(range, numberOfSplits));
            }
        }

        List<PrioritizedRepairPlan> plan = PrioritizedRepairPlan.buildSingleKeyspacePlan(repairType, KEYSPACE, TABLE1, TABLE2, TABLE3);

        Iterator<KeyspaceRepairAssignments> keyspaceAssignments = new FixedSplitTokenRangeSplitter(repairType, Collections.singletonMap(FixedSplitTokenRangeSplitter.NUMBER_OF_SUBRANGES, Integer.toString(numberOfSubRanges)))
                                                                  .getRepairAssignments(true, plan);

        // should be only 1 entry for the keyspace.
        assertTrue(keyspaceAssignments.hasNext());
        KeyspaceRepairAssignments keyspace = keyspaceAssignments.next();
        assertFalse(keyspaceAssignments.hasNext());

        List<RepairAssignment> assignments = keyspace.getRepairAssignments();
        assertEquals(numTokens * numberOfSplits * tables.size(), assignments.size());
        assertEquals(expectedToken.size(), assignments.size());

        int assignmentsPerTable = numTokens * numberOfSplits;
        for (int i = 0; i < tables.size(); i++)
        {
            List<RepairAssignment> assignmentForATable = new ArrayList<>();
            List<Range<Token>> expectedTokensForATable = new ArrayList<>();
            for (int j = 0; j < assignmentsPerTable; j++)
            {
                assertEquals(Arrays.asList(tables.get(i)), assignments.get(i*assignmentsPerTable + j).getTableNames());
                assignmentForATable.add(assignments.get(i*assignmentsPerTable+j));
                expectedTokensForATable.add(expectedToken.get(i*assignmentsPerTable+j));
            }
            compare(numberOfSplits, expectedTokensForATable, assignmentForATable);
        }
    }

    @Test
    public void testTokenRangesSplitByKeyspace()
    {
        int numberOfSplits = calcSplits(numberOfSubRanges);
        AutoRepairService.instance.getAutoRepairConfig().setRepairByKeyspace(repairType, true);
        Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRanges(KEYSPACE);
        assertEquals(numTokens, tokens.size());
        List<String> tables = Arrays.asList(TABLE1, TABLE2, TABLE3);
        List<Range<Token>> expectedToken = new ArrayList<>();
        for (Range<Token> range : tokens)
        {
            expectedToken.addAll(AutoRepairUtils.split(range, numberOfSplits));
        }

        List<PrioritizedRepairPlan> plan = PrioritizedRepairPlan.buildSingleKeyspacePlan(repairType, KEYSPACE, TABLE1, TABLE2, TABLE3);

        Iterator<KeyspaceRepairAssignments> keyspaceAssignments = new FixedSplitTokenRangeSplitter(repairType, Collections.singletonMap(FixedSplitTokenRangeSplitter.NUMBER_OF_SUBRANGES, Integer.toString(numberOfSubRanges)))
                                                                  .getRepairAssignments(true, plan);

        // should be only 1 entry for the keyspace.
        assertTrue(keyspaceAssignments.hasNext());
        KeyspaceRepairAssignments keyspace = keyspaceAssignments.next();
        assertFalse(keyspaceAssignments.hasNext());

        List<RepairAssignment> assignments = keyspace.getRepairAssignments();
        assertNotNull(assignments);

        assertEquals(numTokens * numberOfSplits, assignments.size());
        assertEquals(expectedToken.size(), assignments.size());

        compare(numberOfSplits, expectedToken, assignments);
    }

    @Test
    public void testTokenRangesWithDefaultSplit()
    {
        int numberOfSplits = calcSplits(DEFAULT_SUBRANGES);
        Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRanges(KEYSPACE);
        assertEquals(numTokens, tokens.size());
        List<Range<Token>> expectedToken = new ArrayList<>();
        for (Range<Token> range : tokens)
        {
            expectedToken.addAll(AutoRepairUtils.split(range, numberOfSplits));
        }

        List<PrioritizedRepairPlan> plan = PrioritizedRepairPlan.buildSingleKeyspacePlan(repairType, KEYSPACE, TABLE1);

        Iterator<KeyspaceRepairAssignments> keyspaceAssignments = new FixedSplitTokenRangeSplitter(repairType, Collections.emptyMap()).getRepairAssignments(true, plan);

        // should be only 1 entry for the keyspace.
        assertTrue(keyspaceAssignments.hasNext());
        KeyspaceRepairAssignments keyspace = keyspaceAssignments.next();
        assertFalse(keyspaceAssignments.hasNext());

        List<RepairAssignment> assignments = keyspace.getRepairAssignments();
        assertNotNull(assignments);

        // should be 3 entries for the table which covers each token range.
        assertEquals(numTokens * numberOfSplits, assignments.size());

        compare(numberOfSplits, expectedToken, assignments);
    }

    private void compare(int numberOfSplits, List<Range<Token>> expectedToken, List<RepairAssignment> assignments)
    {
        assertEquals(expectedToken.size(), assignments.size());
        Set<Range<Token>> a = new TreeSet<>();
        Set<Range<Token>> b = new TreeSet<>();
        for (int i = 0; i < numTokens * numberOfSplits; i++)
        {
            a.add(expectedToken.get(i));
            b.add(assignments.get(i).getTokenRange());
        }
        assertEquals(a, b);
    }

    private int calcSplits(int subRange)
    {
        return Math.max(1, subRange / numTokens);
    }
}
