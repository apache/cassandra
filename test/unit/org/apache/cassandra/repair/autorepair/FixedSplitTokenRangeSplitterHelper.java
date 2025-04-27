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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
import org.apache.cassandra.tcm.compatibility.TokenRingUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_DEFAULT_RF;
import static org.apache.cassandra.cql3.CQLTester.Fuzzed.setupSeed;
import static org.apache.cassandra.cql3.CQLTester.Fuzzed.updateConfigs;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.split;
import static org.apache.cassandra.repair.autorepair.FixedSplitTokenRangeSplitter.DEFAULT_NUMBER_OF_SUBRANGES;
import static org.apache.cassandra.repair.autorepair.FixedSplitTokenRangeSplitter.NUMBER_OF_SUBRANGES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Helper class for {@link FixedSplitTokenRangeSplitterNoVNodesTest} and {@link FixedSplitTokenRangeSplitterVNodesTest}
 */
public class FixedSplitTokenRangeSplitterHelper
{
    private static final String TABLE1 = "tbl1";
    private static final String TABLE2 = "tbl2";
    private static final String TABLE3 = "tbl3";
    public static final String KEYSPACE = "ks";
    public static final List<String> tables = Arrays.asList(TABLE1, TABLE2, TABLE3);
    public static final Map<String, Map<Range<Token>, AutoRepairUtils.SizeEstimate>> ksTablesEstimatedBytes = new HashMap<>();

    public static void setupClass(int numTokens) throws Exception
    {
        setupSeed();
        updateConfigs();
        DatabaseDescriptor.setPartitioner("org.apache.cassandra.dht.Murmur3Partitioner");
        ServerTestUtils.prepareServerNoRegister();

        Set<Token> tokens = BootStrapper.getRandomTokens(ClusterMetadata.current(), numTokens);
        ServerTestUtils.registerLocal(tokens);
        // Ensure that the on-disk format statics are loaded before the test run
        Version.LATEST.onDiskFormat();
        StorageService.instance.doAutoRepairSetup();

        SYSTEM_DISTRIBUTED_DEFAULT_RF.setInt(1);
        QueryProcessor.executeInternal(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", FixedSplitTokenRangeSplitterHelper.KEYSPACE));

        Pair<Collection<Range<Token>>, Integer> tokensAndWrappedAroundCount = getTokenRangesAndTotalWrapAroundCount();
        int totalToken = numTokens + tokensAndWrappedAroundCount.right();
        long perTokenSizeTable1 = 512L / totalToken;
        long perTokenSizeTable2 = 1024L / totalToken;
        long perTokenSizeTable3 = 2048L / totalToken;
        for (Range<Token> tokenRange : tokensAndWrappedAroundCount.left)
        {
            ksTablesEstimatedBytes.put(AutoRepairUtils.getKeyspaceTableName(KEYSPACE, TABLE1), new HashMap<>()
            {{
                put(tokenRange, new AutoRepairUtils.SizeEstimate(AutoRepairConfig.RepairType.FULL, "", "", tokenRange, 0, perTokenSizeTable1, perTokenSizeTable1));
            }});
            ksTablesEstimatedBytes.put(AutoRepairUtils.getKeyspaceTableName(KEYSPACE, TABLE2), new HashMap<>()
            {{
                put(tokenRange, new AutoRepairUtils.SizeEstimate(AutoRepairConfig.RepairType.FULL, "", "", tokenRange, 0, perTokenSizeTable2, perTokenSizeTable2));
            }});
            ksTablesEstimatedBytes.put(AutoRepairUtils.getKeyspaceTableName(KEYSPACE, TABLE3), new HashMap<>()
            {{
                put(tokenRange, new AutoRepairUtils.SizeEstimate(AutoRepairConfig.RepairType.FULL, "", "", tokenRange, 0, perTokenSizeTable3, perTokenSizeTable3));
            }});
        }
    }

    public static void testTokenRangesSplitByTable(int numTokens, int numberOfSubRanges, AutoRepairConfig.RepairType repairType)
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setRepairByKeyspace(repairType, false);
        Pair<Collection<Range<Token>>, Integer> tokensAndWrappedAroundCount = getTokenRangesAndTotalWrapAroundCount();
        Collection<Range<Token>> tokens = tokensAndWrappedAroundCount.left();
        // For the test case, the tokens are allocated dynamically, so we do not know which token-ranges wrap around.
        // As a result, we need to adjust the token count on a need basis.
        numTokens += tokensAndWrappedAroundCount.right();
        assertEquals(numTokens, tokens.size());
        List<Range<Token>> expectedToken = new ArrayList<>();
        int numberOfSplits = Math.max(1, numberOfSubRanges / tokens.size());
        for (int i = 0; i < tables.size(); i++)
        {
            for (Range<Token> token : tokens)
            {
                expectedToken.addAll(split(token, numberOfSplits));
            }
        }

        Iterator<KeyspaceRepairAssignments> keyspaceAssignments =
        new FixedSplitTokenRangeSplitter(repairType, Collections.singletonMap(NUMBER_OF_SUBRANGES, Integer.toString(numberOfSubRanges)))
        .getRepairAssignments(config.getRepairPrimaryTokenRangeOnly(repairType), getPlan(repairType));

        // should be only 1 entry for the keyspace.
        assertTrue(keyspaceAssignments.hasNext());
        KeyspaceRepairAssignments keyspaceRepairAssignment = keyspaceAssignments.next();
        assertFalse(keyspaceAssignments.hasNext());

        List<RepairAssignment> assignments = keyspaceRepairAssignment.getRepairAssignments();
        assertEquals(numTokens * numberOfSplits * tables.size(), assignments.size());
        assertEquals(expectedToken.size(), assignments.size());

        int assignmentsPerTable = numTokens * numberOfSplits;
        for (int i = 0; i < tables.size(); i++)
        {
            List<RepairAssignment> assignmentForATable = new ArrayList<>();
            List<Range<Token>> expectedTokensForATable = new ArrayList<>();
            for (int j = 0; j < assignmentsPerTable; j++)
            {
                long expectedBytes = ksTablesEstimatedBytes.get(AutoRepairUtils.getKeyspaceTableName(KEYSPACE, tables.get(i))).values().stream().mapToLong(sizeEstimate -> sizeEstimate.sizeForRepair).sum() / numberOfSplits;
                int theTableAssignmentIdx = i * assignmentsPerTable + j;
                assertEquals(expectedBytes, assignments.get(theTableAssignmentIdx).estimatedBytes);
                assertEquals(Collections.singletonList(tables.get(i)), assignments.get(theTableAssignmentIdx).getTableNames());
                assignmentForATable.add(assignments.get(theTableAssignmentIdx));
                expectedTokensForATable.add(expectedToken.get(theTableAssignmentIdx));
            }
            compare(numTokens, numberOfSplits, expectedTokensForATable, assignmentForATable);
        }
    }

    public static void testTokenRangesSplitByKeyspace(int numTokens, int numberOfSubRanges, AutoRepairConfig.RepairType repairType)
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setRepairByKeyspace(repairType, true);
        Pair<Collection<Range<Token>>, Integer> tokensAndWrappedRanges = getTokenRangesAndTotalWrapAroundCount();
        Collection<Range<Token>> tokens = tokensAndWrappedRanges.left();
        // For the test case, the tokens are allocated dynamically, so we do not know which token-ranges wrap around.
        // As a result, we need to adjust the token count on a need basis.
        numTokens += tokensAndWrappedRanges.right();
        assertEquals(numTokens, tokens.size());
        int numberOfSplits = Math.max(1, numberOfSubRanges / tokens.size());
        List<Range<Token>> expectedToken = new ArrayList<>();
        for (Range<Token> range : tokens)
        {
            expectedToken.addAll(AutoRepairUtils.split(range, numberOfSplits));
        }

        Iterator<KeyspaceRepairAssignments> keyspaceAssignments =
        new FixedSplitTokenRangeSplitter(repairType, Collections.singletonMap(NUMBER_OF_SUBRANGES, Integer.toString(numberOfSubRanges)))
        .getRepairAssignments(config.getRepairPrimaryTokenRangeOnly(repairType), getPlan(repairType));

        // should be only 1 entry for the keyspace.
        assertTrue(keyspaceAssignments.hasNext());
        KeyspaceRepairAssignments keyspace = keyspaceAssignments.next();
        assertFalse(keyspaceAssignments.hasNext());

        List<RepairAssignment> assignments = keyspace.getRepairAssignments();
        assertNotNull(assignments);

        assertEquals(numTokens * numberOfSplits, assignments.size());
        assertEquals(expectedToken.size(), assignments.size());

        compare(numTokens, numberOfSplits, expectedToken, assignments);

        for (int i = 0; i < assignments.size(); i++)
        {
            assertEquals(assignments.get(i).estimatedBytes,
                         ksTablesEstimatedBytes.values().stream()
                                               .flatMap(tableMap -> tableMap.values().stream())
                                               .mapToLong(sizeEstimate -> sizeEstimate.sizeForRepair)
                                               .sum() / numberOfSplits);
        }
    }

    public static void testTokenRangesWithDefaultSplit(int numTokens, AutoRepairConfig.RepairType repairType)
    {
        testTokenRangesSplitByKeyspace(numTokens, DEFAULT_NUMBER_OF_SUBRANGES, repairType);
    }

    private static void compare(int numTokens, int numberOfSplits, List<Range<Token>> expectedToken, List<RepairAssignment> assignments)
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

    private static Pair<Collection<Range<Token>>, Integer> getTokenRangesAndTotalWrapAroundCount()
    {
        int wrappedRanges = 0;
        Collection<Range<Token>> ranges = TokenRingUtils.getPrimaryRangesForEndpoint(KEYSPACE, FBUtilities.getBroadcastAddressAndPort());
        Collection<Range<Token>> tokens = new ArrayList<>();
        for (Range<Token> wrappedRange : ranges)
        {
            if (wrappedRange.isWrapAround())
            {
                wrappedRanges++;
            }
            tokens.addAll(wrappedRange.unwrap());
        }
        return Pair.create(tokens, wrappedRanges);
    }

    private static List<PrioritizedRepairPlan> getPlan(AutoRepairConfig.RepairType repairType)
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        List<PrioritizedRepairPlan> plan = PrioritizedRepairPlan.build(new HashMap<>()
                                                                       {{
                                                                           put(KEYSPACE, tables);
                                                                       }}, repairType, (l) -> {
                                                                       },
                                                                       config.getRepairPrimaryTokenRangeOnly(repairType));
        assertEquals(1, plan.size());
        assertEquals(1, plan.get(0).getKeyspaceRepairPlans().size());
        plan.get(0).getKeyspaceRepairPlans().get(0).ksTablesEstimatedBytes = ksTablesEstimatedBytes;
        return plan;
    }
}
