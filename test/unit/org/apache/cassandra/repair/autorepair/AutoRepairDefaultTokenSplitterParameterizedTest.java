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

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.autorepair.IAutoRepairTokenRangeSplitter.RepairAssignment;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.Util.setAutoRepairEnabled;
import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_DEFAULT_RF;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class AutoRepairDefaultTokenSplitterParameterizedTest extends CQLTester
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE1 = "tbl1";
    private static final String TABLE2 = "tbl2";
    private static final String TABLE3 = "tbl3";

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
        DatabaseDescriptor.setInitialTokens("0,256,1024");
        requireNetwork();
        AutoRepairUtils.setup();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1));
    }

    private static void appendExpectedTokens(long left, long right, int numberOfSplits, List<Range<Token>> expectedToken)
    {
        long repairTokenWidth = (right - left) / numberOfSplits;
        for (int i = 0; i < numberOfSplits; i++)
        {
            long curLeft = left + (i * repairTokenWidth);
            long curRight = curLeft + repairTokenWidth;
            if ((i + 1) == numberOfSplits)
            {
                curRight = right;
            }
            Token childStartToken = StorageService.instance.getTokenMetadata()
                                    .partitioner.getTokenFactory().fromString("" + curLeft);
            Token childEndToken = StorageService.instance.getTokenMetadata()
                                  .partitioner.getTokenFactory().fromString("" + curRight);
            expectedToken.add(new Range<>(childStartToken, childEndToken));
        }
    }

    @Test
    public void testTokenRangesSplitByTable()
    {
        AutoRepairService.instance.getAutoRepairConfig().setRepairByKeyspace(repairType, false);
        int totalTokenRanges = 3;
        Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRanges(KEYSPACE);
        assertEquals(totalTokenRanges, tokens.size());
        int numberOfSplits = 4;
        List<String> tables = Arrays.asList(TABLE1, TABLE2, TABLE3);
        List<Range<Token>> expectedToken = new ArrayList<>();
        for (int i = 0; i<tables.size(); i++)
        {
            appendExpectedTokens(1024, 0, numberOfSplits, expectedToken);
            appendExpectedTokens(0, 256, numberOfSplits, expectedToken);
            appendExpectedTokens(256, 1024, numberOfSplits, expectedToken);
        }

        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setRepairSubRangeNum(repairType, numberOfSplits);
        List<RepairAssignment> assignments = new DefaultAutoRepairTokenSplitter().getRepairAssignments(repairType, true, KEYSPACE, tables);
        assertEquals(totalTokenRanges*numberOfSplits*tables.size(), assignments.size());
        assertEquals(expectedToken.size(), assignments.size());

        int expectedTableIndex = -1;
        for (int i = 0; i<totalTokenRanges*numberOfSplits*tables.size(); i++)
        {
            if (i % (totalTokenRanges*numberOfSplits) == 0)
            {
                expectedTableIndex++;
            }
            assertEquals(expectedToken.get(i), assignments.get(i).getTokenRange());
            assertEquals(Collections.singletonList(tables.get(expectedTableIndex)), assignments.get(i).getTableNames());
        }
    }

    @Test
    public void testTokenRangesSplitByKeyspace()
    {
        AutoRepairService.instance.getAutoRepairConfig().setRepairByKeyspace(repairType, true);
        int totalTokenRanges = 3;
        Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRanges(KEYSPACE);
        assertEquals(totalTokenRanges, tokens.size());
        int numberOfSplits = 4;
        List<String> tables = Arrays.asList(TABLE1, TABLE2, TABLE3);
        List<Range<Token>> expectedToken = new ArrayList<>();
        appendExpectedTokens(1024, 0, numberOfSplits, expectedToken);
        appendExpectedTokens(0, 256, numberOfSplits, expectedToken);
        appendExpectedTokens(256, 1024, numberOfSplits, expectedToken);

        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        config.setRepairSubRangeNum(repairType, numberOfSplits);
        List<RepairAssignment> assignments = new DefaultAutoRepairTokenSplitter().getRepairAssignments(repairType, true, KEYSPACE, tables);
        assertEquals(totalTokenRanges*numberOfSplits, assignments.size());
        assertEquals(expectedToken.size(), assignments.size());

        for (int i = 0; i<totalTokenRanges*numberOfSplits; i++)
        {
            assertEquals(expectedToken.get(i), assignments.get(i).getTokenRange());
            assertEquals(tables, assignments.get(i).getTableNames());
        }
    }
}
