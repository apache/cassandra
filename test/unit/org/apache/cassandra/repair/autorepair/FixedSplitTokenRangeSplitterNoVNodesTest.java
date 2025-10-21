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
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Unit tests for a setup that does not have v-nodes {@link FixedSplitTokenRangeSplitter}
 */
@RunWith(Parameterized.class)
public class FixedSplitTokenRangeSplitterNoVNodesTest
{
    private static final int numTokens = 1;

    @Parameterized.Parameter(0)
    public AutoRepairConfig.RepairType repairType;

    @Parameterized.Parameter(1)
    public int numberOfSubRanges;

    @Parameterized.Parameters(name = "repairType={0}, numberOfSubRanges={1}")
    public static Collection<Object[]> parameters()
    {
        List<Object[]> params = new ArrayList<>();
        for (AutoRepairConfig.RepairType type : AutoRepairConfig.RepairType.values())
        {
            for (int subRange : Arrays.asList(1, 2, 4, 8, 16, 32, 64, 128, 256))
            {
                params.add(new Object[]{ type, subRange });
            }
        }
        return params;
    }

    @BeforeClass
    public static void setupClass() throws Exception
    {
        FixedSplitTokenRangeSplitterHelper.setupClass(numTokens);
    }

    @Test
    public void testTokenRangesSplitByTable()
    {
        FixedSplitTokenRangeSplitterHelper.testTokenRangesSplitByTable(numTokens, numberOfSubRanges, repairType);
    }

    @Test
    public void testTokenRangesSplitByKeyspace()
    {
        FixedSplitTokenRangeSplitterHelper.testTokenRangesSplitByKeyspace(numTokens, numberOfSubRanges, repairType);
    }

    @Test
    public void testTokenRangesWithDefaultSplit()
    {
        FixedSplitTokenRangeSplitterHelper.testTokenRangesWithDefaultSplit(numTokens, repairType);
    }
}
