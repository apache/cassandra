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
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
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
    public void testGetLastRepairTime()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType, new AutoRepairConfig());
        state.lastRepairTimeInMs = 1;

        assertEquals(1, state.getLastRepairTime());
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

        state.setLastRepairTime(1);

        assertEquals(1, state.lastRepairTimeInMs);
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
