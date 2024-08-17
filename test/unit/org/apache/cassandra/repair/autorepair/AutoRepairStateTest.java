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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils.AutoRepairHistory;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(Parameterized.class)
public class AutoRepairStateTest extends CQLTester
{
    private static final String testTable = "test";

    @Parameterized.Parameter
    public RepairType repairType;

    @Mock
    ProgressEvent progressEvent;

    @Parameterized.Parameters
    public static Collection<RepairType> repairTypes()
    {
        return Arrays.asList(RepairType.values());
    }

    @Before
    public void setUp() {
        initMocks(this);
        createTable(String.format("CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", KEYSPACE, testTable));
    }

    @Test
    public void testGetRepairRunnable() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        AutoRepairService.setup();

        Runnable runnable = state.getRepairRunnable(KEYSPACE, ImmutableList.of(testTable), ImmutableSet.of(), false);

        assertNotNull(runnable);
    }

    @Test
    public void testProgressError() throws InterruptedException
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        when(progressEvent.getType()).thenReturn(ProgressEventType.ERROR);

        state.progress("test", progressEvent);

        assertFalse(state.success);
        assertTrue(state.condition.await(0, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testProgress_progress() throws InterruptedException
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        when(progressEvent.getType()).thenReturn(ProgressEventType.PROGRESS);

        state.progress("test", progressEvent);

        assertTrue(state.success);
        assertFalse(state.condition.await(0, TimeUnit.MILLISECONDS));
    }


    @Test
    public void testProgress_complete() throws InterruptedException
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        when(progressEvent.getType()).thenReturn(ProgressEventType.COMPLETE);

        state.progress("test", progressEvent);

        assertTrue(state.success);
        assertTrue(state.condition.await(1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testWaitForRepairToComplete() throws Exception
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.condition.signalAll();
        Condition finishedCondition = Condition.newOneTimeCondition();
        Callable<Void> waitForRepairToComplete = () -> {
            state.waitForRepairToComplete();
            finishedCondition.signalAll();
            return null;
        };

        waitForRepairToComplete.call();

        assertTrue(finishedCondition.await(1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testGetLastRepairTime() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.lastRepairTimeInMs = 1;

        assertEquals(1, state.getLastRepairTime());
    }

    @Test
    public void testSetTotalTablesConsideredForRepair() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);

        state.setTotalTablesConsideredForRepair(1);

        assertEquals(1, state.totalTablesConsideredForRepair);
    }

    @Test
    public void testGetTotalTablesConsideredForRepair() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.totalTablesConsideredForRepair = 1;

        assertEquals(1, state.getTotalTablesConsideredForRepair());
    }

    @Test
    public void testSetLastRepairTimeInMs() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);

        state.setLastRepairTime(1);

        assertEquals(1, state.lastRepairTimeInMs);
    }

    @Test
    public void testGetClusterRepairTimeInSec() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.clusterRepairTimeInSec = 1;

        assertEquals(1, state.getClusterRepairTimeInSec());
    }

    @Test
    public void testGetNodeRepairTimeInSec() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.nodeRepairTimeInSec = 1;

        assertEquals(1, state.getNodeRepairTimeInSec());
    }

    @Test
    public void testSetRepairInProgress() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);

        state.setRepairInProgress(true);

        assertTrue(state.repairInProgress);
    }

    @Test
    public void testIsRepairInProgress() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.repairInProgress = true;

        assertTrue(state.isRepairInProgress());
    }

    @Test
    public void testSetRepairSkippedTablesCount() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);

        state.setRepairSkippedTablesCount(1);

        assertEquals(1, state.repairTableSkipCount);
    }

    @Test
    public void testGetRepairSkippedTablesCount() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.repairTableSkipCount = 1;

        assertEquals(1, state.getRepairSkippedTablesCount());
    }

    @Test
    public void testGetLongestUnrepairedSecNull() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.longestUnrepairedNode = null;

        try
        {
            assertEquals(0, state.getLongestUnrepairedSec());
        } catch (Exception e) {
            assertNull(e);
        }
    }

    @Test
    public void testGetLongestUnrepairedSec() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.longestUnrepairedNode = new AutoRepairHistory(UUID.randomUUID(), "", 0, 1000,
                                                            null, 0, false);
        AutoRepairState.timeFunc = () -> 2000L;

        try
        {
            assertEquals(1, state.getLongestUnrepairedSec());
        } catch (Exception e) {
            assertNull(e);
        }
    }

    @Test
    public void testSetTotalMVTablesConsideredForRepair() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);

        state.setTotalMVTablesConsideredForRepair(1);

        assertEquals(1, state.totalMVTablesConsideredForRepair);
    }

    @Test
    public void testGetTotalMVTablesConsideredForRepair() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.totalMVTablesConsideredForRepair = 1;

        assertEquals(1, state.getTotalMVTablesConsideredForRepair());
    }

    @Test
    public void testSetNodeRepairTimeInSec() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);

        state.setNodeRepairTimeInSec(1);

        assertEquals(1, state.nodeRepairTimeInSec);
    }

    @Test
    public void testSetClusterRepairTimeInSec() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);

        state.setClusterRepairTimeInSec(1);

        assertEquals(1, state.clusterRepairTimeInSec);
    }

    @Test
    public void testSetRepairKeyspaceCount() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);

        state.setRepairKeyspaceCount(1);

        assertEquals(1, state.repairKeyspaceCount);
    }
    @Test
    public void testGetRepairKeyspaceCount() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.repairKeyspaceCount = 1;

        assertEquals(1, state.getRepairKeyspaceCount());
    }

    @Test
    public void testSetLongestUnrepairedNode() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        AutoRepairHistory history = new AutoRepairHistory(UUID.randomUUID(), "", 0, 0, null, 0, false);

        state.setLongestUnrepairedNode(history);

        assertEquals(history, state.longestUnrepairedNode);
    }

    @Test
    public void testSetSucceededTokenRangesCount() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);

        state.setSucceededTokenRangesCount(1);

        assertEquals(1, state.succeededTokenRangesCount);
    }

    @Test
    public void testGetSucceededTokenRangesCount() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.succeededTokenRangesCount = 1;

        assertEquals(1, state.getSucceededTokenRangesCount());
    }

    @Test
    public void testSetFailedTokenRangesCount() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);

        state.setFailedTokenRangesCount(1);

        assertEquals(1, state.failedTokenRangesCount);
    }

    @Test
    public void testGetFailedTokenRangesCount() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.failedTokenRangesCount = 1;

        assertEquals(1, state.getFailedTokenRangesCount());
    }

    @Test
    public void isSuccess() {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.success = true;

        assertTrue(state.isSuccess());

        state.success = false;

        assertFalse(state.isSuccess());
    }

    @Test
    public void testWaitForRepairToCompleteDoesNotSetSuccessWhenProgressReceivesError() throws InterruptedException
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        when(progressEvent.getType()).thenReturn(ProgressEventType.ERROR);

        state.progress("test", progressEvent);
        assertFalse(state.success);

        state.waitForRepairToComplete();
        assertFalse(state.success);
    }

    @Test
    public void testResetWaitCondition()
    {
        AutoRepairState state = RepairType.getAutoRepairState(repairType);
        state.condition.signalAll();
        assertTrue(state.condition.isSignalled());

        state.resetWaitCondition();

        assertFalse(state.condition.isSignalled());
    }
}
