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

package org.apache.cassandra.repair.unifiedrepair;

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

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.repair.unifiedrepair.UnifiedRepairConfig.RepairType;
import org.apache.cassandra.repair.unifiedrepair.UnifiedRepairUtils.UnifiedRepairHistory;
import org.apache.cassandra.service.UnifiedRepairService;
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
public class UnifiedRepairStateTest extends CQLTester
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
        UnifiedRepair.SLEEP_IF_REPAIR_FINISHES_QUICKLY = new DurationSpec.IntSecondsBound("0s");
        initMocks(this);
        createTable(String.format("CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", KEYSPACE, testTable));
    }

    @Test
    public void testGetRepairRunnable() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        UnifiedRepairService.setup();

        Runnable runnable = state.getRepairRunnable(KEYSPACE, ImmutableList.of(testTable), ImmutableSet.of(), false);

        assertNotNull(runnable);
    }

    @Test
    public void testProgressError() throws InterruptedException
    {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        when(progressEvent.getType()).thenReturn(ProgressEventType.ERROR);

        state.progress("test", progressEvent);

        assertFalse(state.success);
        assertTrue(state.condition.await(0, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testProgress_progress() throws InterruptedException
    {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        when(progressEvent.getType()).thenReturn(ProgressEventType.PROGRESS);

        state.progress("test", progressEvent);

        assertTrue(state.success);
        assertFalse(state.condition.await(0, TimeUnit.MILLISECONDS));
    }


    @Test
    public void testProgress_complete() throws InterruptedException
    {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        when(progressEvent.getType()).thenReturn(ProgressEventType.COMPLETE);

        state.progress("test", progressEvent);

        assertTrue(state.success);
        assertTrue(state.condition.await(1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testWaitForRepairToComplete() throws Exception
    {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        state.condition.signalAll();
        Condition finishedCondition = Condition.newOneTimeCondition();
        Callable<Void> waitForRepairToComplete = () -> {
            state.waitForRepairToComplete(new DurationSpec.IntSecondsBound("12h"));
            finishedCondition.signalAll();
            return null;
        };

        waitForRepairToComplete.call();

        assertTrue(finishedCondition.await(1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testGetLastRepairTime() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        state.lastRepairTimeInMs = 1;

        assertEquals(1, state.getLastRepairTime());
    }

    @Test
    public void testSetTotalTablesConsideredForRepair() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);

        state.setTotalTablesConsideredForRepair(1);

        assertEquals(1, state.totalTablesConsideredForRepair);
    }

    @Test
    public void testGetTotalTablesConsideredForRepair() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        state.totalTablesConsideredForRepair = 1;

        assertEquals(1, state.getTotalTablesConsideredForRepair());
    }

    @Test
    public void testSetLastRepairTimeInMs() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);

        state.setLastRepairTime(1);

        assertEquals(1, state.lastRepairTimeInMs);
    }

    @Test
    public void testGetClusterRepairTimeInSec() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        state.clusterRepairTimeInSec = 1;

        assertEquals(1, state.getClusterRepairTimeInSec());
    }

    @Test
    public void testGetNodeRepairTimeInSec() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        state.nodeRepairTimeInSec = 1;

        assertEquals(1, state.getNodeRepairTimeInSec());
    }

    @Test
    public void testSetRepairInProgress() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);

        state.setRepairInProgress(true);

        assertTrue(state.repairInProgress);
    }

    @Test
    public void testIsRepairInProgress() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        state.repairInProgress = true;

        assertTrue(state.isRepairInProgress());
    }

    @Test
    public void testSetSkippedTokenRangesCount() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);

        state.setSkippedTokenRangesCount(1);

        assertEquals(1, state.skippedTokenRangesCount);
    }

    @Test
    public void testGetSkippedTokenRangesCount() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        state.skippedTokenRangesCount = 1;

        assertEquals(1, state.getSkippedTokenRangesCount());
    }

    @Test
    public void testGetLongestUnrepairedSecNull() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
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
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        state.longestUnrepairedNode = new UnifiedRepairHistory(UUID.randomUUID(), "", 0, 1000,
                                                               null, 0, false);
        UnifiedRepairState.timeFunc = () -> 2000L;

        try
        {
            assertEquals(1, state.getLongestUnrepairedSec());
        } catch (Exception e) {
            assertNull(e);
        }
    }

    @Test
    public void testSetTotalMVTablesConsideredForRepair() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);

        state.setTotalMVTablesConsideredForRepair(1);

        assertEquals(1, state.totalMVTablesConsideredForRepair);
    }

    @Test
    public void testGetTotalMVTablesConsideredForRepair() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        state.totalMVTablesConsideredForRepair = 1;

        assertEquals(1, state.getTotalMVTablesConsideredForRepair());
    }

    @Test
    public void testSetNodeRepairTimeInSec() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);

        state.setNodeRepairTimeInSec(1);

        assertEquals(1, state.nodeRepairTimeInSec);
    }

    @Test
    public void testSetClusterRepairTimeInSec() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);

        state.setClusterRepairTimeInSec(1);

        assertEquals(1, state.clusterRepairTimeInSec);
    }

    @Test
    public void testSetRepairKeyspaceCount() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);

        state.setRepairKeyspaceCount(1);

        assertEquals(1, state.repairKeyspaceCount);
    }
    @Test
    public void testGetRepairKeyspaceCount() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        state.repairKeyspaceCount = 1;

        assertEquals(1, state.getRepairKeyspaceCount());
    }

    @Test
    public void testSetLongestUnrepairedNode() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        UnifiedRepairHistory history = new UnifiedRepairHistory(UUID.randomUUID(), "", 0, 0, null, 0, false);

        state.setLongestUnrepairedNode(history);

        assertEquals(history, state.longestUnrepairedNode);
    }

    @Test
    public void testSetSucceededTokenRangesCount() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);

        state.setSucceededTokenRangesCount(1);

        assertEquals(1, state.succeededTokenRangesCount);
    }

    @Test
    public void testGetSucceededTokenRangesCount() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        state.succeededTokenRangesCount = 1;

        assertEquals(1, state.getSucceededTokenRangesCount());
    }

    @Test
    public void testSetFailedTokenRangesCount() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);

        state.setFailedTokenRangesCount(1);

        assertEquals(1, state.failedTokenRangesCount);
    }

    @Test
    public void testGetFailedTokenRangesCount() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        state.failedTokenRangesCount = 1;

        assertEquals(1, state.getFailedTokenRangesCount());
    }

    @Test
    public void isSuccess() {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        state.success = true;

        assertTrue(state.isSuccess());

        state.success = false;

        assertFalse(state.isSuccess());
    }

    @Test
    public void testWaitForRepairToCompleteDoesNotSetSuccessWhenProgressReceivesError() throws InterruptedException
    {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        when(progressEvent.getType()).thenReturn(ProgressEventType.ERROR);

        state.progress("test", progressEvent);
        assertFalse(state.success);

        state.waitForRepairToComplete(new DurationSpec.IntSecondsBound("12h"));
        assertFalse(state.success);
    }

    @Test
    public void testResetWaitCondition()
    {
        UnifiedRepairState state = RepairType.getUnifiedRepairState(repairType);
        state.condition.signalAll();
        assertTrue(state.condition.isSignalled());

        state.resetWaitCondition();

        assertFalse(state.condition.isSignalled());
    }
}
