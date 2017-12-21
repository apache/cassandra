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

package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class PendingRepairManagerTest extends AbstractPendingRepairTest
{
    /**
     * If a local session is ongoing, it should not be cleaned up
     */
    @Test
    public void needsCleanupInProgress()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));

        Assert.assertFalse(prm.canCleanup(repairID));
    }

    /**
     * If a local session is finalized, it should be cleaned up
     */
    @Test
    public void needsCleanupFinalized()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Assert.assertTrue(prm.canCleanup(repairID));
    }

    /**
     * If a local session has failed, it should be cleaned up
     */
    @Test
    public void needsCleanupFailed()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));
        LocalSessionAccessor.failUnsafe(repairID);

        Assert.assertTrue(prm.canCleanup(repairID));
    }

    @Test
    public void needsCleanupNoSession()
    {
        UUID fakeID = UUIDGen.getTimeUUID();
        PendingRepairManager prm = new PendingRepairManager(cfs, null);
        Assert.assertTrue(prm.canCleanup(fakeID));
    }

    @Test
    public void estimateRemainingTasksInProgress()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));

        Assert.assertEquals(0, prm.getEstimatedRemainingTasks());
        Assert.assertEquals(0, prm.getNumPendingRepairFinishedTasks());
    }

    @Test
    public void estimateRemainingFinishedRepairTasks()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));
        Assert.assertNotNull(prm.get(repairID));
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Assert.assertEquals(0, prm.getEstimatedRemainingTasks());
        Assert.assertEquals(1, prm.getNumPendingRepairFinishedTasks());
    }

    @Test
    public void getNextBackgroundTask()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);

        repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Assert.assertEquals(2, prm.getSessions().size());
        Assert.assertNull(prm.getNextBackgroundTask(FBUtilities.nowInSeconds()));
        AbstractCompactionTask compactionTask = prm.getNextRepairFinishedTask();
        try
        {
            Assert.assertNotNull(compactionTask);
            Assert.assertSame(PendingRepairManager.RepairFinishedCompactionTask.class, compactionTask.getClass());
            PendingRepairManager.RepairFinishedCompactionTask cleanupTask = (PendingRepairManager.RepairFinishedCompactionTask) compactionTask;
            Assert.assertEquals(repairID, cleanupTask.getSessionID());
        }
        finally
        {
            compactionTask.transaction.abort();
        }
    }

    @Test
    public void getNextBackgroundTaskNoSessions()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);
        Assert.assertNull(prm.getNextBackgroundTask(FBUtilities.nowInSeconds()));
    }

    /**
     * If all sessions should be cleaned up, getNextBackgroundTask should return null
     */
    @Test
    public void getNextBackgroundTaskAllCleanup() throws Exception
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));
        Assert.assertNotNull(prm.get(repairID));
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Assert.assertNull(prm.getNextBackgroundTask(FBUtilities.nowInSeconds()));

    }

    @Test
    public void maximalTaskNeedsCleanup()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));
        Assert.assertNotNull(prm.get(repairID));
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Collection<AbstractCompactionTask> tasks = prm.getMaximalTasks(FBUtilities.nowInSeconds(), false);
        try
        {
            Assert.assertEquals(1, tasks.size());
        }
        finally
        {
            tasks.stream().forEach(t -> t.transaction.abort());
        }
    }

    @Test
    public void userDefinedTaskTest()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);
        UUID repairId = registerSession(cfs, true, true);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairId);
        prm.addSSTable(sstable);
        List<AbstractCompactionTask> tasks = csm.getUserDefinedTasks(Collections.singleton(sstable), 100);
        try
        {
            Assert.assertEquals(1, tasks.size());
        }
        finally
        {
            tasks.stream().forEach(t -> t.transaction.abort());
        }
    }

    @Test
    public void mixedPendingSessionsTest()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);
        UUID repairId = registerSession(cfs, true, true);
        UUID repairId2 = registerSession(cfs, true, true);
        SSTableReader sstable = makeSSTable(true);
        SSTableReader sstable2 = makeSSTable(true);

        mutateRepaired(sstable, repairId);
        mutateRepaired(sstable2, repairId2);
        prm.addSSTable(sstable);
        prm.addSSTable(sstable2);
        List<AbstractCompactionTask> tasks = csm.getUserDefinedTasks(Lists.newArrayList(sstable, sstable2), 100);
        try
        {
            Assert.assertEquals(2, tasks.size());
        }
        finally
        {
            tasks.stream().forEach(t -> t.transaction.abort());
        }
    }

    /**
     * Tests that a IllegalSSTableArgumentException is thrown if we try to get
     * scanners for an sstable that isn't pending repair
     */
    @Test(expected = PendingRepairManager.IllegalSSTableArgumentException.class)
    public void getScannersInvalidSSTable() throws Exception
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);
        SSTableReader sstable = makeSSTable(true);
        prm.getScanners(Collections.singleton(sstable), Collections.singleton(RANGE1));
    }

    /**
     * Tests that a IllegalSSTableArgumentException is thrown if we try to get
     * scanners for an sstable that isn't pending repair
     */
    @Test(expected = PendingRepairManager.IllegalSSTableArgumentException.class)
    public void getOrCreateInvalidSSTable() throws Exception
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);
        SSTableReader sstable = makeSSTable(true);
        prm.getOrCreate(sstable);
    }

    @Test
    public void sessionHasData()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        Assert.assertFalse(prm.hasDataForSession(repairID));
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        Assert.assertTrue(prm.hasDataForSession(repairID));
    }
}
