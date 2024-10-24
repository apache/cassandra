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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.cassandra.Util;
import org.apache.cassandra.repair.consistent.LocalSession;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.repair.NoSuchRepairSessionException;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Tests CompactionStrategyManager's handling of pending repair sstables
 */
public class CompactionStrategyManagerPendingRepairTest extends AbstractPendingRepairTest
{

    private boolean transientContains(SSTableReader sstable)
    {
        return csm.getTransientRepairsUnsafe().containsSSTable(sstable);
    }

    private boolean pendingContains(SSTableReader sstable)
    {
        return csm.getPendingRepairsUnsafe().containsSSTable(sstable);
    }

    private boolean repairedContains(SSTableReader sstable)
    {
        return csm.getRepairedUnsafe().containsSSTable(sstable);
    }

    private boolean unrepairedContains(SSTableReader sstable)
    {
        return csm.getUnrepairedUnsafe().containsSSTable(sstable);
    }

    private boolean hasPendingStrategiesFor(TimeUUID sessionID)
    {
        return !Iterables.isEmpty(csm.getPendingRepairsUnsafe().getStrategiesFor(sessionID));
    }

    private boolean hasTransientStrategiesFor(TimeUUID sessionID)
    {
        return !Iterables.isEmpty(csm.getTransientRepairsUnsafe().getStrategiesFor(sessionID));
    }

    /**
     * Pending repair strategy should be created when we encounter a new pending id
     */
    @Test
    public void sstableAdded()
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        Assert.assertTrue(Iterables.isEmpty(csm.getPendingRepairsUnsafe().allStrategies()));

        SSTableReader sstable = makeSSTable(true);
        Assert.assertFalse(sstable.isRepaired());
        Assert.assertFalse(sstable.isPendingRepair());

        mutateRepaired(sstable, repairID, false);
        Assert.assertFalse(sstable.isRepaired());
        Assert.assertTrue(sstable.isPendingRepair());
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));

        // add the sstable
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        Assert.assertFalse(repairedContains(sstable));
        Assert.assertFalse(unrepairedContains(sstable));
        Assert.assertTrue(pendingContains(sstable));
        Assert.assertTrue(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
    }

    @Test
    public void sstableListChangedAddAndRemove()
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        SSTableReader sstable1 = makeSSTable(true);
        mutateRepaired(sstable1, repairID, false);

        SSTableReader sstable2 = makeSSTable(true);
        mutateRepaired(sstable2, repairID, false);

        Assert.assertFalse(repairedContains(sstable1));
        Assert.assertFalse(unrepairedContains(sstable1));
        Assert.assertFalse(repairedContains(sstable2));
        Assert.assertFalse(unrepairedContains(sstable2));
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));

        // add only
        SSTableListChangedNotification notification;
        notification = new SSTableListChangedNotification(Collections.singleton(sstable1),
                                                          Collections.emptyList(),
                                                          OperationType.COMPACTION);
        csm.handleNotification(notification, cfs.getTracker());

        Assert.assertFalse(repairedContains(sstable1));
        Assert.assertFalse(unrepairedContains(sstable1));
        Assert.assertTrue(pendingContains(sstable1));
        Assert.assertFalse(repairedContains(sstable2));
        Assert.assertFalse(unrepairedContains(sstable2));
        Assert.assertFalse(pendingContains(sstable2));
        Assert.assertTrue(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));

        // remove and add
        notification = new SSTableListChangedNotification(Collections.singleton(sstable2),
                                                          Collections.singleton(sstable1),
                                                          OperationType.COMPACTION);
        csm.handleNotification(notification, cfs.getTracker());

        Assert.assertFalse(repairedContains(sstable1));
        Assert.assertFalse(unrepairedContains(sstable1));
        Assert.assertFalse(pendingContains(sstable1));
        Assert.assertFalse(repairedContains(sstable2));
        Assert.assertFalse(unrepairedContains(sstable2));
        Assert.assertTrue(pendingContains(sstable2));
    }

    @Test
    public void sstableRepairStatusChanged()
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        // add as unrepaired
        SSTableReader sstable = makeSSTable(false);
        Assert.assertTrue(unrepairedContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));

        SSTableRepairStatusChanged notification;

        // change to pending repaired
        mutateRepaired(sstable, repairID, false);
        notification = new SSTableRepairStatusChanged(Collections.singleton(sstable));
        csm.handleNotification(notification, cfs.getTracker());
        Assert.assertFalse(unrepairedContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
        Assert.assertTrue(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
        Assert.assertTrue(pendingContains(sstable));

        // change to repaired
        mutateRepaired(sstable, System.currentTimeMillis());
        notification = new SSTableRepairStatusChanged(Collections.singleton(sstable));
        csm.handleNotification(notification, cfs.getTracker());
        Assert.assertFalse(unrepairedContains(sstable));
        Assert.assertTrue(repairedContains(sstable));
        Assert.assertFalse(pendingContains(sstable));
    }

    @Test
    public void sstableDeleted()
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        Assert.assertTrue(pendingContains(sstable));

        // delete sstable
        SSTableDeletingNotification notification = new SSTableDeletingNotification(sstable);
        csm.handleNotification(notification, cfs.getTracker());
        Assert.assertFalse(pendingContains(sstable));
        Assert.assertFalse(unrepairedContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
    }

    /**
     * CompactionStrategyManager.getStrategies should include
     * pending repair strategies when appropriate
     */
    @Test
    public void getStrategies()
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        List<List<AbstractCompactionStrategy>> strategies;

        strategies = csm.getStrategies();
        Assert.assertEquals(3, strategies.size());
        Assert.assertTrue(strategies.get(2).isEmpty());

        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());

        strategies = csm.getStrategies();
        Assert.assertEquals(3, strategies.size());
        Assert.assertFalse(strategies.get(2).isEmpty());
    }

    /**
     * Tests that finalized repairs result in cleanup compaction tasks
     * which reclassify the sstables as repaired
     */
    @Test
    public void cleanupCompactionFinalized() throws NoSuchRepairSessionException
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        LocalSessionAccessor.finalizeUnsafe(repairID);
        Assert.assertTrue(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
        Assert.assertTrue(pendingContains(sstable));
        Assert.assertTrue(sstable.isPendingRepair());
        Assert.assertFalse(sstable.isRepaired());

        cfs.getCompactionStrategyManager().enable(); // enable compaction to fetch next background task
        AbstractCompactionTask compactionTask = csm.getNextBackgroundTask(FBUtilities.nowInSeconds());
        Assert.assertNotNull(compactionTask);
        Assert.assertSame(PendingRepairManager.RepairFinishedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute(ActiveCompactionsTracker.NOOP);

        Assert.assertTrue(repairedContains(sstable));
        Assert.assertFalse(unrepairedContains(sstable));
        Assert.assertFalse(pendingContains(sstable));
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));

        // sstable should have pendingRepair cleared, and repairedAt set correctly
        long expectedRepairedAt = ActiveRepairService.instance().getParentRepairSession(repairID).repairedAt;
        Assert.assertFalse(sstable.isPendingRepair());
        Assert.assertTrue(sstable.isRepaired());
        Assert.assertEquals(expectedRepairedAt, sstable.getSSTableMetadata().repairedAt);
    }

    /**
     * Tests that failed repairs result in cleanup compaction tasks
     * which reclassify the sstables as unrepaired
     */
    @Test
    public void cleanupCompactionFailed()
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        LocalSessionAccessor.failUnsafe(repairID);

        Assert.assertTrue(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
        Assert.assertTrue(pendingContains(sstable));
        Assert.assertTrue(sstable.isPendingRepair());
        Assert.assertFalse(sstable.isRepaired());

        cfs.getCompactionStrategyManager().enable(); // enable compaction to fetch next background task
        AbstractCompactionTask compactionTask = csm.getNextBackgroundTask(FBUtilities.nowInSeconds());
        Assert.assertNotNull(compactionTask);
        Assert.assertSame(PendingRepairManager.RepairFinishedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute(ActiveCompactionsTracker.NOOP);

        Assert.assertFalse(repairedContains(sstable));
        Assert.assertTrue(unrepairedContains(sstable));
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));

        // sstable should have pendingRepair cleared, and repairedAt set correctly
        Assert.assertFalse(sstable.isPendingRepair());
        Assert.assertFalse(sstable.isRepaired());
        Assert.assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, sstable.getSSTableMetadata().repairedAt);
    }

    /**
     * Tests that finalized repairs racing with compactions on the same set of sstables don't leave unrepaired sstables behind
     *
     * This test checks that when a repair has been finalized but there are still pending sstables a finalize repair
     * compaction task is issued for that repair session.
     */
    @Test
    public void testFinalizedAndCompactionRace() throws NoSuchRepairSessionException
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        int numberOfSStables = 4;
        List<SSTableReader> sstables = new ArrayList<>(numberOfSStables);
        for (int i = 0; i < numberOfSStables; i++)
        {
            SSTableReader sstable = makeSSTable(true);
            sstables.add(sstable);
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertFalse(sstable.isPendingRepair());
        }

        // change to pending repair
        mutateRepaired(sstables, repairID, false);
        csm.handleNotification(new SSTableAddedNotification(sstables, null), cfs.getTracker());
        for (SSTableReader sstable : sstables)
        {
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertTrue(sstable.isPendingRepair());
            Assert.assertEquals(repairID, sstable.getPendingRepair());
        }

        // Get a compaction taks based on the sstables marked as pending repair
        cfs.getCompactionStrategyManager().enable();
        for (SSTableReader sstable : sstables)
            pendingContains(sstable);
        AbstractCompactionTask compactionTask = csm.getNextBackgroundTask(FBUtilities.nowInSeconds());

        // Finalize the repair session
        LocalSessionAccessor.finalizeUnsafe(repairID);
        LocalSession session = ARS.consistent.local.getSession(repairID);
        ARS.consistent.local.sessionCompleted(session);

        // run the compaction
        if (compactionTask != null)
        {
            compactionTask.execute(ActiveCompactionsTracker.NOOP);
            Assert.assertEquals(1, cfs.getLiveSSTables().size());
        }

        // The repair session is finalized but there could be an sstable left behind pending repair!
        SSTableReader compactedSSTable = cfs.getLiveSSTables().iterator().next();

        System.out.println("*********************************************************************************************");
        System.out.println(compactedSSTable);
        System.out.println("Pending repair UUID: " + compactedSSTable.getPendingRepair());
        System.out.println("Repaired at: " + compactedSSTable.getRepairedAt());
        System.out.println("Creation time: " + compactedSSTable.getDataCreationTime());
        System.out.println("Live sstables: " + cfs.getLiveSSTables().size());
        System.out.println("*********************************************************************************************");

        // Run compaction again. It should pick up the pending repair sstable
        compactionTask = csm.getNextBackgroundTask(FBUtilities.nowInSeconds());
        if (compactionTask != null)
        {
            Assert.assertSame(PendingRepairManager.RepairFinishedCompactionTask.class, compactionTask.getClass());
            compactionTask.execute(ActiveCompactionsTracker.NOOP);

            while ((compactionTask = csm.getNextBackgroundTask(FBUtilities.nowInSeconds())) != null)
                compactionTask.execute(ActiveCompactionsTracker.NOOP);
        }

        // Make sure you consume all pending compactions
        Util.spinAssertEquals(Boolean.FALSE,
                              () -> {
                                  AbstractCompactionTask ctask;
                                  while ((ctask = csm.getNextBackgroundTask(FBUtilities.nowInSeconds())) != null)
                                      ctask.execute(ActiveCompactionsTracker.NOOP);

                                  return hasPendingStrategiesFor(repairID);
                              },
                              30);

        System.out.println("*********************************************************************************************");
        System.out.println(compactedSSTable);
        System.out.println("Pending repair UUID: " + compactedSSTable.getPendingRepair());
        System.out.println("Repaired at: " + compactedSSTable.getRepairedAt());
        System.out.println("Creation time: " + compactedSSTable.getDataCreationTime());
        System.out.println("Live sstables: " + cfs.getLiveSSTables().size());
        System.out.println("*********************************************************************************************");

        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
        Assert.assertTrue(repairedContains(compactedSSTable));
        Assert.assertFalse(unrepairedContains(compactedSSTable));
        Assert.assertFalse(pendingContains(compactedSSTable));
        // sstable should have pendingRepair cleared, and repairedAt set correctly
        long expectedRepairedAt = ActiveRepairService.instance().getParentRepairSession(repairID).repairedAt;
        Assert.assertFalse(compactedSSTable.isPendingRepair());
        Assert.assertTrue(compactedSSTable.isRepaired());
        Assert.assertEquals(expectedRepairedAt, compactedSSTable.getSSTableMetadata().repairedAt);
    }

    @Test
    public void finalizedSessionTransientCleanup()
    {
        Assert.assertTrue(cfs.getLiveSSTables().isEmpty());
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, true);
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertTrue(hasTransientStrategiesFor(repairID));
        Assert.assertTrue(transientContains(sstable));
        Assert.assertFalse(pendingContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
        Assert.assertFalse(unrepairedContains(sstable));

        cfs.getCompactionStrategyManager().enable(); // enable compaction to fetch next background task
        AbstractCompactionTask compactionTask = csm.getNextBackgroundTask(FBUtilities.nowInSeconds());
        Assert.assertNotNull(compactionTask);
        Assert.assertSame(PendingRepairManager.RepairFinishedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute(ActiveCompactionsTracker.NOOP);

        Assert.assertTrue(cfs.getLiveSSTables().isEmpty());
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
    }

    @Test
    public void failedSessionTransientCleanup()
    {
        Assert.assertTrue(cfs.getLiveSSTables().isEmpty());
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, true);
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        LocalSessionAccessor.failUnsafe(repairID);

        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertTrue(hasTransientStrategiesFor(repairID));
        Assert.assertTrue(transientContains(sstable));
        Assert.assertFalse(pendingContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
        Assert.assertFalse(unrepairedContains(sstable));

        cfs.getCompactionStrategyManager().enable(); // enable compaction to fetch next background task
        AbstractCompactionTask compactionTask = csm.getNextBackgroundTask(FBUtilities.nowInSeconds());
        Assert.assertNotNull(compactionTask);
        Assert.assertSame(PendingRepairManager.RepairFinishedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute(ActiveCompactionsTracker.NOOP);

        Assert.assertFalse(cfs.getLiveSSTables().isEmpty());
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
        Assert.assertFalse(transientContains(sstable));
        Assert.assertFalse(pendingContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
        Assert.assertTrue(unrepairedContains(sstable));
    }
}
