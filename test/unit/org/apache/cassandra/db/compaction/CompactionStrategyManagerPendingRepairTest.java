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

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Iterables;
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
