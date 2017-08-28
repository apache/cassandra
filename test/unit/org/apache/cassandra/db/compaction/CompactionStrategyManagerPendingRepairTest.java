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

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Tests CompactionStrategyManager's handling of pending repair sstables
 */
public class CompactionStrategyManagerPendingRepairTest extends AbstractPendingRepairTest
{

    private static boolean strategiesContain(Collection<AbstractCompactionStrategy> strategies, SSTableReader sstable)
    {
        return Iterables.any(strategies, strategy -> strategy.getSSTables().contains(sstable));
    }

    private boolean pendingContains(UUID id, SSTableReader sstable)
    {
        return Iterables.any(csm.getPendingRepairManagers(), p -> p.get(id) != null && p.get(id).getSSTables().contains(sstable));
    }

    private boolean pendingContains(SSTableReader sstable)
    {
        return Iterables.any(csm.getPendingRepairManagers(), p -> strategiesContain(p.getStrategies(), sstable));
    }

    private boolean repairedContains(SSTableReader sstable)
    {
        return strategiesContain(csm.getRepaired(), sstable);
    }

    private boolean unrepairedContains(SSTableReader sstable)
    {
        return strategiesContain(csm.getUnrepaired(), sstable);
    }

    /**
     * Pending repair strategy should be created when we encounter a new pending id
     */
    @Test
    public void sstableAdded()
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        Assert.assertTrue(csm.pendingRepairs().isEmpty());

        SSTableReader sstable = makeSSTable(true);
        Assert.assertFalse(sstable.isRepaired());
        Assert.assertFalse(sstable.isPendingRepair());

        mutateRepaired(sstable, repairID);
        Assert.assertFalse(sstable.isRepaired());
        Assert.assertTrue(sstable.isPendingRepair());
        csm.getForPendingRepair(repairID).forEach(Assert::assertNull);

        // add the sstable
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        Assert.assertFalse(repairedContains(sstable));
        Assert.assertFalse(unrepairedContains(sstable));
        csm.getForPendingRepair(repairID).forEach(Assert::assertNotNull);
        Assert.assertTrue(pendingContains(repairID, sstable));
    }

    @Test
    public void sstableListChangedAddAndRemove()
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        SSTableReader sstable1 = makeSSTable(true);
        mutateRepaired(sstable1, repairID);

        SSTableReader sstable2 = makeSSTable(true);
        mutateRepaired(sstable2, repairID);

        Assert.assertFalse(repairedContains(sstable1));
        Assert.assertFalse(unrepairedContains(sstable1));
        Assert.assertFalse(repairedContains(sstable2));
        Assert.assertFalse(unrepairedContains(sstable2));
        csm.getForPendingRepair(repairID).forEach(Assert::assertNull);

        // add only
        SSTableListChangedNotification notification;
        notification = new SSTableListChangedNotification(Collections.singleton(sstable1),
                                                          Collections.emptyList(),
                                                          OperationType.COMPACTION);
        csm.handleNotification(notification, cfs.getTracker());

        csm.getForPendingRepair(repairID).forEach(Assert::assertNotNull);
        Assert.assertFalse(repairedContains(sstable1));
        Assert.assertFalse(unrepairedContains(sstable1));
        Assert.assertTrue(pendingContains(repairID, sstable1));
        Assert.assertFalse(repairedContains(sstable2));
        Assert.assertFalse(unrepairedContains(sstable2));
        Assert.assertFalse(pendingContains(repairID, sstable2));

        // remove and add
        notification = new SSTableListChangedNotification(Collections.singleton(sstable2),
                                                          Collections.singleton(sstable1),
                                                          OperationType.COMPACTION);
        csm.handleNotification(notification, cfs.getTracker());

        Assert.assertFalse(repairedContains(sstable1));
        Assert.assertFalse(unrepairedContains(sstable1));
        Assert.assertFalse(pendingContains(repairID, sstable1));
        Assert.assertFalse(repairedContains(sstable2));
        Assert.assertFalse(unrepairedContains(sstable2));
        Assert.assertTrue(pendingContains(repairID, sstable2));
    }

    @Test
    public void sstableRepairStatusChanged()
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        // add as unrepaired
        SSTableReader sstable = makeSSTable(false);
        Assert.assertTrue(unrepairedContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
        csm.getForPendingRepair(repairID).forEach(Assert::assertNull);

        SSTableRepairStatusChanged notification;

        // change to pending repaired
        mutateRepaired(sstable, repairID);
        notification = new SSTableRepairStatusChanged(Collections.singleton(sstable));
        csm.handleNotification(notification, cfs.getTracker());
        Assert.assertFalse(unrepairedContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
        csm.getForPendingRepair(repairID).forEach(Assert::assertNotNull);
        Assert.assertTrue(pendingContains(repairID, sstable));

        // change to repaired
        mutateRepaired(sstable, System.currentTimeMillis());
        notification = new SSTableRepairStatusChanged(Collections.singleton(sstable));
        csm.handleNotification(notification, cfs.getTracker());
        Assert.assertFalse(unrepairedContains(sstable));
        Assert.assertTrue(repairedContains(sstable));
        Assert.assertFalse(pendingContains(repairID, sstable));
    }

    @Test
    public void sstableDeleted()
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        Assert.assertTrue(pendingContains(repairID, sstable));

        // delete sstable
        SSTableDeletingNotification notification = new SSTableDeletingNotification(sstable);
        csm.handleNotification(notification, cfs.getTracker());
        Assert.assertFalse(pendingContains(repairID, sstable));
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
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        List<List<AbstractCompactionStrategy>> strategies;

        strategies = csm.getStrategies();
        Assert.assertEquals(3, strategies.size());
        Assert.assertTrue(strategies.get(2).isEmpty());

        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
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
    public void cleanupCompactionFinalized()
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        LocalSessionAccessor.finalizeUnsafe(repairID);
        csm.getForPendingRepair(repairID).forEach(Assert::assertNotNull);
        Assert.assertNotNull(pendingContains(repairID, sstable));
        Assert.assertTrue(sstable.isPendingRepair());
        Assert.assertFalse(sstable.isRepaired());

        cfs.getCompactionStrategyManager().enable(); // enable compaction to fetch next background task
        AbstractCompactionTask compactionTask = csm.getNextBackgroundTask(FBUtilities.nowInSeconds());
        Assert.assertNotNull(compactionTask);
        Assert.assertSame(PendingRepairManager.RepairFinishedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute(null);

        Assert.assertTrue(repairedContains(sstable));
        Assert.assertFalse(unrepairedContains(sstable));
        csm.getForPendingRepair(repairID).forEach(Assert::assertNull);

        // sstable should have pendingRepair cleared, and repairedAt set correctly
        long expectedRepairedAt = ActiveRepairService.instance.getParentRepairSession(repairID).repairedAt;
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
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        LocalSessionAccessor.failUnsafe(repairID);

        csm.getForPendingRepair(repairID).forEach(Assert::assertNotNull);
        Assert.assertNotNull(pendingContains(repairID, sstable));
        Assert.assertTrue(sstable.isPendingRepair());
        Assert.assertFalse(sstable.isRepaired());

        cfs.getCompactionStrategyManager().enable(); // enable compaction to fetch next background task
        AbstractCompactionTask compactionTask = csm.getNextBackgroundTask(FBUtilities.nowInSeconds());
        Assert.assertNotNull(compactionTask);
        Assert.assertSame(PendingRepairManager.RepairFinishedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute(null);

        Assert.assertFalse(repairedContains(sstable));
        Assert.assertTrue(unrepairedContains(sstable));
        csm.getForPendingRepair(repairID).forEach(Assert::assertNull);

        // sstable should have pendingRepair cleared, and repairedAt set correctly
        Assert.assertFalse(sstable.isPendingRepair());
        Assert.assertFalse(sstable.isRepaired());
        Assert.assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, sstable.getSSTableMetadata().repairedAt);
    }
}
