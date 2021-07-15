/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy.Shard;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.consistent.LocalSession;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.repair.consistent.LocalSessions;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests UnifiedCompactionContainer's handling of pending repair sstables
 */
public class UnifiedCompactionContainerPendingRepairTest extends AbstractPendingRepairTest implements CompactionStrategyContainerPendingRepairTest
{
    @Override
    public String createTableCql()
    {
        return String.format("CREATE TABLE %s.%s (k INT PRIMARY KEY, v INT) " +
                             "WITH COMPACTION={'class': 'UnifiedCompactionStrategy'} ",
                             ks, tbl);
    }

    @Override
    void handleOrphan(SSTableReader sstable)
    {
        // UCS is stateless, so nothing to do
    }

    @Override
    @Test
    public void testSstableAdded() throws IOException
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        SSTableReader sstable = makeSSTable(true);
        Assert.assertFalse(sstable.isRepaired());
        Assert.assertFalse(sstable.isPendingRepair());
        assertShardContainsSstable(sstable, false, false, false,null,true, true);

        cfs.mutateRepaired(ImmutableList.of(sstable), 0, repairID, false);

        Assert.assertFalse(sstable.isRepaired());
        assertTrue(sstable.isPendingRepair());
        assertEquals(repairID, sstable.getPendingRepair());
        assertShardContainsSstable(sstable, false, true, false, repairID, true,true);
    }

    @Override
    @Test
    public void testSstableDeleted() throws IOException
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        final boolean isOrphan = true;
        SSTableReader sstable = makeSSTable(isOrphan);
        Assert.assertFalse(sstable.isRepaired());
        Assert.assertFalse(sstable.isPendingRepair());
        assertShardContainsSstable(sstable, false, false, false, null,true, true);

        cfs.mutateRepaired(ImmutableList.of(sstable), 0, repairID, false);

        Assert.assertFalse(sstable.isRepaired());
        Assert.assertTrue(sstable.isPendingRepair());
        assertEquals(repairID, sstable.getPendingRepair());
        assertShardContainsSstable(sstable, false, true, false, repairID, true,true);

        // delete sstable
        cfs.markObsolete(Collections.singletonList(sstable), OperationType.UNKNOWN);

        assertShardContainsSstable(sstable, false, true, false, repairID, false,false);
    }

    @Override
    @Test
    public void testSstableListChangedAddAndRemove() throws IOException
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        final boolean isOrphan = true;
        SSTableReader sstable1 = makeSSTable(isOrphan);
        Assert.assertFalse(sstable1.isRepaired());
        Assert.assertFalse(sstable1.isPendingRepair());
        assertShardContainsSstable(sstable1, false, false, false, null,true, true);

        SSTableReader sstable2 = makeSSTable(isOrphan);
        Assert.assertFalse(sstable2.isRepaired());
        Assert.assertFalse(sstable2.isPendingRepair());
        assertShardContainsSstable(sstable2, false, false, false, null,true, true);

        cfs.mutateRepaired(ImmutableList.of(sstable1, sstable2), 0, repairID, false);

        Assert.assertFalse(sstable1.isRepaired());
        Assert.assertTrue(sstable1.isPendingRepair());
        assertEquals(repairID, sstable1.getPendingRepair());
        assertShardContainsSstable(sstable1, false, true, false, repairID, true,true);

        Assert.assertFalse(sstable2.isRepaired());
        Assert.assertTrue(sstable2.isPendingRepair());
        assertEquals(repairID, sstable2.getPendingRepair());
        assertShardContainsSstable(sstable2, false, true, false, repairID,true,true);

        // remove sstable1
        cfs.markObsolete(Collections.singletonList(sstable1), OperationType.UNKNOWN);

        assertShardContainsSstable(sstable1, false, true, false, repairID,false,false);
        assertShardContainsSstable(sstable2, false, true, false, repairID,true,true);
    }

    @Override
    @Test
    public void testSstableRepairStatusChanged() throws IOException
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        // add as unrepaired
        final boolean isOrphan = false;
        SSTableReader sstable = makeSSTable(isOrphan);
        Assert.assertFalse(sstable.isRepaired());
        Assert.assertFalse(sstable.isPendingRepair());
        assertShardContainsSstable(sstable, false, false, false, null,true, true);

        // change to pending repair
        cfs.mutateRepaired(Collections.singletonList(sstable), 0, repairID, false);

        Assert.assertFalse(sstable.isRepaired());
        Assert.assertTrue(sstable.isPendingRepair());
        assertEquals(repairID, sstable.getPendingRepair());
        assertShardContainsSstable(sstable, false, true, false, repairID, true,true);

        // change to repaired
        long repairedAt = System.currentTimeMillis();
        cfs.mutateRepaired(Collections.singletonList(sstable), repairedAt, null, false);

        Assert.assertTrue(sstable.isRepaired());
        Assert.assertFalse(sstable.isPendingRepair());
        assertShardContainsSstable(sstable, true, false, false, null,true,true);
    }

    @Override
    @Test
    public void testStrategiesContainsPendingRepair() throws IOException
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        final boolean isOrphan = true;
        SSTableReader sstable = makeSSTable(isOrphan);
        Assert.assertFalse(sstable.isRepaired());
        Assert.assertFalse(sstable.isPendingRepair());
        assertShardContainsSstable(sstable, false, false, false, null,true, true);

        assertFalse(cfs.hasPendingRepairSSTables(repairID));

        cfs.mutateRepaired(Collections.singletonList(sstable), 0, repairID, false);

        Assert.assertFalse(sstable.isRepaired());
        Assert.assertTrue(sstable.isPendingRepair());
        assertEquals(repairID, sstable.getPendingRepair());
        assertShardContainsSstable(sstable, false, true, false, repairID,true,true);

        assertTrue(cfs.hasPendingRepairSSTables(repairID));
    }


    /**
     * Tests that finalized repairs result in {@link LocalSessions#sessionCompleted}
     * which reclassify the sstables as repaired
     */
    @Override
    @Test
    public void testCleanupCompactionFinalized() throws IOException
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        final boolean isOrphan = true;
        int numberOfSStables = 4; // this has to be >= T
        List<SSTableReader> sstables = new ArrayList<>(numberOfSStables);
        for (int i = 0; i < numberOfSStables; i++)
        {
            SSTableReader sstable = makeSSTable(isOrphan);
            sstables.add(sstable);
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertFalse(sstable.isPendingRepair());
            assertShardContainsSstable(sstable,
                                       false,
                                       false,
                                       false,
                                       null,
                                       true,
                                       true);
        }

        // change to pending repair
        cfs.mutateRepaired(sstables, 0, repairID, false);

        for (SSTableReader sstable : sstables)
        {
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertTrue(sstable.isPendingRepair());
            assertEquals(repairID, sstable.getPendingRepair());
            assertShardContainsSstable(sstable,
                                       false,
                                       true,
                                       false,
                                       repairID,
                                       true,
                                       true);
        }

        // finalize
        LocalSessionAccessor.finalizeUnsafe(repairID);

        for (SSTableReader sstable : sstables)
        {

            Assert.assertFalse(sstable.isRepaired());
            Assert.assertTrue(sstable.isPendingRepair());
            assertEquals(repairID, sstable.getPendingRepair());
        }

        // enable compaction to fetch next background task
        compactionStrategyContainer.enable();

        // pending repair sstables should be compacted
        Collection<AbstractCompactionTask> compactionTasks = compactionStrategyContainer.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        assertEquals(1, compactionTasks.size());

        AbstractCompactionTask compactionTask = compactionTasks.iterator().next();
        assertNotNull(compactionTask);
        assertSame(UnifiedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute();

        // sstables should not be found in any shards after compacted
        for (SSTableReader sstable : sstables)
        {
            assertShardContainsSstable(sstable,
                                       false,
                                       true,
                                       false,
                                       repairID,
                                       false,
                                       false);
            assertFalse(cfs.getLiveSSTables().contains(sstable));
            assertFalse(cfs.getPendingRepairSSTables(repairID).contains(sstable));
        }

        // new sstable is created with the same repairID
        assertEquals(1, cfs.getPendingRepairSSTables(repairID).size());
        SSTableReader compactedSSTable = cfs.getPendingRepairSSTables(repairID).iterator().next();

        Assert.assertFalse(compactedSSTable.isRepaired());
        Assert.assertTrue(compactedSSTable.isPendingRepair());
        assertEquals(repairID, compactedSSTable.getPendingRepair());

        // complete session
        LocalSession session = ARS.consistent.local.getSession(repairID);
        ARS.consistent.local.sessionCompleted(session);

        Assert.assertTrue(compactedSSTable.isRepaired());
        Assert.assertEquals(compactedSSTable.getRepairedAt(), session.repairedAt);
        Assert.assertFalse(compactedSSTable.isPendingRepair());

        assertEquals(0, cfs.getPendingRepairSSTables(repairID).size());
    }

    @Override
    @Test
    public void testFinalizedSessionTransientCleanup() throws IOException
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        final boolean isOrphan = true;
        int numberOfSStables = 4; // this has to be >= T
        List<SSTableReader> sstables = new ArrayList<>(numberOfSStables);
        for (int i = 0; i < numberOfSStables; i++)
        {
            SSTableReader sstable = makeSSTable(isOrphan);
            sstables.add(sstable);
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertFalse(sstable.isPendingRepair());
            assertShardContainsSstable(sstable,
                                       false,
                                       false,
                                       false,
                                       null,
                                       true,
                                       true);
        }

        // change to pending repair
        cfs.mutateRepaired(sstables, 0, repairID, true);

        for (SSTableReader sstable : sstables)
        {
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertTrue(sstable.isPendingRepair());
            Assert.assertTrue(sstable.isTransient());
            assertEquals(repairID, sstable.getPendingRepair());
            assertShardContainsSstable(sstable,
                                       false,
                                       true,
                                       true,
                                       repairID,
                                       true,
                                       true);
        }

        // finalize
        LocalSessionAccessor.finalizeUnsafe(repairID);

        for (SSTableReader sstable : sstables)
        {
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertTrue(sstable.isPendingRepair());
            Assert.assertTrue(sstable.isTransient());
            assertEquals(repairID, sstable.getPendingRepair());
        }

        // enable compaction to fetch next background task
        compactionStrategyContainer.enable();

        // pending repair sstables should be compacted
        Collection<AbstractCompactionTask> compactionTasks = compactionStrategyContainer.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        assertEquals(1, compactionTasks.size());

        AbstractCompactionTask compactionTask = compactionTasks.iterator().next();
        assertNotNull(compactionTask);
        assertSame(UnifiedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute();

        // sstables should not be found in any shards after compacted
        for (SSTableReader sstable : sstables)
        {
            assertShardContainsSstable(sstable,
                                       false,
                                       true,
                                       true,
                                       repairID,
                                       false,
                                       false);
            assertFalse(cfs.getLiveSSTables().contains(sstable));
            assertFalse(cfs.getPendingRepairSSTables(repairID).contains(sstable));
        }

        // new sstable is created with the same repairID
        assertEquals(1, cfs.getPendingRepairSSTables(repairID).size());
        SSTableReader compactedSSTable = cfs.getPendingRepairSSTables(repairID).iterator().next();

        Assert.assertFalse(compactedSSTable.isRepaired());
        Assert.assertTrue(compactedSSTable.isPendingRepair());
        assertEquals(repairID, compactedSSTable.getPendingRepair());

        // complete session
        LocalSession session = ARS.consistent.local.getSession(repairID);
        ARS.consistent.local.sessionCompleted(session);

        assertTrue(cfs.getLiveSSTables().isEmpty());
        assertEquals(0, cfs.getPendingRepairSSTables(repairID).size());
    }

    @Override
    @Test
    public void testFailedSessionTransientCleanup() throws IOException
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        final boolean isOrphan = true;
        int numberOfSStables = 4; // this has to be >= T
        List<SSTableReader> sstables = new ArrayList<>(numberOfSStables);
        for (int i = 0; i < numberOfSStables; i++)
        {
            SSTableReader sstable = makeSSTable(isOrphan);
            sstables.add(sstable);
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertFalse(sstable.isPendingRepair());
            assertShardContainsSstable(sstable,
                                       false,
                                       false,
                                       false,
                                       null,
                                       true,
                                       true);
        }

        // change to pending repair
        cfs.mutateRepaired(sstables, 0, repairID, true);

        for (SSTableReader sstable : sstables)
        {
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertTrue(sstable.isPendingRepair());
            Assert.assertEquals(repairID, sstable.getPendingRepair());
            assertShardContainsSstable(sstable,
                                       false,
                                       true,
                                       true,
                                       repairID,
                                       true,
                                       true);
        }
        // fail
        LocalSessionAccessor.failUnsafe(repairID);

        for (SSTableReader sstable : sstables)
        {
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertTrue(sstable.isPendingRepair());
            Assert.assertEquals(repairID, sstable.getPendingRepair());
            assertShardContainsSstable(sstable,
                                       false,
                                       true,
                                       true,
                                       repairID,
                                       true,
                                       true);
        }

        // enable compaction to fetch next background task
        compactionStrategyContainer.enable();

        // pending repair sstables should be compacted
        Collection<AbstractCompactionTask> compactionTasks = compactionStrategyContainer.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        assertEquals(1, compactionTasks.size());

        AbstractCompactionTask compactionTask = compactionTasks.iterator().next();
        assertNotNull(compactionTask);
        assertSame(UnifiedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute();

        // sstables should not be found in any shards after compacted
        for (SSTableReader sstable : sstables)
        {
            assertShardContainsSstable(sstable,
                                       false,
                                       true,
                                       true,
                                       repairID,
                                       false,
                                       false);
            assertFalse(cfs.getLiveSSTables().contains(sstable));
            assertFalse(cfs.getPendingRepairSSTables(repairID).contains(sstable));
        }

        // new sstable is created with the same repairID
        assertEquals(1, cfs.getPendingRepairSSTables(repairID).size());
        SSTableReader compactedSSTable = cfs.getPendingRepairSSTables(repairID).iterator().next();
        Assert.assertEquals(repairID, compactedSSTable.getPendingRepair());
        Assert.assertFalse(compactedSSTable.isRepaired());
        Assert.assertTrue(compactedSSTable.isPendingRepair());
        Assert.assertTrue(compactedSSTable.isTransient());

        // complete session
        LocalSession session = ARS.consistent.local.getSession(repairID);
        ARS.consistent.local.sessionCompleted(session);

        Assert.assertFalse(compactedSSTable.isRepaired());
        Assert.assertFalse(compactedSSTable.isPendingRepair());
        Assert.assertFalse(compactedSSTable.isTransient());

        assertEquals(0, cfs.getPendingRepairSSTables(repairID).size());
    }

    /**
     * Tests that failed repairs result in {@link LocalSessions#sessionCompleted}
     * which reclassify the sstables as unrepaired
     */
    @Override
    @Test
    public void testCleanupCompactionFailed() throws IOException
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        final boolean isOrphan = true;
        int numberOfSStables = 4; // this has to be >= T
        List<SSTableReader> sstables = new ArrayList<>(numberOfSStables);
        for (int i = 0; i < numberOfSStables; i++)
        {
            SSTableReader sstable = makeSSTable(isOrphan);
            sstables.add(sstable);
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertFalse(sstable.isPendingRepair());
            assertShardContainsSstable(sstable,
                                       false,
                                       false,
                                       false,
                                       null,
                                       true,
                                       true);
        }

        // change to pending repair
        cfs.mutateRepaired(sstables, 0, repairID, false);

        for (SSTableReader sstable : sstables)
        {
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertTrue(sstable.isPendingRepair());
            Assert.assertEquals(repairID, sstable.getPendingRepair());
            assertShardContainsSstable(sstable,
                                       false,
                                       true,
                                       false,
                                       repairID,
                                       true,
                                       true);
        }

        // fail
        LocalSessionAccessor.failUnsafe(repairID);

        for (SSTableReader sstable : sstables)
        {
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertTrue(sstable.isPendingRepair());
            Assert.assertEquals(repairID, sstable.getPendingRepair());
            assertShardContainsSstable(sstable,
                                       false,
                                       true,
                                       false,
                                       repairID,
                                       true,
                                       true);
        }

        // enable compaction to fetch next background task
        compactionStrategyContainer.enable();

        // pending repair sstables should be compacted
        Collection<AbstractCompactionTask> compactionTasks = compactionStrategyContainer.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        assertEquals(1, compactionTasks.size());

        AbstractCompactionTask compactionTask = compactionTasks.iterator().next();
        assertNotNull(compactionTask);
        assertSame(UnifiedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute();

        // sstables should not be found in any shards after compacted
        for (SSTableReader sstable : sstables)
        {
            assertShardContainsSstable(sstable,
                                       false,
                                       true,
                                       false,
                                       repairID,
                                       false,
                                       false);
            assertFalse(cfs.getLiveSSTables().contains(sstable));
            assertFalse(cfs.getPendingRepairSSTables(repairID).contains(sstable));
        }

        // new sstable is created with the same repairID
        assertEquals(1, cfs.getPendingRepairSSTables(repairID).size());
        SSTableReader compactedSSTable = cfs.getPendingRepairSSTables(repairID).iterator().next();
        Assert.assertEquals(repairID, compactedSSTable.getPendingRepair());
        Assert.assertFalse(compactedSSTable.isRepaired());
        Assert.assertTrue(compactedSSTable.isPendingRepair());

        // complete session
        LocalSession session = ARS.consistent.local.getSession(repairID);
        ARS.consistent.local.sessionCompleted(session);

        Assert.assertFalse(compactedSSTable.isRepaired());
        Assert.assertFalse(compactedSSTable.isPendingRepair());

        assertEquals(0, cfs.getPendingRepairSSTables(repairID).size());
    }

    @Override
    @Test
    public void testSessionCompleted() throws IOException
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        // add sstables as unrepaired
        final boolean isOrphan = false;
        SSTableReader sstable1 = makeSSTable(isOrphan);
        Assert.assertFalse(sstable1.isRepaired());
        Assert.assertFalse(sstable1.isPendingRepair());
        assertShardContainsSstable(sstable1,
                                   false,
                                   false,
                                   false,
                                   null,
                                   true,
                                   true);

        SSTableReader sstable2 = makeSSTable(isOrphan);
        Assert.assertFalse(sstable2.isRepaired());
        Assert.assertFalse(sstable2.isPendingRepair());
        assertShardContainsSstable(sstable2,
                                   false,
                                   false,
                                   false,
                                   null,
                                   true,
                                   true);

        SSTableReader sstable3 = makeSSTable(isOrphan);
        Assert.assertFalse(sstable3.isRepaired());
        Assert.assertFalse(sstable3.isPendingRepair());
        assertShardContainsSstable(sstable3,
                                   false,
                                   false,
                                   false,
                                   null,
                                   true,
                                   true);

        // change to pending repair
        cfs.mutateRepaired(ImmutableList.of(sstable1, sstable2, sstable3), 0, repairID, false);

        Assert.assertFalse(sstable1.isRepaired());
        Assert.assertTrue(sstable1.isPendingRepair());
        Assert.assertEquals(repairID, sstable1.getPendingRepair());
        assertShardContainsSstable(sstable1,
                                   false,
                                   true,
                                   false,
                                   repairID,
                                   true,
                                   true);

        Assert.assertFalse(sstable2.isRepaired());
        Assert.assertTrue(sstable2.isPendingRepair());
        Assert.assertEquals(repairID, sstable2.getPendingRepair());
        assertShardContainsSstable(sstable2,
                                   false,
                                   true,
                                   false,
                                   repairID,
                                   true,
                                   true);

        Assert.assertFalse(sstable3.isRepaired());
        Assert.assertTrue(sstable3.isPendingRepair());
        Assert.assertEquals(repairID, sstable3.getPendingRepair());
        assertShardContainsSstable(sstable3,
                                   false,
                                   true,
                                   false,
                                   repairID,
                                   true,
                                   true);

        // finalize
        LocalSessionAccessor.finalizeUnsafe(repairID);

        // complete (repair) session and sstables should be marked as repaired
        LocalSession session = ARS.consistent.local.getSession(repairID);
        ARS.consistent.local.sessionCompleted(session);

        // sstables are repaired
        assertShardContainsSstable(sstable1,
                                   true,
                                   false,
                                   false,
                                   null,
                                   true,
                                   true);
        assertShardContainsSstable(sstable2,
                                   true,
                                   false,
                                   false,
                                   null,
                                   true,
                                   true);
        assertShardContainsSstable(sstable3,
                                   true,
                                   false,
                                   false,
                                   null,
                                   true,
                                   true);
    }

    @Override
    @Test
    public void testSessionCompletedWithDifferentSSTables() throws IOException
    {
        UUID repairID1 = registerSession(cfs, true, true);
        UUID repairID2 = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID1, COORDINATOR, PARTICIPANTS);
        LocalSessionAccessor.prepareUnsafe(repairID2, COORDINATOR, PARTICIPANTS);

        // add sstables as unrepaired
        final boolean isOrphan = false;
        SSTableReader sstable1 = makeSSTable(isOrphan);
        Assert.assertFalse(sstable1.isRepaired());
        Assert.assertFalse(sstable1.isPendingRepair());
        assertShardContainsSstable(sstable1,
                                   false,
                                   false,
                                   false,
                                   null,
                                   true,
                                   true);

        SSTableReader sstable2 = makeSSTable(isOrphan);
        Assert.assertFalse(sstable2.isRepaired());
        Assert.assertFalse(sstable2.isPendingRepair());
        assertShardContainsSstable(sstable2,
                                   false,
                                   false,
                                   false,
                                   null,
                                   true,
                                   true);

        SSTableReader sstable3 = makeSSTable(isOrphan);
        Assert.assertFalse(sstable3.isRepaired());
        Assert.assertFalse(sstable3.isPendingRepair());
        assertShardContainsSstable(sstable3,
                                   false,
                                   false,
                                   false,
                                   null,
                                   true,
                                   true);

        // change sstable1 to pending repair for session 1
        cfs.mutateRepaired(Collections.singletonList(sstable1), 0, repairID1, false);

        Assert.assertFalse(sstable1.isRepaired());
        Assert.assertTrue(sstable1.isPendingRepair());
        Assert.assertEquals(repairID1, sstable1.getPendingRepair());
        assertShardContainsSstable(sstable1,
                                   false,
                                   true,
                                   false,
                                   repairID1,
                                   true,
                                   true);
        assertNumberOfShards(2);

        // change sstable2 to pending repair for session 2
        cfs.mutateRepaired(Collections.singletonList(sstable2), 0, repairID2, false);

        Assert.assertFalse(sstable2.isRepaired());
        Assert.assertTrue(sstable2.isPendingRepair());
        Assert.assertEquals(repairID2, sstable2.getPendingRepair());
        assertNumberOfShards(3);
        assertShardContainsSstable(sstable2,
                                   false,
                                   true,
                                   false,
                                   repairID2,
                                   true,
                                   true);

        // change sstable3 to repaired
        long repairedAt3 = System.currentTimeMillis();
        cfs.mutateRepaired(Collections.singletonList(sstable3), repairedAt3, null, false);

        Assert.assertTrue(sstable3.isRepaired());
        Assert.assertFalse(sstable3.isPendingRepair());
        assertNumberOfShards(3);
        assertShardContainsSstable(sstable3,
                                   true,
                                   false,
                                   false,
                                   null,
                                   true,
                                   true);

        // finalize session 1
        LocalSessionAccessor.finalizeUnsafe(repairID1);

        // simulate index build on pending sstable for session 1
        cfs.getTracker().tryModify(sstable1, OperationType.INDEX_BUILD);

        // completing session 1 will not require to disable compactions because:
        // * sstable1 is building index (and considered as compacting), which would not be found in any shards
        // * sstable2 belongs to a different session
        // * sstable3 is repaired
        LocalSession session1 = ARS.consistent.local.getSession(repairID1);
        ARS.consistent.local.sessionCompleted(session1);

        // expecting sstable1 not found in any shards
        assertShardContainsSstable(sstable1,
                                   false,
                                   true,
                                   false,
                                   repairID1,
                                   false,
                                   true);

        // expecting sstable2 exists in pending repair shard
        assertShardContainsSstable(sstable2,
                                   false,
                                   true,
                                   false,
                                   repairID2,
                                   true,
                                   true);

        // expecting sstable3 exists in repaired shards
        assertShardContainsSstable(sstable3,
                                   true,
                                   false,
                                   false,
                                   null,
                                   true,
                                   true);
    }

    private void assertNumberOfShards(int expectedNumberOfShards)
    {
        Collection<CompactionStrategy> compactionStrategies = compactionStrategyContainer.getStrategies();
        assertEquals(1, compactionStrategies.size());
        compactionStrategies.forEach(cs -> {
            assertTrue(cs instanceof UnifiedCompactionStrategy);

            UnifiedCompactionStrategy ucs = ((UnifiedCompactionStrategy) cs);
            assertEquals("Expecting number of shards in the strategy.",
                         expectedNumberOfShards,
                         ucs.getShardsWithBuckets().keySet().size());
        });
    }

    private void assertShardContainsSstable(SSTableReader sstable,
                                            boolean expectedIsRepaired,
                                            boolean expectedIsPending,
                                            boolean expectedIsTransient,
                                            UUID expectedRepairId,
                                            boolean expectedRepairStatus,
                                            boolean expectedContainsSstable)
    {
        List<CompactionStrategy> compactionStrategies = compactionStrategyContainer.getStrategies();
        // CompactionStrategyContainer should always contains 1 UnifiedCompactionStrategy
        assertEquals(1, compactionStrategies.size());
        compactionStrategies.forEach(cs -> {
            assertTrue(cs instanceof UnifiedCompactionStrategy);

            UnifiedCompactionStrategy ucs = ((UnifiedCompactionStrategy) cs);
            Set<SSTableReader> ucsSstables = ucs.getSSTables()
                                                .stream()
                                                .filter(sst -> sst.equals(sstable))
                                                .collect(Collectors.toSet());

            assertEquals("Expecting strategy contains sstable.", expectedContainsSstable, ucsSstables.size() == 1);

            Map<Shard, List<UnifiedCompactionStrategy.Bucket>> shardListMap = ucs.getShardsWithBuckets();
            Set<Shard> shards = shardListMap.keySet();

            if (expectedRepairStatus)
            {
                Set<Shard> shardsWithPrefix = shards.stream()
                                                    .filter(shard -> {
                                                        if (shard.sstables.isEmpty())
                                                            return false;

                                                        SSTableReader shardSSTable = shard.sstables.get(0);
                                                        return shardSSTable.isRepaired() == expectedIsRepaired &&
                                                               shardSSTable.isTransient() == expectedIsTransient &&
                                                               shardSSTable.isPendingRepair() == expectedIsPending &&
                                                               (shardSSTable.getPendingRepair() == null
                                                                ? expectedRepairId == null
                                                                : shardSSTable.getPendingRepair().equals(expectedRepairId));
                                                    })
                                                    .collect(Collectors.toSet());

                assertEquals(String.format("Expecting a shard with repair status: pending=%s repaired=%s but found %s of it.",
                                           expectedIsPending, expectedIsRepaired, shardsWithPrefix.size()),
                             1,
                             shardsWithPrefix.size());

                Shard shardWithPrefix = shardsWithPrefix.iterator().next();
                assertEquals(String.format("Expecting a shard with repair status: %s contains the sstable is %s.",
                                           expectedRepairStatus,
                                           expectedContainsSstable),
                             expectedContainsSstable,
                             shardWithPrefix.sstables.contains(sstable));
            }
            else
            {
                // not expecting any shard would contain the sstable
                Set<Shard> shardsContainsSstable = shards.stream()
                                                         .filter(shard -> shard.sstables.contains(sstable))
                                                         .collect(Collectors.toSet());

                assertTrue(String.format("Expecting no shard should contain the sstable but found exists in %s",
                                         shardsContainsSstable),
                           shardsContainsSstable.isEmpty());
            }
        });
    }
}
