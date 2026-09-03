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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import org.apache.cassandra.db.compaction.AbstractStrategyHolder.DestinationRouter;
import org.apache.cassandra.db.compaction.AbstractStrategyHolder.GroupedSSTableContainer;
import org.apache.cassandra.db.compaction.AbstractStrategyHolder.TaskSupplier;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.replication.ImmutableCoordinatorLogOffsets;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TrackedCompactionManagerTest extends AbstractPendingRepairTest
{
    private static ShortMutationId id(long logId, int offset)
    {
        return new ShortMutationId(logId, offset);
    }

    private static ImmutableSet<ShortMutationId> key(ShortMutationId... ids)
    {
        return ImmutableSet.copyOf(ids);
    }

    static final DestinationRouter SINGLE_PARTITION = new DestinationRouter()
    {
        public int getIndexForSSTable(SSTableReader sstable) { return 0; }
        public int getIndexForSSTableDirectory(Descriptor descriptor) { return 0; }
    };

    private TrackedCompactionManager manager()
    {
        TrackedCompactionManager manager = new TrackedCompactionManager(cfs, SINGLE_PARTITION);
        manager.setStrategy(CompactionParams.DEFAULT, 1);
        return manager;
    }

    private static GroupedSSTableContainer group(TrackedCompactionManager manager, Iterable<SSTableReader> sstables)
    {
        GroupedSSTableContainer container = manager.createGroupedSSTableContainer();
        sstables.forEach(container::add);
        return container;
    }

    private static Collection<AbstractCompactionTask> nextBackgroundTasks(TrackedCompactionManager manager)
    {
        for (TaskSupplier supplier : manager.getBackgroundTaskSuppliers(FBUtilities.nowInSeconds()))
        {
            Collection<AbstractCompactionTask> tasks = supplier.getTasks();
            if (tasks != null && !tasks.isEmpty())
                return tasks;
        }
        return Collections.emptyList();
    }

    /**
     * Silo keys other than {@link TrackedCompactionManager#NONE}, which is always present.
     * */
    private static Set<ImmutableSet<ShortMutationId>> transferKeys(TrackedCompactionManager manager)
    {
        return manager.keys().stream().filter(k -> !k.isEmpty()).collect(Collectors.toSet());
    }

    private static void attachTransfer(SSTableReader sstable, ShortMutationId... ids) throws IOException
    {
        ImmutableCoordinatorLogOffsets.Builder builder = new ImmutableCoordinatorLogOffsets.Builder();
        for (ShortMutationId id : ids)
        {
            Bounds<Token> bounds = new Bounds<>(sstable.getFirst().getToken(), sstable.getLast().getToken());
            builder.addTransfer(id, bounds);
        }
        sstable.mutateCoordinatorLogOffsetsAndReload(builder.build());
    }

    private SSTableReader sstableWithTransfers(ShortMutationId... ids) throws IOException
    {
        SSTableReader sstable = makeSSTable(true);
        attachTransfer(sstable, ids);
        return sstable;
    }

    private SSTableReader sstableWithMutations(long logId, int offset) throws IOException
    {
        SSTableReader sstable = makeSSTable(true);
        ImmutableCoordinatorLogOffsets offsets = new ImmutableCoordinatorLogOffsets.Builder()
                                                .add(new MutationId(logId, (long) offset))
                                                .build();
        sstable.mutateCoordinatorLogOffsetsAndReload(offsets);
        return sstable;
    }

    /**
     * Any offsets at all put an sstable in this group
     */
    @Test
    public void anyOffsetsMeanUnreconciled() throws IOException
    {
        SSTableReader transfer = sstableWithTransfers(id(1, 0));
        SSTableReader mutations = sstableWithMutations(1, 0);

        assertEquals(CompactionGroup.UNRECONCILED, CompactionGroup.of(transfer));
        assertEquals(CompactionGroup.UNRECONCILED, CompactionGroup.of(mutations));
        assertEquals("no offsets is untracked data", CompactionGroup.UNREPAIRED, CompactionGroup.of(makeSSTable(true)));
        assertSame(csm.getHolder(CompactionGroup.of(transfer)), csm.getHolder(CompactionGroup.of(mutations)));
    }

    /**
     * A silo per distinct transfer set, so no strategy can select across the boundary
     */
    @Test
    public void sstablesAreSiloedByTransferSet() throws IOException
    {
        TrackedCompactionManager manager = manager();

        SSTableReader a = sstableWithTransfers(id(1, 0));
        SSTableReader b = sstableWithTransfers(id(1, 1));
        SSTableReader both = sstableWithTransfers(id(1, 0), id(1, 1));
        SSTableReader sameAsA = sstableWithTransfers(id(1, 0));

        manager.addSSTable(a);
        manager.addSSTable(b);
        manager.addSSTable(both);
        manager.addSSTable(sameAsA);

        assertEquals(3, transferKeys(manager).size());
        assertEquals("an equal transfer set must share a silo rather than making a fourth",
                     ImmutableSet.of(a, sameAsA), manager.sstablesFor(key(id(1, 0))));
        assertEquals(Collections.singleton(b), manager.sstablesFor(key(id(1, 1))));
        assertEquals(Collections.singleton(both), manager.sstablesFor(key(id(1, 0), id(1, 1))));
    }

    /**
     * Sstables carrying no transfers share the empty-key silo
     */
    @Test
    public void sstablesWithoutTransfersShareOneSilo() throws IOException
    {
        TrackedCompactionManager manager = manager();

        SSTableReader a = sstableWithMutations(1, 0);
        SSTableReader b = sstableWithMutations(2, 7);
        SSTableReader transfer = sstableWithTransfers(id(1, 0));
        manager.addSSTable(a);
        manager.addSSTable(b);
        manager.addSSTable(transfer);

        assertEquals(ImmutableSet.of(a, b), manager.sstablesFor(TrackedCompactionManager.NONE));
        assertSame(manager.getIfPresent(TrackedCompactionManager.NONE), manager.getIfPresent(a));
        assertSame(manager.getIfPresent(TrackedCompactionManager.NONE), manager.getIfPresent(b));

        assertNotSame("a transfer must not be compactable with ordinary tracked data",
                      manager.getIfPresent(transfer), manager.getIfPresent(a));
        assertEquals(Collections.singleton(transfer), manager.sstablesFor(key(id(1, 0))));
    }

    /**
     * An empty transfer silo is pruned however it came to be empty, and holds no data and generates no work in the
     * meantime. Both routes matter: the delete path reaches past the manager straight to the strategy, and a delete
     * notification can resurrect a silo that never held anything.
     */
    @Test
    public void emptyTransferSilosArePruned() throws IOException
    {
        TrackedCompactionManager manager = manager();
        SSTableReader sstable = sstableWithTransfers(id(1, 0));

        // Resurrected by a lookup, never having held anything.
        manager.getOrCreate(sstable);
        // Holds no data and generates no work, and the walk that reads the estimate prunes it.
        assertFalse(manager.hasDataFor(key(id(1, 0))));
        assertTrue(nextBackgroundTasks(manager).isEmpty());
        assertEquals(0, manager.getEstimatedRemainingTasks());
        assertTrue(transferKeys(manager).isEmpty());

        // Emptied past the manager, so the manager is never told.
        manager.addSSTable(sstable);
        assertEquals(1, transferKeys(manager).size());
        manager.getIfPresent(sstable).getStrategyFor(sstable).removeSSTable(sstable);
        assertEquals("manager was not notified, so the silo is still there", 1, transferKeys(manager).size());
        assertEquals(0, manager.getEstimatedRemainingTasks());
        assertTrue(transferKeys(manager).isEmpty());

        // Emptied through the manager, which prunes without waiting for a walk.
        manager.addSSTable(sstable);
        manager.removeSSTable(sstable);
        assertTrue("removal through the manager prunes immediately", transferKeys(manager).isEmpty());
    }

    /** The NONE silo takes ordinary unreconciled writes continuously, so unlike a transfer silo it outlives emptying. */
    @Test
    public void noneTransfersSiloIsNeverPruned() throws IOException
    {
        TrackedCompactionManager manager = manager();
        assertNotNull(manager.getIfPresent(TrackedCompactionManager.NONE));

        SSTableReader sstable = sstableWithMutations(1, 0);
        manager.addSSTable(sstable);
        manager.removeSSTable(sstable);

        assertTrue("the NONE silo must survive being emptied where a transfer silo would not",
                   manager.keys().contains(TrackedCompactionManager.NONE));
        assertNotNull(manager.getIfPresent(TrackedCompactionManager.NONE));
        assertFalse(manager.hasDataFor(TrackedCompactionManager.NONE));
    }

    /**
     * Membership answers track the sstables held, not whether a silo exists for their key.
     * */
    @Test
    public void sessionHasData() throws IOException
    {
        TrackedCompactionManager manager = manager();
        SSTableReader transfer = sstableWithTransfers(id(1, 0));
        SSTableReader unreconciled = sstableWithMutations(1, 0);

        assertFalse(manager.hasDataFor(key(id(1, 0))));
        assertFalse(manager.containsSSTable(transfer));
        assertFalse(manager.containsSSTable(unreconciled));
        assertNull(manager.getIfPresent(transfer));

        manager.addSSTable(transfer);
        manager.addSSTable(unreconciled);

        assertTrue(manager.hasDataFor(key(id(1, 0))));
        assertTrue(manager.containsSSTable(transfer));
        assertTrue(manager.containsSSTable(unreconciled));
        assertNotNull(manager.getIfPresent(transfer));
    }

    /**
     * The repair-status notification sends a removal for sstables whose group has already changed, so a removal for an
     * sstable this manager never held must not throw.
     */
    @Test
    public void removingSomethingNeverHeldIsHarmless() throws IOException
    {
        TrackedCompactionManager manager = manager();
        manager.removeSSTable(sstableWithMutations(1, 0));
        manager.removeSSTable(sstableWithTransfers(id(1, 0)));
    }

    /** Compaction output replaces its inputs in whichever silo each belongs to, without creating a new one. */
    @Test
    public void replaceRoutesProperly() throws IOException
    {
        TrackedCompactionManager manager = manager();

        SSTableReader transfer = sstableWithTransfers(id(1, 0));
        SSTableReader unreconciled = sstableWithMutations(1, 0);
        manager.addSSTable(transfer);
        manager.addSSTable(unreconciled);

        SSTableReader newTransfer = sstableWithTransfers(id(1, 0));
        SSTableReader newUnreconciled = sstableWithMutations(2, 3);

        manager.replaceSSTables(group(manager, ImmutableSet.of(transfer, unreconciled)),
                                group(manager, ImmutableSet.of(newTransfer, newUnreconciled)));

        // Each replacement lands in the silo its input came from, and no new silo appears.
        assertEquals(Collections.singleton(newTransfer), manager.sstablesFor(key(id(1, 0))));
        assertEquals(Collections.singleton(newUnreconciled),
                     manager.sstablesFor(TrackedCompactionManager.NONE));
        assertEquals(1, transferKeys(manager).size());
    }

    @Test
    public void replaceWithNothingRemovedIsAnAdd() throws IOException
    {
        TrackedCompactionManager manager = manager();
        SSTableReader added = sstableWithMutations(1, 0);

        manager.replaceSSTables(group(manager, Collections.emptySet()),
                                group(manager, Collections.singleton(added)));

        assertTrue(manager.containsSSTable(added));
    }

    @Test
    public void getNextBackgroundTaskNoSessions()
    {
        TrackedCompactionManager manager = manager();

        // Neither the always-present silo when empty, nor a silo that does not exist.
        assertNull(manager.getPromotionTask(TrackedCompactionManager.NONE));
        assertNull(manager.getPromotionTask(key(id(9, 9))));
        assertTrue(manager.getNextPromotionTasks().isEmpty());
    }

    @Test
    public void userDefinedTaskTest() throws IOException
    {
        TrackedCompactionManager manager = manager();
        SSTableReader sstable = sstableWithTransfers(id(1, 0));

        assertNull(manager.getIfPresent(sstable));

        GroupedSSTableContainer container = group(manager, Collections.singleton(sstable));
        Collection<AbstractCompactionTask> tasks = manager.getUserDefinedTasks(container, FBUtilities.nowInSeconds());
        try
        {
            assertEquals("the request must be honoured, not dropped", 1, tasks.size());
            assertNotNull(manager.getIfPresent(sstable));
        }
        finally
        {
            tasks.forEach(AbstractCompactionTask::rejected);
        }
    }
}
