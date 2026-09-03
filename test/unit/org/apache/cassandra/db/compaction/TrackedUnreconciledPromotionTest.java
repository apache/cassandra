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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.replication.ImmutableCoordinatorLogOffsets;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TrackedUnreconciledPromotionTest
{
    private static final AtomicInteger keyspaceNumber = new AtomicInteger();

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        MutationJournal.start();
        MutationTrackingService.start();

        // we want to drive this manually for testing
        MutationTrackingService.instance().pauseOffsetsPersisterForTesting();
    }

    @AfterClass
    public static void tearDownClass()
    {
        MutationTrackingService.instance().resumeOffsetsPersisterForTesting();
    }

    private static void persistLogState()
    {
        MutationTrackingService.instance().persistLogStateForTesting(true);
    }

    private static String nextKeyspaceName()
    {
        return "tracked_promotion_" + keyspaceNumber.incrementAndGet();
    }

    private static ColumnFamilyStore newTrackedTable()
    {
        String ks = nextKeyspaceName();
        TableMetadata tableMetadata =
            TableMetadata.builder(ks, "tbl")
                         .addPartitionKeyColumn("k", Int32Type.instance)
                         .addRegularColumn("v", Int32Type.instance)
                         .build();

        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1, ReplicationType.tracked), tableMetadata);

        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore("tbl");
        cfs.disableAutoCompaction();
        return cfs;
    }

    private static MutationId applyMutation(ColumnFamilyStore cfs, int k, int v)
    {
        TableMetadata metadata = cfs.metadata();
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(k));
        MutationId id = MutationTrackingService.instance().nextMutationId(metadata.keyspace, key.getToken());
        SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(id, metadata.keyspace, key);
        PartitionUpdate.SimpleBuilder partition = builder.update(metadata);
        partition.row().add("v", v);
        Mutation mutation = builder.build();
        Assert.assertFalse(mutation.id().isNone());
        mutation.apply();
        return mutation.id();
    }

    private static TrackedCompactionManager manager(ColumnFamilyStore cfs)
    {
        return (TrackedCompactionManager) cfs.getCompactionStrategyManager()
                                             .getHolder(CompactionGroup.UNRECONCILED);
    }

    private static Set<SSTableReader> promotable(ColumnFamilyStore cfs)
    {
        return manager(cfs).promotableSSTables();
    }

    private static Collection<AbstractCompactionTask> promotionTasks(ColumnFamilyStore cfs)
    {
        return manager(cfs).getNextPromotionTasks();
    }

    private static SSTableReader flushUnreconciled(ColumnFamilyStore cfs, int k)
    {
        Set<SSTableReader> before = new HashSet<>(cfs.getLiveSSTables());
        applyMutation(cfs, k, k);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Set<SSTableReader> added = new HashSet<>(cfs.getLiveSSTables());
        added.removeAll(before);
        assertEquals(1, added.size());

        SSTableReader written = Iterables.getOnlyElement(added);
        assertFalse(written.isRepaired());
        assertFalse(written.getSSTableMetadata().coordinatorLogOffsets.isEmpty());
        assertEquals(CompactionGroup.UNRECONCILED, CompactionGroup.of(written));
        return written;
    }

    private static void runPromotion(Collection<AbstractCompactionTask> tasks)
    {
        for (AbstractCompactionTask task : tasks)
            task.execute(ActiveCompactionsTracker.NOOP);
    }

    @Test
    public void backlogDrainsInOnePass()
    {
        ColumnFamilyStore cfs = newTrackedTable();

        int backlog = 4;
        Set<SSTableReader> stranded = new HashSet<>();
        for (int k = 0; k < backlog; k++)
            stranded.add(flushUnreconciled(cfs, k));

        assertEquals(backlog, cfs.getLiveSSTables().size());
        assertTrue(promotable(cfs).isEmpty());

        persistLogState();
        assertEquals(stranded, promotable(cfs));

        Collection<AbstractCompactionTask> tasks = promotionTasks(cfs);
        assertEquals("the whole backlog should be one task, not one per sstable", 1, tasks.size());
        runPromotion(tasks);

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            assertTrue(sstable.isRepaired());
            assertTrue(sstable.getSSTableMetadata().coordinatorLogOffsets.isEmpty());
            assertEquals(CompactionGroup.REPAIRED, CompactionGroup.of(sstable));
            // Tombstone purging is gated on repairedAt, so the timestamp has to be real rather than the sentinel.
            Assert.assertNotEquals(ActiveRepairService.UNREPAIRED_SSTABLE, sstable.getRepairedAt());
        }
        assertTrue(promotable(cfs).isEmpty());
    }

    @Test
    public void busySSTableDoesntBlockRemainder()
    {
        ColumnFamilyStore cfs = newTrackedTable();

        Set<SSTableReader> stranded = new HashSet<>();
        for (int k = 0; k < 3; k++)
            stranded.add(flushUnreconciled(cfs, k));

        persistLogState();
        assertEquals(stranded, promotable(cfs));

        SSTableReader busy = stranded.iterator().next();
        try (LifecycleTransaction held = cfs.getTracker().tryModify(Collections.singleton(busy),
                                                                    OperationType.COMPACTION))
        {
            assertNotNull(held);

            Collection<AbstractCompactionTask> tasks = promotionTasks(cfs);
            assertEquals(1, tasks.size());
            runPromotion(tasks);
        }

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (sstable.descriptor.equals(busy.descriptor))
                assertFalse("the busy sstable should have been left alone", sstable.isRepaired());
            else
                assertTrue("the rest of the backlog should have drained anyway", sstable.isRepaired());
        }

        // The deferred sstable is promoted on a later pass.
        assertEquals("the busy sstable is still eligible on a later pass", 1, promotable(cfs).size());
        runPromotion(promotionTasks(cfs));
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertTrue("the whole backlog should eventually drain", sstable.isRepaired());
    }

    @Test
    public void neitherRepairedNorOffsetFreeSSTablesArePromotable() throws IOException
    {
        ColumnFamilyStore repairedCase = newTrackedTable();
        SSTableReader repaired = flushUnreconciled(repairedCase, 1);
        persistLogState();
        assertTrue(manager(repairedCase).isPromotable(repaired));

        ImmutableCoordinatorLogOffsets offsets = repaired.getSSTableMetadata().coordinatorLogOffsets;
        repaired.mutateRepairedAndReload(Clock.Global.currentTimeMillis(), null);
        repaired.mutateCoordinatorLogOffsetsAndReload(offsets);

        assertFalse("precondition: it still carries offsets, so the repaired guard is the one under test",
                    repaired.getSSTableMetadata().coordinatorLogOffsets.isEmpty());
        assertFalse(manager(repairedCase).isPromotable(repaired));
        assertTrue(promotable(repairedCase).isEmpty());

        ColumnFamilyStore offsetFreeCase = newTrackedTable();
        SSTableReader offsetFree = flushUnreconciled(offsetFreeCase, 1);
        offsetFree.mutateCoordinatorLogOffsetsAndReload(new ImmutableCoordinatorLogOffsets.Builder().build());

        assertFalse("precondition: it stays unrepaired, so the offsets guard is the one under test",
                    offsetFree.isRepaired());
        assertTrue(offsetFree.getSSTableMetadata().coordinatorLogOffsets.isEmpty());
        assertFalse(manager(offsetFreeCase).isPromotable(offsetFree));
        assertTrue(promotable(offsetFreeCase).isEmpty());
    }

    private static TrackedCompactionManager standaloneManager(ColumnFamilyStore cfs)
    {
        TrackedCompactionManager manager =
            new TrackedCompactionManager(cfs, TrackedCompactionManagerTest.SINGLE_PARTITION);
        manager.setStrategy(CompactionParams.DEFAULT, 1);
        return manager;
    }

    private static ShortMutationId shortIdOf(MutationId id)
    {
        return new ShortMutationId(id.logId(), id.offset());
    }

    private static void setOffsets(SSTableReader sstable, ShortMutationId transferId, MutationId... mutations)
        throws IOException
    {
        Bounds<Token> bounds = new Bounds<>(sstable.getFirst().getToken(), sstable.getLast().getToken());
        ImmutableCoordinatorLogOffsets.Builder builder = new ImmutableCoordinatorLogOffsets.Builder();
        builder.addTransfer(transferId, bounds);
        for (MutationId id : mutations)
            builder.add(id);
        sstable.mutateCoordinatorLogOffsetsAndReload(builder.build());
    }

    /**
     * SSTable shouldn't contain transfer and mutation ids offsets, but if they do for some reason, both should
     * be taken into account when promoting to repaired
     */
    @Test
    public void reconciledTransferDoesNotPromoteUnreconciledMutations() throws IOException
    {
        ColumnFamilyStore cfs = newTrackedTable();
        TrackedCompactionManager manager = standaloneManager(cfs);

        MutationId transferOrigin = applyMutation(cfs, 100, 100);
        Set<SSTableReader> reconciled = new HashSet<>();
        for (int k = 0; k < 3; k++)
            reconciled.add(flushUnreconciled(cfs, k));

        persistLogState();
        ShortMutationId transferId = shortIdOf(transferOrigin);
        for (SSTableReader sstable : reconciled)
            setOffsets(sstable, transferId);

        // Applied after the persist, so this one has not reconciled.
        MutationId behindId = applyMutation(cfs, 99, 99);
        SSTableReader behind = flushUnreconciled(cfs, 98);
        setOffsets(behind, transferId, behindId);

        ImmutableSet<ShortMutationId> key = ImmutableSet.of(transferId);
        manager.addSSTables(reconciled);
        manager.addSSTable(behind);

        // It shares the silo, so eligibility is the only thing that can exclude it.
        assertEquals(key, TrackedCompactionManager.keyOf(behind));
        assertEquals(4, manager.sstablesFor(key).size());
        assertFalse("its mutation has not reconciled, so it must not be promotable", manager.isPromotable(behind));
        assertEquals("promotion takes the reconciled members only", reconciled, manager.promotableSSTables(key));

        // One task covers the eligible members of a silo rather than one task each.
        AbstractCompactionTask promotion = manager.getPromotionTask(key);
        try
        {
            assertNotNull(promotion);
            assertEquals(reconciled, promotion.transaction.originals());
        }
        finally
        {
            // it holds a lifecycle transaction over the silo, which leaks if neither run nor released
            if (promotion != null)
                promotion.rejected();
        }
    }

    @Test
    public void partlyEligibleSiloPromotesTheSubset()
    {
        ColumnFamilyStore cfs = newTrackedTable();

        Set<SSTableReader> reconciled = new HashSet<>();
        for (int k = 0; k < 3; k++)
            reconciled.add(flushUnreconciled(cfs, k));
        persistLogState();
        SSTableReader behind = flushUnreconciled(cfs, 3);

        TrackedCompactionManager manager = manager(cfs);
        assertEquals(4, manager.sstablesFor(TrackedCompactionManager.NONE).size());
        assertEquals("only the earlier flushes are eligible", reconciled, promotable(cfs));

        runPromotion(promotionTasks(cfs));

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (sstable.descriptor.equals(behind.descriptor))
                assertFalse("the unreconciled sstable must be left alone", sstable.isRepaired());
            else
                assertTrue(sstable.isRepaired());
        }
    }
    private static AbstractStrategyHolder holderOf(ColumnFamilyStore cfs, SSTableReader sstable)
    {
        return cfs.getCompactionStrategyManager().getHolder(CompactionGroup.of(sstable));
    }

    @Test
    public void promotionMovesTheSSTableToTheRepairedHolder()
    {
        ColumnFamilyStore cfs = newTrackedTable();
        SSTableReader stranded = flushUnreconciled(cfs, 1);

        AbstractStrategyHolder tracked = manager(cfs);
        // Starts in the tracked holder, counted as unrepaired.
        assertTrue(tracked.containsSSTable(stranded));
        assertTrue(cfs.metric.bytesUnrepaired.getValue() > 0);
        assertEquals(0L, (long) cfs.metric.bytesRepaired.getValue());

        persistLogState();
        runPromotion(promotionTasks(cfs));

        SSTableReader promoted = Iterables.getOnlyElement(cfs.getLiveSSTables());
        AbstractStrategyHolder repaired = holderOf(cfs, promoted);

        assertEquals(CompactionGroup.REPAIRED, CompactionGroup.of(promoted));
        // Ownership moves between holders, and the byte metrics follow it.
        assertTrue(repaired.containsSSTable(promoted));
        assertFalse(tracked.containsSSTable(promoted));
        assertEquals(0L, (long) cfs.metric.bytesUnrepaired.getValue());
        assertTrue(cfs.metric.bytesRepaired.getValue() > 0);
    }

    @Test
    public void compactionKeepsOffsetsIncludingReconciledOnes()
    {
        ColumnFamilyStore cfs = newTrackedTable();

        SSTableReader first = flushUnreconciled(cfs, 1);
        persistLogState();                              // first's id reconciles, but nothing rewrites it
        SSTableReader second = flushUnreconciled(cfs, 2);

        Set<ShortMutationId> before = new HashSet<>();
        before.addAll(idsOf(first));
        before.addAll(idsOf(second));
        assertEquals(2, before.size());

        compact(cfs, first, second);

        SSTableReader merged = Iterables.getOnlyElement(cfs.getLiveSSTables());
        assertFalse("one id has not reconciled, so the output must not be promoted at write time",
                    merged.isRepaired());
        assertEquals("the union must survive, reconciled id included", before, idsOf(merged));

        // ...and a second compaction of the same sstable is equally non-destructive
        compact(cfs, merged);
        SSTableReader again = Iterables.getOnlyElement(cfs.getLiveSSTables());
        assertFalse(again.isRepaired());
        assertEquals(before, idsOf(again));

        persistLogState();
        runPromotion(promotionTasks(cfs));

        SSTableReader promoted = Iterables.getOnlyElement(cfs.getLiveSSTables());
        assertTrue(promoted.isRepaired());
        assertTrue(promoted.getSSTableMetadata().coordinatorLogOffsets.isEmpty());
    }

    private static Set<ShortMutationId> idsOf(SSTableReader sstable)
    {
        Set<ShortMutationId> ids = new HashSet<>();
        for (Map.Entry<Long, Offsets.Immutable> entry : sstable.getSSTableMetadata().coordinatorLogOffsets.entries())
            Iterables.addAll(ids, entry.getValue());
        return ids;
    }

    private static void compact(ColumnFamilyStore cfs, SSTableReader... sstables)
    {
        List<Descriptor> descriptors = new ArrayList<>();
        for (SSTableReader sstable : sstables)
            descriptors.add(sstable.descriptor);
        FBUtilities.waitOnFuture(CompactionManager.instance.submitUserDefined(cfs, descriptors, CompactionManager.NO_GC));
    }

    @Test
    public void unpromotableGaugeTest() throws IOException
    {
        ColumnFamilyStore cfs = newTrackedTable();
        assertEquals(0, (int) cfs.metric.unpromotableSSTables.getValue());

        SSTableReader stranded = flushUnreconciled(cfs, 1);
        assertEquals("carrying offsets, so it is promotable and does not count",
                     0, (int) cfs.metric.unpromotableSSTables.getValue());

        // Clearing the offsets while leaving it unrepaired is the state the gauge is for. Promotion cannot produce it,
        // because it sets repairedAt in the same mutation; nodetool verify resetting repairedAt can.
        stranded.mutateCoordinatorLogOffsetsAndReload(new ImmutableCoordinatorLogOffsets.Builder().build());
        assertFalse(stranded.isRepaired());
        assertEquals("unrepaired with no offsets must be counted",
                     1, (int) cfs.metric.unpromotableSSTables.getValue());

        // A promoted sstable also has no offsets, but it is repaired, so it must not be counted.
        ColumnFamilyStore other = newTrackedTable();
        flushUnreconciled(other, 1);
        persistLogState();
        runPromotion(promotionTasks(other));
        SSTableReader promoted = Iterables.getOnlyElement(other.getLiveSSTables());
        assertTrue(promoted.getSSTableMetadata().coordinatorLogOffsets.isEmpty());
        assertTrue(promoted.isRepaired());
        assertEquals("repaired, so not unpromotable however few offsets it carries",
                     0, (int) other.metric.unpromotableSSTables.getValue());
    }
}
