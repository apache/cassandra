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

package org.apache.cassandra.db.memtable;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableSet;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LogDomain;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableProvenance;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * A table takes writes from both logs while part of its token range is migrating, and {@link SplitDomainMemtable} holds
 * one internal memtable per {@link LogDomain} so that each write is bounded against its own log. Every consumer of a
 * {@link Memtable} keeps working without learning it got a wrapper, so the cases here are grouped by consumer, in three
 * sections: what a consumer reads off a split generation, how the tracker installs one, and how one flushes.
 */
public class SplitDomainMemtableTest
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
    }

    private static ColumnFamilyStore newTrackedTable()
    {
        String ks = "domain_split_" + keyspaceNumber.incrementAndGet();
        TableMetadata metadata = TableMetadata.builder(ks, "tbl")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addRegularColumn("v", Int32Type.instance)
                                              .build();
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1, ReplicationType.tracked), metadata);
        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore("tbl");
        cfs.disableAutoCompaction();
        return cfs;
    }

    private static Memtable internal(ColumnFamilyStore cfs, LogDomainBounds bounds, LogDomain domain)
    {
        return cfs.createMemtable(bounds.forDomain(domain), domain);
    }

    private static SplitDomainMemtable newWrapper(ColumnFamilyStore cfs)
    {
        LogDomainBounds bounds = LogDomainBounds.atCurrentPositions();
        return new SplitDomainMemtable(internal(cfs, bounds, LogDomain.COMMIT_LOG),
                                       internal(cfs, bounds, LogDomain.MUTATION_JOURNAL),
                                       1L);
    }

    private static DecoratedKey key(ColumnFamilyStore cfs, int k)
    {
        return cfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(k));
    }

    private static void write(ColumnFamilyStore cfs, Memtable memtable, int k, LogDomain domain)
    {
        write(cfs, memtable, k, domain, FBUtilities.timestampMicros());
    }

    private static void write(ColumnFamilyStore cfs, Memtable memtable, int k, LogDomain domain, long timestamp)
    {
        TableMetadata metadata = cfs.metadata();
        DecoratedKey key = key(cfs, k);
        // An untracked write carries no mutation id, which is what keeps coordinator log offsets off the sstable a
        // commit-log-domain memtable flushes. MigrationRouter guarantees the two agree for a base table.
        MutationId id = domain.isJournal()
                        ? MutationTrackingService.instance().nextMutationId(metadata.keyspace, key.getToken())
                        : MutationId.none();
        SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(id, metadata.keyspace, key);
        builder.timestamp(timestamp);
        builder.update(metadata).row().add("v", k);
        Mutation mutation = builder.build();
        try (OpOrder.Group group = Keyspace.writeOrder.start())
        {
            memtable.put(id, mutation.getPartitionUpdate(metadata), UpdateTransaction.NO_OP, group, domain);
        }
    }

    /** The keys a range read finds, which is the one read path that does not take a key. */
    private static Set<DecoratedKey> partitionKeys(Memtable memtable)
    {
        Set<DecoratedKey> keys = new HashSet<>();
        TableMetadata metadata = memtable.metadata();
        try (UnfilteredPartitionIterator partitions = memtable.partitionIterator(ColumnFilter.all(metadata),
                                                                                DataRange.allData(metadata.partitioner),
                                                                                SSTableReadsListener.NOOP_LISTENER))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator rows = partitions.next())
                {
                    keys.add(rows.partitionKey());
                }
            }
        }
        return keys;
    }

    // ---- What a consumer reads off a split generation ----------------------------------------------------------

    /**
     * Commit log segment reclamation asks which memtables still hold data below a position, through
     * {@link ColumnFamilyStore#forceFlush(CommitLogPosition)}. Only the commit-log internal can hold
     * commit-log-derived rows, so the wrapper answers from it alone. A generation whose journal internal is the dirty
     * one holds no commit log data, and must not pin the segment.
     * <p>
     * The ordering matters. Both internals' {@code approximateCommitLogLowerBound} are commit log positions taken at
     * construction, whatever the internal's domain. Aggregating across them is therefore only distinguishable from
     * delegating when the journal internal is the older of the two. That is also the order an install produces for a
     * tracked table, since the memtable already live becomes an internal and the other is created after it.
     */
    @Test
    public void journalOnlySplitMemtableDoesntPinCommitlogSegments()
    {
        ColumnFamilyStore cfs = newTrackedTable();
        LogDomainBounds bounds = LogDomainBounds.atCurrentPositions();
        Memtable journalInternal = internal(cfs, bounds, LogDomain.MUTATION_JOURNAL);
        write(cfs, journalInternal, 1, LogDomain.MUTATION_JOURNAL);

        // Advance the commit log past the journal internal's creation, so the two internals' bounds differ, then
        // create the commit-log internal above the position under test and leave it clean.
        appendToCommitLog();
        CommitLogPosition reclaimBelow = CommitLog.instance.getCurrentPosition();
        Memtable commitLogInternal = internal(cfs, bounds, LogDomain.COMMIT_LOG);
        SplitDomainMemtable wrapper = new SplitDomainMemtable(commitLogInternal, journalInternal,
                                                             journalInternal.getMemtableId());

        // sanity check
        assertNotEquals(commitLogInternal.mayContainDataBefore(reclaimBelow), journalInternal.mayContainDataBefore(reclaimBelow));

        assertFalse(wrapper.mayContainDataBefore(reclaimBelow));
        assertEquals(commitLogInternal.getApproximateCommitLogLowerBound(),
                     wrapper.getApproximateCommitLogLowerBound());
    }

    @Test
    public void splitDomainMemtablePinsCommitlogSegments()
    {
        ColumnFamilyStore cfs = newTrackedTable();
        LogDomainBounds bounds = LogDomainBounds.atCurrentPositions();
        Memtable commitLogInternal = internal(cfs, bounds, LogDomain.COMMIT_LOG);
        write(cfs, commitLogInternal, 1, LogDomain.COMMIT_LOG);

        appendToCommitLog();
        CommitLogPosition reclaimBelow = CommitLog.instance.getCurrentPosition();
        Memtable journalInternal = internal(cfs, bounds, LogDomain.MUTATION_JOURNAL);
        SplitDomainMemtable wrapper = new SplitDomainMemtable(commitLogInternal, journalInternal,
                                                             commitLogInternal.getMemtableId());

        // sanity check
        assertNotEquals(commitLogInternal.mayContainDataBefore(reclaimBelow), journalInternal.mayContainDataBefore(reclaimBelow));

        assertTrue(wrapper.mayContainDataBefore(reclaimBelow));
        assertEquals(commitLogInternal.getCommitLogLowerBound(), wrapper.getCommitLogLowerBound());
    }

    private static void appendToCommitLog()
    {
        String ks = "domain_split_untracked_" + keyspaceNumber.incrementAndGet();
        TableMetadata metadata = TableMetadata.builder(ks, "tbl")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addRegularColumn("v", Int32Type.instance)
                                              .build();
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1, ReplicationType.untracked), metadata);

        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1));
        SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(MutationId.none(), ks, key);
        builder.update(metadata).row().add("v", 1);
        builder.build().apply();
    }

    /**
     * What the memtable pool and the read short-circuit read off a generation. Under-reporting a size lets a generation
     * grow past its flush threshold. Over-reporting a minimum timestamp lets
     * {@code SinglePartitionReadCommand} and {@code CompactionController} skip an sstable that is still needed.
     */
    @Test
    public void accountingCoversBothInternals()
    {
        ColumnFamilyStore cfs = newTrackedTable();
        SplitDomainMemtable wrapper = newWrapper(cfs);
        Memtable commitLogInternal = wrapper.internalFor(LogDomain.COMMIT_LOG);
        Memtable journalInternal = wrapper.internalFor(LogDomain.MUTATION_JOURNAL);

        write(cfs, wrapper, 1, LogDomain.MUTATION_JOURNAL);

        // A clean memtable reports Long.MAX_VALUE, so answering from the commit-log internal alone would report that.
        assertEquals(Long.MAX_VALUE, commitLogInternal.getMinTimestamp());
        assertNotEquals(Long.MAX_VALUE, journalInternal.getMinTimestamp());
        assertEquals(journalInternal.getMinTimestamp(), wrapper.getMinTimestamp());

        write(cfs, commitLogInternal, 2, LogDomain.COMMIT_LOG);

        // One row went to each internal, so a sum differs from either internal's own count.
        assertEquals(2, wrapper.partitionCount());
        assertEquals(2, wrapper.operationCount());
        assertEquals(commitLogInternal.getLiveDataSize() + journalInternal.getLiveDataSize(),
                     wrapper.getLiveDataSize());
        assertTrue("neither internal alone accounts for the size",
                   wrapper.getLiveDataSize() > Math.max(commitLogInternal.getLiveDataSize(),
                                                        journalInternal.getLiveDataSize()));
        assertEquals(Math.min(commitLogInternal.getMinTimestamp(), journalInternal.getMinTimestamp()),
                     wrapper.getMinTimestamp());
        assertEquals(Math.min(commitLogInternal.getMinLocalDeletionTime(), journalInternal.getMinLocalDeletionTime()),
                     wrapper.getMinLocalDeletionTime());
    }

    /**
     * {@code getMinTimestamp} has two kinds of answer and both are numbers: a real timestamp, or
     * {@code NO_MIN_TIMESTAMP} (-1) meaning the memtable has no usable timestamp. {@code SinglePartitionReadCommand} and
     * {@code CompactionController} test for the sentinel before using the value, so a generation answering -1 while it
     * holds timestamped rows makes them skip a comparison they should make.
     *
     * An internal answers -1 when its tracked minimum equals the epoch {@code EncodingStats} substitutes for an update
     * carrying no liveness timestamp, so the test writes that epoch directly. Since -1 sorts below every real timestamp,
     * a plain minimum over the two internals returns it.
     */
    @Test
    public void generationWithOneEpochTimestampInternalStillReportsAMinimum()
    {
        for (LogDomain epochDomain : LogDomain.values())
        {
            ColumnFamilyStore cfs = newTrackedTable();
            SplitDomainMemtable wrapper = newWrapper(cfs);
            LogDomain otherDomain = epochDomain.isJournal() ? LogDomain.COMMIT_LOG : LogDomain.MUTATION_JOURNAL;
            long realTimestamp = FBUtilities.timestampMicros();

            write(cfs, wrapper, 1, epochDomain, EncodingStats.NO_STATS.minTimestamp);
            write(cfs, wrapper, 2, otherDomain, realTimestamp);

            // we expect the internal memtable to report no timestamp
            assertEquals(Memtable.NO_MIN_TIMESTAMP, wrapper.internalFor(epochDomain).getMinTimestamp());
            // but the wrapper needs to report the minimum actual timestamp
            assertEquals(realTimestamp, wrapper.getMinTimestamp());
        }
    }

    /**
     * Check that writes against split memtables are routed to the proper internal memtable, and that the internal
     * memtables are presented to readers as a single memtable
     */
    @Test
    public void writesAndReadsSpanBothDomains()
    {
        ColumnFamilyStore cfs = newTrackedTable();
        SplitDomainMemtable wrapper = newWrapper(cfs);
        Memtable commitLogInternal = wrapper.internalFor(LogDomain.COMMIT_LOG);
        Memtable journalInternal = wrapper.internalFor(LogDomain.MUTATION_JOURNAL);
        assertTrue(wrapper.isInternal(commitLogInternal));
        assertTrue(wrapper.isInternal(journalInternal));
        assertTrue(wrapper.isClean());

        // Handed to the generation rather than to an internal, so put() does the routing.
        write(cfs, wrapper, 1, LogDomain.MUTATION_JOURNAL);
        write(cfs, wrapper, 2, LogDomain.COMMIT_LOG);

        // Each internal took its own domain's row and nothing else.
        assertEquals(Collections.singleton(key(cfs, 1)), partitionKeys(journalInternal));
        assertEquals(Collections.singleton(key(cfs, 2)), partitionKeys(commitLogInternal));
        assertFalse(wrapper.isClean());

        // Every read path answers for both internals, and for neither when the partition is in neither.
        assertEquals(ImmutableSet.of(key(cfs, 1), key(cfs, 2)), partitionKeys(wrapper));
        assertNotNull(wrapper.snapshotPartition(key(cfs, 1)));
        assertNotNull(wrapper.snapshotPartition(key(cfs, 2)));
        assertNotNull(wrapper.rowIterator(key(cfs, 1)));
        assertNotNull(wrapper.rowIterator(key(cfs, 2)));
        assertNull(wrapper.snapshotPartition(key(cfs, 99)));
        assertNull(wrapper.rowIterator(key(cfs, 99)));
        assertEquals(maxToken(commitLogInternal.lastToken(), journalInternal.lastToken()), wrapper.lastToken());
    }

    private static Token maxToken(Token left, Token right)
    {
        return left.compareTo(right) >= 0 ? left : right;
    }

    /**
     * Both internals take the barrier the generation was switched out with, since the generation flushes as one unit.
     * An internal left holding no barrier keeps accepting writes after the switch, and those rows miss the flush they
     * were bounded into.
     */
    @Test
    public void switchingOutGenerationRetiresBothInternals()
    {
        ColumnFamilyStore cfs = newTrackedTable();
        SplitDomainMemtable wrapper = newWrapper(cfs);
        Memtable commitLogInternal = wrapper.internalFor(LogDomain.COMMIT_LOG);
        Memtable journalInternal = wrapper.internalFor(LogDomain.MUTATION_JOURNAL);

        OpOrder.Barrier barrier = Keyspace.writeOrder.newBarrier();
        LogDomainBounds upperBounds = LogDomainBounds.atCurrentPositions();
        upperBounds.seal();
        wrapper.switchOut(barrier, upperBounds);
        barrier.issue();

        // A write that starts after the barrier is refused for either domain, which holds only if both internals took
        // it. An internal with no barrier reports itself as still the newest and accepts everything.
        try (OpOrder.Group after = Keyspace.writeOrder.start())
        {
            for (LogDomain domain : LogDomain.values())
            {
                CommitLogPosition position = upperBounds.get(domain);
                assertFalse(domain + " internal still accepts writes", wrapper.internalFor(domain)
                                                                              .accepts(after, position, domain));
                assertFalse(domain + " write was accepted by the wrapper", wrapper.accepts(after, position, domain));
            }
        }
    }

    /**
     * The constructor's preconditions are what make {@code internalFor} honest. Two internals holding one domain leave
     * the other unroutable, and internals from different stores would put one table's rows in another's flush.
     */
    @Test
    public void wrapperRefusesInternalsThatCannotCoverBothDomains()
    {
        ColumnFamilyStore cfs = newTrackedTable();
        LogDomainBounds bounds = LogDomainBounds.atCurrentPositions();
        Memtable journalInternal = internal(cfs, bounds, LogDomain.MUTATION_JOURNAL);

        Assertions.assertThatThrownBy(() -> new SplitDomainMemtable(journalInternal,
                                                                    internal(cfs, bounds, LogDomain.MUTATION_JOURNAL),
                                                                    journalInternal.getMemtableId()))
                  .describedAs("two journal internals leave the commit log domain unroutable")
                  .isInstanceOf(IllegalArgumentException.class);

        ColumnFamilyStore otherTable = newTrackedTable();
        Assertions.assertThatThrownBy(() -> new SplitDomainMemtable(internal(otherTable, bounds, LogDomain.COMMIT_LOG),
                                                                    journalInternal,
                                                                    journalInternal.getMemtableId()))
                  .describedAs("internals must belong to the same store")
                  .isInstanceOf(IllegalArgumentException.class);
    }

    // An unsplit memtable refuses a write from the other log, because it's bounds are incompatible
    @Test
    public void normalMemtableRefusesForeignDomainWrite()
    {
        ColumnFamilyStore cfs = newTrackedTable();
        Memtable journalInternal = internal(cfs, LogDomainBounds.atCurrentPositions(), LogDomain.MUTATION_JOURNAL);

        TableMetadata metadata = cfs.metadata();
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1));
        MutationId id = MutationTrackingService.instance().nextMutationId(metadata.keyspace, key.getToken());
        SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(id, metadata.keyspace, key);
        builder.update(metadata).row().add("v", 1);
        PartitionUpdate update = builder.build().getPartitionUpdate(metadata);

        Assertions.assertThatThrownBy(() -> {
                      try (OpOrder.Group group = Keyspace.writeOrder.start())
                      {
                          journalInternal.put(id, update, UpdateTransaction.NO_OP, group, LogDomain.COMMIT_LOG);
                      }
                  })
                  .describedAs("a commit-log write must not land in a journal memtable")
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("COMMIT_LOG");
    }

    // ---- Installing one, through Tracker.getMemtableFor --------------------------------------------------------

    /**
     * A tracked table's memtable refuses an untracked write. The memtable generation splits instead of the write
     * failing, and keeps the memtable that was already live as one internal.
     */
    @Test
    public void foreignDomainWriteSplitsTheCurrentMemtableGeneration()
    {
        ColumnFamilyStore cfs = newTrackedTable();
        Memtable before = cfs.getTracker().getView().getCurrentMemtable();
        assertTrue(before.holds(LogDomain.MUTATION_JOURNAL));

        try (OpOrder.Group group = Keyspace.writeOrder.start())
        {
            Memtable selected = cfs.getTracker().getMemtableFor(group,
                                                               CommitLog.instance.getCurrentPosition(),
                                                               LogDomain.COMMIT_LOG);
            assertTrue("selection returns the split generation, which routes on put",
                       selected instanceof SplitDomainMemtable);
            assertTrue(((SplitDomainMemtable) selected).internalFor(LogDomain.COMMIT_LOG).holds(LogDomain.COMMIT_LOG));
        }

        Memtable after = cfs.getTracker().getView().getCurrentMemtable();
        assertTrue(after instanceof SplitDomainMemtable);
        SplitDomainMemtable wrapper = (SplitDomainMemtable) after;
        assertEquals("the memtable already live is kept as the journal internal",
                     before, wrapper.internalFor(LogDomain.MUTATION_JOURNAL));
        assertEquals("and the wrapper inherits its id, so generation ordering is unbroken",
                     before.getMemtableId(), wrapper.getMemtableId());
    }

    @Test
    public void splitGenerationTakesEitherDomainAndRoutesInside()
    {
        // memtable should initially be a normal memtable
        ColumnFamilyStore cfs = newTrackedTable();
        assertFalse(selectGenerationFor(cfs, LogDomain.MUTATION_JOURNAL) instanceof SplitDomainMemtable);

        //... but requesting a memtable should install a split domain memtable
        Memtable installed = selectGenerationFor(cfs, LogDomain.COMMIT_LOG);
        assertTrue(selectGenerationFor(cfs, LogDomain.COMMIT_LOG) instanceof SplitDomainMemtable);

        //... and continue returning it for either domain
        Memtable forCommitLog = selectGenerationFor(cfs, LogDomain.COMMIT_LOG);
        Memtable forJournal = selectGenerationFor(cfs, LogDomain.MUTATION_JOURNAL);

        assertEquals(forCommitLog, forJournal);
        assertEquals(installed, forCommitLog);
        assertEquals(forCommitLog, cfs.getTracker().getView().getCurrentMemtable());
        SplitDomainMemtable wrapper = (SplitDomainMemtable) forCommitLog;

        // writes against one domain shouldn't touch the other domain
        write(cfs, wrapper, 1, LogDomain.MUTATION_JOURNAL);
        assertFalse(wrapper.internalFor(LogDomain.MUTATION_JOURNAL).isClean());
        assertTrue("the commit-log internal must not have taken the journal write",
                   wrapper.internalFor(LogDomain.COMMIT_LOG).isClean());
    }

    /**
     * Retires a generation for routing without flushing it.
     */
    private static void retireForRouting(Memtable memtable)
    {
        OpOrder.Barrier barrier = Keyspace.writeOrder.newBarrier();
        LogDomainBounds upperBounds = LogDomainBounds.unset();
        upperBounds.seal();
        memtable.switchOut(barrier, upperBounds);
        barrier.issue();
    }

    /**
     * Prior to supporting multiple log domains, having no memtable that would accept an incoming write was an error. Now,
     * it can mean that we have an illegal condition OR we need to split the current memtable. This tests that we still
     * throw if none of the memtables will accept the write AND the current memtable holds the domain of the current write
     * we need to split the current memtable.
     */
    @Test
    public void currentMemtableHoldsDomainRefusesThenThrows()
    {
        ColumnFamilyStore cfs = newTrackedTable();
        Memtable current = cfs.getTracker().getView().getCurrentMemtable();
        assertTrue(current.holds(LogDomain.MUTATION_JOURNAL));

        // Retired without a replacement being appended, which no production path does - View.switchMemtable appends
        // first. That leaves a current memtable carrying an issued barrier, so it refuses a write it holds the domain
        // for.
        retireForRouting(current);

        Assertions.assertThatThrownBy(() -> {
                      try (OpOrder.Group group = Keyspace.writeOrder.start())
                      {
                          cfs.getTracker().getMemtableFor(group, MutationJournal.instance().getCurrentPosition(),
                                                          LogDomain.MUTATION_JOURNAL);
                      }
                  })
                  .describedAs("the invariant must be reported, naming the domain the current memtable holds")
                  .isInstanceOf(AssertionError.class)
                  .hasMessageContaining("holds that domain");
    }

    // ---- Flushing one, through ColumnFamilyStore ---------------------------------------------------------------

    @Test
    public void splitGenerationFlushesAnSSTablePerDomain()
    {
        // Can't call getFlushSet on SplitDomainMemtable directly, getFlushSources returns multiple domain memtables
        Assertions.assertThatThrownBy(() -> newWrapper(newTrackedTable()).getFlushSet(null, null))
                  .isInstanceOf(UnsupportedOperationException.class);

        assertFlushOutput(EnumSet.of(SSTableProvenance.MUTATION_JOURNAL, SSTableProvenance.COMMIT_LOG),
                          LogDomain.MUTATION_JOURNAL, LogDomain.COMMIT_LOG);
        assertFlushOutput(EnumSet.of(SSTableProvenance.MUTATION_JOURNAL),
                          LogDomain.MUTATION_JOURNAL);
    }

    private static void assertFlushOutput(Set<SSTableProvenance> expected, LogDomain... dirty)
    {
        ColumnFamilyStore cfs = newTrackedTable();

        Memtable generation = selectGenerationFor(cfs, LogDomain.COMMIT_LOG);
        assertTrue(generation instanceof SplitDomainMemtable);
        int k = 1;
        for (LogDomain domain : dirty)
            write(cfs, generation, k++, domain);
        assertFalse(generation.isClean());

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Map<SSTableProvenance, SSTableReader> byProvenance = new EnumMap<>(SSTableProvenance.class);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertNull("two sstables from the same log", byProvenance.put(SSTableProvenance.of(sstable), sstable));

        assertEquals("one sstable per dirty domain, and none claiming both logs", expected, byProvenance.keySet());
        assertFalse("the generation is gone from the view", cfs.getTracker().getView().liveMemtables.contains(generation));
    }

    private static Memtable dirtySplitGeneration(ColumnFamilyStore cfs)
    {
        Memtable generation = selectGenerationFor(cfs, LogDomain.COMMIT_LOG);
        write(cfs, generation, 1, LogDomain.COMMIT_LOG);
        write(cfs, generation, 2, LogDomain.MUTATION_JOURNAL);
        return generation;
    }

    /**
     * For accord. Accord assumes a single active memtable and observes them directly when determining durability.
     * If the listener fired once per domain memtable, accord would think a memtable was durable before both parts
     * actually were
     */
    @Test
    public void splitDomainMemtableFiresFlushListenerOnce()
    {
        ColumnFamilyStore cfs = newTrackedTable();
        Memtable generation = dirtySplitGeneration(cfs);

        AtomicInteger fired = new AtomicInteger();
        AtomicInteger liveWhenFired = new AtomicInteger(-1);
        Consumer<TableMetadata> registered = generation.ensureFlushListener("durability", () -> metadata -> {
            fired.incrementAndGet();
            liveWhenFired.set(cfs.getLiveSSTables().size());
        });
        assertNotNull(registered);
        assertSame("one listener per generation, not per registration",
                   registered,
                   generation.ensureFlushListener("durability", () -> { throw new AssertionError("built twice"); }));

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        assertEquals("fires exactly once", 1, fired.get());
        assertEquals("both domains' output must be live before it fires", 2, liveWhenFired.get());
        assertNull("a flushed generation refuses new listeners, which is what ensureDurable retries on",
                   generation.ensureFlushListener("later", () -> metadata -> {}));
    }

    /**
     * A flush failure must leave both logs alone, or a segment is marked clean for data that never reached disk. The two
     * log calls and the listener share one {@code flushFailure == null} guard in {@code PostFlush}.
     */
    @Test
    public void flushFailureInOneDomainLeavesTheGenerationUndurable()
    {
        ColumnFamilyStore cfs = newTrackedTable();
        Memtable generation = dirtySplitGeneration(cfs);

        AtomicInteger fired = new AtomicInteger();
        generation.ensureFlushListener("durability", () -> metadata -> fired.incrementAndGet());

        // Flushing.flushRunnables refuses a memtable that already holds a flush transaction, so the journal internal
        // fails where a writer error would: inside Flush.flushMemtable, after the barrier has issued.
        Memtable journalInternal = ((SplitDomainMemtable) generation).internalFor(LogDomain.MUTATION_JOURNAL);
        journalInternal.setFlushTransaction(LifecycleTransaction.offline(OperationType.FLUSH));

        try
        {
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            org.junit.Assert.fail("expected the flush to fail");
        }
        catch (RuntimeException expected)
        {
            // the failure is rethrown by PostFlush
        }

        // A failed flush must leave the generation undurable and publish nothing.
        assertEquals(0, fired.get());
        assertEquals(0, cfs.getLiveSSTables().size());
    }

    /**
     * Checks that the correct log is notified on flush, even if the schema has moved onto a different domain.
     */
    @Test
    public void originatingLogIsNotifiedOnMemtableFlush()
    {
        ColumnFamilyStore cfs = newTrackedTable();
        assertTrue(cfs.metadata().replicationType().isTracked());

        // Split first, so the commit-log internal's lower bound is below the append that follows.
        Memtable generation = selectGenerationFor(cfs, LogDomain.COMMIT_LOG);
        assertTrue(generation instanceof SplitDomainMemtable);

        CommitLog.instance.add(untrackedMutation(cfs, 1));
        assertTrue(commitLogIsDirtyFor(cfs));

        write(cfs, generation, 1, LogDomain.COMMIT_LOG);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        assertFalse(commitLogIsDirtyFor(cfs));
    }

    private static Mutation untrackedMutation(ColumnFamilyStore cfs, int k)
    {
        TableMetadata metadata = cfs.metadata();
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(k));
        SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(MutationId.none(), metadata.keyspace, key);
        builder.update(metadata).row().add("v", k);
        return builder.build();
    }

    private static boolean commitLogIsDirtyFor(ColumnFamilyStore cfs)
    {
        for (CommitLogSegment segment : CommitLog.instance.segmentManager.getActiveSegments())
            if (segment.getDirtyTableIds().contains(cfs.metadata().id))
                return true;
        return false;
    }

    private static Memtable selectGenerationFor(ColumnFamilyStore cfs, LogDomain domain)
    {
        CommitLogPosition position = domain.isJournal() ? MutationJournal.instance().getCurrentPosition()
                                                        : CommitLog.instance.getCurrentPosition();
        try (OpOrder.Group group = Keyspace.writeOrder.start())
        {
            return cfs.getTracker().getMemtableFor(group, position, domain);
        }
    }

}
