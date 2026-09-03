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

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LogDomain;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * One logical memtable holding a separate internal memtable per {@link LogDomain}.
 *
 * A table takes writes from both logs while part of its token range is migrating, and a position from one log does not
 * compare against a bound from the other. Splitting at the memtable means each write is bounded, flushed and accounted
 * for against its own log, without any consumer of a bound having to ask which log it came from.
 *
 * The internal memtables always flush together and a common set of flush listeners are used.
 */
public class SplitDomainMemtable implements Memtable
{
    private final Memtable commitLogInternal;
    private final Memtable journalInternal;
    private final List<Memtable> internals;
    private final long id;
    private final AtomicReference<LifecycleTransaction> flushTransaction = new AtomicReference<>(null);
    private final FlushListeners listeners = new FlushListeners();

    public SplitDomainMemtable(Memtable left, Memtable right, long id)
    {
        Preconditions.checkArgument(left.owner() == right.owner());
        if (left.holds(LogDomain.COMMIT_LOG))
        {
            Preconditions.checkArgument(right.holds(LogDomain.MUTATION_JOURNAL));
            this.commitLogInternal = left;
            this.journalInternal = right;
        }
        else
        {
            Preconditions.checkArgument(left.holds(LogDomain.MUTATION_JOURNAL));
            Preconditions.checkArgument(right.holds(LogDomain.COMMIT_LOG));
            this.commitLogInternal = right;
            this.journalInternal = left;
        }
        this.internals = ImmutableList.of(commitLogInternal, journalInternal);
        this.id = id;
    }

    @Override
    public List<Memtable> flushSources()
    {
        return internals;
    }

    public Memtable internalFor(LogDomain domain)
    {
        return domain.isJournal() ? journalInternal : commitLogInternal;
    }

    public boolean isInternal(Memtable memtable)
    {
        return memtable == commitLogInternal || memtable == journalInternal;
    }

    @Override
    public boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition, LogDomain domain)
    {
        return internalFor(domain).accepts(opGroup, commitLogPosition, domain);
    }

    @Override
    public boolean holds(LogDomain domain)
    {
        return true;
    }

    @Override
    public Owner owner()
    {
        // One store created both internals, which is asserted on construction
        return commitLogInternal.owner();
    }

    @Override
    public boolean allocatesFromMemtablePool()
    {
        return commitLogInternal.allocatesFromMemtablePool() || journalInternal.allocatesFromMemtablePool();
    }

    @Override
    public void flushIfPeriodExpired()
    {
        commitLogInternal.flushIfPeriodExpired();
        journalInternal.flushIfPeriodExpired();
    }

    @Override
    public long put(MutationId mutationId, PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup, LogDomain domain, boolean assumeMissing)
    {
        return internalFor(domain).put(mutationId, update, indexer, opGroup, domain, assumeMissing);
    }

    @Override
    public long partitionCount()
    {
        return commitLogInternal.partitionCount() + journalInternal.partitionCount();
    }

    @Override
    public long getLiveDataSize()
    {
        return commitLogInternal.getLiveDataSize() + journalInternal.getLiveDataSize();
    }

    @Override
    public long operationCount()
    {
        return commitLogInternal.operationCount() + journalInternal.operationCount();
    }

    @Override
    public void addMemoryUsageTo(MemoryUsage usage)
    {
        commitLogInternal.addMemoryUsageTo(usage);
        journalInternal.addMemoryUsageTo(usage);
    }

    @Override
    public long getMinTimestamp()
    {
        long commitLog = commitLogInternal.getMinTimestamp();
        long journal = journalInternal.getMinTimestamp();
        if (commitLog == NO_MIN_TIMESTAMP)
            return journal;
        if (journal == NO_MIN_TIMESTAMP)
            return commitLog;
        return Math.min(commitLog, journal);
    }

    @Override
    public long getMinLocalDeletionTime()
    {
        return Math.min(commitLogInternal.getMinLocalDeletionTime(), journalInternal.getMinLocalDeletionTime());
    }

    /**
     * Conjoined rather than taken from either internal. Accord's durability path reads this and reports a command store
     * durable without registering a flush listener when it is true, so reporting clean while the other internal still
     * holds unflushed data would declare a transaction durable that is not.
     */
    @Override
    public boolean isClean()
    {
        return commitLogInternal.isClean() && journalInternal.isClean();
    }

    @Override
    public void discard()
    {
        commitLogInternal.discard();
        journalInternal.discard();
    }

    @Override
    public void metadataUpdated()
    {
        commitLogInternal.metadataUpdated();
        journalInternal.metadataUpdated();
    }

    @Override
    public void localRangesUpdated()
    {
        commitLogInternal.localRangesUpdated();
        journalInternal.localRangesUpdated();
    }

    @Override
    public void performSnapshot(String snapshotName)
    {
        commitLogInternal.performSnapshot(snapshotName);
        journalInternal.performSnapshot(snapshotName);
    }

    @Override
    public boolean shouldSwitch(ColumnFamilyStore.FlushReason reason, TableMetadata latest)
    {
        // Either internal wanting to switch switches the whole generation, since they flush together.
        return commitLogInternal.shouldSwitch(reason, latest) || journalInternal.shouldSwitch(reason, latest);
    }

    @Override
    public void markExtraOnHeapUsed(long additionalSpace, OpOrder.Group opGroup)
    {
        commitLogInternal.markExtraOnHeapUsed(additionalSpace, opGroup);
    }

    @Override
    public void markExtraOffHeapUsed(long additionalSpace, OpOrder.Group opGroup)
    {
        commitLogInternal.markExtraOffHeapUsed(additionalSpace, opGroup);
    }

    @Override
    public long getMemtableId()
    {
        return id;
    }

    @Override
    public TableMetadata metadata()
    {
        return commitLogInternal.metadata();
    }

    @Override
    public LifecycleTransaction getFlushTransaction()
    {
        return flushTransaction.get();
    }

    @Override
    public LifecycleTransaction setFlushTransaction(LifecycleTransaction transaction)
    {
        return flushTransaction.getAndSet(transaction);
    }

    /**
     * Held here rather than on an internal, so one listener exists per logical generation and fires once both internals
     * are durable. A listener registered on an internal would fire on a partial generation.
     */
    @Override
    public <T extends Consumer<TableMetadata>> T ensureFlushListener(Object key, Supplier<T> factory)
    {
        return listeners.ensureFlushListener(key, factory);
    }

    @Override
    public void notifyFlushed()
    {
        listeners.notifyFlushed(metadata());
    }

    // Commit-log-only accessors

    /**
     * The commit-log internal's value, not an aggregate. Its only consumer is commit log segment reclamation, via
     * {@link ColumnFamilyStore#forceFlush(CommitLogPosition)} from
     * {@code AbstractCommitLogSegmentManager}, and only that internal can hold commit-log-derived rows.
     *
     * Aggregating would be wrong rather than merely redundant: the field is initialized from
     * {@code CommitLog.instance.getCurrentPosition()} on every memtable regardless of domain, so the journal internal's
     * value describes when it was created, not what it holds. Folding it in reports commit log data the memtable does
     * not have and pins segments that could be recycled.
     */
    @Override
    public CommitLogPosition getApproximateCommitLogLowerBound()
    {
        return commitLogInternal.getApproximateCommitLogLowerBound();
    }

    /** Answered by the commit-log internal alone, for the reason on {@link #getApproximateCommitLogLowerBound}. */
    @Override
    public boolean mayContainDataBefore(CommitLogPosition position)
    {
        return commitLogInternal.mayContainDataBefore(position);
    }

    @Override
    public CommitLogPosition getCommitLogLowerBound()
    {
        return commitLogInternal.getCommitLogLowerBound();
    }

    @Override
    public LastCommitLogPosition getFinalCommitLogUpperBound()
    {
        return commitLogInternal.getFinalCommitLogUpperBound();
    }

    /**
     * Both internals take the same boundary and the same barrier, and each reads the position for its own domain from
     * it. One barrier, because the generation flushes as a whole; see the class javadoc.
     */
    @Override
    public void switchOut(OpOrder.Barrier writeBarrier, LogDomainBounds upperBounds)
    {
        commitLogInternal.switchOut(writeBarrier, upperBounds);
        journalInternal.switchOut(writeBarrier, upperBounds);
    }

    @Override
    public FlushablePartitionSet<?> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        throw new UnsupportedOperationException("Flush iterates flushSources(), so that each output carries one domain's bounds");
    }

    @Override
    public Partition snapshotPartition(DecoratedKey key)
    {
        Partition fromCommitLog = commitLogInternal.snapshotPartition(key);
        Partition fromJournal = journalInternal.snapshotPartition(key);

        if (fromCommitLog == null || fromCommitLog.isEmpty())
            return fromJournal;
        if (fromJournal == null || fromJournal.isEmpty())
            return fromCommitLog;

        try (UnfilteredRowIterator merged = UnfilteredRowIterators.merge(ImmutableList.of(fromCommitLog.unfilteredIterator(),
                                                                                         fromJournal.unfilteredIterator())))
        {
            return ImmutableBTreePartition.create(merged);
        }
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key,
                                             Slices slices,
                                             ColumnFilter columnFilter,
                                             boolean reversed,
                                             SSTableReadsListener listener)
    {
        UnfilteredRowIterator fromCommitLog = commitLogInternal.rowIterator(key, slices, columnFilter, reversed, listener);
        UnfilteredRowIterator fromJournal = journalInternal.rowIterator(key, slices, columnFilter, reversed, listener);
        return mergeRows(fromCommitLog, fromJournal);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key)
    {
        return mergeRows(commitLogInternal.rowIterator(key), journalInternal.rowIterator(key));
    }

    private static UnfilteredRowIterator mergeRows(UnfilteredRowIterator fromCommitLog, UnfilteredRowIterator fromJournal)
    {
        if (fromCommitLog == null)
            return fromJournal;
        if (fromJournal == null)
            return fromCommitLog;
        return UnfilteredRowIterators.merge(ImmutableList.of(fromCommitLog, fromJournal));
    }

    @Override
    public UnfilteredPartitionIterator partitionIterator(ColumnFilter columnFilter,
                                                         DataRange dataRange,
                                                         SSTableReadsListener listener)
    {
        return UnfilteredPartitionIterators.merge(ImmutableList.of(commitLogInternal.partitionIterator(columnFilter, dataRange, listener),
                                                                   journalInternal.partitionIterator(columnFilter, dataRange, listener)),
                                                  UnfilteredPartitionIterators.MergeListener.NOOP);
    }

    @Override
    public Token lastToken()
    {
        Token fromCommitLog = commitLogInternal.lastToken();
        Token fromJournal = journalInternal.lastToken();

        if (fromCommitLog == null)
            return fromJournal;
        if (fromJournal == null)
            return fromCommitLog;
        return fromCommitLog.compareTo(fromJournal) >= 0 ? fromCommitLog : fromJournal;
    }

    @Override
    public String toString()
    {
        return "DomainSplitMemtable(id=" + id + ", commitLog=" + commitLogInternal + ", journal=" + journalInternal + ')';
    }
}
