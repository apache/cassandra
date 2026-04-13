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

package org.apache.cassandra.service.reads.tracked;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadableView;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SimpleBTreePartition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSource;
import org.apache.cassandra.db.transform.EmptyPartitionsDiscarder;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.Index.IndexMatch;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseablePeekingIterator;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

public class PartialTrackedIndexRead<Match extends IndexMatch, Searcher extends Index.MultiStepSearcher<Match>> extends PartialTrackedRead
{
    private final ReadCommand command;
    private final Searcher searcher;

    private ConsistencyLevel consistencyLevel;
    private Dispatcher.RequestTime requestTime;

    PartialTrackedIndexRead(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos, ReadCommand command, Searcher searcher)
    {
        super(executionController, cfs, startTimeNanos);
        this.command = command;
        this.searcher = searcher;
    }

    public static <Match extends IndexMatch, Searcher extends Index.MultiStepSearcher<Match>> PartialTrackedIndexRead<Match, Searcher> create(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos, ReadCommand command, Searcher searcher)
    {
        PartialTrackedIndexRead<Match, Searcher> read = new PartialTrackedIndexRead<>(executionController, cfs, startTimeNanos, command, searcher);
        read.prepare(null);
        return read;
    }

    @Override
    public ReadCommand command()
    {
        return command;
    }

    @Override
    public Searcher searcher()
    {
        return searcher;
    }

    @Override
    public void setFollowUpReadContext(ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        this.consistencyLevel = consistencyLevel;
        this.requestTime = requestTime;
    }

    public interface CompletedIndexPartitionRead<Match extends IndexMatch>
    {
        UnfilteredRowIterator matchingRows(PeekingIterator<Match> matchIterator);
    }

    public interface CompletedIndexRead<Match extends IndexMatch> extends CompletedRead
    {
        CompletedIndexPartitionRead<Match> partitionRead(DecoratedKey key);
        CloseablePeekingIterator<Match> matchIterator();
    }

    private static DecoratedKey maxKey(DecoratedKey left, DecoratedKey right)
    {
        if (left == null)
            return right;
        if (right == null)
            return left;

        return right.compareTo(left) > 0 ? right : left;
    }

    private static class FollowUpRead<Match extends IndexMatch, Searcher extends Index.MultiStepSearcher<Match>> implements CompletedIndexPartitionRead<Match>, AutoCloseable
    {
        private final DecoratedKey key;
        private final PartialTrackedIndexRead<Match, Searcher> read;
        private final CompletedIndexRead<Match> completedRead;
        private final CompletedIndexPartitionRead<Match> partitionRead;

        public FollowUpRead(DecoratedKey key, PartialTrackedIndexRead<Match, Searcher> read)
        {
            Preconditions.checkArgument(!read.command.isRangeRequest());
            this.key = key;
            this.read = read;
            this.completedRead = (CompletedIndexRead<Match>) read.complete();
            this.partitionRead = Preconditions.checkNotNull(completedRead.partitionRead(key));
        }

        static <Match extends IndexMatch, Searcher extends Index.MultiStepSearcher<Match>> Future<FollowUpRead<Match, Searcher>> start(ReadCommand command, DecoratedKey key, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
        {
            ClusterMetadata metadata = ClusterMetadata.current();
            Preconditions.checkState(command instanceof PartitionRangeReadCommand, "additional reads can only be made with range reads");

            PartitionRangeReadCommand rangeReadCommand = (PartitionRangeReadCommand) command;
            SinglePartitionReadCommand partitionReadCommand = SinglePartitionReadCommand.fromRangeRead(key, rangeReadCommand, rangeReadCommand.limits());

            AsyncPromise<FollowUpRead<Match, Searcher>> followUpPromise = new AsyncPromise<>();
            TrackedRead.Partition trackedRead = TrackedRead.Partition.create(metadata, partitionReadCommand, consistencyLevel, requestTime);

            trackedRead.startLocal(requestTime, null, ((promise1, read, consistencyLevel1, rt) -> {
                try
                {
                    followUpPromise.trySuccess(new FollowUpRead<>(key, (PartialTrackedIndexRead<Match, Searcher>) read));
                }
                catch (Exception e)
                {
                    followUpPromise.tryFailure(e);
                }
            }));
            return followUpPromise;
        }

        @Override
        public UnfilteredRowIterator matchingRows(PeekingIterator<Match> matchIterator)
        {
            Preconditions.checkState(matchIterator.hasNext());
            Preconditions.checkState(matchIterator.peek().key().equals(key));
            return partitionRead.matchingRows(matchIterator);
        }

        @Override
        public void close()
        {
            read.close();
        }

        static <Match extends IndexMatch, Searcher extends Index.MultiStepSearcher<Match>> void close(Map<DecoratedKey, Future<FollowUpRead<Match, Searcher>>> followUpReads)
        {
            for (Future<FollowUpRead<Match, Searcher>> future : followUpReads.values())
            {
                future.addCallback((followup, failure) -> {
                    if (failure != null)
                        followup.close();
                });
            }
        }

        static <Match extends IndexMatch, Searcher extends Index.MultiStepSearcher<Match>> Map<DecoratedKey, FollowUpRead<Match, Searcher>> getResults(Map<DecoratedKey, Future<FollowUpRead<Match, Searcher>>> futures, List<CloseablePeekingIterator<Match>> matchIterators)
        {
            Map<DecoratedKey, FollowUpRead<Match, Searcher>> followupReads = new HashMap<>();
            for (Future<FollowUpRead<Match, Searcher>> future : futures.values())
            {
                try
                {
                    FollowUpRead<Match, Searcher> followUpRead = future.get();
                    matchIterators.add(followUpRead.completedRead.matchIterator());
                    followupReads.put(followUpRead.key, followUpRead);
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e);
                }
                catch (InterruptedException e)
                {
                    throw new UncheckedInterruptedException(e);
                }
            }
            return followupReads;
        }

    }

    private static class SnapshotView implements ReadableView
    {
        final List<SinglePartitionSource> snapshots;
        final List<SSTableReader> sstables;
        private AugmentedPartition augmentedPartition = null;

        public SnapshotView(List<SinglePartitionSource> snapshots, List<SSTableReader> sstables)
        {
            this.snapshots = snapshots;
            this.sstables = sstables;
        }

        public static SnapshotView create(DecoratedKey key, ColumnFamilyStore cfs)
        {
            ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, key));
            return new SnapshotView(MemtableSnapshot.create(key, view.memtables), view.sstables());
        }

        @Override
        public Iterable<? extends UnfilteredSource> memtables()
        {
            return snapshots;
        }

        @Override
        public List<SSTableReader> sstables()
        {
            return sstables;
        }

        public void augment(PartitionUpdate update)
        {
            if (augmentedPartition == null)
            {
                augmentedPartition = new AugmentedPartition(update.partitionKey(), update.metadata());
                snapshots.add(augmentedPartition);
            }

            augmentedPartition.augment(update);
        }
    }

    private static abstract class SinglePartitionSource implements UnfilteredSource
    {
        abstract Partition partition();

        @Override
        public UnfilteredRowIterator rowIterator(DecoratedKey key, Slices slices, ColumnFilter columnFilter, boolean reversed, SSTableReadsListener listener)
        {
            Partition partition = partition();
            Preconditions.checkState(key.equals(partition.partitionKey()));
            return partition.unfilteredIterator(columnFilter, slices, reversed);
        }

        @Override
        public UnfilteredPartitionIterator partitionIterator(ColumnFilter columnFilter, DataRange dataRange, SSTableReadsListener listener)
        {
            throw new IllegalStateException("Range scans not supported");
        }

        @Override
        public long getMinTimestamp()
        {
            return partition().stats().minTimestamp;
        }

        @Override
        public long getMinLocalDeletionTime()
        {
            return partition().stats().minLocalDeletionTime;
        }
    }

    private static class MemtableSnapshot extends SinglePartitionSource
    {
        private final Partition partition;

        public MemtableSnapshot(Partition partition)
        {
            this.partition = partition;
        }

        static List<SinglePartitionSource> create(DecoratedKey key, Iterable<Memtable> memtables)
        {
            List<SinglePartitionSource> snapshots = new ArrayList<>();
            for (Memtable memtable : memtables)
            {
                Partition partition = memtable.snapshotPartition(key);
                if (partition != null)
                    snapshots.add(new MemtableSnapshot(partition));
            }
            return snapshots;
        }

        @Override
        Partition partition()
        {
            return partition;
        }
    }

    private static class AugmentedPartition extends SinglePartitionSource
    {
        private final SimpleBTreePartition data;

        AugmentedPartition(DecoratedKey key, TableMetadata metadata)
        {
            this.data = new SimpleBTreePartition(key, metadata, UpdateTransaction.NO_OP);
        }

        void augment(PartitionUpdate update)
        {
            data.update(update);
        }

        @Override
        Partition partition()
        {
            return data;
        }
    }

    class AugmentableIndexPartitionRead implements CompletedIndexPartitionRead<Match>
    {
        private final DecoratedKey partitionKey;
        private final SnapshotView view;

        AugmentableIndexPartitionRead(DecoratedKey partitionKey, SnapshotView view)
        {
            this.partitionKey = partitionKey;
            this.view = view;
        }

        void augment(PartitionUpdate update)
        {
            Preconditions.checkArgument(update.partitionKey().equals(partitionKey));
            view.augment(update);
        }

        @Override
        public UnfilteredRowIterator matchingRows(PeekingIterator<Match> matchIterator)
        {
            Preconditions.checkArgument(matchIterator.hasNext());
            Preconditions.checkArgument(matchIterator.peek().key().equals(partitionKey));
            return searcher.queryNextMatches(executionController, partitionKey, view, matchIterator);
        }
    }

    AugmentableIndexPartitionRead createRead(DecoratedKey key, ColumnFamilyStore cfs)
    {
        SnapshotView view = SnapshotView.create(key, cfs);
        return new AugmentableIndexPartitionRead(key, view);
    }

    @Override
    protected Prepared prepareInternal(UnfilteredPartitionIterator initialData)
    {
        DecoratedKey maxKey = null;
        SortedMap<DecoratedKey, AugmentableIndexPartitionRead> reads = new TreeMap<>();

        if (command instanceof SinglePartitionReadCommand)
        {
            SinglePartitionReadCommand cmd = (SinglePartitionReadCommand) command;
            DecoratedKey key = cmd.partitionKey();
            AugmentableIndexPartitionRead partitionRead = createRead(key, cfs);
            reads.put(key, partitionRead);
            maxKey = key;
        }

        CloseablePeekingIterator<Match> matchIterator = searcher.matchIterator(executionController);
        try
        {
            SortedSet<Match> materializedMatches = new TreeSet<>(searcher.matchComparator());
            while (matchIterator.hasNext() && materializedMatches.size() < command.limits().count())
            {
                Match match = matchIterator.next();
                materializedMatches.add(match);
                if (!reads.containsKey(match.key()))
                {
                    DecoratedKey key = match.key();
                    maxKey = maxKey(maxKey, key);
                    AugmentableIndexPartitionRead partitionRead = createRead(key, cfs);
                    reads.put(key, partitionRead);
                }
            }
            return new IndexPrepared(maxKey, materializedMatches, matchIterator, reads);
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(matchIterator);
            throw t;
        }
    }

    @Override
    public synchronized void complete(AsyncPromise<TrackedDataResponse> promise, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        Preconditions.checkState(state().isPrepared());
        IndexPrepared prepared = (IndexPrepared) state();

        if (prepared.isCompletable())
        {
            super.complete(promise, consistencyLevel, requestTime);
            return;
        }

        IndexPreComplete preComplete = prepared.preComplete();
        state = preComplete;

        // simple listener - completion will handle any failed futures
        preComplete.future().addListener(() -> super.complete(promise, consistencyLevel, requestTime));
    }

    private abstract class AbstractIndexPrepared extends Prepared
    {
        protected DecoratedKey maxKey;
        protected final SortedSet<Match> materializedMatches;
        // there may be additional matches for keys we've already scanned, this allows us to read them before
        // starting a short read
        protected final CloseablePeekingIterator<Match> additionalMatches;

        protected final SortedMap<DecoratedKey, AugmentableIndexPartitionRead> reads;

        // for range scans, if we learn of new keys with matching contents as part of reconciliation, we need
        // to do follow up reads against them since we didn't snapshot memtable contents for the keys during
        // the prepare phase of the read. Futures for those reads are kept here
        protected final Map<DecoratedKey, Future<FollowUpRead<Match, Searcher>>> followUpReads;

        public AbstractIndexPrepared(DecoratedKey maxKey,
                                     SortedSet<Match> materializedMatches,
                                     CloseablePeekingIterator<Match> additionalMatches,
                                     SortedMap<DecoratedKey, AugmentableIndexPartitionRead> reads,
                                     Map<DecoratedKey, Future<FollowUpRead<Match, Searcher>>> followUpReads)
        {
            this.maxKey = maxKey;
            this.materializedMatches = materializedMatches;
            this.reads = reads;
            this.additionalMatches = additionalMatches;
            this.followUpReads = followUpReads;
        }

        boolean isCompletable()
        {
            return Iterables.all(followUpReads.values(), Future::isDone);
        }

        @Override
        Completed complete()
        {
            Preconditions.checkState(isCompletable());
            List<CloseablePeekingIterator<Match>> matchIterators = new ArrayList<>(followUpReads.size() + 1);
            matchIterators.add(CloseablePeekingIterator.wrap(materializedMatches.iterator()));
            Map<DecoratedKey, FollowUpRead<Match, Searcher>> followUpResults = FollowUpRead.getResults(followUpReads, matchIterators);
            return new IndexCompleted(maxKey, new MergingMatchIterator(matchIterators), additionalMatches, reads, followUpResults);
        }

        @Override
        void close()
        {
            FollowUpRead.close(followUpReads);
            super.close();
        }
    }

    private class IndexPrepared extends AbstractIndexPrepared
    {
        private Index.MultiStepSearcher.MatchIndexer<Match> matchIndexer = null;

        public IndexPrepared(DecoratedKey maxKey, SortedSet<Match> materializedMatches, CloseablePeekingIterator<Match> additionalMatches, SortedMap<DecoratedKey, AugmentableIndexPartitionRead> reads)
        {
            super(maxKey, materializedMatches, additionalMatches, reads, new HashMap<>());
        }

        private Index.MultiStepSearcher.MatchIndexer<Match> matchIndexer()
        {
            if (matchIndexer == null)
                matchIndexer = searcher.matchIndexer();
            return matchIndexer;
        }

        private boolean indexNewKey(PartitionUpdate update)
        {
            AtomicBoolean hasMatches = new AtomicBoolean(false);
            matchIndexer().index(update, e -> hasMatches.set(true));
            return hasMatches.get();
        }

        private boolean indexUpdate(PartitionUpdate update)
        {
            int startingSize = materializedMatches.size();
            matchIndexer().index(update, materializedMatches::add);
            return materializedMatches.size() > startingSize;
        }

        @Override
        public void augment(PartitionUpdate update)
        {
            Preconditions.checkState(consistencyLevel != null,
                                     "PartialTrackedRead#setFollowUpReadContext needs to be called before making reads available for augmenting mutation");
            DecoratedKey key = update.partitionKey();
            AugmentableIndexPartitionRead read = reads.get(key);
            if (read == null)
            {
                // TODO: maybe we should immediately start a follow up read if it's likely this key will be included in the response
                if (!followUpReads.containsKey(key) && indexNewKey(update))
                {
                    maxKey = maxKey(maxKey, update.partitionKey());
                    Future<FollowUpRead<Match, Searcher>> followUpRead = FollowUpRead.start(command, update.partitionKey(), consistencyLevel, requestTime);
                    followUpReads.put(key, followUpRead);
                }
                return;
            }

            read.augment(update);
            indexUpdate(update);
        }

        IndexPreComplete preComplete()
        {
            return new IndexPreComplete(maxKey, materializedMatches, additionalMatches, reads, followUpReads);
        }
    }

    private class IndexPreComplete extends AbstractIndexPrepared
    {
        public IndexPreComplete(DecoratedKey maxKey, SortedSet<Match> materializedMatches, CloseablePeekingIterator<Match> additionalMatches, SortedMap<DecoratedKey, AugmentableIndexPartitionRead> reads, Map<DecoratedKey, Future<FollowUpRead<Match, Searcher>>> followUpReads)
        {
            super(maxKey, materializedMatches, additionalMatches, reads, followUpReads);
        }

        @Override
        public void augment(PartitionUpdate update)
        {
            throw new IllegalStateException("Cannot augment reads pending completion");
        }

        Future<List<FollowUpRead<Match, Searcher>>> future()
        {
            return FutureCombiner.allOf(followUpReads.values());
        }
    }

    private class IndexCompleted extends Completed
    {
        private final DecoratedKey maxKey;
        private final CloseablePeekingIterator<Match> materializedMatchIterator;
        private final CloseablePeekingIterator<Match> additionalMatchIterator;
        private final SortedMap<DecoratedKey, AugmentableIndexPartitionRead> reads;
        private final Map<DecoratedKey, FollowUpRead<Match, Searcher>> followUpReads;

        public IndexCompleted(DecoratedKey maxKey, CloseablePeekingIterator<Match> materializedMatchIterator, CloseablePeekingIterator<Match> additionalMatchIterator, SortedMap<DecoratedKey, AugmentableIndexPartitionRead> reads, Map<DecoratedKey, FollowUpRead<Match, Searcher>> followUpReads)
        {
            this.maxKey = maxKey;
            this.materializedMatchIterator = materializedMatchIterator;
            this.additionalMatchIterator = additionalMatchIterator;
            this.reads = reads;
            this.followUpReads = followUpReads;
        }

        @Override
        protected CompletedRead getResult()
        {
            return new FilteringCompletedIndexRead(maxKey, materializedMatchIterator, additionalMatchIterator, reads, followUpReads);
        }
    }

    protected class MergingMatchIterator extends AbstractIterator<Match>
    {
        private final List<CloseablePeekingIterator<Match>> iterators;
        private Match last;

        public MergingMatchIterator(List<CloseablePeekingIterator<Match>> iterators)
        {
            this.iterators = iterators;
        }

        @Override
        protected Match computeNext()
        {
            int minIdx = -1;
            Match minMatch = null;
            for (int i = 0, mi = iterators.size(); i < mi; i++)
            {
                CloseablePeekingIterator<Match> iterator = iterators.get(i);

                if (last != null)
                    searcher.matchComparator().consumeDuplicates(last, iterator);

                if (!iterator.hasNext())
                    continue;

                if (minMatch == null)
                {
                    minMatch = iterator.peek();
                    minIdx = i;
                    continue;
                }

                Match thisMatch = iterator.peek();
                int cmp = searcher.matchComparator().compare(thisMatch, minMatch);
                if (cmp < 0)
                {
                    minMatch = thisMatch;
                    minIdx = i;
                }
                else if (cmp == 0)
                {
                    // if this iterator equals the current minimum, advance the iterator - we don't merge equal matches
                    iterator.next();
                }
            }

            if (minMatch != null)
            {
                iterators.get(minIdx).next();
                last = minMatch;
                return minMatch;
            }

            return endOfData();
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(iterators);
        }
    }

    /**
     * Merges a materialized iterator and an additional iterator. The additional iterator is meant to be the initial
     * match iterator from the searcher. If we encounter previously unseen keys from the initial match iterator, it
     * means that we're in a short read and need to start a follow-up read, which this iterator signals to the caller
     */
    private class MergingStoppingMatchIterator extends AbstractIterator<Match>
    {
        private final DecoratedKey maxKey;
        private final PeekingIterator<Match> materializedIterator;
        private final CloseablePeekingIterator<Match> additionalIterator;
        private boolean followUpRequired = false;

        public MergingStoppingMatchIterator(DecoratedKey maxKey, Iterator<Match> materializedIterator, CloseablePeekingIterator<Match> additionalIterator)
        {
            this.maxKey = maxKey;
            this.materializedIterator = Iterators.peekingIterator(materializedIterator);
            this.additionalIterator = additionalIterator;
        }

        @Override
        protected Match computeNext()
        {
            if (materializedIterator.hasNext() && additionalIterator.hasNext())
            {
                int cmp = searcher.matchComparator().compare(materializedIterator.peek(), additionalIterator.peek(), true);
                if (cmp == 0)
                {
                    additionalIterator.next();
                    return materializedIterator.next();
                }
                else if (cmp < 0)
                {
                    Match match = materializedIterator.next();
                    searcher.matchComparator().consumeDuplicates(match, additionalIterator);
                    return match;
                }
                else
                {
                    Match match = additionalIterator.next();
                    searcher.matchComparator().consumeDuplicates(match, materializedIterator);

                    DecoratedKey key = match.key();
                    Preconditions.checkArgument(key.compareTo(maxKey) <= 0);
                    return match;
                }
            }

            if (materializedIterator.hasNext())
                return materializedIterator.next();

            if (additionalIterator.hasNext())
            {
                Match match = additionalIterator.next();
                DecoratedKey key = match.key();
                if (key.compareTo(maxKey) > 0)
                {
                    Preconditions.checkArgument(command.isRangeRequest());
                    followUpRequired = true;
                    return endOfData();
                }
                return match;
            }

            return endOfData();
        }

        @Override
        public void close()
        {
            additionalIterator.close();
        }
    }

    private class FilteringCompletedIndexRead extends ExtendingCompletedRead implements CompletedIndexRead<Match>
    {
        private final DecoratedKey maxKey;
        private final MergingStoppingMatchIterator matchIterator;
        private final SortedMap<DecoratedKey, AugmentableIndexPartitionRead> reads;

        final Map<DecoratedKey, FollowUpRead<Match, Searcher>> followUpReads;

        public FilteringCompletedIndexRead(DecoratedKey maxKey, CloseablePeekingIterator<Match> materializedMatches, CloseablePeekingIterator<Match> additionalMatches, SortedMap<DecoratedKey, AugmentableIndexPartitionRead> reads, Map<DecoratedKey, FollowUpRead<Match, Searcher>> followupReads)
        {
            super(command, materializedMatches.hasNext(), true);
            this.maxKey = maxKey;
            this.matchIterator = new MergingStoppingMatchIterator(maxKey, materializedMatches, additionalMatches);
            this.reads = reads;
            this.followUpReads = followupReads;
        }

        @Override
        public CloseablePeekingIterator<Match> matchIterator()
        {
            return matchIterator;
        }

        @Override
        ReadCommand command()
        {
            return command;
        }

        @Override
        protected AbstractBounds<PartitionPosition> followUpBounds()
        {
            Preconditions.checkState(command.isRangeRequest());
            Preconditions.checkNotNull(maxKey);
            AbstractBounds<PartitionPosition> bounds = command.dataRange().keyRange();
            return bounds.inclusiveRight()
                   ? new Range<>(maxKey, bounds.right)
                   : new ExcludingBounds<>(maxKey, bounds.right);
        }

        private class UnfilteredResultIterator extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
        {
            private final PeekingIterator<Match> matchIter;

            public UnfilteredResultIterator(PeekingIterator<Match> matchIter)
            {
                this.matchIter = matchIter;
            }

            @Override
            public TableMetadata metadata()
            {
                return command.metadata();
            }

            @Override
            protected UnfilteredRowIterator computeNext()
            {
                for (;;)
                {
                    if (!matchIter.hasNext())
                        return endOfData();

                    DecoratedKey nextKey = matchIter.peek().key();
                    AugmentableIndexPartitionRead read = reads.get(nextKey);
                    if (read != null)
                        return read.matchingRows(matchIter);

                    FollowUpRead<Match, Searcher> followUpRead = followUpReads.get(nextKey);
                    if (followUpRead == null)
                        throw new IllegalStateException("Received match for key without initial or followup read: " + ByteBufferUtil.bytesToHex(nextKey.getKey()));

                    UnfilteredRowIterator next = followUpRead.matchingRows(matchIter);
                    if (next != null)
                        return next;
                }
            }

            @Override
            public void close()
            {
                // match iterator will be closed by FilteringCompletedIndexRead
            }
        }

        private PartitionIterator filter(UnfilteredPartitionIterator iterator)
        {
            iterator = searcher.filterCompletedRead(iterator);
            iterator = command.completeTrackedRead(iterator, PartialTrackedIndexRead.this);
            PartitionIterator filtered = UnfilteredPartitionIterators.filter(iterator, command.nowInSec());
            PartitionIterator counted = Transformation.apply(filtered, mergedResultCounter);
            PartitionIterator result = Transformation.apply(counted, new EmptyPartitionsDiscarder());
            return result;
        }

        @Override
        public TrackedDataResponse response()
        {
            try (UnfilteredResultIterator iterator = new UnfilteredResultIterator(matchIterator))
            {
                PartitionIterator filtered = filter(iterator);
                return TrackedDataResponse.create(filtered, command.columnFilter());
            }
        }

        @Override
        protected boolean followUpRequired()
        {
            if (!command.isRangeRequest())
                return false;
            return matchIterator.followUpRequired || super.followUpRequired();
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(matchIterator);
            FileUtils.closeQuietly(followUpReads.values());
        }

        @Override
        public CompletedIndexPartitionRead<Match> partitionRead(DecoratedKey key)
        {
            return reads.get(key);
        }
    }
}
