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

package accord.local;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.AsyncExecutorFactory;
import accord.api.AsyncExecutor;
import accord.topology.EpochReady;
import accord.api.DataStore;
import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.local.CommandStore.EpochUpdateHolder;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.IndexedQuadConsumer;
import accord.utils.IndexedRangeQuadConsumer;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.Reduce;
import accord.utils.SearchableRangeList;
import accord.utils.LargeBitSet;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.Cancellable;
import accord.utils.async.NestedAsyncResult;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Int2ObjectHashMap;

import static accord.topology.EpochReady.done;
import static accord.api.DataStore.FetchKind.Sync;
import static accord.local.CommandStores.BootstrapRangeAction.BOOTSTRAP_NOT_NEEDED;
import static accord.local.CommandStores.BootstrapRangeAction.SAFE_BOOTSTRAP;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.illegalState;
import static java.util.stream.Collectors.toList;

/**
 * Manages the single threaded metadata shards
 */
public abstract class CommandStores implements AsyncExecutorFactory
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CommandStores.class);

    static final Iterator<CommandStore> INVALID = new Iterator<>()
    {
        @Override public boolean hasNext() { throw new UnsupportedOperationException(); }
        @Override public CommandStore next() { throw new UnsupportedOperationException(); }
    };

    public interface LatentStoreSelector
    {
        StoreSelector refine(TxnId txnId, @Nullable Timestamp executeAt, Participants<?> participants);

        class StandardLatentStoreSelector implements LatentStoreSelector
        {
            private static final StandardLatentStoreSelector INSTANCE = new StandardLatentStoreSelector();

            @Override
            public StoreSelector refine(TxnId txnId, @Nullable Timestamp executeAt, Participants<?> participants)
            {
                return snapshot -> StoreFinder.find(snapshot, participants)
                                              .filter(snapshot, participants, txnId.epoch(), (executeAt != null ? executeAt : txnId).epoch())
                                              .iterator(snapshot);
            }
        }

        static LatentStoreSelector standard()
        {
            return StandardLatentStoreSelector.INSTANCE;
        }
    }

    public interface StoreSelector extends LatentStoreSelector
    {
        default StoreSelector refine(TxnId txnId, @Nullable Timestamp executeAt, Participants<?> participants) { return this; }
        Iterator<CommandStore> select(Snapshot snapshot);
    }

    public static class IncludingSpecificStoreSelector implements StoreSelector
    {
        final int storeId;

        public IncludingSpecificStoreSelector(int storeId)
        {
            this.storeId = storeId;
        }

        @Override
        public StoreSelector refine(TxnId txnId, @Nullable Timestamp executeAt, Participants<?> participants)
        {
            return snapshot -> {
                StoreFinder finder = StoreFinder.find(snapshot, participants)
                                                .filter(snapshot, participants, txnId.epoch(), (executeAt != null ? executeAt : txnId).epoch());
                finder.set(snapshot.byId.get(storeId));
                return finder.iterator(snapshot);
            };
        }

        @Override
        public Iterator<CommandStore> select(Snapshot snapshot)
        {
            return Collections.singletonList(snapshot.byId(storeId)).iterator();
        }
    }

    // TODO (required): as we get more tables this will become expensive to allocate; we need to index first by prefix
    public static class StoreFinder extends LargeBitSet implements IndexedQuadConsumer<Object, Object, Object, Object>, IndexedRangeQuadConsumer<Object, Object, Object, Object>
    {
        final int[] indexMap;
        private boolean invalid;

        private StoreFinder(int size, int[] indexMap)
        {
            super(size);
            this.indexMap = indexMap;
        }

        public StoreFinder(Snapshot snapshot)
        {
            this(snapshot.shards.length, snapshot.indexForRange);
        }

        public static StoreSelector selector(Unseekables<?> unseekables, long minEpoch, long maxEpoch)
        {
            return snapshot -> {
                StoreFinder finder = StoreFinder.find(snapshot, unseekables);
                finder.filter(snapshot, unseekables, minEpoch, maxEpoch);
                return finder.iterator(snapshot);
            };
        }

        public static StoreFinder find(Snapshot snapshot, Unseekables<?> unseekables)
        {
            StoreFinder finder = new StoreFinder(snapshot);
            switch (unseekables.domain())
            {
                default: throw new UnhandledEnum(unseekables.domain());
                case Range:
                {
                    int minIndex = 0;
                    for (Range range : (AbstractRanges)unseekables)
                        minIndex = snapshot.lookupByRange.forEachRange(range, finder, finder, null, null, null, null, minIndex);
                    break;
                }
                case Key:
                {
                    int minIndex = 0;
                    for (RoutingKey key : (AbstractUnseekableKeys)unseekables)
                        minIndex = snapshot.lookupByRange.forEachKey(key, finder, finder, null, null, null, null, minIndex);
                    break;
                }
            }
            return finder;
        }

        public StoreFinder filter(Snapshot snapshot, Unseekables<?> unseekables, long minEpoch, long maxEpoch)
        {
            for (int i = firstSetBit(); i >= 0 ; i = nextSetBit(i + 1, -1))
            {
                ShardHolder shard = snapshot.shards[i];
                Ranges shardRanges = shard.ranges().allBetween(minEpoch, maxEpoch);
                if (shardRanges != shard.ranges.all() && !shardRanges.intersects(unseekables))
                {
                    unset(i);
                }
                else if (unsafelyTouchesRegainedRanges(snapshot, shard, unseekables, minEpoch))
                {
                    invalid = true;
                    break;
                }
            }
            return this;
        }

        public Iterator<CommandStore> iterator(Snapshot snapshot)
        {
            if (invalid)
                return INVALID;

            return new Iterator<>()
            {
                int i = firstSetBit();
                @Override
                public boolean hasNext()
                {
                    return i >= 0;
                }

                @Override
                public CommandStore next()
                {
                    CommandStore next = snapshot.shards[i].store;
                    i = nextSetBit(i + 1, -1);
                    return next;
                }
            };
        }

        @Override
        public void accept(Object p1, Object p2, Object p3, Object p4, int index)
        {
            set(indexMap[index]);
        }

        @Override
        public void accept(Object p1, Object p2, Object p3, Object p4, int fromIndex, int toIndex)
        {
            for (int i = fromIndex ; i < toIndex ; ++i)
                set(indexMap[i]);
        }
    }

    public interface Factory
    {
        CommandStores create(NodeCommandStoreService node,
                             Agent agent,
                             DataStore store,
                             RandomSource random,
                             Journal journal,
                             ShardDistributor shardDistributor,
                             ProgressLog.Factory progressLogFactory,
                             LocalListeners.Factory listenersFactory);
    }

    private static class StoreSupplier
    {
        private final NodeCommandStoreService node;
        private final Agent agent;
        private final DataStore store;
        private final ProgressLog.Factory progressLogFactory;
        private final LocalListeners.Factory listenersFactory;
        private final CommandStore.Factory shardFactory;
        private final RandomSource random;
        private final Journal journal;

        StoreSupplier(NodeCommandStoreService node, Agent agent, DataStore store, RandomSource random, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, CommandStore.Factory shardFactory, Journal journal)
        {
            this.node = node;
            this.agent = agent;
            this.store = store;
            this.random = random;
            this.progressLogFactory = progressLogFactory;
            this.listenersFactory = listenersFactory;
            this.shardFactory = shardFactory;
            this.journal = journal;
        }

        CommandStore create(int id, EpochUpdateHolder rangesForEpoch)
        {
            return shardFactory.create(id, node, agent, this.store, progressLogFactory, listenersFactory, rangesForEpoch, journal);
        }
    }

    public static class ShardHolder
    {
        public final CommandStore store;
        @Nullable final Ranges regainsRanges;
        RangesForEpoch ranges;

        ShardHolder(CommandStore store, @Nullable Ranges regainsRanges)
        {
            this.store = store;
            this.regainsRanges = regainsRanges;
        }

        public ShardHolder(CommandStore store, RangesForEpoch ranges, @Nullable Ranges regainsRanges)
        {
            this.store = store;
            this.regainsRanges = regainsRanges;
            this.ranges = ranges;
        }

        public RangesForEpoch ranges()
        {
            return ranges;
        }

        boolean filter(long minEpoch, long maxEpoch, Unseekables<?> unseekables)
        {
            Ranges shardRanges = ranges.allBetween(minEpoch, maxEpoch);
            return shardRanges != ranges.all() && !shardRanges.intersects(unseekables);
        }

        public String toString()
        {
            return store.id() + " " + ranges;
        }
    }

    public interface RangesForEpochSupplier
    {
        RangesForEpoch ranges();
    }

    public static final class PreviouslyOwned
    {
        public static final PreviouslyOwned EMPTY = new PreviouslyOwned(0, RangesForEpoch.EMPTY.epochs, RangesForEpoch.EMPTY.ranges);
        final long maxEpoch;
        final long[] epochs; // the epoch upon which it was last owned
        final Ranges[] ranges;

        public PreviouslyOwned(long maxEpoch, long[] epochs, Ranges[] ranges)
        {
            this.maxEpoch = maxEpoch;
            this.epochs = epochs;
            this.ranges = ranges;
        }

        PreviouslyOwned prepend(long epoch, Ranges ranges)
        {
            Invariants.require(epochs.length == 0 || epoch > epochs[0]);
            long[] newEpochs = new long[this.epochs.length + 1];
            Ranges[] newRanges = new Ranges[epochs.length];
            newEpochs[0] = epoch;
            newRanges[0] = ranges;
            System.arraycopy(this.epochs, 0, newEpochs, 1, this.epochs.length);
            System.arraycopy(this.ranges, 0, newRanges, 1, this.ranges.length);
            return new PreviouslyOwned(epoch, newEpochs, newRanges);
        }

        public boolean overlaps(long epoch, Unseekables<?> test)
        {
            if (epoch > maxEpoch)
                return false;

            for (int i = 0 ; i < epochs.length && epoch <= epochs[i] ; ++i)
            {
                if (this.ranges[i].intersects(test))
                    return true;
            }

            return false;
        }

        public Ranges regains(Ranges overlapping)
        {
            Ranges regains = Ranges.EMPTY;
            for (Ranges rs : this.ranges)
                regains = regains.without(rs.slice(overlapping, Minimal));
            return regains;
        }

        public int size()
        {
            return epochs.length;
        }

        public long epochs(int i)
        {
            return epochs[i];
        }

        public Ranges ranges(int i)
        {
            return ranges[i];
        }
    }

    // We ONLY remove ranges to keep logic manageable; likely to only merge CommandStores into a new CommandStore via some kind of Bootstrap
    public static class RangesForEpoch
    {
        public static final RangesForEpoch EMPTY = new RangesForEpoch(new long[0], new Ranges[0]);

        final long[] epochs;
        final Ranges[] ranges;

        public RangesForEpoch(long epoch, Ranges ranges)
        {
            this.epochs = new long[] { epoch };
            this.ranges = new Ranges[] { ranges };
        }

        public RangesForEpoch(long[] epochs, Ranges[] ranges)
        {
            Invariants.require(epochs.length == ranges.length);
            this.epochs = epochs;
            this.ranges = ranges;
        }

        public int size()
        {
            return epochs.length;
        }

        public void forEach(BiConsumer<Long, Ranges> forEach)
        {
            for (int i = 0; i < epochs.length; i++)
                forEach.accept(epochs[i], ranges[i]);
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            RangesForEpoch that = (RangesForEpoch) object;
            return Arrays.equals(epochs, that.epochs) && Arrays.equals(ranges, that.ranges);
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        public RangesForEpoch withRanges(long epoch, Ranges latestRanges)
        {
            Invariants.requireArgument(epochs.length == 0 || epochs[epochs.length - 1] <= epoch);
            int newLength = epochs.length == 0 || epochs[epochs.length - 1] < epoch ? epochs.length + 1 : epochs.length;
            long[] newEpochs = Arrays.copyOf(epochs, newLength);
            Ranges[] newRanges = Arrays.copyOf(ranges, newLength);
            newEpochs[newLength - 1] = epoch;
            newRanges[newLength - 1] = latestRanges;
            Invariants.require(newEpochs[newLength - 1] == 0 || newEpochs[newLength - 1] == epoch, "Attempted to override historic epoch %d with %d", newEpochs[newLength - 1], epoch);
            return new RangesForEpoch(newEpochs, newRanges);
        }

        public @Nonnull Ranges coordinates(TxnId txnId)
        {
            return allAt(txnId);
        }

        public @Nonnull Ranges allAt(Timestamp at)
        {
            return allAt(at.epoch());
        }

        public @Nonnull Ranges allAt(long epoch)
        {
            int i = floorIndex(epoch);
            if (i < 0) return Ranges.EMPTY;
            return ranges[i];
        }

        /**
         * Extend a previously computed set of Ranges that included {@code fromInclusive}
         * to include ranges up to {@code toInclusive}
         */
        public @Nonnull Ranges extend(Ranges extend, long curFrom, long curTo, long extendFrom, long extendTo)
        {
            if (extend.isEmpty()) // this captures the case where curTo < epochs[0]
                return allBetween(extendFrom, extendTo);

            if (extendFrom >= curFrom)
                return extend;

            int startCurIndex = floorIndex(curFrom);
            int startExtendIndex = Math.max(0, floorIndex(extendFrom));
            if (startCurIndex <= startExtendIndex)
                return extend;

            return ranges[startExtendIndex];
        }

        public @Nonnull Ranges allBetween(long fromInclusive, EpochSupplier toInclusive)
        {
            return allBetween(fromInclusive, toInclusive.epoch());
        }

        public @Nonnull Ranges allBetween(long fromInclusive, long toInclusive)
        {
            if (fromInclusive > toInclusive)
                throw new IndexOutOfBoundsException();

            int since = floorIndex(fromInclusive);
            if (since >= 0) return ranges[since];

            int to = floorIndex(toInclusive);
            if (to >= 0) return ranges[0];
            return Ranges.EMPTY;
        }

        public @Nonnull Ranges all()
        {
            return ranges[0];
        }

        public @Nonnull Ranges notRetired(SafeCommandStore safeStore)
        {
            return safeStore.redundantBefore().removeRetired(ranges[0]);
        }

        public @Nonnull Ranges allBefore(long toExclusive)
        {
            int to = ceilIndex(toExclusive);
            return to <= 0 ? Ranges.EMPTY : ranges[0];
        }

        public @Nonnull Ranges allUntil(long toInclusive)
        {
            int to = floorIndex(toInclusive);
            return to < 0 ? Ranges.EMPTY : ranges[0];
        }

        public @Nonnull Ranges allSince(long fromInclusive)
        {
            int since = floorIndex(fromInclusive);
            return ranges[Math.max(since, 0)];
        }

        public Ranges rangesAtIndex(int index)
        {
            return ranges[index];
        }

        public long epochAtIndex(int index)
        {
            return epochs[index];
        }

        public int floorIndex(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = -2 - i;
            return i;
        }

        private int ceilIndex(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = -1 - i;
            return i;
        }

        public int indexOffset(long lowEpoch, long highEpoch)
        {
            if (lowEpoch == highEpoch)
                return 0;

            int lowIndex = Math.max(0, floorIndex(lowEpoch));
            int highIndex = lowIndex;
            while (highIndex + 1 < epochs.length && epochs[highIndex + 1] <= highEpoch)
                ++highIndex;
            return highIndex - lowIndex;
        }

        public @Nonnull Ranges currentRanges()
        {
            return ranges[ranges.length - 1];
        }

        public String toString()
        {
            return IntStream.range(0, ranges.length).mapToObj(i -> epochs[i] + ": " + ranges[i])
                            .collect(Collectors.joining(", "));
        }

        public long earliestLaterEpochThatFullyCovers(long sinceEpoch, Unseekables<?> keysOrRanges)
        {
            return Math.max(sinceEpoch, epochs[0]);
        }

        public long latestEarlierEpochThatFullyCovers(long beforeEpoch, Unseekables<?> keysOrRanges)
        {
            int i = ceilIndex(beforeEpoch);
            if (i == 0)
                return beforeEpoch;

            long latest = beforeEpoch;
            Ranges existing = Ranges.EMPTY;
            long next = beforeEpoch;
            if (i < epochs.length)
            {
                existing = ranges[i];
                next = Math.min(next, epochs[i]);
            }
            while (--i >= 0)
            {
                if (ranges[i].without(existing).intersects(keysOrRanges))
                    latest = next - 1;
                existing = existing.with(ranges[i]);
                next = epochs[i];
            }
            return latest;
        }

        public Ranges removed(long presentIn, long removedByInclusive)
        {
            int i = Math.max(1, floorIndex(presentIn));
            int maxi = 1 + floorIndex(removedByInclusive);
            Ranges removed = Ranges.EMPTY;
            while (i < maxi)
            {
                removed = removed.with(ranges[i - 1].without(ranges[i]));
                ++i;
            }
            return removed;
        }
    }

    protected void loadSnapshot(Snapshot toLoad)
    {
        current = toLoad;
    }

    public static class Snapshot extends Journal.TopologyUpdate implements Iterable<ShardHolder>
    {
        public final Topology local;
        final ShardHolder[] shards;
        final Int2IntHashMap byId;
        private final int[] indexForRange;
        final SearchableRangeList lookupByRange;

        public Snapshot(ShardHolder[] shards, Topology local, Topology global, PreviouslyOwned previouslyOwned)
        {
            super(asMap(shards), global, previouslyOwned);
            this.local = local;
            this.shards = shards;
            this.byId = new Int2IntHashMap(shards.length, Hashing.DEFAULT_LOAD_FACTOR, -1);
            int count = 0;
            int prevId = -1;
            for (int i = 0 ; i < shards.length ; ++i)
            {
                ShardHolder shard = shards[i];
                int id = shard.store.id;
                Invariants.require(id > prevId);
                byId.put(id, i);
                count += shard.ranges.all().size();
                prevId = id;
            }
            class RangeAndIndex
            {
                final Range range;
                final int index;

                RangeAndIndex(Range range, int index)
                {
                    this.range = range;
                    this.index = index;
                }
            }
            RangeAndIndex[] rangesAndIndexes = new RangeAndIndex[count];
            count = 0;
            for (int i = 0; i < shards.length ; ++i)
            {
                Ranges add = shards[i].ranges.all();
                for (Range range : add)
                    rangesAndIndexes[count++] = new RangeAndIndex(range, i);
            }

            Arrays.sort(rangesAndIndexes, (a, b) -> a.range.compareTo(b.range));

            Range[] ranges = new Range[count];
            indexForRange = new int[count];
            for (int i = 0 ; i < rangesAndIndexes.length ; ++i)
            {
                ranges[i] = rangesAndIndexes[i].range;
                indexForRange[i] = rangesAndIndexes[i].index;
            }
            lookupByRange = SearchableRangeList.build(ranges);
        }

        // This method exists to ensure we do not hold references to command stores
        public Journal.TopologyUpdate asTopologyUpdate()
        {
            return new Journal.TopologyUpdate(commandStores, global, previouslyOwned);
        }

        private static Int2ObjectHashMap<CommandStores.RangesForEpoch> asMap(ShardHolder[] shards)
        {
            Int2ObjectHashMap<CommandStores.RangesForEpoch> commandStores = new Int2ObjectHashMap<>();
            for (ShardHolder shard : shards)
                commandStores.put(shard.store.id, shard.ranges);
            return commandStores;
        }

        public CommandStore byId(int id)
        {
            return shards[byId.get(id)].store;
        }

        @Override
        public Iterator<ShardHolder> iterator()
        {
            return Arrays.asList(shards).iterator();
        }
    }

    final StoreSupplier supplier;
    final ShardDistributor shardDistributor;
    final Journal journal;
    volatile Snapshot current;
    int nextId;

    private CommandStores(StoreSupplier supplier, ShardDistributor shardDistributor, Journal journal)
    {
        this.supplier = supplier;
        this.shardDistributor = shardDistributor;

        this.current = new Snapshot(new ShardHolder[0], Topology.EMPTY, Topology.EMPTY, PreviouslyOwned.EMPTY);
        this.journal = journal;
    }

    public CommandStores(NodeCommandStoreService time, Agent agent, DataStore store, RandomSource random, Journal journal, ShardDistributor shardDistributor,
                         ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, CommandStore.Factory shardFactory)
    {
        this(new StoreSupplier(time, agent, store, random, progressLogFactory, listenersFactory, shardFactory, journal), shardDistributor, journal);
    }

    public Node.Id nodeId()
    {
        return supplier.node.id();
    }

    public Topology local()
    {
        return current.local;
    }

    public DataStore dataStore()
    {
        return supplier.store;
    }

    static class TopologyUpdate
    {
        final Snapshot snapshot;
        final Supplier<EpochReady> bootstrap;

        TopologyUpdate(Snapshot snapshot, Supplier<EpochReady> bootstrap)
        {
            this.snapshot = snapshot;
            this.bootstrap = bootstrap;
        }
    }

    public enum BootstrapRangeAction
    {
        BOOTSTRAP_NOT_NEEDED, SAFE_BOOTSTRAP, UNSAFE_BOOTSTRAP
    }

    protected BootstrapRangeAction shouldBootstrap(Node node, Topology prevGlobal, Topology newLocalTopology, Range add)
    {
        if (newLocalTopology.epoch() == 1 || !prevGlobal.ranges().contains(add))
            return BOOTSTRAP_NOT_NEEDED;

        return SAFE_BOOTSTRAP;
    }

    public AsyncResult<Void> rebootstrap(Node node)
    {
        List<AsyncResult<EpochReady>> results = new ArrayList<>();
        Snapshot snapshot = current;
        for (ShardHolder shard : snapshot.shards)
            results.add(shard.store.startUnsafeBootstrap(node, shard.ranges.all(), snapshot.global.epoch(), Sync));
        return AsyncResults.allOf(results).flatMap(list -> {
            return AsyncChains.reduce(list.stream()
                                             .flatMap(b -> Stream.of(b.reads.chain(), b.coordinate.chain()))
                                             .collect(Collectors.toList()),
                                      Reduce.toNull()).beginAsResult();
        });
    }

    private synchronized TopologyUpdate updateTopology(Node node, Snapshot prev, Topology newTopology)
    {
        Invariants.requireArgument(!newTopology.isSubset(), "Use full topology for CommandStores.updateTopology");

        long epoch = newTopology.epoch();
        if (epoch <= prev.global.epoch())
        {
            Invariants.require(node.isReplaying(), "Received topology with epoch %d <= %d, but we are not replaying", epoch, prev.global.epoch());
            return new TopologyUpdate(prev, () -> done(epoch));
        }

        Topology newLocalTopology = newTopology.forNode(supplier.node.id()).trim();
        Ranges addedGlobal = newTopology.ranges().without(prev.global.ranges());
        node.addNewRangesToDurableBefore(addedGlobal, epoch);

        Ranges added = newLocalTopology.ranges().without(prev.local.ranges());
        Ranges subtracted = prev.local.ranges().without(newLocalTopology.ranges());

        List<Supplier<EpochReady>> bootstrapUpdates = new ArrayList<>();
        List<ShardHolder> result = new ArrayList<>(prev.shards.length + added.size());
        PreviouslyOwned previouslyOwned = prev.previouslyOwned;

        for (ShardHolder shard : prev.shards)
        {
            Ranges current = shard.ranges().currentRanges();
            Ranges removeRanges = subtracted.slice(current, Minimal);
            if (!removeRanges.isEmpty())
            {
                // TODO (required): This is updating the a non-volatile field in the previous Snapshot, why modify it at all, even with volatile the guaranteed visibility is weak even with mutual exclusion
                shard.ranges = shard.ranges().withRanges(newTopology.epoch(), current.without(subtracted));
                shard.store.epochUpdateHolder.remove(epoch, shard.ranges, removeRanges);

                bootstrapUpdates.add(shard.store.unbootstrap(epoch, removeRanges));
            }

            Ranges regainedRanges = shard.ranges().all().slice(added, Minimal);
            if (!regainedRanges.isEmpty())
                bootstrapUpdates.add(() -> EpochReady.all(epoch, shard.store.markPermanentlyUnsafeToRead(regainedRanges).beginAsResult()));

            // TODO (desired): only sync affected shards
            Ranges ranges = shard.ranges().currentRanges();
            // ranges can be empty when ranges are lost or consolidated across epochs.
            if (epoch > 1 && requiresSync(ranges, prev.global, newTopology))
            {
                logger.debug("Epoch {} requires visibility sync for {}", epoch, ranges);
                bootstrapUpdates.add(shard.store.refreshReadyToCoordinate(node, ranges, epoch));
            }

            result.add(shard);
        }

        if (!added.isEmpty())
        {
            logger.info("Epoch {} adding {} to local command stores", epoch, added);
            for (Ranges addRanges : shardDistributor.split(added))
            {
                EpochUpdateHolder updateHolder = new EpochUpdateHolder();
                RangesForEpoch rangesForEpoch = new RangesForEpoch(epoch, addRanges);
                updateHolder.add(epoch, rangesForEpoch, addRanges);
                ShardHolder shard = new ShardHolder(supplier.create(nextId++, updateHolder), previouslyOwned.regains(addRanges));
                shard.ranges = rangesForEpoch;

                Map<BootstrapRangeAction, Ranges> partitioned = addRanges.partitioningBy(range -> shouldBootstrap(node, prev.global, newLocalTopology, range), BootstrapRangeAction.class);
                for (Map.Entry<BootstrapRangeAction, Ranges> entry : partitioned.entrySet())
                {
                    BootstrapRangeAction action = entry.getKey();
                    bootstrapUpdates.add(shard.store.bootstrapper(node, entry.getValue(), newLocalTopology.epoch(), action));
                }
                result.add(shard);
            }
        }

        Supplier<EpochReady> bootstrap;
        if (bootstrapUpdates.isEmpty())
        {
            logger.debug("Epoch {} implies no change to local command stores", epoch);
            bootstrap = () -> done(epoch);
        }
        else
        {
            if (!subtracted.isEmpty())
                logger.info("Epoch {} removes {} from local command stores", epoch, subtracted);

            bootstrap = () -> {
                List<EpochReady> list = bootstrapUpdates.stream().map(Supplier::get).collect(toList());
                return new EpochReady(epoch,
                                      AsyncResults.debuggableReduce(Lists.transform(list, EpochReady::active), Reduce.toNull()),
                                      AsyncResults.debuggableReduce(Lists.transform(list, EpochReady::coordinate), Reduce.toNull()),
                                      AsyncResults.debuggableReduce(Lists.transform(list, EpochReady::data), Reduce.toNull()),
                                      AsyncResults.debuggableReduce(Lists.transform(list, EpochReady::reads), Reduce.toNull())
                );
            };
        }

        if (!subtracted.isEmpty())
            previouslyOwned = previouslyOwned.prepend(epoch - 1, subtracted);

        return new TopologyUpdate(new Snapshot(result.toArray(new ShardHolder[0]), newLocalTopology, newTopology, previouslyOwned), bootstrap);
    }

    private static boolean requiresSync(Ranges ranges, Topology oldTopology, Topology newTopology)
    {
        List<Shard> oldShards = oldTopology.foldl(ranges, (oldShard, shards, i) -> {
            shards.add(oldShard);
            return shards;
        }, new ArrayList<>());

        List<Shard> newShards = newTopology.foldl(ranges, (newShard, shards, i) -> {
            shards.add(newShard);
            return shards;
        }, new ArrayList<>());

        if (oldShards.size() != newShards.size())
            return true;

        for (int i = 0 ; i < oldShards.size() ; ++i)
        {
            Shard oldShard = oldShards.get(i);
            Shard newShard = newShards.get(i);
            if (!oldShard.notInFastPath.equals(newShard.notInFastPath))
                return true;

            if (!newShard.nodes.equals(oldShard.nodes))
                return true;
        }
        return false;
    }

    public void forAllUnsafe(Consumer<CommandStore> forEach)
    {
        Snapshot snapshot = current;
        for (ShardHolder shard : snapshot.shards)
            forEach.accept(shard.store);
    }

    public AsyncChain<Void> forAll(String reason, Consumer<SafeCommandStore> forEach)
    {
        return mapReduce(snapshot -> Stream.of(snapshot.shards).map(shard -> shard.store).iterator(), new MapReduceCommandStores<>(RoutingKeys.EMPTY)
        {
            @Override public Void reduce(Void o1, Void o2) { return null; }
            @Override public TxnId primaryTxnId() { return null; }
            @Override public String reason() { return reason; }
            @Override
            protected Void applyInternal(SafeCommandStore safeStore)
            {
                forEach.accept(safeStore);
                return null;
            }
        });
    }

    public AsyncChain<Void> forEach(String reason, Participants<?> participants, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(reason, null, participants, minEpoch, maxEpoch, forEach);
    }

    public AsyncChain<Void> forEach(String reason, TxnId txnId, Participants<?> participants, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(reason, txnId, participants, LoadKeys.SYNC, LoadKeysFor.READ_WRITE,  minEpoch, maxEpoch, forEach);
    }

    public AsyncChain<Void> forEach(String reason, TxnId txnId, Participants<?> participants, LoadKeys loadKeys, LoadKeysFor loadKeysFor, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return mapReduce(StoreFinder.selector(participants, minEpoch, maxEpoch), new MapReduceCommandStores<Participants<?>, Void>(participants)
        {
            @Override public LoadKeys loadKeys() { return loadKeys;}
            @Override public LoadKeysFor loadKeysFor() { return loadKeysFor; }
            @Override public Void reduce(Void o1, Void o2) { return null; }
            @Override public TxnId primaryTxnId() { return txnId; }
            @Override public String reason() { return reason; }
            @Override
            protected Void applyInternal(SafeCommandStore safeStore)
            {
                forEach.accept(safeStore);
                return null;
            }
        });
    }

    public <O> Cancellable mapReduceConsume(long minEpoch, long maxEpoch, MapReduceConsumeCommandStores<?, O> mapReduceConsume)
    {
        AsyncChain<O> reduced = mapReduce(StoreFinder.selector(mapReduceConsume.scope, minEpoch, maxEpoch), mapReduceConsume);
        return reduced.begin(mapReduceConsume);
    }

    /**
     * Maybe asynchronously, {@code apply} the function to each applicable {@code CommandStore}, invoke {@code reduce}
     * on pairs of responses until only one remains, then {@code accept} the result.
     *
     * Note that {@code reduce} and {@code accept} are invoked by only one thread, and never concurrently with {@code apply},
     * so they do not require mutual exclusion.
     */
    public  <O> Cancellable mapReduceConsume(StoreSelector selector, MapReduceConsumeCommandStores<?, O> mapReduceConsume)
    {
        AsyncChain<O> reduced = mapReduce(selector, mapReduceConsume);
        return reduced.begin(mapReduceConsume);
    }

    public <O> Cancellable mapReduceConsume(IntStream commandStoreIds, MapReduceConsumeCommandStores<?, O> mapReduceConsume)
    {
        AsyncChain<O> reduced = mapReduce(commandStoreIds, mapReduceConsume);
        return reduced.begin(mapReduceConsume);
    }

    public <O> AsyncChain<O> mapReduce(IntStream commandStoreIds, MapReduceCommandStores<?, O> mapReduce)
    {
        return mapReduce(snapshot -> commandStoreIds.mapToObj(snapshot::byId).iterator(), mapReduce);
    }

    public <O> AsyncChain<O> mapReduce(StoreSelector selector, MapReduceCommandStores<?, O> mapReduceConsume)
    {
        Snapshot snapshot = current;
        Iterator<CommandStore> stores = selector.select(snapshot);
        if (stores == INVALID)
            return AsyncChains.failure(new OverlappingCommandStoresException());

        AsyncChain<O> chain = null;
        while (stores.hasNext())
        {
            CommandStore store = stores.next();
            AsyncChain<O> next = mapReduceConsume.applyAsync(store);
            if (next != null)
                chain = chain != null ? AsyncChains.reduce(chain, next, mapReduceConsume) : next;
        }

        return chain == null ? AsyncChains.success(null) : chain;
    }

    private static boolean unsafelyTouchesRegainedRanges(Snapshot snapshot, ShardHolder shard, Unseekables<?> unseekables, long minEpoch)
    {
        if (shard.regainsRanges == null)
            return false;

        unseekables = unseekables.slice(shard.regainsRanges, Minimal);
        if (unseekables.isEmpty())
            return false;

        return snapshot.previouslyOwned.overlaps(minEpoch, unseekables);
    }

    /**
     * Initialize topology from snapshot on boot.
     */
    public synchronized void initializeTopologyUnsafe(Journal.TopologyUpdate update)
    {
        Invariants.require(current.global.epoch() == 0);
        ShardHolder[] shards = new ShardHolder[update.commandStores.size()];
        int i = 0;
        int maxId = -1;
        for (Map.Entry<Integer, RangesForEpoch> e : update.commandStores.entrySet())
        {
            RangesForEpoch rfe = e.getValue();
            Invariants.require(rfe != null);
            EpochUpdateHolder holder = new EpochUpdateHolder();
            holder.add(1, rfe, rfe.all());
            shards[i++] = new ShardHolder(supplier.create(e.getKey(), holder), rfe, update.previouslyOwned.regains(rfe.all()));
            maxId = Math.max(maxId, e.getKey());
        }
        Arrays.sort(shards, Comparator.comparingInt(shard -> shard.store.id));

        nextId = maxId + 1;
        loadSnapshot(new Snapshot(shards, update.global.forNode(supplier.node.id()).trim(), update.global, update.previouslyOwned));
    }

    public synchronized void resetTopology(Journal.TopologyUpdate update)
    {
        Snapshot current = this.current;
        Invariants.require(update.global.epoch() == current.local.epoch());
        ShardHolder[] shards = new ShardHolder[current.commandStores.size()];
        int maxId = -1;
        for (Map.Entry<Integer, RangesForEpoch> e : update.commandStores.entrySet())
        {
            int storeId = e.getKey();
            RangesForEpoch rfe = e.getValue();
            Invariants.require(rfe != null);
            ShardHolder shard = new ShardHolder(current.byId(storeId), rfe, update.previouslyOwned.regains(rfe.all()));
            EpochUpdateHolder holder = shard.store.epochUpdateHolder;
            rfe.forEach(new BiConsumer<>()
            {
                RangesForEpoch accumulator = null;
                Ranges prev = null;
                public void accept(Long epoch, Ranges ranges)
                {
                    if (accumulator == null)
                        accumulator = new RangesForEpoch(epoch, ranges);
                    else
                        accumulator = accumulator.withRanges(epoch, ranges);

                    Ranges additions = ranges;
                    Ranges removals = Ranges.EMPTY;
                    if (prev != null)
                    {
                        additions = ranges.without(prev);
                        removals = prev.without(ranges);
                    }

                    if (!additions.isEmpty())
                        holder.add(epoch, accumulator, additions);
                    if (!removals.isEmpty())
                        holder.remove(epoch, accumulator, removals);
                    shard.store.unsafeUpdateRangesForEpoch();
                    prev = ranges;
                }
            });

            shards[storeId] = shard;
            maxId = Math.max(maxId, storeId);
        }

        nextId = maxId + 1;
        loadSnapshot(new Snapshot(shards, current.local, current.global, update.previouslyOwned));
    }

    public synchronized Supplier<EpochReady> updateTopology(Node node, Topology newTopology)
    {
        TopologyUpdate update = updateTopology(node, current, newTopology);
        if (update.snapshot != current)
        {
            AsyncResults.SettableResult<Void> flush = new AsyncResults.SettableWithDescription<>("Write Topology To Journal");
            journal.saveTopology(update.snapshot.asTopologyUpdate(), () -> flush.setSuccess(null));
            current = update.snapshot;
            return () -> {
                EpochReady ready = update.bootstrap.get();
                return new EpochReady(ready.epoch,
                                      ready.active,
                                      NestedAsyncResult.flatMap(flush, ignore -> ready.coordinate),
                                      NestedAsyncResult.flatMap(flush, ignore -> ready.data),
                                      NestedAsyncResult.flatMap(flush, ignore -> ready.reads)
                );
            };
        }
        return update.bootstrap;
    }

    public void shutdown()
    {
        for (ShardHolder shard : current.shards)
            shard.store.shutdown();
    }

    @Override
    public AsyncExecutor someExecutor()
    {
        return someSequentialExecutor();
    }

    @Override
    public SequentialAsyncExecutor someSequentialExecutor()
    {
        return any();
    }

    @VisibleForTesting
    public CommandStore any()
    {
        ShardHolder[] shards = current.shards;
        if (shards.length == 0) throw illegalState("Unable to get CommandStore; non defined");
        return shards[supplier.random.nextInt(shards.length)].store;
    }

    public CommandStore[] all()
    {
        ShardHolder[] shards = current.shards;
        CommandStore[] all = new CommandStore[shards.length];
        for (int i = 0; i < shards.length; i++)
            all[i] = shards[i].store;
        return all;
    }

    public CommandStore forId(int id)
    {
        Snapshot snapshot = current;
        return snapshot.shards[snapshot.byId.get(id)].store;
    }

    public int[] ids()
    {
        ShardHolder[] shards = current.shards;
        int[] ids = new int[shards.length];
        for (int i = 0; i < ids.length; i++)
            ids[i] = shards[i].store.id;
        Arrays.sort(ids);
        return ids;
    }

    public int count()
    {
        return current.shards.length;
    }

    public ShardDistributor shardDistributor()
    {
        return shardDistributor;
    }

    @VisibleForTesting
    public CommandStore unsafeForKey(RoutingKey key)
    {
        ShardHolder[] shards = current.shards;
        for (ShardHolder shard : shards)
        {
            if (shard.ranges().currentRanges().contains(key))
                return shard.store;
        }
        throw new IllegalArgumentException();
    }

    protected Snapshot current()
    {
        return current;
    }
}
