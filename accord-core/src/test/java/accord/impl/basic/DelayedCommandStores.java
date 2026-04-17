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

package accord.impl.basic;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.collect.ImmutableSortedMap;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.impl.InMemoryCommandStore;
import accord.impl.InMemoryCommandStores;
import accord.impl.InMemorySafeCommand;
import accord.impl.InMemorySafeCommandsForKey;
import accord.impl.PrefixedIntHashKey;
import accord.impl.basic.TaskExecutorService.Task;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommandStore;
import accord.local.ShardDistributor;
import accord.local.cfk.CommandsForKey;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import static accord.api.Journal.CommandUpdate;
import static accord.utils.Invariants.Paranoia.LINEAR;
import static accord.utils.Invariants.ParanoiaCostFactor.HIGH;

public class DelayedCommandStores extends InMemoryCommandStores.SingleThread
{
    private DelayedCommandStores(NodeCommandStoreService time, Agent agent, DataStore store, RandomSource random, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, SimulatedDelayedExecutorService executorService, CacheLoading isLoadedCheck, Journal journal)
    {
        super(time, agent, store, random, journal, shardDistributor, progressLogFactory, listenersFactory, DelayedCommandStore.factory(executorService, isLoadedCheck));
    }

    public static CommandStores.Factory factory(PendingQueue pending, CacheLoading isLoadedCheck)
    {
        return (time, agent, store, random, journal, shardDistributor, progressLogFactory, listenersFactory) ->
               new DelayedCommandStores(time, agent, store, random, shardDistributor, progressLogFactory, listenersFactory, new SimulatedDelayedExecutorService(pending, agent, time.id()), isLoadedCheck, journal);
    }

    public void validateShardStateForTesting(Journal.TopologyUpdate lastUpdate)
    {
        PreviouslyOwned previouslyOwned = lastUpdate.previouslyOwned;
        ShardHolder[] shards = new ShardHolder[lastUpdate.commandStores.size()];
        int i = 0;
        for (Map.Entry<Integer, RangesForEpoch> e : lastUpdate.commandStores.entrySet())
        {
            Snapshot current = current();
            RangesForEpoch ranges = e.getValue();
            CommandStore commandStore = null;
            for (ShardHolder shard : current)
            {
                if (shard.ranges().equals(ranges))
                {
                    Invariants.require(commandStore == null);
                    commandStore = shard.store;
                }
            }
            Invariants.nonNull(commandStore, "Each set of ranges should have a corresponding command store, but %d did not:(%s)",
                               ranges, Arrays.toString(shards))
                      .restore();

            ShardHolder shard = new ShardHolder(commandStore, ranges, previouslyOwned.regains(ranges.all()));
            shards[i++] = shard;
        }
        Arrays.sort(shards, Comparator.comparingInt(shard -> shard.store.id()));

        loadSnapshot(new Snapshot(shards, lastUpdate.global.forNode(nodeId()).trim(), lastUpdate.global, lastUpdate.previouslyOwned));
    }

    protected void loadSnapshot(Snapshot nextSnapshot)
    {
        Snapshot current = current();
        // These checks are only applicable to delayed command stores.
        for (ShardHolder shard : current)
        {
            CommandStore prev = current.byId(shard.store.id());
            CommandStore next = nextSnapshot.byId(shard.store.id());
            {
                RedundantBefore orig = prev.unsafeGetRedundantBefore();
                RedundantBefore loaded = next.unsafeGetRedundantBefore();
                Invariants.require(orig.equals(loaded), "%s should equal %s", loaded, orig);
            }

            {
                NavigableMap<TxnId, Ranges> orig = prev.unsafeGetBootstrapBeganAt();
                NavigableMap<TxnId, Ranges> loaded = next.unsafeGetBootstrapBeganAt();
                Invariants.require(orig.equals(loaded), "%s should equal %s", loaded, orig);
            }

            {
                NavigableMap<Timestamp, Ranges> orig = prev.unsafeGetSafeToRead();
                NavigableMap<Timestamp, Ranges> loaded = next.unsafeGetSafeToRead();
                Invariants.require(orig.equals(loaded), "%s should equal %s", loaded, orig);
            }

            {
                RangesForEpoch orig = prev.unsafeGetRangesForEpoch();
                RangesForEpoch loaded = next.unsafeGetRangesForEpoch();
                Invariants.require(orig.equals(loaded), "%s should equal %s", loaded, orig);
            }
        }

        super.loadSnapshot(nextSnapshot);
    }

    private static boolean contains(Topology previous, int searchPrefix)
    {
        for (Range range : previous.ranges())
        {
            int prefix = ((PrefixedIntHashKey) range.start()).prefix;
            if (prefix == searchPrefix)
                return true;
        }
        return false;
    }

    public static class DelayedCommandStore extends InMemoryCommandStore
    {
        public class DelayedTask<T> extends Task<T>
        {
            private DelayedTask(Callable<T> call)
            {
                super(call);
            }

            private DelayedTask(Callable<T> call, Pending origin)
            {
                super(call, origin);
            }

            public DelayedCommandStore owner()
            {
                return DelayedCommandStore.this;
            }

            @Override
            public void run()
            {
                Invariants.require(active == null);
                Invariants.require(activeThread == null);
                active = this;
                activeThread = Thread.currentThread();
                try
                {
                    super.run();
                }
                finally
                {
                    Invariants.require(active == this);
                    Invariants.require(activeThread == Thread.currentThread());
                    active = null;
                    activeThread = null;
                }
            }

            public Callable<T> callable()
            {
                return callable;
            }

            public String toString()
            {
                return callable.toString();
            }
        }

        private final SimulatedDelayedExecutorService executor;
        private final Queue<Task<?>> pending = new ArrayDeque<>();
        private final CacheLoading cacheLoading;
        private final Journal journal;
        private Task<?> active;
        private Thread activeThread;

        public DelayedCommandStore(int id, NodeCommandStoreService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder, SimulatedDelayedExecutorService executor, CacheLoading cacheLoading, Journal journal)
        {
            super(id, time, agent, store, progressLogFactory, listenersFactory, epochUpdateHolder, journal);
            this.executor = executor;
            this.cacheLoading = cacheLoading;
            this.journal = journal;
            restore();
        }

        protected void loadRedundantBefore(RedundantBefore redundantBefore)
        {
            if (redundantBefore == null)
            {
                Invariants.require(unsafeGetRedundantBefore().size() == 0);
            }
            else
            {
                unsafeClearRedundantBefore();
                super.loadRedundantBefore(redundantBefore);
            }
        }

        protected void loadBootstrapBeganAt(NavigableMap<TxnId, Ranges> bootstrapBeganAt)
        {
            unsafeClearBootstrapBeganAt();
            if (bootstrapBeganAt == null) bootstrapBeganAt = ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY);
            super.loadBootstrapBeganAt(bootstrapBeganAt);
        }

        protected void loadSafeToRead(NavigableMap<Timestamp, Ranges> safeToRead)
        {
            unsafeClearSafeToRead();
            if (safeToRead == null) safeToRead = ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY);
            super.loadSafeToRead(safeToRead);
        }

        @Override
        protected void loadRangesForEpoch(RangesForEpoch newRangesForEpoch)
        {
            if (newRangesForEpoch == null) Invariants.require(super.rangesForEpoch == null);
            else
            {
                unsafeClearRangesForEpoch();
                super.loadRangesForEpoch(newRangesForEpoch);
            }
        }

        public void restore()
        {
            loadRangesForEpoch(journal.loadRangesForEpoch(id()));
            loadRedundantBefore(journal.loadRedundantBefore(id()));
            loadBootstrapBeganAt(journal.loadBootstrapBeganAt(id()));
            loadSafeToRead(journal.loadSafeToRead(id()));
        }

        @Override
        public void onRead(Command current)
        {
            cacheLoading.validateRead(this, current);
        }

        @Override
        public void onWrite(Command current)
        {
            cacheLoading.validateWrite(this, current);
        }

        @Override
        public void onRead(CommandsForKey current)
        {
            cacheLoading.validateRead(this, current);
        }

        @Override
        protected boolean canExposeUnloaded()
        {
            return !cacheLoading.cacheEmpty();
        }

        private static CommandStore.Factory factory(SimulatedDelayedExecutorService executor, CacheLoading isLoadedCheck)
        {
            return (id, node, agent, store, progressLogFactory, listenersFactory, rangesForEpoch, journal) -> new DelayedCommandStore(id, node, agent, store, progressLogFactory, listenersFactory, rangesForEpoch, executor, isLoadedCheck, journal);
        }

        @Override
        public boolean inStore()
        {
            return Thread.currentThread() == activeThread;
        }

        @Override
        public AsyncChain<Void> chain(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return submit(newTask(context, i -> { consumer.accept(i); return null; }));
        }

        @Override
        public <T> AsyncChain<T> chain(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return submit(newTask(context, function));
        }

        @Override
        public <T> AsyncChain<T> chain(Callable<T> call)
        {
            return submit(new DelayedTask<>(call));
        }

        @Override
        public void execute(Runnable run)
        {
            execute(new DelayedTask<>(() -> {
                run.run();
                return null;
            }));
        }

        private void execute(DelayedTask<?> task)
        {
            boolean wasEmpty = pending.isEmpty();
            executor.preregister(task);
            pending.add(task);
            if (wasEmpty)
                runNextTask();
        }

        private <T> DelayedTask<T> newTask(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            Pending origin = Pending.Global.activeOrigin();
            if (RecurringPendingRunnable.isRecurring(origin) && context.primaryTxnId() != null && !context.primaryTxnId().isSystemTxn())
                origin = null;
            return new DelayedTask<>(() -> executeInContext(this, context, function), origin);
        }

        private <T> AsyncChain<T> submit(DelayedTask<T> task)
        {
            if (Invariants.testParanoia(LINEAR, LINEAR, HIGH))
            {
                return AsyncChains.detectLeak(agent::onException, () -> {
                    boolean wasEmpty = pending.isEmpty();
                    pending.add(task);
                    if (wasEmpty)
                        runNextTask();
                }).flatMap(ignore -> task.chain());
            }
            else
            {
                return new AsyncChains.Head<>()
                {
                    @Override
                    protected Cancellable start(BiConsumer<? super T, Throwable> callback)
                    {
                        execute(task);
                        task.invoke(callback);
                        return () -> {
                            if (pending.peek() != task)
                            {
                                pending.remove(task);
                                executor.cancel(task);
                                task.cancel(false);
                            }
                        };
                    }
                };
            }
        }

        private void runNextTask()
        {
            Task<?> next = pending.peek();
            if (next == null)
                return;

            next.invoke(this.agent()); // used to track unexpected exceptions and notify simulations
            next.invoke(this::afterExecution);
            executor.executePreregistered(next);
        }

        private void afterExecution()
        {
            pending.poll();
            runNextTask();
        }

        @Override
        public void shutdown()
        {

        }

        @Override
        protected InMemorySafeStore createSafeStore(PreLoadContext context, RangesForEpoch ranges, Map<TxnId, InMemorySafeCommand> commands, Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKeys)
        {
            return new DelayedSafeStore(this, ranges, context, commands, commandsForKeys, cacheLoading);
        }
    }

    static int counter = 0;
    public static class DelayedSafeStore extends InMemoryCommandStore.InMemorySafeStore
    {
        private final DelayedCommandStore commandStore;
        private final CacheLoading cacheLoading;

        public DelayedSafeStore(DelayedCommandStore commandStore,
                                RangesForEpoch ranges,
                                PreLoadContext context,
                                Map<TxnId, InMemorySafeCommand> commands,
                                Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey,
                                CacheLoading cacheLoading)
        {
            super(commandStore, ranges, context, commands, commandsForKey);
            this.commandStore = commandStore;
            this.cacheLoading = cacheLoading;
            ++counter;
        }

        @Override
        public void postExecute()
        {
            persistFieldUpdates();
            commands.entrySet().forEach(e -> {
                InMemorySafeCommand safe = e.getValue();
                if (!safe.isModified()) return;

                Command before = safe.original();
                Command after = safe.current();
                commandStore.journal.saveCommand(commandStore.id(), new CommandUpdate(before, after), () -> {});
                commandStore.onWrite(safe.current());
            });
            super.postExecute();
        }

        @Override
        protected void persistFieldUpdates()
        {
            Journal.FieldUpdates fieldUpdates = fieldUpdates();
            if (fieldUpdates != null)
                commandStore.journal.saveStoreState(commandStore.id(), fieldUpdates, () -> {});
            super.persistFieldUpdates();
        }

        @Override
        protected InMemoryCommandStore.InMemoryCommandStoreCaches tryGetCaches()
        {
            if (!cacheLoading.cacheEmpty())
            {
                boolean cacheFull = cacheLoading.cacheFull();
                return commandStore.new InMemoryCommandStoreCaches() {
                    @Override
                    public InMemorySafeCommand acquireIfLoaded(TxnId txnId)
                    {
                        if (cacheFull || cacheLoading.isLoaded(txnId))
                            return super.acquireIfLoaded(txnId);
                        return null;
                    }

                    @Override
                    public InMemorySafeCommandsForKey acquireIfLoaded(RoutingKey key)
                    {
                        if (cacheFull || cacheLoading.isLoaded(key))
                            return super.acquireIfLoaded(key);
                        return null;
                    }
                };
            }
            return null;

        }
    }

    public List<DelayedCommandStore> unsafeStores()
    {
        List<DelayedCommandStore> stores = new ArrayList<>();
        for (ShardHolder holder : current())
            stores.add((DelayedCommandStore) holder.store);
        return stores;
    }

    public interface CacheLoading
    {
        boolean cacheEmpty();
        boolean cacheFull();
        boolean isLoaded(TxnId txnId);
        boolean isLoaded(RoutingKey key);
        boolean tfkLoaded();
        void validateRead(CommandStore commandStore, Command command);
        void validateWrite(CommandStore commandStore, Command command);
        void validateRead(CommandStore commandStore, CommandsForKey cfk);
    }
}
