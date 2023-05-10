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

package org.apache.cassandra.service.accord;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.impl.CommandTimeseries;
import accord.impl.CommandTimeseriesHolder;
import accord.impl.CommandsForKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.CommandStores.RangesForEpochHolder;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.Deps;
import accord.local.SaveStatus;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRanges;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Observable;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.service.accord.async.AsyncOperation;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class AccordCommandStore extends CommandStore
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommandStore.class);

    private static final class RangeCommandSummary
    {
        private final TxnId txnId;
        private final SaveStatus status;
        private final @Nullable Timestamp executeAt;
        private final List<TxnId> deps;

        private RangeCommandSummary(TxnId txnId, SaveStatus status, @Nullable Timestamp executeAt, List<TxnId> deps)
        {
            this.txnId = txnId;
            this.status = status;
            this.executeAt = executeAt;
            this.deps = deps;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RangeCommandSummary that = (RangeCommandSummary) o;
            return txnId.equals(that.txnId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(txnId);
        }
    }

    private enum RangeCommandSummaryLoader implements CommandTimeseries.CommandLoader<RangeCommandSummary>
    {
        INSTANCE;

        @Override
        public RangeCommandSummary saveForCFK(Command command)
        {
            //TODO split write from read?
            throw new UnsupportedOperationException();
        }

        @Override
        public TxnId txnId(RangeCommandSummary data)
        {
            return data.txnId;
        }

        @Override
        public Timestamp executeAt(RangeCommandSummary data)
        {
            return data.executeAt;
        }

        @Override
        public SaveStatus saveStatus(RangeCommandSummary data)
        {
            return data.status;
        }

        @Override
        public List<TxnId> depsIds(RangeCommandSummary data)
        {
            return data.deps;
        }
    }

    private static long getThreadId(ExecutorService executor)
    {
        try
        {
            return executor.submit(() -> Thread.currentThread().getId()).get();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private final long threadId;
    public final String loggingId;
    private final ExecutorService executor;
    private final AccordStateCache stateCache;
    private final AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> commandCache;
    private final AccordStateCache.Instance<RoutableKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKeyCache;
    private AsyncOperation<?> currentOperation = null;
    private AccordSafeCommandStore current = null;
    private long lastSystemTimestampMicros = Long.MIN_VALUE;
    private IntervalTree<RoutableKey, RangeCommandSummary, Interval<RoutableKey, RangeCommandSummary>> rangesToCommands = IntervalTree.emptyTree();

    public AccordCommandStore(int id,
                              NodeTimeService time,
                              Agent agent,
                              DataStore dataStore,
                              ProgressLog.Factory progressLogFactory,
                              RangesForEpochHolder rangesForEpoch)
    {
        super(id, time, agent, dataStore, progressLogFactory, rangesForEpoch);
        this.loggingId = String.format("[%s]", id);
        this.executor = executorFactory().sequential(CommandStore.class.getSimpleName() + '[' + id + ']');
        this.threadId = getThreadId(this.executor);
        this.stateCache = new AccordStateCache(8<<20);
        this.commandCache = stateCache.instance(TxnId.class, accord.local.Command.class, AccordSafeCommand::new, AccordObjectSizes::command);
        this.commandsForKeyCache = stateCache.instance(RoutableKey.class, CommandsForKey.class, AccordSafeCommandsForKey::new, AccordObjectSizes::commandsForKey);
        executor.execute(() -> CommandStore.register(this));
        executor.execute(this::loadRangesToCommands);
    }

    private void loadRangesToCommands()
    {
        AsyncPromise<IntervalTree<RoutableKey, RangeCommandSummary, Interval<RoutableKey, RangeCommandSummary>>> future = new AsyncPromise<>();
        AccordKeyspace.findAllCommandsByDomain(id, Routable.Domain.Range, ImmutableSet.of("txn_id", "status", "txn", "execute_at", "dependencies"), new Observable<UntypedResultSet.Row>()
        {
            private IntervalTree.Builder<RoutableKey, RangeCommandSummary, Interval<RoutableKey, RangeCommandSummary>> builder = IntervalTree.builder();
            @Override
            public void onNext(UntypedResultSet.Row row) throws Exception
            {
                TxnId txnId = AccordKeyspace.deserializeTxnId(row);
                SaveStatus status = AccordKeyspace.deserializeStatus(row);
                Timestamp executeAt = AccordKeyspace.deserializeExecuteAt(row);

                PartialTxn txn = AccordKeyspace.deserializeTxn(row);
                Seekables<?, ?> keys = txn.keys();
                if (keys.domain() != Routable.Domain.Range)
                    throw new AssertionError(String.format("Txn keys are not range", txn));
                Ranges ranges = (Ranges) keys;

                PartialDeps deps = AccordKeyspace.deserializeDependencies(row);
                List<TxnId> dependsOn = deps == null ? Collections.emptyList() : deps.txnIds();
                RangeCommandSummary summary = new RangeCommandSummary(txnId, status, executeAt, dependsOn);

                for (Range range : ranges)
                    builder.add(new Interval<>(range.start(), range.end(), summary));
            }

            @Override
            public void onError(Throwable t)
            {
                builder = null;
                future.tryFailure(t);
            }

            @Override
            public void onCompleted()
            {
                IntervalTree<RoutableKey, RangeCommandSummary, Interval<RoutableKey, RangeCommandSummary>> result = builder.build();
                builder = null;
                future.trySuccess(result);
            }
        });
        try
        {
            rangesToCommands = future.get();
            logger.debug("Loaded {} intervals", rangesToCommands.intervalCount());
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public boolean inStore()
    {
        return Thread.currentThread().getId() == threadId;
    }

    @Override
    protected void registerHistoricalTransactions(Deps deps)
    {
    }

    public void setCacheSize(long bytes)
    {
        checkInStoreThread();
        stateCache.setMaxSize(bytes);
    }

    public long getCacheSize()
    {
        return stateCache.getMaxSize();
    }

    public void checkInStoreThread()
    {
        Invariants.checkState(inStore());
    }

    public void checkNotInStoreThread()
    {
        Invariants.checkState(!inStore());
    }

    public ExecutorService executor()
    {
        return executor;
    }

    public AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> commandCache()
    {
        return commandCache;
    }

    public AccordStateCache.Instance<RoutableKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKeyCache()
    {
        return commandsForKeyCache;
    }

    @VisibleForTesting
    public AccordStateCache cache()
    {
        return stateCache;
    }

    @VisibleForTesting
    public void clearCache()
    {
        stateCache.clear();
    }

    public void setCurrentOperation(AsyncOperation<?> operation)
    {
        Invariants.checkState(currentOperation == null);
        currentOperation = operation;
    }

    public AsyncOperation<?> getContext()
    {
        Invariants.checkState(currentOperation != null);
        return currentOperation;
    }

    public void unsetCurrentOperation(AsyncOperation<?> operation)
    {
        Invariants.checkState(currentOperation == operation);
        currentOperation = null;
    }

    public long nextSystemTimestampMicros()
    {
        lastSystemTimestampMicros = Math.max(TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis()), lastSystemTimestampMicros + 1);
        return lastSystemTimestampMicros;
    }
    @Override
    public <T> AsyncChain<T> submit(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
    {
        return AsyncOperation.create(this, loadCtx, function);
    }

    @Override
    public <T> AsyncChain<T> submit(Callable<T> task)
    {
        return AsyncChains.ofCallable(executor, task);
    }

    public DataStore dataStore()
    {
        return store;
    }

    NodeTimeService time()
    {
        return time;
    }

    ProgressLog progressLog()
    {
        return progressLog;
    }

    RangesForEpoch ranges()
    {
        return rangesForEpochHolder.get();
    }

    @Override
    public AsyncChain<Void> execute(PreLoadContext preLoadContext, Consumer<? super SafeCommandStore> consumer)
    {
        return AsyncOperation.create(this, preLoadContext, consumer);
    }

    public void executeBlocking(Runnable runnable)
    {
        try
        {
            executor.submit(runnable).get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public AccordSafeCommandStore beginOperation(PreLoadContext preLoadContext,
                                                 Map<TxnId, AccordSafeCommand> commands,
                                                 NavigableMap<RoutableKey, AccordSafeCommandsForKey> commandsForKeys)
    {
        Invariants.checkState(current == null);
        commands.values().forEach(AccordSafeState::preExecute);
        commandsForKeys.values().forEach(AccordSafeState::preExecute);
        current = new AccordSafeCommandStore(preLoadContext, commands, commandsForKeys, this);
        return current;
    }

    public void completeOperation(AccordSafeCommandStore store,
                                  Map<TxnId, AccordSafeCommand> commands,
                                  Map<RoutableKey, AccordSafeCommandsForKey> commandsForKeys)
    {
        Invariants.checkState(current == store);
        maybeUpdateRangeIndex(commands);
        current.complete();
        current = null;
    }


    private static CommandTimeseriesHolder fromRangeSummary(Seekable keyOrRange, List<RangeCommandSummary> matches)
    {
        return new CommandTimeseriesHolder()
        {
            @Override
            public CommandTimeseries<?> byId()
            {
                CommandTimeseries.Update<RangeCommandSummary> builder = new CommandTimeseries.Update<>(keyOrRange, RangeCommandSummaryLoader.INSTANCE);
                for (RangeCommandSummary m : matches)
                {
                    if (m.status == SaveStatus.Invalidated)
                        continue;
                    builder.add(m.txnId, m);
                }
                return builder.build();
            }

            @Override
            public CommandTimeseries<?> byExecuteAt()
            {
                CommandTimeseries.Update<RangeCommandSummary> builder = new CommandTimeseries.Update<>(null, RangeCommandSummaryLoader.INSTANCE);
                for (RangeCommandSummary m : matches)
                {
                    if (m.status == SaveStatus.Invalidated)
                        continue;
                    builder.add(m.executeAt != null ? m.executeAt : m.txnId, m);
                }
                return builder.build();
            }
        };
    }

    <O> O mapReduceForRange(Routables<?, ?> keysOrRanges, Ranges slice, BiFunction<CommandTimeseriesHolder, O, O> map, O accumulate, O terminalValue)
    {
        switch (keysOrRanges.domain())
        {
            case Key:
            {
                AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                for (Key key : keys)
                {
                    if (!slice.contains(key)) continue;

                    accumulate = map.apply(fromRangeSummary(key, rangesToCommands.search(key)), accumulate);
                    if (accumulate.equals(terminalValue))
                        return accumulate;
                }
            }
            break;
            case Range:
            {
                AbstractRanges<?> ranges = (AbstractRanges<?>) keysOrRanges;
                ranges = ranges.slice(slice, Routables.Slice.Minimal);
                for (Range range : ranges)
                {
                    accumulate = map.apply(fromRangeSummary(range, rangesToCommands.search(Interval.create(range.start(), range.end()))), accumulate);
                    if (accumulate.equals(terminalValue))
                        return accumulate;
                }
            }
            break;
            default:
                throw new AssertionError("Unknown domain: " + keysOrRanges.domain());
        }
        return accumulate;
    }

    private void maybeUpdateRangeIndex(Map<TxnId, AccordSafeCommand> commands)
    {
        for (Map.Entry<TxnId, AccordSafeCommand> e : commands.entrySet())
        {
            TxnId txnId = e.getKey();
            if (txnId.domain() != Routable.Domain.Range)
                continue;
            Command current = e.getValue().current();
            if (current.saveStatus() == SaveStatus.NotWitnessed)
                continue; // don't know the range/dependencies, so can't cache
            PartialTxn txn = current.partialTxn();
            Seekables<?, ?> keys = txn.keys();
            if (keys.domain() != Routable.Domain.Range)
                throw new AssertionError("Found a Range Transaction that had non-Range keys: " + current);
            if (keys.isEmpty())
                throw new AssertionError("Found a Range Transaction that has empty keys: " + current);
            PartialDeps deps = current.partialDeps();
            List<TxnId> dependsOn = deps == null ? Collections.emptyList() : deps.txnIds();

            RangeCommandSummary summary = new RangeCommandSummary(txnId, current.saveStatus(), current.executeAt(), dependsOn);
            Ranges ranges = (Ranges) keys;
            put(ranges, summary);
        }
    }

    private void put(Ranges ranges, RangeCommandSummary summary)
    {
        IntervalTree.Builder<RoutableKey, RangeCommandSummary, Interval<RoutableKey, RangeCommandSummary>> builder = rangesToCommands.unbuild();
        //TODO double check this tree has same inclusive/exclusive semantics as this range...
        for (Range range : ranges)
            builder.add(new Interval<>(range.start(), range.end(), summary));
        rangesToCommands = builder.build();
    }

    public void abortCurrentOperation()
    {
        current = null;
    }

    @Override
    public void shutdown()
    {
        executor.shutdown();
    }
}
